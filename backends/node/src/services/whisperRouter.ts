/**
 * Failover across the faster-whisper servers that transcribe rdio's audio.
 *
 * WHY THIS IS HERE AT ALL
 * rdio-scanner's transcripts plugin takes exactly ONE base URL. There are two
 * whisper servers — one on an always-on VM, one on a PC that can only run
 * while nobody is using it — so something has to choose between them per call.
 * rdio points at this backend permanently and never needs reconfiguring.
 *
 *     rdio ──► /api/whisper/v1 ──┬─► pc  (preferred; up only while idle)
 *                                └─► vm  (always on; the safety net)
 *
 * ORDER IS PREFERENCE. Backends are tried in the order WHISPER_BACKENDS lists
 * them: the first healthy, non-draining one takes the call, and if that
 * request fails outright the next one gets it. Fast one first, dependable one
 * last.
 *
 * DRAINING is what makes the PC's stop-on-use safe. Killing whisper
 * mid-transcription loses that call's transcript and rdio does not come back
 * for it, so the PC's watcher drains this backend first, waits for inFlight to
 * reach 0, and only then stops the service.
 *
 * HEALTH is GET /v1/models on each backend. That is the right probe precisely
 * because whisper_openai_server loads its model at import, before uvicorn
 * binds — a backend that answers at all has a model in memory and is genuinely
 * ready, so there is no warm-up state to guess about.
 *
 * This lives in the backend rather than as its own service because everything
 * is on one LAN, so there is no network cost to the extra hop, and one fewer
 * supervised process is worth more than the isolation. The cost that buys is
 * real and worth knowing: a pm2 restart now interrupts transcription, which is
 * why ecosystem.config.js carries a kill_timeout longer than the slowest
 * observed call and shutdown() lets in-flight requests finish.
 */
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { describeRelayError } from '../lib/relayError.js';

export interface WhisperBackend {
  name: string;
  url: string;
  priority: number;
  healthy: boolean;
  /** Finish what you have, take nothing new. Set by the PC's idle watcher. */
  draining: boolean;
  inFlight: number;
  consecutiveFailures: number;
  requests: number;
  failures: number;
  lastOkAt: number | null;
  lastErrorAt: number | null;
  lastError: string | null;
  /** When `healthy` last flipped — a backend flapping every 90s looks
   *  identical to a steady one without it. */
  stateSince: number;
  recentMs: number[];
}

/** How often each backend is probed, and how long a probe may take. */
const HEALTH_INTERVAL_MS = 5_000;
const HEALTH_TIMEOUT_MS = 3_000;
/**
 * A whole transcription. Generous on purpose: a long call on a busy CPU
 * backend is slow, and cutting it off loses that transcript for good. Measured
 * on this deployment, an honest one has taken 16.9s.
 */
const REQUEST_TIMEOUT_MS = 180_000;
/**
 * Consecutive probe failures before a backend is taken out. One is too eager —
 * a single dropped packet would flap the whole feed to the other server.
 */
const FAIL_THRESHOLD = 2;
/** Rolling window for the average shown on the staff panel. */
const RECENT_SAMPLES = 50;

let _backends: WhisperBackend[] | null = null;
let _timer: NodeJS.Timeout | null = null;

/** "name=url,name=url", order significant. */
function parseBackends(spec: string): WhisperBackend[] {
  const out: WhisperBackend[] = [];
  for (const [i, part] of spec.split(',').map((p) => p.trim()).entries()) {
    if (!part) continue;
    const eq = part.indexOf('=');
    if (eq <= 0) {
      log.warn({ entry: part }, 'WHISPER_BACKENDS entry is not name=url — ignored');
      continue;
    }
    out.push({
      name: part.slice(0, eq).trim(),
      url: part.slice(eq + 1).trim().replace(/\/$/, ''),
      priority: i,
      healthy: false,
      draining: false,
      inFlight: 0,
      consecutiveFailures: 0,
      requests: 0,
      failures: 0,
      lastOkAt: null,
      lastErrorAt: null,
      lastError: null,
      stateSince: Date.now(),
      recentMs: [],
    });
  }
  return out;
}

export function whisperBackends(): WhisperBackend[] {
  _backends ??= parseBackends(config.WHISPER_BACKENDS ?? '');
  return _backends;
}

/** Configured at all? Unset means the whole feature is off, not broken. */
export function whisperConfigured(): boolean {
  return whisperBackends().length > 0;
}

function noteState(b: WhisperBackend, healthy: boolean): void {
  if (b.healthy === healthy) return;
  b.healthy = healthy;
  b.stateSince = Date.now();
  log.info({ backend: b.name }, `whisper backend ${healthy ? 'healthy' : 'DOWN'}`);
}

async function probe(b: WhisperBackend): Promise<void> {
  try {
    const r = await fetch(`${b.url}/v1/models`, {
      signal: AbortSignal.timeout(HEALTH_TIMEOUT_MS),
    });
    if (r.status < 500) {
      b.consecutiveFailures = 0;
      b.lastOkAt = Date.now();
      // One good answer is enough to come back: the model is loaded before the
      // port is bound, so there is no half-ready state to wait out.
      noteState(b, true);
      return;
    }
    b.lastError = `HTTP ${r.status}`;
  } catch (err) {
    b.lastError = describeRelayError(err);
  }
  b.lastErrorAt = Date.now();
  b.consecutiveFailures += 1;
  if (b.consecutiveFailures >= FAIL_THRESHOLD) noteState(b, false);
}

/** Healthy, not draining, in preference order. */
export function whisperCandidates(): WhisperBackend[] {
  return whisperBackends().filter((b) => b.healthy && !b.draining);
}

export interface ForwardResult {
  status: number;
  body: ArrayBuffer;
  contentType: string | null;
  backend: string | null;
  /** Set when nothing could take it, for the log line and the 502 body. */
  detail?: string;
}

/**
 * Forward one transcription to the first backend that will take it.
 *
 * The body is buffered rather than streamed, which is what makes the retry
 * possible at all: a streamed body is consumed by the first attempt and there
 * is nothing left to send to the second. rdio's calls are seconds of
 * narrowband audio — a few KB to a few tens of KB — so this is cheap.
 */
export async function whisperForward(
  body: ArrayBuffer,
  contentType: string | null,
): Promise<ForwardResult> {
  const candidates = whisperCandidates();
  if (candidates.length === 0) {
    return { status: 503, body: new ArrayBuffer(0), contentType: null, backend: null,
      detail: 'no whisper backend available' };
  }

  let detail = 'no backend attempted';
  for (const b of candidates) {
    b.inFlight += 1;
    b.requests += 1;
    const started = Date.now();
    try {
      const r = await fetch(`${b.url}/v1/audio/transcriptions`, {
        method: 'POST',
        // The multipart boundary lives in Content-Type, so it has to go
        // through verbatim.
        headers: contentType ? { 'Content-Type': contentType } : {},
        body,
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      const buf = await r.arrayBuffer();
      b.inFlight -= 1;
      b.recentMs.push(Date.now() - started);
      if (b.recentMs.length > RECENT_SAMPLES) b.recentMs.splice(0, b.recentMs.length - RECENT_SAMPLES);

      // A 5xx is the backend saying it could not do the job, which the other
      // one might manage. A 4xx is the REQUEST being wrong, and sending it
      // again changes nothing — hand that straight back.
      if (r.status >= 500) {
        b.failures += 1;
        b.lastErrorAt = Date.now();
        b.lastError = `HTTP ${r.status}`;
        detail = `${b.name}: HTTP ${r.status}`;
        log.warn({ backend: b.name, status: r.status }, 'whisper backend errored — trying next');
        continue;
      }
      return {
        status: r.status,
        body: buf,
        contentType: r.headers.get('content-type'),
        backend: b.name,
      };
    } catch (err) {
      b.inFlight -= 1;
      b.failures += 1;
      b.lastErrorAt = Date.now();
      b.lastError = describeRelayError(err);
      detail = `${b.name}: ${b.lastError}`;
      log.warn(
        { backend: b.name, cause: b.lastError, ms: Date.now() - started },
        'whisper backend failed — trying next',
      );
    }
  }

  return { status: 502, body: new ArrayBuffer(0), contentType: null, backend: null, detail };
}

/**
 * Stop sending new work to a backend, or start again.
 *
 * Deliberately NOT persisted. If the backend restarts, everything healthy is
 * in play again — the safe default, since the alternative is coming back up
 * quietly refusing to use a server that is running perfectly well.
 */
export function whisperSetDrain(name: string, draining: boolean): WhisperBackend | null {
  const b = whisperBackends().find((x) => x.name === name);
  if (!b) return null;
  b.draining = draining;
  log.info({ backend: b.name, draining, inFlight: b.inFlight }, 'whisper drain set');
  return b;
}

/** The shape the staff panel renders, and the watcher polls for inFlight. */
export function whisperStatus() {
  const candidates = whisperCandidates();
  return {
    configured: true,
    // The backend that would take the NEXT call. Null means every one of them
    // is down or draining and transcripts are being refused.
    current: candidates[0]?.name ?? null,
    anyAvailable: candidates.length > 0,
    generatedAt: new Date().toISOString(),
    backends: whisperBackends().map((b) => ({
      name: b.name,
      url: b.url,
      priority: b.priority,
      healthy: b.healthy,
      draining: b.draining,
      inFlight: b.inFlight,
      requests: b.requests,
      failures: b.failures,
      avgMs: b.recentMs.length
        ? Math.round(b.recentMs.reduce((a, n) => a + n, 0) / b.recentMs.length)
        : null,
      lastOkAt: b.lastOkAt ? new Date(b.lastOkAt).toISOString() : null,
      lastErrorAt: b.lastErrorAt ? new Date(b.lastErrorAt).toISOString() : null,
      lastError: b.lastError,
      stateSince: new Date(b.stateSince).toISOString(),
    })),
  };
}

export function startWhisperHealth(): void {
  if (_timer) return;
  const backends = whisperBackends();
  if (backends.length === 0) return;
  log.info(
    { backends: backends.map((b) => `${b.name}@${b.url}`).join(' -> ') },
    'whisper routing enabled (in preference order)',
  );
  const tick = () => {
    void Promise.all(backends.map((b) => probe(b)));
  };
  tick();
  _timer = setInterval(tick, HEALTH_INTERVAL_MS);
  _timer.unref?.();
}

export function stopWhisperHealth(): void {
  if (_timer) clearInterval(_timer);
  _timer = null;
}

/** Tests only: forget the parsed config so a new one can be read. */
export function resetWhisperBackends(): void {
  stopWhisperHealth();
  _backends = null;
}
