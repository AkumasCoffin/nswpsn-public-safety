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
import { recordWhisperAttempt } from './whisperStats.js';

/**
 * What a whisper server reports about ITSELF (GET /v1/stats on
 * whisper_openai_server.py). `waiting` is the reason this exists: a call
 * queued inside whisper looks in-flight to this router, so queue depth — the
 * "is this server keeping up" signal — is structurally unknowable from here.
 */
export interface BackendStats {
  model: string | null;
  device: string | null;
  computeType: string | null;
  waiting: number;
  active: number;
  totalOk: number;
  totalFailed: number;
  avgS: number | null;
  p95S: number | null;
  uptimeS: number;
  /** How many transcriptions this server can genuinely run at once
   *  (faster-whisper's num_workers). The router caps itself at this rather
   *  than duplicating the value in its own config, so the two cannot drift.
   *  Null on a server too old to report it. */
  numWorkers: number | null;
}

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
  /** The server's own /v1/stats, refreshed by the probe. Null when the
   *  backend does not expose it (older whisper_openai_server.py) or the
   *  last fetch failed. */
  stats: BackendStats | null;
  statsAt: number | null;
  recentMs: number[];
  /** Consecutive TRANSCRIPTION failures — the probe cannot see these. */
  workFailures: number;
  /** Until when this backend is excluded for failing real work. */
  quarantinedUntil: number | null;
  /**
   * Hard ceiling on concurrent requests to this backend, from an explicit
   * `@N` in WHISPER_BACKENDS. Null means "follow whatever the server says
   * it can do" (stats.numWorkers), falling back to DEFAULT_MAX_IN_FLIGHT.
   */
  maxInFlightCfg: number | null;
  /** Callbacks waiting for a slot, oldest first. */
  waiters: Array<() => void>;
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

/**
 * Consecutive transcription failures before a backend is quarantined, and for
 * how long.
 *
 * The health probe cannot catch everything: measured here, a whisper whose
 * CUDA libraries were missing loaded its model, answered /v1/models, and
 * 500'd every single transcription — so it stayed "healthy", stayed
 * preferred, and taxed every call with a failed attempt before the retry
 * landed on the other server.
 *
 * Deliberately NOT folded into `healthy`: probe() marks a backend healthy on
 * every good probe, so it would un-quarantine within 5 seconds — undone by
 * the very check that cannot see the problem. Time-boxed rather than
 * permanent because the fault is usually fixed by a restart, and the first
 * call after expiry is the re-test.
 */
const WORK_FAIL_THRESHOLD = 3;
const QUARANTINE_MS = 60_000;

/**
 * Concurrency ceiling used until a backend reports its own `numWorkers`.
 *
 * Deliberately small. Over-committing is the failure this whole mechanism
 * exists to prevent, and a backend that can take more will say so on its
 * next 5s probe — whereas guessing high re-creates the queue-inside-whisper
 * problem for the few seconds before the first probe lands.
 */
const DEFAULT_MAX_IN_FLIGHT = 2;

/**
 * How long a call will wait for a slot before giving up.
 *
 * Queueing here rather than shedding is deliberate: a dropped transcript is
 * gone for good, and a call that waits is only slow. This is bounded so a
 * total stall surfaces as an error instead of holding rdio's connection
 * open indefinitely.
 */
const SLOT_WAIT_MS = 120_000;

let _backends: WhisperBackend[] | null = null;
let _timer: NodeJS.Timeout | null = null;

/**
 * "name=url,name=url", order significant.
 *
 * A url may carry an optional `@N` concurrency ceiling — `pc=http://x:8000@2`.
 * Omit it and the backend follows whatever it reports as its own num_workers,
 * which is the preferred arrangement: one source of truth, on the box.
 */
function parseBackends(spec: string): WhisperBackend[] {
  const out: WhisperBackend[] = [];
  for (const [i, part] of spec.split(',').map((p) => p.trim()).entries()) {
    if (!part) continue;
    const eq = part.indexOf('=');
    if (eq <= 0) {
      log.warn({ entry: part }, 'WHISPER_BACKENDS entry is not name=url — ignored');
      continue;
    }
    let rawUrl = part.slice(eq + 1).trim();
    let maxInFlightCfg: number | null = null;
    // Matched at the END only, so it cannot eat an '@' inside credentials.
    const at = /@(\d+)$/.exec(rawUrl);
    if (at) {
      const n = Number(at[1]);
      if (Number.isFinite(n) && n >= 1) {
        maxInFlightCfg = n;
        rawUrl = rawUrl.slice(0, at.index);
      } else {
        log.warn({ entry: part }, 'WHISPER_BACKENDS @concurrency must be >= 1 — ignored');
      }
    }
    out.push({
      name: part.slice(0, eq).trim(),
      url: rawUrl.replace(/\/$/, ''),
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
      workFailures: 0,
      quarantinedUntil: null,
      stats: null,
      statsAt: null,
      maxInFlightCfg,
      waiters: [],
    });
  }
  return out;
}

/**
 * The live ceiling for a backend: explicit config, else what the server says
 * it can run in parallel, else the conservative default.
 */
export function maxInFlightFor(b: WhisperBackend): number {
  if (b.maxInFlightCfg !== null) return b.maxInFlightCfg;
  const n = b.stats?.numWorkers;
  if (typeof n === 'number' && Number.isFinite(n) && n >= 1) return n;
  return DEFAULT_MAX_IN_FLIGHT;
}

function hasFreeSlot(b: WhisperBackend): boolean {
  return b.inFlight < maxInFlightFor(b);
}

/** Take a slot. Callers MUST pair this with releaseSlot in a finally. */
function takeSlot(b: WhisperBackend): void {
  b.inFlight += 1;
}

/**
 * Give a slot back and hand it to the longest-waiting caller, if any.
 *
 * Single release point on purpose: inFlight used to be decremented in two
 * separate places (the response path and the catch), which is exactly how a
 * permit leaks the first time a third path is added.
 */
function releaseSlot(b: WhisperBackend): void {
  b.inFlight -= 1;
  const next = b.waiters.shift();
  if (next) next();
}

/**
 * Wait until any of `candidates` has a free slot. Resolves with that backend,
 * or null if nothing freed within SLOT_WAIT_MS.
 */
async function waitForSlot(candidates: WhisperBackend[]): Promise<WhisperBackend | null> {
  if (!candidates.length) return null;
  return new Promise<WhisperBackend | null>((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      // Leave no dangling callbacks behind: a waiter that fired after the
      // timeout would consume a slot nobody is going to use.
      for (const b of candidates) {
        const i = b.waiters.indexOf(wake);
        if (i >= 0) b.waiters.splice(i, 1);
      }
      resolve(null);
    }, SLOT_WAIT_MS);

    function wake(): void {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      for (const b of candidates) {
        const i = b.waiters.indexOf(wake);
        if (i >= 0) b.waiters.splice(i, 1);
      }
      resolve(candidates.find((b) => hasFreeSlot(b)) ?? null);
    }

    for (const b of candidates) b.waiters.push(wake);
  });
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
      // The server's own view of itself, on the SAME tick — not fetched per
      // status call, which the watcher (~5s), the Nodes tab (10s) and the
      // Data tab all make: that would multiply LAN round-trips by caller
      // count for the same answer. Best-effort: a 404 (a whisper without the
      // endpoint yet) or a failure just means no stats line, never unhealthy.
      await probeStats(b);
      return;
    }
    b.lastError = `HTTP ${r.status}`;
  } catch (err) {
    b.lastError = describeRelayError(err);
  }
  b.stats = null;
  b.lastErrorAt = Date.now();
  b.consecutiveFailures += 1;
  if (b.consecutiveFailures >= FAIL_THRESHOLD) noteState(b, false);
}

async function probeStats(b: WhisperBackend): Promise<void> {
  try {
    const r = await fetch(`${b.url}/v1/stats`, {
      signal: AbortSignal.timeout(HEALTH_TIMEOUT_MS),
    });
    if (!r.ok) {
      b.stats = null;
      return;
    }
    const raw = (await r.json()) as Record<string, unknown>;
    const num = (v: unknown): number => (typeof v === 'number' && Number.isFinite(v) ? v : 0);
    const numOrNull = (v: unknown): number | null =>
      typeof v === 'number' && Number.isFinite(v) ? v : null;
    b.stats = {
      model: typeof raw['model'] === 'string' ? raw['model'] : null,
      device: typeof raw['device'] === 'string' ? raw['device'] : null,
      computeType: typeof raw['computeType'] === 'string' ? raw['computeType'] : null,
      waiting: num(raw['waiting']),
      active: num(raw['active']),
      totalOk: num(raw['totalOk']),
      totalFailed: num(raw['totalFailed']),
      avgS: numOrNull(raw['avgS']),
      p95S: numOrNull(raw['p95S']),
      uptimeS: num(raw['uptimeS']),
      numWorkers: numOrNull(raw['numWorkers']),
    };
    b.statsAt = Date.now();
  } catch {
    b.stats = null;
  }
}

/**
 * Record how long a call actually took, success or failure.
 *
 * Both outcomes go in: from rdio's point of view a call that timed out after
 * 180s really did take 180s, and leaving those out is what made the panel's
 * average look comfortable while a third of calls were failing.
 */
/** Nearest-rank percentile over a copy; null when there is nothing to rank. */
function percentile(values: number[], q: number): number | null {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1));
  return Math.round(sorted[idx]!);
}

function noteLatency(b: WhisperBackend, ms: number): void {
  b.recentMs.push(ms);
  if (b.recentMs.length > RECENT_SAMPLES) {
    b.recentMs.splice(0, b.recentMs.length - RECENT_SAMPLES);
  }
}

/** Healthy, not draining, not quarantined — in preference order. */
export function whisperCandidates(): WhisperBackend[] {
  const now = Date.now();
  return whisperBackends().filter((b) => {
    if (!b.healthy || b.draining) return false;
    if (b.quarantinedUntil !== null) {
      if (now < b.quarantinedUntil) return false;
      // Expired: a clean slate, not a hair trigger. Without this the counter
      // is still at the threshold and the first failure after expiry — even
      // an unrelated blip — re-quarantines immediately.
      b.quarantinedUntil = null;
      b.workFailures = 0;
      log.info({ backend: b.name }, 'whisper backend quarantine expired — re-testing');
    }
    return true;
  });
}

function noteWorkFailure(b: WhisperBackend): void {
  b.workFailures += 1;
  if (b.workFailures >= WORK_FAIL_THRESHOLD && b.quarantinedUntil === null) {
    b.quarantinedUntil = Date.now() + QUARANTINE_MS;
    log.warn(
      { backend: b.name, failures: b.workFailures, forMs: QUARANTINE_MS },
      'whisper backend quarantined — passes its health check but fails real work',
    );
  }
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
    recordWhisperAttempt('none', false, null); // durable hourly stats (whisper_hourly)
    return { status: 503, body: new ArrayBuffer(0), contentType: null, backend: null,
      detail: 'no whisper backend available' };
  }

  let detail = 'no backend attempted';
  // Each backend is tried at most once, in preference order, exactly as
  // before — the change is that a backend already at its ceiling is skipped
  // rather than piled onto, and we wait for a slot instead of over-committing.
  const untried = [...candidates];

  while (untried.length) {
    let b = untried.find((c) => hasFreeSlot(c)) ?? null;
    if (!b) {
      // Everything is busy. Queue rather than shed: a slow transcript beats
      // a lost one, and over-committing here is precisely what buries a
      // queue inside whisper where nothing can see or manage it.
      const freed = await waitForSlot(untried);
      if (!freed) {
        detail = `all backends busy for ${Math.round(SLOT_WAIT_MS / 1000)}s`;
        log.warn({ waited: SLOT_WAIT_MS }, 'whisper: gave up waiting for a slot');
        break;
      }
      b = freed;
    }
    // Remove from the not-yet-tried list whichever backend we settled on.
    untried.splice(untried.indexOf(b), 1);

    b.requests += 1;
    const started = Date.now();
    takeSlot(b);
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
      noteLatency(b, Date.now() - started);

      // A 5xx is the backend saying it could not do the job, which the other
      // one might manage. A 4xx is the REQUEST being wrong, and sending it
      // again changes nothing — hand that straight back.
      if (r.status >= 500) {
        b.failures += 1;
        b.lastErrorAt = Date.now();
        b.lastError = `HTTP ${r.status}`;
        detail = `${b.name}: HTTP ${r.status}`;
        noteWorkFailure(b);
        recordWhisperAttempt(b.name, false, null);
        log.warn({ backend: b.name, status: r.status }, 'whisper backend errored — trying next');
        continue;
      }
      b.workFailures = 0;
      b.quarantinedUntil = null;
      // A 4xx is a rejected REQUEST, not a transcript and not a backend
      // fault. Recording it as a success made whisper_hourly count malformed
      // uploads as work done; recording it as a failure would blame a
      // healthy server. It is neither, so it is not recorded at all.
      if (r.status < 400) recordWhisperAttempt(b.name, true, Date.now() - started);
      return {
        status: r.status,
        body: buf,
        contentType: r.headers.get('content-type'),
        backend: b.name,
      };
    } catch (err) {
      // The slow calls ARE the problem, so they belong in the latency ring.
      // Omitting them censored the displayed average at the top: it could
      // never exceed the timeout, and sat far below it.
      noteLatency(b, Date.now() - started);
      b.failures += 1;
      b.lastErrorAt = Date.now();
      b.lastError = describeRelayError(err);
      detail = `${b.name}: ${b.lastError}`;
      noteWorkFailure(b);
      recordWhisperAttempt(b.name, false, null);
      log.warn(
        { backend: b.name, cause: b.lastError, ms: Date.now() - started },
        'whisper backend failed — trying next',
      );
    } finally {
      // Single release point, and in a finally so neither the timeout path
      // nor an early return can leak a permit.
      releaseSlot(b);
    }
  }

  recordWhisperAttempt('none', false, null); // every candidate failed — the CALL got no transcript
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
      // avg and p95 come from the SAME ring on purpose. They used to be a
      // router-side mean beside a server-side p95 — two processes, two
      // populations — which could report a p95 arithmetically impossible
      // beside its own average.
      avgMs: b.recentMs.length
        ? Math.round(b.recentMs.reduce((a, n) => a + n, 0) / b.recentMs.length)
        : null,
      p95Ms: percentile(b.recentMs, 0.95),
      maxInFlight: maxInFlightFor(b),
      waiting: b.waiters.length,
      lastOkAt: b.lastOkAt ? new Date(b.lastOkAt).toISOString() : null,
      lastErrorAt: b.lastErrorAt ? new Date(b.lastErrorAt).toISOString() : null,
      lastError: b.lastError,
      stateSince: new Date(b.stateSince).toISOString(),
      stats: b.stats,
      statsAt: b.statsAt ? new Date(b.statsAt).toISOString() : null,
      quarantinedUntil:
        b.quarantinedUntil !== null && Date.now() < b.quarantinedUntil
          ? new Date(b.quarantinedUntil).toISOString()
          : null,
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
