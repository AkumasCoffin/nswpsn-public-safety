/**
 * The whisper endpoint rdio transcribes through, and the status the staff
 * panel reads.
 *
 *   POST /api/whisper/v1/audio/transcriptions  — rdio; forwarded to a backend
 *   GET  /api/whisper/v1/models                — rdio's probe; answered here
 *   GET  /api/whisper/status                   — staff panel + the PC watcher
 *   POST /api/whisper/drain                    — the PC watcher, before a stop
 *
 * rdio's transcripts plugin takes one base URL, so it points at
 * `<this backend>/api/whisper/v1` and services/whisperRouter.ts picks a
 * healthy faster-whisper server per call. See that module for why.
 *
 * AUTH IS TWO DIFFERENT THINGS HERE, on purpose:
 *
 *   - the transcription path keeps the ordinary site API key gate
 *     (requireApiKey, applied to every /api/ route that is not explicitly
 *     public). rdio has no login, but it does have an API key field for its
 *     whisper provider, so NSWPSN_API_KEY goes in there and no new secret has
 *     to exist. This route is deliberately NOT added to PUBLIC_ENDPOINTS: it
 *     is the one that spends GPU time.
 *   - /status and /drain ARE public to that gate, because the PC's watcher is
 *     a headless script with no session and no reason to hold the site key.
 *     They verify their own credential instead: the panel authenticates as a
 *     user, the watcher carries WHISPER_ADMIN_TOKEN. Either is accepted for
 *     status; drain takes the token only, because it is a machine action.
 *
 * NOT CONFIGURED IS NOT AN ERROR. With no WHISPER_BACKENDS set, /status
 * answers 200 with configured:false so the panel hides its card rather than
 * showing a failure for a feature that was never turned on.
 */
import { Hono } from 'hono';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { requireRole, canViewNodeData } from '../services/auth/roles.js';
import {
  whisperConfigured,
  whisperForward,
  whisperSetDrain,
  whisperStatus,
} from '../services/whisperRouter.js';

export const whisperRouter = new Hono();

/**
 * A transcription request body. rdio sends a few seconds of narrowband audio;
 * this is a sanity ceiling, not a working limit. Matches the standalone
 * router's posture of refusing before reading rather than buffering anything
 * that arrives.
 */
const MAX_AUDIO_BYTES = 25 * 1024 * 1024;

function adminAuthorised(c: { req: { header: (n: string) => string | undefined } }): boolean {
  const expected = config.WHISPER_ADMIN_TOKEN;
  return Boolean(expected) && c.req.header('x-whisper-token') === expected;
}

// ---------------------------------------------------------------------------
// The transcription path — rdio's side.
// ---------------------------------------------------------------------------

whisperRouter.post('/api/whisper/v1/audio/transcriptions', async (c) => {
  // No backends = the feature is off. 404 rather than 403, so an
  // unconfigured deployment gives nothing away about the endpoint existing —
  // the same posture as scanner-ingest. The CALLER has already been checked by
  // requireApiKey; there is no second key here.
  if (!whisperConfigured()) return c.notFound();

  // Checked BEFORE the body is read, so an oversize push costs nothing.
  const len = Number(c.req.header('content-length') ?? '');
  if (Number.isFinite(len) && len > MAX_AUDIO_BYTES) {
    return c.json({ error: 'body too large' }, 413);
  }

  const body = await c.req.arrayBuffer();
  const result = await whisperForward(body, c.req.header('content-type') ?? null);

  if (result.backend === null) {
    log.error({ detail: result.detail }, 'whisper: no backend took the transcription');
    return c.json({ error: result.detail ?? 'whisper unavailable' }, result.status === 503 ? 503 : 502);
  }

  return c.body(result.body, result.status as 200, {
    ...(result.contentType ? { 'Content-Type': result.contentType } : {}),
    // Which server did it, so a transcript can be traced back to a machine
    // without reading the log.
    'X-Whisper-Backend': result.backend,
  });
});

/**
 * Answered HERE, not forwarded.
 *
 * Clients ping this before transcribing. Forwarding it would make us look down
 * whenever the preferred backend happened to be mid-restart, when in fact the
 * actual work can still be routed to the other one.
 */
whisperRouter.get('/api/whisper/v1/models', (c) => {
  if (!whisperConfigured()) return c.notFound();
  return c.json({ data: [{ id: 'whisper-router', object: 'model' }] });
});

// ---------------------------------------------------------------------------
// Operator surfaces — the staff panel and the PC's idle watcher.
// ---------------------------------------------------------------------------

/**
 * Status. The panel comes with a user session; the watcher comes with the
 * admin token and polls this for inFlight while draining, so both are let in.
 */
const statusBody = () =>
  whisperConfigured() ? whisperStatus() : { configured: false, backends: [] };

whisperRouter.get(
  '/api/whisper/status',
  // The token short-circuits the role check; anything without it falls through
  // to the normal staff gate. Written as one middleware rather than two
  // registrations of the same path, which would have depended on Hono's
  // fall-through ordering to mean "or".
  async (c, next) => {
    if (adminAuthorised(c)) return c.json(statusBody());
    return requireRole(canViewNodeData)(c, next);
  },
  (c) => c.json(statusBody()),
);

/**
 * Drain, the first half of a clean stop on the PC.
 *
 * The watcher sets draining, polls /status until that backend's inFlight is 0,
 * and only then stops the service. Without it a stop lands on top of a
 * transcription in progress and that call simply never gets a transcript —
 * rdio does not retry one.
 */
whisperRouter.post('/api/whisper/drain', async (c) => {
  if (!adminAuthorised(c)) return c.json({ error: 'unauthorised' }, 401);
  if (!whisperConfigured()) return c.json({ error: 'whisper routing not configured' }, 404);

  let payload: { backend?: unknown; draining?: unknown };
  try {
    payload = (await c.req.json()) as typeof payload;
  } catch {
    return c.json({ error: 'invalid json' }, 400);
  }
  const name = typeof payload.backend === 'string' ? payload.backend : '';
  const draining = payload.draining !== false;

  const b = whisperSetDrain(name, draining);
  if (!b) return c.json({ error: `unknown backend ${JSON.stringify(name)}` }, 404);
  return c.json({ ok: true, backend: b.name, draining: b.draining, inFlight: b.inFlight });
});
