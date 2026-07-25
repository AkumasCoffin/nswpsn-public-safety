/**
 * Feeder-node call relay (Phase 2).
 *
 * A remote "node agent" (Go) receives radio calls from its LOCAL rdio-scanner
 * and relays each one to this backend, which forwards it into the CENTRAL
 * rdio-scanner. The node authenticates with its own feeder token (X-Node-Token
 * + X-Node-Install) — NOT the site NSWPSN_API_KEY — and we swap the incoming
 * `key` field for the one server-held RDIO_INTERNAL_API_KEY before forwarding.
 * That means there is exactly ONE key in the central instance and toggling a
 * node's `enabled` flag in the staff panel is what cuts its calls (no juggling
 * rdio keys). See config.ts (RDIO_INTERNAL_URL / RDIO_INTERNAL_API_KEY).
 *
 * The prefix '/api/node-ingest/' is exempted from the global NSWPSN_API_KEY
 * gate (see services/auth/apiKey.ts) because the handler does its own feeder
 * token auth.
 *
 *   POST /api/node-ingest/call-upload   — relay one call into central rdio
 *   GET  /api/node-ingest/capabilities  — downstream probe target
 */
import { Hono } from 'hono';
import type { BodyData } from 'hono/utils/body';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { resolveFeederToken } from '../services/auth/nodeToken.js';
import { getNodeByInstall, bumpNodeCallStat } from '../services/nodes/registry.js';
import { hub } from '../services/nodes/hub.js';

export const nodeIngestRouter = new Hono();

// Hard cap on a relayed call body. Calls are small (~100KB MP3s); anything
// over this is almost certainly a bug or abuse, so reject before buffering.
// We check Content-Length manually rather than using hono's bodyLimit
// middleware, which fully buffers chunked (no Content-Length) bodies in RAM.
const MAX_CALL_BYTES = 20 * 1024 * 1024;

// ---------------------------------------------------------------------------
// POST /api/node-ingest/call-upload
// ---------------------------------------------------------------------------
nodeIngestRouter.post('/api/node-ingest/call-upload', async (c) => {
  // 1. Relay must be configured. Capture into locals so the string narrowing
  //    survives the awaits below (property narrowing on `config` would not).
  const internalUrl = config.RDIO_INTERNAL_URL;
  const internalKey = config.RDIO_INTERNAL_API_KEY;
  if (!internalUrl || !internalKey) {
    return c.json({ error: 'relay not configured' }, 503);
  }

  // 2. Node credentials.
  const token = c.req.header('X-Node-Token');
  const installId = c.req.header('X-Node-Install');
  if (!token || !installId) {
    return c.json({ error: 'missing node credentials' }, 401);
  }

  // 3. Resolve the feeder token → owning user (still holding the role).
  const r = await resolveFeederToken(token);
  if (!r.ok) {
    if (r.reason === 'no_role') {
      return c.json({ error: 'contributor role removed' }, 403);
    }
    if (r.reason === 'unconfigured') {
      return c.json({ error: 'relay not configured' }, 503);
    }
    return c.json({ error: 'unauthorized' }, 401);
  }

  // 4. Node must exist under this user and be enabled. The agent treats
  //    401/403/404 as "drop this call, don't retry".
  const node = await getNodeByInstall(r.userId, installId);
  if (!node) {
    return c.json({ error: 'unknown node' }, 404);
  }
  if (!node.enabled) {
    return c.json({ error: 'node disabled' }, 403);
  }

  // 5. Size guard (manual — see MAX_CALL_BYTES comment). Legit uploads (the
  //    agent's Go sender, which posts a fixed []byte) always carry a
  //    Content-Length, so REQUIRE it and cap on it. This also closes the
  //    Transfer-Encoding: chunked bypass: without this, a chunked body has no
  //    Content-Length, `len` reads as 0, sails past the cap, and parseBody then
  //    buffers the whole (unbounded) body into RAM.
  const lenHeader = c.req.header('content-length');
  const len = Number(lenHeader ?? '');
  if (lenHeader === undefined || !Number.isFinite(len)) {
    return c.json({ error: 'length required' }, 411);
  }
  if (len > MAX_CALL_BYTES) {
    return c.json({ error: 'call too large' }, 413);
  }

  // 6. Parse the multipart body.
  let form: BodyData<{ all: true }>;
  try {
    form = await c.req.parseBody({ all: true });
  } catch {
    return c.json({ error: 'bad body' }, 400);
  }

  // 6b. Feed gate. A node is enabled (decoding, streaming spectrum/status) but
  //     its feed to the central rdio is a separate switch that starts OFF. When
  //     the feed is off, accept + COUNT the call (so reception shows in the
  //     node's per-node stats) but do NOT forward it, and ack so the agent's
  //     queue drains instead of retrying.
  if (!node.feed_enabled) {
    let bytes = 0;
    const audio = form['audio'];
    const af = Array.isArray(audio) ? audio.find((x) => x instanceof File) : audio;
    if (af instanceof File) bytes = af.size;
    try {
      await bumpNodeCallStat(node.id, bytes);
    } catch (err) {
      log.warn({ err, node: node.id.slice(0, 8) }, 'node relay: stat bump failed (feed off)');
    }
    log.info(`node relay: feed off, not forwarded node=${node.id.slice(0, 8)} bytes=${bytes}`);
    return c.json({ ok: true, fed: false });
  }

  // 7. Rebuild a FormData, copying every field/file EXCEPT `key`, which we
  //    replace with the server-held internal key. Repeated keys arrive as
  //    arrays (all: true) — append each. Files keep their filename + we grab
  //    the audio file's size for the per-node byte rollup.
  const fd = new FormData();
  let bytes = 0;
  for (const [name, value] of Object.entries(form)) {
    // Drop any incoming `key` — the internal key is force-set below so the
    // node can never inject an alternate central-rdio key.
    if (name === 'key') continue;
    const values = Array.isArray(value) ? value : [value];
    for (const v of values) {
      if (v instanceof File) {
        fd.append(name, v, v.name);
        if (name === 'audio') bytes = v.size;
      } else {
        fd.append(name, v);
      }
    }
  }
  // set() (not append()) guarantees exactly one `key`, present even if the
  // incoming body carried none.
  fd.set('key', internalKey);

  // 8. Forward to the central rdio-scanner. fetch derives the multipart
  //    boundary from the FormData automatically.
  const target = `${internalUrl.replace(/\/$/, '')}/api/call-upload`;
  let resp: Response;
  try {
    resp = await fetch(target, { method: 'POST', body: fd });
  } catch (err) {
    log.warn({ err, node: node.id.slice(0, 8) }, 'node relay: fetch failed');
    return c.json({ error: 'relay failed' }, 502);
  }

  // 9. Success → best-effort stats bump + ok. Otherwise surface upstream.
  if (resp.ok) {
    hub.recordUpload(node.id); // rolling 10-min "calls forwarded" signal
    try {
      await bumpNodeCallStat(node.id, bytes);
    } catch (err) {
      log.warn({ err, node: node.id.slice(0, 8) }, 'node relay: stat bump failed');
    }
    log.info(`node relay ok node=${node.id.slice(0, 8)} bytes=${bytes}`);
    return c.json({ ok: true });
  }

  // 10. Upstream rejected — log status + a short body snippet.
  const snippet = (await resp.text().catch(() => '')).slice(0, 200);
  log.warn(
    `node relay: upstream ${resp.status} node=${node.id.slice(0, 8)} ${snippet}`,
  );
  return c.json({ error: 'upstream rejected', status: resp.status }, 502);
});

// ---------------------------------------------------------------------------
// GET /api/node-ingest/capabilities
// Lets the LOCAL rdio's downstream probe succeed while signalling no
// transcript-forward (transcription is central).
// ---------------------------------------------------------------------------
nodeIngestRouter.get('/api/node-ingest/capabilities', (c) => {
  return c.json({ features: [] });
});
