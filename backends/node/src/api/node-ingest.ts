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
import { z } from 'zod';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { resolveNodeToken } from '../services/auth/nodeToken.js';
import { bumpNodeCallStat, getNode } from '../services/nodes/registry.js';
import { getPagerIngest } from '../services/nodes/globalConfig.js';
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

  // 3. Resolve the per-node token → the node it belongs to (role gated).
  const r = await resolveNodeToken(token);
  if (!r.ok) {
    if (r.reason === 'no_role') {
      return c.json({ error: 'contributor role removed' }, 403);
    }
    return c.json({ error: 'unauthorized' }, 401);
  }
  // TOFU: the token is bound to one machine; a different install is rejected.
  if (r.installId && r.installId !== installId) {
    return c.json({ error: 'install mismatch' }, 401);
  }
  const node = { id: r.nodeId, feed_enabled: r.feedEnabled };
  // NOTE: `enabled` (capture on/off) is NOT gated here — with capture off the
  // agent isn't decoding so no calls arrive; feed_enabled is the upload control.

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
// POST /api/node-ingest/pager-upload
//
// A PAGER feeder node decodes POCSAG locally (rtl_fm | multimon-ng) and relays
// each decoded message here. We forward it into the ONE central Pagermon with
// the server-held apikey (see globalConfig.getPagerIngest / config.PAGERMON_
// INGEST_*), so the Pagermon key stays server-side and the node's feed toggle
// cuts the feed — the SAME relay model as call-upload. Body is JSON (messages
// are tiny), NOT multipart.
// ---------------------------------------------------------------------------

// A pager message is small — a few hundred bytes. Cap hard well below that so a
// hostile/buggy node can't stream a giant body into RAM.
const MAX_PAGER_BYTES = 64 * 1024;

// Capcodes to drop entirely (non-message data/encoded transmitters). Parsed once
// from PAGER_BLOCKED_CAPCODES. Dropped before forward + drawer buffer.
const BLOCKED_CAPCODES = new Set(
  (config.PAGER_BLOCKED_CAPCODES || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean),
);

// Per-node rate limit: at most PAGER_RATE_MAX messages per PAGER_RATE_WINDOW_MS.
// Real paging is a few/min; this bounds a compromised node's flood into Pagermon
// + the DB. In-memory rolling window (ephemeral, like hub uploads).
const PAGER_RATE_MAX = 120;
const PAGER_RATE_WINDOW_MS = 60_000;
const pagerRate = new Map<string, number[]>();
function pagerRateOk(nodeId: string): boolean {
  const now = Date.now();
  const cutoff = now - PAGER_RATE_WINDOW_MS;
  const arr = (pagerRate.get(nodeId) ?? []).filter((t) => t >= cutoff);
  if (arr.length >= PAGER_RATE_MAX) {
    pagerRate.set(nodeId, arr);
    return false;
  }
  arr.push(now);
  pagerRate.set(nodeId, arr);
  return true;
}

const PagerMsgSchema = z.object({
  // POCSAG capcode / address (digits). Kept as a string — Pagermon treats it as text.
  address: z.string().min(1).max(32),
  // Decoder function bits 0-3 (optional).
  function: z.union([z.number().int().min(0).max(3), z.string().max(2)]).optional(),
  // The decoded message text.
  message: z.string().max(4000).default(''),
  // ISO-8601 timestamp from the agent; we default to now() if absent/garbage.
  timestamp: z.string().max(40).optional(),
  // A short label of the source reader (e.g. "NSWRFS" / "FRNSW").
  source: z.string().max(64).default('pager'),
  // The frequency in MHz the message was heard on (optional, informational).
  freqMhz: z.number().optional(),
});

nodeIngestRouter.post('/api/node-ingest/pager-upload', async (c) => {
  // 1. Forward target must be configured (DB row first, then env).
  const ingest = await getPagerIngest();
  if (!ingest.url || !ingest.apiKey) {
    return c.json({ error: 'pagermon ingest not configured' }, 503);
  }

  // 2-3. Node credentials + per-node token resolve (role gated), TOFU install
  //      match — identical to call-upload.
  const token = c.req.header('X-Node-Token');
  const installId = c.req.header('X-Node-Install');
  if (!token || !installId) {
    return c.json({ error: 'missing node credentials' }, 401);
  }
  const r = await resolveNodeToken(token);
  if (!r.ok) {
    if (r.reason === 'no_role') {
      return c.json({ error: 'contributor role removed' }, 403);
    }
    return c.json({ error: 'unauthorized' }, 401);
  }
  if (r.installId && r.installId !== installId) {
    return c.json({ error: 'install mismatch' }, 401);
  }
  // Only pager-kind nodes may use this route — a radio node has no reason to,
  // and its agent never posts here. Hard-reject rather than warn.
  if (r.kind !== 'pager') {
    return c.json({ error: 'not a pager node' }, 403);
  }
  const node = { id: r.nodeId, feed_enabled: r.feedEnabled };

  // 3b. Per-node rate limit — a compromised node with feed on could otherwise
  //     flood central Pagermon + the DB. Generous vs. real paging (a few/min);
  //     excess is ack'd + dropped so the agent's queue still drains.
  if (!pagerRateOk(node.id)) {
    log.warn(`pager relay: RATE-LIMITED (dropped) node=${node.id.slice(0, 8)}`);
    return c.json({ ok: true, dropped: 'rate limit' });
  }

  // 4. Size guard — require Content-Length and cap it (same rationale as the
  //    radio route: closes the chunked-body bypass).
  const lenHeader = c.req.header('content-length');
  const len = Number(lenHeader ?? '');
  if (lenHeader === undefined || !Number.isFinite(len)) {
    return c.json({ error: 'length required' }, 411);
  }
  if (len > MAX_PAGER_BYTES) {
    return c.json({ error: 'message too large' }, 413);
  }

  // 5. Parse + validate the JSON message.
  let parsed: z.infer<typeof PagerMsgSchema>;
  try {
    parsed = PagerMsgSchema.parse(await c.req.json());
  } catch {
    return c.json({ error: 'bad body' }, 400);
  }

  // Trace EVERY received message (capcode + source only, never content) so a
  // missing page can be traced to where it dropped: reception (never logged),
  // blocklist, feed-off, or forwarded. Grep the backend log for "pager rx".
  log.info(`pager rx node=${node.id.slice(0, 8)} addr=${parsed.address} src=${parsed.source} freq=${parsed.freqMhz ?? '?'}`);

  // 5a. Drop blocked capcodes (non-message data transmitters) entirely — no
  //     buffer, no forward. Ack so the agent's queue still drains.
  if (BLOCKED_CAPCODES.has(parsed.address)) {
    log.info(`pager relay: BLOCKED capcode ${parsed.address} (dropped) node=${node.id.slice(0, 8)}`);
    return c.json({ ok: true, dropped: 'blocked capcode' });
  }

  // 5b. Buffer the decoded message for the staff drawer BEFORE the feed gate, so
  //     reception is visible even when the feed is off.
  hub.recordPagerMessage(node.id, {
    address: parsed.address,
    message: parsed.message,
    source: parsed.source,
    freqMhz: typeof parsed.freqMhz === 'number' ? parsed.freqMhz : null,
    at: Date.now(),
  });

  // 6. Feed gate — accept + COUNT but don't forward when the feed is off (so
  //    reception still shows in the node's stats), ack so the queue drains.
  if (!node.feed_enabled) {
    try {
      await bumpNodeCallStat(node.id, 0);
    } catch (err) {
      log.warn({ err, node: node.id.slice(0, 8) }, 'pager relay: stat bump failed (feed off)');
    }
    log.info(`pager relay: FEED OFF, not forwarded addr=${parsed.address} node=${node.id.slice(0, 8)}`);
    return c.json({ ok: true, fed: false });
  }

  // 7. Forward to central Pagermon. Its ingest API is POST /api/messages with
  //    address/message/datetime/source. We send the apikey both as the
  //    Authorization header (Pagermon 1.x) and an `apikey` field (older) for
  //    compatibility. The `source` is the NODE'S NAME (authoritative, set here)
  //    so each page in Pagermon is attributable to the node that heard it —
  //    falling back to the agent-reported label if the row can't be read.
  const nodeRow = await getNode(node.id).catch(() => null);
  const source = nodeRow?.name || parsed.source;
  const datetime = normalisePagerDatetime(parsed.timestamp);
  const body = new URLSearchParams({
    address: parsed.address,
    message: parsed.message,
    datetime,
    source,
    apikey: ingest.apiKey,
  });
  const target = `${ingest.url.replace(/\/$/, '')}/api/messages`;
  let resp: Response;
  try {
    resp = await fetch(target, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Authorization: ingest.apiKey,
      },
      body,
    });
  } catch (err) {
    log.warn({ err, node: node.id.slice(0, 8) }, 'pager relay: fetch failed');
    return c.json({ error: 'relay failed' }, 502);
  }

  if (resp.ok) {
    hub.recordUpload(node.id);
    try {
      await bumpNodeCallStat(node.id, 0);
    } catch (err) {
      log.warn({ err, node: node.id.slice(0, 8) }, 'pager relay: stat bump failed');
    }
    log.info(`pager relay ok node=${node.id.slice(0, 8)} addr=${parsed.address} src=${source}`);
    return c.json({ ok: true });
  }

  const snippet = (await resp.text().catch(() => '')).slice(0, 200);
  log.warn(`pager relay: upstream ${resp.status} node=${node.id.slice(0, 8)} ${snippet}`);
  return c.json({ error: 'upstream rejected', status: resp.status }, 502);
});

/** Coerce the agent-supplied timestamp into an ISO string Pagermon accepts,
 *  falling back to now() when it's absent or unparseable. */
function normalisePagerDatetime(ts: string | undefined): string {
  if (ts) {
    const d = new Date(ts);
    if (!Number.isNaN(d.getTime())) return d.toISOString();
  }
  return new Date().toISOString();
}

// ---------------------------------------------------------------------------
// GET /api/node-ingest/capabilities
// Lets the LOCAL rdio's downstream probe succeed while signalling no
// transcript-forward (transcription is central).
// ---------------------------------------------------------------------------
nodeIngestRouter.get('/api/node-ingest/capabilities', (c) => {
  return c.json({ features: [] });
});
