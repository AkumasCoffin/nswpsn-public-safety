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
 *   POST /api/node-ingest/activity      — vce activity event batches (radio
 *                                         Data-tab row source, migration 044)
 *   POST /api/node-ingest/pager-upload  — relay one page into central Pagermon
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
import {
  recordActivityEvents,
  markRecorded,
  mergeAutomaticPatch,
  recordPagerEvent,
  upsertSiteSnapshots,
  safeInt,
} from '../services/nodeEvents.js';

export const nodeIngestRouter = new Hono();

/** Per-node rolling-window rate limiter (in-memory, ephemeral — same
 *  lifetime model as hub upload signals). Returns an `ok(nodeId)` check
 *  allowing at most `max` hits per `windowMs`. */
function makeNodeRateLimiter(max: number, windowMs: number): (nodeId: string) => boolean {
  const hits = new Map<string, number[]>();
  return (nodeId: string): boolean => {
    const now = Date.now();
    const cutoff = now - windowMs;
    const arr = (hits.get(nodeId) ?? []).filter((t) => t >= cutoff);
    if (arr.length >= max) {
      hits.set(nodeId, arr);
      return false;
    }
    arr.push(now);
    hits.set(nodeId, arr);
    return true;
  };
}

// Hard cap on a relayed call body. Calls are small (~100KB MP3s); anything
// over this is almost certainly a bug or abuse, so reject before buffering.
// We check Content-Length manually rather than using hono's bodyLimit
// middleware, which fully buffers chunked (no Content-Length) bodies in RAM.
const MAX_CALL_BYTES = 20 * 1024 * 1024;

/**
 * The `patches` field on an audio upload: the talkgroups the decoder observed
 * carrying this one transmission, as an AUTOMATIC patch.
 *
 * Sent as a JSON array of ids. Anything else — absent, empty, unparseable — is
 * "not patched", which is overwhelmingly the common case and must stay silent.
 */
function parsePatchMembers(raw: string | null): number[] {
  const text = (raw ?? '').trim();
  if (!text || text === '[]') return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed)) return [];
  const out: number[] = [];
  for (const entry of parsed) {
    // Tolerate both a bare id and an object carrying one, since the decoder's
    // shape here is not something we control.
    const raw2 =
      entry !== null && typeof entry === 'object'
        ? (entry as Record<string, unknown>)['id'] ?? (entry as Record<string, unknown>)['talkgroup']
        : entry;
    const n = Number(raw2);
    if (Number.isInteger(n) && n > 0 && !out.includes(n)) out.push(n);
  }
  return out;
}

/** First STRING value of a multipart field parsed with { all: true }
 *  (repeated keys arrive as arrays; files are skipped). */
function formFirstString(form: BodyData<{ all: true }>, name: string): string | null {
  const v = form[name];
  const first = Array.isArray(v) ? v.find((x) => typeof x === 'string') : v;
  return typeof first === 'string' ? first : null;
}

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

  // 6a. Recorded-flag stamp (migration 044). Deferred to the DELIVERED paths
  //     below (feed-off, and a successful forward) rather than run here.
  //
  //     It used to run unconditionally at this point, before we knew whether
  //     the call reached central rdio. A failed forward returns 502, the
  //     agent's queue re-POSTs the identical body, and this ran AGAIN — each
  //     time flagging another event row and folding the same audio_bytes into
  //     node_radio_hourly, a FOREVER table. Because an outage makes every
  //     queued call retry, that was correlated mass over-counting, not noise.
  //     bumpNodeCallStat was immune only because it already sat in those two
  //     branches; this now follows the same rule.
  //
  //     It also makes the flag truthful: `recorded` means "audio exists in
  //     central rdio", so a call whose forward permanently failed should NOT
  //     carry it, and no longer does.
  const stampRecorded = async (): Promise<void> => {
    if (r.kind !== 'radio') return;
    try {
      // The call's OWN start time, not arrival time — matching is keyed on
      // when the call happened, so a late delivery still finds its event.
      const epoch = Number(formFirstString(form, 'dateTime') ?? '');
      const startedAt = Number.isFinite(epoch) ? new Date(epoch * 1000) : new Date();
      const audioField = form['audio'];
      const audioFile = Array.isArray(audioField)
        ? audioField.find((x) => x instanceof File)
        : audioField;
      await markRecorded(
        node.id,
        safeInt(formFirstString(form, 'talkgroup')),
        startedAt,
        audioFile instanceof File ? audioFile.size : 0,
        // vce sends the calling radio and the traffic-channel frequency with
        // every upload (RdioScannerBroadcaster: FormField.SOURCE / FREQUENCY).
        // They identify WHICH call this audio is, which timestamp proximity
        // alone cannot when a talkgroup is busy.
        safeInt(formFirstString(form, 'source')),
        safeInt(formFirstString(form, 'frequency')),
        // The radio's over-the-air alias, when it transmitted one. Sent on
        // every upload (FormField.TALKER_ALIAS) and previously discarded here,
        // which is why every radio showed as a bare id.
        formFirstString(form, 'talkerAlias') ?? null,
      );
      // AUTOMATIC patches. vce sends the talkgroups it saw carrying this
      // transmission as `patches` (RdioScannerBroadcaster), and this upload is
      // the ONLY place that reaches us — the activity feed the grouping runs on
      // carries no patch field at all (ControlActivityLookup emits a fixed
      // column set). By now each member has usually opened its own logical
      // call, so this folds them back into one. Configured patches are handled
      // earlier, at grouping time, from rdio's own patch table.
      //
      // System is deliberately NOT passed. The upload's `system` field is the
      // node's own rdio config number, while node_radio_events.system holds
      // the P25 systemId the activity feed decodes (migration 044) — handing
      // one to the other would scope the merge by a number that means nothing
      // in that column. Null widens the merge to any system, which is correct
      // here because this deployment decodes exactly one; a multi-system
      // deployment must resolve the real systemId and pass it.
      await mergeAutomaticPatch(
        node.id,
        parsePatchMembers(formFirstString(form, 'patches')),
        startedAt,
        safeInt(formFirstString(form, 'source')),
      );
    } catch (err) {
      log.warn({ err, node: node.id.slice(0, 8) }, 'node relay: recorded stamp failed');
    }
  };

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
    // Delivered as far as it will ever go: the feed is off by policy, not by
    // failure, so the flag belongs here.
    await stampRecorded();
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
    // The audio is now in central rdio, so the flag is true. Only here —
    // a 502 below leaves it unflagged and the agent's retry stamps it once,
    // on the attempt that actually lands.
    await stampRecorded();
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
// POST /api/node-ingest/activity
//
// Batches of vce ACTIVITY events from a RADIO node's agent. These are the
// ONLY source of node_radio_events rows (migration 044): they carry the real
// P25 identity (wacn/systemId) + per-event site/action/encryption that rdio
// call uploads don't. Nothing is forwarded anywhere — this is pure capture,
// so neither the relay config nor the feed gate applies. Re-sent batches
// dedupe on (node, streamId, event id) and are NOT errors: `accepted` counts
// newly-inserted events only.
// ---------------------------------------------------------------------------

// JSON body cap. 500 events × ~200 bytes is well under this; anything bigger
// is a bug or abuse. Manual Content-Length guard like the other routes (NEVER
// hono bodyLimit — it fully buffers chunked bodies in RAM).
const MAX_ACTIVITY_BYTES = 256 * 1024;

// Per-node rate limit: the agent batches (≤500 events/post), so even a busy
// network needs a handful of posts per minute. 30/min bounds a hostile node.
const activityRateOk = makeNodeRateLimiter(30, 60_000);

const ActivityEventSchema = z.object({
  // Agent-side event id, unique per stream (dedupe key with streamId).
  id: z.number().int().min(0),
  // Event time, unix milliseconds (server clamps to now±48h).
  atMs: z.number().int(),
  action: z.string().min(1).max(32),
  eventType: z.string().min(1).max(48),
  source: z.number().int().nullish(),
  target: z.number().int().nullish(),
  frequencyHz: z.number().int().nullish(),
  timeslot: z.number().int().nullish(),
  encrypted: z.boolean().default(false),
  rfss: z.number().int().nullish(),
  site: z.number().int().nullish(),
  nac: z.number().int().nullish(),
  wacn: z.number().int().nullish(),
  systemId: z.number().int().nullish(),
  // Validated but not stored — labels resolve from the global agencies
  // config at read time (see services/nodeEvents.ts).
  channelName: z.string().max(128).nullish(),
  // Friendly P25 system name (e.g. "NSWPSN") → node_radio_events.system_label.
  // Talker/OTA alias for the source radio → node_radio_events.source_alias.
  // Both optional/nullable so older agents (which omit them) still validate.
  systemName: z.string().max(128).nullish(),
  sourceAlias: z.string().max(128).nullish(),
  // The talkgroups patched into this call. A patched transmission carries the
  // PATCH GROUP as its `target` — a supergroup nobody scans — so without these
  // the real channels carrying the conversation are unknowable. Omitted by the
  // agent when empty and absent entirely from control servers older than the
  // change that added it, hence nullish. Capped well above any real patch.
  patchMembers: z.array(z.number().int().positive()).max(64).nullish(),
});

const ActivityBodySchema = z.object({
  // The agent's decoder-session id: event ids restart per stream, so the
  // pair (streamId, id) is the idempotency key.
  streamId: z.string().min(8).max(64),
  events: z.array(ActivityEventSchema).max(500),
});

nodeIngestRouter.post('/api/node-ingest/activity', async (c) => {
  // 1-2. Node credentials + per-node token resolve (role gated), TOFU install
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
  // Only radio-kind nodes decode P25 activity. Hard-reject the rest.
  if (r.kind !== 'radio') {
    return c.json({ error: 'not a radio node' }, 403);
  }
  const node = { id: r.nodeId };

  // 3. Per-node rate limit. Must be a NON-2xx: the shipper advances its cursor
  //    on any 2xx, so an ack here would silently lose the batch. 429 keeps the
  //    cursor in place and the agent retries the same events next tick.
  if (!activityRateOk(node.id)) {
    log.warn(`activity ingest: RATE-LIMITED node=${node.id.slice(0, 8)}`);
    return c.json({ ok: false, error: 'rate limit' }, 429);
  }

  // 4. Size guard — require Content-Length and cap it (closes the
  //    chunked-body bypass, same rationale as the other routes).
  const lenHeader = c.req.header('content-length');
  const len = Number(lenHeader ?? '');
  if (lenHeader === undefined || !Number.isFinite(len)) {
    return c.json({ error: 'length required' }, 411);
  }
  if (len > MAX_ACTIVITY_BYTES) {
    return c.json({ error: 'batch too large' }, 413);
  }

  // 5. Parse + validate.
  let parsed: z.infer<typeof ActivityBodySchema>;
  try {
    parsed = ActivityBodySchema.parse(await c.req.json());
  } catch {
    return c.json({ error: 'bad body' }, 400);
  }

  // 6. Record. Fire-safe by contract (never throws); returns how many events
  //    were NEWLY inserted — deduped re-sends yield a smaller `accepted` and
  //    that is success, not an error. `failed` is different: those events
  //    rolled back and exist nowhere.
  const { accepted, failed } = await recordActivityEvents(
    node.id,
    parsed.streamId,
    parsed.events.map((ev) => ({
      id: ev.id,
      atMs: ev.atMs,
      action: ev.action,
      eventType: ev.eventType,
      source: ev.source ?? null,
      target: ev.target ?? null,
      frequencyHz: ev.frequencyHz ?? null,
      timeslot: ev.timeslot ?? null,
      encrypted: ev.encrypted,
      rfss: ev.rfss ?? null,
      site: ev.site ?? null,
      nac: ev.nac ?? null,
      wacn: ev.wacn ?? null,
      systemId: ev.systemId ?? null,
      channelName: ev.channelName ?? null,
      systemName: ev.systemName ?? null,
      sourceAlias: ev.sourceAlias ?? null,
      patchMembers: ev.patchMembers ?? null,
    })),
  );
  log.info(
    `activity ingest node=${node.id.slice(0, 8)} stream=${parsed.streamId.slice(0, 12)} events=${parsed.events.length} accepted=${accepted}${failed ? ` FAILED=${failed}` : ''}`,
  );
  // Any failure refuses the whole ack. The agent advances its cursor on any
  // 2xx, so acking a partial batch made the failed events cease to exist —
  // they rolled back here and the agent never offered them again. A 503 makes
  // it re-send the batch, and the (node_id, stream_id, source_event_id)
  // unique index absorbs the events that DID land, so a retry can only fill
  // the gap, never double-count.
  if (failed > 0) {
    return c.json({ ok: false, accepted, failed }, 503);
  }
  return c.json({ ok: true, accepted });
});

// ---------------------------------------------------------------------------
// POST /api/node-ingest/site-snapshots
//
// Deep P25 site metadata (migration 047) from a RADIO node's agent — the
// sdrtrunk-vce GET /site/snapshots feed (control channel, channel plan,
// neighbors, frequency bands, decode quality). Pure capture like /activity:
// nothing forwarded, no relay config / feed gate. Idempotent — re-POSTing a
// batch UPSERTs on (node, system, rfss, site), never duplicates. Same node
// auth + guards as /activity.
// ---------------------------------------------------------------------------

// JSON body cap. Manual Content-Length guard (NEVER hono bodyLimit — it
// buffers chunked bodies).
//
// The original 512KB was sized for "a node monitors a handful of sites". That
// is wrong: a node reports every site it has OBSERVED, and a live node was
// posting 116 of them, each carrying nested channels, neighbours, bands and
// patch groups. It outgrew the cap and began returning 413, which silently
// stopped ALL site metadata updating — the site drill-downs just went stale,
// with the rejection visible only in the API log.
//
// 4MB is a stopgap. The real fix is upstream: the node's /site/snapshots has
// no LIMIT and no time filter, so this payload grows without bound as the node
// observes more site generations. Until that is bounded, this cap must sit
// above whatever a real node produces, because exceeding it loses the data.
const MAX_SITE_BYTES = 4 * 1024 * 1024;

// Per-node rate limit. Sites change rarely; the agent re-posts its full set
// on a slow cadence, so a few posts/min is plenty. 20/min bounds abuse.
const siteRateOk = makeNodeRateLimiter(20, 60_000);

// Nested facts are stored verbatim as JSONB (read whole, never queried
// column-wise), so they are passthrough-validated as arrays/records rather
// than field-by-field — keeps the ingest tolerant of vce contract additions.
const SiteChannelSchema = z.record(z.string(), z.unknown());
const SiteSnapshotSchema = z.object({
  systemId: z.number().int().nullish(),
  rfss: z.number().int().nullish(),
  siteId: z.number().int().nullish(),
  guid: z.string().max(128).nullish(),
  systemName: z.string().max(128).nullish(),
  wacn: z.number().int().nullish(),
  nac: z.number().int().nullish(),
  lra: z.number().int().nullish(),
  channelName: z.string().max(128).nullish(),
  controlFrequencyMhz: z.number().nullish(),
  controlLcn: z.string().max(32).nullish(),
  affiliatedRadioCount: z.number().int().nullish(),
  observationCount: z.number().int().nullish(),
  firstSeenMs: z.number().int().nullish(),
  lastSeenMs: z.number().int().nullish(),
  status: z.record(z.string(), z.unknown()).nullish(),
  channels: z.array(SiteChannelSchema).max(512).nullish(),
  neighbors: z.array(SiteChannelSchema).max(512).nullish(),
  bands: z.array(SiteChannelSchema).max(64).nullish(),
  // Active patch groups: {patchGroup, version, confirmedAtMs, talkgroups[]}.
  // Loose element shape (like the sibling lists) so a runtime that adds a field
  // doesn't 400 the whole batch; capped because a patch merges a handful of
  // talkgroups, never hundreds.
  patches: z.array(z.record(z.string(), z.unknown())).max(256).nullish(),
  quality: z.record(z.string(), z.unknown()).nullish(),
});

// The vce endpoint returns {sites:[...]}; the agent may forward either that
// wrapper or the bare array. Accept both, nullish-tolerant.
const SiteBodySchema = z.union([
  z.array(SiteSnapshotSchema).max(256),
  z.object({ sites: z.array(SiteSnapshotSchema).max(256) }),
]);

nodeIngestRouter.post('/api/node-ingest/site-snapshots', async (c) => {
  // 1-2. Node credentials + per-node token resolve (role gated), TOFU install
  //      match — identical to /activity.
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
  // Only radio-kind nodes decode P25 sites. Hard-reject the rest.
  if (r.kind !== 'radio') {
    return c.json({ error: 'not a radio node' }, 403);
  }
  const node = { id: r.nodeId };

  // 3. Per-node rate limit. NON-2xx so the agent retries the same set next
  //    tick (a 2xx would advance its cursor and lose the batch).
  if (!siteRateOk(node.id)) {
    log.warn(`site ingest: RATE-LIMITED node=${node.id.slice(0, 8)}`);
    return c.json({ ok: false, error: 'rate limit' }, 429);
  }

  // 4. Size guard — require Content-Length and cap it (closes the
  //    chunked-body bypass, same rationale as the other routes).
  const lenHeader = c.req.header('content-length');
  const len = Number(lenHeader ?? '');
  if (lenHeader === undefined || !Number.isFinite(len)) {
    return c.json({ error: 'length required' }, 411);
  }
  if (len > MAX_SITE_BYTES) {
    return c.json({ error: 'batch too large' }, 413);
  }

  // 5. Parse + validate.
  let parsed: z.infer<typeof SiteBodySchema>;
  try {
    parsed = SiteBodySchema.parse(await c.req.json());
  } catch {
    return c.json({ error: 'bad body' }, 400);
  }
  const sites = Array.isArray(parsed) ? parsed : parsed.sites;

  // 6. Upsert. Fire-safe by contract (never throws); returns rows written.
  const written = await upsertSiteSnapshots(
    node.id,
    sites.map((s) => ({
      systemId: s.systemId ?? null,
      rfss: s.rfss ?? null,
      siteId: s.siteId ?? null,
      guid: s.guid ?? null,
      systemName: s.systemName ?? null,
      wacn: s.wacn ?? null,
      nac: s.nac ?? null,
      lra: s.lra ?? null,
      channelName: s.channelName ?? null,
      controlFrequencyMhz: s.controlFrequencyMhz ?? null,
      controlLcn: s.controlLcn ?? null,
      affiliatedRadioCount: s.affiliatedRadioCount ?? null,
      observationCount: s.observationCount ?? null,
      firstSeenMs: s.firstSeenMs ?? null,
      lastSeenMs: s.lastSeenMs ?? null,
      status: s.status ?? null,
      channels: s.channels ?? [],
      neighbors: s.neighbors ?? [],
      bands: s.bands ?? [],
      patches: s.patches ?? [],
      quality: s.quality ?? null,
    })),
  );
  log.info(
    `site ingest node=${node.id.slice(0, 8)} sites=${sites.length} written=${written}`,
  );
  return c.json({ ok: true, written });
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

// Per-node rate limit: at most 120 messages/min. Real paging is a few/min;
// this bounds a compromised node's flood into Pagermon + the DB.
const pagerRateOk = makeNodeRateLimiter(120, 60_000);

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

  // 5·5. Sanitize the decoded text server-side too (defense in depth). The node
  //      agent already strips POCSAG framing artifacts (control bytes + their
  //      "EOT"/"NUL" text mnemonics), but doing it here as well keeps pages clean
  //      even when a node is still on an older agent that didn't — so it flows to
  //      both the drawer buffer and Pagermon clean, without waiting for updates.
  const rawMessage = parsed.message;
  parsed.message = sanitizePagerText(parsed.message);
  // Invariant check: after cleaning, no bracketed mnemonic or control byte should
  // remain. If one does, the cleaner missed a form — log it escaped (JSON.stringify
  // renders control bytes as \u00XX and shows any brackets) so it's visible.
  // Should stay silent now; grep the backend log for "pager msg not fully cleaned".
  if (/<[A-Za-z]{2,3}>|[\u0000-\u001f\u007f]/.test(parsed.message)) {
    log.warn(
      `pager msg not fully cleaned: raw=${JSON.stringify(rawMessage)} clean=${JSON.stringify(parsed.message)}`,
    );
  }

  // Trace EVERY received message (capcode + source only, never content) so a
  // missing page can be traced to where it dropped: reception (never logged),
  // blocklist, feed-off, or forwarded. Grep the backend log for "pager rx".
  log.info(`pager rx node=${node.id.slice(0, 8)} addr=${parsed.address} src=${parsed.source} freq=${parsed.freqMhz ?? '?'}`);

  // 5·6. Per-event capture (migration 043) — record every reception (even
  //      blocked capcodes / feed-off) BEFORE the gates below, so the Data
  //      tab reflects what nodes actually hear. Fire-safe: recordPagerEvent
  //      swallows its own errors; belt-and-braces catch here too.
  try {
    const tsMs = parsed.timestamp ? new Date(parsed.timestamp).getTime() : NaN;
    await recordPagerEvent({
      nodeId: node.id,
      receivedAt: Number.isFinite(tsMs) ? new Date(tsMs) : new Date(),
      capcode: parsed.address,
      function: safeInt(parsed.function),
      freqMhz: typeof parsed.freqMhz === 'number' ? parsed.freqMhz : null,
      message: parsed.message,
    });
  } catch (err) {
    log.warn({ err, node: node.id.slice(0, 8) }, 'pager relay: event capture failed');
  }

  const view = {
    address: parsed.address,
    message: parsed.message,
    source: parsed.source,
    freqMhz: typeof parsed.freqMhz === 'number' ? parsed.freqMhz : null,
    at: Date.now(),
  };

  // 5a. Blocked capcodes (non-message data transmitters) are NOT forwarded to
  //     Pagermon, but we STILL buffer them tagged as filtered so staff can see
  //     what's being dropped in the drawer (useful when debugging a node).
  if (BLOCKED_CAPCODES.has(parsed.address)) {
    hub.recordPagerMessage(node.id, { ...view, filtered: 'blocked capcode' });
    log.info(`pager relay: BLOCKED capcode ${parsed.address} (buffered as filtered, not forwarded) node=${node.id.slice(0, 8)}`);
    return c.json({ ok: true, dropped: 'blocked capcode' });
  }

  // 5b. Buffer the decoded message for the staff drawer BEFORE the feed gate, so
  //     reception is visible even when the feed is off.
  hub.recordPagerMessage(node.id, view);

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

/** multimon-ng renders unprintable POCSAG control codes as BRACKETED mnemonics —
 *  "<EOT>", "<NUL>", "<STX>", 2-letter "<CR>"/"<LF>", … — never real content.
 *  Same class Pagermon's own reader.js strips (with /<[A-Za-z]{3}>/g); widened to
 *  {2,3} for the 2-letter controls. */
const BRACKET_MNEMONIC = /<[A-Za-z]{2,3}>/g;

/** Strip a decoded POCSAG page's framing/padding artifacts, mirroring both the
 *  node agent's cleanText and Pagermon's reference reader: (1) raw control bytes
 *  (< 0x20, DEL 0x7F) → space, (2) remove bracketed control mnemonics, (3) map
 *  Ä/Ü to [ ] (POCSAG national chars), (4) collapse whitespace + trim. Kept
 *  server-side too so pages stay clean even from a node on an older/other agent.
 *  Unbracketed words (e.g. "CAN") are never touched. */
function sanitizePagerText(s: string): string {
  let out = '';
  for (const ch of s) {
    const c = ch.codePointAt(0) ?? 0;
    out += c < 0x20 || c === 0x7f ? ' ' : ch;
  }
  out = out.replace(BRACKET_MNEMONIC, '').replace(/Ä/g, '[').replace(/Ü/g, ']');
  return out.split(/\s+/).filter(Boolean).join(' ');
}

/** Pagermon's /api/messages expects `datetime` as a UNIX timestamp in SECONDS
 *  (its DB + our read path in sources/pager.ts both treat the column as unix
 *  seconds). Sending an ISO string made Pagermon store an unparseable value and
 *  render "Invalid date". We derive the seconds from the agent's timestamp,
 *  which is stamped at DECODE/RECEIVE time on the node and preserved through the
 *  upload queue — so the page carries when it was heard, not when it reached the
 *  backend. Fall back to now() only if the agent sent nothing parseable. */
function normalisePagerDatetime(ts: string | undefined): string {
  let ms = NaN;
  if (ts) ms = new Date(ts).getTime();
  if (Number.isNaN(ms)) ms = Date.now();
  return String(Math.floor(ms / 1000));
}

// ---------------------------------------------------------------------------
// GET /api/node-ingest/capabilities
// Lets the LOCAL rdio's downstream probe succeed while signalling no
// transcript-forward (transcription is central).
// ---------------------------------------------------------------------------
nodeIngestRouter.get('/api/node-ingest/capabilities', (c) => {
  return c.json({ features: [] });
});
