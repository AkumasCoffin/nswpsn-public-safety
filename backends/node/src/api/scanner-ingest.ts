/**
 * Scanner-feed ingest — a contributor whose receiver is NOT part of the node
 * system.
 *
 * The case this exists for: someone running a desktop scanner into their own
 * rdio-scanner, who cannot run the agent or sdrtrunk. Their rdio already knows
 * how to forward every call to another server — that is what a DOWNSTREAM is —
 * so the whole integration is: they point one downstream at this endpoint with
 * a key we issue. No agent, no install, no software to run.
 *
 * rdio's downstream (server/downstream.go) POSTs multipart to
 * `<downstream.url>/api/call-upload` with the key as a FORM FIELD and no custom
 * headers, which is why this cannot reuse /api/node-ingest/call-upload — that
 * one authenticates on X-Node-Token + X-Node-Install, neither of which rdio can
 * send. Hence the path shape: the operator enters
 *
 *     https://nswpsn.forcequit.xyz/api/scanner-ingest
 *
 * and rdio appends /api/call-upload itself.
 *
 * WHAT WE TAKE, AND WHAT WE IGNORE
 * Their talkgroup ids and radio ids are the same network, so those are the
 * whole point. Their LABELS are not — a third-party rdio names things its own
 * way — so every display name still resolves from OUR global config by
 * talkgroup id, exactly as it does for node traffic.
 *
 * WHAT THIS CANNOT PROVIDE
 * A scanner has no control-channel view: no sites, no RFSS, no decode quality,
 * no grants, no live state. So the feed contributes CALLS only, and its rows
 * carry a null site. That is a property of the source, not a gap to fix.
 */
import { Hono } from 'hono';
import type { Context } from 'hono';
import { createHash } from 'node:crypto';
import type { BodyData } from 'hono/utils/body';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { getPool } from '../db/pool.js';
import { recordScannerCall } from '../services/nodeEvents.js';

export const scannerIngestRouter = new Hono();

/** First value of a multipart field, ignoring files and repeats. */
function formFirstString(form: BodyData, name: string): string | null {
  const v = form[name];
  const first = Array.isArray(v) ? v[0] : v;
  return typeof first === 'string' ? first : null;
}

function safeInt(v: string | null): number | null {
  if (v === null || v.trim() === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

/**
 * The call's own start time, as the sender stamped it.
 *
 * rdio's downstream sends this as RFC3339 — `call.DateTime.UTC().Format(
 * time.RFC3339)`, server/downstream.go:172 — NOT epoch seconds. Reading it with
 * Number() yields NaN, and the old fallback then used `new Date()`, silently
 * replacing the sender's clock with OUR processing time. That discarded the one
 * value the whole alignment depends on: the offset was applied to the wrong
 * base, so the stored stamp drifted by however long the relay happened to take
 * (measured 0.9-7s) instead of the intended fixed 1s, and every call the nodes
 * also heard was stored twice because rdio's duplicate window is only 500ms.
 *
 * Accept both forms — rdio's own upload parser does the same
 * (server/parsers.go:257-265) — and return null rather than a fabricated time
 * so the caller can log a miss instead of burying it.
 */
function parseCallDateTime(raw: string | null): Date | null {
  const text = (raw ?? '').trim();
  if (!text) return null;
  if (/^\d+$/.test(text)) {
    const secs = Number(text);
    return Number.isFinite(secs) && secs > 0 ? new Date(secs * 1000) : null;
  }
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

/**
 * The nodes row every event must reference (node_radio_events.node_id is NOT
 * NULL and foreign-keyed). The contributor never sees or touches it — it is
 * created on the first upload and exists so the feed has somewhere to hang:
 * it gives the Nodes tab an entry showing what they contribute, and `enabled`
 * gives a way to cut the feed without rotating the key.
 *
 * Deterministic id from the key so restarting or re-issuing never orphans the
 * history behind a fresh row.
 */
async function ensureScannerNode(key: string): Promise<{ id: string; enabled: boolean } | null> {
  const pool = await getPool();
  if (!pool) return null;
  const id = 'scanner-' + createHash('sha256').update(key).digest('hex').slice(0, 12);
  const res = await pool.query<{ id: string; enabled: boolean }>(
    `INSERT INTO nodes (id, kind, user_id, install_id, name, enabled)
     VALUES ($1, 'scanner', 'system:scanner-feed', $1, $2, true)
     ON CONFLICT (id) DO UPDATE SET last_seen_at = now()
     RETURNING id, enabled`,
    [id, config.SCANNER_INGEST_NAME ?? 'Scanner feed'],
  );
  const row = res.rows[0];
  return row ? { id: row.id, enabled: row.enabled } : null;
}

// ---------------------------------------------------------------------------
// POST /api/scanner-ingest/api/call-upload
//
// The path carries rdio's own /api/call-upload suffix so the contributor
// configures the BASE url only, exactly like any other downstream target.
// ---------------------------------------------------------------------------
/**
 * Refuse a body before reading it.
 *
 * hono's parseBody/json buffer the WHOLE body into RAM, and a chunked request
 * carries no Content-Length to check afterwards — so the only safe order is to
 * demand the length, check it, and only then read. node-ingest.ts:146 has done
 * this from the start; these routes did not, which left an UNAUTHENTICATED
 * caller able to make the process buffer arbitrary bytes and then be told 401.
 * A handful of concurrent requests is an out-of-memory kill, and this router is
 * in PUBLIC_ENDPOINT_PREFIXES (a third-party rdio downstream cannot send the
 * site API key), so nothing upstream would have stopped it.
 */
function refuseOversizeBody(c: Context, max: number): Response | null {
  const raw = c.req.header('content-length');
  const len = Number(raw ?? '');
  if (raw === undefined || !Number.isFinite(len)) {
    return c.json({ error: 'length required' }, 411);
  }
  if (len > max) return c.json({ error: 'body too large' }, 413);
  return null;
}

/** An audio upload. Matches node-ingest's MAX_CALL_BYTES. */
const MAX_UPLOAD_BYTES = 20 * 1024 * 1024;
/** A transcript is text; a megabyte is already far past any real one. */
const MAX_TRANSCRIPT_BYTES = 1024 * 1024;

scannerIngestRouter.post('/api/scanner-ingest/api/call-upload', async (c) => {
  const expected = config.SCANNER_INGEST_KEY;
  // Unset key = feature off. A 404 rather than a 403 so an unconfigured
  // deployment gives nothing away about the endpoint existing.
  if (!expected) return c.notFound();

  // BEFORE parseBody, and before the key check, because parseBody is what
  // spends the memory.
  const tooBig = refuseOversizeBody(c, MAX_UPLOAD_BYTES);
  if (tooBig) return tooBig;

  let form: BodyData;
  try {
    form = await c.req.parseBody({ all: true });
  } catch {
    return c.json({ error: 'expected multipart form data' }, 400);
  }

  // rdio sends the key as a form field; there is no header to read.
  const key = formFirstString(form, 'key');
  if (!key || key !== expected) {
    log.warn('scanner ingest: rejected upload with bad key');
    return c.json({ error: 'unauthorised' }, 401);
  }

  const node = await ensureScannerNode(key);
  if (!node) return c.json({ error: 'database unavailable' }, 503);
  if (!node.enabled) {
    // Accepted-and-dropped, not an error: rdio retries a failed downstream, and
    // a deliberately disabled feed should not become a retry loop.
    return c.json({ ok: true, fed: false });
  }

  const talkgroup = safeInt(formFirstString(form, 'talkgroup'));
  if (talkgroup === null) return c.json({ error: 'talkgroup required' }, 400);

  // The call's OWN start time — the same value the sender's rdio displays.
  // Using it means a call shows the identical timestamp here and there, and a
  // delayed delivery still lands in the right place in the feed.
  const sentAt = parseCallDateTime(formFirstString(form, 'dateTime'));
  if (!sentAt) {
    // Never silent: falling back to now() is what made the original bug
    // invisible. A feed hitting this is mis-stamping every call it sends.
    log.warn(
      { dateTime: formFirstString(form, 'dateTime') },
      'scanner ingest: unparseable dateTime, falling back to arrival time',
    );
  }
  const startedAt = sentAt ?? new Date();

  const audioField = form['audio'];
  const audioFile = Array.isArray(audioField)
    ? audioField.find((x) => x instanceof File)
    : audioField;
  const audioBytes = audioFile instanceof File ? audioFile.size : 0;

  // The aligned timestamp, in rdio's own unit (whole epoch seconds). The offset
  // is a whole second so this stays integral.
  const alignedSecs = Math.floor((startedAt.getTime() + config.SCANNER_TIME_OFFSET_MS) / 1000);

  // -----------------------------------------------------------------------
  // Diagnostic: dump exactly what the sender put on the wire, before we touch
  // anything. Set SCANNER_INGEST_DIAG=true to enable.
  //
  // This exists because the endpoint had no way to answer "what is actually
  // arriving?" — every earlier theory about duplicate and mis-stamped calls
  // was inferred from downstream effects. Log the raw fields, plus the parse
  // and the exact value we are about to hand rdio, so the transformation is
  // visible end to end in one line.
  // -----------------------------------------------------------------------
  if (config.SCANNER_INGEST_DIAG) {
    const fields: Record<string, string> = {};
    for (const [name, value] of Object.entries(form)) {
      if (name === 'key') continue; // never log the shared secret
      const first = Array.isArray(value) ? value[0] : value;
      fields[name] =
        first instanceof File
          ? `<file ${first.name} ${first.size}B ${first.type || 'no-type'}>`
          : String(first).slice(0, 120);
    }
    log.info(
      {
        fields,
        parsedDateTime: sentAt ? sentAt.toISOString() : null,
        offsetMs: config.SCANNER_TIME_OFFSET_MS,
        relayedDateTime: new Date(alignedSecs * 1000).toISOString(),
        arrivedAt: new Date().toISOString(),
      },
      'scanner ingest: received',
    );
  }

  // ---------------------------------------------------------------------
  // Forward to central rdio, with the aligned dateTime.
  //
  // They point their EXISTING downstream here rather than adding a second one
  // (two would upload every call twice), so this relay is what keeps their
  // audio flowing into central rdio — same model as the node relay.
  //
  // The timestamp rewrite is the point of the whole exercise. rdio collapses
  // the copies of a patched transmission with an EXACT dateTime equality —
  // `where dateTime = ? and system = ? and talkgroup in (...)`, no window at
  // all (Calls.GetPatchDuplicate). A patch is two or more talkgroups carrying
  // the same transmission at the same instant, so if this feed's stamps sit a
  // second off the nodes', copies of one patched transmission never match:
  // rdio stores them as separate calls and the patch's member list is never
  // assembled. Aligning here is what lets the patch collapse fire across both
  // sources.
  // ---------------------------------------------------------------------
  const internalUrl = config.RDIO_INTERNAL_URL;
  const internalKey = config.RDIO_INTERNAL_API_KEY;
  if (internalUrl && internalKey) {
    // SCANNER_TIME_OFFSET_MS=0 means VERBATIM PASSTHROUGH: forward the
    // sender's own dateTime byte-for-byte instead of re-deriving it. Use this
    // to observe the feed's untouched behaviour — with any rewrite in play you
    // cannot tell a sender-side problem from one we introduced.
    const passthrough = config.SCANNER_TIME_OFFSET_MS === 0;
    const fd = new FormData();
    for (const [name, value] of Object.entries(form)) {
      // Their key never reaches central rdio. dateTime is re-set below unless
      // we are passing it through untouched.
      if (name === 'key') continue;
      if (name === 'dateTime' && !passthrough) continue;
      const values = Array.isArray(value) ? value : [value];
      for (const v of values) {
        if (v instanceof File) fd.append(name, v, v.name);
        else fd.append(name, v);
      }
    }
    fd.set('key', internalKey);
    if (!passthrough) fd.set('dateTime', String(alignedSecs));
    try {
      const resp = await fetch(`${internalUrl.replace(/\/$/, '')}/api/call-upload`, {
        method: 'POST',
        body: fd,
      });
      if (!resp.ok) {
        // Surface it: rdio retries a failed downstream, and the retry is
        // idempotent on our side, so a real upstream problem should not be
        // silently swallowed into a 200.
        log.warn({ status: resp.status }, 'scanner ingest: central rdio rejected the relay');
        return c.json({ error: 'relay rejected' }, 502);
      }
    } catch (err) {
      log.warn({ err }, 'scanner ingest: relay to central rdio failed');
      return c.json({ error: 'relay failed' }, 502);
    }
  }

  try {
    await recordScannerCall({
      nodeId: node.id,
      receivedAt: startedAt,
      talkgroup,
      sourceUnit: safeInt(formFirstString(form, 'source')),
      frequency: safeInt(formFirstString(form, 'frequency')),
      // A scanner reports no encryption state; anything it could record audio
      // for was in the clear by definition.
      talkerAlias: formFirstString(form, 'talkerAlias'),
      audioBytes,
    });
  } catch (err) {
    // NOT fatal. The relay above already delivered the audio to central rdio,
    // which is this endpoint's actual job — the event row is our own
    // bookkeeping. Returning 500 here told rdio the upload had failed, so it
    // retried and relayed the SAME call again on every attempt, duplicating it
    // upstream while never succeeding. Report success and carry the failure in
    // the body instead.
    log.error({ err }, 'scanner ingest: failed to record call');
    return c.json({ ok: true, recorded: false });
  }

  return c.json({ ok: true, recorded: true });
});

// A downstream probe target, mirroring the node relay's. rdio checks the base
// URL is reachable before it starts sending.
scannerIngestRouter.get('/api/scanner-ingest/api/call-upload', (c) =>
  c.json({ ok: true, service: 'scanner-ingest' }),
);

// ---------------------------------------------------------------------------
// Transcript forwarding — the second half of the downstream protocol.
//
// The contributor's rdio transcribes his own calls, and his transcripts are
// worth more to us than our own right now: central's transcription plugin is
// rate-limited across every key, so his fill gaps rather than duplicate work.
//
// This is NOT part of the call upload. The transcripts plugin pushes it
// separately, and only after a capability handshake:
//
//   1. rdio GETs <downstream.url>/api/capabilities and looks for the feature
//      named in the plugin's `requireFeature` (plugin_host.go:90-134).
//   2. Only if it is advertised does it POST JSON to
//      <downstream.url>/api/call-transcript.
//
// A downstream that 404s the probe is skipped SILENTLY — "does not support
// transcript-forward" — so before these two routes existed no transcript was
// ever attempted, and nothing appeared in our log to say so.
// ---------------------------------------------------------------------------

/**
 * Capability probe. Deliberately unauthenticated: rdio sends no key on this
 * request (plugin_host.go:107-111), and the answer reveals nothing beyond which
 * server-to-server protocols we speak.
 *
 * Gated on the feature being configured — an unset key means the whole scanner
 * feed is off, and advertising a capability we would then reject is worse than
 * advertising nothing.
 */
scannerIngestRouter.get('/api/scanner-ingest/api/capabilities', (c) => {
  if (!config.SCANNER_INGEST_KEY) return c.notFound();
  return c.json({ features: ['transcript-forward'] });
});

/**
 * POST /api/scanner-ingest/api/call-transcript
 *
 * JSON, not multipart, and the key travels in the BODY — the forwarder builds
 * the payload then sets `payload["key"] = downstream.Apikey`
 * (plugin_host.go:187). There is no header to authenticate on.
 *
 * We store nothing: transcripts live in rdio, which owns that schema, and this
 * backend stays read-only against the rdio database. So this is a pure relay.
 */
scannerIngestRouter.post('/api/scanner-ingest/api/call-transcript', async (c) => {
  const expected = config.SCANNER_INGEST_KEY;
  if (!expected) return c.notFound();

  const tooBig = refuseOversizeBody(c, MAX_TRANSCRIPT_BYTES);
  if (tooBig) return tooBig;

  let body: Record<string, unknown>;
  try {
    body = (await c.req.json()) as Record<string, unknown>;
  } catch {
    return c.json({ error: 'invalid json' }, 400);
  }

  if (typeof body['key'] !== 'string' || body['key'] !== expected) {
    log.warn('scanner ingest: rejected transcript push with bad key');
    return c.json({ error: 'unauthorised' }, 401);
  }

  // Same shape check central applies, so a malformed push fails here with a
  // clear reason instead of being relayed and rejected one hop away.
  const dateTime = typeof body['dateTime'] === 'string' ? body['dateTime'] : '';
  if (!/^\d{4}-\d{2}-\d{2}T/.test(dateTime)) {
    return c.json({ error: 'invalid dateTime' }, 400);
  }

  const transcript = typeof body['transcript'] === 'string' ? body['transcript'] : '';
  if (!transcript.trim()) return c.json({ ok: true, forwarded: false, reason: 'empty' });

  if (config.SCANNER_INGEST_DIAG) {
    log.info(
      {
        system: body['system'],
        talkgroup: body['talkgroup'],
        dateTime,
        chars: transcript.length,
      },
      'scanner ingest: transcript received',
    );
  }

  const internalUrl = config.RDIO_INTERNAL_URL;
  const internalKey = config.RDIO_INTERNAL_API_KEY;
  if (!internalUrl || !internalKey) {
    return c.json({ ok: true, forwarded: false, reason: 'relay not configured' });
  }

  try {
    const resp = await fetch(`${internalUrl.replace(/\/$/, '')}/api/call-transcript`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // dateTime is passed through UNCHANGED. Central matches the transcript to
      // a stored call on (system, talkgroup, dateTime), and the call itself was
      // relayed with the sender's own timestamp, so rewriting it here would
      // orphan every transcript.
      body: JSON.stringify({
        system: body['system'],
        talkgroup: body['talkgroup'],
        dateTime,
        transcript,
        key: internalKey,
      }),
    });
    if (!resp.ok) {
      log.warn({ status: resp.status }, 'scanner ingest: central rdio rejected the transcript');
      return c.json({ error: 'relay rejected' }, 502);
    }
  } catch (err) {
    log.warn({ err }, 'scanner ingest: transcript relay failed');
    return c.json({ error: 'relay failed' }, 502);
  }

  return c.json({ ok: true, forwarded: true });
});
