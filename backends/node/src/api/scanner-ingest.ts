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
scannerIngestRouter.post('/api/scanner-ingest/api/call-upload', async (c) => {
  const expected = config.SCANNER_INGEST_KEY;
  // Unset key = feature off. A 404 rather than a 403 so an unconfigured
  // deployment gives nothing away about the endpoint existing.
  if (!expected) return c.notFound();

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
