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

  // The call's OWN start time (rdio's `dateTime`, seconds since epoch) — the
  // same value rdio displays. Using it means a call shows the identical
  // timestamp here and in rdio, and a delayed delivery still lands in the
  // right place in the feed.
  const epoch = Number(formFirstString(form, 'dateTime') ?? '');
  const startedAt = Number.isFinite(epoch) && epoch > 0 ? new Date(epoch * 1000) : new Date();

  const audioField = form['audio'];
  const audioFile = Array.isArray(audioField)
    ? audioField.find((x) => x instanceof File)
    : audioField;
  const audioBytes = audioFile instanceof File ? audioFile.size : 0;

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
    log.error({ err }, 'scanner ingest: failed to record call');
    return c.json({ error: 'failed to record call' }, 500);
  }

  return c.json({ ok: true });
});

// A downstream probe target, mirroring the node relay's. rdio checks the base
// URL is reachable before it starts sending.
scannerIngestRouter.get('/api/scanner-ingest/api/call-upload', (c) =>
  c.json({ ok: true, service: 'scanner-ingest' }),
);
