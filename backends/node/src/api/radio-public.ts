/**
 * The PUBLIC radio surface — deliberately its own tiny router.
 *
 * Every route in node-data.ts is staff-gated and its header says so; rather
 * than make that claim false for one endpoint, anything radio-flavoured that
 * the public map may read lives here, where the whole public surface can be
 * audited in one screen. Key-gated like the rest of /api, no role — the same
 * tier as /api/pager/hits and /api/adsb/aircraft.
 *
 * GET /api/radio/monitored-sites
 *
 * Which P25 sites the fleet is receiving RIGHT NOW, for the map's repeater
 * layer to badge. "Right now" is vce's OWN clock — site_last_seen_ms, when
 * the decoder last actually heard the site — inside a five-minute window
 * (~3 report intervals: one missed poll doesn't flicker the badge, a stopped
 * channel clears within minutes).
 *
 * THREE predicates are load-bearing. received_at freshness and
 * channel_name IS NOT NULL come from /api/node-data/system (nothing ever
 * prunes this table, and neighbour sites SDR-Trunk merely learns about from
 * control broadcasts carry no channel). The site_last_seen_ms window is this
 * endpoint's own, found the hard way: the agent re-reports EVERY site it has
 * observed on every poll, so a site whose channel was STOPPED still got a
 * fresh received_at each minute and badged as monitored (observed live: a
 * stopped channel "confirmed 77s ago", and six-hour-old observations from a
 * second node re-reported fresh forever). site_last_seen_ms freezes the
 * moment decoding stops, so IT carries the liveness; received_at stays as
 * the belt, because vce's clock is the node's clock and a node with a wrong
 * clock must not pin sites to the map on timestamps nobody here minted.
 *
 * The response carries site identity only — rfss/site (and the zero-padded
 * "004-083" form the public GRN dataset uses), the decoded site name, NAC,
 * how many nodes hold it and when it was last confirmed. Never node identity
 * or location: which RECEIVERS exist and where stays role-gated
 * (/api/adsb/receivers is the precedent).
 */
import { Hono } from 'hono';
import { readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { requireRole, isOwner } from '../services/auth/roles.js';

export const radioPublicRouter = new Hono();

/** How stale a snapshot may be and still count as "monitoring". */
const MONITORED_WINDOW_SECONDS = 5 * 60;

interface MonitoredSite {
  rfss: number;
  site: number;
  key: string;
  name: string | null;
  nac: number | null;
  nodes: number;
  lastSeen: string | null;
}

/** GRN publishes site ids as zero-padded "rfss-site" ("004-083"). */
function grnKey(rfss: number, site: number): string {
  return `${String(rfss).padStart(3, '0')}-${String(site).padStart(3, '0')}`;
}

// The agents' upsert cadence is ~60s, so a 30s cache never serves anything
// meaningfully staler than the data itself. One key, one entry — the
// siteNames() pattern from node-data.ts.
const CACHE_TTL_MS = 30_000;
let _cache: { at: number; body: Record<string, unknown> } | null = null;

radioPublicRouter.get('/api/radio/monitored-sites', async (c) => {
  const now = Date.now();
  if (_cache && now - _cache.at < CACHE_TTL_MS) {
    return c.json(_cache.body);
  }
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    // DISTINCT ON inner query so the NAME is the newest node's view of the
    // site rather than an arbitrary aggregate; the outer group folds the
    // per-node rows into one site with a node count.
    const r = await pool.query<{
      rfss: number; site_id: number; nac: number | null;
      name: string | null; nodes: string; last_seen: Date;
    }>(
      `SELECT rfss, site_id,
              MAX(nac) AS nac,
              (ARRAY_AGG(channel_name ORDER BY site_last_seen_ms DESC))[1] AS name,
              COUNT(DISTINCT node_id) AS nodes,
              to_timestamp(MAX(site_last_seen_ms) / 1000.0) AS last_seen
         FROM node_site_snapshots
        WHERE received_at >= now() - ($1 || ' seconds')::interval
          AND site_last_seen_ms IS NOT NULL
          AND to_timestamp(site_last_seen_ms / 1000.0) >= now() - ($1 || ' seconds')::interval
          AND channel_name IS NOT NULL
          AND rfss >= 0 AND site_id >= 0
        GROUP BY rfss, site_id
        ORDER BY rfss, site_id`,
      [String(MONITORED_WINDOW_SECONDS)],
    );
    const sites: MonitoredSite[] = r.rows.map((row) => ({
      rfss: row.rfss,
      site: row.site_id,
      key: grnKey(row.rfss, row.site_id),
      name: row.name,
      nac: row.nac,
      nodes: Number(row.nodes),
      lastSeen: row.last_seen ? row.last_seen.toISOString() : null,
    }));
    const body = {
      generatedAt: new Date(now).toISOString(),
      windowSeconds: MONITORED_WINDOW_SECONDS,
      sites,
    };
    _cache = { at: now, body };
    return c.json(body);
  } catch (err) {
    log.warn({ err }, 'monitored-sites query failed');
    // Serve the stale cache over an error — a 30s-old answer to "what is
    // being monitored" is still true to within its own window.
    if (_cache) return c.json(_cache.body);
    return c.json({ error: 'query failed' }, 500);
  }
});

/** Test seam. */
export function _resetRadioPublicCache(): void {
  _cache = null;
}

// ---------------------------------------------------------------------------
// GRN repeater sites — the dataset behind the map's repeater layer.
//
// Lived as a static JSON file until the owner needed to CORRECT it (missing
// and outdated fields in the community compilation). Now Postgres-backed:
// seeded once from the file when the table is empty, then the DB is
// authoritative. Reads are public (same tier as monitored-sites); writes are
// OWNER ONLY — this is reference data the whole map trusts.
// ---------------------------------------------------------------------------

const HERE = path.dirname(fileURLToPath(import.meta.url));
const GRN_SEED_REL = 'data/nswpsn/NSW GRN Version 1.json';
function grnSeedPath(): string | null {
  for (const c of [
    path.resolve(HERE, '../../../..', GRN_SEED_REL), // dist/api|src/api → repo root
    path.resolve(process.cwd(), '../..', GRN_SEED_REL), // cwd = backends/node
    path.resolve(process.cwd(), GRN_SEED_REL), // cwd = repo root
  ]) {
    try {
      if (statSync(c).isFile()) return c;
    } catch { /* keep looking */ }
  }
  return null;
}

/** One-time import of the JSON seed when grn_sites is empty. Boot-time, like
 *  seedAgencyDataIfEmpty — a failure logs and leaves the endpoint serving an
 *  empty list rather than blocking startup. */
export async function seedGrnSitesIfEmpty(): Promise<void> {
  const pool = await getPool();
  if (!pool) return;
  try {
    const n = await pool.query<{ n: string }>('SELECT COUNT(*)::text AS n FROM grn_sites');
    if (Number(n.rows[0]?.n ?? 0) > 0) return;
    const file = grnSeedPath();
    if (!file) {
      log.warn({ rel: GRN_SEED_REL }, 'grn seed file not found — repeater editing starts empty');
      return;
    }
    const raw = JSON.parse(readFileSync(file, 'utf8')) as unknown[];
    if (!Array.isArray(raw) || raw.length === 0) return;
    // Multi-row inserts in chunks; order preserved so ids follow the file.
    const CHUNK = 200;
    for (let i = 0; i < raw.length; i += CHUNK) {
      const slice = raw.slice(i, i + CHUNK);
      const values = slice.map((_, j) => `($${j + 1}::jsonb)`).join(',');
      await pool.query(
        `INSERT INTO grn_sites (data) VALUES ${values}`,
        slice.map((x) => JSON.stringify(x)),
      );
    }
    log.info({ count: raw.length, file }, 'grn_sites seeded from file');
  } catch (err) {
    log.warn({ err }, 'grn_sites seed failed');
  }
}

let _grnCache: { at: number; body: Record<string, unknown> } | null = null;

radioPublicRouter.get('/api/radio/grn-sites', async (c) => {
  const now = Date.now();
  if (_grnCache && now - _grnCache.at < CACHE_TTL_MS) return c.json(_grnCache.body);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const r = await pool.query<{ id: number; data: Record<string, unknown> }>(
      'SELECT id, data FROM grn_sites ORDER BY id',
    );
    const body = { sites: r.rows.map((row) => ({ id: row.id, ...row.data })) };
    _grnCache = { at: now, body };
    return c.json(body);
  } catch (err) {
    log.warn({ err }, 'grn-sites read failed');
    if (_grnCache) return c.json(_grnCache.body);
    return c.json({ error: 'query failed' }, 500);
  }
});

// The editable surface IS the dataset's own vocabulary — every key the file
// uses, nothing else. A new field means adding it here deliberately, not
// whatever a request happens to carry.
const GrnSitePatchSchema = z.object({
  'NAME': z.string().trim().min(1).max(200).optional(),
  'Latitude': z.number().min(-90).max(90).optional(),
  'Longitude': z.number().min(-180).max(180).optional(),
  'SITE_ID': z.string().trim().max(40).optional(),
  'GRN Site ID #': z.string().trim().max(120).optional(),
  'Previous Site ID #': z.string().trim().max(120).optional(),
  'SYSTEM NAME': z.string().trim().max(120).optional(),
  'System #': z.string().trim().max(60).optional(),
  'Zone Assignment': z.string().trim().max(60).optional(),
  'NAC Code': z.string().trim().max(20).optional(),
  'Control Channel': z.string().trim().max(60).optional(),
  'Alt Control Channel': z.string().trim().max(60).optional(),
  'POSTCODE': z.string().trim().max(10).optional(),
  'FAV NAME & QK#': z.string().trim().max(120).optional(),
  'Notes': z.string().trim().max(500).optional(),
  'Frequencies': z.array(z.string().trim().min(1).max(40)).max(64).optional(),
}).strict();

radioPublicRouter.patch('/api/radio/grn-sites/:id', requireRole(isOwner), async (c) => {
  const idRaw = c.req.param('id');
  if (!/^\d+$/.test(idRaw)) return c.json({ error: 'invalid id' }, 400);
  const parsed = GrnSitePatchSchema.safeParse(await c.req.json().catch(() => null));
  if (!parsed.success) {
    return c.json({ error: 'invalid fields', detail: parsed.error.issues.map((i) => i.path.join('.')).slice(0, 5) }, 400);
  }
  if (Object.keys(parsed.data).length === 0) return c.json({ error: 'nothing to update' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    // Merge into the stored object; an empty string clears a field (the
    // dataset's own convention is simply absent keys, so store that).
    const patch: Record<string, unknown> = {};
    const clears: string[] = [];
    for (const [k, v] of Object.entries(parsed.data)) {
      if (typeof v === 'string' && v === '') clears.push(k);
      else patch[k] = v;
    }
    // Every edit stamps the dataset's own "Last Update" field (DD/MM/YYYY,
    // Sydney) — the card renders it, and it is how readers judge staleness.
    const sydney = new Intl.DateTimeFormat('en-AU', {
      timeZone: 'Australia/Sydney', day: '2-digit', month: '2-digit', year: 'numeric',
    }).format(new Date());
    patch['Last Update'] = sydney;
    const r = await pool.query<{ id: number; data: Record<string, unknown> }>(
      `UPDATE grn_sites
          SET data = (data || $2::jsonb) - $3::text[],
              updated_at = now(),
              updated_by = $4
        WHERE id = $1
        RETURNING id, data`,
      [Number(idRaw), JSON.stringify(patch), clears, c.get('userId') ?? null],
    );
    if (r.rowCount === 0) return c.json({ error: 'site not found' }, 404);
    _grnCache = null; // the next GET serves the edit
    log.info({ id: idRaw, by: c.get('userId'), fields: Object.keys(parsed.data) }, 'grn site edited');
    return c.json({ ok: true, site: { id: r.rows[0]!.id, ...r.rows[0]!.data } });
  } catch (err) {
    log.error({ err, id: idRaw }, 'grn site edit failed');
    return c.json({ error: 'update failed' }, 500);
  }
});
