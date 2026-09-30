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
 * layer to badge. "Right now" is defined by node_site_snapshots.received_at:
 * every radio agent re-upserts its snapshot per (node, system, rfss, site)
 * about once a minute REGARDLESS of traffic, so freshness there is a live
 * lock — unlike call events, which go quiet whenever the network does. The
 * five-minute window is ~3 poll intervals: one missed poll doesn't flicker
 * the badge, a stopped node clears it within minutes.
 *
 * Two predicates are load-bearing (both borrowed from /api/node-data/system):
 * nothing ever prunes node_site_snapshots, so WITHOUT the freshness filter
 * every site a node ever saw would read as monitored forever; and
 * channel_name IS NOT NULL keeps it to sites a node actually has a channel
 * for, excluding the many neighbour sites SDR-Trunk merely learns about from
 * control broadcasts.
 *
 * The response carries site identity only — rfss/site (and the zero-padded
 * "004-083" form the public GRN dataset uses), the decoded site name, NAC,
 * how many nodes hold it and when it was last confirmed. Never node identity
 * or location: which RECEIVERS exist and where stays role-gated
 * (/api/adsb/receivers is the precedent).
 */
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';

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
              (ARRAY_AGG(channel_name ORDER BY received_at DESC))[1] AS name,
              COUNT(DISTINCT node_id) AS nodes,
              MAX(received_at) AS last_seen
         FROM node_site_snapshots
        WHERE received_at >= now() - ($1 || ' seconds')::interval
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
