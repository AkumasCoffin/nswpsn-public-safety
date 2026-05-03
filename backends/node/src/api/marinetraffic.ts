/**
 * MarineTraffic vessel-position proxy.
 *
 *   GET /api/marinetraffic/vessels
 *
 * Fetches a hardcoded set of MarineTraffic tiles via the headless browser
 * worker (services/marinetrafficBrowser.ts), merges them into a single
 * `{ data: { rows, areaShips } }` payload, dedupes by SHIP_ID/MMSI, and
 * caches the result. The tile list below was hand-picked by the user
 * after verifying each URL returns vessels in their browser.
 *
 * Caching strategy:
 *   - A background refresh loop keeps the cache populated so user
 *     requests never wait on a cold fetch. The loop schedules its next
 *     run after each completion, with the gap depending on whether a
 *     user has hit the endpoint recently:
 *       - Active (request within last ACTIVE_WINDOW_MS): 60 s gap.
 *       - Idle: 120 s gap.
 *   - The HTTP handler always serves whatever's in the cache. It only
 *     blocks on a live fetch if there's no cached payload at all
 *     (cold-start, before the first background refresh completes).
 *
 * Failure modes:
 *   - 503 if the browser worker isn't ready (chromium not installed,
 *     `MARINETRAFFIC_DISABLED=true`, or initial page load failed) AND
 *     we have no cached payload to fall back on.
 *   - 502 on cold-start if every tile fetch fails. Partial successes
 *     are merged and returned regardless.
 *   - On any failure with a previous cached payload available, that
 *     payload is served (stale-while-failing).
 */
import { Hono } from 'hono';
import { log } from '../lib/log.js';
import { marinetrafficBrowser } from '../services/marinetrafficBrowser.js';

export const marinetrafficRouter = new Hono();

// User-verified working tiles (paste one in a browser and you get vessel
// JSON back). Together they cover the major shipping basins worldwide
// plus Australian waters at multiple zoom levels.
const UPSTREAM_URLS = [
  // Global mid-ocean basins (zoom 3)
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:2/Y:1/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:1/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:2/Y:2/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:2/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:0/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:1/Y:2/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:1/Y:1/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:3/station:0',
  // Australia: broad
  'https://www.marinetraffic.com/getData/get_data_json_4/z:2/X:1/Y:1/station:0',
  // Australia: medium (zoom 5 — six tiles around AU coast)
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:15/Y:10/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:13/Y:10/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:15/Y:8/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:13/Y:8/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:15/Y:7/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:5/X:12/Y:10/station:0',
  // Australia: tight (zoom 6 — four tiles around NSW/VIC/QLD)
  'https://www.marinetraffic.com/getData/get_data_json_4/z:6/X:30/Y:19/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:6/X:28/Y:19/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:6/X:30/Y:18/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:6/X:28/Y:18/station:0',
  // NSW coast wider (zoom 9 — seven tiles)
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:233/Y:153/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:233/Y:154/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:233/Y:155/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:234/Y:155/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:235/Y:153/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:235/Y:154/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:9/X:235/Y:155/station:0',
  // Sydney metro (zoom 10 — six tiles)
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:469/Y:305/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:470/Y:306/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:471/Y:305/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:472/Y:305/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:472/Y:306/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:472/Y:307/station:0',
  // Sydney harbour / Botany Bay tight (zoom 11 — eight tiles).
  // These need the per-tile landing URL handling in
  // services/marinetrafficBrowser.ts to return data — MT only serves
  // tiles inside the SPA's current viewport at the matching zoom.
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:940/Y:612/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:940/Y:613/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:941/Y:612/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:941/Y:613/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:942/Y:612/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:943/Y:612/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:943/Y:614/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:11/X:943/Y:615/station:0',
];

// Background refresh cadence — gap between the end of one batch and the
// start of the next. Active mode kicks in when a user request has hit
// the endpoint within ACTIVE_WINDOW_MS.
const REFRESH_ACTIVE_MS = 60_000;
const REFRESH_IDLE_MS = 120_000;
const ACTIVE_WINDOW_MS = 5 * 60_000;
// Initial delay before the first background refresh — gives the browser
// worker time to launch chromium and complete its prewarm fetch.
const REFRESH_INITIAL_DELAY_MS = 30_000;

let cache: { data: unknown; fetchedAt: number } | null = null;
let inFlight: Promise<unknown | null> | null = null;
let lastUserHitMs = 0;
let refreshTimer: NodeJS.Timeout | null = null;

// Merge several upstream payloads. MarineTraffic returns
// `{ type: 1, data: { rows: [...], areaShips: N } }` per tile. We merge
// all rows, dedupe by SHIP_ID / SHIPID / MMSI, and sum areaShips counts.
function mergeUpstream(payloads: unknown[]): unknown {
  const seen = new Set<string>();
  const merged: Record<string, unknown>[] = [];
  let total = 0;
  for (const p of payloads) {
    if (!p || typeof p !== 'object') continue;
    const data = (p as { data?: { rows?: unknown[]; areaShips?: number } }).data;
    const rows = Array.isArray(data?.rows) ? data!.rows : [];
    if (typeof data?.areaShips === 'number') total += data.areaShips;
    for (const row of rows as Record<string, unknown>[]) {
      const id = String(row['SHIP_ID'] ?? row['SHIPID'] ?? row['MMSI'] ?? '');
      if (id && seen.has(id)) continue;
      if (id) seen.add(id);
      merged.push(row);
    }
  }
  return { type: 1, data: { rows: merged, areaShips: total || merged.length } };
}

// Fetch every tile in sequence (the browser worker serialises page access
// anyway, so parallel calls would just queue). Returns the merged payload,
// or null if every tile failed.
async function fetchAllTiles(): Promise<unknown | null> {
  const payloads: unknown[] = [];
  let succeeded = 0;
  let failed = 0;
  for (const url of UPSTREAM_URLS) {
    const data = await marinetrafficBrowser.fetchJson(url);
    if (data != null) {
      payloads.push(data);
      succeeded++;
    } else {
      failed++;
    }
  }
  log.info(
    { succeeded, failed, total: UPSTREAM_URLS.length },
    'marinetraffic: tile batch complete',
  );
  if (succeeded === 0) return null;
  return mergeUpstream(payloads);
}

// Background refresh loop. Each tick fetches all tiles, replaces the
// cache, then reschedules itself based on whether a user has hit the
// endpoint recently. Runs forever; errors are swallowed so a single
// failed batch doesn't stop the loop.
async function backgroundRefresh(): Promise<void> {
  refreshTimer = null;
  try {
    if (!marinetrafficBrowser.isReady()) {
      log.debug('marinetraffic: skipping refresh — browser not ready');
    } else if (inFlight) {
      log.debug('marinetraffic: skipping refresh — fetch already in flight');
    } else {
      inFlight = fetchAllTiles().finally(() => {
        inFlight = null;
      });
      const data = await inFlight;
      if (data != null) {
        cache = { data, fetchedAt: Date.now() };
        log.info('marinetraffic: background refresh complete');
      }
    }
  } catch (err) {
    log.warn({ err }, 'marinetraffic: background refresh failed');
  } finally {
    const sinceHit = Date.now() - lastUserHitMs;
    const next = sinceHit < ACTIVE_WINDOW_MS ? REFRESH_ACTIVE_MS : REFRESH_IDLE_MS;
    refreshTimer = setTimeout(backgroundRefresh, next);
    refreshTimer.unref?.();
  }
}

// Kick off the loop once the module loads. The first tick waits a bit
// so the browser worker has time to prewarm; the HTTP handler will
// block on a live fetch in the meantime if anyone hits the endpoint.
refreshTimer = setTimeout(backgroundRefresh, REFRESH_INITIAL_DELAY_MS);
refreshTimer.unref?.();

marinetrafficRouter.get('/api/marinetraffic/vessels', async (c) => {
  lastUserHitMs = Date.now();

  // Always serve whatever's in the cache — the background loop keeps it
  // fresh, and stale data beats blocking the request for ~2 min on a
  // batch refetch.
  if (cache) {
    return c.json(cache.data);
  }

  // Cold start: no cache yet. Block on the first batch.
  if (!marinetrafficBrowser.isReady()) {
    log.warn('marinetraffic: browser not ready');
    return c.json(
      {
        error: 'browser worker not ready',
        message:
          'MarineTraffic proxy requires the headless browser worker. Check that playwright is installed and MARINETRAFFIC_DISABLED is not set.',
        vessels: [],
      },
      503,
    );
  }

  if (!inFlight) {
    inFlight = fetchAllTiles().finally(() => {
      inFlight = null;
    });
  }
  const data = await inFlight;
  if (data == null) {
    return c.json(
      { error: 'every upstream tile failed (see api-node logs)', vessels: [] },
      502,
    );
  }
  cache = { data, fetchedAt: Date.now() };
  return c.json(data);
});
