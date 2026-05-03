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
 * Caching: 120 seconds — fetching all eight tiles takes ~40–60 s of
 * sequential browser work (the worker serialises page access), so we
 * keep the result around long enough for follow-up requests within the
 * cache window to hit instantly.
 *
 * Failure modes:
 *   - 503 if the browser worker isn't ready (chromium not installed,
 *     `MARINETRAFFIC_DISABLED=true`, or initial page load failed).
 *   - 502 if every tile fetch fails. Partial successes are merged and
 *     returned regardless.
 *   - On any failure with a previous cached payload available, that
 *     payload is served (stale-while-failing).
 */
import { Hono } from 'hono';
import { log } from '../lib/log.js';
import { marinetrafficBrowser } from '../services/marinetrafficBrowser.js';

export const marinetrafficRouter = new Hono();

// User-verified working tiles (paste one in a browser and you get vessel
// JSON back). Together they cover the major shipping basins; merging them
// gives the front-end a comprehensive vessel layer without per-viewport
// fetching. The first eight tiles are the user's z:3 set covering mid-
// ocean basins worldwide. The two extras at the end are confirmed
// Australia-region tiles (z:2/X:1/Y:1 returns ~11 vessels around AU
// coast; z:10/X:472/Y:306 is Sydney-specific) — without them the merged
// result was leaving Australian waters empty.
const UPSTREAM_URLS = [
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:2/Y:1/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:1/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:2/Y:2/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:2/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:0/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:1/Y:2/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:1/Y:1/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:3/station:0',
  // Australia-coverage extras
  'https://www.marinetraffic.com/getData/get_data_json_4/z:2/X:1/Y:1/station:0',
  'https://www.marinetraffic.com/getData/get_data_json_4/z:10/X:472/Y:306/station:0',
];

const CACHE_TTL_MS = 120_000;
let cache: { data: unknown; expires: number } | null = null;
let inFlight: Promise<unknown | null> | null = null;

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

marinetrafficRouter.get('/api/marinetraffic/vessels', async (c) => {
  const now = Date.now();
  if (cache && cache.expires > now) {
    return c.json(cache.data);
  }

  if (!marinetrafficBrowser.isReady()) {
    log.warn('marinetraffic: browser not ready');
    if (cache) return c.json(cache.data);
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

  // De-duplicate concurrent refreshes — a batch takes ~40-60 s and
  // multiple front-end refreshes during that window would otherwise
  // each kick off their own batch.
  if (!inFlight) {
    inFlight = fetchAllTiles().finally(() => {
      inFlight = null;
    });
  }
  const data = await inFlight;
  if (data == null) {
    if (cache) {
      cache.expires = now + 30_000;
      return c.json(cache.data);
    }
    return c.json(
      { error: 'every upstream tile failed (see api-node logs)', vessels: [] },
      502,
    );
  }
  cache = { data, expires: now + CACHE_TTL_MS };
  return c.json(data);
});
