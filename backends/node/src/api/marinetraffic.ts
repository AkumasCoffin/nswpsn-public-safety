/**
 * MarineTraffic vessel-position proxy.
 *
 *   GET /api/marinetraffic/vessels
 *     Optional query: z, x, y (slippy-map tile coords, default 2/1/1)
 *
 * MarineTraffic's `getData/get_data_json_4/...` endpoint requires a
 * Cloudflare-validated session — server-side undici fetches are 403-ed.
 * We work around this by running a headless Chromium tab (kept warm in
 * services/marinetrafficBrowser.ts) parked on the marinetraffic AIS map
 * page, and executing the JSON fetch inside that tab via page.evaluate.
 *
 * Caching: 30 seconds in-process keyed by tile coords. MarineTraffic
 * refreshes positions on the order of seconds-to-a-minute and we don't
 * want to thrash the upstream (or our browser).
 *
 * Failure modes:
 *   - 503 if the browser worker isn't ready (chromium not installed,
 *     `MARINETRAFFIC_DISABLED=true`, or initial page load failed).
 *   - 502 if the upstream returns non-2xx or a non-JSON body.
 */
import { Hono } from 'hono';
import { log } from '../lib/log.js';
import { marinetrafficBrowser } from '../services/marinetrafficBrowser.js';

export const marinetrafficRouter = new Hono();

// 60s cache: each upstream hit involves a real browser navigating to the map
// page + the data URL (~5-8s of headless work). Hitting upstream more than
// once a minute also seems to trip MarineTraffic's app router into serving
// 404s for the data URL — likely a heuristic against scraping.
const CACHE_TTL_MS = 60_000;
const cache = new Map<string, { data: unknown; expires: number; staleAfter: number }>();

function buildUpstreamUrl(z: string, x: string, y: string): string {
  // The working endpoint shape is .../z:Z/X:X/Y:Y/station:0 — appending
  // /fleet_id:0/embed:1 makes it 403. Verified live 2026-05.
  return `https://www.marinetraffic.com/getData/get_data_json_4/z:${z}/X:${x}/Y:${y}/station:0`;
}

// Default tile — z:2/X:1/Y:1 is the tile the user verified covers Australian
// waters in MarineTraffic's internal tile scheme (which doesn't follow
// standard slippy-map coordinates exactly).
const DEFAULT_TILES = [
  { z: '2', x: '1', y: '1' },
];

type Tile = { z: string; x: string; y: string };

function parseTilesQuery(raw: string | undefined): Tile[] | null {
  if (!raw) return null;
  const out: Tile[] = [];
  for (const part of raw.split(',')) {
    const m = part.trim().match(/^(\d+):(\d+):(\d+)$/);
    if (!m) continue;
    out.push({ z: m[1] ?? '0', x: m[2] ?? '0', y: m[3] ?? '0' });
  }
  return out.length ? out : null;
}

// Merge several upstream payloads into one. MarineTraffic returns
// `{ type: 1, data: { rows: [...], areaShips: N } }` per tile. We merge
// `data.rows`, dedupe by SHIP_ID, and sum `areaShips`.
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

marinetrafficRouter.get('/api/marinetraffic/vessels', async (c) => {
  // Three modes (priority order):
  //   1. ?tiles=z:x:y,z:x:y,…  — explicit comma-separated list, merged.
  //   2. ?z=&x=&y=             — single tile (legacy single-tile mode).
  //   3. no params              — DEFAULT_TILES (Pacific + Australian seas).
  let tiles: Tile[];
  const tilesQuery = parseTilesQuery(c.req.query('tiles'));
  if (tilesQuery) {
    tiles = tilesQuery;
  } else if (c.req.query('z') || c.req.query('x') || c.req.query('y')) {
    tiles = [{
      z: (c.req.query('z') ?? '2').replace(/\D/g, '') || '2',
      x: (c.req.query('x') ?? '1').replace(/\D/g, '') || '1',
      y: (c.req.query('y') ?? '1').replace(/\D/g, '') || '1',
    }];
  } else {
    tiles = DEFAULT_TILES;
  }
  const cacheKey = tiles.map((t) => `${t.z}/${t.x}/${t.y}`).join('|');

  const now = Date.now();
  const hit = cache.get(cacheKey);
  if (hit && hit.expires > now) {
    return c.json(hit.data);
  }

  if (!marinetrafficBrowser.isReady()) {
    log.warn('marinetraffic: browser not ready');
    if (hit) return c.json(hit.data);
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

  // Fetch tiles serially — the browser worker serialises page access anyway,
  // and parallel calls would just queue. Skip null results so a single bad
  // tile doesn't sink the whole batch.
  const payloads: unknown[] = [];
  let anySucceeded = false;
  for (const t of tiles) {
    const url = buildUpstreamUrl(t.z, t.x, t.y);
    const data = await marinetrafficBrowser.fetchJson(url);
    if (data != null) {
      payloads.push(data);
      anySucceeded = true;
    }
  }

  if (!anySucceeded) {
    if (hit) {
      hit.expires = now + 30_000;
      return c.json(hit.data);
    }
    return c.json(
      { error: 'upstream fetch failed (see api-node logs)', vessels: [] },
      502,
    );
  }

  const merged = tiles.length === 1 ? payloads[0] : mergeUpstream(payloads);
  cache.set(cacheKey, { data: merged, expires: now + CACHE_TTL_MS, staleAfter: now + CACHE_TTL_MS });
  return c.json(merged);
});
