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

const CACHE_TTL_MS = 30_000;
const cache = new Map<string, { data: unknown; expires: number }>();

function buildUpstreamUrl(z: string, x: string, y: string): string {
  // The working endpoint shape is .../z:Z/X:X/Y:Y/station:0 — appending
  // /fleet_id:0/embed:1 makes it 403. Verified live 2026-05.
  return `https://www.marinetraffic.com/getData/get_data_json_4/z:${z}/X:${x}/Y:${y}/station:0`;
}

marinetrafficRouter.get('/api/marinetraffic/vessels', async (c) => {
  // Default tile is z:10/X:472/Y:306 — the same tile the live MarineTraffic
  // SPA fetches when the map is centred on Sydney/Newcastle (centerx:151.6
  // centery:-33.2 zoom:10). Using their internal tile scheme so the WAF
  // cookies obtained on the matching map page apply to the data fetch.
  const z = (c.req.query('z') ?? '10').replace(/\D/g, '') || '10';
  const x = (c.req.query('x') ?? '472').replace(/\D/g, '') || '472';
  const y = (c.req.query('y') ?? '306').replace(/\D/g, '') || '306';
  const cacheKey = `${z}/${x}/${y}`;

  const now = Date.now();
  const hit = cache.get(cacheKey);
  if (hit && hit.expires > now) {
    return c.json(hit.data);
  }

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

  const url = buildUpstreamUrl(z, x, y);
  const data = await marinetrafficBrowser.fetchJson(url);
  if (data == null) {
    return c.json(
      { error: 'upstream fetch failed (see api-node logs)', vessels: [] },
      502,
    );
  }
  cache.set(cacheKey, { data, expires: now + CACHE_TTL_MS });
  return c.json(data);
});
