/**
 * MarineTraffic vessel-position proxy.
 *
 *   GET /api/marinetraffic/vessels
 *
 * Always proxies the same fixed upstream URL (`UPSTREAM_URL` below).
 * No tile/zoom query parameters — the front-end can't usefully map a
 * viewport to MarineTraffic's non-standard tile scheme, and rotating
 * tiles per request would just thrash the browser session and Cloudflare.
 *
 * MarineTraffic's `getData/get_data_json_4/...` endpoint requires a
 * Cloudflare-validated browser session. We work around that with a
 * headless Chromium tab kept warm in services/marinetrafficBrowser.ts;
 * fetchJson() there handles the navigation + cookie session.
 *
 * Caching: 60 seconds in-process. Each upstream hit involves a real
 * browser navigating to the map page + the data URL (~5–8 s). Hitting
 * upstream more than once a minute also seems to trip MarineTraffic's
 * app router into serving 404s for the data URL.
 *
 * Failure modes:
 *   - 503 if the browser worker isn't ready (chromium not installed,
 *     `MARINETRAFFIC_DISABLED=true`, or initial page load failed).
 *   - 502 if the upstream returns non-2xx or a non-JSON body.
 *   - On any failure, the last cached payload is served if present so
 *     the front-end layer keeps showing vessels through transient blocks.
 */
import { Hono } from 'hono';
import { log } from '../lib/log.js';
import { marinetrafficBrowser } from '../services/marinetrafficBrowser.js';

export const marinetrafficRouter = new Hono();

// Single fixed upstream tile — verified by the user to return Australian
// waters. Change this constant if MarineTraffic's tile scheme shifts.
const UPSTREAM_URL =
  'https://www.marinetraffic.com/getData/get_data_json_4/z:3/X:0/Y:3/station:0';

const CACHE_TTL_MS = 60_000;
let cache: { data: unknown; expires: number } | null = null;

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

  const data = await marinetrafficBrowser.fetchJson(UPSTREAM_URL);
  if (data == null) {
    if (cache) {
      cache.expires = now + 30_000; // try again in 30s
      return c.json(cache.data);
    }
    return c.json(
      { error: 'upstream fetch failed (see api-node logs)', vessels: [] },
      502,
    );
  }
  cache = { data, expires: now + CACHE_TTL_MS };
  return c.json(data);
});
