/**
 * MarineTraffic vessel-position proxy.
 *
 *   GET /api/marinetraffic/vessels
 *     Optional query: z, x, y (slippy-map tile coords, default 2/1/1)
 *
 * Re-fetches the upstream `getData/get_data_json_4/z:Z/X:X/Y:Y/...` endpoint
 * server-side and returns the JSON to the browser. Direct fetches from the
 * client are blocked by CORS, hence this proxy.
 *
 * Caching: 30 seconds in-process. MarineTraffic refreshes positions on the
 * order of seconds-to-a-minute and we don't want to hammer them.
 *
 * Note: this hits an internal MarineTraffic endpoint, not their licensed API.
 * Consider switching to AISStream.io / AIS Hub / paid MarineTraffic API for
 * production use that respects upstream terms of service.
 */
import { Hono } from 'hono';
import { fetch } from 'undici';
import { log } from '../lib/log.js';

export const marinetrafficRouter = new Hono();

const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
  '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

const CACHE_TTL_MS = 30_000;
const cache = new Map<string, { data: unknown; expires: number }>();

function buildUpstreamUrl(z: string, x: string, y: string): string {
  // Mirrors the URL the user gave: marinetraffic.com/getData/get_data_json_4/z:2/X:1/Y:1/station:0/fleet_id:0/embed:1
  return `https://www.marinetraffic.com/getData/get_data_json_4/z:${z}/X:${x}/Y:${y}/station:0/fleet_id:0/embed:1`;
}

marinetrafficRouter.get('/api/marinetraffic/vessels', async (c) => {
  // Tile params let the front-end (or admin) request a different bbox without
  // a code change. Validate to digits only so we can't be coerced into building
  // a malicious URL by accident.
  const z = (c.req.query('z') ?? '2').replace(/\D/g, '') || '2';
  const x = (c.req.query('x') ?? '1').replace(/\D/g, '') || '1';
  const y = (c.req.query('y') ?? '1').replace(/\D/g, '') || '1';
  const cacheKey = `${z}/${x}/${y}`;

  const now = Date.now();
  const hit = cache.get(cacheKey);
  if (hit && hit.expires > now) {
    return c.json(hit.data);
  }

  const url = buildUpstreamUrl(z, x, y);
  const ac = new AbortController();
  const timeout = setTimeout(() => ac.abort(), 15_000);
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        'User-Agent': UA,
        Accept: 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-AU,en;q=0.9',
        Referer: 'https://www.marinetraffic.com/',
        'X-Requested-With': 'XMLHttpRequest',
      },
      signal: ac.signal,
    });
    if (!res.ok) {
      log.warn(
        { status: res.status, url },
        'marinetraffic upstream returned non-200',
      );
      return c.json(
        { error: `upstream HTTP ${res.status}`, vessels: [] },
        502,
      );
    }
    // MarineTraffic returns JSON but the content-type is sometimes text/html.
    // Parse from text to be defensive about that.
    const body = await res.text();
    let data: unknown;
    try {
      data = JSON.parse(body);
    } catch (err) {
      log.warn(
        { err: (err as Error).message, snippet: body.slice(0, 120) },
        'marinetraffic upstream returned non-JSON',
      );
      return c.json(
        { error: 'upstream non-JSON response', vessels: [] },
        502,
      );
    }
    cache.set(cacheKey, { data, expires: now + CACHE_TTL_MS });
    return c.json(data);
  } catch (err) {
    if ((err as Error).name === 'AbortError') {
      log.warn({ url }, 'marinetraffic upstream timed out');
      return c.json({ error: 'upstream timeout', vessels: [] }, 504);
    }
    log.warn(
      { err: (err as Error).message, url },
      'marinetraffic upstream fetch failed',
    );
    return c.json(
      { error: 'upstream fetch failed', vessels: [] },
      502,
    );
  } finally {
    clearTimeout(timeout);
  }
});
