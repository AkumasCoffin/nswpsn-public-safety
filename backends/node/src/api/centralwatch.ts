/**
 * Central Watch routes.
 *
 *   GET /api/centralwatch/cameras       — joined camera + site list
 *   GET /api/centralwatch/sites         — cameras grouped by site
 *   GET /api/centralwatch/image/:id     — cache-only image proxy
 *
 * Mirrors python external_api_proxy.py:8505, 9050, 9096. The cameras +
 * sites endpoints read from backends/data/centralwatch_cameras.json which
 * the Node refresh loop (sources/centralwatch.ts) keeps fresh via the
 * Playwright browser worker. The image proxy serves bytes from the
 * in-memory cache that the batch worker (services/centralwatchImageCache.ts)
 * populates every 30 s.
 */
import { Hono } from 'hono';
import {
  getCentralwatchCameras,
  getCentralwatchSites,
} from '../sources/centralwatch.js';
import {
  getImage,
  STALE_AFTER_MS_EXPORT,
} from '../services/centralwatchImageCache.js';
import { log } from '../lib/log.js';

export const centralwatchRouter = new Hono();

centralwatchRouter.get('/api/centralwatch/cameras', async (c) => {
  try {
    const cameras = await getCentralwatchCameras();
    return c.json(cameras);
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'centralwatch cameras read failed',
    );
    return c.json([]);
  }
});

centralwatchRouter.get('/api/centralwatch/sites', async (c) => {
  try {
    const sites = await getCentralwatchSites();
    return c.json(sites);
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'centralwatch sites read failed',
    );
    return c.json([]);
  }
});

const PLACEHOLDER_SVG = `<svg xmlns="http://www.w3.org/2000/svg" width="640" height="480" viewBox="0 0 640 480">
      <rect width="640" height="480" fill="#1e293b"/>
      <text x="320" y="220" text-anchor="middle" fill="#94a3b8" font-family="sans-serif" font-size="18">🔥 Fire Watch Camera</text>
      <text x="320" y="260" text-anchor="middle" fill="#64748b" font-family="sans-serif" font-size="14">Image loading... please wait</text>
    </svg>`;

centralwatchRouter.get('/api/centralwatch/image/:cameraId', (c) => {
  const cameraId = c.req.param('cameraId');
  const cached = getImage(cameraId);

  if (cached) {
    const ageMs = Date.now() - cached.ts;
    const cacheStatus = ageMs <= STALE_AFTER_MS_EXPORT ? 'HIT' : 'STALE';
    // Copy into a plain Uint8Array so the response body type matches
    // Hono's expected `Data` union and we don't expose the underlying
    // (potentially shared) Buffer pool to the response stream.
    const body = new Uint8Array(cached.data);
    return c.body(body, 200, {
      'Content-Type': cached.contentType || 'image/jpeg',
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      Pragma: 'no-cache',
      Expires: '0',
      'Access-Control-Allow-Origin': '*',
      'X-Cache': cacheStatus,
      'X-Cache-Age': String(Math.floor(ageMs / 1000)),
    });
  }

  return c.body(PLACEHOLDER_SVG, 200, {
    'Content-Type': 'image/svg+xml',
    'Cache-Control': 'no-cache',
    'Access-Control-Allow-Origin': '*',
    'X-Cache': 'PLACEHOLDER',
  });
});
