/**
 * Central Watch routes.
 *
 *   GET /api/centralwatch/cameras       — joined camera + site list
 *   GET /api/centralwatch/sites         — cameras grouped by site
 *   GET /api/centralwatch/image/:id     — 503 stub (Playwright worker
 *                                          stays on python; Apache pins
 *                                          this route there)
 *
 * Mirrors python external_api_proxy.py:8505, 9050, 9096. The cameras +
 * sites endpoints read from backends/data/centralwatch_cameras.json which
 * the python Playwright worker keeps fresh.
 */
import { Hono } from 'hono';
import {
  getCentralwatchCameras,
  getCentralwatchSites,
} from '../sources/centralwatch.js';
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

// Image proxy stays on python. The Playwright worker that solves the
// Vercel Security Checkpoint to fetch images is python-bound (greenlet
// constraints + chromium dependency), and the python in-memory cache
// is what serves images to clients. Apache routes this path to python.
centralwatchRouter.get('/api/centralwatch/image/:cameraId', (c) =>
  c.json(
    {
      error: 'centralwatch image proxy not yet ported to node backend',
      message:
        'The image cache is populated by a Playwright worker on the python ' +
        'service that solves the Vercel Security Checkpoint. Route ' +
        '/api/centralwatch/image/* to the python service via Apache until ' +
        'the worker is ported (likely never — port shape was never the goal).',
    },
    503,
  ),
);
