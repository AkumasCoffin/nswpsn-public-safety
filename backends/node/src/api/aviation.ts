/**
 * GET /api/aviation/cameras            — list of airport weather cameras
 * GET /api/aviation/cameras/:airport   — detail for a single airport's cameras
 *
 * Mirrors python external_api_proxy.py:7604 (list) + 7680 (detail). The
 * list reads from LiveStore (filled by the poller). The detail handler
 * fetches on demand with a 2 min in-process cache, matching python's
 * `@cached(ttl=120)` decorator.
 */
import { Hono } from 'hono';
import {
  aviationSnapshot,
  fetchAviationCameraDetail,
} from '../sources/aviation.js';
import { log } from '../lib/log.js';

export const aviationRouter = new Hono();

aviationRouter.get('/api/aviation/cameras', (c) => c.json(aviationSnapshot()));

aviationRouter.get('/api/aviation/cameras/:airport', async (c) => {
  const airport = c.req.param('airport');
  try {
    const detail = await fetchAviationCameraDetail(airport);
    return c.json(detail);
  } catch (err) {
    log.warn(
      { err: (err as Error).message, airport },
      'aviation camera detail fetch failed',
    );
    return c.json({ airport, cameras: [], count: 0 });
  }
});
