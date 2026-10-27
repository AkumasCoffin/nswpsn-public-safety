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
import { scoped } from '../services/auth/apiKeyScopes.js';

export const aviationRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_CAMERAS = scoped({ source: 'aviation', type: 'cameras', window: 'live' });

aviationRouter.get('/api/aviation/cameras', SCOPE_CAMERAS, (c) => c.json(aviationSnapshot()));

aviationRouter.get('/api/aviation/cameras/:airport', SCOPE_CAMERAS, async (c) => {
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
