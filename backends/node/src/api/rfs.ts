/**
 * GET /api/rfs/incidents       — parsed GeoJSON FeatureCollection
 * GET /api/rfs/incidents/raw   — pass-through of the upstream XML feed
 *                                 converted to JSON, for clients that
 *                                 want to do their own parsing
 *
 * Mirrors the Python routes at external_api_proxy.py:10621 + 10726.
 * The /incidents path serves out of LiveStore (filled by the poller).
 * The /raw path hits upstream on every request — same as Python which
 * caches it at HTTP layer rather than via the prewarm pipeline.
 */
import { Hono } from 'hono';
import { rfsSnapshot, fetchRfsRaw } from '../sources/rfs.js';
import { log } from '../lib/log.js';

export const rfsRouter = new Hono();

rfsRouter.get('/api/rfs/incidents', (c) => c.json(rfsSnapshot()));

rfsRouter.get('/api/rfs/incidents/raw', async (c) => {
  try {
    const raw = await fetchRfsRaw();
    return c.json(raw);
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'rfs raw fetch failed');
    return c.json({ channel: {}, items: [], count: 0 });
  }
});
