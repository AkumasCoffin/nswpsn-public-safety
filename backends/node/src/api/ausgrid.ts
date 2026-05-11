/**
 * Ausgrid outage routes.
 *
 * Reads from LiveStore — the poller (src/sources/ausgrid.ts) keeps two
 * keys fresh:
 *   - ausgrid         → {Markers, Polygons} payload
 *   - ausgrid_stats   → aggregate counter dict
 *
 * Response shape mirrors Python (external_api_proxy.py:5127-5168):
 *   /api/ausgrid/outages → `{Markers, Polygons}`. On error, Python
 *                          returns `{error, Markers: [], Polygons: []}`
 *                          with HTTP 200; we mirror that by returning
 *                          the empty payload on missing LiveStore data.
 *   /api/ausgrid/stats   → upstream counter object verbatim.
 */
import { Hono } from 'hono';
import { liveStore } from '../store/live.js';
import type { AusgridOutagesPayload } from '../sources/ausgrid.js';

export const ausgridRouter = new Hono();

ausgridRouter.get('/api/ausgrid/outages', (c) => {
  const data = liveStore.getData<AusgridOutagesPayload>('ausgrid');
  if (data && (data.Markers || data.Polygons)) {
    return c.json(data);
  }
  // Empty fallback — Python uses the same empty shape when the live
  // fetch errors so the frontend's "render markers" code path is happy.
  return c.json({ Markers: [], Polygons: [] });
});

ausgridRouter.get('/api/ausgrid/stats', (c) => {
  const data = liveStore.getData<unknown>('ausgrid_stats');
  // Python's ausgrid_stats handler returns the upstream payload as-is
  // (whatever shape it happens to have); on missing cache it returns
  // {error: ...}. We just return null when missing — frontend is
  // already null-tolerant for stats since the value is cosmetic.
  return c.json(data ?? null);
});
