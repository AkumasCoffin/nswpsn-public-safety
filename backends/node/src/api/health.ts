/**
 * GET /api/health — public liveness probe.
 *
 * Bug-for-bug compatible with the Python implementation at
 * external_api_proxy.py:11007-11014. Same response shape so existing
 * monitors and the frontend never see a difference during cutover:
 *
 *   {
 *     "status": "ok",
 *     "mode": "dev" | "production",
 *     "cache_keys": string[],
 *     "active_viewers": number
 *   }
 *
 * `cache_keys` and `active_viewers` are dummied out in W1 — there's no
 * cache or viewer tracker yet on the Node side. Both fields are
 * preserved so JSON shape doesn't drift; once W2/W4 land they'll get
 * real values.
 */
import { Hono } from 'hono';
import { modeLabel } from '../config.js';
import { liveStore } from '../store/live.js';
import { activeViewerCount } from '../services/activityMode.js';

export const healthRouter = new Hono();

healthRouter.get('/api/health', (c) =>
  c.json({
    status: 'ok',
    mode: modeLabel(),
    // Real LiveStore keys — same shape Python returns via cache.keys()
    // (list of source names currently holding a snapshot in memory).
    cache_keys: liveStore.keys(),
    // Real heartbeat-driven viewer count from the activityMode service.
    active_viewers: activeViewerCount(),
  }),
);
