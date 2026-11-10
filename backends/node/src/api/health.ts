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
 * Also served at /api/v1/health (server.ts mirrors it), wrapped in `data`
 * there (api/v1/respond.ts). The unversioned path stays for good: monitors
 * point at it.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { modeLabel } from '../config.js';
import { liveStore } from '../store/live.js';
import { activeViewerCount } from '../services/activityMode.js';
import { ok } from './v1/respond.js';
import { envelope, okJson } from './v1/schemas.js';

export const healthRouter = new Hono();

const Health = z.object({
  status: z.literal('ok'),
  mode: z.enum(['dev', 'production']),
  cache_keys: z.array(z.string()).describe('Live sources holding data right now'),
  active_viewers: z.number().int().describe('Pages open on the site right now'),
}).meta({ id: 'Health' });

healthRouter.get(
  '/api/health',
  describeRoute({
    operationId: 'getHealth',
    tags: ['Service'],
    summary: 'Liveness',
    description: 'Answers whenever the API process is up. For whether the data behind it is flowing, see the service status.',
    responses: okJson(envelope(Health), 'The API is up'),
  }),
  (c) =>
    ok(c, {
      status: 'ok',
      mode: modeLabel(),
      // Real LiveStore keys — same shape Python returns via cache.keys()
      // (list of source names currently holding a snapshot in memory).
      cache_keys: liveStore.keys(),
      // Real heartbeat-driven viewer count from the activityMode service.
      active_viewers: activeViewerCount(),
    } satisfies z.infer<typeof Health>),
);
