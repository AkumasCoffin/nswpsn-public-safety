/**
 * GET /api/sa-fire/cfs — SA Country Fire Service incidents
 * GET /api/sa-fire/mfs — SA Metropolitan Fire Service incidents
 *
 * Both serve out of LiveStore in the same property shape as
 * /api/rfs/incidents, so the map renders every agency on the Fires
 * layer through one path.
 *
 * COMPLETE incidents are NOT returned here. SA keeps finished jobs in
 * the feed after they close — a sampled sa_mfs poll had 2 of 7 already
 * COMPLETE — and serving them put finished work on the map as live pins.
 * They are still archived, the way NT's closed records are; `?all=1`
 * returns them too, for debugging.
 */
import { Hono } from 'hono';
import {
  saCfsSnapshot,
  saMfsSnapshot,
  saCfsActiveSnapshot,
  saMfsActiveSnapshot,
} from '../sources/saFire.js';
import { scoped } from '../services/auth/apiKeyScopes.js';

export const saFireRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'safire', type: 'incidents', window: 'live' });

saFireRouter.get('/api/sa-fire/cfs', SCOPE_INCIDENTS, (c) =>
  c.json(c.req.query('all') === '1' ? saCfsSnapshot() : saCfsActiveSnapshot()),
);
saFireRouter.get('/api/sa-fire/mfs', SCOPE_INCIDENTS, (c) =>
  c.json(c.req.query('all') === '1' ? saMfsSnapshot() : saMfsActiveSnapshot()),
);
