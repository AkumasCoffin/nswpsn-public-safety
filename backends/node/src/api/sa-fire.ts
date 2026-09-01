/**
 * GET /api/sa-fire/cfs — SA Country Fire Service incidents
 * GET /api/sa-fire/mfs — SA Metropolitan Fire Service incidents
 *
 * Both serve out of LiveStore in the same property shape as
 * /api/rfs/incidents, so the map renders every agency on the Fires
 * layer through one path.
 */
import { Hono } from 'hono';
import { saCfsSnapshot, saMfsSnapshot } from '../sources/saFire.js';

export const saFireRouter = new Hono();

saFireRouter.get('/api/sa-fire/cfs', (c) => c.json(saCfsSnapshot()));
saFireRouter.get('/api/sa-fire/mfs', (c) => c.json(saMfsSnapshot()));
