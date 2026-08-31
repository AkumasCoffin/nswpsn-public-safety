/**
 * GET /api/qld-fire/incidents — live QFD incidents (ESCAD)
 * GET /api/qld-fire/warnings  — QFD public warnings (OCS)
 *
 * Both serve out of LiveStore in the same property shape as
 * /api/rfs/incidents, so the map renders every agency on the Fires
 * layer through one path.
 */
import { Hono } from 'hono';
import { qldIncidentsSnapshot, qldWarningsSnapshot } from '../sources/qldFire.js';

export const qldFireRouter = new Hono();

qldFireRouter.get('/api/qld-fire/incidents', (c) => c.json(qldIncidentsSnapshot()));
qldFireRouter.get('/api/qld-fire/warnings', (c) => c.json(qldWarningsSnapshot()));
