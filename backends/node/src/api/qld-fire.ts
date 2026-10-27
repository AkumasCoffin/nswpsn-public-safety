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
import { scoped } from '../services/auth/apiKeyScopes.js';

export const qldFireRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'qfd', type: 'incidents', window: 'live' });
const SCOPE_WARNINGS = scoped({ source: 'qfd', type: 'warnings', window: 'live' });

qldFireRouter.get('/api/qld-fire/incidents', SCOPE_INCIDENTS, (c) => c.json(qldIncidentsSnapshot()));
qldFireRouter.get('/api/qld-fire/warnings', SCOPE_WARNINGS, (c) => c.json(qldWarningsSnapshot()));
