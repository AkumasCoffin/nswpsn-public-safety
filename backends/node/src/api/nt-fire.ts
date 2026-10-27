/**
 * GET /api/nt-fire/incidents — live NT Fire & Rescue incidents.
 *
 * Serves out of LiveStore (filled by sources/ntFire.ts) in the same
 * property shape as /api/rfs/incidents, so the map renders NSW RFS and
 * NTFRS through one path.
 *
 * Closed incidents are NOT returned here. They are still archived — the
 * logs page shows how each one progressed — but a closed job does not
 * belong on a live map. `?all=1` returns them too, for debugging.
 */
import { Hono } from 'hono';
import { ntFireSnapshot, ntFireActiveSnapshot } from '../sources/ntFire.js';
import { scoped } from '../services/auth/apiKeyScopes.js';

export const ntFireRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'ntfrs', type: 'incidents', window: 'live' });

ntFireRouter.get('/api/nt-fire/incidents', SCOPE_INCIDENTS, (c) =>
  c.json(c.req.query('all') === '1' ? ntFireSnapshot() : ntFireActiveSnapshot()),
);
