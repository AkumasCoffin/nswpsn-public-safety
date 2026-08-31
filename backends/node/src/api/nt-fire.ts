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

export const ntFireRouter = new Hono();

ntFireRouter.get('/api/nt-fire/incidents', (c) =>
  c.json(c.req.query('all') === '1' ? ntFireSnapshot() : ntFireActiveSnapshot()),
);
