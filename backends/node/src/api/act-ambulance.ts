/**
 * GET /api/act-ambulance/incidents — live ACT Ambulance Service responses
 *                                    as a GeoJSON FeatureCollection.
 *
 * Serves out of LiveStore (filled by the poller in
 * sources/actAmbulance.ts). The upstream ESA feed also carries NSW RFS
 * incidents and ACT fire; both are filtered out there, so this route
 * only ever returns ACT ambulance responses that are still running.
 */
import { Hono } from 'hono';
import { actAmbulanceSnapshot } from '../sources/actAmbulance.js';
import { scoped } from '../services/auth/apiKeyScopes.js';

export const actAmbulanceRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'actas', type: 'incidents', window: 'live' });

actAmbulanceRouter.get('/api/act-ambulance/incidents', SCOPE_INCIDENTS, (c) =>
  c.json(actAmbulanceSnapshot()),
);
