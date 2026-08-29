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

export const actAmbulanceRouter = new Hono();

actAmbulanceRouter.get('/api/act-ambulance/incidents', (c) =>
  c.json(actAmbulanceSnapshot()),
);
