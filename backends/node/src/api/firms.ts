/**
 * NASA FIRMS fire-hotspot endpoint.
 *
 *   GET /api/firms/hotspots — last-24h hotspot detections over Australia as a
 *                              GeoJSON FeatureCollection. Each feature is
 *                              a Polygon representing the satellite pixel
 *                              footprint (scan × track in km).
 */
import { Hono } from 'hono';
import { firmsSnapshot } from '../sources/firms.js';
import { scoped } from '../services/auth/apiKeyScopes.js';

export const firmsRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_HAZARDS = scoped({ source: 'firms', type: 'hazards', window: 'live' });

firmsRouter.get('/api/firms/hotspots', SCOPE_HAZARDS, (c) => c.json(firmsSnapshot()));
