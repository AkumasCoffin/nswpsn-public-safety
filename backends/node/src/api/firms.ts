/**
 * NASA FIRMS fire-hotspot endpoint.
 *
 *   GET /api/firms/hotspots — last-24h hotspot detections over NSW as a
 *                              GeoJSON FeatureCollection. Each feature is
 *                              a Polygon representing the satellite pixel
 *                              footprint (scan × track in km).
 */
import { Hono } from 'hono';
import { firmsSnapshot } from '../sources/firms.js';

export const firmsRouter = new Hono();

firmsRouter.get('/api/firms/hotspots', (c) => c.json(firmsSnapshot()));
