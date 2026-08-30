/**
 * GET /api/qld/cameras       — QLD Traffic roadside cameras
 * GET /api/qld/flood-cameras — QLD Traffic flood-watch cameras
 *
 * Both serve out of LiveStore (filled by the poller in
 * sources/qldCameras.ts) as GeoJSON FeatureCollections, in the same
 * property shape as /api/traffic/cameras so the map renders every camera
 * layer through one code path.
 */
import { Hono } from 'hono';
import { qldCamerasSnapshot, qldFloodCamerasSnapshot } from '../sources/qldCameras.js';

export const qldCamerasRouter = new Hono();

qldCamerasRouter.get('/api/qld/cameras', (c) => c.json(qldCamerasSnapshot()));

qldCamerasRouter.get('/api/qld/flood-cameras', (c) => c.json(qldFloodCamerasSnapshot()));
