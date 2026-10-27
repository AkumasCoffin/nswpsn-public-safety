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
import { scoped } from '../services/auth/apiKeyScopes.js';

export const qldCamerasRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_CAMERAS = scoped({ source: 'qldtraffic', type: 'cameras', window: 'live' });

qldCamerasRouter.get('/api/qld/cameras', SCOPE_CAMERAS, (c) => c.json(qldCamerasSnapshot()));

qldCamerasRouter.get('/api/qld/flood-cameras', SCOPE_CAMERAS, (c) => c.json(qldFloodCamerasSnapshot()));
