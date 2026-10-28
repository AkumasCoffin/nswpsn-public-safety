/**
 * GET /api/qld/cameras       — QLD Traffic roadside cameras
 * GET /api/qld/flood-cameras — QLD Traffic flood-watch cameras
 *
 * Both serve out of LiveStore (filled by the poller in
 * sources/qldCameras.ts) as GeoJSON FeatureCollections, in the same
 * property shape as /api/traffic/cameras so the map renders every camera
 * layer through one code path.
 *
 * Both are also served at /api/v1/ (server.ts mirrors them), bare there too:
 * GeoJSON is never wrapped (api/v1/respond.ts).
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import {
  qldCamerasSnapshot,
  qldFloodCamerasSnapshot,
  type QldCamerasSnapshot,
} from '../sources/qldCameras.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { geo } from './v1/respond.js';
import {
  assertShape,
  feature,
  featureCollection,
  GATED_ERRORS,
  okJson,
  PointGeometry,
  type SameShape,
} from './v1/schemas.js';

export const qldCamerasRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_CAMERAS = scoped({ source: 'qldtraffic', type: 'cameras', window: 'live' });

// What /api/v1/ documents (api/v1/openapi.ts). One shape for both feeds;
// `source` tells them apart.
const QldCameras = featureCollection(
  feature(
    PointGeometry,
    z.object({
      id: z.string().describe("QLDTraffic's id for the camera, after qld: (roadside) or qldflood: (flood), so the two feeds never share one"),
      title: z
        .string()
        .describe('The camera\'s description as QLDTraffic writes it, e.g. "Mossman - Mossman Daintree Rd at Foxton bridge - North"; "QLD Traffic Camera" when it gives none'),
      view: z.string().describe('The locality the camera is in, as QLDTraffic gives it'),
      direction: z.string().describe('The way the camera looks, as QLDTraffic words it, e.g. NorthEast'),
      region: z.string().describe("QLDTraffic's district for the camera, e.g. Metropolitan or Far North"),
      imageUrl: z.string().describe("The camera's image (a JPEG), as QLDTraffic links it"),
      path: z.string().describe('Always empty: QLDTraffic has no counterpart of the NSW cameras\' path'),
      credit: z.string().describe('Who supplies the image — an agency or council — when QLDTraffic names one; empty otherwise'),
      note: z.string().describe("QLDTraffic's own caveat for the camera, such as how often its image updates; empty when there is none"),
      source: z
        .enum(['qldtraffic_cameras', 'qldtraffic_floodcameras'])
        .describe('qldtraffic_cameras on /qld/cameras, qldtraffic_floodcameras on /qld/flood-cameras'),
    }),
  ),
).meta({ id: 'QldCameras' });
assertShape<SameShape<z.infer<typeof QldCameras>, QldCamerasSnapshot>>();

const COMMON =
  "in the same property shape as /traffic/cameras, each with a link to its image. Cameras without an image are left out. QLDTraffic gives coordinates in GDA2020, passed through as they are (within a metre of WGS84). The list is refreshed every minute, and is empty until the first refresh after a restart; the images are QLDTraffic's and update on their own schedule.";

qldCamerasRouter.get(
  '/api/qld/cameras',
  SCOPE_CAMERAS,
  describeRoute({
    operationId: 'getQldCameras',
    tags: ['Cameras'],
    summary: 'Queensland traffic cameras',
    description: `Roadside traffic cameras across Queensland from QLDTraffic (Queensland Department of Transport and Main Roads), ${COMMON}`,
    responses: { ...okJson(QldCameras, 'A GeoJSON FeatureCollection of cameras'), ...GATED_ERRORS },
  }),
  (c) => geo(c, qldCamerasSnapshot()),
);

qldCamerasRouter.get(
  '/api/qld/flood-cameras',
  SCOPE_CAMERAS,
  describeRoute({
    operationId: 'getQldFloodCameras',
    tags: ['Cameras'],
    summary: 'Queensland flood cameras',
    description: `Cameras watching flood-prone roads and crossings in Queensland from QLDTraffic (Queensland Department of Transport and Main Roads), many of them supplied by councils (\`credit\`), ${COMMON} Some update only every 30 minutes, which \`note\` says where QLDTraffic does.`,
    responses: { ...okJson(QldCameras, 'A GeoJSON FeatureCollection of cameras'), ...GATED_ERRORS },
  }),
  (c) => geo(c, qldFloodCamerasSnapshot()),
);
