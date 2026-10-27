/**
 * GET /api/aviation/cameras            — list of airport weather cameras
 * GET /api/aviation/cameras/:airport   — detail for a single airport's cameras
 *
 * Mirrors python external_api_proxy.py:7604 (list) + 7680 (detail). The
 * list reads from LiveStore (filled by the poller). The detail handler
 * fetches on demand with a 2 min in-process cache, matching python's
 * `@cached(ttl=120)` decorator.
 *
 * Both are also served at /api/v1/ (server.ts mirrors them). When
 * Airservices cannot be read, the detail answers an empty list (200) on the
 * old path, as it always has, and 503 service_unavailable on /api/v1/.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import {
  aviationSnapshot,
  fetchAviationCameraDetail,
  type AviationDetail,
  type AviationSnapshot,
} from '../sources/aviation.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, geo, isV1, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  feature,
  featureCollection,
  GATED_ERRORS,
  okJson,
  PointGeometry,
  UNAVAILABLE_ERROR,
  type SameShape,
} from './v1/schemas.js';

export const aviationRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_CAMERAS = scoped({ source: 'aviation', type: 'cameras', window: 'live' });

// What /api/v1/ documents (api/v1/openapi.ts).
const AviationCameras = featureCollection(
  feature(
    PointGeometry,
    z.object({
      id: z.string().describe("Airservices' id for the airport"),
      title: z.string().describe('The airport\'s name, e.g. "Albury Airport"; "Airport Camera" when Airservices gives none'),
      name: z
        .string()
        .describe('Airservices\' short name for the airport, e.g. "albury-airport" — what /aviation/cameras/{airport} takes'),
      state: z.string().describe('State or territory, abbreviated: NSW, VIC, …'),
      state_full: z.string().describe('State or territory in full'),
      link: z.string().describe("The airport's page on the Airservices weather cameras site"),
      imageUrl: z.string().describe("An image from one of the airport's cameras"),
      source: z.literal('airservices_australia'),
    }),
  ),
).meta({ id: 'AviationCameras' });
assertShape<SameShape<z.infer<typeof AviationCameras>, AviationSnapshot>>();

const AviationCamera = z
  .object({
    airport: z.string().describe('The airport as asked for'),
    title: z.string().describe("The airport's name; the airport as asked for when Airservices gives none"),
    state: z.string().describe('State or territory in full, e.g. "New South Wales"; empty when Airservices gives none'),
    name: z.string().describe("Airservices' short name for the airport; the airport as asked for when it gives none"),
    cameras: z
      .array(
        z.object({
          direction: z.string().describe("Which of Airservices' four camera slots this is: North, East, South or West"),
          angle: z.string().describe('As Airservices gives it, e.g. "070"'),
          imageUrl: z.string().describe('The image'),
          thumbnail: z.string().describe('A smaller copy of the image'),
          timestamp: z
            .string()
            .describe('When the image was taken, as Airservices writes it ("DD/MM/YYYY HH:MM:SS"); UTC in the samples seen'),
        }),
      )
      .describe('Only the slots that have an image'),
    count: z.number().int().describe('Number of cameras'),
  })
  .meta({ id: 'AviationCamera' });
assertShape<SameShape<z.infer<typeof AviationCamera>, AviationDetail>>();

aviationRouter.get(
  '/api/aviation/cameras',
  SCOPE_CAMERAS,
  describeRoute({
    operationId: 'getAviationCameras',
    tags: ['Cameras'],
    summary: 'Airport weather cameras',
    description:
      "Airports with Airservices Australia weather cameras, each with an image from one of its cameras; /aviation/cameras/{airport} has every camera at an airport. Refreshed from Airservices every five minutes.",
    responses: { ...okJson(AviationCameras, 'A GeoJSON FeatureCollection of airports'), ...GATED_ERRORS },
  }),
  (c) => geo(c, aviationSnapshot()),
);

aviationRouter.get(
  '/api/aviation/cameras/:airport',
  SCOPE_CAMERAS,
  describeRoute({
    operationId: 'getAviationCamera',
    tags: ['Cameras'],
    summary: "One airport's weather cameras",
    description:
      "Every weather camera at one airport, with its image, from Airservices Australia. {airport} is the airport's name from /aviation/cameras (e.g. albury-airport); its id works too. An airport Airservices does not know answers an empty list. Read from Airservices when asked, then kept for two minutes.",
    responses: {
      ...okJson(envelope(AviationCamera), "The airport's cameras"),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    const airport = c.req.param('airport');
    try {
      const detail = await fetchAviationCameraDetail(airport);
      return ok(c, detail);
    } catch (err) {
      log.warn(
        { err: (err as Error).message, airport },
        'aviation camera detail fetch failed',
      );
      // The old path has always answered an empty list (200) rather than an error.
      if (!isV1(c)) return ok(c, { airport, cameras: [], count: 0 });
      return fail(c, 503, 'service_unavailable', 'Airservices could not be read just now.');
    }
  },
);
