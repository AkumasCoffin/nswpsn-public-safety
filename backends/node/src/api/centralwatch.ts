/**
 * Central Watch routes.
 *
 *   GET /api/centralwatch/cameras       — joined camera + site list
 *   GET /api/centralwatch/sites         — cameras grouped by site
 *   GET /api/centralwatch/image/:id     — cache-only image proxy
 *
 * Mirrors python external_api_proxy.py:8505, 9050, 9096. The cameras +
 * sites endpoints read from backends/data/centralwatch_cameras.json which
 * the Node refresh loop (sources/centralwatch.ts) keeps fresh via the
 * Playwright browser worker. The image proxy serves bytes from the
 * in-memory cache that the batch worker (services/centralwatchImageCache.ts)
 * populates every 30 s.
 *
 * All three are also served at /api/v1/ (server.ts mirrors them). There the
 * camera lists are wrapped in `data`, and each camera's imageUrl names the
 * /api/v1/ image path, so a v1 client never depends on an unversioned one.
 * The old paths keep /api/centralwatch/image/<id>.
 */
import { Hono, type Context } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import {
  getCentralwatchCameras,
  getCentralwatchSites,
  type CentralwatchCamera,
} from '../sources/centralwatch.js';
import {
  getImage,
  hasImage,
  cacheSize,
  STALE_AFTER_MS_EXPORT,
} from '../services/centralwatchImageCache.js';
import { log } from '../lib/log.js';
import { toV1Path } from '../lib/apiVersion.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { isV1, ok } from './v1/respond.js';
import { assertShape, envelope, GATED_ERRORS, okJson, type SameShape } from './v1/schemas.js';

export const centralwatchRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_CAMERAS = scoped({ source: 'centralwatch', type: 'cameras', window: 'live' });

// Cameras that the batch image worker can't reach (offline towers, upstream
// 404s, etc.) get dropped from the public list so the map doesn't show
// markers that perpetually display the placeholder SVG. Gated on the cache
// having warmed up at least once — if it's empty we trust the camera roster
// from the JSON file and return everything, otherwise a cold boot or a
// blocked Vercel WAF would briefly hide every camera.
function filterOnline<T extends { id: string }>(list: T[]): T[] {
  if (cacheSize() === 0) return list;
  return list.filter((cam) => hasImage(cam.id));
}

/**
 * The source builds each imageUrl as /api/centralwatch/image/<id>. On
 * /api/v1/ it names the v1 twin of that path instead; on the old path it is
 * left exactly as built.
 */
const LEGACY_IMAGE_PREFIX = '/api/centralwatch/image/';
function imageUrlsFor<T extends { imageUrl: string }>(c: Context, list: T[]): T[] {
  if (!isV1(c)) return list;
  return list.map((cam) =>
    cam.imageUrl.startsWith(LEGACY_IMAGE_PREFIX) ? { ...cam, imageUrl: toV1Path(cam.imageUrl) } : cam,
  );
}

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const IMAGE_URL =
  "The camera's latest image (see /centralwatch/image/{cameraId}): a path on this API, to be joined to its origin — /api/v1/centralwatch/image/<id>";

const CentralwatchCameraSchema = z.object({
  id: z.string().describe("Central Watch's id for the camera"),
  name: z.string().describe('"Fire Watch Camera" when Central Watch gives none'),
  siteName: z.string().describe('The tower the camera is on; empty when Central Watch gives none'),
  siteId: z.string().describe("Central Watch's id for the tower"),
  latitude: z.number().nullable().describe("The tower's; null when Central Watch gives none"),
  longitude: z.number().nullable().describe("The tower's; null when Central Watch gives none"),
  altitude: z.number().nullable().describe("The tower's, as Central Watch gives it; null when it gives none"),
  state: z.string().describe("The tower's state, as Central Watch gives it (e.g. NSW); empty when it gives none"),
  imageUrl: z.string().describe(IMAGE_URL),
  time: z
    .string()
    .describe("Central Watch's timestamp for the camera, ISO 8601, with a +hh:mm offset rewritten as Z; empty when it gives none"),
  source: z.literal('centralwatch'),
});
assertShape<SameShape<z.infer<typeof CentralwatchCameraSchema>, CentralwatchCamera>>();

const CentralwatchCameras = z.array(CentralwatchCameraSchema).meta({ id: 'CentralwatchCameras' });

const CentralwatchSite = z.object({
  siteId: z.string().describe("Central Watch's id for the tower (the camera's id when it gives none)"),
  siteName: z.string().describe('"Unknown Site" when Central Watch gives none'),
  latitude: z.number().nullable(),
  longitude: z.number().nullable(),
  altitude: z.number().nullable().describe('As Central Watch gives it'),
  state: z.string().describe('As Central Watch gives it (e.g. NSW); empty when it gives none'),
  cameras: z
    .array(
      z.object({
        id: z.string().describe("Central Watch's id for the camera"),
        name: z.string(),
        imageUrl: z.string().describe(IMAGE_URL),
      }),
    )
    .describe('Never empty: a tower with no camera to show is left out'),
});
assertShape<SameShape<z.infer<typeof CentralwatchSite>, Awaited<ReturnType<typeof getCentralwatchSites>>[number]>>();

const CentralwatchSites = z.array(CentralwatchSite).meta({ id: 'CentralwatchSites' });

const ONLINE_ONLY =
  'Once images are being fetched, a camera with no recent image is left out, so the list holds only cameras with a picture to show. The roster is refreshed from Central Watch every six hours.';

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// getCentralwatchCameras/Sites read a local file (through a one-minute
// memory cache) and swallow their own read errors, so the catch below is a
// last resort, not an upstream outage: both paths answer the empty list.
centralwatchRouter.get(
  '/api/centralwatch/cameras',
  SCOPE_CAMERAS,
  describeRoute({
    operationId: 'getCentralwatchCameras',
    tags: ['Cameras'],
    summary: 'Fire-tower cameras',
    description: `Fire-watch cameras from Central Watch, one record per camera, with the position of the tower it is on. ${ONLINE_ONLY} No key is needed.`,
    responses: okJson(envelope(CentralwatchCameras), 'The cameras'),
  }),
  async (c) => {
    try {
      const cameras = await getCentralwatchCameras();
      return ok(c, imageUrlsFor(c, filterOnline(cameras)));
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'centralwatch cameras read failed',
      );
      return ok(c, []);
    }
  },
);

centralwatchRouter.get(
  '/api/centralwatch/sites',
  SCOPE_CAMERAS,
  describeRoute({
    operationId: 'getCentralwatchSites',
    tags: ['Cameras'],
    summary: 'Fire-tower cameras by tower',
    description: `The Central Watch fire-watch cameras grouped by the tower they are on, one record per tower. ${ONLINE_ONLY}`,
    responses: { ...okJson(envelope(CentralwatchSites), 'The towers'), ...GATED_ERRORS },
  }),
  async (c) => {
    try {
      const sites = await getCentralwatchSites();
      // Drop offline cameras within each site, then drop sites with no
      // remaining cameras — a tower with all-offline cameras shouldn't
      // produce a marker.
      const filtered = sites
        .map((s) => ({ ...s, cameras: imageUrlsFor(c, filterOnline(s.cameras)) }))
        .filter((s) => s.cameras.length > 0);
      return ok(c, filtered);
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'centralwatch sites read failed',
      );
      return ok(c, []);
    }
  },
);

const PLACEHOLDER_SVG = `<svg xmlns="http://www.w3.org/2000/svg" width="640" height="480" viewBox="0 0 640 480">
      <rect width="640" height="480" fill="#1e293b"/>
      <text x="320" y="220" text-anchor="middle" fill="#94a3b8" font-family="sans-serif" font-size="18">🔥 Fire Watch Camera</text>
      <text x="320" y="260" text-anchor="middle" fill="#64748b" font-family="sans-serif" font-size="14">Image loading... please wait</text>
    </svg>`;

const xCache = (values: string[], description: string) => ({
  description,
  schema: { type: 'string' as const, enum: values },
});

centralwatchRouter.get(
  '/api/centralwatch/image/:cameraId',
  SCOPE_CAMERAS,
  describeRoute({
    operationId: 'getCentralwatchImage',
    tags: ['Cameras'],
    summary: "A fire-tower camera's latest image",
    description:
      "The latest image from a Central Watch camera — the image itself, not JSON. Images are fetched from Central Watch for every camera about every 30 seconds (less often while it is refusing) and dropped once they are about five minutes old; this answers from what has been fetched and never reads Central Watch itself. A camera with no image to hand (an unknown id, a camera offline, or one not fetched yet) answers a 640×480 SVG placeholder, also with status 200; X-Cache says which you got. Sent with headers that tell browsers not to reuse it without asking again. No key is needed.",
    parameters: [
      {
        in: 'path',
        name: 'cameraId',
        required: true,
        schema: { type: 'string' },
        description: 'The camera, by its id from /centralwatch/cameras',
      },
    ],
    responses: {
      200: {
        description: 'The image (JPEG), or the SVG placeholder when there is none to hand',
        headers: {
          'X-Cache': xCache(
            ['HIT', 'STALE', 'PLACEHOLDER'],
            'HIT: an image fetched in the last two minutes. STALE: an older one. PLACEHOLDER: no image to hand; the body is the SVG placeholder',
          ),
          'X-Cache-Age': {
            description: 'How old the image is, whole seconds. Not sent with the placeholder',
            schema: { type: 'integer' as const },
          },
        },
        // The image goes out with the type Central Watch served it with
        // (image/jpeg when it gave none) — JPEG in practice.
        content: {
          'image/jpeg': { schema: { type: 'string', format: 'binary' } },
          'image/svg+xml': { schema: { type: 'string', description: 'The placeholder' } },
        },
      },
    },
  }),
  (c) => {
    const cameraId = c.req.param('cameraId');
    const cached = getImage(cameraId);

    if (cached) {
      const ageMs = Date.now() - cached.ts;
      const cacheStatus = ageMs <= STALE_AFTER_MS_EXPORT ? 'HIT' : 'STALE';
      // Copy into a plain Uint8Array so the response body type matches
      // Hono's expected `Data` union and we don't expose the underlying
      // (potentially shared) Buffer pool to the response stream.
      const body = new Uint8Array(cached.data);
      return c.body(body, 200, {
        'Content-Type': cached.contentType || 'image/jpeg',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        Pragma: 'no-cache',
        Expires: '0',
        'Access-Control-Allow-Origin': '*',
        'X-Cache': cacheStatus,
        'X-Cache-Age': String(Math.floor(ageMs / 1000)),
      });
    }

    return c.body(PLACEHOLDER_SVG, 200, {
      'Content-Type': 'image/svg+xml',
      'Cache-Control': 'no-cache',
      'Access-Control-Allow-Origin': '*',
      'X-Cache': 'PLACEHOLDER',
    });
  },
);
