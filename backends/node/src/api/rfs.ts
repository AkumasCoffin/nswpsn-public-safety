/**
 * GET /api/rfs/incidents       — parsed GeoJSON FeatureCollection
 * GET /api/rfs/incidents/raw   — the upstream feed unparsed, for clients
 *                                 that want to do their own parsing
 * GET /api/rfs/fdr             — fire danger ratings and fire bans
 *
 * Mirrors the Python routes at external_api_proxy.py:10621 + 10726.
 * The /incidents path serves out of LiveStore (filled by the poller from
 * majorIncidents.json — sources/rfs.ts).
 *
 * /raw differs by path. On /api/v1/ it is majorIncidents.json exactly as
 * the RFS published it at the last poll, from memory. On the old /api/ path
 * it is what it always was: the channel + item view of the RSS version,
 * fetched per request (Python cached it at the HTTP layer instead) — kept
 * until that path is retired.
 *
 * All three are also served at /api/v1/ (server.ts mirrors them).
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import {
  rfsSnapshot,
  rfsFeedSnapshot,
  fetchRfsRaw,
  rfsFireDanger,
  type RfsSnapshot,
  type RfsFeed,
  type RfsFireDanger,
} from '../sources/rfs.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, geo, isV1, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  feature,
  featureCollection,
  GATED_ERRORS,
  incidentProperties,
  okJson,
  PointGeometry,
  UNAVAILABLE_ERROR,
  type SameShape,
} from './v1/schemas.js';

export const rfsRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'rfs', type: 'incidents', window: 'live' });
const SCOPE_WARNINGS = scoped({ source: 'rfs', type: 'warnings', window: 'live' });

// What /api/v1/ documents (api/v1/openapi.ts).
const RfsIncidents = featureCollection(
  feature(
    PointGeometry,
    incidentProperties({
      link: z.string(),
      description: z.string().describe("The feed's HTML-free description block"),
      size: z.string().describe('Fire size as the RFS writes it, e.g. "12 ha"'),
      councilArea: z.string(),
      polygons: z.array(z.string()).describe('Fire-ground polygons as GeoRSS writes them, one "lat lon lat lon …" string each'),
      source: z.literal('rfs'),
    }),
  ),
).meta({ id: 'RfsIncidents' });
assertShape<SameShape<z.infer<typeof RfsIncidents>, RfsSnapshot>>();

const RfsFeedSchema = z
  .object({
    type: z.literal('FeatureCollection'),
    features: z.array(
      z.object({
        type: z.literal('Feature'),
        geometry: z
          .object({
            type: z.string(),
            coordinates: z.unknown().optional(),
            geometries: z.array(z.unknown()).optional(),
          })
          .describe('A Point, or a GeometryCollection of the Point and a GeometryCollection of the fire-ground Polygons'),
        properties: z
          .object({
            title: z.string(),
            link: z.string(),
            category: z.string().describe('The warning level'),
            guid: z.string(),
            guid_isPermaLink: z.string(),
            pubDate: z.string().describe('As the RFS writes it, e.g. "27/10/2026 6:46:00 AM" — UTC in every sample seen'),
            description: z
              .string()
              .describe('HTML: ALERT LEVEL, LOCATION, COUNCIL AREA, STATUS, TYPE, FIRE, SIZE, RESPONSIBLE AGENCY and UPDATED, separated by <br />'),
          })
          .catchall(z.unknown()),
      }),
    ),
  })
  .meta({ id: 'RfsFeed' });
assertShape<SameShape<z.infer<typeof RfsFeedSchema>, RfsFeed>>();

const FireDanger = z
  .object({
    ratings: z.array(
      z.object({
        title: z.string().describe('The district name (the field this route has always had)'),
        description: z.string().describe('The ratings and bans as one line of text, for display'),
        source: z.literal('rfs_fdr'),
        district: z.string().describe('NSW fire weather district'),
        region_number: z.number().int().nullable(),
        councils: z.array(z.string()).describe('Local government areas in the district'),
        danger_today: z
          .string()
          .describe('Fire danger rating as the RFS publishes it: NONE (no rating), MODERATE, HIGH, EXTREME or CATASTROPHIC'),
        danger_tomorrow: z.string(),
        fire_ban_today: z.boolean().describe('A total fire ban is in force'),
        fire_ban_tomorrow: z.boolean(),
      }),
    ),
    fetched_at: z.string().nullable().describe('When the RFS was last read successfully, ISO 8601'),
  })
  .meta({ id: 'RfsFireDanger' });
assertShape<SameShape<z.infer<typeof FireDanger>, RfsFireDanger>>();

rfsRouter.get(
  '/api/rfs/incidents',
  SCOPE_INCIDENTS,
  describeRoute({
    operationId: 'getRfsIncidents',
    tags: ['Incidents'],
    summary: 'NSW RFS incidents',
    description: 'Current NSW Rural Fire Service incidents (major incidents feed).',
    responses: { ...okJson(RfsIncidents, 'A GeoJSON FeatureCollection of incidents'), ...GATED_ERRORS },
  }),
  (c) => geo(c, rfsSnapshot()),
);

rfsRouter.get(
  '/api/rfs/incidents/raw',
  SCOPE_INCIDENTS,
  describeRoute({
    operationId: 'getRfsIncidentsRaw',
    tags: ['Incidents'],
    summary: 'NSW RFS incidents, unparsed',
    description:
      "The RFS major incidents feed (majorIncidents.json) exactly as the RFS published it at the last poll, for clients that parse it themselves. Empty until the first poll after a restart.",
    responses: { ...okJson(RfsFeedSchema, 'The RFS GeoJSON FeatureCollection, unchanged'), ...GATED_ERRORS },
  }),
  async (c) => {
    if (isV1(c)) return geo(c, rfsFeedSnapshot());
    try {
      const raw = await fetchRfsRaw();
      return ok(c, raw);
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'rfs raw fetch failed');
      return ok(c, { channel: {}, items: [], count: 0 });
    }
  },
);

// /api/rfs/fdr — Fire Danger Ratings and total fire bans, one record per
// NSW fire weather district, from fdrToban.xml (sources/rfs.ts). Read at most
// every ten minutes, not per request.
//
// It once read that URL as RSS (python line 10772-10799), which it is not —
// the RSS version is fdrTobanRSS.xml — so for a long time it answered
// `{ ratings: [] }`. The records keep the old `title`/`description`/`source`
// and add the fields themselves.
rfsRouter.get(
  '/api/rfs/fdr',
  SCOPE_WARNINGS,
  describeRoute({
    operationId: 'getRfsFireDanger',
    tags: ['Warnings'],
    summary: 'NSW fire danger ratings and total fire bans',
    description:
      "Today's and tomorrow's fire danger rating and total fire ban for each NSW fire weather district, from the RFS. Refreshed from the RFS at most every ten minutes.",
    responses: {
      ...okJson(envelope(FireDanger), 'One record per district'),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    try {
      return ok(c, await rfsFireDanger());
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'rfs fdr fetch failed');
      // The old path has always answered an empty list (200) rather than an error.
      if (!isV1(c)) return ok(c, { ratings: [] });
      return fail(c, 503, 'service_unavailable', 'The RFS fire danger ratings could not be read just now.');
    }
  },
);
