/**
 * Beach endpoints.
 *
 *   GET /api/beachwatch              — NSW DCCEEW water-quality GeoJSON
 *   GET /api/beachsafe                — BeachSafe (Surf Life Saving Australia) summary
 *   GET /api/beachsafe/details        — pre-fetched detail dict by slug
 *   GET /api/beachsafe/beach/:slug    — single beach detail (live fetch)
 *
 * Mirrors python's routes at external_api_proxy.py:5644 / 5670 / 5743 /
 * 5813. The summary + details routes serve from LiveStore. The
 * `/beach/:slug` route hits upstream live, identical to the python
 * implementation which caches it at the HTTP layer.
 *
 * All but /beachsafe/details (an empty store) are also served at /api/v1/
 * (server.ts mirrors them). When
 * BeachSafe cannot be read, /beach/:slug answers `{}` (200) on the old path,
 * as it always has; on /api/v1/ it answers 404 not_found for a beach
 * BeachSafe does not know and 503 service_unavailable otherwise.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute, resolver } from 'hono-openapi';
import {
  beachsafeDetailsSnapshot,
  beachsafeSnapshot,
  beachwatchSnapshot,
  type BeachsafeBeach,
} from '../sources/beach.js';
import { fetchJson, HttpError } from '../sources/shared/http.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, geo, isV1, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  ErrorBody,
  GATED_ERRORS,
  LonLat,
  okJson,
  UNAVAILABLE_ERROR,
  type SameShape,
} from './v1/schemas.js';

export const beachRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_HAZARDS = scoped({ source: 'beach', type: 'hazards', window: 'live' });

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const errorJson = (description: string) => ({
  description,
  content: { 'application/json': { schema: resolver(ErrorBody) } },
});

// Beachwatch's GeoJSON is passed on as published, so this names what its
// sites were seen to carry and lets anything else through.
const BeachwatchSites = z
  .object({
    type: z.literal('FeatureCollection'),
    features: z.array(
      z
        .object({
          type: z.literal('Feature'),
          geometry: z.object({ type: z.literal('Point'), coordinates: LonLat }).catchall(z.unknown()),
          properties: z
            .object({
              id: z.string().optional().describe("Beachwatch's id for the site"),
              siteName: z.string().optional(),
              latestResult: z
                .string()
                .optional()
                .describe('The latest water-quality result in words, as Beachwatch gives it (Good, Fair, Poor or Bad in the samples seen)'),
              latestResultRating: z
                .number()
                .optional()
                .describe('The same result as a number, as Beachwatch gives it (4 for Good down to 1 for Bad in the samples seen)'),
              latestResultObservationDate: z
                .string()
                .optional()
                .describe('When the latest result was observed, ISO 8601 with an offset'),
              pollutionForecast: z
                .string()
                .optional()
                .describe('As Beachwatch words it: Unlikely, Possible, Likely or "Forecast not available" in the samples seen'),
              pollutionForecastTimeStamp: z.string().optional().describe('When the forecast was issued, ISO 8601 with an offset'),
            })
            .catchall(z.unknown()),
        })
        .catchall(z.unknown()),
    ),
  })
  .catchall(z.unknown())
  .meta({ id: 'BeachwatchSites' });

const BeachsafeBeachSchema = z.object({
  id: z.unknown().describe("BeachSafe's id for the beach (a number in the samples seen)"),
  name: z.string().describe('"Unknown Beach" when BeachSafe gives none'),
  lat: z.number().nullable(),
  lng: z.number().nullable().describe('Longitude'),
  url: z.string().describe("The beach's path on beachsafe.org.au, e.g. /nsw/waverley/bondi-beach"),
  slug: z.string().describe('The last part of url — what /beachsafe/beach/{slug} takes'),
  patrolled: z.boolean().describe('true when status reads "Patrolled", in any case'),
  status: z.string().describe('As BeachSafe words it (Open or Unpatrolled in the samples seen); "Unknown" when it gives none'),
  hasToilet: z.boolean(),
  hasParking: z.boolean(),
  dogsAllowed: z.boolean(),
  image: z.string().describe('A photo of the beach, as a URL; empty when there is none'),
  weather: z.record(z.string(), z.unknown()).describe("BeachSafe's weather fields for the beach, as published"),
  hazards: z.array(z.unknown()).describe("BeachSafe's hazards for the beach, as published; empty when there are none"),
  isPatrolledToday: z.boolean(),
  patrolStart: z.string().describe("Today's patrol start, as BeachSafe gives it (e.g. 09:00); empty when there is none"),
  patrolEnd: z.string().describe("Today's patrol end, as BeachSafe gives it; empty when there is none"),
  patrol: z.union([z.number(), z.string()]).describe('As BeachSafe gives it; 0 when it gives none'),
});
assertShape<SameShape<z.infer<typeof BeachsafeBeachSchema>, BeachsafeBeach>>();

const BeachsafeBeaches = z.array(BeachsafeBeachSchema).meta({ id: 'BeachsafeBeaches' });

const BeachsafeBeachDetail = z
  .object({
    weather: z.record(z.string(), z.unknown()).describe("BeachSafe's weather fields for the beach, as published"),
    currentTide: z.unknown().describe("BeachSafe's current tide record, as published; null when there is none"),
    currentUV: z.unknown().describe("BeachSafe's current UV record, as published; null when there is none"),
    latestAttendance: z
      .object({
        date: z.string().describe("The record's key in BeachSafe's attendance records"),
        entries: z.unknown().describe('The record, as published'),
      })
      .nullable()
      .describe("The last of BeachSafe's attendance records for the beach; null when there are none"),
    todays_marine_warnings: z.array(z.unknown()).describe('As published; empty when there are none'),
    patrol: z.union([z.number(), z.string()]).describe('As BeachSafe gives it; 0 when it gives none'),
    patrolStart: z.string().describe("Today's patrol start, as BeachSafe gives it; empty when there is none"),
    patrolEnd: z.string().describe("Today's patrol end, as BeachSafe gives it; empty when there is none"),
    isPatrolledToday: z.boolean(),
    status: z.string().describe('As BeachSafe words it; "Unknown" when it gives none'),
    hazard: z.union([z.number(), z.string()]).describe("BeachSafe's hazard rating for the beach, as published; 0 when it gives none"),
  })
  .meta({ id: 'BeachsafeBeachDetail' });

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

beachRouter.get(
  '/api/beachwatch',
  SCOPE_HAZARDS,
  describeRoute({
    operationId: 'getBeachwatch',
    tags: ['Hazards'],
    summary: 'NSW beach water quality',
    description:
      "Water quality at NSW swimming sites from Beachwatch, the NSW Government's water-quality programme: each site's latest sampled result and its pollution forecast, passed on as Beachwatch publishes them. Refreshed from Beachwatch every ten minutes; empty until the first refresh after a restart.",
    responses: { ...okJson(BeachwatchSites, 'A GeoJSON FeatureCollection of sites, as Beachwatch publishes it'), ...GATED_ERRORS },
  }),
  (c) => geo(c, beachwatchSnapshot()),
);

beachRouter.get(
  '/api/beachsafe',
  SCOPE_HAZARDS,
  describeRoute({
    operationId: 'getBeachsafe',
    tags: ['Hazards'],
    summary: 'NSW beaches: patrols and hazards',
    description:
      "NSW beaches from BeachSafe, Surf Life Saving Australia's beach information service: patrol status and today's patrol hours, hazards, facilities and weather, as BeachSafe's beach map lists them for NSW. /beachsafe/beach/{slug} has more on one beach. Refreshed from BeachSafe every ten minutes; empty until the first refresh after a restart.",
    responses: { ...okJson(envelope(BeachsafeBeaches), 'The beaches'), ...GATED_ERRORS },
  }),
  (c) => ok(c, beachsafeSnapshot()),
);

// Not on /api/v1/: nothing fills this store, so it is always `{}`. It goes
// when the old paths are retired; /beachsafe/beach/:slug is the detail.
beachRouter.get('/api/beachsafe/details', SCOPE_HAZARDS, (c) =>
  c.json(beachsafeDetailsSnapshot()),
);

const BAD_SLUG = 'slug: must be 1 to 100 characters';

beachRouter.get(
  '/api/beachsafe/beach/:slug',
  SCOPE_HAZARDS,
  describeRoute({
    operationId: 'getBeachsafeBeach',
    tags: ['Hazards'],
    summary: 'One beach from BeachSafe',
    description:
      "Today's conditions at one beach from BeachSafe, Surf Life Saving Australia's beach information service: weather, tide, UV, patrol hours, marine warnings and its hazard rating. Read from BeachSafe on each request.",
    parameters: [
      {
        in: 'path',
        name: 'slug',
        required: true,
        schema: { type: 'string' },
        description:
          "The beach's slug, as /beachsafe lists it (e.g. bondi-beach); upper case and spaces are taken as lower case and hyphens. At most 100 characters.",
      },
    ],
    responses: {
      ...okJson(envelope(BeachsafeBeachDetail), 'The beach'),
      ...GATED_ERRORS,
      400: errorJson('The slug is empty or longer than 100 characters (validation_failed)'),
      404: errorJson('BeachSafe has no beach by that slug (not_found)'),
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    const rawSlug = c.req.param('slug') ?? '';
    const slug = rawSlug.trim().toLowerCase().replace(/\s+/g, '-');
    if (!slug || slug.length > 100) {
      return fail(c, 400, 'validation_failed', BAD_SLUG, { error: 'Invalid slug' }, {
        issues: [{ path: 'slug', message: 'must be 1 to 100 characters' }],
      });
    }

    try {
      const data = await fetchJson<unknown>(
        `https://beachsafe.org.au/api/v4/beach/${encodeURIComponent(slug)}`,
        {
          timeoutMs: 10_000,
          headers: {
            'User-Agent':
              'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            Accept: 'application/json',
            Referer: 'https://beachsafe.org.au/',
          },
        },
      );
      if (isV1(c) && !hasBeach(data)) {
        log.warn({ slug }, 'beachsafe detail fetch: no beach in the answer');
        return fail(c, 503, 'service_unavailable', 'BeachSafe did not answer with the beach just now.');
      }
      return ok(c, shapeBeachDetail(data));
    } catch (err) {
      if (err instanceof HttpError) {
        log.warn({ slug, status: err.status }, 'beachsafe detail fetch failed');
      } else {
        log.warn({ slug, err: (err as Error).message }, 'beachsafe detail error');
      }
      // The old path has always answered an empty object (200) rather than an error.
      if (!isV1(c)) return ok(c, {});
      // BeachSafe answers 404 for a beach it does not know.
      if (err instanceof HttpError && err.status === 404) {
        return fail(c, 404, 'not_found', 'BeachSafe has no beach by that slug.');
      }
      return fail(c, 503, 'service_unavailable', 'BeachSafe could not be read just now.');
    }
  },
);

interface BeachDetailResponse {
  weather: Record<string, unknown>;
  currentTide: unknown;
  currentUV: unknown;
  latestAttendance: { date: string; entries: unknown } | null;
  todays_marine_warnings: unknown[];
  patrol: number | string;
  patrolStart: string;
  patrolEnd: string;
  isPatrolledToday: boolean;
  status: string;
  hazard: number | string;
}
assertShape<SameShape<z.infer<typeof BeachsafeBeachDetail>, BeachDetailResponse>>();

/** Did BeachSafe answer with a beach — the object shapeBeachDetail reads? */
function hasBeach(data: unknown): boolean {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return false;
  const beach = (data as Record<string, unknown>)['beach'];
  return !!beach && typeof beach === 'object' && !Array.isArray(beach);
}

function shapeBeachDetail(data: unknown): BeachDetailResponse {
  const root = data && typeof data === 'object' && !Array.isArray(data)
    ? (data as Record<string, unknown>)
    : {};
  const beach = root['beach'] && typeof root['beach'] === 'object' && !Array.isArray(root['beach'])
    ? (root['beach'] as Record<string, unknown>)
    : {};

  const attendances =
    typeof beach['attendances'] === 'object' && beach['attendances'] !== null
      ? (beach['attendances'] as Record<string, unknown>)
      : {};
  const attKeys = Object.keys(attendances);
  let latestAttendance: { date: string; entries: unknown } | null = null;
  if (attKeys.length > 0) {
    const lastKey = attKeys[attKeys.length - 1];
    if (lastKey) {
      const entries = attendances[lastKey];
      if (entries) latestAttendance = { date: lastKey, entries };
    }
  }

  const patrolToday =
    typeof beach['is_patrolled_today'] === 'object' &&
    beach['is_patrolled_today'] !== null &&
    !Array.isArray(beach['is_patrolled_today'])
      ? (beach['is_patrolled_today'] as Record<string, unknown>)
      : {};

  const patrolVal = beach['patrol'];
  const hazardVal = beach['hazard'];
  return {
    weather:
      typeof beach['weather'] === 'object' && beach['weather'] !== null
        ? (beach['weather'] as Record<string, unknown>)
        : {},
    currentTide: beach['currentTide'] ?? null,
    currentUV: beach['currentUV'] ?? null,
    latestAttendance,
    todays_marine_warnings: Array.isArray(beach['todays_marine_warnings'])
      ? (beach['todays_marine_warnings'] as unknown[])
      : [],
    patrol: typeof patrolVal === 'number' || typeof patrolVal === 'string' ? patrolVal : 0,
    patrolStart: stringField(patrolToday['start']),
    patrolEnd: stringField(patrolToday['end']),
    isPatrolledToday: Boolean(patrolToday['flag']),
    status: stringField(beach['status']) || 'Unknown',
    hazard: typeof hazardVal === 'number' || typeof hazardVal === 'string' ? hazardVal : 0,
  };
}

function stringField(v: unknown): string {
  return typeof v === 'string' ? v : '';
}
