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
 * /raw is what it always was: the channel + item view of the RSS version,
 * fetched per request (Python cached it at the HTTP layer instead).
 *
 * /incidents and /raw are this site's routes, for the map and the Discord
 * bot; neither is on /api/v1/. API keys read the RFS alongside every other
 * fire service at /api/v1/fires (api/fires.ts). /fdr is also served at
 * /api/v1/ (server.ts mirrors it), under the fires feed.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import {
  rfsSnapshot,
  fetchRfsRaw,
  rfsFireDanger,
  type RfsSnapshot,
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

// What a scoped API key needs to read /fdr (services/auth/apiKeyScopes.ts).
// /incidents and /raw declare nothing, so no scoped key reaches them; a
// `read` key still does, through the old feed list.
const SCOPE_FIRES = scoped({ feed: 'fires', window: 'live' });

// The answers' shapes, for describeRoute() below. Only /fdr is on /api/v1/,
// so only FireDanger appears in the API description (api/v1/openapi.ts).
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
  async (c) => {
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
  SCOPE_FIRES,
  describeRoute({
    operationId: 'getRfsFireDanger',
    tags: ['Fires'],
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
