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
 * /incidents and /incidents/raw are also served at /api/v1/ (server.ts
 * mirrors them). /fdr is not: see the note on it below.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { rfsSnapshot, rfsFeedSnapshot, fetchRfsRaw, type RfsSnapshot, type RfsFeed } from '../sources/rfs.js';
import { fetchText } from '../sources/shared/http.js';
import { parseXml, asArray, textOf } from '../sources/shared/xml.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { geo, isV1, ok } from './v1/respond.js';
import {
  assertShape,
  feature,
  featureCollection,
  GATED_ERRORS,
  incidentProperties,
  okJson,
  PointGeometry,
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

// /api/rfs/fdr — Fire Danger Ratings. Mirrors python line 10772-10799.
// Fetched per request because the cadence is slow (daily) and the
// payload is small; not worth a poller slot. Description's HTML is
// stripped to match python's re.sub(r'<[^>]+>', '', ...) behaviour.
//
// BROKEN UPSTREAM, AND NOT ON /api/v1/ FOR THAT REASON. fdrToban.xml is no
// longer RSS: it is <FireDangerMap><District> records (Name, RegionNumber,
// Councils, DangerLevelToday/Tomorrow, FireBanToday/Tomorrow), so the
// channel/item walk below finds nothing and this answers `{ ratings: [] }`
// every time. Seen 2026-10-28. It goes on v1 once it reads the new format.
rfsRouter.get(
  '/api/rfs/fdr',
  SCOPE_WARNINGS,
  async (c) => {
    try {
      const xml = await fetchText('https://www.rfs.nsw.gov.au/feeds/fdrToban.xml', {
        timeoutMs: 15_000,
        headers: { 'User-Agent': 'Mozilla/5.0' },
      });
      const parsed = parseXml(xml);
      const rss =
        (parsed['rss'] as Record<string, unknown> | undefined) ?? parsed;
      const channel =
        rss && typeof rss === 'object'
          ? ((rss as Record<string, unknown>)['channel'] as
              | Record<string, unknown>
              | undefined)
          : undefined;
      const items = asArray(channel?.['item']) as Array<Record<string, unknown>>;
      const ratings = items.map((item) => ({
        title: textOf(item, 'title'),
        description: textOf(item, 'description')
          .replace(/<[^>]+>/g, '')
          .trim(),
        source: 'rfs_fdr' as const,
      }));
      return ok(c, { ratings });
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'rfs fdr fetch failed');
      return ok(c, { ratings: [] });
    }
  },
);
