/**
 * GET /api/rfs/incidents       — parsed GeoJSON FeatureCollection
 * GET /api/rfs/incidents/raw   — pass-through of the upstream XML feed
 *                                 converted to JSON, for clients that
 *                                 want to do their own parsing
 * GET /api/rfs/fdr             — fire danger ratings and fire bans
 *
 * Mirrors the Python routes at external_api_proxy.py:10621 + 10726.
 * The /incidents path serves out of LiveStore (filled by the poller).
 * The /raw path hits upstream on every request — same as Python which
 * caches it at HTTP layer rather than via the prewarm pipeline.
 *
 * /incidents and /incidents/raw are also served at /api/v1/ (server.ts
 * mirrors them). /fdr is not: see the note on it below.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { rfsSnapshot, fetchRfsRaw, type RfsSnapshot, type RfsRawSnapshot } from '../sources/rfs.js';
import { fetchText } from '../sources/shared/http.js';
import { parseXml, asArray, textOf } from '../sources/shared/xml.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { geo, isV1, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
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

const RfsRaw = z.object({
  channel: z.object({ title: z.string(), description: z.string(), pubDate: z.string().describe('As the feed writes it (RFC 822)') }),
  items: z.array(
    z.object({
      title: z.string(),
      link: z.string(),
      description: z.string(),
      pubDate: z.string().describe('As the feed writes it (RFC 822)'),
      guid: z.string(),
      category: z.string(),
      point: z.string().nullable().describe('"lat lon" from georss:point, or null'),
    }),
  ),
  count: z.number().int(),
}).meta({ id: 'RfsIncidentsRaw' });
assertShape<SameShape<z.infer<typeof RfsRaw>, RfsRawSnapshot>>();

/** What /raw answers when the RFS feed cannot be reached; on /api/v1/ it keeps the documented shape. */
const EMPTY_RAW: RfsRawSnapshot = { channel: { title: '', description: '', pubDate: '' }, items: [], count: 0 };

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
      "The RFS major incidents feed's channel and items as JSON — each item's title, link, description (with its HTML), date, guid, category and point — fetched from the RFS when asked rather than from memory. Empty (not an error) when the RFS feed cannot be reached.",
    responses: { ...okJson(envelope(RfsRaw), 'The feed channel and its items'), ...GATED_ERRORS },
  }),
  async (c) => {
    try {
      const raw = await fetchRfsRaw();
      return ok(c, raw);
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'rfs raw fetch failed');
      // The old path keeps the body it always sent.
      return ok(c, isV1(c) ? EMPTY_RAW : { channel: {}, items: [], count: 0 });
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
