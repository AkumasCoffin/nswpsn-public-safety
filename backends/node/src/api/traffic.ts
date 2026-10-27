/**
 * Live Traffic NSW endpoints.
 *
 *   GET /api/traffic/incidents        — parsed FeatureCollection
 *   GET /api/traffic/incidents/raw    — upstream JSON pass-through
 *   GET /api/traffic/roadwork         — parsed
 *   GET /api/traffic/roadwork/raw     — pass-through
 *   GET /api/traffic/flood            — parsed
 *   GET /api/traffic/flood/raw        — pass-through
 *   GET /api/traffic/fire             — parsed
 *   GET /api/traffic/fire/raw         — pass-through
 *   GET /api/traffic/majorevent       — parsed
 *   GET /api/traffic/majorevent/raw   — pass-through
 *   GET /api/traffic/alpine           — parsed
 *   GET /api/traffic/alpine/raw       — pass-through
 *   GET /api/traffic/lga              — council-submitted local-road records
 *   GET /api/traffic/lga/raw          — pass-through
 *   GET /api/traffic/works            — works + ACT records from all-feeds-web
 *                                       (no /raw — see /api/traffic/all-feeds)
 *   GET /api/traffic/cameras          — camera FeatureCollection
 *   GET /api/traffic/lga-incidents    — the council feed's records, unparsed
 *   GET /api/traffic/all-feeds        — all-feeds-web.json, grouped by eventType
 *
 * Mirrors python's routes at external_api_proxy.py:7239+. The /raw
 * variants hit upstream every time (Python caches them at HTTP TTL
 * level); the parsed variants serve out of LiveStore.
 *
 * All of them are also served at /api/v1/ (server.ts mirrors them). The
 * routes that read upstream per request (/raw, lga-incidents, all-feeds)
 * answer an empty body (200) on the old path when upstream cannot be read,
 * as they always have; on /api/v1/ they answer 503 service_unavailable, so
 * an outage does not read as "nothing on the roads".
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import {
  trafficCamerasSnapshot,
  trafficHazardSnapshot,
  fetchHazardRaw,
  type TrafficCamerasSnapshot,
  type TrafficSnapshot,
} from '../sources/traffic.js';
import { fetchJson } from '../sources/shared/http.js';
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

export const trafficRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_HAZARDS = scoped({ source: 'livetraffic', type: 'hazards', window: 'live' });
const SCOPE_CAMERAS = scoped({ source: 'livetraffic', type: 'cameras', window: 'live' });

interface AllFeedsItem {
  eventType?: string;
  eventCategory?: string;
  [k: string]: unknown;
}

interface LgaItem {
  [k: string]: unknown;
}

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

// The seven hazard feeds and /traffic/works share one record shape
// (sources/traffic.ts parseTrafficItem); where works fills a field
// differently, the field says so.
const TIME = 'On the hazard feeds, unix milliseconds written as a string; on /traffic/works, ISO 8601. Empty when the feed has none';
const FIRST_ROAD = "From the record's first road";
const PROSE = 'Plain text (the HTML the feed writes is removed)';

const TrafficHazards = featureCollection(
  feature(
    PointGeometry,
    z.object({
      id: z
        .string()
        .describe(
          "The feed's own id for the record. On /traffic/works, an id made from the record's position, title and category instead, because that feed issues new ids each time it is rebuilt",
        ),
      type: z
        .string()
        .describe(
          "Which feed the record came from: Incident, Roadwork, Flood, Fire, Major Event, Alpine or Council. On /traffic/works, the record's eventCategory (else its eventType, else Works)",
        ),
      incidentType: z
        .string()
        .describe(
          'The kind of incident named at the start of the title, upper case (CRASH, BREAKDOWN, HAZARD, FLOODING, ROADWORK, …); empty when the title starts with none of them',
        ),
      mainCategory: z
        .string()
        .describe(
          "The feed's own category, e.g. CRASH or SCHEDULED ROADWORK. On /traffic/works, its eventCategory (Roadwork, Flood, generalHazards, …)",
        ),
      subCategory: z
        .string()
        .describe('The feed\'s sub-category (its subCategoryA, else subCategoryB) as published — sometimes the text "null"'),
      subcategory: z
        .string()
        .describe('A filter key: incidentType, else mainCategory. On /traffic/works: mainCategory, else incidentKind'),
      incidentKind: z
        .string()
        .describe("As the hazard feeds give it — Planned or Unplanned in the samples seen. On /traffic/works, the record's eventType"),
      title: z
        .string()
        .describe('The headline, else the title, else the display name, with a leading incidentType taken off when enough is left to stand as a title'),
      headline: z.string(),
      displayName: z.string(),
      subtitle: z.string(),
      otherAdvice: z.string().describe(`${PROSE}: the feed's otherAdvice, else its adviceA`),
      adviceB: z.string().describe(PROSE),
      roads: z
        .string()
        .describe('The affected roads, each as "street suburb", separated by "; " (or the feed\'s own text, where it gives the roads as text)'),
      affectedDirection: z
        .string()
        .describe(`${FIRST_ROAD}: its affectedDirection, else its conditionTendency — empty in the hazard feeds as currently published`),
      impactedLanes: z.array(z.unknown()).describe(`${FIRST_ROAD}: its impactedLanes, where the feed gives them as a list`),
      speedLimit: z.string().describe('As the feed gives it, written as text; "-1" or empty where it sets none'),
      expectedDelay: z
        .string()
        .describe('The feed\'s expectedDelay (else delay), written as text; "-1" or empty where there is none'),
      diversions: z.string(),
      encodedPolyline: z.string().describe('The first line in encodedPolylines, or empty'),
      encodedPolylines: z
        .array(
          z.object({
            coords: z.string().describe('The line as an encoded polyline, as the feed gives it'),
            direction: z.string().describe('As the feed gives it, e.g. BOTH_DIRECTIONS'),
          }),
        )
        .describe('Every line the record carries'),
      created: z.string().describe(`When the record was created (else its start). ${TIME}`),
      lastUpdated: z.string().describe(`When the record was last updated (else its end). ${TIME}`),
      start: z.string().describe(TIME),
      end: z.string().describe(TIME),
      isEnded: z.boolean(),
      isMajor: z.boolean(),
      arrangement: z.string(),
      periods: z.array(z.unknown()).describe("The feed's periods, as published"),
      isLocalRoad: z.string().describe('Road ownership as the hazard feeds word it: "State road" or "Local road"'),
      region: z.string().describe(FIRST_ROAD),
      crossStreet: z.string().describe(FIRST_ROAD),
      secondLocation: z.string().describe(FIRST_ROAD),
      locationQualifier: z.string().describe(`${FIRST_ROAD}, e.g. "between"`),
      queueLength: z.number().nullable().describe(`${FIRST_ROAD}: its queueLength, or null`),
      duration: z.string(),
      adviceC: z.string().describe(PROSE),
      webLinks: z.array(z.unknown()).describe('Links the feed gives for the record, as published'),
      upstreamId: z
        .string()
        .optional()
        .describe("/traffic/works only: that feed's own id for the record, which changes when the feed is rebuilt"),
      orgName: z.string().describe('The council that submitted the record — Council feed only, empty elsewhere'),
      orgContact: z.string().describe("The council's phone number — Council feed only"),
      orgEmail: z.string().describe('Council feed only'),
      orgWebsite: z.string().describe('Council feed only'),
      source: z.literal('livetraffic'),
    }),
  ),
).meta({ id: 'TrafficHazards' });
assertShape<SameShape<z.infer<typeof TrafficHazards>, TrafficSnapshot>>();

// The hazard feeds as Transport for NSW publishes them — passed through
// untyped, so this names only what every feed was seen to carry.
const TrafficHazardFeed = z
  .object({
    type: z.literal('FeatureCollection'),
    lastPublished: z.number().optional().describe('When Transport for NSW last published the feed, unix milliseconds'),
    layerName: z.string().optional().describe('Which feed this is, e.g. Incident, Roadwork, LGA'),
    rights: z
      .object({ copyright: z.string(), licence: z.string().describe('Link to the licence') })
      .catchall(z.unknown())
      .optional()
      .describe("Transport for NSW's copyright and licence for the data"),
    features: z.array(
      z
        .object({
          type: z.literal('Feature'),
          id: z.number().optional().describe("Transport for NSW's id for the record"),
          geometry: z.object({ type: z.string(), coordinates: z.unknown() }).catchall(z.unknown()),
          properties: z
            .record(z.string(), z.unknown())
            .describe("The record's fields as Transport for NSW names them; its times (created, lastUpdated, start, end) are unix milliseconds"),
        })
        .catchall(z.unknown()),
    ),
  })
  .catchall(z.unknown())
  .meta({ id: 'TrafficHazardFeed' });

const TrafficCameras = featureCollection(
  feature(
    PointGeometry,
    z.object({
      id: z.string(),
      title: z.string().describe('The camera\'s name; "Traffic Camera" when the feed gives none'),
      view: z.string().describe('What the camera shows, in words'),
      direction: z.string().describe('Roughly the way the camera looks, as a compass point: N, N-E, E, S-E, S, S-W, W or N-W'),
      region: z.string().describe("The feed's region for the camera, mostly a code such as SYD_MET or REG_NORTH"),
      imageUrl: z.string().describe("The camera's image (a JPEG), on Transport for NSW's servers"),
      path: z.string().describe('The feed\'s short name for the camera, e.g. "5-ways-miranda"'),
      source: z.literal('livetraffic_cameras'),
    }),
  ),
).meta({ id: 'TrafficCameras' });
assertShape<SameShape<z.infer<typeof TrafficCameras>, TrafficCamerasSnapshot>>();

const TrafficLgaIncidents = z
  .object({
    type: z.literal('FeatureCollection'),
    features: z
      .array(z.record(z.string(), z.unknown()))
      .describe("Each record exactly as Transport for NSW publishes it: a GeoJSON Feature, with the council's fields in its properties"),
  })
  .meta({ id: 'TrafficLgaIncidents' });

const AllFeedsRecord = z
  .object({ eventType: z.string().optional(), eventCategory: z.string().optional() })
  .catchall(z.unknown())
  .describe('A record exactly as published');
const TrafficAllFeeds = z
  .object({
    raw: z.array(AllFeedsRecord).describe('Every record in the feed, in its order'),
    grouped: z
      .record(z.string(), z.array(AllFeedsRecord))
      .describe('The same records by eventType (else eventCategory, else "unknown")'),
    eventTypes: z.array(z.string()).describe('The keys of grouped'),
    totalCount: z.number().int().describe('Number of records in raw'),
  })
  .meta({ id: 'TrafficAllFeeds' });

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

const UNREADABLE = 'Live Traffic NSW could not be read just now.';
const NOT_THE_FEED = 'Live Traffic NSW did not answer with the feed just now.';

function isFeatureCollection(v: unknown): boolean {
  return (
    v !== null &&
    typeof v === 'object' &&
    (v as { type?: unknown }).type === 'FeatureCollection' &&
    Array.isArray((v as { features?: unknown }).features)
  );
}

const HAZARDS: Array<{
  path: string;
  storeKey: string;
  rawEndpoint: string;
  /** operationId stem: getTraffic<op> and getTraffic<op>Raw. */
  op: string;
  summary: string;
  what: string;
  /** How often the poller refreshes it (sources/traffic.ts HAZARD_KINDS). */
  every: string;
}> = [
  {
    path: 'incidents', storeKey: 'traffic_incidents', rawEndpoint: 'incident', op: 'Incidents',
    summary: 'NSW road incidents', what: 'Crashes, breakdowns, hazards and changed traffic conditions on NSW roads', every: 'minute',
  },
  {
    path: 'roadwork', storeKey: 'traffic_roadwork', rawEndpoint: 'roadwork', op: 'Roadwork',
    summary: 'NSW roadwork', what: 'Roadwork on NSW roads', every: 'five minutes',
  },
  {
    path: 'flood', storeKey: 'traffic_flood', rawEndpoint: 'flood', op: 'Flood',
    summary: 'NSW roads affected by flooding', what: 'Road closures and hazards on NSW roads caused by flooding', every: 'five minutes',
  },
  {
    path: 'fire', storeKey: 'traffic_fire', rawEndpoint: 'fire', op: 'Fire',
    summary: 'NSW roads affected by fire', what: 'Fire affecting NSW roads', every: 'five minutes',
  },
  {
    path: 'majorevent', storeKey: 'traffic_majorevent', rawEndpoint: 'majorevent', op: 'MajorEvents',
    summary: 'NSW major events on the roads', what: 'Special events that close or change NSW roads', every: 'five minutes',
  },
  {
    path: 'alpine', storeKey: 'traffic_alpine', rawEndpoint: 'alpine', op: 'Alpine',
    summary: 'NSW alpine road conditions', what: 'Conditions on NSW alpine roads', every: 'five minutes',
  },
  {
    path: 'lga', storeKey: 'traffic_lga', rawEndpoint: 'regional/lga-incidents', op: 'Council',
    summary: 'NSW council road records',
    what: "Records NSW councils submit for their local roads — roadwork, hazards, weather and events — with the council's contact details",
    every: 'five minutes',
  },
];

for (const h of HAZARDS) {
  trafficRouter.get(
    `/api/traffic/${h.path}`,
    SCOPE_HAZARDS,
    describeRoute({
      operationId: `getTraffic${h.op}`,
      tags: ['Hazards'],
      summary: h.summary,
      description: `${h.what}, from Live Traffic NSW (Transport for NSW), in one record shape across the Live Traffic feeds. Records the feed marks ended are left out. Refreshed every ${h.every}.`,
      responses: { ...okJson(TrafficHazards, 'A GeoJSON FeatureCollection of records'), ...GATED_ERRORS },
    }),
    (c) => geo(c, trafficHazardSnapshot(h.storeKey)),
  );
  trafficRouter.get(
    `/api/traffic/${h.path}/raw`,
    SCOPE_HAZARDS,
    describeRoute({
      operationId: `getTraffic${h.op}Raw`,
      tags: ['Hazards'],
      summary: `${h.summary}, unparsed`,
      description: `The Live Traffic NSW feed behind /traffic/${h.path}, exactly as Transport for NSW publishes it, for clients that parse it themselves. Read from Live Traffic NSW on each request, and ended records are not removed.`,
      responses: {
        ...okJson(TrafficHazardFeed, 'The feed as published: a GeoJSON FeatureCollection'),
        ...GATED_ERRORS,
        ...UNAVAILABLE_ERROR,
      },
    }),
    async (c) => {
      try {
        const raw = await fetchHazardRaw(h.rawEndpoint);
        // The old path passes on whatever upstream sent, as it always has.
        if (!isV1(c)) return ok(c, raw);
        if (!isFeatureCollection(raw)) {
          log.warn({ kind: h.path }, 'traffic raw fetch: not a FeatureCollection');
          return fail(c, 503, 'service_unavailable', NOT_THE_FEED);
        }
        return geo(c, raw);
      } catch (err) {
        log.warn(
          { err: (err as Error).message, kind: h.path },
          'traffic raw fetch failed',
        );
        // The old path has always answered an empty collection (200) rather than an error.
        if (!isV1(c)) return geo(c, { type: 'FeatureCollection', features: [] });
        return fail(c, 503, 'service_unavailable', UNREADABLE);
      }
    },
  );
}

trafficRouter.get(
  '/api/traffic/cameras',
  SCOPE_CAMERAS,
  describeRoute({
    operationId: 'getTrafficCameras',
    tags: ['Cameras'],
    summary: 'NSW traffic cameras',
    description: 'Live Traffic NSW (Transport for NSW) traffic cameras, each with a link to its image. Refreshed every minute.',
    responses: { ...okJson(TrafficCameras, 'A GeoJSON FeatureCollection of cameras'), ...GATED_ERRORS },
  }),
  (c) => geo(c, trafficCamerasSnapshot()),
);

// Works + ACT records lifted out of all-feeds-web.json. No /raw variant —
// that whole file is already served verbatim by /api/traffic/all-feeds
// below, and pointing a raw route at a hazards endpoint would return a
// different feed entirely.
trafficRouter.get(
  '/api/traffic/works',
  SCOPE_HAZARDS,
  describeRoute({
    operationId: 'getTrafficWorks',
    tags: ['Hazards'],
    summary: 'Road works and hazards from the Live Traffic aggregate feed',
    description:
      "Roadwork, hazards, flooding and events from Live Traffic NSW's aggregate web feed, which carries other states' and territories' road agencies, in the same record shape as the hazard feeds. Cameras, rest areas, heavy vehicle checking stations, fire records (eventType Fire, which carries the RFS's own incidents) and records marked ended are left out. When every record from one upstream source (apiSource) drops out of the feed at once, the last ones seen are kept for up to six hours. Refreshed every five minutes.",
    responses: { ...okJson(TrafficHazards, 'A GeoJSON FeatureCollection of records'), ...GATED_ERRORS },
  }),
  (c) => geo(c, trafficHazardSnapshot('traffic_works')),
);

// /api/traffic/lga-incidents — regional LGA incident feed. Mirrors python
// line 5415-5432. Hits upstream on every request (Python wraps in @cached
// at the HTTP layer; for now we fetch each call — we can add a TTL cache
// later if upstream complains). Response is a FeatureCollection with
// minimal parsing; the upstream is already JSON-shaped per-incident.
trafficRouter.get(
  '/api/traffic/lga-incidents',
  SCOPE_HAZARDS,
  describeRoute({
    operationId: 'getTrafficLgaIncidents',
    tags: ['Hazards'],
    summary: 'NSW council road records, unparsed',
    description:
      'The records NSW councils submit for their local roads, exactly as Transport for NSW publishes them, read from Live Traffic NSW on each request. /traffic/lga has the same records in the hazard record shape; /traffic/lga/raw has the feed with its own top-level fields.',
    responses: {
      ...okJson(TrafficLgaIncidents, 'A GeoJSON FeatureCollection of records'),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    try {
      const data = await fetchJson<unknown>(
        'https://www.livetraffic.com/traffic/hazards/regional/lga-incidents.json',
        { timeoutMs: 15_000, headers: { 'User-Agent': 'Mozilla/5.0' } },
      );
      const isList = Array.isArray(data);
      const isCollection = Array.isArray((data as { features?: LgaItem[] })?.features);
      const items: LgaItem[] = isList
        ? (data as LgaItem[])
        : isCollection
          ? ((data as { features: LgaItem[] }).features)
          : [];
      if (isV1(c) && !isList && !isCollection) {
        log.warn('traffic lga-incidents fetch: not the feed');
        return fail(c, 503, 'service_unavailable', NOT_THE_FEED);
      }
      return geo(c, { type: 'FeatureCollection', features: items } satisfies z.infer<typeof TrafficLgaIncidents>);
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'traffic lga-incidents fetch failed',
      );
      // The old path has always answered an empty collection (200) rather than an error.
      if (!isV1(c)) return geo(c, { type: 'FeatureCollection', features: [] });
      return fail(c, 503, 'service_unavailable', UNREADABLE);
    }
  },
);

// /api/traffic/all-feeds — pass-through with eventType grouping.
// Mirrors python line 9132-9162.
trafficRouter.get(
  '/api/traffic/all-feeds',
  SCOPE_HAZARDS,
  describeRoute({
    operationId: 'getTrafficAllFeeds',
    tags: ['Hazards'],
    summary: 'The Live Traffic aggregate feed, unparsed',
    description:
      "Every record in Live Traffic NSW's aggregate web feed (all-feeds-web.json) as published — other states' and territories' road events, NSW RFS fires, traffic cameras, rest areas and more — and the same records grouped by eventType. Read from Live Traffic NSW on each request; a large response.",
    responses: {
      ...okJson(envelope(TrafficAllFeeds), 'The records, and the records grouped'),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
    },
  }),
  async (c) => {
    try {
      const data = await fetchJson<AllFeedsItem[]>(
        'https://www.livetraffic.com/datajson/all-feeds-web.json',
        { timeoutMs: 15_000, headers: { 'User-Agent': 'Mozilla/5.0' } },
      );
      if (!Array.isArray(data)) {
        if (!isV1(c)) return ok(c, { raw: [], grouped: {}, eventTypes: [], totalCount: 0 });
        log.warn('traffic all-feeds fetch: not a list');
        return fail(c, 503, 'service_unavailable', NOT_THE_FEED);
      }
      const grouped: Record<string, AllFeedsItem[]> = {};
      for (const item of data) {
        const key = item.eventType ?? item.eventCategory ?? 'unknown';
        if (!grouped[key]) grouped[key] = [];
        grouped[key].push(item);
      }
      return ok(c, {
        raw: data,
        grouped,
        eventTypes: Object.keys(grouped),
        totalCount: data.length,
      } satisfies z.infer<typeof TrafficAllFeeds>);
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'traffic all-feeds fetch failed');
      // The old path has always answered this body (200) rather than an error.
      if (!isV1(c)) {
        return ok(c, {
          error: (err as Error).message,
          raw: [],
          grouped: {},
          totalCount: 0,
        });
      }
      return fail(c, 503, 'service_unavailable', UNREADABLE);
    }
  },
);
