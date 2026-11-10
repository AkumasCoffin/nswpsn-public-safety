/**
 * Road endpoints: Live Traffic NSW, and the other states' road events.
 *
 *   GET /api/traffic/incidents        — parsed FeatureCollection, every state
 *   GET /api/traffic/incidents/raw    — upstream JSON pass-through (NSW)
 *   GET /api/traffic/roadwork         — parsed, every state
 *   GET /api/traffic/roadwork/raw     — pass-through
 *   GET /api/traffic/flood            — parsed, every state
 *   GET /api/traffic/flood/raw        — pass-through
 *   GET /api/traffic/fire             — parsed (NSW)
 *   GET /api/traffic/fire/raw         — pass-through
 *   GET /api/traffic/majorevent       — parsed, every state
 *   GET /api/traffic/majorevent/raw   — pass-through
 *   GET /api/traffic/alpine           — parsed (NSW)
 *   GET /api/traffic/alpine/raw       — pass-through
 *   GET /api/traffic/lga              — council-submitted local-road records (NSW)
 *   GET /api/traffic/lga/raw          — pass-through
 *   GET /api/traffic/cameras          — camera FeatureCollection
 *   GET /api/traffic/lga-incidents    — the council feed's records, unparsed
 *   GET /api/traffic/all-feeds        — all-feeds-web.json, grouped by eventType
 *
 * Mirrors python's routes at external_api_proxy.py:7239+. The /raw
 * variants hit upstream every time (Python caches them at HTTP TTL
 * level); the parsed variants serve out of LiveStore.
 *
 * Incidents, roadwork, flood and major events carry QLD, SA, VIC and ACT
 * records too, each from that state's own feed (sources/interstateRoads.ts),
 * after NSW's; every record says its `state`, and `?state=` keeps the ones
 * asked for. Those records used to have a feed of their own, /traffic/works,
 * read from Live Traffic's aggregate; it is gone.
 *
 * All of them but /cameras are also served at /api/v1/ (server.ts mirrors
 * them). The routes that read upstream per request (/raw, lga-incidents,
 * all-feeds) answer an empty body (200) on the old path when upstream cannot
 * be read, as they always have; on /api/v1/ they answer 503
 * service_unavailable, so an outage does not read as "nothing on the roads".
 *
 * /cameras is this site's route, for the map: camera images are not part of
 * the API, so it declares no scope and no API key reaches it, and all-feeds
 * leaves out the camera records Live Traffic's aggregate feed carries.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import {
  trafficCamerasSnapshot,
  trafficHazardSnapshot,
  fetchHazardRaw,
  LIVE_TRAFFIC_CREDIT,
  type TrafficCamerasSnapshot,
  type TrafficFeature,
  type TrafficSnapshot,
} from '../sources/traffic.js';
import { interstateRecords, type RoadType } from '../sources/interstateRoads.js';
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
  queryValidator,
  UNAVAILABLE_ERROR,
  VALIDATION_ERROR,
  type SameShape,
} from './v1/schemas.js';

export const trafficRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
// /cameras declares nothing, so no scoped key reaches it.
const SCOPE_HAZARDS = scoped({ feed: 'roads', window: 'live' });

/** Live Traffic's eventType for its camera records, which all-feeds leaves out. */
const CAMERA_EVENT_TYPE = 'liveCams';

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

// The NSW hazard feeds and the other states' records share one record shape
// (sources/traffic.ts parseTrafficItem, sources/interstateRoads.ts
// roadRecord); where another state's record fills a field differently, the
// field says so.
const TIME = "On the NSW feeds, unix milliseconds written as a string; on other states' records, ISO 8601 in UTC. Empty when the feed has none";
const FIRST_ROAD = "From the record's first road";
const PROSE = 'Plain text (the HTML the feed writes is removed)';

const TrafficHazards = featureCollection(
  feature(
    PointGeometry,
    z.object({
      id: z
        .string()
        .describe(
          "The feed's own id for the record. Other states' ids start with the state (qld:, sa:, vic:, act:); a VIC record read from Live Traffic's aggregate (until a Transport Victoria key is set) has an id made from its position, title and category (ltw:), because that feed issues new ids each time it is rebuilt",
        ),
      type: z
        .string()
        .describe(
          "Which feed the record is on: Incident, Roadwork, Flood, Fire, Major Event, Alpine or Council. Another state's record takes the feed it belongs on",
        ),
      incidentType: z
        .string()
        .describe(
          "The kind of incident, upper case (CRASH, BREAKDOWN, HAZARD, FLOODING, ROADWORK, ROAD CLOSURE, …): on the NSW feeds, named at the start of the title; for other states, from the publisher's own type. Empty when there is none",
        ),
      mainCategory: z
        .string()
        .describe(
          "The feed's own category, e.g. CRASH or SCHEDULED ROADWORK; for other states, the publisher's own word (Hazard, Roadworks, Road closure, Outback road conditions, …)",
        ),
      subCategory: z
        .string()
        .describe('The feed\'s sub-category (its subCategoryA, else subCategoryB) as published — sometimes the text "null"; for other states, the publisher\'s subtype'),
      subcategory: z
        .string()
        .describe('A filter key: incidentType, else mainCategory'),
      incidentKind: z
        .string()
        .describe('Planned or Unplanned'),
      title: z
        .string()
        .describe(
          "The headline, else the title, else the display name, with a leading incidentType taken off when enough is left to stand as a title; for other states, the publisher's title, or its type and subtype",
        ),
      headline: z.string(),
      displayName: z.string(),
      subtitle: z.string(),
      otherAdvice: z.string().describe(`${PROSE}: the feed's otherAdvice, else its adviceA`),
      adviceB: z.string().describe(PROSE),
      roads: z
        .string()
        .describe(
          'The affected roads, each as "street suburb", separated by "; " (or the feed\'s own text, where it gives the roads as text); an SA job over several suburbs as "road, suburb to suburb", the suburbs at either end',
        ),
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
        .describe("A VIC record read from Live Traffic's aggregate only: that feed's own id for it, which changes when the feed is rebuilt"),
      orgName: z.string().describe('The council that submitted the record — the NSW Council feed, and QLD records a council supplied; empty elsewhere'),
      orgContact: z.string().describe("The council's phone number — NSW Council feed only"),
      orgEmail: z.string().describe('NSW Council feed only'),
      orgWebsite: z.string().describe('The NSW Council feed, and QLD records a council supplied'),
      state: z.enum(['NSW', 'QLD', 'VIC', 'SA', 'ACT']).describe('The state or territory the record is in'),
      source: z
        .enum(['livetraffic', 'qldtraffic', 'traffic_sa', 'roads_act', 'transport_vic'])
        .describe(
          "Who publishes it: livetraffic (Live Traffic NSW, Transport for NSW), qldtraffic (QLDTraffic, Queensland Department of Transport and Main Roads), traffic_sa (South Australia's Department for Infrastructure and Transport), roads_act (Roads ACT), transport_vic (Victoria's Department of Transport and Planning)",
        ),
      credit: z
        .string()
        .describe("The attribution its publisher asks for — show it with the record. The QLD, SA and VIC data is published under CC BY 4.0"),
      sites: z
        .array(
          z.object({
            lat: z.number(),
            lon: z.number(),
            crossStreet: z.string(),
            suburb: z.string(),
            direction: z.string(),
            lanes: z.string().describe('What the site does to the lanes, in words ("shuttle flow", "one lane closed"); empty for nothing'),
            speedLimit: z.string().describe('km/h; empty for no limit'),
          }),
        )
        .optional()
        .describe(
          "SA's roadworks only. SA lists works site by site, a record for each cross street along a job; here each job is one record, pinned at the site nearest its middle, and this lists every site in order along the road, from one end to the other. Its encodedPolylines are the road between the end sites, from the Department for Infrastructure and Transport's drawing of SA's main roads (CC BY 3.0 AU) — part of the job where that drawing has only part of the road, none where it lacks the road. Sites that restrict nothing (no lane closed, no speed limit) are left out",
        ),
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
  /** The other states' records this feed carries too (sources/interstateRoads.ts); NSW only when absent. */
  interstate?: RoadType;
}> = [
  {
    path: 'incidents', storeKey: 'traffic_incidents', rawEndpoint: 'incident', op: 'Incidents',
    summary: 'Road incidents', what: 'Crashes, breakdowns, hazards, closures and changed traffic conditions', every: 'minute',
    interstate: 'Incident',
  },
  {
    path: 'roadwork', storeKey: 'traffic_roadwork', rawEndpoint: 'roadwork', op: 'Roadwork',
    summary: 'Roadwork', what: 'Roadwork, and other works on and beside the road', every: 'five minutes',
    interstate: 'Roadwork',
  },
  {
    path: 'flood', storeKey: 'traffic_flood', rawEndpoint: 'flood', op: 'Flood',
    summary: 'Roads affected by flooding', what: 'Road closures and hazards caused by flooding', every: 'five minutes',
    interstate: 'Flood',
  },
  {
    path: 'fire', storeKey: 'traffic_fire', rawEndpoint: 'fire', op: 'Fire',
    summary: 'NSW roads affected by fire', what: 'Fire affecting NSW roads', every: 'five minutes',
  },
  {
    path: 'majorevent', storeKey: 'traffic_majorevent', rawEndpoint: 'majorevent', op: 'MajorEvents',
    summary: 'Major events on the roads', what: 'Special events that close or change roads', every: 'five minutes',
    interstate: 'Major Event',
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

const ROAD_STATES = ['NSW', 'QLD', 'VIC', 'SA', 'ACT'] as const;

const RoadQuery = z.object({
  state: z
    .string()
    .optional()
    .transform((v, ctx) => {
      const items = (v ?? '').split(',').map((s) => s.trim().toUpperCase()).filter(Boolean);
      const bad = items.filter((s) => !(ROAD_STATES as readonly string[]).includes(s));
      if (bad.length) {
        ctx.addIssue({ code: 'custom', message: `unknown state: ${bad.join(', ')} (one of ${ROAD_STATES.join(', ')})` });
        return z.NEVER;
      }
      return items;
    })
    .describe(`Comma-separated; any of ${ROAD_STATES.join(', ')}, in any case. Leave out for every state the feed has.`),
});

const OTHER_STATES =
  "in NSW from Live Traffic NSW (Transport for NSW), and in Queensland, Victoria, South Australia and the ACT from each one's own feed (QLDTraffic; Transport Victoria; the SA Department for Infrastructure and Transport, outback road conditions included; Roads ACT), NSW's first. Every record says its state and carries its publisher's credit";

for (const h of HAZARDS) {
  trafficRouter.get(
    `/api/traffic/${h.path}`,
    SCOPE_HAZARDS,
    describeRoute({
      operationId: `getTraffic${h.op}`,
      tags: ['Road hazards'],
      summary: h.summary,
      description: h.interstate
        ? `${h.what}, ${OTHER_STATES}; ?state= keeps the states asked for. One record shape across them all. Records a feed marks ended are left out. NSW's are refreshed every ${h.every}, the other states' every two to five minutes.`
        : `${h.what}, from Live Traffic NSW (Transport for NSW), in one record shape across the Live Traffic feeds. Records the feed marks ended are left out. Refreshed every ${h.every}.`,
      responses: { ...okJson(TrafficHazards, 'A GeoJSON FeatureCollection of records'), ...VALIDATION_ERROR, ...GATED_ERRORS },
    }),
    queryValidator(RoadQuery),
    (c) => {
      const states = new Set<string>(c.req.valid('query').state ?? []);
      // A snapshot saved before records named their state (LiveStore
      // reloads it from disk on restart) is NSW's all the same.
      const nsw = trafficHazardSnapshot(h.storeKey).features.map((f) =>
        f.properties.state ? f : { ...f, properties: { ...f.properties, state: 'NSW' as const, credit: f.properties.credit ?? LIVE_TRAFFIC_CREDIT } },
      );
      const all: TrafficFeature[] = h.interstate ? [...nsw, ...interstateRecords(h.interstate)] : nsw;
      const features = states.size ? all.filter((f) => states.has(f.properties.state)) : all;
      return geo(c, { type: 'FeatureCollection', features, count: features.length });
    },
  );
  trafficRouter.get(
    `/api/traffic/${h.path}/raw`,
    SCOPE_HAZARDS,
    describeRoute({
      operationId: `getTraffic${h.op}Raw`,
      tags: ['Road hazards'],
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
  describeRoute({
    operationId: 'getTrafficCameras',
    tags: ['Cameras'],
    summary: 'NSW traffic cameras',
    description: 'Live Traffic NSW (Transport for NSW) traffic cameras, each with a link to its image. Refreshed every minute.',
    responses: { ...okJson(TrafficCameras, 'A GeoJSON FeatureCollection of cameras'), ...GATED_ERRORS },
  }),
  (c) => geo(c, trafficCamerasSnapshot()),
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
    tags: ['Road hazards'],
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
    tags: ['Road hazards'],
    summary: 'The Live Traffic aggregate feed, unparsed',
    description:
      "Every record in Live Traffic NSW's aggregate web feed (all-feeds-web.json) as published — other states' and territories' road events, NSW RFS fires, rest areas and more — and the same records grouped by eventType. Its traffic cameras (eventType liveCams) are left out. Read from Live Traffic NSW on each request; a large response.",
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
      // Camera images are not part of what this API hands on.
      const records = data.filter((item) => item.eventType !== CAMERA_EVENT_TYPE);
      const grouped: Record<string, AllFeedsItem[]> = {};
      for (const item of records) {
        const key = item.eventType ?? item.eventCategory ?? 'unknown';
        if (!grouped[key]) grouped[key] = [];
        grouped[key].push(item);
      }
      return ok(c, {
        raw: records,
        grouped,
        eventTypes: Object.keys(grouped),
        totalCount: records.length,
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
