/**
 * GET /api/v1/outages — every electricity distributor's outages in one feed.
 *
 * Two kinds of distributor are merged here. Endeavour Energy, Essential
 * Energy and Ausgrid (NSW) are read from the same LiveStore snapshots the
 * per-distributor routes serve (sources/endeavour.ts, essential.ts,
 * ausgrid.ts), each in its own shape, and mapped to the shared one below.
 * Those routes (/api/endeavour/current, /api/essential/outages, …) stay /api/
 * routes for this site's map and live page and the Discord bot, which draw
 * each distributor its own way. Every other distributor (Victoria's,
 * Queensland's, South Australia's, Western Australia's) has a source under
 * sources/outages/ that stores the shared shape already, so its store is read
 * as it is. An API key reads this route (feed `outages`, apiKeyScopes.ts).
 *
 * Every feature carries the same properties, snake_case like the rest of
 * /api/v1/: the distributor's id, the archived source it is stored under (so
 * /api/v1/history?source=…&source_id=… finds its past), the distributor and
 * its state, whether it is unplanned or planned and whether the power is off
 * now or the work is still to come, the area, customers affected, cause,
 * start and expected restoration and last update (UTC), the outage area's
 * outline where the distributor gives one, and `details` — the distributor's
 * own extra fields, under their own names, so nothing its own feed carries is
 * lost.
 *
 * Times are UTC in the answer. Endeavour Energy writes its own with a zone.
 * Essential Energy writes "02/11/2026 12:00:00" with none, which is NSW time,
 * so it is read as Sydney time; the other states' sources read theirs in
 * their own zone (sources/outages/common.ts zonedUtc). Anything that cannot
 * be read is left in `details` under its own name and the common field is
 * null.
 *
 * `sources` says, per distributor, how many outages it gave and when this
 * server last read it, so a distributor that is not being read (one switched
 * off, or not read yet since a restart) shows as not read rather than as no
 * outages.
 *
 * Registered at /api/outages and mirrored to /api/v1/ like every other feed;
 * the old path is nobody's and goes when old paths are retired.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { liveStore } from '../store/live.js';
import type { EndeavourOutage } from '../sources/endeavour.js';
import type { EssentialOutage } from '../sources/essential.js';
import { ausgridScheduled, type AusgridOutage, type AusgridOutagesPayload } from '../sources/ausgrid.js';
import {
  DISTRIBUTOR_INFO,
  DISTRIBUTORS,
  OUTAGE_KINDS,
  OUTAGE_SOURCES,
  OUTAGE_STATES,
  OUTAGE_STATUSES,
  OUTAGE_STORES,
  withStatus,
  zonedUtc,
  type Distributor,
  type Outage,
  type OutageState,
} from '../sources/outages/common.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { geo } from './v1/respond.js';
import {
  feature,
  featureCollection,
  GATED_ERRORS,
  okJson,
  PointGeometry,
  queryValidator,
  Rings,
  VALIDATION_ERROR,
} from './v1/schemas.js';

export const outagesRouter = new Hono();

// What a scoped API key needs to read this route (services/auth/apiKeyScopes.ts).
const SCOPE_OUTAGES = scoped({ feed: 'outages', window: 'live' });

// The record and the table of distributors live with the sources that make
// them (sources/outages/common.ts); what counts from the merged list
// (api/stats.ts) reads them from here, beside mergedOutages.
export { DISTRIBUTOR_INFO, DISTRIBUTORS } from '../sources/outages/common.js';
export type { Distributor, Outage, OutageState } from '../sources/outages/common.js';

type Props = Record<string, unknown>;

// ---------------------------------------------------------------------------
// Times
// ---------------------------------------------------------------------------

/**
 * An NSW distributor's time as UTC ISO 8601: with a zone as written, without
 * one as Sydney time. Null when it is empty or not a time. Only the three
 * NSW mappers below use it; every other distributor's times are read by its
 * source, in its own zone.
 */
export function outageUtc(v: unknown): string | null {
  return zonedUtc(v, 'Australia/Sydney');
}

// ---------------------------------------------------------------------------
// Each distributor's records, in the shared shape
// ---------------------------------------------------------------------------

const str = (v: unknown): string => (typeof v === 'string' ? v : v == null ? '' : String(v));
const num = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);

/** The distributor's other fields; a time that could not be read stays here under its own name. */
function detailsOf(record: object, mapped: ReadonlySet<string>, times: Readonly<Record<string, string | null>>): Props {
  const details: Props = {};
  for (const [k, v] of Object.entries(record)) {
    if (k in times) {
      if (times[k] === null && str(v).trim()) details[k] = v;
      continue;
    }
    if (!mapped.has(k)) details[k] = v;
  }
  return details;
}

// Endeavour's own fields that become common ones (the times are handled by detailsOf).
const ENDEAVOUR_MAPPED = new Set(['id', 'suburb', 'customersAffected', 'cause', 'outageType', 'latitude', 'longitude', 'hasGPS', 'endTime']);

export function fromEndeavour(o: EndeavourOutage): Outage {
  const planned = o.outageType !== 'Unplanned';
  const times = {
    startTime: outageUtc(o.startTime),
    estimatedRestoration: outageUtc(o.estimatedRestoration),
    lastUpdated: outageUtc(o.lastUpdated),
  };
  return {
    id: o.id,
    source: planned ? 'endeavour_planned' : 'endeavour_current',
    distributor: 'endeavour',
    state: 'NSW',
    kind: planned ? 'planned' : 'unplanned',
    status: o.outageType === 'Future Maintenance' ? 'scheduled' : 'active',
    // The poller writes "Unknown" when Endeavour gives no suburb; here that is empty.
    area: o.suburb === 'Unknown' ? '' : o.suburb,
    customers_affected: o.customersAffected,
    cause: o.cause,
    start_at: times.startTime,
    restore_at: times.estimatedRestoration,
    updated_at: times.lastUpdated,
    lat: num(o.latitude),
    lon: num(o.longitude),
    polygons: [],
    details: detailsOf(o, ENDEAVOUR_MAPPED, times),
  };
}

const ESSENTIAL_MAPPED = new Set([
  'incidentId', 'suburb', 'outageType', 'feedType', 'latitude', 'longitude', 'cause', 'customersAffected',
  'status', 'source', 'sourceTimestamp', 'polygon', 'provider',
]);

export function fromEssential(o: EssentialOutage): Outage {
  const times = { timeOff: outageUtc(o.timeOff), estTimeOn: outageUtc(o.estTimeOn), lastUpdated: outageUtc(o.lastUpdated) };
  return {
    id: o.incidentId,
    source: o.source,
    distributor: 'essential',
    state: 'NSW',
    kind: o.outageType,
    status: o.status,
    area: o.suburb,
    customers_affected: o.customersAffected,
    // The poller writes "Unknown" when Essential gives no reason; here that is empty.
    cause: o.cause === 'Unknown' ? '' : o.cause,
    start_at: times.timeOff,
    restore_at: times.estTimeOn,
    updated_at: times.lastUpdated,
    lat: num(o.latitude),
    lon: num(o.longitude),
    polygons: o.polygon && o.polygon.length >= 3 ? [o.polygon.map(([lon, lat]) => [lon, lat])] : [],
    details: detailsOf(o, ESSENTIAL_MAPPED, times),
  };
}

// Every field of an Ausgrid record is accounted for here, so detailsOf adds
// nothing but a time that could not be read. The record gives most fields
// twice (PascalCase and camelCase) and several that Ausgrid never fills
// (JobId, Postcode, Detail, Classification, Polygons); fromAusgrid writes the
// ones that say something into details itself, under the names Endeavour's
// details use where it has the same thing.
const AUSGRID_MAPPED = new Set([
  'OutageId', 'JobId', 'Suburb', 'StreetName', 'Streets', 'Postcode', 'CustomersAffected', 'OutageType', 'Cause',
  'Reason', 'Detail', 'StatusText', 'EndDateTime', 'Latitude', 'Longitude', 'Status', 'Classification', 'Polygons',
  'outageId', 'suburb', 'streetName', 'streets', 'postcode', 'customersAffected', 'outageType', 'cause',
  'startTime', 'estRestoration', 'endDateTime', 'latitude', 'longitude',
]);

export function fromAusgrid(o: AusgridOutage, nowMs: number = Date.now()): Outage {
  const times = {
    StartTime: outageUtc(o.StartTime),
    EstRestoration: outageUtc(o.EstRestoration),
    WhenUpdated: outageUtc(o.WhenUpdated),
  };
  return {
    id: str(o.OutageId),
    source: 'ausgrid',
    distributor: 'ausgrid',
    state: 'NSW',
    kind: o.OutageType === 'Planned' ? 'planned' : 'unplanned',
    // Ausgrid lists planned work up to 30 days ahead; until it starts nobody is off.
    status: ausgridScheduled(o, nowMs) ? 'scheduled' : 'active',
    area: o.Suburb,
    customers_affected: o.CustomersAffected,
    cause: o.Cause,
    start_at: times.StartTime,
    restore_at: times.EstRestoration,
    updated_at: times.WhenUpdated,
    lat: num(o.Latitude),
    lon: num(o.Longitude),
    polygons: [],
    details: {
      streets: str(o.Streets),
      // In words ("Proceeding as scheduled"), as Endeavour's status is.
      status: str(o.StatusText),
      // As Ausgrid writes it ("ProceedingAsScheduled"), for a program to match on.
      power_status: str(o.Status),
      reason: str(o.Reason),
      ...detailsOf(o, AUSGRID_MAPPED, times),
    },
  };
}

/** One LiveStore snapshot a distributor's outages come from. */
interface Feed {
  distributor: Distributor;
  store: string;
  read: () => Outage[];
}

const list = <T>(store: string): T[] => {
  const data = liveStore.getData<T[]>(store);
  return Array.isArray(data) ? data : [];
};

/**
 * A distributor whose source stores the shared shape: its outages as they
 * are, with each one's status as of now. Planned work is listed days ahead,
 * and the status stored when the file was read would still say "scheduled"
 * after the work began. A store that is missing (not read yet since a
 * restart, or switched off) is no outages, and `sources` says it is not read.
 */
function stored(store: (typeof OUTAGE_STORES)[number]): Outage[] {
  const now = Date.now();
  return list<Outage>(store)
    .filter((o) => o && typeof o === 'object')
    .map((o) => withStatus(o, now));
}

/** In the order the answer lists them. */
const FEEDS: readonly Feed[] = [
  { distributor: 'endeavour', store: 'endeavour_current', read: () => list<EndeavourOutage>('endeavour_current').map(fromEndeavour) },
  { distributor: 'endeavour', store: 'endeavour_maintenance', read: () => list<EndeavourOutage>('endeavour_maintenance').map(fromEndeavour) },
  { distributor: 'endeavour', store: 'endeavour_planned', read: () => list<EndeavourOutage>('endeavour_planned').map(fromEndeavour) },
  { distributor: 'essential', store: 'essential_current', read: () => list<EssentialOutage>('essential_current').map(fromEssential) },
  { distributor: 'essential', store: 'essential_future', read: () => list<EssentialOutage>('essential_future').map(fromEssential) },
  {
    distributor: 'ausgrid',
    store: 'ausgrid',
    read: () => {
      const markers = liveStore.getData<AusgridOutagesPayload>('ausgrid')?.Markers;
      if (!Array.isArray(markers)) return [];
      const now = Date.now();
      return markers.map((m) => fromAusgrid(m, now));
    },
  },
  ...OUTAGE_STORES.map((store): Feed => ({ distributor: store, store, read: () => stored(store) })),
];

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

/** "endeavour: Endeavour Energy (NSW); …", from the one table that names them. */
const DISTRIBUTOR_LIST = DISTRIBUTORS.map((d) => `${d}: ${DISTRIBUTOR_INFO[d].name} (${DISTRIBUTOR_INFO[d].state})`).join('; ');

/** "NSW (Endeavour Energy, Essential Energy, Ausgrid), Victoria (…), …" for the route's description. */
const STATE_NAMES: Readonly<Record<OutageState, string>> = {
  NSW: 'NSW',
  VIC: 'Victoria',
  QLD: 'Queensland',
  SA: 'South Australia',
  WA: 'Western Australia',
  NT: 'the Northern Territory',
  ACT: 'the ACT',
};
const namesIn = (state: OutageState): string[] =>
  DISTRIBUTORS.filter((d) => DISTRIBUTOR_INFO[d].state === state).map((d) => DISTRIBUTOR_INFO[d].name);

/** The states the feed has a distributor in: what ?state= takes. */
const FEED_STATES = OUTAGE_STATES.filter((state) => namesIn(state).length > 0);
const STATE_LIST = FEED_STATES.map((state) => `${STATE_NAMES[state]} (${namesIn(state).join(', ')})`)
  .join(', ')
  .replace(/, (?=[^,(]*\([^)]*\)$)/, ' and ');

const TIME_NOTE = 'ISO 8601 UTC; null when the distributor gives none, or one that cannot be read (then details has it as written)';

const OutageProperties = z.object({
  id: z.string().describe("The distributor's own id for the outage; unique with source"),
  source: z
    .enum(OUTAGE_SOURCES)
    .describe('What stored history files it under (/api/v1/history?source=…&source_id=<id> finds its past): endeavour_current for unplanned and endeavour_planned for planned Endeavour Energy outages; essential_current (unplanned), essential_planned (planned, under way) and essential_future (to come) for Essential Energy; for every other distributor its own name, the same as `distributor` (ausgrid, ausnet, citipower, …)'),
  distributor: z.enum(DISTRIBUTORS).describe(DISTRIBUTOR_LIST),
  state: z.enum(OUTAGE_STATES).describe("The state the distributor's network is in"),
  kind: z.enum(OUTAGE_KINDS).describe('unplanned (a fault) or planned (scheduled work)'),
  status: z.enum(OUTAGE_STATUSES).describe('active: the power is off now (every unplanned outage, and planned work under way); scheduled: planned work not yet started. Decided when the feed is read, from the start time'),
  area: z.string().describe('The suburb or locality, as the distributor names it; empty when it gives none'),
  customers_affected: z.number().int().describe('Customers without power; 0 when not given'),
  cause: z.string().describe("The cause, as the distributor gives it (Endeavour Energy's planned work with none: \"Planned maintenance\"); empty when not given"),
  start_at: z.string().nullable().describe(`When the power went or goes off, ${TIME_NOTE}`),
  restore_at: z.string().nullable().describe(`When the power is expected back, ${TIME_NOTE}`),
  updated_at: z.string().nullable().describe(`When the distributor last updated the outage, ${TIME_NOTE}`),
  polygons: Rings.describe("The outage area's outline as outer rings of [longitude, latitude]; empty when the distributor gives none (of the NSW distributors, only Essential Energy gives one)"),
  details: z
    .record(z.string(), z.unknown())
    .describe("The distributor's own extra fields, under its own names; what is in it differs by distributor (Endeavour Energy: streets, postcode, status; Essential Energy: title; Ausgrid: streets, status in words, power_status as Ausgrid writes it, reason)"),
});

const OutageFeature = feature(PointGeometry.nullable().describe('Where the distributor marks the outage; null when it gives no position'), OutageProperties);
type OutageFeatureT = z.infer<typeof OutageFeature>;

const DistributorStatus = z.object({
  distributor: z.enum(DISTRIBUTORS),
  name: z.string(),
  state: z.enum(OUTAGE_STATES).describe("The state the distributor's network is in"),
  count: z.number().int().describe('Outages from this distributor in this answer, after the filters'),
  updated_at: z
    .string()
    .nullable()
    .describe("When this server last read all of the distributor's outage feeds (the earliest of their last reads), ISO 8601 UTC; null if one has not been read yet or is switched off, and then its count of 0 does not mean it has no outages"),
});

const Outages = featureCollection(OutageFeature, {
  sources: z.array(DistributorStatus).describe('One entry per distributor asked for, in this order'),
}).meta({ id: 'Outages' });

/**
 * A comma list of allowed values, case-insensitive; empty means all. `fold`
 * is how the values are written: the states are upper-case, the rest lower.
 */
const oneOf = <T extends string>(values: readonly T[], what: string, fold: 'lower' | 'upper' = 'lower') =>
  z
    .string()
    .optional()
    .transform((v, ctx) => {
      const items = (v ?? '')
        .split(',')
        .map((s) => (fold === 'upper' ? s.trim().toUpperCase() : s.trim().toLowerCase()))
        .filter(Boolean);
      const bad = items.filter((s) => !(values as readonly string[]).includes(s));
      if (bad.length) {
        ctx.addIssue({ code: 'custom', message: `unknown ${what}: ${bad.join(', ')} (one of ${values.join(', ')})` });
        return z.NEVER;
      }
      return items as T[];
    })
    .describe(`Comma-separated; any of ${values.join(', ')}, in any case. Leave out for all.`);

const Query = z.object({
  state: oneOf(FEED_STATES, 'state', 'upper'),
  distributor: oneOf(DISTRIBUTORS, 'distributor'),
  kind: oneOf(OUTAGE_KINDS, 'kind'),
  status: oneOf(OUTAGE_STATUSES, 'status'),
});

function toFeature(o: Outage): OutageFeatureT {
  return {
    type: 'Feature',
    geometry: o.lat !== null && o.lon !== null ? { type: 'Point', coordinates: [o.lon, o.lat] } : null,
    properties: {
      id: o.id,
      source: o.source,
      distributor: o.distributor,
      state: o.state,
      kind: o.kind,
      status: o.status,
      area: o.area,
      customers_affected: o.customers_affected,
      cause: o.cause,
      start_at: o.start_at,
      restore_at: o.restore_at,
      updated_at: o.updated_at,
      polygons: o.polygons,
      details: o.details,
    },
  };
}

/** What narrows the merged list; nothing given is every distributor's outages. */
export interface OutagesFilter {
  /** Upper-case state codes. */
  state?: readonly string[] | undefined;
  distributor?: readonly string[] | undefined;
  kind?: readonly string[] | undefined;
  status?: readonly string[] | undefined;
}

/**
 * Every distributor's outages in the shared shape, with what each one gave.
 * Reads the LiveStore only. /api/outages answers with it, and the stats
 * (api/stats.ts) count from it.
 *
 * An outage two feeds of one distributor both list is in it twice: Endeavour
 * Energy lists some in both its current and its maintenance feed, and
 * Essential Energy lists planned work under way in its current feed and its
 * future feed, filed under a different `source` in each. What counts outages
 * counts a distributor's id once.
 *
 * A state filter keeps the distributors of those states, so `sources` lists
 * only them. `read` holds the distributors with at least one feed read so
 * far; `sources[].updated_at` is stricter (every feed read).
 */
export function mergedOutages(q: OutagesFilter = {}): {
  outages: Outage[];
  sources: Array<z.infer<typeof DistributorStatus>>;
  read: Set<Distributor>;
} {
  const distributors: readonly string[] = q.distributor?.length ? q.distributor : DISTRIBUTORS;
  const states = new Set<string>(q.state ?? []);
  const kinds = new Set<string>(q.kind ?? []);
  const statuses = new Set<string>(q.status ?? []);
  const outages: Outage[] = [];
  const sources: Array<z.infer<typeof DistributorStatus>> = [];
  const read = new Set<Distributor>();
  for (const distributor of DISTRIBUTORS) {
    if (!distributors.includes(distributor)) continue;
    const info = DISTRIBUTOR_INFO[distributor];
    if (states.size && !states.has(info.state)) continue;
    let count = 0;
    let oldest: number | null = null;
    let unread = false;
    for (const feed of FEEDS) {
      if (feed.distributor !== distributor) continue;
      const ts = liveStore.get(feed.store)?.ts;
      if (!ts) unread = true;
      else {
        read.add(distributor);
        oldest = oldest === null ? ts : Math.min(oldest, ts);
      }
      for (const o of feed.read()) {
        if (states.size && !states.has(o.state)) continue;
        if (kinds.size && !kinds.has(o.kind)) continue;
        if (statuses.size && !statuses.has(o.status)) continue;
        outages.push(o);
        count++;
      }
    }
    sources.push({
      distributor,
      name: info.name,
      state: info.state,
      count,
      updated_at: unread || oldest === null ? null : new Date(oldest * 1000).toISOString(),
    });
  }
  return { outages, sources, read };
}

outagesRouter.get(
  '/api/outages',
  SCOPE_OUTAGES,
  describeRoute({
    operationId: 'getOutages',
    tags: ['Power outages'],
    summary: 'Every distributor, one feed',
    description:
      `Electricity outages — unplanned and planned, now and to come — on the networks of ${STATE_LIST}, ` +
      "from each distributor's public outage data, in one shape: the common fields every distributor has, plus `details` for each one's own. " +
      'Every outage says its distributor and its state. `kind` says whether an outage is unplanned or planned, and `status` whether the power is off now or ' +
      'the work is still to come; state, distributor, kind and status narrow the list. Every time is ISO 8601 UTC, whatever zone the distributor wrote it in. ' +
      '`sources` says per distributor how many outages it gave and when this server last read it; a distributor that is not being read has a null ' +
      '`updated_at` there, rather than looking as if it had no outages. Planned work is listed as far ahead as each distributor publishes it ' +
      '(Ausgrid: 30 days). Refreshed every one to fifteen minutes, depending on the distributor.',
    responses: { ...okJson(Outages, 'A GeoJSON FeatureCollection of outages'), ...VALIDATION_ERROR, ...GATED_ERRORS },
  }),
  queryValidator(Query),
  (c) => {
    const { outages, sources } = mergedOutages(c.req.valid('query'));
    const features = outages.map(toFeature);
    return geo(c, { type: 'FeatureCollection', features, count: features.length, sources } satisfies z.infer<typeof Outages>);
  },
);
