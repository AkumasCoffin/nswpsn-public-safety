/**
 * GET /api/v1/outages — every electricity distributor's outages in one feed.
 *
 * Endeavour Energy, Essential Energy and Ausgrid, read from the same
 * LiveStore snapshots the per-distributor routes serve (sources/endeavour.ts,
 * essential.ts, ausgrid.ts), in one shape. The per-distributor routes
 * (/api/endeavour/current, /api/essential/outages, …) stay /api/ routes for
 * this site's map and live page and the Discord bot, which draw each
 * distributor its own way; an API key reads this one (feed `outages`,
 * apiKeyScopes.ts).
 *
 * Every feature carries the same properties, snake_case like the rest of
 * /api/v1/: the distributor's id, the archived source it is stored under (so
 * /api/v1/history?source=…&source_id=… finds its past), whether it is
 * unplanned or planned and whether the power is off now or the work is still
 * to come, the area, customers affected, cause, start and expected
 * restoration and last update (UTC), the outage area's outline where the
 * distributor gives one, and `details` — the distributor's own extra fields,
 * under their own names, so nothing the per-distributor feed carries is lost.
 *
 * Times: Endeavour Energy writes them with a zone. Essential Energy writes
 * "02/11/2026 12:00:00" with none, which is NSW time, so it is read as Sydney
 * time; anything that cannot be read is left in `details` under its own name
 * and the common field is null.
 *
 * `sources` says, per distributor, how many outages it gave and when this
 * server last read it, so a distributor that is not being read (Ausgrid, for
 * now — its outage map service stopped answering) shows as not read rather
 * than as no outages.
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
import type { AusgridOutage, AusgridOutagesPayload } from '../sources/ausgrid.js';
import { sydneyUnixFromNaive } from '../lib/sydneyTime.js';
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

const DISTRIBUTORS = ['endeavour', 'essential', 'ausgrid'] as const;
type Distributor = (typeof DISTRIBUTORS)[number];
const KINDS = ['unplanned', 'planned'] as const;
type Kind = (typeof KINDS)[number];
const STATUSES = ['active', 'scheduled'] as const;
type Status = (typeof STATUSES)[number];

/** What each distributor's records are stored under (sources/*.ts archiveItems). */
const SOURCES = [
  'endeavour_current',
  'endeavour_planned',
  'essential_current',
  'essential_planned',
  'essential_future',
  'ausgrid',
] as const;
type Source = (typeof SOURCES)[number];

type Props = Record<string, unknown>;

/** One outage in the shared shape, before it becomes a feature. */
export interface Outage {
  id: string;
  source: Source;
  distributor: Distributor;
  kind: Kind;
  status: Status;
  area: string;
  customers_affected: number;
  cause: string;
  start_at: string | null;
  restore_at: string | null;
  updated_at: string | null;
  lat: number | null;
  lon: number | null;
  polygons: number[][][];
  details: Props;
}

// ---------------------------------------------------------------------------
// Times
// ---------------------------------------------------------------------------

/** ISO 8601 with a zone (Z or an offset): its instant is certain. */
const ISO_WITH_ZONE = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}(:?\d{2})?)$/;
/** ISO 8601 with no zone: a wall-clock time. */
const ISO_NAIVE = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?$/;
/** DD/MM/YYYY HH:mm[:ss], as Essential Energy writes it. */
const DMY_NAIVE = /^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2})(?::(\d{2}))?$/;

/** A Sydney wall-clock time as UTC ISO 8601, or null when the parts are not a time. */
function sydneyUtc(y: number, mo: number, d: number, hh: number, mi: number, ss: number): string | null {
  if (mo < 1 || mo > 12 || d < 1 || d > 31 || hh > 23 || mi > 59 || ss > 59) return null;
  const unix = sydneyUnixFromNaive(y, mo, d, hh, mi, ss);
  return unix === null ? null : new Date(unix * 1000).toISOString();
}

/**
 * A distributor's time as UTC ISO 8601: with a zone as written, without one
 * as Sydney time (NSW's, where all three distributors are). Null when it is
 * empty or not a time.
 */
export function outageUtc(v: unknown): string | null {
  if (typeof v !== 'string') return null;
  const s = v.trim();
  if (!s) return null;
  if (ISO_WITH_ZONE.test(s)) {
    const ms = Date.parse(s.replace(' ', 'T'));
    return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
  }
  let m = ISO_NAIVE.exec(s);
  if (m) return sydneyUtc(+m[1]!, +m[2]!, +m[3]!, +m[4]!, +m[5]!, +(m[6] ?? 0));
  m = DMY_NAIVE.exec(s);
  if (m) return sydneyUtc(+m[3]!, +m[2]!, +m[1]!, +m[4]!, +m[5]!, +(m[6] ?? 0));
  return null;
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

// Ausgrid gives every field twice (PascalCase and camelCase); the camelCase
// copies are left out of details, and the PascalCase ones mapped are too.
const AUSGRID_MAPPED = new Set([
  'OutageId', 'Suburb', 'CustomersAffected', 'OutageType', 'Cause', 'EndDateTime', 'Latitude', 'Longitude',
  'outageId', 'suburb', 'streetName', 'streets', 'postcode', 'customersAffected', 'outageType', 'cause',
  'startTime', 'estRestoration', 'endDateTime', 'latitude', 'longitude',
]);

export function fromAusgrid(o: AusgridOutage): Outage {
  const times = { StartTime: outageUtc(o.StartTime), EstRestoration: outageUtc(o.EstRestoration) };
  return {
    id: str(o.OutageId),
    source: 'ausgrid',
    distributor: 'ausgrid',
    kind: o.OutageType === 'Planned' ? 'planned' : 'unplanned',
    status: 'active',
    area: o.Suburb,
    customers_affected: o.CustomersAffected,
    cause: o.Cause,
    start_at: times.StartTime,
    restore_at: times.EstRestoration,
    updated_at: null,
    lat: num(o.Latitude),
    lon: num(o.Longitude),
    polygons: [],
    details: detailsOf(o, AUSGRID_MAPPED, times),
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
      return Array.isArray(markers) ? markers.map(fromAusgrid) : [];
    },
  },
];

const NAMES: Readonly<Record<Distributor, string>> = {
  endeavour: 'Endeavour Energy',
  essential: 'Essential Energy',
  ausgrid: 'Ausgrid',
};

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const TIME_NOTE = 'ISO 8601 UTC; null when the distributor gives none, or one that cannot be read (then details has it as written)';

const OutageProperties = z.object({
  id: z.string().describe("The distributor's own id for the outage; unique with source"),
  source: z
    .enum(SOURCES)
    .describe('What stored history files it under (/api/v1/history?source=…&source_id=<id> finds its past): endeavour_current for unplanned and endeavour_planned for planned Endeavour Energy outages; essential_current (unplanned), essential_planned (planned, under way) and essential_future (to come) for Essential Energy; ausgrid'),
  distributor: z.enum(DISTRIBUTORS),
  state: z.literal('NSW').describe('All three distributors are in NSW'),
  kind: z.enum(KINDS).describe('unplanned (a fault) or planned (scheduled work)'),
  status: z.enum(STATUSES).describe('active: the power is off now (every unplanned outage, and planned work under way); scheduled: planned work not yet started'),
  area: z.string().describe('The suburb or locality, as the distributor names it; empty when it gives none'),
  customers_affected: z.number().int().describe('Customers without power; 0 when not given'),
  cause: z.string().describe("The cause, as the distributor gives it (Endeavour Energy's planned work with none: \"Planned maintenance\"); empty when not given"),
  start_at: z.string().nullable().describe(`When the power went or goes off, ${TIME_NOTE}`),
  restore_at: z.string().nullable().describe(`When the power is expected back, ${TIME_NOTE}`),
  updated_at: z.string().nullable().describe(`When the distributor last updated the outage, ${TIME_NOTE}; Ausgrid gives none`),
  polygons: Rings.describe("The outage area's outline as outer rings of [longitude, latitude]; empty when the distributor gives none (Essential Energy gives one)"),
  details: z
    .record(z.string(), z.unknown())
    .describe("The distributor's own extra fields, under the per-distributor feed's names (Endeavour Energy: streets, postcode, status; Essential Energy: title)"),
});

const OutageFeature = feature(PointGeometry.nullable().describe('Where the distributor marks the outage; null when it gives no position'), OutageProperties);
type OutageFeatureT = z.infer<typeof OutageFeature>;

const DistributorStatus = z.object({
  distributor: z.enum(DISTRIBUTORS),
  name: z.string(),
  count: z.number().int().describe('Outages from this distributor in this answer, after the filters'),
  updated_at: z
    .string()
    .nullable()
    .describe("When this server last read all of the distributor's outage feeds (the earliest of their last reads), ISO 8601 UTC; null if one has not been read yet — as for Ausgrid, which is not being read for now"),
});

const Outages = featureCollection(OutageFeature, {
  sources: z.array(DistributorStatus).describe('One entry per distributor asked for, in this order'),
}).meta({ id: 'Outages' });

/** A comma list of allowed values, case-insensitive; empty means all. */
const oneOf = <T extends string>(values: readonly T[], what: string) =>
  z
    .string()
    .optional()
    .transform((v, ctx) => {
      const items = (v ?? '').split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
      const bad = items.filter((s) => !(values as readonly string[]).includes(s));
      if (bad.length) {
        ctx.addIssue({ code: 'custom', message: `unknown ${what}: ${bad.join(', ')} (one of ${values.join(', ')})` });
        return z.NEVER;
      }
      return items as T[];
    })
    .describe(`Comma-separated; any of ${values.join(', ')}. Leave out for all.`);

const Query = z.object({
  distributor: oneOf(DISTRIBUTORS, 'distributor'),
  kind: oneOf(KINDS, 'kind'),
  status: oneOf(STATUSES, 'status'),
});

function toFeature(o: Outage): OutageFeatureT {
  return {
    type: 'Feature',
    geometry: o.lat !== null && o.lon !== null ? { type: 'Point', coordinates: [o.lon, o.lat] } : null,
    properties: {
      id: o.id,
      source: o.source,
      distributor: o.distributor,
      state: 'NSW',
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
  distributor?: readonly string[] | undefined;
  kind?: readonly string[] | undefined;
  status?: readonly string[] | undefined;
}

/**
 * Every distributor's outages in the shared shape, with what each one gave.
 * Reads the LiveStore only; an outage two feeds both list is in it twice.
 * `read` says whether any feed has been read at all. /api/outages answers with
 * it, and the per-state stats (api/stats.ts) count from it.
 */
export function mergedOutages(q: OutagesFilter = {}): {
  outages: Outage[];
  sources: Array<z.infer<typeof DistributorStatus>>;
  read: boolean;
} {
  const distributors: readonly string[] = q.distributor?.length ? q.distributor : DISTRIBUTORS;
  const kinds = new Set<string>(q.kind ?? []);
  const statuses = new Set<string>(q.status ?? []);
  const outages: Outage[] = [];
  const sources: Array<z.infer<typeof DistributorStatus>> = [];
  let read = false;
  for (const distributor of DISTRIBUTORS) {
    if (!distributors.includes(distributor)) continue;
    let count = 0;
    let oldest: number | null = null;
    let unread = false;
    for (const feed of FEEDS) {
      if (feed.distributor !== distributor) continue;
      const ts = liveStore.get(feed.store)?.ts;
      if (!ts) unread = true;
      else {
        read = true;
        oldest = oldest === null ? ts : Math.min(oldest, ts);
      }
      for (const o of feed.read()) {
        if (kinds.size && !kinds.has(o.kind)) continue;
        if (statuses.size && !statuses.has(o.status)) continue;
        outages.push(o);
        count++;
      }
    }
    sources.push({
      distributor,
      name: NAMES[distributor],
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
      "Electricity outages on Endeavour Energy's, Essential Energy's and Ausgrid's networks — unplanned and planned, now and to come — " +
      "from each distributor's public outage data, in one shape: the common fields every distributor has, plus `details` for each one's own. " +
      '`kind` says whether an outage is unplanned or planned, and `status` whether the power is off now or the work is still to come; ' +
      'distributor, kind and status narrow the list. Times are UTC; Essential Energy writes its own without a zone, so they are read as ' +
      "Sydney time. `sources` says per distributor how many outages it gave and when this server last read it: Ausgrid's outage map " +
      'service has stopped answering, so it is not being read for now and gives none. Refreshed every one to five minutes.',
    responses: { ...okJson(Outages, 'A GeoJSON FeatureCollection of outages'), ...VALIDATION_ERROR, ...GATED_ERRORS },
  }),
  queryValidator(Query),
  (c) => {
    const { outages, sources } = mergedOutages(c.req.valid('query'));
    const features = outages.map(toFeature);
    return geo(c, { type: 'FeatureCollection', features, count: features.length, sources } satisfies z.infer<typeof Outages>);
  },
);
