/**
 * Live public-transport proxy (AnyTrip upstream).
 *
 *   GET /api/transport/vehicles?minLat&maxLat&minLon&maxLon&feeds=bs,st,...
 *   GET /api/transport/stops?minLat&maxLat&minLon&maxLon&modes=metro,...
 *
 * AnyTrip's (unofficial, keyless) API is viewport-bbox-scoped — Sydney
 * alone runs thousands of buses, so unlike the ADS-B source this is an
 * ON-DEMAND proxy, not a registered poller. Proxied because the API
 * sends no CORS headers for our origin, and because the raw payload is
 * enormous (~100 KB for 19 vehicles) — normalization shrinks it ~50×.
 *
 * Five AnyTrip regions are served: au2 (NSW), au3 (Victoria), au4
 * (SEQ/Queensland), au5 (SA/Adelaide) and au9 (ACT). Viewport
 * endpoints take ?region= (default au2); id-addressed endpoints
 * (shape/trip/departures) derive the region from the id's own au№:
 * prefix. Every cache key carries the region — coverage areas overlap
 * (Tweed Heads, the Murray border band, the whole ACT inside au2's
 * envelope), so a bbox alone is not a unique key.
 *
 * Bbox handling: coords are clamped into the region's coverage bounds
 * (rejecting would break padded coastal/border viewports), then
 * snapped OUTWARD to a 0.01° (~1.1 km) grid. The snapped bbox is both
 * the cache key and the upstream query, so the cached result always
 * covers the requested area and nearby pans re-hit the same cache
 * cell.
 *
 * Deliberately NOT in CACHEABLE_PATHS: that middleware matches the
 * pathname ignoring query strings, so a CDN would cross-serve one
 * viewport's vehicles to every viewport.
 *
 * All seven routes are also served at /api/v1/ (server.ts mirrors them):
 * the same handlers, wrapped in `data` there except /lines, which is
 * GeoJSON (api/v1/respond.ts). The query schemas below only describe the
 * parameters — the handlers keep their own parsing, so the old path
 * answers every input exactly as it always did.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute, resolver } from 'hono-openapi';
import { fetchJson } from '../sources/shared/http.js';
import { SwrCache } from '../services/swrCache.js';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import {
  fetchTfnswPositions,
  applyTfnswPositions,
  fetchTfnswAlerts,
  tfnswConfigured,
  tfnswPositionsEnabled,
  type TfnswAlert,
} from '../sources/tfnsw.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, geo, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  ErrorBody,
  GATED_ERRORS,
  okJson,
  queryValidator,
  VALIDATION_ERROR,
  type SameShape,
} from './v1/schemas.js';

export const transportRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_TRANSIT = scoped({ source: 'transport', type: 'transit', window: 'live' });

const ANYTRIP_HOST = 'https://api-cf-oc2.anytrip.com.au/api/v3/region';

interface RegionBounds {
  minLat: number;
  maxLat: number;
  minLon: number;
  maxLon: number;
}

interface TransportRegion {
  base: string;
  bounds: RegionBounds;
  /** Vehicles: short feed codes → upstream feed ids. au2's feeds are
   *  per-mode, so they double as the vehicle filter there. */
  vehicleFeeds: Record<string, string>;
  /** Modes-filtered regions (au3/au4/au5/au9): the feed(s) carry every
   *  mode, so vehicles filter by modes= instead; null means feeds= is
   *  the whole selector (au2). An empty vehicleFeeds map (au3/au5/au9)
   *  sends modes= alone — the AnyTrip app itself queries them that
   *  way. */
  vehicleModes: Record<string, string> | null;
  stopModes: Record<string, string>;
}

// `sp` (school/special services) reports mode au2:buses. No region
// offers 'buses' stops — tens of thousands of bus stops would blow
// the upstream limit=500 and truncate arbitrarily in dense areas.
const REGIONS: Record<string, TransportRegion> = {
  au2: {
    base: `${ANYTRIP_HOST}/au2`,
    bounds: { minLat: -38, maxLat: -28, minLon: 140, maxLon: 154 },
    vehicleFeeds: {
      bs: 'au2:bs',
      st: 'au2:st',
      mt: 'au2:mt',
      nt: 'au2:nt',
      fr: 'au2:fr',
      lr: 'au2:lr',
      sp: 'au2:sp',
    },
    vehicleModes: null,
    stopModes: {
      metro: 'au2:metro',
      sydneytrains: 'au2:sydneytrains',
      nswtrains: 'au2:nswtrains',
      ferries: 'au2:ferries',
      lightrail: 'au2:lightrail',
    },
  },
  au4: {
    base: `${ANYTRIP_HOST}/au4`,
    // SEQ envelope: Gympie fringe north, Toowoomba west, and a
    // DELIBERATE overlap with NSW's −28 band down to Tweed Heads —
    // border viewports legitimately query both regions.
    bounds: { minLat: -29.5, maxLat: -24.5, minLon: 150.0, maxLon: 154.5 },
    vehicleFeeds: { se: 'au4:se' },
    vehicleModes: {
      trains: 'au4:trains',
      ferries: 'au4:ferries',
      lightrail: 'au4:lightrail',
      buses: 'au4:buses',
    },
    stopModes: {
      trains: 'au4:trains',
      ferries: 'au4:ferries',
      lightrail: 'au4:lightrail',
    },
  },
  au3: {
    base: `${ANYTRIP_HOST}/au3`,
    // Victoria, plus the Murray border band — V/Line reaches Albury
    // inside au2's envelope, so border viewports query both regions.
    bounds: { minLat: -39.3, maxLat: -33.8, minLon: 140.8, maxLon: 150.2 },
    vehicleFeeds: {},
    vehicleModes: {
      metrotrains: 'au3:metrotrains',
      vlinetrains: 'au3:vlinetrains',
      tram: 'au3:tram',
      buses: 'au3:buses',
    },
    stopModes: {
      metrotrains: 'au3:metrotrains',
      vlinetrains: 'au3:vlinetrains',
      tram: 'au3:tram',
    },
  },
  au5: {
    base: `${ANYTRIP_HOST}/au5`,
    // Adelaide Metro's reach: Gawler to Victor Harbor fringe.
    bounds: { minLat: -36.3, maxLat: -33.7, minLon: 136.7, maxLon: 140.0 },
    vehicleFeeds: {},
    vehicleModes: {
      trains: 'au5:trains',
      lightrail: 'au5:lightrail',
      buses: 'au5:buses',
      schoolbuses: 'au5:schoolbuses',
    },
    stopModes: {
      trains: 'au5:trains',
      lightrail: 'au5:lightrail',
    },
  },
  au9: {
    base: `${ANYTRIP_HOST}/au9`,
    // ACT + surrounds (Transport Canberra buses run to Bungendore).
    // Sits ENTIRELY inside au2's envelope — Canberra viewports query
    // both regions, which is correct: NSW coaches pass through.
    bounds: { minLat: -36.0, maxLat: -34.6, minLon: 148.0, maxLon: 149.8 },
    vehicleFeeds: {},
    vehicleModes: {
      lightrail: 'au9:lightrail',
      buses: 'au9:buses',
      schoolbuses: 'au9:schoolbuses',
    },
    stopModes: { lightrail: 'au9:lightrail' },
  },
};

function parseRegion(
  raw: string | undefined,
): { id: string; region: TransportRegion } | string {
  const id = (raw ?? 'au2').trim().toLowerCase();
  const region = REGIONS[id];
  return region ? { id, region } : `unknown region: ${id}`;
}

/** Region config for a prefixed AnyTrip id (shape/trip/stop ids). */
function regionForId(id: string): TransportRegion | null {
  return REGIONS[id.split(':')[0] ?? ''] ?? null;
}
const MAX_SPAN_DEG = 2.5; // per axis — a zoom-11 viewport is ~0.35°
const GRID = 0.01; // snap-outward grid (degrees)
const UPSTREAM_TIMEOUT_MS = 12_000;
// Params the AnyTrip web app itself sends — keep our traffic ordinary.
const OTR_FILTER = 300;
const SPEED_FILTER = 15;

// 5s, deliberately UNDER the client's 10s tick: at 10s the two clocks
// beat against each other and consecutive ticks often served the
// IDENTICAL snapshot (marker freezes 10s, then sprints 20s of travel).
// At 5s each client tick lands past the fresh window, triggering a
// background SWR refresh whose result feeds the NEXT tick.
const VEH_FRESH_MS = 5_000;
const VEH_STALE_MS = 30_000;
// Drop vehicles whose last position report is older than this.
const MAX_VEHICLE_AGE_SEC = 600;
const STOPS_FRESH_MS = 3_600_000;
const STOPS_STALE_MS = 86_400_000;
const MAX_VEHICLES = 1500;

export type TransportMode =
  | 'buses'
  | 'sydneytrains'
  | 'metro'
  | 'nswtrains'
  | 'ferries'
  | 'lightrail'
  | 'other';

export interface TransportVehicle {
  id: string;
  lat: number;
  lon: number;
  bearing: number | null;
  speedKmh: number | null;
  mode: TransportMode;
  route: {
    id: string | null;
    name: string | null;
    longName: string | null;
    color: string | null; // '#'-prefixed 6-hex or null
    textColor: string | null;
  };
  headsign: string | null;
  headsignSub: string | null;
  agency: string | null;
  occupancy: number | null; // 0-6, see AnyTrip occupancyDescription
  /** GTFS tri-state — null means unknown, NOT "no". */
  wheelchair: boolean | null;
  aircon: boolean | null;
  model: string | null;
  ageSec: number | null;
  /** Upstream lastPosition.time (epoch seconds) — when the position was
   *  REPORTED. Unlike ageSec (age at snapshot build time, which a
   *  stale-served SWR snapshot understates), this is absolute: the
   *  client uses it as a per-vehicle monotonic ratchet so an older
   *  cache cell can never rewind a marker. Null when the feed omits it. */
  posTime: number | null;
  tripId: string | null;
  /** GTFS shape id — resolves to the route track via /api/transport/shape. */
  shapeId: string | null;
  /** Vehicle distance along path (metres from the shape start), from
   *  AnyTrip's lastPosition. Lets the client place the vehicle at its
   *  exact point ON the shape instead of snapping lat/lon (which lands on
   *  the wrong parallel track). Null when the feed omits it. */
  vdap: number | null;
  /** Trip-instance coordinates for /api/transport/trip lookups. */
  startDate: string | null; // YYYYMMDD
  instanceNumber: number | null;
}

export interface TransportVehiclesSnapshot {
  vehicles: TransportVehicle[];
  count: number;
  /** NETWORK-WIDE active-vehicle tally per mode, from the TfNSW GTFS-R
   *  feeds (which are state-wide, unlike the bbox-scoped vehicle list).
   *  Absent when TfNSW is unconfigured. Drives the frontend's filter
   *  pill counts so they show ALL vehicles, not just the viewport. */
  network_counts?: Record<string, number>;
  fetched_at: string;
}

export interface TransportStop {
  id: string;
  name: string;
  lat: number;
  lon: number;
  modes: string[];
  locality: string | null;
  wheelchair: boolean | null;
  accessibility: string[];
}

export interface TransportStopsSnapshot {
  stops: TransportStop[];
  count: number;
  fetched_at: string;
}

// ---------------------------------------------------------------------
// Raw upstream shapes (only the fields we read; everything optional —
// the API is unofficial and may drift).
interface RawVehicleEntry {
  tripInstance?: {
    shapeId?: string;
    startDate?: string;
    instanceNumber?: number;
    trip?: {
      id?: string;
      /** Realtime trip id — the one tripInstance/... paths accept. For
       *  buses it equals `id`; for trains it's the short form. */
      rtTripId?: string;
      shapeId?: string;
      headsign?: { headline?: string; subtitle?: string | null };
      wheelchair?: boolean | number;
      route?: {
        id?: string;
        name?: string;
        longName?: string;
        color?: string;
        textColor?: string;
        mode?: string;
        agency?: { name?: string };
      };
    };
  };
  vehicleInstance?: {
    id?: string;
    lastPosition?: {
      time?: number; // epoch seconds
      bearing?: number;
      speed?: number; // m/s
      vdap?: number; // vehicle distance along path, metres from shape start
      occupancy?: number[];
      vehicleOccupancy?: number;
      coordinates?: { lat?: number; lon?: number };
    };
    wheelchair?: number | boolean;
    aircon?: boolean;
    vehicleModel?: string;
  };
}
interface RawVehiclesResponse {
  response?: { vehicles?: RawVehicleEntry[] };
}
interface RawStopEntry {
  stop?: {
    id?: string;
    fullName?: string;
    name?: { station_name?: string };
    coordinates?: { lat?: number; lon?: number };
    modes?: string[];
    locality?: string;
    wheelchair?: boolean;
    facilities?: { accessibility?: string[] };
  };
}
interface RawStopsResponse {
  response?: { stops?: RawStopEntry[] };
}

// ---------------------------------------------------------------------

interface Bbox {
  minLat: number;
  maxLat: number;
  minLon: number;
  maxLon: number;
}

/** Parse, clamp into NSW, validate span, snap outward to the grid.
 *  Returns the snapped bbox or an error string. */
export function parseBbox(
  q: Record<string, string | undefined>,
  bounds: RegionBounds = REGIONS['au2']!.bounds,
): Bbox | string {
  const minLat = Number(q['minLat']);
  const maxLat = Number(q['maxLat']);
  const minLon = Number(q['minLon']);
  const maxLon = Number(q['maxLon']);
  if (![minLat, maxLat, minLon, maxLon].every(Number.isFinite)) {
    return 'invalid bbox';
  }
  // Clamp — not reject. The frontend pads its viewport, so legitimate
  // coastal/border views poke past the region's envelope.
  const cMinLat = Math.max(minLat, bounds.minLat);
  const cMaxLat = Math.min(maxLat, bounds.maxLat);
  const cMinLon = Math.max(minLon, bounds.minLon);
  const cMaxLon = Math.min(maxLon, bounds.maxLon);
  // Catches inverted boxes AND boxes entirely outside the region
  // (which clamp to zero/negative span).
  if (cMaxLat <= cMinLat || cMaxLon <= cMinLon) return 'bbox outside coverage';
  if (cMaxLat - cMinLat > MAX_SPAN_DEG || cMaxLon - cMinLon > MAX_SPAN_DEG) {
    return 'bbox too large';
  }
  const snap = (v: number, up: boolean): number => {
    const s = up ? Math.ceil(v / GRID) * GRID : Math.floor(v / GRID) * GRID;
    return Math.round(s * 100) / 100; // kill float dust; GRID is 0.01
  };
  return {
    minLat: snap(cMinLat, false),
    maxLat: snap(cMaxLat, true),
    minLon: snap(cMinLon, false),
    maxLon: snap(cMaxLon, true),
  };
}

/** Comma list → validated, deduped, sorted keys of `table`.
 *  Absent/empty → all keys. Unknown entry → error string. */
function parseListParam(
  raw: string | undefined,
  table: Record<string, string>,
  label: string,
): string[] | string {
  if (!raw || !raw.trim()) return Object.keys(table).sort();
  const keys = Array.from(
    new Set(
      raw
        .split(',')
        .map((s) => s.trim().toLowerCase())
        .filter(Boolean),
    ),
  ).sort();
  for (const k of keys) {
    if (!(k in table)) return `unknown ${label}: ${k}`;
  }
  return keys;
}

function normColor(raw: string | undefined): string | null {
  return raw && /^[0-9a-fA-F]{6}$/.test(raw) ? `#${raw.toUpperCase()}` : null;
}

/** GTFS-ish tri-state → boolean|null. Accepts 0/1/2 and booleans. */
function triState(v: unknown): boolean | null {
  if (v === true || v === 1) return true;
  if (v === 2) return false;
  if (v === false) return false;
  return null;
}

/** Strip the region prefix and collapse regional names onto the shared
 *  vocabulary, so the frontend pill set never grows: every network's
 *  suburban heavy rail rides the Trains pill ('sydneytrains'), V/Line
 *  rides the regional-trains pill ('nswtrains'), Melbourne trams are
 *  light rail, and school buses ride with Buses (as NSW's `sp` feed
 *  already does by reporting mode au2:buses). */
const MODE_SYNONYMS: Record<string, string> = {
  trains: 'sydneytrains', // au4 QR / au5 Adelaide Metro
  metrotrains: 'sydneytrains', // au3 Metro Trains Melbourne
  vlinetrains: 'nswtrains', // au3 V/Line
  tram: 'lightrail', // au3 Yarra Trams
  schoolbuses: 'buses', // au5/au9
};
function canonicalModeName(raw: string): string {
  const m = raw.replace(/^au\d+:/, '');
  return MODE_SYNONYMS[m] ?? m;
}

function normMode(raw: string | undefined): TransportMode {
  const m = canonicalModeName(raw ?? '');
  switch (m) {
    case 'buses':
    case 'sydneytrains':
    case 'metro':
    case 'nswtrains':
    case 'ferries':
    case 'lightrail':
      return m;
    default:
      return 'other';
  }
}

export function normalizeVehicles(raw: RawVehiclesResponse): TransportVehicle[] {
  const out = new Map<string, TransportVehicle>();
  const nowSec = Date.now() / 1000;
  for (const entry of raw.response?.vehicles ?? []) {
    const vi = entry.vehicleInstance;
    const pos = vi?.lastPosition;
    const lat = pos?.coordinates?.lat;
    const lon = pos?.coordinates?.lon;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    const trip = entry.tripInstance?.trip;
    const route = trip?.route;
    const id = String(vi?.id ?? trip?.id ?? '');
    if (!id) continue;
    const occRaw = Array.isArray(pos?.occupancy)
      ? pos.occupancy[0]
      : pos?.vehicleOccupancy;
    const occ =
      typeof occRaw === 'number' && Number.isInteger(occRaw) && occRaw >= 0 && occRaw <= 6
        ? occRaw
        : null;
    // Parked/ghost vehicles: OCCP track-occupation entries and stabled
    // sets report positions that are an hour old — a vehicle whose
    // last report is older than this is not usefully "live". Trains
    // dwelling at platforms keep reporting and stay well under it.
    if (
      typeof pos?.time === 'number' && pos.time > 0 &&
      nowSec - pos.time > MAX_VEHICLE_AGE_SEC
    ) {
      continue;
    }
    const speed = pos?.speed;
    const bearing = pos?.bearing;
    out.set(id, {
      id,
      lat: lat as number,
      lon: lon as number,
      bearing: Number.isFinite(bearing) ? (bearing as number) : null,
      speedKmh:
        Number.isFinite(speed) && (speed as number) >= 0
          ? Math.round((speed as number) * 36) / 10
          : null,
      mode: normMode(route?.mode),
      route: {
        id: route?.id ?? null,
        name: route?.name ?? null,
        longName: route?.longName ?? null,
        color: normColor(route?.color),
        textColor: normColor(route?.textColor),
      },
      headsign: trip?.headsign?.headline ?? null,
      headsignSub: trip?.headsign?.subtitle ?? null,
      agency: route?.agency?.name ?? null,
      occupancy: occ,
      wheelchair: triState(vi?.wheelchair ?? trip?.wheelchair),
      aircon: triState(vi?.aircon),
      model: vi?.vehicleModel ?? null,
      ageSec:
        typeof pos?.time === 'number' && pos.time > 0
          ? Math.max(0, Math.round(nowSec - pos.time))
          : null,
      posTime: typeof pos?.time === 'number' && pos.time > 0 ? pos.time : null,
      tripId: trip?.rtTripId ?? trip?.id ?? null,
      shapeId: entry.tripInstance?.shapeId ?? trip?.shapeId ?? null,
      vdap: Number.isFinite(pos?.vdap) ? (pos!.vdap as number) : null,
      startDate: entry.tripInstance?.startDate ?? null,
      instanceNumber:
        typeof entry.tripInstance?.instanceNumber === 'number'
          ? entry.tripInstance.instanceNumber
          : null,
    });
    if (out.size >= MAX_VEHICLES) break;
  }
  return Array.from(out.values());
}

export function normalizeStops(raw: RawStopsResponse): TransportStop[] {
  const out: TransportStop[] = [];
  for (const entry of raw.response?.stops ?? []) {
    const s = entry.stop;
    const lat = s?.coordinates?.lat;
    const lon = s?.coordinates?.lon;
    if (!s?.id || !Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    out.push({
      id: s.id,
      name: s.fullName ?? s.name?.station_name ?? s.id,
      lat: lat as number,
      lon: lon as number,
      modes: (s.modes ?? []).map((m) => canonicalModeName(m)),
      locality: s.locality ?? null,
      wheelchair: typeof s.wheelchair === 'boolean' ? s.wheelchair : null,
      accessibility: s.facilities?.accessibility ?? [],
    });
  }
  return out;
}

// ---------------------------------------------------------------------

// Entry counts here are deliberately small because the ENTRIES are large, not
// because the key space is. A vehicles entry holds up to MAX_VEHICLES (1500)
// objects with a nested route — roughly a megabyte — and the key is a snapped
// viewport, so the key space is effectively every place anyone has panned to.
// At 500 entries that is ~500MB resident, which was a large part of the heap
// the process was dying on. 60 covers the viewports actually in play at any
// moment (SwrCache also ages entries out now), and a miss just refetches.
const vehiclesCache = new SwrCache<TransportVehiclesSnapshot>(60);
const stopsCache = new SwrCache<TransportStopsSnapshot>(60);
// Shapes are single encoded polyline strings — small, and worth keeping more of.
const shapeCache = new SwrCache<TransportShapeSnapshot>(400);

export interface TransportShapeSnapshot {
  /** Google encoded polyline (precision 5) — decoded client-side. */
  id: string;
  enc: string | null;
}

/** TEST-ONLY: wipe caches between unit tests. */
export function _resetTransportCacheForTests(): void {
  vehiclesCache.clear();
  stopsCache.clear();
  shapeCache.clear();
  tripCache.clear();
  depCache.clear();
  linesCache.clear();
}

function bboxKey(b: Bbox): string {
  return `${b.minLat},${b.maxLat},${b.minLon},${b.maxLon}`;
}
function bboxParams(b: Bbox): string {
  return `minLat=${b.minLat}&maxLat=${b.maxLat}&minLon=${b.minLon}&maxLon=${b.maxLon}`;
}

// ---------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts).

/** An error answer, in the shared v1 shape (api/v1/schemas.ts writes its own the same way). */
const errorResponse = (description: string) => ({
  description,
  content: { 'application/json': { schema: resolver(ErrorBody) } },
});
const ANYTRIP_UNAVAILABLE = {
  502: errorResponse('AnyTrip did not answer usably and there was nothing recent to serve instead (service_unavailable)'),
};
const TRANSPORT_SWITCHED_OFF = {
  404: errorResponse('Transport data is switched off on this server (not_found)'),
};
const BAD_ID = (what: string) => ({
  400: errorResponse(`${what} is not valid, or its region prefix is not one this API serves (validation_failed)`),
});
const UPSTREAM_MESSAGE = 'Transport data could not be read from AnyTrip just now.';
const SWITCHED_OFF_MESSAGE = 'Transport data is switched off on this server.';

const DISABLED = z
  .literal(true)
  .optional()
  .describe('Present only when transport data is switched off on this server; the answer is then empty');

const TransportModeSchema = z
  .enum(['buses', 'sydneytrains', 'metro', 'nswtrains', 'ferries', 'lightrail', 'other'])
  .meta({
    id: 'TransportMode',
    description:
      'One vocabulary for every region: suburban heavy rail anywhere is sydneytrains (Queensland Rail, Adelaide Metro and Metro Trains Melbourne too), regional trains are nswtrains (V/Line too), trams are lightrail and school buses are buses; anything not recognised is other',
  });
assertShape<SameShape<z.infer<typeof TransportModeSchema>, TransportMode>>();

const ROUTE_COLOR = '#RRGGBB (upper-case hex); null when the feed has no valid colour';
const OCCUPANCY_SCALE =
  "AnyTrip's 0–6 crowding scale: 0–1 space available, 2 limited space, 3–4 standing room only, 5–6 no space";

const TransportVehicleSchema = z
  .object({
    id: z.string(),
    lat: z.number().describe('Latitude of the last reported position'),
    lon: z.number().describe('Longitude of the last reported position'),
    bearing: z.number().nullable().describe('Heading in degrees, as the feed reports it'),
    speedKmh: z.number().nullable().describe('Speed in km/h, to one decimal'),
    mode: TransportModeSchema,
    route: z.object({
      id: z.string().nullable(),
      name: z.string().nullable().describe('Short name, e.g. "SW1"'),
      longName: z.string().nullable().describe('e.g. "Bankstown to Sydenham"'),
      color: z.string().nullable().describe(ROUTE_COLOR),
      textColor: z.string().nullable().describe(ROUTE_COLOR),
    }),
    headsign: z.string().nullable().describe('Where it is going, e.g. "Sydenham"'),
    headsignSub: z.string().nullable().describe('The second line of the headsign, e.g. "via Marrickville"'),
    agency: z.string().nullable().describe('The operator'),
    occupancy: z
      .number()
      .int()
      .min(0)
      .max(6)
      .nullable()
      .describe(`${OCCUPANCY_SCALE} (the first carriage where the feed reports each); null when not reported`),
    wheelchair: z.boolean().nullable().describe('Wheelchair accessible; null means unknown, not no'),
    aircon: z.boolean().nullable().describe('Air-conditioned; null means unknown, not no'),
    model: z.string().nullable(),
    ageSec: z
      .number()
      .int()
      .nullable()
      .describe('Seconds between the position report and when this answer was built, which a cached answer understates; posTime is the absolute time'),
    posTime: z.number().nullable().describe('When the position was reported, unix seconds'),
    tripId: z
      .string()
      .nullable()
      .describe('The trip it is running; with startDate and instanceNumber it addresses /api/v1/transport/trip/{date}/{tripId}/{instance}'),
    shapeId: z.string().nullable().describe("The trip's route track, for /api/v1/transport/shape/{id}"),
    vdap: z.number().nullable().describe('How far along the shape it is, in metres from the start'),
    startDate: z.string().nullable().describe("The trip's service date, YYYYMMDD"),
    instanceNumber: z.number().nullable(),
  })
  .meta({ id: 'TransportVehicle' });
assertShape<SameShape<z.infer<typeof TransportVehicleSchema>, TransportVehicle>>();

const FETCHED_AT = 'When AnyTrip was read, ISO 8601 (when switched off, the time of this answer)';

const TransportVehiclesBase = z.object({
  vehicles: z.array(TransportVehicleSchema),
  count: z.number().int().describe('Number of vehicles'),
  network_counts: z
    .record(z.string(), z.number().int())
    .optional()
    .describe('NSW only, and only while this server joins TfNSW positions: vehicles per mode across the whole TfNSW network for the feeds asked for, not just this area'),
  fetched_at: z.string().describe(FETCHED_AT),
});
assertShape<SameShape<z.infer<typeof TransportVehiclesBase>, TransportVehiclesSnapshot>>();
const TransportVehiclesSchema = TransportVehiclesBase.extend({ disabled: DISABLED }).meta({ id: 'TransportVehicles' });

/**
 * A parameter the handler needs, documented as required but never refused
 * by the query schema: the handler refuses it itself, in its own order and
 * words — which is what the old path has always answered. Nothing reads the
 * caught value; the handlers parse c.req.query() as they always have.
 */
const neededByHandler = <S extends z.ZodType>(schema: S) => schema.catch(undefined as never);

const RegionParam = z
  .enum(Object.keys(REGIONS).sort() as [string, ...string[]])
  .optional()
  .catch(undefined)
  .describe('au2 NSW (the default), au3 Victoria, au4 South East Queensland, au5 Adelaide, au9 the ACT; any other value is refused');
const BboxParams = {
  minLat: neededByHandler(z.coerce.number()).describe('Southern edge of the area, degrees latitude'),
  maxLat: neededByHandler(z.coerce.number()).describe('Northern edge of the area, degrees latitude'),
  minLon: neededByHandler(z.coerce.number()).describe('Western edge of the area, degrees longitude'),
  maxLon: neededByHandler(z.coerce.number()).describe('Eastern edge of the area, degrees longitude'),
};
const BBOX_RULES =
  "The area is clamped to the region's coverage and widened outward to a 0.01° grid; after clamping it may span at most 2.5° each way, and an area outside the region is refused.";

const VehiclesQuery = z.object({
  region: RegionParam,
  ...BboxParams,
  feeds: z
    .string()
    .optional()
    .catch(undefined)
    .describe(
      'au2 only: comma-separated feeds — st (sydneytrains), mt (metro), nt (nswtrains), fr (ferries), lr (lightrail), bs (buses), sp (school and special services, reported as buses). All of them when absent; an unknown one is refused',
    ),
  modes: z
    .string()
    .optional()
    .catch(undefined)
    .describe(
      'Every region but au2: comma-separated modes — au3 metrotrains, vlinetrains, tram, buses; au4 trains, ferries, lightrail, buses; au5 trains, lightrail, buses, schoolbuses; au9 lightrail, buses, schoolbuses. All of them when absent; an unknown one is refused',
    ),
});

transportRouter.get(
  '/api/transport/vehicles',
  SCOPE_TRANSIT,
  describeRoute({
    operationId: 'getTransportVehicles',
    tags: ['Transport'],
    summary: 'Public transport vehicles in an area',
    description: `Live positions of trains, metro, light rail, trams, buses and ferries inside an area, for one region, from AnyTrip. ${BBOX_RULES} Vehicles whose last report is more than ten minutes old are left out, and at most 1,500 are returned. Each area's answer is cached for 5 seconds and can be up to 30 seconds old.`,
    responses: {
      ...okJson(envelope(TransportVehiclesSchema), 'The vehicles in the area'),
      ...GATED_ERRORS,
      ...VALIDATION_ERROR,
      ...ANYTRIP_UNAVAILABLE,
    },
  }),
  queryValidator(VehiclesQuery),
  async (c) => {
    const empty: TransportVehiclesSnapshot = {
      vehicles: [],
      count: 0,
      fetched_at: new Date().toISOString(),
    };
    if (config.TRANSPORT_DISABLED) {
      return ok(c, { ...empty, disabled: true } satisfies z.infer<typeof TransportVehiclesSchema>);
    }

    const parsedRegion = parseRegion(c.req.query('region'));
    if (typeof parsedRegion === 'string') {
      return fail(c, 400, 'validation_failed', parsedRegion, { error: parsedRegion });
    }
    const { id: regionId, region } = parsedRegion;
    const bbox = parseBbox({
      minLat: c.req.query('minLat'),
      maxLat: c.req.query('maxLat'),
      minLon: c.req.query('minLon'),
      maxLon: c.req.query('maxLon'),
    }, region.bounds);
    if (typeof bbox === 'string') return fail(c, 400, 'validation_failed', bbox, { error: bbox });
    // Selector: au2 filters upstream by its per-mode feeds; the other
    // regions' feeds carry every mode, so modes= is the filter there
    // (au4 also names its `se` feed; au3/au5/au9 send modes alone).
    let selector: string[];
    let upstreamFilter: string;
    if (region.vehicleModes) {
      const modes = parseListParam(c.req.query('modes'), region.vehicleModes, 'mode');
      if (typeof modes === 'string') return fail(c, 400, 'validation_failed', modes, { error: modes });
      selector = modes;
      const feedList = Object.values(region.vehicleFeeds).join(',');
      const modeList = modes.map((m) => region.vehicleModes![m]).join(',');
      upstreamFilter =
        (feedList ? `feeds=${encodeURIComponent(feedList)}&` : '') +
        `modes=${encodeURIComponent(modeList)}`;
    } else {
      const feeds = parseListParam(c.req.query('feeds'), region.vehicleFeeds, 'feed');
      if (typeof feeds === 'string') return fail(c, 400, 'validation_failed', feeds, { error: feeds });
      selector = feeds;
      const feedList = feeds.map((f) => region.vehicleFeeds[f]).join(',');
      upstreamFilter = `feeds=${encodeURIComponent(feedList)}`;
    }

    // Region is ALWAYS in the key: overlapping coverage (Tweed Heads,
    // the Murray band, the ACT) produces identical snapped bboxes for
    // more than one region.
    const key = `v|${regionId}|${bboxKey(bbox)}|${selector.join(',')}`;
    try {
      const { value } = await vehiclesCache.get(
        key,
        async () => {
          const url =
            `${region.base}/vehicles?${upstreamFilter}` +
            `&${bboxParams(bbox)}&otrFilter=${OTR_FILTER}&speedFilter=${SPEED_FILTER}`;
          // Positions are AnyTrip-only by default: AnyTrip interpolates its
          // own (smooth, self-consistent) positions from the same TfNSW
          // feed, so pure AnyTrip beats layering the raw TfNSW frame on top
          // (which jumped/mis-tracked trains and hit TfNSW's rate limit).
          // The TfNSW position join is off unless TFNSW_POSITIONS_DISABLED
          // is set false; when off we don't even fetch the position feeds.
          // TfNSW is NSW-only — never join (or even fetch) it for au4.
          const wantTfnsw = regionId === 'au2' && tfnswPositionsEnabled();
          const [raw, tfnsw] = await Promise.all([
            fetchJson<RawVehiclesResponse>(url, { timeoutMs: UPSTREAM_TIMEOUT_MS }),
            wantTfnsw ? fetchTfnswPositions(selector) : Promise.resolve([]),
          ]);
          let vehicles = normalizeVehicles(raw);
          const vehiclesBeforeJoin = vehicles.length;
          let networkCounts: Record<string, number> | undefined;
          if (wantTfnsw && tfnsw.length) {
            const joined = applyTfnswPositions(vehicles, tfnsw, bbox, MAX_VEHICLES);
            vehicles = joined.vehicles;
            networkCounts = {};
            for (const p of tfnsw) {
              networkCounts[p.mode] = (networkCounts[p.mode] ?? 0) + 1;
            }
            log.debug(
              {
                matched: joined.matched,
                byTrip: joined.byTrip,
                byVeh: joined.byVeh,
                added: joined.added,
                anytrip: vehiclesBeforeJoin,
                tfnsw: tfnsw.length,
              },
              'transport: tfnsw position join',
            );
          }
          return {
            vehicles,
            count: vehicles.length,
            ...(networkCounts ? { network_counts: networkCounts } : {}),
            fetched_at: new Date().toISOString(),
          };
        },
        {
          fresh: VEH_FRESH_MS,
          stale: VEH_STALE_MS,
          onError: (err) => log.warn({ err, key }, 'transport: vehicles refresh failed'),
        },
      );
      return ok(c, value);
    } catch (err) {
      // Cold-path failure only — SWR serves stale inside its window.
      log.warn({ err, key }, 'transport: vehicles upstream unavailable');
      return fail(c, 502, 'service_unavailable', UPSTREAM_MESSAGE, { error: 'transport upstream unavailable' });
    }
  },
);

// Route track geometry for one GTFS shape id (from a vehicle's shapeId).
// Shapes are static per id, so they cache long; the encoded polyline is
// passed through and decoded client-side (~1.5 KB per route).
// Tail may itself contain colons — dynamic services use ids like
// au2:ds:dyn:918-841-289 (verified served by upstream). The region is
// derived from the id's own prefix (au2:/au4:), so no ?region= param.
const SHAPE_ID_RE = /^au\d+:[a-z]{2}:[A-Za-z0-9_.:-]+$/;
const SHAPE_FRESH_MS = 24 * 3600_000;
const SHAPE_STALE_MS = 7 * 24 * 3600_000;

interface RawShapeResponse {
  response?: { shape?: { id?: string; enc?: string } };
}

const TransportShapeBase = z.object({
  id: z.string().describe('The shape id asked for; empty when switched off'),
  enc: z
    .string()
    .nullable()
    .describe('The track as a Google encoded polyline (precision 5, latitude first); null when AnyTrip has no geometry for it'),
});
assertShape<SameShape<z.infer<typeof TransportShapeBase>, TransportShapeSnapshot>>();
const TransportShapeSchema = TransportShapeBase.extend({ disabled: DISABLED }).meta({ id: 'TransportShape' });

transportRouter.get(
  '/api/transport/shape/:id',
  SCOPE_TRANSIT,
  describeRoute({
    operationId: 'getTransportShape',
    tags: ['Transport'],
    summary: "A trip's route track",
    description:
      "The track a trip follows, for a vehicle's shapeId, from AnyTrip. The region comes from the id's own prefix. Cached for a day; an answer can be up to a week old.",
    parameters: [
      {
        in: 'path',
        name: 'id',
        required: true,
        schema: { type: 'string', pattern: SHAPE_ID_RE.source },
        description: "A vehicle's shapeId, e.g. au2:bs:222299",
      },
    ],
    responses: {
      ...okJson(envelope(TransportShapeSchema), 'The track'),
      ...GATED_ERRORS,
      ...BAD_ID('The shape id'),
      ...ANYTRIP_UNAVAILABLE,
    },
  }),
  async (c) => {
    if (config.TRANSPORT_DISABLED) {
      return ok(c, { id: '', enc: null, disabled: true } satisfies z.infer<typeof TransportShapeSchema>);
    }
    const id = c.req.param('id').trim();
    if (!SHAPE_ID_RE.test(id)) {
      return fail(c, 400, 'validation_failed', 'invalid shape id', { error: 'invalid shape id' });
    }
    const shapeRegion = regionForId(id);
    if (!shapeRegion) return fail(c, 400, 'validation_failed', 'unknown region', { error: 'unknown region' });
    try {
      const { value } = await shapeCache.get(
        `sh|${id}`,
        async () => {
          // Ids go in RAW — upstream 404s on percent-encoded colons, and
          // the SHAPE_ID_RE whitelist already limits to URL-safe chars.
          const raw = await fetchJson<RawShapeResponse>(
            `${shapeRegion.base}/shape/${id}`,
            { timeoutMs: UPSTREAM_TIMEOUT_MS },
          );
          return { id, enc: raw.response?.shape?.enc ?? null };
        },
        {
          fresh: SHAPE_FRESH_MS,
          stale: SHAPE_STALE_MS,
          onError: (err) => log.warn({ err, id }, 'transport: shape refresh failed'),
        },
      );
      return ok(c, value);
    } catch (err) {
      log.warn({ err, id }, 'transport: shape upstream unavailable');
      return fail(c, 502, 'service_unavailable', UPSTREAM_MESSAGE, { error: 'transport upstream unavailable' });
    }
  },
);

// ---------------------------------------------------------------------
// Trip detail (stop sequence + live times) and station departures —
// power the click-through timetable panels. Both are realtime-ish, so
// short fresh windows; both normalize heavily (the raw departures
// payload is ~560 KB for 10 rows).

export interface TransportTripStop {
  name: string;
  lat: number | null;
  lon: number | null;
  seq: number;
  arr: number | null; // epoch seconds
  arrDelay: number | null; // seconds
  dep: number | null;
  depDelay: number | null;
  platform: string | null;
  locality: string | null;
  code: string | null;
  /** Per-carriage occupancy (0-6 each; single element for buses). */
  occupancy: Array<number | null> | null;
  /** GTFS pickup_type / drop_off_type: 1 = not available. */
  pickUp: number | null;
  dropOff: number | null;
}
/** Link to an adjacent trip in the same block (preceded by / continues as). */
export interface TransportTripRel {
  tripId: string;
  startDate: string;
  instanceNumber: number;
  routeName: string | null;
  routeColor: string | null;
  routeTextColor: string | null;
  headsign: string | null;
}
export interface TransportTripSnapshot {
  tripId: string;
  startDate: string;
  instanceNumber: number;
  headsign: string | null;
  route: {
    name: string | null;
    longName: string | null;
    color: string | null;
    textColor: string | null;
    mode: TransportMode;
  };
  shapeId: string | null;
  stops: TransportTripStop[];
  alerts: string[];
  /** Live vehicle position summary for the "Xs ago: at …" status line. */
  vehicle: {
    time: number | null;
    statusString: string | null;
    lat: number | null;
    lon: number | null;
  } | null;
  prev: TransportTripRel | null;
  next: TransportTripRel | null;
  fetched_at: string;
}
export interface TransportDeparture {
  route: {
    name: string | null;
    color: string | null;
    textColor: string | null;
    mode: TransportMode;
  };
  headsign: string | null;
  headsignSub: string | null;
  dep: number | null; // epoch seconds (realtime when available)
  delay: number | null; // seconds
  platform: string | null;
  tripId: string | null;
  startDate: string | null;
  instanceNumber: number | null;
}
export interface TransportDeparturesSnapshot {
  stopId: string;
  stopName: string | null;
  departures: TransportDeparture[];
  alerts: string[];
  fetched_at: string;
}

// QLD trip ids embed the timetable name WITH SPACES ("QR 26_27"), so
// the trip whitelist admits them; the upstream URL re-encodes a space
// as %20 while keeping colons raw (upstream 404s on %3A).
const TRIP_ID_RE = /^au\d+:[a-z]{2}:[A-Za-z0-9 _.:-]+$/;
// au2/au4/au9 stop ids are `au2:200060`-shaped; au3 uses `au3:G1058`
// and au5 keeps a feed segment (`au5:ad:50009`) — hence the colon.
const STOP_ID_RE = /^au\d+:[A-Za-z0-9_.:-]+$/;
const TRIP_FRESH_MS = 15_000;
const TRIP_STALE_MS = 60_000;
const DEP_FRESH_MS = 20_000;
const DEP_STALE_MS = 60_000;

interface RawStopTime {
  stop?: {
    fullName?: string;
    code?: string;
    locality?: string;
    name?: { station_name?: string };
    disassembled?: { platformCombinedName?: string };
    coordinates?: { lat?: number; lon?: number };
  };
  stopHeadsign?: { headline?: string; subtitle?: string | null };
  stopSequence?: number;
  arrival?: { time?: number; delay?: number; occupancy?: Array<number | null> };
  departure?: { time?: number; delay?: number; occupancy?: Array<number | null> };
  pickUp?: number;
  dropOff?: number;
}
interface RawRelTripInstance {
  startDate?: string;
  instanceNumber?: number;
  trip?: {
    id?: string;
    rtTripId?: string;
    headsign?: { headline?: string };
    route?: { name?: string; color?: string; textColor?: string };
  };
}
interface RawTripDetailResponse {
  response?: {
    tripInstance?: {
      shapeId?: string;
      trip?: {
        id?: string;
        shapeId?: string;
        headsign?: { headline?: string };
        route?: {
          name?: string;
          longName?: string;
          color?: string;
          textColor?: string;
          mode?: string;
        };
      };
    };
    realtimePattern?: RawStopTime[];
    alerts?: Array<{ header?: string }>;
    vehicle?: {
      lastPosition?: {
        time?: number;
        statusString?: string;
        coordinates?: { lat?: number; lon?: number };
      };
    };
    rel?: {
      prev?: { tripInstance?: RawRelTripInstance };
      next?: { tripInstance?: RawRelTripInstance };
    };
  };
}
interface RawDeparturesResponse {
  response?: {
    stop?: { fullName?: string; name?: { station_name?: string } };
    alerts?: Array<{ header?: string }>;
    departures?: Array<{
      tripInstance?: {
        startDate?: string;
        instanceNumber?: number;
        trip?: {
          id?: string;
          rtTripId?: string;
          headsign?: { headline?: string };
          route?: {
            name?: string;
            color?: string;
            textColor?: string;
            mode?: string;
          };
        };
      };
      stopTimeInstance?: RawStopTime;
    }>;
  };
}

const tripCache = new SwrCache<TransportTripSnapshot>(300);
const depCache = new SwrCache<TransportDeparturesSnapshot>(300);

function num(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}
function stopPlatform(st: RawStopTime['stop']): string | null {
  return st?.disassembled?.platformCombinedName ?? null;
}
function stopName(st: RawStopTime['stop']): string {
  return st?.fullName ?? st?.name?.station_name ?? 'Unknown stop';
}

const DELAY = 'seconds late; negative is early';
const PLATFORM = 'Platform or stand, as AnyTrip names it';

const TransportTripStopSchema = z
  .object({
    name: z.string().describe('"Unknown stop" when the feed names none'),
    lat: z.number().nullable(),
    lon: z.number().nullable(),
    seq: z.number().describe("The stop's sequence number in the trip, or its place in this list when the feed gives none"),
    arr: z.number().nullable().describe('Arrival time, unix seconds'),
    arrDelay: z.number().nullable().describe(`Arrival delay, ${DELAY}`),
    dep: z.number().nullable().describe('Departure time, unix seconds'),
    depDelay: z.number().nullable().describe(`Departure delay, ${DELAY}`),
    platform: z.string().nullable().describe(PLATFORM),
    locality: z.string().nullable(),
    code: z.string().nullable(),
    occupancy: z
      .array(z.number().nullable())
      .nullable()
      .describe(`One entry per carriage (one for a bus), each on ${OCCUPANCY_SCALE}, or null; null when not reported`),
    pickUp: z.number().nullable().describe('GTFS pickup_type: 1 means passengers cannot board here'),
    dropOff: z.number().nullable().describe('GTFS drop_off_type: 1 means passengers cannot get off here'),
  })
  .meta({ id: 'TransportTripStop' });
assertShape<SameShape<z.infer<typeof TransportTripStopSchema>, TransportTripStop>>();

const TransportTripLinkSchema = z
  .object({
    tripId: z.string(),
    startDate: z.string().describe('YYYYMMDD'),
    instanceNumber: z.number(),
    routeName: z.string().nullable(),
    routeColor: z.string().nullable().describe(ROUTE_COLOR),
    routeTextColor: z.string().nullable().describe(ROUTE_COLOR),
    headsign: z.string().nullable(),
  })
  .meta({
    id: 'TransportTripLink',
    description: 'An adjacent trip in the same block, addressed the same way as this one',
  });
assertShape<SameShape<z.infer<typeof TransportTripLinkSchema>, TransportTripRel>>();

const TransportTripSchema = z
  .object({
    tripId: z.string().describe('As asked for'),
    startDate: z.string().describe('As asked for, YYYYMMDD'),
    instanceNumber: z.number().int().describe('As asked for'),
    headsign: z.string().nullable().describe('Where it is going'),
    route: z.object({
      name: z.string().nullable(),
      longName: z.string().nullable(),
      color: z.string().nullable().describe(ROUTE_COLOR),
      textColor: z.string().nullable().describe(ROUTE_COLOR),
      mode: TransportModeSchema,
    }),
    shapeId: z.string().nullable().describe("The trip's route track, for /api/v1/transport/shape/{id}"),
    stops: z.array(TransportTripStopSchema).describe('Its stops, with times as AnyTrip currently estimates them'),
    alerts: z.array(z.string()).describe('Headlines of up to three alerts AnyTrip lists for the trip'),
    vehicle: z
      .object({
        time: z.number().nullable().describe('When the position was reported, unix seconds'),
        statusString: z
          .string()
          .nullable()
          .describe("AnyTrip's account of where it is, e.g. \"at Lakemba Station, Stand D\" or \"2.5km from …\""),
        lat: z.number().nullable(),
        lon: z.number().nullable(),
      })
      .nullable()
      .describe('Where the vehicle running the trip is; null when AnyTrip reports none'),
    prev: TransportTripLinkSchema.nullable().describe('The trip before this one in the block'),
    next: TransportTripLinkSchema.nullable().describe('The trip after this one in the block'),
    fetched_at: z.string().describe('When AnyTrip was read, ISO 8601'),
  })
  .meta({ id: 'TransportTrip' });
assertShape<SameShape<z.infer<typeof TransportTripSchema>, TransportTripSnapshot>>();

transportRouter.get(
  '/api/transport/trip/:date/:tripId/:instance',
  SCOPE_TRANSIT,
  describeRoute({
    operationId: 'getTransportTrip',
    tags: ['Transport'],
    summary: 'One trip, stop by stop',
    description:
      "One trip's stops with live times and delays, where its vehicle is, and the trips before and after it in the same block. Address it with a vehicle's startDate, tripId and instanceNumber; the region comes from the trip id's own prefix. From AnyTrip, cached for 15 seconds; an answer can be up to a minute old.",
    parameters: [
      {
        in: 'path',
        name: 'date',
        required: true,
        schema: { type: 'string', pattern: '^\\d{8}$' },
        description: "The trip's service date, YYYYMMDD — a vehicle's startDate",
      },
      {
        in: 'path',
        name: 'tripId',
        required: true,
        schema: { type: 'string', pattern: TRIP_ID_RE.source },
        description: "A vehicle's tripId, URL-encoded",
      },
      {
        in: 'path',
        name: 'instance',
        required: true,
        schema: { type: 'string', pattern: '^\\d{1,3}$' },
        description: "A vehicle's instanceNumber",
      },
    ],
    responses: {
      ...okJson(envelope(TransportTripSchema), 'The trip'),
      ...GATED_ERRORS,
      ...BAD_ID('The date, trip id or instance'),
      ...TRANSPORT_SWITCHED_OFF,
      ...ANYTRIP_UNAVAILABLE,
    },
  }),
  async (c) => {
    if (config.TRANSPORT_DISABLED) return fail(c, 404, 'not_found', SWITCHED_OFF_MESSAGE, { error: 'disabled' });
    const date = c.req.param('date');
    const tripId = c.req.param('tripId');
    const instance = c.req.param('instance');
    if (!/^\d{8}$/.test(date)) return fail(c, 400, 'validation_failed', 'invalid date', { error: 'invalid date' });
    if (!TRIP_ID_RE.test(tripId)) {
      return fail(c, 400, 'validation_failed', 'invalid trip id', { error: 'invalid trip id' });
    }
    if (!/^\d{1,3}$/.test(instance)) {
      return fail(c, 400, 'validation_failed', 'invalid instance', { error: 'invalid instance' });
    }
    const tripRegion = regionForId(tripId);
    if (!tripRegion) return fail(c, 400, 'validation_failed', 'unknown region', { error: 'unknown region' });
    const key = `t|${date}|${tripId}|${instance}`;
    try {
      const { value } = await tripCache.get(
        key,
        async () => {
          // tripId goes in RAW — upstream 404s on percent-encoded colons;
          // TRIP_ID_RE already restricts it to URL-safe characters.
          const encTripId = tripId.replace(/ /g, '%20');
          const raw = await fetchJson<RawTripDetailResponse>(
            `${tripRegion.base}/tripInstance/${date}/${encTripId}/${instance}`,
            { timeoutMs: UPSTREAM_TIMEOUT_MS },
          );
          const ti = raw.response?.tripInstance;
          const trip = ti?.trip;
          const stops: TransportTripStop[] = (raw.response?.realtimePattern ?? []).map(
            (st, i) => {
              const occ = st.departure?.occupancy ?? st.arrival?.occupancy;
              return {
                name: stopName(st.stop),
                lat: num(st.stop?.coordinates?.lat),
                lon: num(st.stop?.coordinates?.lon),
                seq: num(st.stopSequence) ?? i,
                arr: num(st.arrival?.time),
                arrDelay: num(st.arrival?.delay),
                dep: num(st.departure?.time),
                depDelay: num(st.departure?.delay),
                platform: stopPlatform(st.stop),
                locality: st.stop?.locality ?? null,
                code: st.stop?.code ?? null,
                occupancy: Array.isArray(occ)
                  ? occ.map((o) => (typeof o === 'number' && o >= 0 && o <= 6 ? o : null))
                  : null,
                pickUp: num(st.pickUp),
                dropOff: num(st.dropOff),
              };
            },
          );
          const relRef = (r?: { tripInstance?: RawRelTripInstance }): TransportTripRel | null => {
            const rti = r?.tripInstance;
            const relTripId = rti?.trip?.rtTripId ?? rti?.trip?.id;
            if (!relTripId || !rti?.startDate || typeof rti.instanceNumber !== 'number') {
              return null;
            }
            return {
              tripId: relTripId,
              startDate: rti.startDate,
              instanceNumber: rti.instanceNumber,
              routeName: rti.trip?.route?.name ?? null,
              routeColor: normColor(rti.trip?.route?.color),
              routeTextColor: normColor(rti.trip?.route?.textColor),
              headsign: rti.trip?.headsign?.headline ?? null,
            };
          };
          const pos = raw.response?.vehicle?.lastPosition;
          return {
            tripId,
            startDate: date,
            instanceNumber: Number(instance),
            headsign: trip?.headsign?.headline ?? null,
            route: {
              name: trip?.route?.name ?? null,
              longName: trip?.route?.longName ?? null,
              color: normColor(trip?.route?.color),
              textColor: normColor(trip?.route?.textColor),
              mode: normMode(trip?.route?.mode),
            },
            shapeId: ti?.shapeId ?? trip?.shapeId ?? null,
            stops,
            alerts: (raw.response?.alerts ?? [])
              .map((a) => a.header ?? '')
              .filter(Boolean)
              .slice(0, 3),
            vehicle: pos
              ? {
                  time: num(pos.time),
                  statusString: pos.statusString ?? null,
                  lat: num(pos.coordinates?.lat),
                  lon: num(pos.coordinates?.lon),
                }
              : null,
            prev: relRef(raw.response?.rel?.prev),
            next: relRef(raw.response?.rel?.next),
            fetched_at: new Date().toISOString(),
          };
        },
        {
          fresh: TRIP_FRESH_MS,
          stale: TRIP_STALE_MS,
          onError: (err) => log.warn({ err, key }, 'transport: trip refresh failed'),
        },
      );
      return ok(c, value);
    } catch (err) {
      log.warn({ err, key }, 'transport: trip upstream unavailable');
      return fail(c, 502, 'service_unavailable', UPSTREAM_MESSAGE, { error: 'transport upstream unavailable' });
    }
  },
);

const TransportDepartureSchema = z
  .object({
    route: z.object({
      name: z.string().nullable(),
      color: z.string().nullable().describe(ROUTE_COLOR),
      textColor: z.string().nullable().describe(ROUTE_COLOR),
      mode: TransportModeSchema,
    }),
    headsign: z.string().nullable().describe("Where it is going: the stop's own headsign where there is one, else the trip's"),
    headsignSub: z.string().nullable(),
    dep: z.number().nullable().describe('Departure time (the arrival time where there is none), unix seconds'),
    delay: z.number().nullable().describe(`Departure delay, ${DELAY}`),
    platform: z.string().nullable().describe(PLATFORM),
    tripId: z
      .string()
      .nullable()
      .describe('With startDate and instanceNumber it addresses /api/v1/transport/trip/{date}/{tripId}/{instance}'),
    startDate: z.string().nullable().describe('YYYYMMDD'),
    instanceNumber: z.number().nullable(),
  })
  .meta({ id: 'TransportDeparture' });
assertShape<SameShape<z.infer<typeof TransportDepartureSchema>, TransportDeparture>>();

const TransportDeparturesSchema = z
  .object({
    stopId: z.string().describe('As asked for'),
    stopName: z.string().nullable(),
    departures: z.array(TransportDepartureSchema),
    alerts: z.array(z.string()).describe('Headlines of up to three alerts AnyTrip lists with the departures'),
    fetched_at: z.string().describe('When AnyTrip was read, ISO 8601'),
  })
  .meta({ id: 'TransportDepartures' });
assertShape<SameShape<z.infer<typeof TransportDeparturesSchema>, TransportDeparturesSnapshot>>();

const DeparturesQuery = z.object({
  limit: z.coerce
    .number()
    .int()
    .min(1)
    .max(20)
    .optional()
    .catch(undefined)
    .describe('How many departures to ask AnyTrip for, 1–20 (default 10); more than 20 is read as 20'),
});

transportRouter.get(
  '/api/transport/departures/:stopId',
  SCOPE_TRANSIT,
  describeRoute({
    operationId: 'getTransportDepartures',
    tags: ['Transport'],
    summary: 'Departures from a stop',
    description:
      "Departures from a stop or station as AnyTrip lists them, with delays and platforms. The region comes from the stop id's own prefix. Cached for 20 seconds; an answer can be up to a minute old.",
    parameters: [
      {
        in: 'path',
        name: 'stopId',
        required: true,
        schema: { type: 'string', pattern: STOP_ID_RE.source },
        description: 'A stop id from /api/v1/transport/stops, e.g. au2:200060',
      },
    ],
    responses: {
      ...okJson(envelope(TransportDeparturesSchema), 'The departures'),
      ...GATED_ERRORS,
      ...BAD_ID('The stop id'),
      ...TRANSPORT_SWITCHED_OFF,
      ...ANYTRIP_UNAVAILABLE,
    },
  }),
  queryValidator(DeparturesQuery),
  async (c) => {
    if (config.TRANSPORT_DISABLED) return fail(c, 404, 'not_found', SWITCHED_OFF_MESSAGE, { error: 'disabled' });
    const stopId = c.req.param('stopId');
    if (!STOP_ID_RE.test(stopId)) {
      return fail(c, 400, 'validation_failed', 'invalid stop id', { error: 'invalid stop id' });
    }
    const stopRegion = regionForId(stopId);
    if (!stopRegion) return fail(c, 400, 'validation_failed', 'unknown region', { error: 'unknown region' });
    const limit = Math.min(20, Math.max(1, Number(c.req.query('limit')) || 10));
    const key = `d|${stopId}|${limit}`;
    try {
      const { value } = await depCache.get(
        key,
        async () => {
          // stopId raw for the same percent-encoding reason (STOP_ID_RE
          // whitelists it).
          const raw = await fetchJson<RawDeparturesResponse>(
            `${stopRegion.base}/departures/${stopId}?limit=${limit}`,
            { timeoutMs: UPSTREAM_TIMEOUT_MS },
          );
          const r = raw.response;
          const departures: TransportDeparture[] = (r?.departures ?? []).map((d) => {
            const trip = d.tripInstance?.trip;
            const sti = d.stopTimeInstance;
            return {
              route: {
                name: trip?.route?.name ?? null,
                color: normColor(trip?.route?.color),
                textColor: normColor(trip?.route?.textColor),
                mode: normMode(trip?.route?.mode),
              },
              headsign: sti?.stopHeadsign?.headline ?? trip?.headsign?.headline ?? null,
              headsignSub: sti?.stopHeadsign?.subtitle ?? null,
              dep: num(sti?.departure?.time) ?? num(sti?.arrival?.time),
              delay: num(sti?.departure?.delay),
              platform: stopPlatform(sti?.stop),
              tripId: trip?.rtTripId ?? trip?.id ?? null,
              startDate: d.tripInstance?.startDate ?? null,
              instanceNumber:
                typeof d.tripInstance?.instanceNumber === 'number'
                  ? d.tripInstance.instanceNumber
                  : null,
            };
          });
          return {
            stopId,
            stopName: r?.stop?.fullName ?? r?.stop?.name?.station_name ?? null,
            departures,
            alerts: (r?.alerts ?? [])
              .map((a) => a.header ?? '')
              .filter(Boolean)
              .slice(0, 3),
            fetched_at: new Date().toISOString(),
          };
        },
        {
          fresh: DEP_FRESH_MS,
          stale: DEP_STALE_MS,
          onError: (err) => log.warn({ err, key }, 'transport: departures refresh failed'),
        },
      );
      return ok(c, value);
    } catch (err) {
      log.warn({ err, key }, 'transport: departures upstream unavailable');
      return fail(c, 502, 'service_unavailable', UPSTREAM_MESSAGE, { error: 'transport upstream unavailable' });
    }
  },
);

// ---------------------------------------------------------------------
// Static NSW rail/metro/light-rail network geometry (AnyTrip's
// pre-styled GeoJSON, per-feature official line colours). ~434 KB raw,
// gzipped by the compress() middleware; cached a day.

// lines.json only covers greater Sydney; otherrail.json adds the few
// styled segments outside it (e.g. Canberra light rail); qldlines.json
// is QR Citytrain (SEQ heavy rail — no G:link, which auto-tracks
// client-side instead). Merged into one FeatureCollection; any file
// failing alone is tolerated.
const LINES_URLS = [
  'https://static.anytrip.com.au/tiles/lines.json',
  'https://static.anytrip.com.au/tiles/otherrail.json',
  'https://static.anytrip.com.au/tiles/qldlines.json',
];
const LINES_FRESH_MS = 24 * 3600_000;
const LINES_STALE_MS = 7 * 24 * 3600_000;
/** The merged network: AnyTrip's own features, passed through untouched. */
interface TransportLinesCollection {
  type: 'FeatureCollection';
  features: unknown[];
}
const linesCache = new SwrCache<TransportLinesCollection>(2);

interface GeoFeatureCollection {
  type?: string;
  features?: unknown[];
}

const TransportLinesBase = z.object({
  type: z.literal('FeatureCollection'),
  features: z
    .array(z.unknown())
    .describe("AnyTrip's GeoJSON features, passed through unchanged"),
});
assertShape<SameShape<z.infer<typeof TransportLinesBase>, TransportLinesCollection>>();
const TransportLinesSchema = TransportLinesBase.extend({ disabled: DISABLED }).meta({ id: 'TransportLines' });

transportRouter.get(
  '/api/transport/lines',
  SCOPE_TRANSIT,
  describeRoute({
    operationId: 'getTransportLines',
    tags: ['Transport'],
    summary: 'Rail network lines',
    description:
      "The rail, metro and light-rail network as AnyTrip draws it, each line in its official colour: greater Sydney, a few lines elsewhere such as Canberra light rail, and Queensland Rail's South East Queensland network. AnyTrip's GeoJSON is passed through unchanged; if one of its files cannot be read the others are still served. Cached for a day; an answer can be up to a week old.",
    responses: {
      ...okJson(TransportLinesSchema, 'A GeoJSON FeatureCollection of the network'),
      ...GATED_ERRORS,
      ...ANYTRIP_UNAVAILABLE,
    },
  }),
  async (c) => {
    if (config.TRANSPORT_DISABLED) {
      return geo(c, { type: 'FeatureCollection', features: [], disabled: true } satisfies z.infer<typeof TransportLinesSchema>);
    }
    try {
      const { value } = await linesCache.get(
        'lines',
        async () => {
          const results = await Promise.allSettled(
            LINES_URLS.map((u) =>
              fetchJson<GeoFeatureCollection>(u, { timeoutMs: UPSTREAM_TIMEOUT_MS }),
            ),
          );
          const features: unknown[] = [];
          let loaded = 0;
          for (const r of results) {
            if (r.status === 'fulfilled' && Array.isArray(r.value.features)) {
              features.push(...r.value.features);
              loaded += 1;
            }
          }
          if (loaded === 0) throw new Error('all line sources failed');
          return { type: 'FeatureCollection', features };
        },
        {
          fresh: LINES_FRESH_MS,
          stale: LINES_STALE_MS,
          onError: (err) => log.warn({ err }, 'transport: lines refresh failed'),
        },
      );
      return geo(c, value);
    } catch (err) {
      log.warn({ err }, 'transport: lines upstream unavailable');
      return fail(c, 502, 'service_unavailable', UPSTREAM_MESSAGE, { error: 'transport upstream unavailable' });
    }
  },
);

const TransportStopSchema = z
  .object({
    id: z
      .string()
      .describe("AnyTrip's stop id, region first (au2:200060, au3:G1058, au5:ad:50009) — the {stopId} of /api/v1/transport/departures/{stopId}"),
    name: z.string(),
    lat: z.number(),
    lon: z.number(),
    modes: z
      .array(z.string())
      .describe("Modes that serve it: AnyTrip's names without the region prefix, regional ones mapped as for TransportMode (tram is lightrail, and so on)"),
    locality: z.string().nullable(),
    wheelchair: z.boolean().nullable().describe('Wheelchair accessible; null means unknown, not no'),
    accessibility: z.array(z.string()).describe('Accessibility facilities as AnyTrip lists them, e.g. "Lift"'),
  })
  .meta({ id: 'TransportStop' });
assertShape<SameShape<z.infer<typeof TransportStopSchema>, TransportStop>>();

const TransportStopsBase = z.object({
  stops: z.array(TransportStopSchema),
  count: z.number().int().describe('Number of stops'),
  fetched_at: z.string().describe(FETCHED_AT),
});
assertShape<SameShape<z.infer<typeof TransportStopsBase>, TransportStopsSnapshot>>();
const TransportStopsSchema = TransportStopsBase.extend({ disabled: DISABLED }).meta({ id: 'TransportStops' });

const StopsQuery = z.object({
  region: RegionParam,
  ...BboxParams,
  modes: z
    .string()
    .optional()
    .catch(undefined)
    .describe(
      'Comma-separated modes — au2 metro, sydneytrains, nswtrains, ferries, lightrail; au3 metrotrains, vlinetrains, tram; au4 trains, ferries, lightrail; au5 trains, lightrail; au9 lightrail. All of them when absent; an unknown one is refused. No region offers bus stops',
    ),
});

transportRouter.get(
  '/api/transport/stops',
  SCOPE_TRANSIT,
  describeRoute({
    operationId: 'getTransportStops',
    tags: ['Transport'],
    summary: 'Stations and stops in an area',
    description: `Train, metro, light-rail, tram and ferry stops inside an area, for one region, from AnyTrip — bus stops are not included. ${BBOX_RULES} At most 500 stops. Each area's answer is cached for an hour and can be up to a day old.`,
    responses: {
      ...okJson(envelope(TransportStopsSchema), 'The stops in the area'),
      ...GATED_ERRORS,
      ...VALIDATION_ERROR,
      ...ANYTRIP_UNAVAILABLE,
    },
  }),
  queryValidator(StopsQuery),
  async (c) => {
    const empty: TransportStopsSnapshot = {
      stops: [],
      count: 0,
      fetched_at: new Date().toISOString(),
    };
    if (config.TRANSPORT_DISABLED) {
      return ok(c, { ...empty, disabled: true } satisfies z.infer<typeof TransportStopsSchema>);
    }

    const parsedRegion = parseRegion(c.req.query('region'));
    if (typeof parsedRegion === 'string') {
      return fail(c, 400, 'validation_failed', parsedRegion, { error: parsedRegion });
    }
    const { id: regionId, region } = parsedRegion;
    const bbox = parseBbox({
      minLat: c.req.query('minLat'),
      maxLat: c.req.query('maxLat'),
      minLon: c.req.query('minLon'),
      maxLon: c.req.query('maxLon'),
    }, region.bounds);
    if (typeof bbox === 'string') return fail(c, 400, 'validation_failed', bbox, { error: bbox });
    const modes = parseListParam(c.req.query('modes'), region.stopModes, 'mode');
    if (typeof modes === 'string') return fail(c, 400, 'validation_failed', modes, { error: modes });

    const key = `s|${regionId}|${bboxKey(bbox)}|${modes.join(',')}`;
    try {
      const { value } = await stopsCache.get(
        key,
        async () => {
          const modeList = modes.map((m) => region.stopModes[m]).join(',');
          const url =
            `${region.base}/stops?limit=500&modes=${encodeURIComponent(modeList)}` +
            `&${bboxParams(bbox)}`;
          const raw = await fetchJson<RawStopsResponse>(url, {
            timeoutMs: UPSTREAM_TIMEOUT_MS,
          });
          const stops = normalizeStops(raw);
          return { stops, count: stops.length, fetched_at: new Date().toISOString() };
        },
        {
          fresh: STOPS_FRESH_MS,
          stale: STOPS_STALE_MS,
          onError: (err) => log.warn({ err, key }, 'transport: stops refresh failed'),
        },
      );
      return ok(c, value);
    } catch (err) {
      log.warn({ err, key }, 'transport: stops upstream unavailable');
      return fail(c, 502, 'service_unavailable', UPSTREAM_MESSAGE, { error: 'transport upstream unavailable' });
    }
  },
);

const GTFS_ENUM = (field: string, example: string) =>
  `The GTFS-realtime ${field}, by its name (e.g. ${example}); null when not given`;

const TransportAlertSchema = z
  .object({
    id: z.string().describe("The feed's own id for the alert, or the start of its headline where it has none"),
    header: z.string().describe('Headline, in English where the feed offers a choice'),
    description: z.string().nullable(),
    url: z.string().nullable(),
    cause: z.string().nullable().describe(GTFS_ENUM('cause', 'MAINTENANCE')),
    effect: z.string().nullable().describe(GTFS_ENUM('effect', 'DETOUR')),
    severity: z.string().nullable().describe(GTFS_ENUM('severity level', 'WARNING')),
    start: z.number().nullable().describe('Start of its first active period, unix seconds; null when not given'),
    end: z.number().nullable().describe('End of its first active period, unix seconds; null when not given'),
    routes: z.array(z.string()).describe('TfNSW GTFS route ids it names (not AnyTrip ids)'),
    stops: z.array(z.string()).describe('TfNSW GTFS stop ids it names (not AnyTrip ids)'),
    agencies: z.array(z.string()).describe('TfNSW GTFS agency ids it names'),
  })
  .meta({ id: 'TransportAlert' });
assertShape<SameShape<z.infer<typeof TransportAlertSchema>, TfnswAlert>>();

const TransportAlertsSchema = z
  .object({
    alerts: z.array(TransportAlertSchema),
    count: z.number().int().describe('Number of alerts'),
    configured: z.boolean().describe('false when TfNSW alerts are not set up on this server; the list is then empty'),
    fetched_at: z
      .string()
      .optional()
      .describe('The time of this answer, ISO 8601 — not when TfNSW was last read; absent when configured is false'),
    disabled: DISABLED,
  })
  .meta({ id: 'TransportAlerts' });

// Service alerts (official TfNSW GTFS-realtime alert feeds). Fixed
// path, no viewport scoping — safe for the CDN cache list. Returns
// `configured:false` (not an error) when no TFNSW_API_KEY is set so
// the frontend can hide the UI.
transportRouter.get(
  '/api/transport/alerts',
  SCOPE_TRANSIT,
  describeRoute({
    operationId: 'getTransportAlerts',
    tags: ['Transport'],
    summary: 'Transport for NSW service alerts',
    description:
      "Service alerts from Transport for NSW's official GTFS-realtime alert feeds, for every mode. Cached for a minute; an answer can be up to ten minutes old. Empty, with configured false, when this server has no TfNSW access set up.",
    responses: {
      ...okJson(envelope(TransportAlertsSchema), 'The alerts'),
      ...GATED_ERRORS,
      502: errorResponse('The TfNSW alerts could not be read (service_unavailable)'),
    },
  }),
  async (c) => {
    if (config.TRANSPORT_DISABLED) {
      return ok(c, {
        alerts: [],
        count: 0,
        configured: false,
        disabled: true,
      } satisfies z.infer<typeof TransportAlertsSchema>);
    }
    if (!tfnswConfigured()) {
      return ok(c, { alerts: [], count: 0, configured: false } satisfies z.infer<typeof TransportAlertsSchema>);
    }
    try {
      const alerts = await fetchTfnswAlerts();
      return ok(c, {
        alerts,
        count: alerts.length,
        configured: true,
        fetched_at: new Date().toISOString(),
      } satisfies z.infer<typeof TransportAlertsSchema>);
    } catch (err) {
      log.warn({ err }, 'transport: alerts unavailable');
      return fail(c, 502, 'service_unavailable', 'Transport for NSW alerts could not be read just now.', {
        error: 'alerts upstream unavailable',
      });
    }
  },
);
