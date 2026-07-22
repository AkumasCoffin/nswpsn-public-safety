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
 * Bbox handling: coords are clamped into NSW (rejecting would break
 * padded coastal/border viewports), then snapped OUTWARD to a 0.01°
 * (~1.1 km) grid. The snapped bbox is both the cache key and the
 * upstream query, so the cached result always covers the requested
 * area and nearby pans re-hit the same cache cell.
 *
 * Deliberately NOT in CACHEABLE_PATHS: that middleware matches the
 * pathname ignoring query strings, so a CDN would cross-serve one
 * viewport's vehicles to every viewport.
 */
import { Hono } from 'hono';
import { fetchJson } from '../sources/shared/http.js';
import { SwrCache } from '../services/swrCache.js';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import {
  fetchTfnswPositions,
  applyTfnswPositions,
  fetchTfnswAlerts,
  tfnswConfigured,
} from '../sources/tfnsw.js';

export const transportRouter = new Hono();

const ANYTRIP_BASE = 'https://api-cf-oc2.anytrip.com.au/api/v3/region/au2';

// Vehicles use short feed codes, stops use long mode names — two
// whitelists. `sp` (school/special services) reports mode au2:buses.
const VEHICLE_FEEDS: Record<string, string> = {
  bs: 'au2:bs',
  st: 'au2:st',
  mt: 'au2:mt',
  nt: 'au2:nt',
  fr: 'au2:fr',
  lr: 'au2:lr',
  sp: 'au2:sp',
};
// No 'buses' here in v1 — tens of thousands of bus stops would blow the
// upstream limit=500 and truncate arbitrarily in dense areas.
const STOP_MODES: Record<string, string> = {
  metro: 'au2:metro',
  sydneytrains: 'au2:sydneytrains',
  nswtrains: 'au2:nswtrains',
  ferries: 'au2:ferries',
  lightrail: 'au2:lightrail',
};

const NSW_BOUNDS = { minLat: -38, maxLat: -28, minLon: 140, maxLon: 154 };
const MAX_SPAN_DEG = 2.5; // per axis — a zoom-11 viewport is ~0.35°
const GRID = 0.01; // snap-outward grid (degrees)
const UPSTREAM_TIMEOUT_MS = 12_000;
// Params the AnyTrip web app itself sends — keep our traffic ordinary.
const OTR_FILTER = 300;
const SPEED_FILTER = 15;

const VEH_FRESH_MS = 10_000;
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
  tripId: string | null;
  /** GTFS shape id — resolves to the route track via /api/transport/shape. */
  shapeId: string | null;
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
export function parseBbox(q: Record<string, string | undefined>): Bbox | string {
  const minLat = Number(q['minLat']);
  const maxLat = Number(q['maxLat']);
  const minLon = Number(q['minLon']);
  const maxLon = Number(q['maxLon']);
  if (![minLat, maxLat, minLon, maxLon].every(Number.isFinite)) {
    return 'invalid bbox';
  }
  // Clamp — not reject. The frontend pads its viewport, so legitimate
  // coastal/border views poke past the NSW envelope.
  const cMinLat = Math.max(minLat, NSW_BOUNDS.minLat);
  const cMaxLat = Math.min(maxLat, NSW_BOUNDS.maxLat);
  const cMinLon = Math.max(minLon, NSW_BOUNDS.minLon);
  const cMaxLon = Math.min(maxLon, NSW_BOUNDS.maxLon);
  // Catches inverted boxes AND boxes entirely outside NSW (which clamp
  // to zero/negative span).
  if (cMaxLat <= cMinLat || cMaxLon <= cMinLon) return 'bbox outside NSW';
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

function normMode(raw: string | undefined): TransportMode {
  const m = (raw ?? '').replace(/^au2:/, '');
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
      tripId: trip?.rtTripId ?? trip?.id ?? null,
      shapeId: entry.tripInstance?.shapeId ?? trip?.shapeId ?? null,
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
      modes: (s.modes ?? []).map((m) => m.replace(/^au2:/, '')),
      locality: s.locality ?? null,
      wheelchair: typeof s.wheelchair === 'boolean' ? s.wheelchair : null,
      accessibility: s.facilities?.accessibility ?? [],
    });
  }
  return out;
}

// ---------------------------------------------------------------------

const vehiclesCache = new SwrCache<TransportVehiclesSnapshot>(500);
const stopsCache = new SwrCache<TransportStopsSnapshot>(500);
const shapeCache = new SwrCache<TransportShapeSnapshot>(1000);

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
}

function bboxKey(b: Bbox): string {
  return `${b.minLat},${b.maxLat},${b.minLon},${b.maxLon}`;
}
function bboxParams(b: Bbox): string {
  return `minLat=${b.minLat}&maxLat=${b.maxLat}&minLon=${b.minLon}&maxLon=${b.maxLon}`;
}

transportRouter.get('/api/transport/vehicles', async (c) => {
  const empty: TransportVehiclesSnapshot = {
    vehicles: [],
    count: 0,
    fetched_at: new Date().toISOString(),
  };
  if (config.TRANSPORT_DISABLED) return c.json({ ...empty, disabled: true });

  const bbox = parseBbox({
    minLat: c.req.query('minLat'),
    maxLat: c.req.query('maxLat'),
    minLon: c.req.query('minLon'),
    maxLon: c.req.query('maxLon'),
  });
  if (typeof bbox === 'string') return c.json({ error: bbox }, 400);
  const feeds = parseListParam(c.req.query('feeds'), VEHICLE_FEEDS, 'feed');
  if (typeof feeds === 'string') return c.json({ error: feeds }, 400);

  const key = `v|${bboxKey(bbox)}|${feeds.join(',')}`;
  try {
    const { value } = await vehiclesCache.get(
      key,
      async () => {
        const feedList = feeds.map((f) => VEHICLE_FEEDS[f]).join(',');
        const url =
          `${ANYTRIP_BASE}/vehicles?feeds=${encodeURIComponent(feedList)}` +
          `&${bboxParams(bbox)}&otrFilter=${OTR_FILTER}&speedFilter=${SPEED_FILTER}`;
        // AnyTrip (metadata + interpolated fallback positions) and the
        // official TfNSW GTFS-R positions fetch concurrently; TfNSW is
        // authoritative for trips it knows, AnyTrip fills the rest.
        // fetchTfnswPositions returns [] when unconfigured or failing,
        // so the join degrades to AnyTrip-only.
        const [raw, tfnsw] = await Promise.all([
          fetchJson<RawVehiclesResponse>(url, { timeoutMs: UPSTREAM_TIMEOUT_MS }),
          fetchTfnswPositions(feeds),
        ]);
        let vehicles = normalizeVehicles(raw);
        const vehiclesBeforeJoin = vehicles.length;
        let networkCounts: Record<string, number> | undefined;
        if (tfnsw.length) {
          const joined = applyTfnswPositions(vehicles, tfnsw, bbox, MAX_VEHICLES);
          vehicles = joined.vehicles;
          networkCounts = {};
          for (const p of tfnsw) {
            networkCounts[p.mode] = (networkCounts[p.mode] ?? 0) + 1;
          }
          // TEMP info-level so the join breakdown shows in prod logs while
          // confirming trains match by vehicle id (byVeh) — drop back to
          // debug once verified.
          log.info(
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
    return c.json(value);
  } catch (err) {
    // Cold-path failure only — SWR serves stale inside its window.
    log.warn({ err, key }, 'transport: vehicles upstream unavailable');
    return c.json({ error: 'transport upstream unavailable' }, 502);
  }
});

// Route track geometry for one GTFS shape id (from a vehicle's shapeId).
// Shapes are static per id, so they cache long; the encoded polyline is
// passed through and decoded client-side (~1.5 KB per route).
// Tail may itself contain colons — dynamic services use ids like
// au2:ds:dyn:918-841-289 (verified served by upstream).
const SHAPE_ID_RE = /^au2:[a-z]{2}:[A-Za-z0-9_.:-]+$/;
const SHAPE_FRESH_MS = 24 * 3600_000;
const SHAPE_STALE_MS = 7 * 24 * 3600_000;

interface RawShapeResponse {
  response?: { shape?: { id?: string; enc?: string } };
}

transportRouter.get('/api/transport/shape/:id', async (c) => {
  if (config.TRANSPORT_DISABLED) return c.json({ id: '', enc: null, disabled: true });
  const id = c.req.param('id').trim();
  if (!SHAPE_ID_RE.test(id)) return c.json({ error: 'invalid shape id' }, 400);
  try {
    const { value } = await shapeCache.get(
      `sh|${id}`,
      async () => {
        // Ids go in RAW — upstream 404s on percent-encoded colons, and
        // the SHAPE_ID_RE whitelist already limits to URL-safe chars.
        const raw = await fetchJson<RawShapeResponse>(
          `${ANYTRIP_BASE}/shape/${id}`,
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
    return c.json(value);
  } catch (err) {
    log.warn({ err, id }, 'transport: shape upstream unavailable');
    return c.json({ error: 'transport upstream unavailable' }, 502);
  }
});

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

const TRIP_ID_RE = /^au2:[a-z]{2}:[A-Za-z0-9_.:-]+$/;
const STOP_ID_RE = /^au2:[A-Za-z0-9_.-]+$/;
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

transportRouter.get('/api/transport/trip/:date/:tripId/:instance', async (c) => {
  if (config.TRANSPORT_DISABLED) return c.json({ error: 'disabled' }, 404);
  const date = c.req.param('date');
  const tripId = c.req.param('tripId');
  const instance = c.req.param('instance');
  if (!/^\d{8}$/.test(date)) return c.json({ error: 'invalid date' }, 400);
  if (!TRIP_ID_RE.test(tripId)) return c.json({ error: 'invalid trip id' }, 400);
  if (!/^\d{1,3}$/.test(instance)) return c.json({ error: 'invalid instance' }, 400);
  const key = `t|${date}|${tripId}|${instance}`;
  try {
    const { value } = await tripCache.get(
      key,
      async () => {
        // tripId goes in RAW — upstream 404s on percent-encoded colons;
        // TRIP_ID_RE already restricts it to URL-safe characters.
        const raw = await fetchJson<RawTripDetailResponse>(
          `${ANYTRIP_BASE}/tripInstance/${date}/${tripId}/${instance}`,
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
    return c.json(value);
  } catch (err) {
    log.warn({ err, key }, 'transport: trip upstream unavailable');
    return c.json({ error: 'transport upstream unavailable' }, 502);
  }
});

transportRouter.get('/api/transport/departures/:stopId', async (c) => {
  if (config.TRANSPORT_DISABLED) return c.json({ error: 'disabled' }, 404);
  const stopId = c.req.param('stopId');
  if (!STOP_ID_RE.test(stopId)) return c.json({ error: 'invalid stop id' }, 400);
  const limit = Math.min(20, Math.max(1, Number(c.req.query('limit')) || 10));
  const key = `d|${stopId}|${limit}`;
  try {
    const { value } = await depCache.get(
      key,
      async () => {
        // stopId raw for the same percent-encoding reason (STOP_ID_RE
        // whitelists it).
        const raw = await fetchJson<RawDeparturesResponse>(
          `${ANYTRIP_BASE}/departures/${stopId}?limit=${limit}`,
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
    return c.json(value);
  } catch (err) {
    log.warn({ err, key }, 'transport: departures upstream unavailable');
    return c.json({ error: 'transport upstream unavailable' }, 502);
  }
});

// ---------------------------------------------------------------------
// Static NSW rail/metro/light-rail network geometry (AnyTrip's
// pre-styled GeoJSON, per-feature official line colours). ~434 KB raw,
// gzipped by the compress() middleware; cached a day.

// lines.json only covers greater Sydney; otherrail.json adds the few
// styled segments outside it (e.g. Canberra light rail). Merged into
// one FeatureCollection; either file failing alone is tolerated.
const LINES_URLS = [
  'https://static.anytrip.com.au/tiles/lines.json',
  'https://static.anytrip.com.au/tiles/otherrail.json',
];
const LINES_FRESH_MS = 24 * 3600_000;
const LINES_STALE_MS = 7 * 24 * 3600_000;
const linesCache = new SwrCache<unknown>(2);

interface GeoFeatureCollection {
  type?: string;
  features?: unknown[];
}

transportRouter.get('/api/transport/lines', async (c) => {
  if (config.TRANSPORT_DISABLED) {
    return c.json({ type: 'FeatureCollection', features: [], disabled: true });
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
        let ok = 0;
        for (const r of results) {
          if (r.status === 'fulfilled' && Array.isArray(r.value.features)) {
            features.push(...r.value.features);
            ok += 1;
          }
        }
        if (ok === 0) throw new Error('all line sources failed');
        return { type: 'FeatureCollection', features };
      },
      {
        fresh: LINES_FRESH_MS,
        stale: LINES_STALE_MS,
        onError: (err) => log.warn({ err }, 'transport: lines refresh failed'),
      },
    );
    return c.json(value as object);
  } catch (err) {
    log.warn({ err }, 'transport: lines upstream unavailable');
    return c.json({ error: 'transport upstream unavailable' }, 502);
  }
});

transportRouter.get('/api/transport/stops', async (c) => {
  const empty: TransportStopsSnapshot = {
    stops: [],
    count: 0,
    fetched_at: new Date().toISOString(),
  };
  if (config.TRANSPORT_DISABLED) return c.json({ ...empty, disabled: true });

  const bbox = parseBbox({
    minLat: c.req.query('minLat'),
    maxLat: c.req.query('maxLat'),
    minLon: c.req.query('minLon'),
    maxLon: c.req.query('maxLon'),
  });
  if (typeof bbox === 'string') return c.json({ error: bbox }, 400);
  const modes = parseListParam(c.req.query('modes'), STOP_MODES, 'mode');
  if (typeof modes === 'string') return c.json({ error: modes }, 400);

  const key = `s|${bboxKey(bbox)}|${modes.join(',')}`;
  try {
    const { value } = await stopsCache.get(
      key,
      async () => {
        const modeList = modes.map((m) => STOP_MODES[m]).join(',');
        const url =
          `${ANYTRIP_BASE}/stops?limit=500&modes=${encodeURIComponent(modeList)}` +
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
    return c.json(value);
  } catch (err) {
    log.warn({ err, key }, 'transport: stops upstream unavailable');
    return c.json({ error: 'transport upstream unavailable' }, 502);
  }
});

// Service alerts (official TfNSW GTFS-realtime alert feeds). Fixed
// path, no viewport scoping — safe for the CDN cache list. Returns
// `configured:false` (not an error) when no TFNSW_API_KEY is set so
// the frontend can hide the UI.
transportRouter.get('/api/transport/alerts', async (c) => {
  if (config.TRANSPORT_DISABLED) {
    return c.json({ alerts: [], count: 0, configured: false, disabled: true });
  }
  if (!tfnswConfigured()) {
    return c.json({ alerts: [], count: 0, configured: false });
  }
  try {
    const alerts = await fetchTfnswAlerts();
    return c.json({
      alerts,
      count: alerts.length,
      configured: true,
      fetched_at: new Date().toISOString(),
    });
  } catch (err) {
    log.warn({ err }, 'transport: alerts unavailable');
    return c.json({ error: 'alerts upstream unavailable' }, 502);
  }
});
