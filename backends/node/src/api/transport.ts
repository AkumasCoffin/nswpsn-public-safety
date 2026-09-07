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
  tfnswPositionsEnabled,
} from '../sources/tfnsw.js';

export const transportRouter = new Hono();

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

  const parsedRegion = parseRegion(c.req.query('region'));
  if (typeof parsedRegion === 'string') return c.json({ error: parsedRegion }, 400);
  const { id: regionId, region } = parsedRegion;
  const bbox = parseBbox({
    minLat: c.req.query('minLat'),
    maxLat: c.req.query('maxLat'),
    minLon: c.req.query('minLon'),
    maxLon: c.req.query('maxLon'),
  }, region.bounds);
  if (typeof bbox === 'string') return c.json({ error: bbox }, 400);
  // Selector: au2 filters upstream by its per-mode feeds; the other
  // regions' feeds carry every mode, so modes= is the filter there
  // (au4 also names its `se` feed; au3/au5/au9 send modes alone).
  let selector: string[];
  let upstreamFilter: string;
  if (region.vehicleModes) {
    const modes = parseListParam(c.req.query('modes'), region.vehicleModes, 'mode');
    if (typeof modes === 'string') return c.json({ error: modes }, 400);
    selector = modes;
    const feedList = Object.values(region.vehicleFeeds).join(',');
    const modeList = modes.map((m) => region.vehicleModes![m]).join(',');
    upstreamFilter =
      (feedList ? `feeds=${encodeURIComponent(feedList)}&` : '') +
      `modes=${encodeURIComponent(modeList)}`;
  } else {
    const feeds = parseListParam(c.req.query('feeds'), region.vehicleFeeds, 'feed');
    if (typeof feeds === 'string') return c.json({ error: feeds }, 400);
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
// au2:ds:dyn:918-841-289 (verified served by upstream). The region is
// derived from the id's own prefix (au2:/au4:), so no ?region= param.
const SHAPE_ID_RE = /^au\d+:[a-z]{2}:[A-Za-z0-9_.:-]+$/;
const SHAPE_FRESH_MS = 24 * 3600_000;
const SHAPE_STALE_MS = 7 * 24 * 3600_000;

interface RawShapeResponse {
  response?: { shape?: { id?: string; enc?: string } };
}

transportRouter.get('/api/transport/shape/:id', async (c) => {
  if (config.TRANSPORT_DISABLED) return c.json({ id: '', enc: null, disabled: true });
  const id = c.req.param('id').trim();
  if (!SHAPE_ID_RE.test(id)) return c.json({ error: 'invalid shape id' }, 400);
  const shapeRegion = regionForId(id);
  if (!shapeRegion) return c.json({ error: 'unknown region' }, 400);
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

transportRouter.get('/api/transport/trip/:date/:tripId/:instance', async (c) => {
  if (config.TRANSPORT_DISABLED) return c.json({ error: 'disabled' }, 404);
  const date = c.req.param('date');
  const tripId = c.req.param('tripId');
  const instance = c.req.param('instance');
  if (!/^\d{8}$/.test(date)) return c.json({ error: 'invalid date' }, 400);
  if (!TRIP_ID_RE.test(tripId)) return c.json({ error: 'invalid trip id' }, 400);
  if (!/^\d{1,3}$/.test(instance)) return c.json({ error: 'invalid instance' }, 400);
  const tripRegion = regionForId(tripId);
  if (!tripRegion) return c.json({ error: 'unknown region' }, 400);
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
  const stopRegion = regionForId(stopId);
  if (!stopRegion) return c.json({ error: 'unknown region' }, 400);
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

  const parsedRegion = parseRegion(c.req.query('region'));
  if (typeof parsedRegion === 'string') return c.json({ error: parsedRegion }, 400);
  const { id: regionId, region } = parsedRegion;
  const bbox = parseBbox({
    minLat: c.req.query('minLat'),
    maxLat: c.req.query('maxLat'),
    minLon: c.req.query('minLon'),
    maxLon: c.req.query('maxLon'),
  }, region.bounds);
  if (typeof bbox === 'string') return c.json({ error: bbox }, 400);
  const modes = parseListParam(c.req.query('modes'), region.stopModes, 'mode');
  if (typeof modes === 'string') return c.json({ error: modes }, 400);

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
