/**
 * Transport for NSW Open Data — GTFS-realtime feeds (official source).
 *
 * Two jobs:
 *   1. PRIMARY vehicle positions: per-feed GTFS-R vehiclepos protobufs,
 *      decoded and joined onto the AnyTrip vehicle list by trip id.
 *      AnyTrip stays the metadata source (route colours/names, shapes,
 *      headsigns, trip-instance coordinates); TfNSW supplies the
 *      authoritative coordinates when it knows the trip.
 *   2. Service alerts: GTFS-R alert feeds normalized to JSON for
 *      /api/transport/alerts.
 *
 * Everything is gated on TFNSW_API_KEY — without it every export
 * no-ops and the transport endpoints behave exactly as before.
 *
 * Endpoint paths live in tables (FEED table per mode + alert feed
 * list) because TfNSW moves feeds between /v1 and /v2 over time; a
 * 404/410 on one feed is tolerated (logged once, skipped) so a moved
 * path never takes the layer down.
 */
import GtfsRealtimeBindings from 'gtfs-realtime-bindings';
import { fetchBuffer } from './shared/http.js';
import { SwrCache } from '../services/swrCache.js';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import type { TransportVehicle, TransportMode } from '../api/transport.js';

const rt = GtfsRealtimeBindings.transit_realtime;

const TFNSW_BASE = 'https://api.transport.nsw.gov.au';
const FEED_TIMEOUT_MS = 12_000;
// 15s + per-feed jitter: seven concurrent 10s-cadence fetches tripped
// TfNSW's burst rate limit (429s in prod) and ran the daily quota hot.
const POS_FRESH_MS = 15_000;
// Long stale window: while a refresh fails, serving the last-good
// decode keeps matched vehicles in the TfNSW frame of reference
// instead of snapping back to AnyTrip's (~30s ahead) interpolation —
// the alternation is what looked like random teleporting. Sized to
// bridge a full 2-min rate-limit park. Entities carry their own
// timestamps and age out at decode regardless.
const POS_STALE_MS = 300_000;
const ALERT_FRESH_MS = 60_000;
const ALERT_STALE_MS = 600_000;
// Positions older than this are dropped — a stale GTFS-R entity is
// worse than AnyTrip's interpolated position.
const POS_MAX_AGE_SEC = 120;

/** Vehicle-position feeds per short feed code (same codes the frontend
 * sends). Trains/metro moved to v2; the rest are v1. `sp` (school
 * services) rides in the buses feed upstream, so it has no feed of its
 * own. Light rail is split per line network upstream. */
const POSITION_FEEDS: Record<string, string[]> = {
  st: ['/v2/gtfs/vehiclepos/sydneytrains'],
  mt: ['/v2/gtfs/vehiclepos/metro'],
  nt: ['/v1/gtfs/vehiclepos/nswtrains'],
  fr: ['/v1/gtfs/vehiclepos/ferries/sydneyferries'],
  // innerwest removed: retired upstream (404s in prod) — L1 reports
  // via the remaining feeds.
  lr: [
    '/v1/gtfs/vehiclepos/lightrail/cbdandsoutheast',
    '/v1/gtfs/vehiclepos/lightrail/newcastle',
    '/v1/gtfs/vehiclepos/lightrail/parramatta',
  ],
  bs: ['/v1/gtfs/vehiclepos/buses'],
  sp: [],
};

const FEED_MODE: Record<string, TransportMode> = {
  st: 'sydneytrains',
  mt: 'metro',
  nt: 'nswtrains',
  fr: 'ferries',
  lr: 'lightrail',
  bs: 'buses',
  sp: 'buses',
};

/** Alert feeds, most-consolidated first. The v2 "all" feed covers every
 * mode; if it's unavailable we fall back to the per-mode v1 feeds. */
const ALERT_FEEDS_PRIMARY = ['/v2/gtfs/alerts/all'];
const ALERT_FEEDS_FALLBACK = [
  '/v1/gtfs/alerts/sydneytrains',
  '/v1/gtfs/alerts/nswtrains',
  '/v1/gtfs/alerts/metro',
  '/v1/gtfs/alerts/buses',
  '/v1/gtfs/alerts/ferries/sydneyferries',
  '/v1/gtfs/alerts/lightrail',
];

export const tfnswConfigured = (): boolean =>
  Boolean(config.TFNSW_API_KEY) && !config.TFNSW_DISABLED;

export interface TfnswPosition {
  tripId: string | null;
  routeId: string | null;
  startDate: string | null;
  lat: number;
  lon: number;
  bearing: number | null;
  speedKmh: number | null;
  timestamp: number | null; // epoch sec
  occupancy: number | null; // GTFS-R OccupancyStatus 0-6+ (we clamp 0-6)
  vehicleId: string | null;
  label: string | null;
  mode: TransportMode;
}

function authHeaders(): Record<string, string> {
  return { Authorization: `apikey ${config.TFNSW_API_KEY}` };
}

// Erroring feeds get PARKED (skipped without fetching) so they can't
// hammer upstream: 404/410 (moved or unlicensed for this key) for an
// hour, 429 (rate limited) for two minutes — retrying a rate limit on
// the regular cadence just feeds the limiter.
const _parkedFeeds = new Map<string, number>(); // path → parked-until ts
const DEAD_FEED_PARK_MS = 3_600_000;
const RATE_LIMIT_PARK_MS = 120_000;

function feedParked(path: string): boolean {
  const until = _parkedFeeds.get(path);
  if (until == null) return false;
  if (Date.now() >= until) {
    _parkedFeeds.delete(path);
    return false;
  }
  return true;
}

// Stable per-feed cache-window jitter (0-5s) so the feeds' refresh
// times drift apart instead of bursting together every window —
// concurrent bursts are what trip TfNSW's per-second rate limit.
function feedJitterMs(path: string): number {
  let h = 0;
  for (let i = 0; i < path.length; i += 1) h = (h * 31 + path.charCodeAt(i)) | 0;
  return Math.abs(h) % 5000;
}

async function fetchFeed(path: string): Promise<GtfsRealtimeBindings.transit_realtime.FeedMessage | null> {
  if (feedParked(path)) return null;
  try {
    const buf = await fetchBuffer(`${TFNSW_BASE}${path}`, {
      timeoutMs: FEED_TIMEOUT_MS,
      headers: authHeaders(),
    });
    return rt.FeedMessage.decode(buf);
  } catch (err) {
    const status = (err as { status?: number | null }).status;
    if (status === 404 || status === 410) {
      _parkedFeeds.set(path, Date.now() + DEAD_FEED_PARK_MS);
      log.warn({ path, status }, 'tfnsw: feed unavailable — parked for 1h');
    } else if (status === 429) {
      _parkedFeeds.set(path, Date.now() + RATE_LIMIT_PARK_MS);
      log.warn({ path }, 'tfnsw: rate limited — parked for 2min');
    } else {
      log.warn({ err, path }, 'tfnsw: feed fetch failed');
    }
    return null;
  }
}

// protobufjs serves DEFAULT VALUES (0) through the message prototype
// for fields absent from the wire — `pos.bearing === 0` can mean
// "heading north" or "no bearing at all". Only own properties were
// actually decoded; treating defaults as data pointed every
// bearing-less vehicle north and overrode real AnyTrip speeds and
// occupancy with zeros.
const hasOwn = (obj: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(obj, key);

function decodePositions(
  msg: GtfsRealtimeBindings.transit_realtime.FeedMessage,
  mode: TransportMode,
): TfnswPosition[] {
  const nowSec = Date.now() / 1000;
  const out: TfnswPosition[] = [];
  for (const entity of msg.entity ?? []) {
    const v = entity.vehicle;
    const pos = v?.position;
    if (!v || !pos) continue;
    const lat = pos.latitude;
    const lon = pos.longitude;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    const ts =
      hasOwn(v, 'timestamp') && Number(v.timestamp) > 0
        ? Number(v.timestamp)
        : null;
    if (ts != null && nowSec - ts > POS_MAX_AGE_SEC) continue;
    const occ =
      hasOwn(v, 'occupancyStatus') &&
      typeof v.occupancyStatus === 'number' &&
      v.occupancyStatus >= 0
        ? Math.min(6, v.occupancyStatus)
        : null;
    out.push({
      tripId: v.trip?.tripId || null,
      routeId: v.trip?.routeId || null,
      startDate: v.trip?.startDate || null,
      lat,
      lon,
      bearing:
        hasOwn(pos, 'bearing') && Number.isFinite(pos.bearing)
          ? (pos.bearing as number)
          : null,
      speedKmh:
        hasOwn(pos, 'speed') && Number.isFinite(pos.speed) && (pos.speed as number) >= 0
          ? Math.round((pos.speed as number) * 36) / 10
          : null,
      timestamp: ts,
      occupancy: occ,
      vehicleId: v.vehicle?.id || null,
      label: v.vehicle?.label || null,
      mode,
    });
  }
  return out;
}

const positionsCache = new SwrCache<TfnswPosition[]>(20);

// Bounded-concurrency map: TfNSW rate-limits ~5 req/s and a burst of
// 6+ simultaneous feed fetches trips it (429s in prod). Two at a time
// spreads a full refresh over a second or two.
async function mapLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const idx = next;
      next += 1;
      results[idx] = await fn(items[idx] as T);
    }
  });
  await Promise.all(workers);
  return results;
}

/** All TfNSW positions for the requested short feed codes, each feed
 * behind its own SWR window; a failed feed serves last-good data
 * through the stale window and contributes nothing after that.
 * Returns [] when unconfigured. */
export async function fetchTfnswPositions(
  feedCodes: string[],
): Promise<TfnswPosition[]> {
  if (!tfnswConfigured()) return [];
  const paths: Array<{ path: string; mode: TransportMode }> = [];
  for (const code of feedCodes) {
    for (const path of POSITION_FEEDS[code] ?? []) {
      paths.push({ path, mode: FEED_MODE[code] ?? 'other' });
    }
  }
  const results = await mapLimit(paths, 2, async ({ path, mode }) => {
    try {
      const { value } = await positionsCache.get(
        path,
        async () => {
          // Parked feeds throw QUIETLY (no fetch, no warn) — a parked
          // 404 feed otherwise dumped a stack trace every window.
          if (feedParked(path)) {
            throw Object.assign(new Error(`feed parked: ${path}`), { parked: true });
          }
          const msg = await fetchFeed(path);
          // THROW on failure — returning [] here would cache an empty
          // batch as a SUCCESSFUL refresh, dropping last-good data and
          // flipping every matched vehicle to the AnyTrip frame (the
          // teleport glitch). The throw makes SwrCache serve the
          // previous batch through the stale window.
          if (!msg) throw new Error(`feed unavailable: ${path}`);
          return decodePositions(msg, mode);
        },
        {
          fresh: POS_FRESH_MS + feedJitterMs(path),
          stale: POS_STALE_MS,
          onError: (err) => {
            if (!(err as { parked?: boolean }).parked) {
              log.warn({ err, path }, 'tfnsw: positions refresh failed');
            }
          },
        },
      );
      return value;
    } catch {
      return [] as TfnswPosition[];
    }
  });
  return results.flat();
}

/** Best-effort route short-name from a GTFS route_id ("CTY_T1" → "T1",
 * "SMNW_M1" → "M1"). Only used for TfNSW-only vehicles AnyTrip doesn't
 * know — matched vehicles keep AnyTrip's metadata. */
export function routeNameFromId(routeId: string | null): string | null {
  if (!routeId) return null;
  const m = /(?:^|[_-])(T\d{1,2}|M\d{1,2}|L\d|F\d{1,2}|CCN|HUN|STH|WST|NTH)(?![A-Za-z0-9])/i.exec(
    routeId,
  );
  if (m?.[1]) return m[1].toUpperCase();
  const tail = routeId.split(/[_:]/).pop() ?? '';
  return /^[A-Za-z0-9]{1,4}$/.test(tail) ? tail.toUpperCase() : null;
}

// Per-trip TfNSW position stickiness. GTFS-R entities BLINK — a vehicle
// can miss one decode batch and reappear next tick. Remember each trip's
// last TfNSW position and keep serving it through short blinks so the
// marker holds still rather than vanishing for a beat.
const TFNSW_STICKY_MS = 60_000;
const _lastByTrip = new Map<string, { p: TfnswPosition; at: number }>();

function _rememberPositions(positions: TfnswPosition[]): void {
  const now = Date.now();
  for (const p of positions) {
    if (p.tripId) _lastByTrip.set(p.tripId, { p, at: now });
  }
  if (_lastByTrip.size > 4000) {
    for (const [k, v] of _lastByTrip) {
      if (now - v.at > TFNSW_STICKY_MS) _lastByTrip.delete(k);
    }
  }
}

// Per-trip AnyTrip METADATA cache (everything except position). Lets a
// train keep its stable id, route colours/name, headsign and — crucially
// — its route SHAPE (so it still snaps to track) on the ticks AnyTrip
// drops it while TfNSW still reports a position. Without this a matched
// train would flip to a synthetic marker with a new id and no shape,
// which recreated the marker (teleport) and lost route-snapping.
interface TripMeta {
  id: string;
  mode: TransportMode;
  route: TransportVehicle['route'];
  headsign: string | null;
  headsignSub: string | null;
  agency: string | null;
  wheelchair: boolean | null;
  aircon: boolean | null;
  model: string | null;
  shapeId: string | null;
  startDate: string | null;
  instanceNumber: number | null;
  at: number;
}
const _metaByTrip = new Map<string, TripMeta>();

function _rememberMeta(v: TransportVehicle, now: number): void {
  if (!v.tripId) return;
  _metaByTrip.set(v.tripId, {
    id: v.id,
    mode: v.mode,
    route: v.route,
    headsign: v.headsign,
    headsignSub: v.headsignSub,
    agency: v.agency,
    wheelchair: v.wheelchair,
    aircon: v.aircon,
    model: v.model,
    shapeId: v.shapeId,
    startDate: v.startDate,
    instanceNumber: v.instanceNumber,
    at: now,
  });
  if (_metaByTrip.size > 4000) {
    for (const [k, m] of _metaByTrip) {
      if (now - m.at > TFNSW_STICKY_MS) _metaByTrip.delete(k);
    }
  }
}

/**
 * TfNSW GTFS-Realtime is the SINGLE source of vehicle POSITIONS — AnyTrip
 * (which derives its own positions from the same TfNSW feed, interpolated
 * ~30s ahead) is used only for metadata: route colours/names, headsigns,
 * shapes and trip-instance ids. Mixing the two produced the "trains jump
 * around" behaviour (two frames of reference ~30s apart alternating).
 *
 * So: every emitted vehicle is positioned by a real TfNSW entity (fresh,
 * or its last position through a short blink). An AnyTrip vehicle with no
 * TfNSW position is DROPPED rather than shown at AnyTrip's interpolated
 * coordinates. TfNSW trips AnyTrip doesn't list are emitted too, reusing
 * cached AnyTrip metadata (stable id + shape) when we've seen the trip.
 */
export function applyTfnswPositions(
  vehicles: TransportVehicle[],
  positions: TfnswPosition[],
  bbox: { minLat: number; maxLat: number; minLon: number; maxLon: number },
  maxVehicles: number,
): { vehicles: TransportVehicle[]; matched: number; added: number } {
  if (!positions.length) {
    // No TfNSW positions at all this cycle → we have no authoritative
    // location for anything. Serve nothing rather than fall back to
    // AnyTrip's interpolated frame; the positions SWR cache already
    // bridges transient upstream failures with last-good data.
    return { vehicles: [], matched: 0, added: 0 };
  }
  _rememberPositions(positions);
  const now = Date.now();
  const nowSec = now / 1000;
  const byTrip = new Map<string, TfnswPosition>();
  for (const p of positions) {
    if (p.tripId) byTrip.set(p.tripId, p);
  }

  const out: TransportVehicle[] = [];
  const usedTrips = new Set<string>();
  let matched = 0;

  // Pass 1 — AnyTrip vehicles positioned by TfNSW (fresh, else sticky).
  // AnyTrip contributes metadata only; no TfNSW position ⇒ dropped.
  for (const v of vehicles) {
    if (!v.tripId) continue; // no key to match a TfNSW position → drop
    _rememberMeta(v, now);
    let p = byTrip.get(v.tripId);
    if (!p) {
      const mem = _lastByTrip.get(v.tripId);
      if (mem && now - mem.at < TFNSW_STICKY_MS) p = mem.p;
    }
    if (!p) continue; // no authoritative position → drop
    usedTrips.add(v.tripId);
    matched += 1;
    out.push({
      ...v,
      lat: p.lat,
      lon: p.lon,
      bearing: p.bearing ?? v.bearing,
      speedKmh: p.speedKmh ?? v.speedKmh,
      occupancy: p.occupancy ?? v.occupancy,
      ageSec:
        p.timestamp != null
          ? Math.max(0, Math.round(nowSec - p.timestamp))
          : v.ageSec,
    });
  }

  // Pass 2 — TfNSW trips AnyTrip didn't list this tick. Real vehicles;
  // reuse cached AnyTrip metadata (stable id + shape colour/name) so a
  // train that AnyTrip momentarily drops keeps its marker and snapping.
  let added = 0;
  for (const p of positions) {
    if (out.length >= maxVehicles) break;
    if (!p.tripId || usedTrips.has(p.tripId)) continue;
    if (
      p.lat < bbox.minLat || p.lat > bbox.maxLat ||
      p.lon < bbox.minLon || p.lon > bbox.maxLon
    ) {
      continue;
    }
    usedTrips.add(p.tripId);
    added += 1;
    const meta = _metaByTrip.get(p.tripId);
    out.push({
      // Stable id: the id AnyTrip assigned this trip if we've seen it,
      // so a matched↔TfNSW-only flip retargets the SAME marker instead
      // of recreating it (which teleported the pin).
      id: meta?.id || p.vehicleId || `tfnsw:${p.tripId}`,
      lat: p.lat,
      lon: p.lon,
      bearing: p.bearing,
      speedKmh: p.speedKmh,
      mode: meta?.mode ?? p.mode,
      route: meta?.route ?? {
        id: p.routeId,
        name: routeNameFromId(p.routeId),
        longName: null,
        color: null,
        textColor: null,
      },
      headsign: meta?.headsign ?? null,
      headsignSub: meta?.headsignSub ?? null,
      agency: meta?.agency ?? null,
      occupancy: p.occupancy,
      wheelchair: meta?.wheelchair ?? null,
      aircon: meta?.aircon ?? null,
      model: meta?.model ?? p.label,
      ageSec:
        p.timestamp != null
          ? Math.max(0, Math.round(nowSec - p.timestamp))
          : null,
      tripId: p.tripId,
      // Cached shape lets the frontend keep snapping this train to its
      // route track even on ticks AnyTrip drops it.
      shapeId: meta?.shapeId ?? null,
      startDate: meta?.startDate ?? p.startDate,
      instanceNumber: meta?.instanceNumber ?? 0,
    });
  }
  return { vehicles: out, matched, added };
}

// ---------------------------------------------------------------------
// Service alerts
// ---------------------------------------------------------------------

export interface TfnswAlert {
  id: string;
  header: string;
  description: string | null;
  url: string | null;
  cause: string | null;
  effect: string | null;
  severity: string | null;
  start: number | null; // epoch sec
  end: number | null;
  routes: string[]; // informed route ids
  stops: string[];
  agencies: string[];
}

function firstText(
  ts: GtfsRealtimeBindings.transit_realtime.ITranslatedString | null | undefined,
): string | null {
  const tr = ts?.translation;
  if (!tr?.length) return null;
  const en = tr.find((t) => !t.language || t.language.startsWith('en'));
  return (en ?? tr[0])?.text || null;
}

function enumName(
  enums: Record<string, number>,
  value: number | null | undefined,
): string | null {
  if (value == null) return null;
  for (const [k, v] of Object.entries(enums)) {
    if (v === value) return k;
  }
  return null;
}

function decodeAlerts(
  msg: GtfsRealtimeBindings.transit_realtime.FeedMessage,
): TfnswAlert[] {
  const out: TfnswAlert[] = [];
  for (const entity of msg.entity ?? []) {
    const a = entity.alert;
    if (!a) continue;
    const header = firstText(a.headerText);
    if (!header) continue;
    const periods = a.activePeriod ?? [];
    const first = periods[0];
    const routes = new Set<string>();
    const stops = new Set<string>();
    const agencies = new Set<string>();
    for (const ie of a.informedEntity ?? []) {
      if (ie.routeId) routes.add(ie.routeId);
      if (ie.trip?.routeId) routes.add(ie.trip.routeId);
      if (ie.stopId) stops.add(ie.stopId);
      if (ie.agencyId) agencies.add(ie.agencyId);
    }
    out.push({
      id: entity.id || header.slice(0, 64),
      header,
      description: firstText(a.descriptionText),
      url: firstText(a.url),
      cause: enumName(rt.Alert.Cause as unknown as Record<string, number>, a.cause),
      effect: enumName(rt.Alert.Effect as unknown as Record<string, number>, a.effect),
      severity: enumName(
        rt.Alert.SeverityLevel as unknown as Record<string, number>,
        a.severityLevel,
      ),
      start: first?.start != null && Number(first.start) > 0 ? Number(first.start) : null,
      end: first?.end != null && Number(first.end) > 0 ? Number(first.end) : null,
      routes: Array.from(routes),
      stops: Array.from(stops),
      agencies: Array.from(agencies),
    });
  }
  return out;
}

const alertsCache = new SwrCache<TfnswAlert[]>(2);

export async function fetchTfnswAlerts(): Promise<TfnswAlert[]> {
  if (!tfnswConfigured()) return [];
  const { value } = await alertsCache.get(
    'alerts',
    async () => {
      // Consolidated v2 feed first; per-mode v1 fallbacks otherwise.
      for (const path of ALERT_FEEDS_PRIMARY) {
        const msg = await fetchFeed(path);
        if (msg) return decodeAlerts(msg);
      }
      const parts = await Promise.all(
        ALERT_FEEDS_FALLBACK.map(async (path) => {
          const msg = await fetchFeed(path);
          return msg ? decodeAlerts(msg) : [];
        }),
      );
      // Dedupe by id across per-mode feeds.
      const seen = new Map<string, TfnswAlert>();
      for (const a of parts.flat()) {
        if (!seen.has(a.id)) seen.set(a.id, a);
      }
      return Array.from(seen.values());
    },
    {
      fresh: ALERT_FRESH_MS,
      stale: ALERT_STALE_MS,
      onError: (err) => log.warn({ err }, 'tfnsw: alerts refresh failed'),
    },
  );
  return value;
}

/** TEST-ONLY */
export function _resetTfnswForTests(): void {
  positionsCache.clear();
  alertsCache.clear();
  _parkedFeeds.clear();
  _lastByTrip.clear();
  _metaByTrip.clear();
}

/** TEST-ONLY: decode helpers exposed for fixture-based tests. */
export const _testables = { decodePositions, decodeAlerts, fetchFeed };
