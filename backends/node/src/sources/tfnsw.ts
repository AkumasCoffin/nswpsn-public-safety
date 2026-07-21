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
  lr: [
    '/v1/gtfs/vehiclepos/lightrail/innerwest',
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

/** All TfNSW positions for the requested short feed codes. Feeds fetch
 * concurrently, each behind its own SWR window; one failed feed just
 * contributes nothing. Returns [] when unconfigured. */
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
  const results = await Promise.all(
    paths.map(async ({ path, mode }) => {
      try {
        const { value } = await positionsCache.get(
          path,
          async () => {
            const msg = await fetchFeed(path);
            // THROW on failure/park — returning [] here would cache an
            // empty batch as a SUCCESSFUL refresh, dropping last-good
            // data and flipping every matched vehicle to the AnyTrip
            // frame (the teleport glitch). The throw makes SwrCache
            // serve the previous batch through the stale window.
            if (!msg) throw new Error(`feed unavailable: ${path}`);
            return decodePositions(msg, mode);
          },
          {
            fresh: POS_FRESH_MS + feedJitterMs(path),
            stale: POS_STALE_MS,
            onError: (err) => log.warn({ err, path }, 'tfnsw: positions refresh failed'),
          },
        );
        return value;
      } catch {
        return [];
      }
    }),
  );
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

/**
 * Join: TfNSW positions become authoritative for AnyTrip vehicles with
 * a matching trip id; TfNSW-only trips inside the bbox are appended as
 * synthetic vehicles (mode colours, no shape — the frontend renders
 * them un-snapped, and the trip panel still works because AnyTrip's
 * tripInstance API accepts the GTFS-R trip id).
 */
export function applyTfnswPositions(
  vehicles: TransportVehicle[],
  positions: TfnswPosition[],
  bbox: { minLat: number; maxLat: number; minLon: number; maxLon: number },
  maxVehicles: number,
): { vehicles: TransportVehicle[]; matched: number; added: number } {
  if (!positions.length) return { vehicles, matched: 0, added: 0 };
  const byTrip = new Map<string, TfnswPosition>();
  for (const p of positions) {
    if (p.tripId) byTrip.set(p.tripId, p);
  }
  const nowSec = Date.now() / 1000;
  let matched = 0;
  const usedTrips = new Set<string>();
  const anytripByMode = new Map<string, number>();
  const matchedByMode = new Map<string, number>();
  for (const v of vehicles) {
    anytripByMode.set(v.mode, (anytripByMode.get(v.mode) ?? 0) + 1);
  }
  const out = vehicles.map((v) => {
    const p = v.tripId ? byTrip.get(v.tripId) : undefined;
    if (!p) return v;
    usedTrips.add(v.tripId as string);
    matched += 1;
    matchedByMode.set(v.mode, (matchedByMode.get(v.mode) ?? 0) + 1);
    // TfNSW telemetry is PRIMARY for matched vehicles (fresher, and
    // the official source). The one hard rule: never ALTERNATE frames
    // of reference — AnyTrip's interpolation runs up to ~30s of travel
    // ahead of the raw feed, so flip-flopping between them (e.g. on a
    // transient feed failure) teleports pins back and forth. Matched
    // vehicles therefore always take the TfNSW position (entities are
    // age-filtered at decode), and the positions cache holds last-good
    // data long enough to ride out fetch blips; the frontend's shape
    // snap, backward-jitter guard and glide smooth the raw telemetry.
    return {
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
    };
  });
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
    // Duplicate guard: if AnyTrip has vehicles of this mode in view
    // but NONE of them trip-matched, the feed's trip-id space differs
    // from AnyTrip's rtTripId — every "TfNSW-only" entity is then the
    // SAME physical vehicle AnyTrip already shows, and appending it
    // duplicates trains with degraded labels. Only append for modes
    // that are genuinely absent from AnyTrip or provably id-compatible.
    if (
      (anytripByMode.get(p.mode) ?? 0) > 0 &&
      (matchedByMode.get(p.mode) ?? 0) === 0
    ) {
      continue;
    }
    usedTrips.add(p.tripId);
    added += 1;
    out.push({
      id: p.vehicleId || `tfnsw:${p.tripId}`,
      lat: p.lat,
      lon: p.lon,
      bearing: p.bearing,
      speedKmh: p.speedKmh,
      mode: p.mode,
      route: {
        id: p.routeId,
        name: routeNameFromId(p.routeId),
        longName: null,
        color: null,
        textColor: null,
      },
      headsign: null,
      headsignSub: null,
      agency: null,
      occupancy: p.occupancy,
      wheelchair: null,
      aircon: null,
      model: p.label,
      ageSec:
        p.timestamp != null
          ? Math.max(0, Math.round(nowSec - p.timestamp))
          : null,
      tripId: p.tripId,
      shapeId: null,
      startDate: p.startDate,
      instanceNumber: 0,
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
}

/** TEST-ONLY: decode helpers exposed for fixture-based tests. */
export const _testables = { decodePositions, decodeAlerts, fetchFeed };
