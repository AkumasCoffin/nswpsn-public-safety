/**
 * The 3D coverage picture of each ADS-B receiver: where, by bearing, distance
 * AND altitude, it has heard aircraft — and where aircraft passed inside its
 * line of sight without being heard.
 *
 * The polar envelope (adsbCoverage.ts) keeps one number per bearing, which
 * cannot tell a receiver that hears airliners at 300 km but nothing below
 * 3,000 ft past the next hill from one that hears everything to 200 km. This
 * keeps a sparse voxel count instead: 500 m altitude slices x 5-degree
 * bearings x 10 km rings, one integer per occupied cell, per node per day.
 *
 * "Missed" comes from comparing the receiver against the public feed the
 * backend already polls. An aircraft that feed saw, airborne, inside this
 * receiver's terrain line-of-sight limit for its altitude, that the receiver
 * has not reported for a minute and a half, counts as missed in its cell. The
 * limit matters: past it, not hearing an aircraft is physics, not a fault.
 */
import { getPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';
import { formatSydneyNaive } from '../../lib/sydneyTime.js';
import type { AdsbAircraft } from '../../sources/adsb.js';
import { bearingDeg, bucketOf, emptyEnvelope } from './adsbCoverage.js';
import { distanceKm } from './adsbNodeStore.js';
import {
  BEARINGS, DEFAULT_ANTENNA_HEIGHT_M, MAX_KM, SLICES, SLICE_M,
  cachedTerrain, horizonKm,
} from './adsbTerrain.js';

export const RING_KM = 10;
export const RINGS = MAX_KM / RING_KM;

/** Below this many sightings a heard cell is not drawn: one CPR decode error
 *  puts an aircraft somewhere it never was, once. */
const MIN_HEARD = 2;
/** A missed cell needs two separate polls to agree too. */
const MIN_MISSED = 2;
/** How long after the receiver last reported an aircraft it still counts as
 *  heard. An upload every few seconds, so 90 s is generous. */
const HEARD_GRACE_MS = 90_000;
/** A receiver counts toward missed aircraft only while it is uploading. */
const ACTIVE_MS = 30_000;
/** Public-feed positions older than this are not compared. */
const MAX_FIX_AGE_SEC = 15;
/** Cap on cells returned to a client, keeping the busiest. */
const MAX_CELLS_OUT = 30_000;

export function cellOf(slice: number, bearing: number, ring: number): number {
  return (slice * BEARINGS + bearing) * RINGS + ring;
}

/** Altitude slice for a barometric altitude in feet; null when unknown. */
export function sliceOf(altFt: number | null | undefined): number | null {
  if (typeof altFt !== 'number' || !Number.isFinite(altFt)) return null;
  const m = altFt * 0.3048;
  return Math.min(SLICES - 1, Math.max(0, Math.floor(m / SLICE_M)));
}

interface Pending {
  heard: Map<number, number>;
  missed: Map<number, number>;
  /** False until the stored row for this day has been added in. */
  hydrated: boolean;
  /** A cell has been counted since the last write. */
  dirty: boolean;
  /** When this entry was last written (0: never). */
  flushedMs: number;
}

/**
 * How often a day's cells are rewritten while they keep changing.
 *
 * The two cell maps are one large JSONB value each, and a write replaces the
 * whole value. Every minute, for every receiver seen that day, changed or
 * not, that was hundreds of megabytes a day of dead rows and WAL per receiver
 * on a slow disk. Nothing reads the stored row between passes (the view adds
 * memory on top), so ten minutes loses nothing but ten minutes of cells if
 * the process dies without its shutdown flush.
 */
const FLUSH_EVERY_MS = 10 * 60_000;

const pending = new Map<string, Pending>();

interface Pin { lat: number; lon: number; heightM: number; lastMs: number }
const pins = new Map<string, Pin>();
/** Per node: hex -> last time the receiver reported it. */
const heardAt = new Map<string, Map<string, number>>();
const lastPrune = new Map<string, number>();

function sydneyDay(ms: number): string {
  return formatSydneyNaive(ms).slice(0, 10);
}

function dayEntry(nodeId: string, nowMs: number): Pending {
  const key = `${nodeId}|${sydneyDay(nowMs)}`;
  let cur = pending.get(key);
  if (!cur) {
    cur = { heard: new Map(), missed: new Map(), hydrated: false, dirty: false, flushedMs: 0 };
    pending.set(key, cur);
  }
  return cur;
}

function cellFor(
  site: { lat: number; lon: number }, lat: number, lon: number, altFt: number | null,
): number | null {
  const slice = sliceOf(altFt);
  if (slice === null || !Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const km = distanceKm(site.lat, site.lon, lat, lon);
  if (km >= MAX_KM) return null;
  const ring = Math.floor(km / RING_KM);
  return cellOf(slice, bucketOf(bearingDeg(site.lat, site.lon, lat, lon)), ring);
}

function bump(cur: Pending, m: Map<number, number>, cell: number): void {
  const n = m.get(cell) ?? 0;
  if (n >= 65_535) return; // saturated: nothing changes, nothing to write
  m.set(cell, n + 1);
  cur.dirty = true;
}

/**
 * Fold one upload: every airborne position (the current one and the trail the
 * agent carried) into the heard cells, and every aircraft into the receiver's
 * "reported recently" set that the missed count checks against.
 */
export function foldLightcone(
  nodeId: string,
  lat: unknown,
  lon: unknown,
  heightM: unknown,
  records: ReadonlyArray<AdsbAircraft>,
  trails: ReadonlyMap<string, ReadonlyArray<readonly [number, number, number, number | null]>>,
  nowMs: number = Date.now(),
): void {
  if (typeof lat !== 'number' || typeof lon !== 'number') return;
  const h = typeof heightM === 'number' && Number.isFinite(heightM) ? heightM : DEFAULT_ANTENNA_HEIGHT_M;
  pins.set(nodeId, { lat, lon, heightM: h, lastMs: nowMs });
  const site = { lat, lon };
  const cur = dayEntry(nodeId, nowMs);

  let seen = heardAt.get(nodeId);
  if (!seen) { seen = new Map(); heardAt.set(nodeId, seen); }

  for (const r of records) {
    seen.set(r.hex, nowMs);
    if (r.onGround) continue;
    const c = cellFor(site, r.lat, r.lon, r.altFt);
    if (c !== null) bump(cur, cur.heard, c);
    const trail = trails.get(r.hex);
    if (!trail) continue;
    for (const p of trail) {
      const tc = cellFor(site, p[1], p[2], p[3]);
      if (tc !== null) bump(cur, cur.heard, tc);
    }
  }

  if (nowMs - (lastPrune.get(nodeId) ?? 0) > 60_000) {
    lastPrune.set(nodeId, nowMs);
    for (const [hex, ms] of seen) if (nowMs - ms > HEARD_GRACE_MS) seen.delete(hex);
  }
}

/**
 * Compare one public-feed poll against every uploading receiver and count the
 * aircraft each should have heard but has not reported.
 */
export function noteNetworkAircraft(
  aircraft: ReadonlyArray<AdsbAircraft>,
  nowMs: number = Date.now(),
): void {
  for (const [nodeId, pin] of pins) {
    if (nowMs - pin.lastMs > ACTIVE_MS) continue;
    const seen = heardAt.get(nodeId);
    const terrain = cachedTerrain(nodeId, pin.lat, pin.lon, pin.heightM);
    const dLat = MAX_KM / 111;
    const dLon = dLat / Math.max(0.2, Math.cos(pin.lat * Math.PI / 180));
    let cur: Pending | null = null;
    for (const a of aircraft) {
      if (a.estimated || a.onGround || a.ageSec > MAX_FIX_AGE_SEC) continue;
      if (Math.abs(a.lat - pin.lat) > dLat || Math.abs(a.lon - pin.lon) > dLon) continue;
      const last = seen?.get(a.hex);
      if (last !== undefined && nowMs - last < HEARD_GRACE_MS) continue;
      const slice = sliceOf(a.altFt);
      // Below 500 m nothing is expected past the nearest rooftops.
      if (slice === null || slice === 0) continue;
      const km = distanceKm(pin.lat, pin.lon, a.lat, a.lon);
      if (km >= MAX_KM) continue;
      const b = bucketOf(bearingDeg(pin.lat, pin.lon, a.lat, a.lon));
      // The limit at the slice's FLOOR (the top of the slice below): the
      // conservative reading, so an aircraft is only called missed where the
      // whole slice it is in was in view.
      const limit = terrain
        ? terrain.limits[slice - 1]![b]!
        : horizonKm(pin.heightM, slice * SLICE_M);
      if (km > limit) continue;
      cur ??= dayEntry(nodeId, nowMs);
      bump(cur, cur.missed, cellOf(slice, b, Math.floor(km / RING_KM)));
    }
  }
}

function toObject(m: Map<number, number>): Record<string, number> {
  const o: Record<string, number> = {};
  for (const [k, v] of m) o[String(k)] = v;
  return o;
}

function addObject(into: Map<number, number>, o: unknown): void {
  if (!o || typeof o !== 'object' || Array.isArray(o)) return;
  for (const [k, v] of Object.entries(o as Record<string, unknown>)) {
    const cell = Number(k);
    if (!Number.isInteger(cell) || cell < 0 || typeof v !== 'number' || !Number.isFinite(v)) continue;
    into.set(cell, Math.min(65_535, (into.get(cell) ?? 0) + v));
  }
}

/**
 * Write the day's cells, alongside the envelope in node_adsb_coverage. Same
 * shape as flushAdsbCoverage: the first write of a (node, day) folds in what
 * is already stored, after which memory is the superset and a replace is
 * correct.
 */
export async function flushAdsbLightcone(
  /** Write every changed entry now, whatever its age (shutdown). */
  force: boolean = false,
  nowMs: number = Date.now(),
): Promise<number> {
  if (pending.size === 0) return 0;
  const pool = await getPool();
  if (!pool) return 0;
  let written = 0;
  const today = sydneyDay(nowMs);
  for (const [key, cur] of Array.from(pending.entries())) {
    const sep = key.lastIndexOf('|');
    const nodeId = key.slice(0, sep);
    const day = key.slice(sep + 1);
    // Unchanged since the last write: nothing to say. Changed but written
    // recently: wait, unless the day is over (its last write) or we are
    // shutting down.
    if (!cur.dirty) continue;
    if (!force && day === today && nowMs - cur.flushedMs < FLUSH_EVERY_MS) continue;
    try {
      if (!cur.hydrated) {
        const prev = await pool.query<{ heard: unknown; missed: unknown }>(
          'SELECT heard, missed FROM node_adsb_coverage WHERE node_id = $1 AND day = $2::date',
          [nodeId, day],
        );
        addObject(cur.heard, prev.rows[0]?.heard);
        addObject(cur.missed, prev.rows[0]?.missed);
        cur.hydrated = true;
      }
      await pool.query(
        `INSERT INTO node_adsb_coverage (node_id, day, buckets, heard, missed)
         VALUES ($1, $2::date, $3::jsonb, $4::jsonb, $5::jsonb)
         ON CONFLICT (node_id, day) DO UPDATE SET heard = EXCLUDED.heard, missed = EXCLUDED.missed`,
        [nodeId, day, JSON.stringify(emptyEnvelope()),
          JSON.stringify(toObject(cur.heard)), JSON.stringify(toObject(cur.missed))],
      );
      cur.dirty = false;
      cur.flushedMs = nowMs;
      written += 1;
    } catch (err) {
      log.debug({ err, nodeId, day }, 'adsb lightcone flush failed');
    }
  }
  // A finished day goes once it has been written; one whose write failed
  // stays for the next pass.
  for (const [key, cur] of Array.from(pending.entries())) {
    if (!key.endsWith(`|${today}`) && !cur.dirty) pending.delete(key);
  }
  return written;
}

export interface LightconeView {
  ringKm: number;
  rings: number;
  bearings: number;
  sliceM: number;
  slices: number;
  /** Cells heard at least twice, and how often, in parallel arrays. */
  heard: number[];
  heardN: number[];
  /** Cells missed at least twice and never (reliably) heard. */
  missed: number[];
  daysCovered: number;
}

function topCells(m: Map<number, number>, min: number, skip?: Map<number, number>): Array<[number, number]> {
  const out: Array<[number, number]> = [];
  for (const [cell, n] of m) {
    if (n < min) continue;
    if (skip && (skip.get(cell) ?? 0) >= MIN_HEARD) continue;
    out.push([cell, n]);
  }
  if (out.length > MAX_CELLS_OUT) {
    out.sort((a, b) => b[1] - a[1]);
    out.length = MAX_CELLS_OUT;
  }
  return out.sort((a, b) => a[0] - b[0]);
}

/** One receiver's cells over a trailing window of days. */
export async function adsbLightconeFor(
  nodeId: string,
  days: number,
  nowMs: number = Date.now(),
): Promise<LightconeView | null> {
  const pool = await getPool();
  if (!pool) return null;
  const r = await pool.query<{ day: string; heard: unknown; missed: unknown }>(
    `SELECT to_char(day, 'YYYY-MM-DD') AS day, heard, missed
       FROM node_adsb_coverage
      WHERE node_id = $1
        AND heard IS NOT NULL
        AND day > (now() AT TIME ZONE 'Australia/Sydney')::date - $2::int`,
    [nodeId, days],
  );
  const heard = new Map<number, number>();
  const missed = new Map<number, number>();
  const today = sydneyDay(nowMs);
  const live = pending.get(`${nodeId}|${today}`);
  const dayset = new Set<string>();
  for (const row of r.rows) {
    dayset.add(row.day);
    // Today's stored row is already inside memory once hydrated; adding both
    // would count the morning twice.
    if (row.day === today && live?.hydrated) continue;
    addObject(heard, row.heard);
    addObject(missed, row.missed);
  }
  if (live) {
    dayset.add(today);
    for (const [c, n] of live.heard) heard.set(c, Math.min(65_535, (heard.get(c) ?? 0) + n));
    for (const [c, n] of live.missed) missed.set(c, Math.min(65_535, (missed.get(c) ?? 0) + n));
  }
  const daysCovered = dayset.size;
  const h = topCells(heard, MIN_HEARD);
  const m = topCells(missed, MIN_MISSED, heard);
  return {
    ringKm: RING_KM,
    rings: RINGS,
    bearings: BEARINGS,
    sliceM: SLICE_M,
    slices: SLICES,
    heard: h.map((x) => x[0]),
    heardN: h.map((x) => x[1]),
    missed: m.map((x) => x[0]),
    daysCovered,
  };
}

/** Forget a deleted node. Stored rows go with the node by ON DELETE CASCADE. */
export function clearAdsbLightcone(nodeId: string): void {
  for (const key of Array.from(pending.keys())) {
    if (key.startsWith(`${nodeId}|`)) pending.delete(key);
  }
  pins.delete(nodeId);
  heardAt.delete(nodeId);
  lastPrune.delete(nodeId);
}

/** TEST-ONLY. */
export function _resetAdsbLightcone(): void {
  pending.clear();
  pins.clear();
  heardAt.clear();
  lastPrune.clear();
}
