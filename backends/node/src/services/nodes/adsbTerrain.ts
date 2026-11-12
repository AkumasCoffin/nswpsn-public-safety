/**
 * The terrain line-of-sight limit around an ADS-B receiver: on each bearing,
 * and for each altitude, how far out an aircraft can be before the hills or
 * the curve of the earth hide it from the antenna.
 *
 * This is the "what the terrain allows" outline in the 3D coverage view, and
 * the yardstick the missed-aircraft count is measured against: an aircraft
 * beyond this line was never going to be heard, so not hearing it says nothing
 * about the receiver.
 *
 * Built once per pin and antenna height from an elevation profile along each
 * of the 72 bearings (Open-Meteo's elevation endpoint, the same one the
 * weather mask uses), then stored. A pin or a height change rebuilds it.
 */
import { config } from '../../config.js';
import { getPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';
import { isPublicOpenMeteo, pacedFetch } from '../../sources/weatherGridSource.js';

export const BEARINGS = 72;
/** Altitude slices: 500 m each, 0-13 km. Limits are computed at each slice's
 *  TOP, so slice k's limit is for an aircraft at (k + 1) * 500 m. */
export const SLICE_M = 500;
export const SLICES = 26;
/** Furthest distance the view or the limit ever considers. */
export const MAX_KM = 600;
/** Used until the owner sets their antenna height: a mast on a house roof. */
export const DEFAULT_ANTENNA_HEIGHT_M = 6;

/** Effective earth radius for VHF/UHF propagation (the standard 4/3 model,
 *  which accounts for ordinary atmospheric refraction). */
const R_EFF_M = 6_371_000 * (4 / 3);

/**
 * The profile's sample distances, km. Dense near the antenna, where a nearby
 * ridge or roofline blocks a wide slice of sky, and sparse far out, where only
 * a big range matters. 100 points: 1 km to 30, 3 km to 120, 8 km to 440.
 */
export const PROFILE_KM: readonly number[] = (() => {
  const out: number[] = [];
  for (let d = 1; d <= 30; d += 1) out.push(d);
  for (let d = 33; d <= 120; d += 3) out.push(d);
  for (let d = 128; d <= 440; d += 8) out.push(d);
  return out;
})();

/** Beyond the profile only the earth's curve limits, so these carry no terrain. */
const TAIL_KM: readonly number[] = (() => {
  const out: number[] = [];
  for (let d = 448; d <= MAX_KM; d += 8) out.push(d);
  return out;
})();

/** Centre of bearing bucket `b`, degrees. */
export function bucketCentreDeg(b: number): number {
  return (b + 0.5) * (360 / BEARINGS);
}

/** The point `km` along a great circle from (lat, lon) on `bearingDeg`. */
export function destination(
  lat: number, lon: number, bearingDeg: number, km: number,
): { lat: number; lon: number } {
  const toRad = Math.PI / 180;
  const δ = km / 6371;
  const θ = bearingDeg * toRad;
  const φ1 = lat * toRad, λ1 = lon * toRad;
  const φ2 = Math.asin(Math.sin(φ1) * Math.cos(δ) + Math.cos(φ1) * Math.sin(δ) * Math.cos(θ));
  const λ2 = λ1 + Math.atan2(
    Math.sin(θ) * Math.sin(δ) * Math.cos(φ1),
    Math.cos(δ) - Math.sin(φ1) * Math.sin(φ2),
  );
  return { lat: φ2 / toRad, lon: ((λ2 / toRad + 540) % 360) - 180 };
}

/**
 * Smooth-earth radio horizon, km: the limit when no terrain profile is known
 * yet. Treats the ground as sea level, so it is a little generous inland.
 */
export function horizonKm(antennaHeightM: number, aircraftM: number): number {
  const a = Math.sqrt(2 * R_EFF_M * Math.max(0, antennaHeightM));
  const b = Math.sqrt(2 * R_EFF_M * Math.max(0, aircraftM));
  return Math.min(MAX_KM, (a + b) / 1000);
}

/**
 * One bearing's limits, one per altitude slice.
 *
 * Works in "angle above the antenna's horizontal", corrected for the earth's
 * curve: a terrain point at distance d and elevation e subtends
 * (e - A)/d - d/2R. An aircraft at height H and distance d is visible when its
 * own angle is at least the steepest terrain angle between it and the antenna.
 * The scan stops at the first place the aircraft is hidden: what lies beyond
 * a ridge is not coverage the antenna can count on, even where a far peak
 * pokes back into view.
 *
 * `profile[i]` is the elevation at PROFILE_KM[i] in metres; NaN means unknown,
 * and is treated as no obstruction.
 */
export function bearingLimits(
  groundM: number, heightM: number, profile: readonly number[],
): number[] {
  const A = groundM + heightM;
  const out: number[] = [];
  for (let k = 0; k < SLICES; k += 1) {
    const H = (k + 1) * SLICE_M;
    let thetaMax = -Infinity;
    let limit = 0;
    let prevKm = 0;
    const n = PROFILE_KM.length + TAIL_KM.length;
    for (let i = 0; i < n; i += 1) {
      const km = i < PROFILE_KM.length ? PROFILE_KM[i]! : TAIL_KM[i - PROFILE_KM.length]!;
      const e = i < PROFILE_KM.length ? profile[i] : undefined;
      const dm = km * 1000;
      const need = (H - A) / dm - dm / (2 * R_EFF_M);
      if (typeof e === 'number' && Number.isFinite(e) && H < e) { limit = prevKm; break; }
      if (need < thetaMax) {
        // Where exactly between the last visible sample and this one the
        // aircraft drops below the terrain angle: solve
        // (H - A)/x - x/2R = thetaMax for x.
        const disc = thetaMax * thetaMax + 2 * (H - A) / R_EFF_M;
        if (disc >= 0) {
          const x = R_EFF_M * (-thetaMax + Math.sqrt(disc)) / 1000;
          limit = x > prevKm && x < km ? x : prevKm;
        } else {
          limit = prevKm;
        }
        break;
      }
      limit = km;
      prevKm = km;
      if (typeof e === 'number' && Number.isFinite(e)) {
        thetaMax = Math.max(thetaMax, (e - A) / dm - dm / (2 * R_EFF_M));
      }
    }
    out.push(Math.round(limit * 10) / 10);
  }
  return out;
}

export interface Terrain {
  lat: number;
  lon: number;
  heightM: number;
  groundM: number;
  /** [slice][bearing], km. */
  limits: number[][];
}

/** Memory copy of what is stored, by node. */
const cache = new Map<string, Terrain>();
/** Nodes whose stored row has been read this process (found or not). */
const loaded = new Set<string>();
/** Builds queued or running; one at a time, so two new receivers do not
 *  double the burst against the elevation API. */
const building = new Set<string>();
let chain: Promise<void> = Promise.resolve();
/** The public elevation API is free with a daily allowance, and one build is
 *  ~7,200 locations. Self-hosted has no limit; public gets one build a day. */
let lastPublicBuildMs = 0;
const PUBLIC_BUILD_GAP_MS = 24 * 3_600_000;

function matches(t: Terrain, lat: number, lon: number, heightM: number): boolean {
  return Math.abs(t.lat - lat) < 1e-6 && Math.abs(t.lon - lon) < 1e-6
    && Math.abs(t.heightM - heightM) < 0.01;
}

/** The terrain for this exact pin and height, if it is in memory. Synchronous,
 *  for the hot path that counts missed aircraft. */
export function cachedTerrain(
  nodeId: string, lat: number, lon: number, heightM: number,
): Terrain | null {
  const t = cache.get(nodeId);
  return t && matches(t, lat, lon, heightM) ? t : null;
}

export type TerrainStatus = 'ready' | 'building' | 'waiting' | 'unavailable';

/**
 * The terrain for a receiver, building it in the background when it is missing
 * or out of date. Never waits for a build: the first view after a pin change
 * says "building" and the next one has it.
 */
export async function terrainFor(
  nodeId: string, lat: number, lon: number, heightM: number,
): Promise<{ status: TerrainStatus; terrain: Terrain | null }> {
  const hit = cachedTerrain(nodeId, lat, lon, heightM);
  if (hit) return { status: 'ready', terrain: hit };

  const pool = await getPool();
  if (!pool) return { status: 'unavailable', terrain: null };

  if (!loaded.has(nodeId)) {
    loaded.add(nodeId);
    try {
      const r = await pool.query<{
        lat: number; lon: number; height_m: number; ground_m: number; limits: unknown;
      }>(
        'SELECT lat, lon, height_m, ground_m, limits FROM node_adsb_terrain WHERE node_id = $1',
        [nodeId],
      );
      const row = r.rows[0];
      if (row && Array.isArray(row.limits)) {
        cache.set(nodeId, {
          lat: row.lat, lon: row.lon, heightM: row.height_m, groundM: row.ground_m,
          limits: row.limits as number[][],
        });
      }
    } catch (err) {
      log.debug({ err, nodeId }, 'adsb terrain read failed');
    }
    const again = cachedTerrain(nodeId, lat, lon, heightM);
    if (again) return { status: 'ready', terrain: again };
  }

  if (building.has(nodeId)) return { status: 'building', terrain: null };
  if (isPublicOpenMeteo(config.OPEN_METEO_ELEVATION_URL)
      && Date.now() - lastPublicBuildMs < PUBLIC_BUILD_GAP_MS) {
    return { status: 'waiting', terrain: null };
  }
  building.add(nodeId);
  if (isPublicOpenMeteo(config.OPEN_METEO_ELEVATION_URL)) lastPublicBuildMs = Date.now();
  chain = chain
    .then(() => buildTerrain(nodeId, lat, lon, heightM))
    .catch((err) => log.warn({ err, nodeId }, 'adsb terrain build failed'))
    .finally(() => building.delete(nodeId));
  return { status: 'building', terrain: null };
}

const ELEVATION_MAX_COORDS = 100;

async function fetchElevationBatch(
  pts: ReadonlyArray<{ lat: number; lon: number }>,
): Promise<number[]> {
  const latitude = pts.map((p) => p.lat.toFixed(4)).join(',');
  const longitude = pts.map((p) => p.lon.toFixed(4)).join(',');
  const url = `${config.OPEN_METEO_ELEVATION_URL}?latitude=${latitude}&longitude=${longitude}`;
  const res = await pacedFetch<{ elevation?: unknown }>(url, pts.length);
  const el = res.elevation;
  // Answers are positional: a short array is an offset, not a gap, and would
  // put every later sample on the wrong bearing.
  if (!Array.isArray(el) || el.length !== pts.length) {
    throw new Error('elevation API returned a misaligned answer');
  }
  return el.map((v) => (typeof v === 'number' && Number.isFinite(v) ? v : Number.NaN));
}

async function buildTerrain(
  nodeId: string, lat: number, lon: number, heightM: number,
): Promise<void> {
  const pts: Array<{ lat: number; lon: number }> = [{ lat, lon }];
  for (let b = 0; b < BEARINGS; b += 1) {
    const brg = bucketCentreDeg(b);
    for (const km of PROFILE_KM) pts.push(destination(lat, lon, brg, km));
  }
  const elev: number[] = [];
  for (let i = 0; i < pts.length; i += ELEVATION_MAX_COORDS) {
    elev.push(...(await fetchElevationBatch(pts.slice(i, i + ELEVATION_MAX_COORDS))));
  }
  const groundM = Number.isFinite(elev[0]) ? elev[0]! : 0;
  const perBearing: number[][] = [];
  for (let b = 0; b < BEARINGS; b += 1) {
    const start = 1 + b * PROFILE_KM.length;
    perBearing.push(bearingLimits(groundM, heightM, elev.slice(start, start + PROFILE_KM.length)));
  }
  // Transpose to [slice][bearing], the shape the view reads.
  const limits: number[][] = [];
  for (let k = 0; k < SLICES; k += 1) limits.push(perBearing.map((row) => row[k]!));

  const t: Terrain = { lat, lon, heightM, groundM, limits };
  cache.set(nodeId, t);
  const pool = await getPool();
  if (!pool) return;
  await pool.query(
    `INSERT INTO node_adsb_terrain (node_id, lat, lon, height_m, ground_m, limits, computed_at)
     VALUES ($1, $2, $3, $4, $5, $6::jsonb, now())
     ON CONFLICT (node_id) DO UPDATE SET lat = EXCLUDED.lat, lon = EXCLUDED.lon,
       height_m = EXCLUDED.height_m, ground_m = EXCLUDED.ground_m,
       limits = EXCLUDED.limits, computed_at = now()`,
    [nodeId, lat, lon, heightM, groundM, JSON.stringify(limits)],
  );
  log.info({ nodeId, groundM }, 'adsb terrain built');
}

/** TEST-ONLY. */
export function _resetAdsbTerrain(): void {
  cache.clear();
  loaded.clear();
  building.clear();
  chain = Promise.resolve();
  lastPublicBuildMs = 0;
}
