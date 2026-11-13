/**
 * Network areas: the outline of each electricity distributor's supply area,
 * for the map to draw behind the outages (GET /api/v1/outages/areas,
 * api/outages.ts).
 *
 * Four distributors publish their area as a file behind their outage map, each
 * in its own shape:
 *
 *   ausgrid     one ring, a list of {lat, lng}, not closed
 *   citipower   a FeatureCollection of one Polygon
 *   powercor    the same, 20,000 points (756 KB)
 *   jemena      a mask: a Polygon covering the whole world with the supply
 *               area cut out of it as a hole, then one small Polygon for each
 *               pocket inside the area that Jemena does not supply
 *
 * The other distributors publish none, and have no area here.
 *
 * These are not on the poller and not archived: an area changes when a
 * distributor redraws its map, which is years apart. Each is read when the
 * route is first asked for, then again once it is a day old, and kept in this
 * module's memory only. Nothing is written to disk: the LiveStore would save
 * it, but everything in the LiveStore is a polled source with a poll time
 * that the status pages report, and after a restart the first request reads
 * four small files once. A read that fails keeps the copy held and is tried
 * again an hour later, not on the next request. Requests that arrive while a
 * read is under way share it.
 *
 * An area follows its distributor's <NAME>_DISABLED switch: while the
 * distributor's outages are off its area is not asked for at all.
 *
 * The files are drawn for a desktop map and are far too detailed for a
 * phone, so every area is simplified before it is kept: see simplifyArea.
 */
import { fetchJson } from '../shared/http.js';
import { log } from '../../lib/log.js';
import { BROWSER_HEADERS } from '../ausgrid.js';
import { DISTRIBUTOR_INFO, ring, switchedOff, type Distributor, type OutageState } from './common.js';

// ---------------------------------------------------------------------------
// Shapes
// ---------------------------------------------------------------------------

/** One polygon as GeoJSON writes it: the outer ring, then its holes; points are [longitude, latitude]. */
export type PolygonRings = number[][][];

export type AreaGeometry =
  | { type: 'Polygon'; coordinates: PolygonRings }
  | { type: 'MultiPolygon'; coordinates: PolygonRings[] };

export type AreaDistributor = 'ausgrid' | 'citipower' | 'powercor' | 'jemena';

// ---------------------------------------------------------------------------
// Reading what the files hold
// ---------------------------------------------------------------------------

const isPoint = (v: unknown): boolean =>
  Array.isArray(v) ? typeof v[0] === 'number' || typeof v[0] === 'string' : !!v && typeof v === 'object';

/** The rings of one polygon; a polygon whose outer ring is not a ring is no polygon. */
function polygonOf(rings: unknown): PolygonRings[] {
  if (!Array.isArray(rings)) return [];
  const outer = ring(rings[0]);
  if (!outer.length) return [];
  const holes = rings.slice(1).map((r) => ring(r)).filter((r) => r.length > 0);
  return [[outer, ...holes]];
}

/**
 * Every polygon in a distributor's file, whichever way it is written: a
 * GeoJSON FeatureCollection, Feature, GeometryCollection, Polygon or
 * MultiPolygon, or bare coordinates — one ring of {lat, lng} objects or
 * [longitude, latitude] pairs, a list of rings (a polygon), or a list of
 * those. Anything else gives no polygons, which the caller takes as a failed
 * read.
 */
export function areaPolygons(payload: unknown): PolygonRings[] {
  if (Array.isArray(payload)) {
    if (!payload.length) return [];
    // A ring: its members are points.
    if (isPoint(payload[0]) && !Array.isArray((payload[0] as unknown[])[0])) return polygonOf([payload]);
    // A polygon: its members are rings.
    if (Array.isArray(payload[0]) && isPoint((payload[0] as unknown[])[0]) && !Array.isArray((payload[0] as unknown[][])[0]![0])) {
      return polygonOf(payload);
    }
    return payload.flatMap((p) => (Array.isArray(p) ? polygonOf(p) : []));
  }
  if (!payload || typeof payload !== 'object') return [];
  const o = payload as { type?: unknown; features?: unknown; geometry?: unknown; geometries?: unknown; coordinates?: unknown };
  switch (o.type) {
    case 'FeatureCollection':
      return Array.isArray(o.features) ? o.features.flatMap((f) => areaPolygons(f)) : [];
    case 'Feature':
      return areaPolygons(o.geometry);
    case 'GeometryCollection':
      return Array.isArray(o.geometries) ? o.geometries.flatMap((g) => areaPolygons(g)) : [];
    case 'Polygon':
      return polygonOf(o.coordinates);
    case 'MultiPolygon':
      return Array.isArray(o.coordinates) ? o.coordinates.flatMap((p) => polygonOf(p)) : [];
    default:
      return [];
  }
}

/** Is the point inside the ring? Even-odd rule; the ring may be closed or not. */
function inside(point: number[], r: number[][]): boolean {
  const [x, y] = point as [number, number];
  let within = false;
  for (let i = 0, j = r.length - 1; i < r.length; j = i++) {
    const [xi, yi] = r[i] as [number, number];
    const [xj, yj] = r[j] as [number, number];
    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) within = !within;
  }
  return within;
}

/** A ring that reaches round most of the world is a mask, not a supply area. */
const MASK_SPAN_DEG = 300;

/**
 * Jemena's file is drawn to grey out everything that is not Jemena's: the
 * first polygon covers the world and its holes are the supply area, and the
 * polygons after it grey out pockets inside the area. Turned the right way
 * round here: each hole of the mask is an area, and each further polygon is a
 * hole in the area it lies in. A file with no mask in it is not this file,
 * and throws.
 */
export function unmask(polygons: PolygonRings[]): PolygonRings[] {
  const areas: PolygonRings[] = [];
  const pockets: number[][][] = [];
  let masked = false;
  for (const [outer, ...holes] of polygons as Array<[number[][], ...number[][][]]>) {
    let west = Infinity;
    let east = -Infinity;
    for (const p of outer) {
      west = Math.min(west, p[0]!);
      east = Math.max(east, p[0]!);
    }
    if (east - west >= MASK_SPAN_DEG) {
      masked = true;
      for (const hole of holes) areas.push([hole]);
    } else {
      pockets.push(outer);
    }
  }
  if (!masked) throw new Error('no mask polygon');
  for (const pocket of pockets) {
    const area = areas.find((a) => inside(pocket[0]!, a[0]!));
    if (area) area.push(pocket);
  }
  return areas;
}

// ---------------------------------------------------------------------------
// Simplifying
// ---------------------------------------------------------------------------

/**
 * How far, in degrees, the simplified outline may stray from the
 * distributor's: 0.002° is about 220 m north–south and 180 m east–west at
 * Melbourne. The outline is drawn behind a state-wide map; at the zoom where
 * 200 m shows, the outages themselves are what is being looked at. All the
 * areas together come to about 70 KB at this tolerance, from 1.26 MB.
 */
export const AREA_TOLERANCE_DEG = 0.002;

/**
 * A ring smaller than this, in square degrees (about 1 km²), is left out: an
 * islet off the coast, or a pocket of a few streets. As a hole it only goes
 * missing from the outline; the area around it is still drawn.
 */
export const AREA_MIN_RING_DEG2 = 1e-4;

/** Four decimals is about 11 m, well inside the tolerance, and a third off every number's length. */
const round4 = (n: number): number => Math.round(n * 1e4) / 1e4;

/** The square of the distance from p to the segment a–b, in degrees. */
function segmentDistance2(p: number[], a: number[], b: number[]): number {
  const dx = b[0]! - a[0]!;
  const dy = b[1]! - a[1]!;
  const length2 = dx * dx + dy * dy;
  let t = length2 === 0 ? 0 : ((p[0]! - a[0]!) * dx + (p[1]! - a[1]!) * dy) / length2;
  t = Math.max(0, Math.min(1, t));
  const x = a[0]! + t * dx - p[0]!;
  const y = a[1]! + t * dy - p[1]!;
  return x * x + y * y;
}

/** The area a ring encloses, in square degrees (shoelace). */
export function ringArea(r: number[][]): number {
  let sum = 0;
  for (let i = 0, j = r.length - 1; i < r.length; j = i++) sum += r[j]![0]! * r[i]![1]! - r[i]![0]! * r[j]![1]!;
  return Math.abs(sum) / 2;
}

/**
 * One ring with the points that do not change its shape taken out
 * (Douglas–Peucker), rounded to four decimals and closed. Null when what is
 * left is not a ring (under four points, the closing one included) or
 * encloses less than `minArea`.
 *
 * Douglas–Peucker works on a line with two ends, and a ring has none, so the
 * ring is cut in two at its first point and the point farthest from it; both
 * are kept. The spans still to look at are held in a list, not in the call
 * stack: Powercor's ring is 20,000 points, and a coastline can nest a call
 * per point.
 */
export function simplifyRing(
  input: number[][],
  tolerance: number = AREA_TOLERANCE_DEG,
  minArea: number = AREA_MIN_RING_DEG2,
): number[][] | null {
  const pts = input.filter((p) => Array.isArray(p) && Number.isFinite(p[0]) && Number.isFinite(p[1]));
  // Work on the ring closed, whether it came closed or not.
  const first = pts[0];
  const last = pts[pts.length - 1];
  if (!first || !last) return null;
  if (first[0] !== last[0] || first[1] !== last[1]) pts.push(first);
  const end = pts.length - 1;
  if (end < 3) return null;

  let far = 0;
  let farthest = -1;
  for (let i = 1; i < end; i++) {
    const d = (pts[i]![0]! - first[0]!) ** 2 + (pts[i]![1]! - first[1]!) ** 2;
    if (d > farthest) {
      farthest = d;
      far = i;
    }
  }

  const keep = new Uint8Array(pts.length);
  keep[0] = keep[far] = keep[end] = 1;
  const tolerance2 = tolerance * tolerance;
  const spans: Array<[number, number]> = [[0, far], [far, end]];
  for (let span = spans.pop(); span; span = spans.pop()) {
    const [from, to] = span;
    let worst = -1;
    let at = -1;
    for (let i = from + 1; i < to; i++) {
      const d = segmentDistance2(pts[i]!, pts[from]!, pts[to]!);
      if (d > worst) {
        worst = d;
        at = i;
      }
    }
    if (at !== -1 && worst > tolerance2) {
      keep[at] = 1;
      spans.push([from, at], [at, to]);
    }
  }

  const out: number[][] = [];
  for (let i = 0; i <= end; i++) {
    if (!keep[i]) continue;
    const p = [round4(pts[i]![0]!), round4(pts[i]![1]!)];
    const before = out[out.length - 1];
    // Rounding can land two neighbours on the same spot.
    if (before && before[0] === p[0] && before[1] === p[1]) continue;
    out.push(p);
  }
  if (out.length < 4 || ringArea(out) < minArea) return null;
  return out;
}

/**
 * A distributor's polygons as one small GeoJSON geometry: every ring
 * simplified, a polygon whose outer ring does not survive left out with its
 * holes, and a hole kept only if it survives itself. Null when nothing
 * survives.
 */
export function simplifyArea(
  polygons: PolygonRings[],
  tolerance: number = AREA_TOLERANCE_DEG,
  minArea: number = AREA_MIN_RING_DEG2,
): AreaGeometry | null {
  const kept: PolygonRings[] = [];
  for (const [outer, ...holes] of polygons) {
    const shell = outer ? simplifyRing(outer, tolerance, minArea) : null;
    if (!shell) continue;
    const rings: PolygonRings = [shell];
    for (const hole of holes) {
      const h = simplifyRing(hole, tolerance, minArea);
      if (h) rings.push(h);
    }
    kept.push(rings);
  }
  if (!kept.length) return null;
  return kept.length === 1 ? { type: 'Polygon', coordinates: kept[0]! } : { type: 'MultiPolygon', coordinates: kept };
}

// ---------------------------------------------------------------------------
// The distributors that publish an area
// ---------------------------------------------------------------------------

interface AreaSource {
  distributor: AreaDistributor;
  url: string;
  /** Extra request headers, where the distributor's other reads send them. */
  headers?: Record<string, string>;
  /** Jemena's file is the area turned inside out. */
  mask?: boolean;
}

/** In the order the answer lists them, which is the merged feed's. */
export const AREA_SOURCES: readonly AreaSource[] = [
  { distributor: 'ausgrid', url: 'https://www.ausgrid.com.au/api/outages-map/network-area', headers: BROWSER_HEADERS },
  { distributor: 'citipower', url: 'https://www.powercor.com.au/assets/geo/citipower-network.json' },
  { distributor: 'powercor', url: 'https://www.powercor.com.au/assets/geo/powercor-network.json' },
  { distributor: 'jemena', url: 'https://poweroutages.jemena.com.au/boundary.json', mask: true },
];

const sourceOf = (distributor: AreaDistributor): AreaSource => AREA_SOURCES.find((s) => s.distributor === distributor)!;

/**
 * One distributor's file as the geometry that is served. Throws when the
 * file holds no polygon, or none that survives simplifying: an error page or
 * a changed file read as "no area" would take the outline off the map and
 * replace the copy held.
 */
export function parseArea(distributor: AreaDistributor, payload: unknown): AreaGeometry {
  let polygons = areaPolygons(payload);
  if (!polygons.length) throw new Error(`${distributor}: network area file holds no polygon`);
  if (sourceOf(distributor).mask) {
    try {
      polygons = unmask(polygons);
    } catch (err) {
      throw new Error(`${distributor}: network area file: ${(err as Error).message}`);
    }
  }
  const geometry = simplifyArea(polygons);
  if (!geometry) throw new Error(`${distributor}: network area file holds no area large enough to draw`);
  return geometry;
}

// ---------------------------------------------------------------------------
// The copies held
// ---------------------------------------------------------------------------

/** An area is read again once its copy is this old. */
export const AREA_REFRESH_MS = 24 * 3_600_000;
/** After a failed read, how long before the next one. */
export const AREA_RETRY_MS = 3_600_000;
/** Powercor's file is 756 KB; give it longer than the default 15 s. */
const TIMEOUT_MS = 30_000;

interface Held {
  geometry: AreaGeometry | null;
  /** When the copy held was read (ms); null while there is none. */
  readAt: number | null;
  /** No read is started before this (ms). */
  nextAt: number;
  reading: Promise<void> | null;
}

const held = new Map<AreaDistributor, Held>();

/** Test hook: forget every copy and every read under way. */
export function _resetNetworkAreas(): void {
  held.clear();
}

function entry(distributor: AreaDistributor): Held {
  let h = held.get(distributor);
  if (!h) {
    h = { geometry: null, readAt: null, nextAt: 0, reading: null };
    held.set(distributor, h);
  }
  return h;
}

/** Read one distributor's file into its entry. Never rejects: a failure is logged and the copy held stays. */
function read(source: AreaSource, h: Held): Promise<void> {
  const { distributor } = source;
  return fetchJson<unknown>(source.url, { timeoutMs: TIMEOUT_MS, ...(source.headers ? { headers: source.headers } : {}) })
    .then((payload) => {
      h.geometry = parseArea(distributor, payload);
      h.readAt = Date.now();
      h.nextAt = h.readAt + AREA_REFRESH_MS;
    })
    .catch((err: unknown) => {
      h.nextAt = Date.now() + AREA_RETRY_MS;
      log.warn(
        { distributor, kept: h.geometry !== null, err: (err as Error)?.message },
        'outages: network area read failed; trying again in an hour',
      );
    })
    .finally(() => {
      h.reading = null;
    });
}

export interface NetworkArea {
  distributor: AreaDistributor;
  name: string;
  state: OutageState;
  geometry: AreaGeometry;
}

export interface NetworkAreaStatus {
  distributor: Distributor;
  name: string;
  state: OutageState;
  /** When the area served was read, ISO 8601 UTC; null when there is none. */
  updated_at: string | null;
}

/**
 * Every distributor's area that has been read, and for each distributor that
 * publishes one, when. Starts whatever reads are due. A distributor with no
 * copy yet is waited for, so the first request after a restart answers with
 * the areas rather than without them; one whose copy is only old is answered
 * from that copy while the new one is read, so nobody waits on a slow
 * distributor for an outline that has not changed.
 *
 * `complete` is false while a distributor that is switched on has no area:
 * the route then lets the answer be kept for seconds, not a day.
 */
export async function networkAreas(): Promise<{ areas: NetworkArea[]; sources: NetworkAreaStatus[]; complete: boolean }> {
  const now = Date.now();
  const on = AREA_SOURCES.filter((s) => !switchedOff(s.distributor));
  const waits: Array<Promise<void>> = [];
  for (const source of on) {
    const h = entry(source.distributor);
    if (!h.reading && now >= h.nextAt) h.reading = read(source, h);
    if (h.reading && !h.geometry) waits.push(h.reading);
  }
  await Promise.all(waits);

  const areas: NetworkArea[] = [];
  const sources: NetworkAreaStatus[] = [];
  let complete = true;
  for (const source of AREA_SOURCES) {
    const { distributor } = source;
    const info = DISTRIBUTOR_INFO[distributor];
    // A distributor switched off is not served from a copy read before the switch.
    const h = on.includes(source) ? held.get(distributor) : undefined;
    if (h?.geometry) areas.push({ distributor, ...info, geometry: h.geometry });
    else if (on.includes(source)) complete = false;
    sources.push({ distributor, ...info, updated_at: h?.geometry && h.readAt !== null ? new Date(h.readAt).toISOString() : null });
  }
  return { areas, sources, complete };
}

/** Test hook: the reads under way, to wait for one that was started in the background. */
export function _networkAreaReads(): Promise<unknown> {
  return Promise.all([...held.values()].map((h) => h.reading));
}
