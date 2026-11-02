/**
 * South Australia's main roads as the Department for Infrastructure and
 * Transport draws them, so an SA roadworks job (interstateRoads.ts,
 * saJobRecords) can be drawn along its road from one end to the other.
 *
 * DIT's Functional Hierarchy for South Australia's Land Transport Network
 * (data.sa.gov.au, CC BY 3.0 AU) is a zip of six GeoJSON layers — major
 * traffic and freight, tourist, public transport, peak hour, cycling and
 * pedestrian routes — drawing about 490 numbered roads between them. Each
 * piece names its road (gis_road_no: the ROAD_NO on SA's roadworks) and how
 * far along the road it starts and ends (start_rrd, end_rrd, in km), and is
 * drawn in that direction. A road split into two carriageways has a piece
 * for the right-hand one (side 'R') beside the rest ('U/L'); only 'U/L' is
 * kept, so each road is one line. The layers overlap, so a stretch is read
 * from whichever pieces cover it.
 *
 * Seen 2026-11-02: 1,152 of SA's 1,259 roadwork sites that restrict traffic
 * were on a road drawn here, 1,135 of them within 100 m of its line and half
 * within 2 m. Read when first needed and weekly after; a failed read keeps
 * the roads already held and is tried again an hour later.
 */
import { log } from '../lib/log.js';
import { fetchBuffer } from './shared/http.js';
import { unzip } from './shared/zip.js';

export const SA_ROADS_URL = 'https://www.dptiapps.com.au/dataportal/FunctionalHierarchy_geojson.zip';

/** One piece of a road: where along the road it starts and ends (km), and its line. */
export interface SaRoadPiece {
  from: number;
  to: number;
  /** The line, as longitude, latitude, longitude, latitude, … */
  pts: Float64Array;
  /** How far along the line (km) each point is. */
  cum: Float64Array;
  box: [minLon: number, minLat: number, maxLon: number, maxLat: number];
}

/** Each road's pieces, by road number without leading zeros ("3400"). */
export type SaRoads = ReadonlyMap<string, readonly SaRoadPiece[]>;

// Kilometres per degree, near enough over the length of a road segment.
const KM_LAT = 110.574;
const kmLon = (lat: number): number => 111.32 * Math.cos((lat * Math.PI) / 180);

type Obj = Record<string, unknown>;
const obj = (v: unknown): Obj => (v && typeof v === 'object' && !Array.isArray(v) ? (v as Obj) : {});
const isPoint = (p: unknown): p is number[] => Array.isArray(p) && Number.isFinite(p[0]) && Number.isFinite(p[1]);

/** Every road in DIT's layers (GeoJSON FeatureCollections), one line each. */
export function parseSaRoads(layers: readonly unknown[]): SaRoads {
  const roads = new Map<string, SaRoadPiece[]>();
  const seen = new Set<string>();
  for (const layer of layers) {
    const features = obj(layer)['features'];
    for (const f of Array.isArray(features) ? features : []) {
      const p = obj(obj(f)['properties']);
      const no = String(p['gis_road_no'] ?? '').trim().replace(/^0+/, '');
      const from = Number(p['start_rrd']);
      const to = Number(p['end_rrd']);
      if (!no || String(p['side'] ?? '') === 'R' || !Number.isFinite(from) || !(to > from)) continue;
      const key = `${no}|${from}|${to}`;
      if (seen.has(key)) continue;
      const g = obj(obj(f)['geometry']);
      const c = g['coordinates'];
      const parts = g['type'] === 'LineString' ? [c] : g['type'] === 'MultiLineString' && Array.isArray(c) ? c : [];
      const line = parts.flatMap((part: unknown) => (Array.isArray(part) ? part.filter(isPoint) : []));
      if (line.length < 2) continue;
      seen.add(key);
      const pts = new Float64Array(line.length * 2);
      const cum = new Float64Array(line.length);
      const box: SaRoadPiece['box'] = [Infinity, Infinity, -Infinity, -Infinity];
      line.forEach(([lon, lat], i) => {
        pts[i * 2] = lon!;
        pts[i * 2 + 1] = lat!;
        if (i > 0) cum[i] = cum[i - 1]! + Math.hypot((lon! - line[i - 1]![0]!) * kmLon(lat!), (lat! - line[i - 1]![1]!) * KM_LAT);
        box[0] = Math.min(box[0], lon!);
        box[1] = Math.min(box[1], lat!);
        box[2] = Math.max(box[2], lon!);
        box[3] = Math.max(box[3], lat!);
      });
      (roads.get(no) ?? roads.set(no, []).get(no)!).push({ from, to, pts, cum, box });
    }
  }
  return roads;
}

/** Where a point is along its road (km, as the road measures it), and how far it lies from the road's line. */
export function placeOnRoad(roads: SaRoads, roadNo: string, lon: number, lat: number): { at: number; km: number } | null {
  const pieces = roads.get(roadNo);
  if (!pieces) return null;
  const kx = kmLon(lat);
  let best: { at: number; km: number } | null = null;
  for (const p of pieces) {
    // A piece whose box is further off than the nearest so far cannot be nearer.
    const dx = Math.max(p.box[0] - lon, 0, lon - p.box[2]) * kx;
    const dy = Math.max(p.box[1] - lat, 0, lat - p.box[3]) * KM_LAT;
    if (best && Math.hypot(dx, dy) >= best.km) continue;
    const total = p.cum[p.cum.length - 1]!;
    for (let i = 0; i + 1 < p.cum.length; i++) {
      const ax = (p.pts[i * 2]! - lon) * kx;
      const ay = (p.pts[i * 2 + 1]! - lat) * KM_LAT;
      const ex = (p.pts[i * 2 + 2]! - lon) * kx - ax;
      const ey = (p.pts[i * 2 + 3]! - lat) * KM_LAT - ay;
      const len2 = ex * ex + ey * ey;
      const t = len2 > 0 ? Math.min(1, Math.max(0, -(ax * ex + ay * ey) / len2)) : 0;
      const km = Math.hypot(ax + t * ex, ay + t * ey);
      if (best && km >= best.km) continue;
      const along = p.cum[i]! + t * (p.cum[i + 1]! - p.cum[i]!);
      best = { km, at: p.from + (total > 0 ? along / total : 0) * (p.to - p.from) };
    }
  }
  return best;
}

/** The point `k` km along a piece's line. */
function pointAt(p: SaRoadPiece, k: number): number[] {
  let i = 0;
  while (i < p.cum.length - 2 && p.cum[i + 1]! < k) i++;
  const span = p.cum[i + 1]! - p.cum[i]!;
  const t = span > 0 ? Math.min(1, Math.max(0, (k - p.cum[i]!) / span)) : 0;
  return [p.pts[i * 2]! + t * (p.pts[i * 2 + 2]! - p.pts[i * 2]!), p.pts[i * 2 + 1]! + t * (p.pts[i * 2 + 3]! - p.pts[i * 2 + 1]!)];
}

/** A piece's line between two places along the road. */
function subline(p: SaRoadPiece, from: number, to: number): number[][] {
  const total = p.cum[p.cum.length - 1]!;
  const k1 = ((from - p.from) / (p.to - p.from)) * total;
  const k2 = ((to - p.from) / (p.to - p.from)) * total;
  const out = [pointAt(p, k1)];
  for (let i = 0; i < p.cum.length; i++) if (p.cum[i]! > k1 && p.cum[i]! < k2) out.push([p.pts[i * 2]!, p.pts[i * 2 + 1]!]);
  out.push(pointAt(p, k2));
  return out;
}

/** Kilometres from point p to the segment a–b. */
function kmToSegment(p: number[], a: number[], b: number[]): number {
  const kx = kmLon(p[1]!);
  const ax = (a[0]! - p[0]!) * kx;
  const ay = (a[1]! - p[1]!) * KM_LAT;
  const ex = (b[0]! - p[0]!) * kx - ax;
  const ey = (b[1]! - p[1]!) * KM_LAT - ay;
  const len2 = ex * ex + ey * ey;
  const t = len2 > 0 ? Math.min(1, Math.max(0, -(ax * ex + ay * ey) / len2)) : 0;
  return Math.hypot(ax + t * ex, ay + t * ey);
}

/** A line with the points that bend it less than `km` taken out (Douglas–Peucker). */
export function simplifyLine(line: number[][], km = 0.01): number[][] {
  if (line.length < 3) return line;
  const keep = new Uint8Array(line.length);
  keep[0] = 1;
  keep[line.length - 1] = 1;
  const stack: Array<[number, number]> = [[0, line.length - 1]];
  while (stack.length) {
    const [s, e] = stack.pop()!;
    let worst = -1;
    let at = -1;
    for (let i = s + 1; i < e; i++) {
      const d = kmToSegment(line[i]!, line[s]!, line[e]!);
      if (d > worst) [worst, at] = [d, i];
    }
    if (worst > km) {
      keep[at] = 1;
      stack.push([s, at], [at, e]);
    }
  }
  return line.filter((_, i) => keep[i]);
}

/**
 * The road between two places along it, as [longitude, latitude] lines —
 * more than one where DIT's drawing of the road has a gap between them.
 */
export function roadStretch(roads: SaRoads, roadNo: string, from: number, to: number): number[][][] {
  const pieces = roads.get(roadNo) ?? [];
  const EPS = 1e-6;
  const lines: number[][][] = [];
  let line: number[][] | null = null;
  let at = from;
  while (at < to - EPS) {
    // The piece covering this place that reaches furthest.
    let best: SaRoadPiece | null = null;
    for (const p of pieces) if (p.from <= at + EPS && p.to > at + EPS && (!best || p.to > best.to)) best = p;
    if (!best) {
      // A gap in the drawing: go on from the next piece, as a line of its own.
      let next: SaRoadPiece | null = null;
      for (const p of pieces) if (p.from > at && (!next || p.from < next.from)) next = p;
      if (!next || next.from >= to) break;
      at = next.from;
      line = null;
      continue;
    }
    const end = Math.min(best.to, to);
    const part = subline(best, at, end);
    if (line) line.push(...part.slice(1));
    else lines.push((line = part));
    at = end;
  }
  return lines.filter((l) => l.length >= 2).map((l) => simplifyLine(l));
}

const REFRESH_MS = 7 * 86_400_000;
const RETRY_MS = 3_600_000;
let _held: { roads: SaRoads; at: number } | null = null;
let _nextTry = 0;
let _reading: Promise<void> | null = null;

async function readSaRoads(): Promise<void> {
  try {
    const files = unzip(await fetchBuffer(SA_ROADS_URL, { timeoutMs: 120_000 }), (name) => name.endsWith('_GDA2020.geojson'));
    const roads = parseSaRoads([...files.values()].map((b) => JSON.parse(b.toString('utf8')) as unknown));
    if (roads.size === 0) throw new Error('no roads in the download');
    _held = { roads, at: Date.now() };
    log.info({ roads: roads.size }, 'SA roads: read DIT functional hierarchy');
  } catch (err) {
    _nextTry = Date.now() + RETRY_MS;
    log.warn({ err: (err as Error).message, held: _held !== null }, 'SA roads: could not read DIT functional hierarchy');
  }
}

/**
 * The roads, or null while none could be read. The first call waits for the
 * download; a weekly re-read after that runs while the roads held are used.
 */
export async function saRoads(now = Date.now()): Promise<SaRoads | null> {
  if ((!_held || now - _held.at > REFRESH_MS) && now >= _nextTry) {
    _reading ??= readSaRoads().finally(() => {
      _reading = null;
    });
    if (!_held) await _reading;
  }
  return _held?.roads ?? null;
}

/** Forget the roads held — tests need a clean slate. */
export function _resetSaRoads(): void {
  _held = null;
  _nextTry = 0;
  _reading = null;
}
