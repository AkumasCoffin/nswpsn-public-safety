/**
 * Live ADS-B aircraft source.
 *
 * Merges four free, no-key readsb-style aggregators — adsb.lol,
 * adsb.fi, airplanes.live and adsb.one — for the union of their feeder
 * coverage. All expose the same v2 point-radius query and return the
 * same readsb JSON shape ({ ac: [...] }), so records dedupe cleanly by
 * ICAO hex, keeping whichever aggregator saw the aircraft most recently.
 *
 * Coverage: Australia-wide. The 250 nm (~463 km) max radius shared by
 * all the APIs can't come close to spanning the continent, so a set of
 * 28 circles covers all Australian land (mainland + Tasmania + near
 * islands, verified by set-cover: every land point is within 447 km of
 * a centre). High-traffic circles sit centred on the capitals.
 *
 * Politeness: adsb.fi, airplanes.live and adsb.one ask for ≤1
 * request/second, which makes 28 circles × 4 upstreams per poll
 * impossible at a live cadence. Instead each poll PARTITIONS the
 * circles into one shard per upstream (7 circles each, staggered 1 s
 * apart), and the shard↔upstream assignment rotates every poll — so
 * every circle is fetched from some upstream every poll, and from
 * every upstream over any 4 polls. Instantaneous per-upstream rate
 * never exceeds 1 req/s; the average is ~0.5 req/s at the ~15 s
 * effective cadence (8 s re-arm + ~7 s staggered sweep). Aircraft that
 * only one aggregator can see would otherwise blink during rotation,
 * so a short holdover keeps an aircraft's last position for up to 90 s
 * (one full rotation is ~60 s) — see applyHoldover.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';
import { config } from '../config.js';
import { log } from '../lib/log.js';

// Australia bbox [W,S,E,N]. Filter applies a 0.3° buffer so aircraft
// riding the edge don't flap in/out between ticks; the API reports the
// unbuffered box.
const AU_BBOX: [number, number, number, number] = [112.0, -44.3, 154.3, -9.7];
const BBOX_BUFFER_DEG = 0.3;

const RADIUS_NM = 250; // max allowed by all the upstreams

// Australia-wide cover: 28 circles of 250 nm. Derived by greedy
// set-cover over a 0.4° land-sample grid (mainland + Tasmania
// polygons + island/extreme must-cover points), seeded with
// hub-centred circles (the NSW quadrants kept from the NSW-only era,
// each capital, the QLD coast) then filled and pruned. Worst-case land
// sample sits 447 km from its nearest centre (< 463 km radius). Keep
// the count divisible by UPSTREAMS.length so rotation shards stay
// equal-sized.
interface Circle {
  id: string;
  lat: number;
  lon: number;
}
export const CIRCLES: readonly Circle[] = [
  { id: 'nsw_nw', lat: -30.4, lon: 144.1 },
  { id: 'nsw_ne', lat: -30.4, lon: 150.5 },
  { id: 'nsw_sw', lat: -35.2, lon: 144.1 },
  { id: 'nsw_se', lat: -35.2, lon: 150.5 },
  { id: 'vic', lat: -37.4, lon: 144.6 },
  { id: 'tas', lat: -42.0, lon: 146.7 },
  { id: 'qld_se', lat: -26.8, lon: 152.0 },
  { id: 'qld_c', lat: -22.5, lon: 148.5 },
  { id: 'qld_n', lat: -17.8, lon: 145.0 },
  { id: 'qld_channel', lat: -24.0, lon: 140.5 },
  { id: 'qld_outback', lat: -27.5, lon: 143.0 },
  { id: 'gulf_ne', lat: -13.5, lon: 140.0 },
  { id: 'gulf_sw', lat: -17.5, lon: 138.0 },
  { id: 'nt_top', lat: -13.2, lon: 132.0 },
  { id: 'nt_c', lat: -20.0, lon: 134.0 },
  { id: 'alice', lat: -24.5, lon: 133.5 },
  { id: 'sa_n', lat: -31.0, lon: 136.5 },
  { id: 'sa_se', lat: -35.5, lon: 139.0 },
  { id: 'nullarbor', lat: -31.0, lon: 129.0 },
  { id: 'wa_sw', lat: -32.5, lon: 117.5 },
  { id: 'wa_goldfields', lat: -30.5, lon: 123.0 },
  { id: 'wa_interior', lat: -29.0, lon: 121.0 },
  { id: 'wa_desert', lat: -24.5, lon: 126.5 },
  { id: 'gascoyne', lat: -24.5, lon: 117.5 },
  { id: 'shark_bay', lat: -26.0, lon: 113.5 },
  { id: 'wa_pilbara', lat: -21.5, lon: 119.5 },
  { id: 'wa_kimberley', lat: -16.5, lon: 125.5 },
  { id: 'kimberley_e', lat: -19.5, lon: 127.5 },
] as const;

interface Upstream {
  id: string;
  url: (c: Circle) => string;
}
const UPSTREAMS: readonly Upstream[] = [
  {
    id: 'adsb_lol',
    url: (c) => `https://api.adsb.lol/v2/point/${c.lat}/${c.lon}/${RADIUS_NM}`,
  },
  {
    id: 'adsb_fi',
    url: (c) =>
      `https://opendata.adsb.fi/api/v2/lat/${c.lat}/lon/${c.lon}/dist/${RADIUS_NM}`,
  },
  {
    id: 'airplanes_live',
    url: (c) => `https://api.airplanes.live/v2/point/${c.lat}/${c.lon}/${RADIUS_NM}`,
  },
  {
    // ADSB One (run by the airplanes.live folks; ADSBx-v2-compatible).
    // Sits behind aggressive Cloudflare bot protection that 403s some
    // networks — a challenge page here is just a failed upstream, the
    // other aggregators still merge.
    id: 'adsb_one',
    url: (c) => `https://api.adsb.one/v2/point/${c.lat}/${c.lon}/${RADIUS_NM}`,
  },
] as const;

// Drop positions older than this — point queries can include aircraft
// whose last known position is minutes stale.
const MAX_SEEN_POS_SECS = 60;

/** Raw readsb v2 aircraft record. Every field except `hex` is optional
 *  in practice — presence varies by aggregator and by message type
 *  (TIS-B/MLAT targets often lack alt_baro, category, etc.). */
interface RawAircraft {
  hex?: string;
  flight?: string;
  r?: string; // registration
  t?: string; // ICAO type designator
  lat?: number;
  lon?: number;
  alt_baro?: number | 'ground';
  gs?: number;
  track?: number;
  category?: string;
  squawk?: string;
  emergency?: string;
  seen_pos?: number;
  dbFlags?: number;
}
interface ReadsbPointResponse {
  ac?: RawAircraft[];
  /** adsb.fi serves the tar1090-style `aircraft` key instead of `ac`. */
  aircraft?: RawAircraft[];
}

export type EsTag = 'polair' | 'rescue' | 'firebomber' | 'ambulance' | 'military';

export interface AdsbAircraft {
  /** Lowercase ICAO id; readsb prefixes non-ICAO TIS-B ids with '~'. */
  hex: string;
  callsign: string | null;
  reg: string | null;
  type: string | null;
  lat: number;
  lon: number;
  altFt: number | null;
  onGround: boolean;
  gsKt: number | null;
  trackDeg: number | null;
  category: string | null;
  squawk: string | null;
  emergencySquawk: boolean;
  esTag: EsTag | null;
  ageSec: number;
  sourceCount: number;
  sources: string[];
}

export interface AdsbSnapshot {
  aircraft: AdsbAircraft[];
  count: number;
  emergency_count: number;
  upstreams: Array<{
    id: string;
    ok: boolean;
    circles_ok: number;
    circles_total: number;
    count: number;
    error?: string;
  }>;
  bbox: [number, number, number, number];
  fetched_at: string;
}

const EMPTY_SNAPSHOT: AdsbSnapshot = {
  aircraft: [],
  count: 0,
  emergency_count: 0,
  upstreams: UPSTREAMS.map((u) => ({
    id: u.id,
    ok: false,
    circles_ok: 0,
    circles_total: CIRCLES.length / UPSTREAMS.length,
    count: 0,
  })),
  bbox: AU_BBOX,
  fetched_at: new Date(0).toISOString(),
};

// Emergency-service callsign prefixes. Intentionally data-driven so new
// prefixes (LIFS Westpac Lifesaver, FDxx RFDS, ...) can be appended
// without logic changes. Callsign rules win over the military dbFlag.
const ES_CALLSIGN_RULES: ReadonlyArray<{ re: RegExp; tag: EsTag }> = [
  { re: /^POL\d/, tag: 'polair' }, // NSW PolAir: POL30, POL32...
  // Aeromedical: Toll/NSW Ambulance rescue helos (RSCU201...), Westpac
  // Life Saver (LIFS21...), Westpac Rescue Helicopter Service (WPR...),
  // Toll-callsigned airframes (TOL...).
  { re: /^(RSCU\d|LIFS|WPR\d|TOL\d)/, tag: 'rescue' },
  { re: /^(FIRE|BMBR|BDOG)/, tag: 'firebomber' }, // RFS bombers + birddogs
  { re: /^(AM\d|MDS\d)/, tag: 'ambulance' }, // Air Ambulance / RFDS SE
];

export function classifyEmergencyService(
  callsign: string | null,
  dbFlags: number | undefined,
): EsTag | null {
  const cs = (callsign ?? '').trim().toUpperCase();
  for (const r of ES_CALLSIGN_RULES) {
    if (r.re.test(cs)) return r.tag;
  }
  if (dbFlags !== undefined && (dbFlags & 1) === 1) return 'military';
  return null;
}

const EMERGENCY_SQUAWKS = new Set(['7500', '7600', '7700']);

/** Normalize one raw record; null when it lacks a usable position. */
export function normalizeAircraft(
  raw: RawAircraft,
  upstreamId: string,
): AdsbAircraft | null {
  const hex = (raw.hex ?? '').trim().toLowerCase();
  if (!hex) return null;
  if (!Number.isFinite(raw.lat) || !Number.isFinite(raw.lon)) return null;
  const ageSec = Number.isFinite(raw.seen_pos) ? (raw.seen_pos as number) : 0;
  if (ageSec > MAX_SEEN_POS_SECS) return null;

  const callsign = (raw.flight ?? '').trim() || null;
  const onGround = raw.alt_baro === 'ground';
  const squawk = (raw.squawk ?? '').trim() || null;
  const emergency = (raw.emergency ?? '').trim();
  return {
    hex,
    callsign,
    reg: (raw.r ?? '').trim() || null,
    type: (raw.t ?? '').trim() || null,
    lat: raw.lat as number,
    lon: raw.lon as number,
    altFt: typeof raw.alt_baro === 'number' ? raw.alt_baro : null,
    onGround,
    gsKt: Number.isFinite(raw.gs) ? (raw.gs as number) : null,
    trackDeg: Number.isFinite(raw.track) ? (raw.track as number) : null,
    category: (raw.category ?? '').trim() || null,
    squawk,
    emergencySquawk:
      (squawk !== null && EMERGENCY_SQUAWKS.has(squawk)) ||
      (emergency !== '' && emergency !== 'none'),
    esTag: classifyEmergencyService(callsign, raw.dbFlags),
    ageSec,
    sourceCount: 1,
    sources: [upstreamId],
  };
}

/**
 * Merge normalized records from all upstreams/circles. Dedupe by hex,
 * keeping the freshest position (lowest ageSec); metadata the winner
 * lacks (callsign/reg/type/category — aggregators differ in db
 * completeness) is backfilled from losing records.
 */
export function mergeAircraft(records: AdsbAircraft[]): AdsbAircraft[] {
  const byHex = new Map<string, AdsbAircraft>();
  for (const rec of records) {
    const prev = byHex.get(rec.hex);
    if (!prev) {
      byHex.set(rec.hex, { ...rec, sources: [...rec.sources] });
      continue;
    }
    const winner = rec.ageSec < prev.ageSec ? { ...rec } : prev;
    const loser = winner === prev ? rec : prev;
    winner.callsign = winner.callsign ?? loser.callsign;
    winner.reg = winner.reg ?? loser.reg;
    winner.type = winner.type ?? loser.type;
    winner.category = winner.category ?? loser.category;
    winner.esTag = winner.esTag ?? loser.esTag;
    winner.emergencySquawk = winner.emergencySquawk || loser.emergencySquawk;
    winner.sources = Array.from(new Set([...prev.sources, ...rec.sources]));
    winner.sourceCount = winner.sources.length;
    byHex.set(rec.hex, winner);
  }
  return Array.from(byHex.values());
}

export function inAuBbox(lat: number, lon: number): boolean {
  const [w, s, e, n] = AU_BBOX;
  return (
    lat >= s - BBOX_BUFFER_DEG &&
    lat <= n + BBOX_BUFFER_DEG &&
    lon >= w - BBOX_BUFFER_DEG &&
    lon <= e + BBOX_BUFFER_DEG
  );
}

interface UpstreamResult {
  id: string;
  ok: boolean;
  circlesOk: number;
  circlesTotal: number;
  records: AdsbAircraft[];
  error?: string;
}

/**
 * Shard the circle list for one poll: upstream `upstreamIndex` takes
 * the circles where (i + tick) % UPSTREAMS.length matches. At any tick
 * the shards partition CIRCLES exactly (28 % 4 === 0), and rotating by
 * tick walks every circle through every upstream over 4 polls — so no
 * circle is permanently blind to one aggregator's feeder coverage, and
 * an upstream outage only ever staleneses a quarter of the map for one
 * poll.
 */
export function shardCircles(tick: number, upstreamIndex: number): Circle[] {
  const n = UPSTREAMS.length;
  return CIRCLES.filter((_, i) => (((i + tick) % n) + n) % n === upstreamIndex);
}

let _pollTick = 0;

// Holdover: each poll sees every circle from ONE aggregator, so an
// aircraft only a different aggregator's feeders receive would blink
// in and out on a 4-poll (~60 s) rotation period. Keep the last-known
// record for up to 90 s (aged by wall clock) to bridge the rotation;
// genuinely-gone aircraft still clear inside a couple of polls' worth
// of the old behaviour (MAX_SEEN_POS_SECS already allowed 60 s).
const HOLDOVER_MAX_AGE_SECS = 90;
const _lastSeen = new Map<string, { rec: AdsbAircraft; atMs: number }>();

export function applyHoldover(fresh: AdsbAircraft[], nowMs: number): AdsbAircraft[] {
  const out = new Map<string, AdsbAircraft>();
  for (const a of fresh) {
    out.set(a.hex, a);
    _lastSeen.set(a.hex, { rec: a, atMs: nowMs });
  }
  for (const [hex, h] of _lastSeen) {
    const age = h.rec.ageSec + (nowMs - h.atMs) / 1000;
    if (age > HOLDOVER_MAX_AGE_SECS) {
      _lastSeen.delete(hex);
      continue;
    }
    if (!out.has(hex)) out.set(hex, { ...h.rec, ageSec: Math.round(age) });
  }
  return Array.from(out.values());
}

/** TEST-ONLY: reset holdover + rotation state between unit tests. */
export function _resetAdsbHoldoverForTests(): void {
  _lastSeen.clear();
  _pollTick = 0;
}

/**
 * Fetch one upstream's shard of circles, staggered 1 s apart to
 * respect the ~1 req/s politeness ceiling. Per-circle failures are
 * tolerated (that circle just contributes nothing); the upstream is
 * only marked down when every circle fails.
 */
async function fetchUpstream(up: Upstream, circles: readonly Circle[]): Promise<UpstreamResult> {
  const results = await Promise.all(
    circles.map(async (c, i) => {
      if (i > 0) await new Promise((r) => setTimeout(r, i * 1000));
      try {
        const body = await fetchJson<ReadsbPointResponse>(up.url(c), {
          timeoutMs: 8_000,
        });
        const ac = Array.isArray(body.ac)
          ? body.ac
          : Array.isArray(body.aircraft)
            ? body.aircraft
            : [];
        const records: AdsbAircraft[] = [];
        for (const raw of ac) {
          const norm = normalizeAircraft(raw, up.id);
          if (norm) records.push(norm);
        }
        return { ok: true as const, records };
      } catch (err) {
        return { ok: false as const, error: (err as Error).message };
      }
    }),
  );
  const records: AdsbAircraft[] = [];
  const errors: string[] = [];
  let circlesOk = 0;
  for (const r of results) {
    if (r.ok) {
      circlesOk += 1;
      records.push(...r.records);
    } else {
      errors.push(r.error);
    }
  }
  const out: UpstreamResult = {
    id: up.id,
    ok: circlesOk > 0,
    circlesOk,
    circlesTotal: circles.length,
    records,
  };
  if (circlesOk === 0) out.error = errors[0] ?? 'unknown error';
  return out;
}

// ---------------------------------------------------------------------
// Position trails. Accumulated server-side once per poll and shared by
// every client (the "fast at high usage" property — per-user cost is
// only downloading the prebuilt snapshot, which is also CDN-cached).
// Trails persist for the whole time an aircraft is tracked and end when
// it lands / is no longer detected (10 min dropout grace). Unbounded
// duration stays bounded in size via progressive simplification: the
// recent ~10 min is kept at full resolution, older history is
// Douglas-Peucker-thinned — straight cruise segments collapse to a few
// points while helicopter orbits keep their shape.

type TrailPoint = [number, number, number, number | null]; // [t, lat, lon, altFt]

const TRAIL_RECENT_MS = 10 * 60_000; // full-resolution window
const TRAIL_SIMPLIFY_TRIGGER = 60; // points before old-portion simplify
const TRAIL_MAX_POINTS = 150; // hard cap per hex (larger epsilon on overflow)
const TRAIL_DP_EPSILON_DEG = 0.002; // ~200 m
const TRAIL_ABSENT_GRACE_MS = 10 * 60_000; // dropout tolerance before deletion
const TRAIL_MAX_HEXES = 4000;

const _trails = new Map<string, TrailPoint[]>();
let _trailsSnapshot: AdsbTrailsSnapshot = {
  trails: {},
  fetched_at: new Date(0).toISOString(),
};

export interface AdsbTrailsSnapshot {
  /** hex → [[lat, lon, altFt|null], ...] oldest→newest (5 dp coords). */
  trails: Record<string, Array<[number, number, number | null]>>;
  fetched_at: string;
}

/** Douglas-Peucker on [t, lat, lon, alt] points (lat/lon distance). */
export function simplifyTrail(pts: TrailPoint[], epsilon: number): TrailPoint[] {
  if (pts.length <= 2) return pts;
  const keep = new Array<boolean>(pts.length).fill(false);
  keep[0] = keep[pts.length - 1] = true;
  const stack: Array<[number, number]> = [[0, pts.length - 1]];
  while (stack.length) {
    const seg = stack.pop() as [number, number];
    const a = pts[seg[0]] as TrailPoint;
    const b = pts[seg[1]] as TrailPoint;
    let maxD = 0;
    let maxI = -1;
    const dx = b[2] - a[2];
    const dy = b[1] - a[1];
    const len2 = dx * dx + dy * dy;
    for (let i = seg[0] + 1; i < seg[1]; i++) {
      const p = pts[i] as TrailPoint;
      let d: number;
      if (len2 === 0) {
        const ex = p[2] - a[2];
        const ey = p[1] - a[1];
        d = Math.sqrt(ex * ex + ey * ey);
      } else {
        const t = ((p[2] - a[2]) * dx + (p[1] - a[1]) * dy) / len2;
        const cx = a[2] + Math.max(0, Math.min(1, t)) * dx;
        const cy = a[1] + Math.max(0, Math.min(1, t)) * dy;
        const ex = p[2] - cx;
        const ey = p[1] - cy;
        d = Math.sqrt(ex * ex + ey * ey);
      }
      if (d > maxD) {
        maxD = d;
        maxI = i;
      }
    }
    if (maxD > epsilon && maxI !== -1) {
      keep[maxI] = true;
      stack.push([seg[0], maxI], [maxI, seg[1]]);
    }
  }
  return pts.filter((_, i) => keep[i]);
}

function updateTrails(aircraft: AdsbAircraft[], nowMs: number): void {
  const seen = new Set<string>();
  for (const a of aircraft) {
    seen.add(a.hex);
    let buf = _trails.get(a.hex);
    if (!buf) {
      buf = [];
      _trails.set(a.hex, buf);
    }
    const last = buf[buf.length - 1];
    // Parked/stationary aircraft don't accumulate duplicate points.
    if (last && last[1] === a.lat && last[2] === a.lon) continue;
    buf.push([nowMs, a.lat, a.lon, a.altFt]);
    if (buf.length > TRAIL_SIMPLIFY_TRIGGER) {
      // Simplify the portion older than the full-resolution window.
      const cut = buf.findIndex((p) => p[0] >= nowMs - TRAIL_RECENT_MS);
      const splitAt = cut === -1 ? buf.length : cut;
      if (splitAt > 2) {
        const older = simplifyTrail(buf.slice(0, splitAt), TRAIL_DP_EPSILON_DEG);
        const next = older.concat(buf.slice(splitAt));
        buf.length = 0;
        buf.push(...next);
      }
      if (buf.length > TRAIL_MAX_POINTS) {
        const crushed = simplifyTrail(buf, TRAIL_DP_EPSILON_DEG * 3);
        buf.length = 0;
        buf.push(...crushed.slice(-TRAIL_MAX_POINTS));
      }
    }
  }
  // Landed / out-of-coverage: absent hexes keep their trail for a
  // dropout grace, then delete.
  for (const [hex, buf] of _trails) {
    if (seen.has(hex)) continue;
    const newest = buf[buf.length - 1];
    if (!newest || nowMs - newest[0] > TRAIL_ABSENT_GRACE_MS) _trails.delete(hex);
  }
  // Memory backstop.
  if (_trails.size > TRAIL_MAX_HEXES) {
    const byAge = Array.from(_trails.entries()).sort(
      (a, b) => (a[1][a[1].length - 1]?.[0] ?? 0) - (b[1][b[1].length - 1]?.[0] ?? 0),
    );
    for (const [hex] of byAge.slice(0, _trails.size - TRAIL_MAX_HEXES)) {
      _trails.delete(hex);
    }
  }
  // Prebuild the served snapshot once per poll — requests do zero work.
  const out: AdsbTrailsSnapshot['trails'] = {};
  const r5 = (v: number): number => Math.round(v * 1e5) / 1e5;
  for (const [hex, buf] of _trails) {
    if (buf.length < 2) continue;
    out[hex] = buf.map((p) => [r5(p[1]), r5(p[2]), p[3]]);
  }
  _trailsSnapshot = { trails: out, fetched_at: new Date(nowMs).toISOString() };
}

export function adsbTrailsSnapshot(): AdsbTrailsSnapshot {
  return _trailsSnapshot;
}

/** TEST-ONLY: reset trail state between unit tests. */
export function _resetAdsbTrailsForTests(): void {
  _trails.clear();
  _trailsSnapshot = { trails: {}, fetched_at: new Date(0).toISOString() };
}

export async function fetchAdsbAircraft(): Promise<AdsbSnapshot> {
  // Upstreams in parallel — different hosts, no shared rate limit.
  // Each gets this poll's shard of the circle rotation.
  const tick = _pollTick++;
  const results = await Promise.all(
    UPSTREAMS.map((u, i) => fetchUpstream(u, shardCircles(tick, i))),
  );

  const fresh = mergeAircraft(results.flatMap((r) => r.records)).filter((a) =>
    inAuBbox(a.lat, a.lon),
  );
  const allDown = results.every((r) => !r.ok);
  if (allDown && fresh.length === 0) {
    // Real outage — throw so the poller's failure counter and backoff
    // engage. Partial failures never reach here.
    throw new Error(
      `adsb: all upstreams failed: ${results
        .map((r) => `${r.id}: ${r.error ?? 'unknown'}`)
        .join('; ')}`,
    );
  }

  const merged = applyHoldover(fresh, Date.now());
  updateTrails(merged, Date.now());

  // Stable ordering: emergency services first, then lowest altitude —
  // matches the frontend's render cap so a truncated list keeps the
  // aircraft that matter.
  merged.sort((a, b) => {
    const ae = a.esTag !== null || a.emergencySquawk ? 0 : 1;
    const be = b.esTag !== null || b.emergencySquawk ? 0 : 1;
    if (ae !== be) return ae - be;
    return (a.altFt ?? Infinity) - (b.altFt ?? Infinity);
  });

  return {
    aircraft: merged,
    count: merged.length,
    emergency_count: merged.filter((a) => a.esTag !== null).length,
    upstreams: results.map((r) => {
      const u: AdsbSnapshot['upstreams'][number] = {
        id: r.id,
        ok: r.ok,
        circles_ok: r.circlesOk,
        circles_total: r.circlesTotal,
        count: r.records.length,
      };
      if (r.error !== undefined) u.error = r.error;
      return u;
    }),
    bbox: AU_BBOX,
    fetched_at: new Date().toISOString(),
  };
}

export default function register(): void {
  if (config.ADSB_DISABLED) {
    log.warn('adsb: disabled via ADSB_DISABLED, source not registered');
    return;
  }
  registerSource<AdsbSnapshot>({
    name: 'adsb_aircraft',
    family: 'misc',
    // The poller re-arms *after* each run completes; 8 s + ~7 s of
    // staggered shard fetching lands the effective cadence at ~15 s
    // (≤ 1 req/s instantaneous, ~0.5 req/s average per upstream).
    intervalMs: 8_000,
    fetch: fetchAdsbAircraft,
  });
}

export function adsbSnapshot(): AdsbSnapshot {
  return liveStore.getData<AdsbSnapshot>('adsb_aircraft') ?? EMPTY_SNAPSHOT;
}
