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
import { nodeAdsbRecords, nodeAdsbFeedCount } from '../services/nodes/adsbNodeStore.js';
// One-way, like the node store above: this module hands the archive its
// trails, the archive never reaches back for them.
import {
  noteAdsbIdentities,
  maybeFlushAdsbTracks,
} from '../services/adsbTrackArchive.js';

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
export interface RawAircraft {
  hex?: string | null;
  flight?: string | null;
  r?: string | null; // registration
  t?: string | null; // ICAO type designator
  lat?: number | null;
  lon?: number | null;
  alt_baro?: number | 'ground' | null;
  gs?: number | null;
  track?: number | null;
  category?: string | null;
  squawk?: string | null;
  emergency?: string | null;
  seen_pos?: number | null;
  dbFlags?: number | null;
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
  /**
   * True when this position was DEAD-RECKONED rather than received.
   *
   * Set only by applyHoldover, for an aircraft that has stopped reporting but
   * was last seen airborne with a known heading and speed. Every consumer must
   * present it as a guess: it is the only field on this record that is not an
   * observation, and a client that renders it identically to a real fix is
   * showing an aircraft somewhere nobody said it was.
   */
  estimated: boolean;
  /** Seconds since the last REAL fix, when `estimated`. Null otherwise. */
  estimatedSec: number | null;
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
  dbFlags: number | null | undefined,
): EsTag | null {
  const cs = (callsign ?? '').trim().toUpperCase();
  for (const r of ES_CALLSIGN_RULES) {
    if (r.re.test(cs)) return r.tag;
  }
  if (dbFlags !== undefined && dbFlags !== null && (dbFlags & 1) === 1) return 'military';
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
    // Everything out of this function IS an observation; only applyHoldover
    // ever sets these.
    estimated: false,
    estimatedSec: null,
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

/**
 * Convert one ADS-B feeder node's uploaded snapshot into internal records.
 *
 * Lives here rather than in the node store because this module owns
 * `normalizeAircraft` and the age cutoff — and because keeping the runtime
 * dependency one-way (source -> store) avoids a cycle between the upstream
 * poller and the node layer.
 *
 * `seen_pos` in the payload is relative to the snapshot's own `at`, so transit
 * and queue delay are added here. Without that, a snapshot delayed by a retry
 * would present minute-old positions as fresh and beat a genuinely current
 * aggregator record in the merge.
 *
 * The delay is measured against the NODE'S OWN CLOCK, so the raw difference is
 * transit plus however far that clock is out — see nodeTransitSec.
 */
/** Recent (backendNow - nodeAt) deltas per node, newest last. */
const _nodeClockDeltas = new Map<string, number[]>();

/** How many uploads the clock estimate looks back over. At one upload every
 *  five seconds this is a few minutes — long enough to be stable, short enough
 *  to follow an NTP correction rather than being pinned by it forever. */
const NODE_CLOCK_SAMPLES = 32;

/**
 * How long this upload really took to arrive, in seconds.
 *
 * The obvious answer — backend clock minus the `at` the node stamped — is not
 * transit. It is transit PLUS the node's clock error, and that error is
 * unbounded: a receiver a minute behind made every one of its records look a
 * minute old, so they lost the merge to whatever an aggregator had and, past
 * MAX_AGE_SEC, were dropped before reaching the map at all. The symptom was a
 * node feeding perfectly while its aircraft went stale or never appeared.
 *
 * The clock error cannot be measured directly, but it is very nearly the
 * SMALLEST delta seen recently: across many uploads the quickest one is the
 * one that spent almost no time in transit, so whatever is left in it is the
 * offset. Subtracting that leaves about zero for an ordinary upload and the
 * genuine extra for one held up by a retry, which is exactly what this figure
 * is for — and it works whichever way the clock is wrong, where the old
 * `sentMs <= nowMs` guard only caught clocks running fast.
 */
export function nodeTransitSec(sourceId: string, sentMs: number, nowMs: number): number {
  const delta = nowMs - sentMs;
  let ring = _nodeClockDeltas.get(sourceId);
  if (!ring) {
    ring = [];
    _nodeClockDeltas.set(sourceId, ring);
  }
  ring.push(delta);
  if (ring.length > NODE_CLOCK_SAMPLES) ring.shift();

  let floor = ring[0]!;
  for (const d of ring) if (d < floor) floor = d;
  // Never negative: an upload cannot arrive before it was sent, and the first
  // upload from a node has nothing to compare against, so it counts as prompt.
  return Math.max(0, (delta - floor) / 1000);
}

/** Test seam, and for forgetting a node that has gone. */
export function _resetNodeClock(sourceId?: string): void {
  if (sourceId) _nodeClockDeltas.delete(sourceId);
  else _nodeClockDeltas.clear();
}

export function normalizeNodeUpload(
  upload: { at: string; aircraft: RawAircraft[] },
  sourceId: string,
  nowMs: number = Date.now(),
): AdsbAircraft[] {
  return normalizeNodeUploadWithTrails(upload, sourceId, nowMs).records;
}

/**
 * The same normalisation, plus the positions each aircraft carried between
 * uploads.
 *
 * One function rather than two passes because the transit correction must be
 * measured ONCE per upload: nodeTransitSec learns from every delta it is shown,
 * and calling it twice for the same snapshot would weight that upload double in
 * the clock estimate.
 */
export function normalizeNodeUploadWithTrails(
  upload: { at: string; aircraft: RawAircraft[] },
  sourceId: string,
  nowMs: number = Date.now(),
): { records: AdsbAircraft[]; trails: Map<string, TrailPoint[]> } {
  const sentMs = Date.parse(upload.at);
  const transitSec = Number.isFinite(sentMs)
    ? nodeTransitSec(sourceId, sentMs, nowMs)
    : 0;
  // `at` on the node's clock is not a usable instant, but `at` corrected by the
  // transit we just derived is: it is the moment the snapshot describes, in our
  // time. Carried positions are ages relative to it.
  const atMs = nowMs - transitSec * 1000;

  const out: AdsbAircraft[] = [];
  const trails = new Map<string, TrailPoint[]>();
  for (const raw of upload.aircraft) {
    const rec = normalizeAircraft(
      {
        ...raw,
        seen_pos:
          (Number.isFinite(raw.seen_pos) ? (raw.seen_pos as number) : 0) + transitSec,
      },
      sourceId,
    );
    if (!rec) continue;
    out.push(rec);

    const carried = (raw as { positions?: unknown }).positions;
    if (!Array.isArray(carried) || carried.length === 0) continue;
    const pts: TrailPoint[] = [];
    for (const c of carried) {
      if (!Array.isArray(c) || c.length < 3) continue;
      const [age, lat, lon, alt] = c as [number, number, number, number | null];
      if (!Number.isFinite(age) || !Number.isFinite(lat) || !Number.isFinite(lon)) continue;
      pts.push([atMs - age * 1000, lat, lon,
        typeof alt === 'number' && Number.isFinite(alt) ? alt : null]);
    }
    // Oldest first, whatever order they arrived in.
    pts.sort((a, b) => a[0] - b[0]);
    if (pts.length > 0) trails.set(rec.hex, pts);
  }
  return { records: out, trails };
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

/**
 * How long a DEAD-RECKONED position may be carried, versus the 90 s a frozen
 * one gets.
 *
 * Longer because a projected aircraft is still telling you something true —
 * roughly where it went — while a frozen one is just a stale dot. Short
 * because the error grows with every second: the projection assumes the
 * aircraft flew straight, and three minutes at 450 kt is ~40 km of committed
 * straight line. A turn inside that window puts the icon somewhere the
 * aircraft never was, and no amount of extra time improves the guess.
 */
const ESTIMATE_MAX_SECS = 180;

/**
 * Below this groundspeed, a reported track is mostly noise — a taxiing or
 * drifting target can report a heading that swings wildly — and projecting
 * noise produces drift in an arbitrary direction.
 */
const ESTIMATE_MIN_GS_KT = 40;

/**
 * How long an aircraft must go UNREPORTED before it is projected rather than
 * simply held.
 *
 * Absence from one poll is not evidence an aircraft has gone. The circles are
 * split across four upstreams that rotate each tick, any one of them can fail
 * or answer late, partial circle failures are tolerated silently, and
 * normalizeAircraft drops any record whose own seen_pos already exceeds 60s.
 * All of those produce a gap for an aircraft sitting in perfectly good
 * coverage — which is exactly what was showing up as "estimated" over
 * well-covered ground.
 *
 * At a ~15s effective cadence this is three consecutive misses: enough that
 * the aircraft really is not being reported, while still leaving most of the
 * 180s horizon for the projection itself. Until then the record is HELD at its
 * last real position, which is what the holdover did before estimation
 * existed and is the right answer for a transient gap.
 */
const ESTIMATE_MIN_ABSENT_SEC = 45;

/**
 * The running state of each aircraft, which is more than its last record.
 *
 * ADS-B sends position, velocity and identity in SEPARATE message types, and a
 * point query against an aggregator returns whatever it happened to hold at
 * that instant. So a perfectly healthy aircraft routinely arrives with a
 * position and nothing else — no altitude, no track, no callsign — which the
 * map drew as a bare dot labelled with a hex, filling itself in a poll or two
 * later. That is what "shows as on the ground then starts moving" is: not a
 * ground record at all, but a record with no velocity yet.
 *
 * Every real ADS-B display keeps an assembled state table for exactly this
 * reason — dump1090's own aircraft.json IS one. This is ours.
 *
 * Altitude and velocity carry their OWN observation times, separate from when
 * a record last arrived: a stale value must age out on the clock of the thing
 * that was measured, not on the clock of the last packet to mention the
 * aircraft.
 */
interface LastSeen {
  rec: AdsbAircraft;
  /** When any record for this hex last arrived. */
  atMs: number;
  altFt: number | null;
  altAtMs: number;
  gsKt: number | null;
  trackDeg: number | null;
  velAtMs: number;
}

const _lastSeen = new Map<string, LastSeen>();

/**
 * How long a carried-over altitude or velocity stays usable.
 *
 * Generous, because an airborne aircraft transmits both continuously and a gap
 * this long means the aggregators — not the aircraft — went quiet. Short
 * enough that a landed aircraft stops claiming a cruise altitude.
 */
const CARRY_MAX_SEC = 120;

/**
 * Fill a fresh record's gaps from what we already knew about that aircraft.
 *
 * Identity (callsign, registration, type, category, service tag) is carried
 * unconditionally: it does not go stale, and losing it mid-flight is what made
 * a labelled aircraft revert to a bare hex.
 *
 * Altitude and velocity are carried only while recent, and never across the
 * ground boundary in either direction — an aircraft that has just landed must
 * not keep its cruise altitude, and one just airborne must not inherit zero.
 */
function backfillFromState(a: AdsbAircraft, prev: LastSeen, nowMs: number): AdsbAircraft {
  const out = { ...a };
  out.callsign = out.callsign ?? prev.rec.callsign;
  out.reg = out.reg ?? prev.rec.reg;
  out.type = out.type ?? prev.rec.type;
  out.category = out.category ?? prev.rec.category;
  out.esTag = out.esTag ?? prev.rec.esTag;

  if (!out.onGround && !prev.rec.onGround) {
    if (out.altFt === null && prev.altFt !== null
        && (nowMs - prev.altAtMs) / 1000 <= CARRY_MAX_SEC) {
      out.altFt = prev.altFt;
    }
    const velFresh = (nowMs - prev.velAtMs) / 1000 <= CARRY_MAX_SEC;
    if (out.gsKt === null && prev.gsKt !== null && velFresh) out.gsKt = prev.gsKt;
    if (out.trackDeg === null && prev.trackDeg !== null && velFresh) out.trackDeg = prev.trackDeg;
  }
  return out;
}

/** Nautical miles per degree of latitude. Longitude shrinks by cos(lat). */
const NM_PER_DEG = 60;

/**
 * Project a position forward along a constant heading at a constant speed.
 *
 * Flat-earth on purpose: over the three minutes this is allowed to run, the
 * great-circle correction is metres, well inside the uncertainty the guess
 * already carries.
 */
function deadReckon(
  lat: number, lon: number, trackDeg: number, gsKt: number, secs: number,
): { lat: number; lon: number } {
  const nm = gsKt * (secs / 3600);
  const rad = (trackDeg * Math.PI) / 180;
  const dLat = (nm * Math.cos(rad)) / NM_PER_DEG;
  const cosLat = Math.cos((lat * Math.PI) / 180);
  // Guard the poles, where a degree of longitude collapses to nothing.
  const dLon = Math.abs(cosLat) < 1e-6
    ? 0
    : (nm * Math.sin(rad)) / (NM_PER_DEG * cosLat);
  return { lat: lat + dLat, lon: lon + dLon };
}

export function applyHoldover(fresh: AdsbAircraft[], nowMs: number): AdsbAircraft[] {
  const out = new Map<string, AdsbAircraft>();
  for (const raw of fresh) {
    const prev = _lastSeen.get(raw.hex);
    const a = prev ? backfillFromState(raw, prev, nowMs) : raw;
    out.set(a.hex, a);
    // Each measurement keeps the time it was actually OBSERVED, so a value
    // carried across several polls still expires on its own age rather than
    // being refreshed by whatever packet happened to arrive next.
    _lastSeen.set(a.hex, {
      rec: a,
      atMs: nowMs,
      altFt: raw.altFt !== null ? raw.altFt : (prev?.altFt ?? null),
      altAtMs: raw.altFt !== null ? nowMs : (prev?.altAtMs ?? 0),
      gsKt: raw.gsKt !== null ? raw.gsKt : (prev?.gsKt ?? null),
      trackDeg: raw.trackDeg !== null ? raw.trackDeg : (prev?.trackDeg ?? null),
      velAtMs: (raw.gsKt !== null || raw.trackDeg !== null) ? nowMs : (prev?.velAtMs ?? 0),
    });
  }
  for (const [hex, h] of _lastSeen) {
    const rec = h.rec;
    // TWO different clocks, and conflating them was the bug.
    //
    //   age         — how old the POSITION is: the aggregator's own seen_pos
    //                 when we received it, plus the time held since. This is
    //                 the distance to project, and it is right for that.
    //   sinceHeard  — how long since we last RECEIVED anything about this
    //                 aircraft. This is the evidence that it is missing.
    //
    // Projecting on `age` alone meant a record that arrived already 40s stale
    // began dead-reckoning after a single missed poll — on ground with full
    // coverage, where a fresher real position existed and simply had not
    // reached us that tick.
    const age = rec.ageSec + (nowMs - h.atMs) / 1000;
    const sinceHeard = (nowMs - h.atMs) / 1000;

    // Whether the record SUPPORTS dead reckoning at all. An aircraft on the
    // ground, stationary, or without a heading never can.
    const projectable =
      !rec.onGround &&
      Number.isFinite(rec.trackDeg) &&
      (rec.gsKt ?? 0) > ESTIMATE_MIN_GS_KT;

    // The expiry window keys off `projectable`, NOT off whether it is being
    // projected right now: an aircraft still inside the grace period must not
    // be dropped at 90s before it ever gets the chance to be projected.
    if (age > (projectable ? ESTIMATE_MAX_SECS : HOLDOVER_MAX_AGE_SECS)) {
      _lastSeen.delete(hex);
      continue;
    }
    if (out.has(hex)) continue;

    // Held, not projected, until it has genuinely been quiet for a while.
    const estimating = projectable && sinceHeard >= ESTIMATE_MIN_ABSENT_SEC;
    if (!estimating) {
      out.set(hex, { ...rec, ageSec: Math.round(age) });
      continue;
    }

    const p = deadReckon(
      rec.lat, rec.lon, rec.trackDeg as number, rec.gsKt as number, age,
    );
    // The bbox filter runs BEFORE holdover, so a projection that flies out of
    // the region has nothing downstream to catch it. Forget it here rather
    // than serving an aircraft off the edge of the map.
    if (!inAuBbox(p.lat, p.lon)) {
      _lastSeen.delete(hex);
      continue;
    }
    out.set(hex, {
      ...rec,
      lat: p.lat,
      lon: p.lon,
      ageSec: Math.round(age),
      estimated: true,
      estimatedSec: Math.round(age),
    });
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

/**
 * Keep one trail within its budget: simplify what is past the full-resolution
 * window, and crush the whole thing if it is still too long.
 *
 * Shared, because node uploads now add points here too and a trail that grew
 * through one path while only the other trimmed it would run away.
 */
function trimTrail(buf: TrailPoint[], nowMs: number): void {
  if (buf.length <= TRAIL_SIMPLIFY_TRIGGER) return;
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

/** Two points close enough in time to be the same fix seen twice. */
const TRAIL_SAME_FIX_MS = 400;

/**
 * Add the positions a node carried between its uploads.
 *
 * The poller samples the merged picture on its own schedule, so a trail built
 * from it alone has one point per poll however fast the aircraft was actually
 * being heard. A receiver of ours reads its decoder every second and ships what
 * it saw, so these are the positions that were always there and never had a way
 * to reach the map.
 *
 * Points can be OLDER than what the poller has already appended — they are
 * stamped when the fix was taken, while the poller stamps when it looked — so
 * this merges by time rather than appending, and drops anything that lands on a
 * fix already held.
 */
export function ingestNodeTrailPoints(
  hex: string,
  points: ReadonlyArray<TrailPoint>,
  nowMs: number,
): void {
  if (!hex || points.length === 0) return;

  // Filtered and ordered BEFORE anything is created: a call carrying only
  // points from the future or from beyond the window must leave no trace, and
  // the fast path below reads points[0] as the oldest.
  const fresh = points
    .filter((p) => p[0] <= nowMs && p[0] > nowMs - TRAIL_RECENT_MS)
    .sort((a, b) => a[0] - b[0]);
  if (fresh.length === 0) return;

  let buf = _trails.get(hex);
  if (!buf) {
    buf = [];
    _trails.set(hex, buf);
  }

  const last = buf[buf.length - 1];
  if (!last || fresh[0]![0] > last[0] + TRAIL_SAME_FIX_MS) {
    // The common case: everything carried is newer than anything held.
    for (const p of fresh) buf.push([p[0], p[1], p[2], p[3]]);
  } else {
    const all = buf.concat(fresh.map((p) => [p[0], p[1], p[2], p[3]] as TrailPoint));
    all.sort((a, b) => a[0] - b[0]);
    const merged: TrailPoint[] = [];
    for (const p of all) {
      const prev = merged[merged.length - 1];
      // Same instant, or the same place: one fix, however many ways it arrived.
      if (prev && (p[0] - prev[0] <= TRAIL_SAME_FIX_MS
        || (prev[1] === p[1] && prev[2] === p[2]))) continue;
      merged.push(p);
    }
    buf.length = 0;
    buf.push(...merged);
  }
  trimTrail(buf, nowMs);
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
    trimTrail(buf, nowMs);
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

/** One aircraft's raw, timestamped trail — the archive's input.
 *
 *  The served snapshot drops timestamps (a path does not need them) and rounds
 *  to 5 dp, both of which history does need, so persistence reads the buffer
 *  rather than the snapshot. */
export interface ArchivableTrack {
  hex: string;
  /** [epochMs, lat, lon, altFt|null], oldest first. */
  points: ReadonlyArray<TrailPoint>;
}

/**
 * Every live trail, for services/adsbTrackArchive.ts.
 *
 * Exposed here rather than exporting `_trails` itself so the buffer stays
 * owned by this module — the archive reads, it never appends or prunes, and
 * the decimation rules stay in one place.
 */
export function adsbTrailsForArchive(): ArchivableTrack[] {
  const out: ArchivableTrack[] = [];
  for (const [hex, points] of _trails) {
    if (points.length === 0) continue;
    out.push({ hex, points });
  }
  return out;
}

/** TEST-ONLY: reset trail state between unit tests. */
export function _resetAdsbTrailsForTests(): void {
  _trails.clear();
  _trailsSnapshot = { trails: {}, fetched_at: new Date(0).toISOString() };
}

export async function fetchAdsbAircraft(): Promise<AdsbSnapshot> {
  // Upstreams in parallel — different hosts, no shared rate limit.
  // Each gets this poll's shard of the circle rotation.
  // ADSB_DISABLED turns off UPSTREAM polling only; our own feeder nodes keep
  // reporting through the same pipeline below.
  const tick = _pollTick++;
  const results = config.ADSB_DISABLED
    ? []
    : await Promise.all(
        UPSTREAMS.map((u, i) => fetchUpstream(u, shardCircles(tick, i))),
      );

  // Our own ADS-B feeder nodes are just another source: a ~5s cadence against
  // the aggregators' ~15s, covering whatever the circle rotation misses.
  // mergeAircraft resolves any overlap — freshest position wins, metadata
  // backfills from the other record, sources union — so nothing here needs to
  // know which kind of source a record came from.
  const nodeRecords = nodeAdsbRecords(Date.now());
  const nodeFeeds = nodeAdsbFeedCount();

  const fresh = mergeAircraft([
    ...results.flatMap((r) => r.records),
    ...nodeRecords,
  ]).filter((a) => inAuBbox(a.lat, a.lon));
  // Only a genuine upstream outage is a failure. With ADSB_DISABLED there are
  // no upstreams to be down, and node data flowing during an aggregator
  // outage means the source is working — neither should trip the backoff.
  const allDown = results.length > 0 && results.every((r) => !r.ok);
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
  // Trails (and, through them, the persisted history) record only OBSERVED
  // positions. A frozen holdover record was harmless here because
  // updateTrails already skips a point identical to the last one, but an
  // estimated record MOVES — without this filter every trail would accumulate
  // fabricated positions indistinguishable from real ones.
  const observed = merged.filter((a) => !a.estimated);
  updateTrails(observed, Date.now());
  // Persist the trails for the historical view. Driven from here rather than
  // its own timer so it can only ever see trails this poll has finished
  // writing; it rate-limits itself to once a minute.
  noteAdsbIdentities(observed);
  maybeFlushAdsbTracks(adsbTrailsForArchive(), Date.now());

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
    upstreams: [
      ...results.map((r) => {
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
      // Our own receivers as one synthetic entry, so the status page and the
      // live view show what the fleet is contributing. circles_* carry the
      // node count: a receiver is not a circle query, but the shape is shared.
      {
        id: 'nodes',
        ok: nodeFeeds > 0,
        circles_ok: nodeFeeds,
        circles_total: nodeFeeds,
        count: nodeRecords.length,
      },
    ],
    bbox: AU_BBOX,
    fetched_at: new Date().toISOString(),
  };
}

export default function register(): void {
  // Registered even when ADSB_DISABLED: the flag means "don't poll the public
  // aggregators", not "drop our own receivers". fetchAdsbAircraft skips the
  // upstream fetch and serves node data alone, so a deployment can run purely
  // on its own hardware.
  if (config.ADSB_DISABLED) {
    log.warn('adsb: ADSB_DISABLED — upstream aggregators off, feeder nodes still served');
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
