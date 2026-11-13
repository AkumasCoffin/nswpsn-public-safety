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
 * Politeness: adsb.fi, airplanes.live and adsb.one document 1
 * request/second; adsb.lol's limit moves with its load and it says a 4xx
 * means the client is doing something wrong. 28 circles × 4 upstreams per
 * poll is impossible at a live cadence, so each poll PARTITIONS the
 * circles among the upstreams that are answering (staggered STAGGER_MS
 * apart on each host), and the assignment rotates every poll — every
 * circle is fetched from some upstream every poll, and from every
 * answering upstream over a few polls. With all four up that is 7
 * circles each at the ~16 s effective cadence (8 s re-arm + the sweep).
 *
 * An upstream that refuses us (401/403) or rate-limits us (429) is
 * PAUSED rather than polled into the ground — see recordUpstreamHealth:
 * its sweep stops at the first such answer, its circles go to the others,
 * and it gets a single probe circle when the pause ends. A 429 also halves
 * how many circles that upstream is dealt per poll, and the share grows
 * back one circle at a time while it answers cleanly, so it settles at
 * what the host will take instead of cycling in and out of pauses. Aircraft that
 * only one aggregator can see would otherwise blink during rotation,
 * so a short holdover keeps an aircraft's last position for up to 90 s
 * — see applyHoldover.
 *
 * FlyItalyADSB is a fifth upstream, used only when FLYITALY_ADSB_KEY is set.
 * It is NOT dealt circles from the rotation: its free plan allows 300,000
 * requests a month (~7 a minute), a fraction of what the shared rotation
 * would ask of it. So it sits beside the rotation as a supplement — one
 * request at most every FLYITALY_MIN_INTERVAL_MS, cycling over the four NSW
 * quadrants only. The other four still cover the whole country; FlyItaly adds
 * whatever its own receivers hear over NSW, with each quadrant revisited
 * about once a minute, inside the 90 s holdover, so an aircraft only it sees
 * stays on the map. Its data is CC BY-SA 4.0 and is credited on
 * data-sources.html and on the map's aircraft layer.
 */
import { simplifyTrail } from '../lib/simplifyTrail.js';
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { nodeAdsbRecords, nodeAdsbFeedCount } from '../services/nodes/adsbNodeStore.js';
import { noteNetworkAircraft } from '../services/nodes/adsbLightcone.js';
// One-way, like the node store above: this module hands the archive the
// positions it observed, the archive never reaches back for them.
import {
  noteAdsbPositions,
  noteAdsbNodePoints,
  maybeFlushAdsbTracks,
} from '../services/adsbTrackArchive.js';

// Australia bbox and its edge buffer live in lib/auBbox.ts, shared with the
// globe-history import (which runs in a worker and must not load this module).
import { AU_BBOX, inAuBbox } from '../lib/auBbox.js';
export { inAuBbox };

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
  /** Extra request headers (an API key). */
  headers?: () => Record<string, string>;
}
const UPSTREAMS: readonly Upstream[] = [
  {
    id: 'adsb_lol',
    url: (c) => `https://api.adsb.lol/v2/point/${c.lat}/${c.lon}/${RADIUS_NM}`,
  },
  {
    id: 'adsb_fi',
    url: (c) =>
      `https://opendata.adsb.fi/api/v3/lat/${c.lat}/lon/${c.lon}/dist/${RADIUS_NM}`,
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

// --- FlyItalyADSB (supplementary, key-gated) ------------------------------
// The same 250 nm circles, asked for in kilometres (the free plan caps a
// radius at 500 km).
const FLYITALY_RADIUS_KM = Math.round(RADIUS_NM * 1.852);
const FLYITALY: Upstream = {
  id: 'flyitaly',
  url: (c) =>
    `https://api.flyitalyadsb.com/v2/lat/${c.lat}/lon/${c.lon}/dist/${FLYITALY_RADIUS_KM}`,
  headers: () => ({ 'X-Api-Key': (config.FLYITALY_ADSB_KEY ?? '').trim() }),
};
/**
 * At most one request this often. The free plan is 300,000 requests a month
 * and 30 a minute per key; one every 12 s is 5 a minute, ~216,000 a month,
 * which leaves room for restarts and the odd probe after a pause.
 */
const FLYITALY_MIN_INTERVAL_MS = 12_000;
/** The circles it cycles over: the four NSW quadrants, which also cover the
 *  ACT. Four keeps each one revisited inside the holdover. */
const FLYITALY_FOCUS: readonly Circle[] = CIRCLES.filter((c) => c.id.startsWith('nsw_'));
let _flyLastAt = -Infinity;
let _flyNext = 0;
let _flyLastOk = false;

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
  // Everything below feeds AdsbDetail. None of it is needed to place an
  // aircraft; all of it is what a tracker shows once you select one.
  alt_geom?: number | null;
  baro_rate?: number | null;
  geom_rate?: number | null;
  ias?: number | null;
  tas?: number | null;
  mach?: number | null;
  true_heading?: number | null;
  mag_heading?: number | null;
  roll?: number | null;
  nav_altitude_mcp?: number | null;
  nav_altitude_fms?: number | null;
  nav_heading?: number | null;
  nav_qnh?: number | null;
  nav_modes?: string[] | null;
  wd?: number | null;
  ws?: number | null;
  oat?: number | null;
  /** How the position was obtained: adsb_icao, mlat, tisb_icao, adsr_icao… */
  type?: string | null;
  nic?: number | null;
  nac_p?: number | null;
  sil?: number | null;
  rc?: number | null;
  version?: number | null;
  rssi?: number | null;
  messages?: number | null;
  desc?: string | null;
  ownOp?: string | null;
  year?: string | number | null;
}
interface ReadsbPointResponse {
  ac?: RawAircraft[];
  /** adsb.fi serves the tar1090-style `aircraft` key instead of `ac`. */
  aircraft?: RawAircraft[];
}

export type EsTag = 'polair' | 'rescue' | 'firebomber' | 'ambulance' | 'military';

export type AdsbPosSource = 'adsb' | 'mlat' | 'tisb' | 'adsr' | 'adsc' | 'modes' | 'other';
export type AdsbFlag = 'military' | 'interesting' | 'pia' | 'ladd';

/**
 * What an aircraft says about itself beyond where it is. Every field is
 * absent when the source did not send it (or sent nonsense), never null or
 * zero: most aircraft send only some of these, and a receiver's decoder a
 * different set from an aggregator's.
 *
 * Live only. None of this is written to the history archive, which stays
 * [time, lat, lon, altitude] per point.
 */
export interface AdsbDetail {
  /** Vertical rate from pressure altitude, feet per minute; negative descending. */
  baroRateFpm?: number;
  /** Vertical rate from GPS altitude, feet per minute. */
  geomRateFpm?: number;
  /** GPS (geometric) altitude, feet above the WGS84 ellipsoid. */
  altGeomFt?: number;
  iasKt?: number;
  tasKt?: number;
  mach?: number;
  /** Where the nose points, degrees true. Differs from trackDeg by the wind. */
  trueHeadingDeg?: number;
  magHeadingDeg?: number;
  /** Bank angle, degrees; negative is left. */
  rollDeg?: number;
  /** Altitude selected on the autopilot (MCP/FCU, else the FMS), feet. */
  navAltFt?: number;
  navHeadingDeg?: number;
  /** Altimeter setting, hPa. */
  navQnhHpa?: number;
  /** Engaged modes: autopilot, vnav, althold, approach, lnav, tcas. */
  navModes?: string[];
  /** Wind and outside air temperature the aggregator derived from the aircraft's own data. */
  windDirDeg?: number;
  windKt?: number;
  oatC?: number;
  /** How the position was obtained. MLAT and TIS-B are less exact than ADS-B. */
  posSource?: AdsbPosSource;
  /** Navigation integrity (0-11) and position accuracy (0-11) categories, source integrity level (0-3). */
  nic?: number;
  nacp?: number;
  sil?: number;
  /** Radius of containment, metres: the position is within this of the truth. */
  rcM?: number;
  /** ADS-B version the transponder speaks (0-2). */
  adsbVersion?: number;
  /** Signal strength at the receiver that heard it, dBFS. */
  rssiDb?: number;
  /** Messages received from this aircraft by that receiver. */
  messages?: number;
  /** The declared emergency, when there is one: general, lifeguard, minfuel, nordo, unlawful, downed. */
  emergency?: string;
  /** Database flags the aggregator holds for the airframe. */
  flags?: AdsbFlag[];
  /** Airframe description, operator and year of manufacture from the aggregator's database. */
  desc?: string;
  operator?: string;
  year?: string;
}

/** Every AdsbDetail key, for code that strips or copies the block. */
export const ADSB_DETAIL_KEYS = [
  'baroRateFpm', 'geomRateFpm', 'altGeomFt', 'iasKt', 'tasKt', 'mach',
  'trueHeadingDeg', 'magHeadingDeg', 'rollDeg', 'navAltFt', 'navHeadingDeg',
  'navQnhHpa', 'navModes', 'windDirDeg', 'windKt', 'oatC', 'posSource', 'nic',
  'nacp', 'sil', 'rcM', 'adsbVersion', 'rssiDb', 'messages', 'emergency',
  'flags', 'desc', 'operator', 'year',
] as const satisfies ReadonlyArray<keyof AdsbDetail>;

/**
 * The detail kept on the plain aircraft list: the three a map needs for every
 * aircraft (a climb arrow, an "approximate position" mark, the kind of
 * emergency). The rest is a few hundred bytes per aircraft that only matters
 * for the one selected, and is served on request (`detail`, `hex`).
 */
export const ADSB_LIST_DETAIL_KEYS: ReadonlyArray<keyof AdsbDetail> = [
  'baroRateFpm', 'posSource', 'emergency',
];

/** Detail that describes the airframe, not the moment: safe to fill from an
 *  older record or carry between polls. */
const ADSB_IDENTITY_DETAIL_KEYS = ['flags', 'desc', 'operator', 'year'] as const;

function num(v: unknown, min: number, max: number): number | undefined {
  return typeof v === 'number' && Number.isFinite(v) && v >= min && v <= max ? v : undefined;
}
function text(v: unknown, maxLen: number): string | undefined {
  if (typeof v !== 'string' && typeof v !== 'number') return undefined;
  const t = String(v).trim();
  return t && t.length <= maxLen ? t : undefined;
}

/** readsb's position `type` in our seven words. */
export function posSourceOf(type: unknown, hex: string): AdsbPosSource | undefined {
  const t = typeof type === 'string' ? type.trim().toLowerCase() : '';
  if (!t) return hex.startsWith('~') ? 'tisb' : undefined;
  if (t.startsWith('adsb')) return 'adsb';
  if (t.startsWith('adsr')) return 'adsr';
  if (t.startsWith('tisb')) return 'tisb';
  if (t === 'adsc') return 'adsc';
  if (t === 'mlat') return 'mlat';
  if (t === 'mode_s') return 'modes';
  return 'other';
}

const DB_FLAGS: ReadonlyArray<[number, AdsbFlag]> = [
  [1, 'military'], [2, 'interesting'], [4, 'pia'], [8, 'ladd'],
];

/** The detail one raw record carries. Out-of-range values are dropped, not
 *  clamped: a wrong number shown as fact is worse than a missing row. */
export function detailOf(raw: RawAircraft, hex: string): AdsbDetail {
  const d: AdsbDetail = {};
  const set = <K extends keyof AdsbDetail>(k: K, v: AdsbDetail[K] | undefined): void => {
    if (v !== undefined) d[k] = v;
  };
  set('baroRateFpm', num(raw.baro_rate, -30_000, 30_000));
  set('geomRateFpm', num(raw.geom_rate, -30_000, 30_000));
  set('altGeomFt', num(raw.alt_geom, -2_000, 100_000));
  set('iasKt', num(raw.ias, 0, 1_500));
  set('tasKt', num(raw.tas, 0, 1_500));
  set('mach', num(raw.mach, 0, 5));
  set('trueHeadingDeg', num(raw.true_heading, 0, 360));
  set('magHeadingDeg', num(raw.mag_heading, 0, 360));
  set('rollDeg', num(raw.roll, -180, 180));
  set('navAltFt', num(raw.nav_altitude_mcp, 0, 100_000) ?? num(raw.nav_altitude_fms, 0, 100_000));
  set('navHeadingDeg', num(raw.nav_heading, 0, 360));
  set('navQnhHpa', num(raw.nav_qnh, 800, 1_100));
  if (Array.isArray(raw.nav_modes)) {
    const modes = raw.nav_modes
      .filter((m): m is string => typeof m === 'string' && m.length > 0 && m.length <= 12)
      .slice(0, 8);
    if (modes.length > 0) d.navModes = modes;
  }
  set('windDirDeg', num(raw.wd, 0, 360));
  set('windKt', num(raw.ws, 0, 500));
  set('oatC', num(raw.oat, -100, 60));
  set('posSource', posSourceOf(raw.type, hex));
  set('nic', num(raw.nic, 0, 11));
  set('nacp', num(raw.nac_p, 0, 11));
  set('sil', num(raw.sil, 0, 3));
  set('rcM', num(raw.rc, 0, 500_000));
  set('adsbVersion', num(raw.version, 0, 3));
  set('rssiDb', num(raw.rssi, -60, 0));
  const messages = num(raw.messages, 0, 1e12);
  if (messages !== undefined) d.messages = Math.round(messages);
  const emergency = text(raw.emergency, 16)?.toLowerCase();
  if (emergency && emergency !== 'none') d.emergency = emergency;
  if (typeof raw.dbFlags === 'number' && Number.isFinite(raw.dbFlags)) {
    const flags = DB_FLAGS.filter(([bit]) => ((raw.dbFlags as number) & bit) !== 0).map(([, f]) => f);
    if (flags.length > 0) d.flags = flags;
  }
  set('desc', text(raw.desc, 80));
  set('operator', text(raw.ownOp, 80));
  set('year', text(raw.year, 4));
  return d;
}

/** A record without the detail the plain list leaves out. */
export function withoutFullDetail<T extends AdsbAircraft>(a: T): T {
  const out = { ...a };
  for (const k of ADSB_DETAIL_KEYS) {
    if (!ADSB_LIST_DETAIL_KEYS.includes(k)) delete out[k];
  }
  return out;
}

export interface AdsbAircraft extends AdsbDetail {
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
    ...detailOf(raw, hex),
  };
}

/** ADS-B is the aircraft's own GPS; these are worked out by someone else. */
function isDerivedPosition(a: AdsbAircraft): boolean {
  return a.posSource === 'mlat' || a.posSource === 'tisb';
}

/** A derived position must be fresher by more than this to beat a real one. */
const DERIVED_POSITION_HANDICAP_SEC = 3;

/** True when `a`'s position should be preferred to `b`'s. */
function fresher(a: AdsbAircraft, b: AdsbAircraft): boolean {
  const handicap = (x: AdsbAircraft): number => (isDerivedPosition(x) ? DERIVED_POSITION_HANDICAP_SEC : 0);
  return a.ageSec + handicap(a) < b.ageSec + handicap(b);
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
    // Freshest wins, except that an MLAT or TIS-B fix only a moment newer
    // does not displace the aircraft's own ADS-B position.
    const winner = fresher(rec, prev) ? { ...rec } : prev;
    const loser = winner === prev ? rec : prev;
    for (const k of ADSB_IDENTITY_DETAIL_KEYS) {
      if (winner[k] === undefined && loser[k] !== undefined) (winner as AdsbDetail)[k] = loser[k] as never;
    }
    winner.callsign = winner.callsign ?? loser.callsign;
    winner.reg = winner.reg ?? loser.reg;
    winner.type = winner.type ?? loser.type;
    winner.category = winner.category ?? loser.category;
    winner.esTag = winner.esTag ?? loser.esTag;
    // A squawk is one fact with its emergency meaning attached. The winner's
    // own stands; only a winner with no squawk at all borrows the other's.
    // OR-ing the flag alone let an older record's 7700 mark an aircraft the
    // fresher record already showed back on 1200.
    if (winner.squawk === null && loser.squawk !== null) {
      winner.squawk = loser.squawk;
      winner.emergencySquawk = winner.emergencySquawk || loser.emergencySquawk;
    }
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
export function nodeTransitSec(clockKey: string, sentMs: number, nowMs: number): number {
  const delta = nowMs - sentMs;
  let ring = _nodeClockDeltas.get(clockKey);
  if (!ring) {
    ring = [];
    _nodeClockDeltas.set(clockKey, ring);
  }
  ring.push(delta);
  if (ring.length > NODE_CLOCK_SAMPLES) ring.shift();

  let floor = ring[0]!;
  for (const d of ring) if (d < floor) floor = d;
  // Never negative: an upload cannot arrive before it was sent, and the first
  // upload from a node has nothing to compare against, so it counts as prompt.
  return Math.max(0, (delta - floor) / 1000);
}

/** Forget one node's clock estimate (it was deleted), or all of them (tests). */
export function _resetNodeClock(clockKey?: string): void {
  if (clockKey) _nodeClockDeltas.delete(clockKey);
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
  /**
   * Whose clock this is: the node's id. Not the source id, which is built
   * from the node's NAME and names are not unique: two receivers called
   * "home" shared one clock estimate, so one with a clock 30 s fast made
   * every upload from the other look 30 s late.
   */
  clockKey: string = sourceId,
): { records: AdsbAircraft[]; trails: Map<string, TrailPoint[]> } {
  const sentMs = Date.parse(upload.at);
  const transitSec = Number.isFinite(sentMs)
    ? nodeTransitSec(clockKey, sentMs, nowMs)
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

interface UpstreamResult {
  id: string;
  ok: boolean;
  circlesOk: number;
  circlesTotal: number;
  records: AdsbAircraft[];
  error?: string;
  /** A circle came back 401/403. */
  refused?: boolean;
  /** A circle came back 429. */
  limited?: boolean;
  retryAfterMs?: number | null;
}

/**
 * Spacing between one host's requests in a sweep. A shade over the
 * documented 1 req/s: the timers all start together and fire late by
 * varying amounts, so exactly 1000 ms could put two requests closer than
 * a second, and adsb.fi counts 429s toward a temporary IP ban.
 */
const STAGGER_MS = 1_100;

/**
 * Shard the circle list for one poll among `probes` upstreams that get a
 * single circle each and `full` upstreams that share the rest. The list is
 * rotated by tick before it is dealt, so at any tick the shards partition
 * CIRCLES exactly, and over successive polls every circle passes through
 * every full upstream — no circle is permanently blind to one aggregator's
 * feeder coverage.
 *
 * `caps` limits how many circles each full upstream may take (its rate-limit
 * share); what a capped upstream can't take goes to the others. Only when
 * every one is at its cap are circles left out, and then `lastDealt` (the
 * tick each circle was last dealt, by CIRCLES index) puts the longest-waiting
 * circles first, so the ones left out change every poll instead of the same
 * corner of the map going dark.
 */
export function shardCircles(
  tick: number,
  full: number,
  probes = 0,
  caps?: readonly number[],
  lastDealt?: readonly number[],
): Circle[][] {
  return shardCircleIndexes(tick, full, probes, caps, lastDealt).map((s) => s.map((i) => CIRCLES[i]!));
}

function shardCircleIndexes(
  tick: number,
  full: number,
  probes: number,
  caps?: readonly number[],
  lastDealt?: readonly number[],
): number[][] {
  const n = CIRCLES.length;
  const order = CIRCLES.map((_, j) => (((j + tick) % n) + n) % n);
  // Stable: with nothing left out last poll every circle ties, and the
  // rotation above stands.
  if (lastDealt) order.sort((a, b) => (lastDealt[a] ?? -1) - (lastDealt[b] ?? -1));
  const shards: number[][] = Array.from({ length: probes + full }, () => []);
  let k = 0;
  for (let p = 0; p < probes && k < n; p++) shards[p]!.push(order[k++]!);
  const capOf = (j: number): number => caps?.[j] ?? Infinity;
  for (let j = 0; k < n && full > 0; j = (j + 1) % full) {
    if (shards.slice(probes).every((s, q) => s.length >= capOf(q))) break;
    if (shards[probes + j]!.length >= capOf(j)) continue;
    shards[probes + j]!.push(order[k++]!);
  }
  return shards;
}

let _pollTick = 0;
/** The tick each circle (by CIRCLES index) was last dealt to an upstream. */
const _circleDealtTick: number[] = [];

// --- Upstream health ----------------------------------------------------
// Refused (401/403): the host has decided not to serve us — airplanes.live
// answers with a "contact us" message, adsb.one with a Cloudflare challenge.
// Nothing we send changes that, so one probe an hour.
const REFUSED_PAUSE_MS = 60 * 60_000;
// Rate-limited (429): honour Retry-After, else back off from 30 s doubling.
const LIMITED_MIN_PAUSE_MS = 30_000;
const LIMITED_MAX_PAUSE_MS = 10 * 60_000;
// ...and halve its circles per poll. Each run of this many clean polls that
// used its whole share earns one circle back (~1 min at the poll cadence):
// slow enough that it doesn't walk straight back into the limit it just hit.
const SHARE_GROW_POLLS = 4;
// Anything else (timeouts, 5xx) is usually transient; only a run of polls
// with every circle failing pauses it.
const FAILING_POLLS = 3;
const FAILING_PAUSE_MS = 2 * 60_000;

type PauseReason = 'refused' | 'rate_limited' | 'failing';
interface UpstreamHealth {
  pausedUntil: number;
  /** Why it was last paused; null while healthy. Non-null and past
   *  pausedUntil means its next poll is a single-circle probe. */
  reason: PauseReason | null;
  error: string | null;
  limitedPauseMs: number;
  failedPolls: number;
  /** Most circles it is dealt per poll; Infinity until it has rate-limited us. */
  share: number;
  /** Clean polls at its full share since the share last changed. */
  cleanAtShare: number;
}
const _health = new Map<string, UpstreamHealth>();

function healthOf(id: string): UpstreamHealth {
  let h = _health.get(id);
  if (!h) {
    h = {
      pausedUntil: 0, reason: null, error: null, limitedPauseMs: 0, failedPolls: 0,
      share: Infinity, cleanAtShare: 0,
    };
    _health.set(id, h);
  }
  return h;
}

/** Each upstream's circles for this poll; empty for one that is paused. */
function planShards(tick: number, nowMs: number): Circle[][] {
  const due = UPSTREAMS.map((u, i) => ({ i, h: healthOf(u.id) })).filter(({ h }) => h.pausedUntil <= nowMs);
  let probes = due.filter(({ h }) => h.reason !== null);
  let full = due.filter(({ h }) => h.reason === null);
  // Nobody healthy to share the map: the probes take all of it rather than
  // leave it dark. A probe that fails again just pauses again.
  if (full.length === 0) {
    full = probes;
    probes = [];
  }
  const dealt = shardCircleIndexes(
    tick,
    full.length,
    probes.length,
    full.map(({ h }) => h.share),
    _circleDealtTick,
  );
  for (const s of dealt) for (const c of s) _circleDealtTick[c] = tick;
  const out: Circle[][] = UPSTREAMS.map(() => []);
  [...probes, ...full].forEach(({ i }, k) => (out[i] = (dealt[k] ?? []).map((c) => CIRCLES[c]!)));
  return out;
}

/**
 * Fold one poll's result for an upstream into its health, and log only the
 * transitions — a source that has been refusing us for a day says so once.
 */
function recordUpstreamHealth(up: Upstream, r: UpstreamResult, nowMs: number): void {
  const h = healthOf(up.id);
  if (r.circlesTotal === 0) return; // paused this poll
  const pause = (reason: PauseReason, ms: number, error: string): void => {
    if (h.reason === null) {
      log.warn({ upstream: up.id, error, pauseSec: Math.round(ms / 1000) }, `adsb: upstream ${reason}, pausing`);
    }
    h.reason = reason;
    h.error = error;
    h.pausedUntil = nowMs + ms;
  };
  if (r.refused) {
    pause('refused', REFUSED_PAUSE_MS, r.error ?? 'refused');
    return;
  }
  if (r.limited) {
    // Halve what it was actually asked for: a share it never used is no
    // evidence it can take that much.
    const prevShare = h.share;
    h.share = Math.max(1, Math.floor(Math.min(h.share, r.circlesTotal) / 2));
    h.cleanAtShare = 0;
    if (h.share !== prevShare) {
      log.info({ upstream: up.id, share: h.share }, 'adsb: upstream rate-limited, fewer circles per poll');
    }
    h.limitedPauseMs = Math.min(
      LIMITED_MAX_PAUSE_MS,
      Math.max(LIMITED_MIN_PAUSE_MS, h.limitedPauseMs * 2),
    );
    pause('rate_limited', r.retryAfterMs ?? h.limitedPauseMs, r.error ?? 'HTTP 429');
    return;
  }
  if (r.circlesOk === 0) {
    h.failedPolls += 1;
    // A failed probe goes straight back to sleep.
    if (h.reason !== null || h.failedPolls >= FAILING_POLLS) {
      pause(h.reason ?? 'failing', h.reason === 'refused' ? REFUSED_PAUSE_MS : FAILING_PAUSE_MS, r.error ?? 'unknown error');
    }
    return;
  }
  if (h.reason !== null) log.info({ upstream: up.id, was: h.reason }, 'adsb: upstream answering again');
  // Grow the share back only on polls that used all of it (a probe doesn't),
  // and stop counting once it covers the whole map.
  if (Number.isFinite(h.share) && h.reason === null && r.circlesTotal >= h.share) {
    h.cleanAtShare += 1;
    if (h.cleanAtShare >= SHARE_GROW_POLLS) {
      h.share = h.share + 1 >= CIRCLES.length ? Infinity : h.share + 1;
      h.cleanAtShare = 0;
    }
  }
  h.reason = null;
  h.error = null;
  h.pausedUntil = 0;
  h.limitedPauseMs = 0;
  h.failedPolls = 0;
}

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
  /** The last squawk actually received, its emergency meaning, and when. */
  squawk: string | null;
  emergencySquawk: boolean;
  squawkAtMs: number;
}

const _lastSeen = new Map<string, LastSeen>();

/**
 * How long a squawk is carried onto records that arrive without one.
 *
 * A squawk rides in its own message, so a record often lacks it, and with it
 * the emergency flag: an aircraft on 7700 dropped off the emergency list
 * every poll whose freshest record happened to have no squawk. Short, because
 * a code can be changed at any moment and an emergency that has ended must
 * not linger.
 */
const SQUAWK_CARRY_SEC = 60;

/**
 * Below this pressure altitude an unheard aircraft is held, not projected.
 *
 * Losing an aircraft low down usually means it is landing: projecting a
 * final-approach heading and speed for three minutes flew the icon miles past
 * the runway, and a helicopter past its hospital pad.
 */
const ESTIMATE_MIN_ALT_FT = 2500;

/** A heading and speed older than this when last heard are not projected. */
const ESTIMATE_MAX_VEL_AGE_SEC = 60;

/**
 * hex -> age in seconds of an altitude this poll CARRIED onto a record that
 * arrived without one. For the map a two-minute-old altitude beats none; the
 * history archive is a record of what was heard, so it takes only a recent
 * one (see ARCHIVE_CARRIED_ALT_MAX_SEC). Rebuilt by every applyHoldover.
 */
const _carriedAltAgeSec = new Map<string, number>();

/** The archive keeps a carried altitude only this fresh: 30 s is at most
 *  about 1,000 ft out in a hard climb, against 4,000 ft at the full carry. */
const ARCHIVE_CARRIED_ALT_MAX_SEC = 30;

/** Records as the archive should store them: an altitude that was carried
 *  from too long ago is left out rather than written down as heard. */
export function withoutStaleCarriedAlt(records: readonly AdsbAircraft[]): AdsbAircraft[] {
  return records.map((a) => {
    const age = _carriedAltAgeSec.get(a.hex);
    return age !== undefined && age > ARCHIVE_CARRIED_ALT_MAX_SEC ? { ...a, altFt: null } : a;
  });
}

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
  for (const k of ADSB_IDENTITY_DETAIL_KEYS) {
    if (out[k] === undefined && prev.rec[k] !== undefined) (out as AdsbDetail)[k] = prev.rec[k] as never;
  }

  if (out.squawk === null && prev.squawk !== null
      && (nowMs - prev.squawkAtMs) / 1000 <= SQUAWK_CARRY_SEC) {
    out.squawk = prev.squawk;
    out.emergencySquawk = out.emergencySquawk || prev.emergencySquawk;
  }

  if (!out.onGround && !prev.rec.onGround) {
    if (out.altFt === null && prev.altFt !== null
        && (nowMs - prev.altAtMs) / 1000 <= CARRY_MAX_SEC) {
      out.altFt = prev.altFt;
      _carriedAltAgeSec.set(out.hex, (nowMs - prev.altAtMs) / 1000);
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
  _carriedAltAgeSec.clear();
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
      squawk: raw.squawk !== null ? raw.squawk : (prev?.squawk ?? null),
      emergencySquawk: raw.squawk !== null ? raw.emergencySquawk : (prev?.emergencySquawk ?? false),
      squawkAtMs: raw.squawk !== null ? nowMs : (prev?.squawkAtMs ?? 0),
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
      (rec.gsKt ?? 0) > ESTIMATE_MIN_GS_KT &&
      // Low down it is probably landing; an unknown altitude is not evidence.
      (rec.altFt === null || rec.altFt >= ESTIMATE_MIN_ALT_FT) &&
      // The heading and speed must themselves have been heard recently: a
      // carried pair can be two minutes old, and the aircraft has turned.
      (h.atMs - h.velAtMs) / 1000 <= ESTIMATE_MAX_VEL_AGE_SEC;

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

/** TEST-ONLY: reset holdover, rotation and upstream health between unit tests. */
export function _resetAdsbHoldoverForTests(): void {
  _lastSeen.clear();
  _carriedAltAgeSec.clear();
  _allDownLogged = false;
  _pollTick = 0;
  _health.clear();
  _circleDealtTick.length = 0;
  _flyLastAt = -Infinity;
  _flyNext = 0;
  _flyLastOk = false;
}

/**
 * Fetch one upstream's shard of circles, staggered STAGGER_MS apart to
 * respect the ~1 req/s politeness ceiling. Per-circle failures are
 * tolerated (that circle just contributes nothing); the upstream is
 * only marked down when every circle fails. A 401/403/429 stops the
 * rest of the sweep: the host has told us to go away or slow down, and
 * the remaining circles would only repeat the answer.
 */
/**
 * When each feed record's answer arrived. A record's ageSec is true at that
 * moment only: the sweep staggers its requests and waits for the slowest, so
 * by the merge a record can be many seconds older than it says, while a
 * receiver's record is aged right up to the merge. Left uncorrected, a feed
 * position really ten seconds old claimed one and beat the receiver's true
 * three, and its track point was stamped that much too late.
 */
const _arrivedMs = new WeakMap<AdsbAircraft, number>();

/** Feed records aged to `nowMs`; any past the age limit by then are dropped. */
export function ageToNow(records: readonly AdsbAircraft[], nowMs: number): AdsbAircraft[] {
  const out: AdsbAircraft[] = [];
  for (const r of records) {
    const at = _arrivedMs.get(r);
    if (at === undefined) {
      out.push(r);
      continue;
    }
    const ageSec = r.ageSec + Math.max(0, nowMs - at) / 1000;
    if (ageSec > MAX_SEEN_POS_SECS) continue;
    out.push({ ...r, ageSec });
  }
  return out;
}

async function fetchUpstream(up: Upstream, circles: readonly Circle[]): Promise<UpstreamResult> {
  let halted: { refused: boolean; retryAfterMs: number | null } | null = null;
  const results = await Promise.all(
    circles.map(async (c, i) => {
      if (i > 0) await new Promise((r) => setTimeout(r, i * STAGGER_MS));
      if (halted) return { ok: false as const, error: 'skipped' };
      try {
        const body = await fetchJson<ReadsbPointResponse>(up.url(c), {
          timeoutMs: 8_000,
          ...(up.headers ? { headers: up.headers() } : {}),
        });
        const ac = Array.isArray(body.ac)
          ? body.ac
          : Array.isArray(body.aircraft)
            ? body.aircraft
            : [];
        const arrivedMs = Date.now();
        const records: AdsbAircraft[] = [];
        for (const raw of ac) {
          const norm = normalizeAircraft(raw, up.id);
          if (!norm) continue;
          _arrivedMs.set(norm, arrivedMs);
          records.push(norm);
        }
        return { ok: true as const, records };
      } catch (err) {
        const e = err as Error & { status?: number | null; retryAfterMs?: number | null };
        if (e.status === 401 || e.status === 403 || e.status === 429) {
          halted ??= { refused: e.status !== 429, retryAfterMs: e.retryAfterMs ?? null };
        }
        return { ok: false as const, error: e.message };
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
  if (circlesOk === 0) out.error = errors.find((e) => e !== 'skipped') ?? errors[0] ?? 'unknown error';
  const stop = halted as { refused: boolean; retryAfterMs: number | null } | null;
  if (stop) {
    if (stop.refused) out.refused = true;
    else out.limited = true;
    out.retryAfterMs = stop.retryAfterMs;
    out.error ??= errors.find((e) => e !== 'skipped');
  }
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

// Shared with services/adsbTrackArchive.ts, which must not import this module
// at runtime (this module imports it). Re-exported so callers keep one name.
export { simplifyTrail };

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
  // The archive keeps every one of these at the rate the receiver heard
  // them; the trail below thins them for display.
  noteAdsbNodePoints(hex, fresh);

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
  _trailsDirty = true;
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
    // Stamped with when the fix was MADE, not when the poll saw it. A
    // receiver's own uploads land here between polls with their real times,
    // so an aggregator fix a few seconds old stamped "now" went on the end
    // of a trail that already ran past it: the line overshot the aircraft
    // and doubled back to it. A fix no newer than what the trail already
    // holds adds nothing.
    const fixMs = nowMs - Math.max(0, Number.isFinite(a.ageSec) ? a.ageSec : 0) * 1000;
    if (last && fixMs <= last[0] + TRAIL_SAME_FIX_MS) continue;
    buf.push([fixMs, a.lat, a.lon, a.altFt]);
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
  rebuildTrailsSnapshot(nowMs);
}

const r5 = (v: number): number => Math.round(v * 1e5) / 1e5;

/** Rebuild the served trails from `_trails`. Reads nothing else and changes
 *  nothing else, so it is safe to run between polls. */
function rebuildTrailsSnapshot(nowMs: number): void {
  const out: AdsbTrailsSnapshot['trails'] = {};
  for (const [hex, buf] of _trails) {
    if (buf.length < 2) continue;
    out[hex] = buf.map((p) => [r5(p[1]), r5(p[2]), p[3]]);
  }
  _trailsSnapshot = { trails: out, fetched_at: new Date(nowMs).toISOString() };
  _trailsDirty = false;
  _trailsBuiltMs = nowMs;
}

/**
 * Receiver uploads add points between polls. Without this the served trails
 * only caught up when the next poll finished — every 16-23 s — so a receiver's
 * one-second points reached the map as coarsely as an aggregator's. Rebuilt on
 * read, at most every TRAIL_REBUILD_MIN_MS, so many readers cost one build.
 */
const TRAIL_REBUILD_MIN_MS = 2_000;
let _trailsDirty = false;
let _trailsBuiltMs = 0;

export function adsbTrailsSnapshot(nowMs: number = Date.now()): AdsbTrailsSnapshot {
  if (_trailsDirty && nowMs - _trailsBuiltMs >= TRAIL_REBUILD_MIN_MS) rebuildTrailsSnapshot(nowMs);
  return _trailsSnapshot;
}

// ---------------------------------------------------------------------
// "Recent" reads: what changed in the last few seconds.
//
// The full snapshot is rebuilt when the aggregator sweep finishes, every
// 16-23 s, and that is as fast as aggregator positions change anyway. Our own
// receivers upload every couple of seconds, so for the aircraft they hear there
// is fresher data sitting in the node store and the trail buffer. These reads
// hand out only that fresh part, so a client can poll it often and cheaply.
//
// Pure reads. They deliberately do NOT go through fetchAdsbAircraft: its side
// effects (missed-aircraft counts, the history archive, the holdover clock)
// are per poll and would be skewed by running more often.

/** Upper bound for `recent`, seconds. Past a minute it is the full snapshot. */
export const ADSB_RECENT_MAX_SEC = 60;

export interface AdsbRecentTrails {
  /** hex → [[lat, lon, altFt|null, epochMs], ...] oldest→newest. */
  trails: Record<string, Array<[number, number, number | null, number]>>;
  fetched_at: string;
}

const _recentAircraftMemo = new Map<string, AdsbSnapshot>();
const _recentTrailsMemo = new Map<string, AdsbRecentTrails>();

/** Memoised per second: any number of readers cost one build a second. */
function memo<T>(cache: Map<string, T>, key: string, build: () => T): T {
  const hit = cache.get(key);
  if (hit) return hit;
  if (cache.size > 16) cache.clear();
  const v = build();
  cache.set(key, v);
  return v;
}

/**
 * Aircraft whose position is no older than `recentSec`, in the snapshot shape.
 *
 * The snapshot's records are aged by how long the snapshot has been held, then
 * every receiver record is merged over them (freshest position wins, metadata
 * fills in from the other), exactly as the poll merges them.
 */
export function adsbRecentAircraft(recentSec: number, nowMs: number = Date.now()): AdsbSnapshot {
  const sec = Math.max(1, Math.min(ADSB_RECENT_MAX_SEC, Math.round(recentSec)));
  return memo(_recentAircraftMemo, `${sec}|${Math.floor(nowMs / 1000)}`, () => {
    const snap = adsbSnapshot();
    const fetchedMs = Date.parse(snap.fetched_at);
    const held = Number.isFinite(fetchedMs) ? Math.max(0, (nowMs - fetchedMs) / 1000) : 0;
    const aged = snap.aircraft
      .filter((a) => !a.estimated)
      .map((a) => ({ ...a, ageSec: a.ageSec + held }));
    const merged = mergeAircraft([...aged, ...nodeAdsbRecords(nowMs)])
      .filter((a) => a.ageSec <= sec && !a.estimated && inAuBbox(a.lat, a.lon));
    return {
      ...snap,
      aircraft: merged,
      count: merged.length,
      emergency_count: merged.filter((a) => a.esTag !== null).length,
      fetched_at: new Date(nowMs).toISOString(),
    };
  });
}

/** Trail points from the last `recentSec`, each with the time of its fix. */
export function adsbRecentTrails(recentSec: number, nowMs: number = Date.now()): AdsbRecentTrails {
  const sec = Math.max(1, Math.min(ADSB_RECENT_MAX_SEC, Math.round(recentSec)));
  return memo(_recentTrailsMemo, `${sec}|${Math.floor(nowMs / 1000)}`, () => {
    const cutoff = nowMs - sec * 1000;
    const out: AdsbRecentTrails['trails'] = {};
    for (const [hex, buf] of _trails) {
      // Newest last, so walk back only as far as the window reaches.
      let i = buf.length;
      while (i > 0 && buf[i - 1]![0] >= cutoff) i -= 1;
      if (i === buf.length) continue;
      out[hex] = buf.slice(i).map((p) => [r5(p[1]), r5(p[2]), p[3], Math.round(p[0])]);
    }
    return { trails: out, fetched_at: new Date(nowMs).toISOString() };
  });
}

/** One aircraft's raw, timestamped trail. */
export interface ArchivableTrack {
  hex: string;
  /** [epochMs, lat, lon, altFt|null], oldest first. */
  points: ReadonlyArray<TrailPoint>;
}

/**
 * Every live trail with its timestamps, which the served snapshot drops.
 *
 * Exposed rather than exporting `_trails` itself so the buffer stays owned by
 * this module. History does NOT read it: the trail is thinned for display, so
 * services/adsbTrackArchive.ts collects positions as they are observed.
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
  _trailsDirty = false;
  _trailsBuiltMs = 0;
  _recentAircraftMemo.clear();
  _recentTrailsMemo.clear();
}

/**
 * This poll's FlyItalyADSB request, if it has one. Null when no key is set,
 * so the source leaves no trace in the snapshot.
 *
 * A poll inside the request interval sends nothing and reports the last
 * request's outcome, so the status page does not flicker between polls.
 */
async function pollFlyItaly(nowMs: number): Promise<UpstreamResult | null> {
  if (!(config.FLYITALY_ADSB_KEY ?? '').trim()) return null;
  const h = healthOf(FLYITALY.id);
  const idle = (ok: boolean, error?: string): UpstreamResult => {
    const r: UpstreamResult = { id: FLYITALY.id, ok, circlesOk: 0, circlesTotal: 0, records: [] };
    if (error !== undefined) r.error = error;
    return r;
  };
  if (h.pausedUntil > nowMs) {
    return idle(false, `paused (${h.reason ?? 'unknown'}) until ${new Date(h.pausedUntil).toISOString()}: ${h.error ?? ''}`.trim());
  }
  if (nowMs - _flyLastAt < FLYITALY_MIN_INTERVAL_MS) return idle(_flyLastOk);
  _flyLastAt = nowMs;
  const circle = FLYITALY_FOCUS[_flyNext++ % FLYITALY_FOCUS.length]!;
  const r = await fetchUpstream(FLYITALY, [circle]);
  recordUpstreamHealth(FLYITALY, r, Date.now());
  _flyLastOk = r.ok;
  return r;
}

/** True while a total outage is in progress, so it is logged once. */
let _allDownLogged = false;

export async function fetchAdsbAircraft(): Promise<AdsbSnapshot> {
  // Upstreams in parallel — different hosts, no shared rate limit.
  // Each answering one gets this poll's shard of the circle rotation; a
  // paused one gets none and reports why.
  // ADSB_DISABLED turns off UPSTREAM polling only; our own feeder nodes keep
  // reporting through the same pipeline below.
  const tick = _pollTick++;
  const planNow = Date.now();
  const shards = planShards(tick, planNow);
  const [rotation, fly] = config.ADSB_DISABLED
    ? [[] as UpstreamResult[], null]
    : await Promise.all([Promise.all(
        UPSTREAMS.map(async (u, i): Promise<UpstreamResult> => {
          const circles = shards[i] ?? [];
          if (circles.length > 0) return fetchUpstream(u, circles);
          const h = healthOf(u.id);
          return {
            id: u.id,
            ok: false,
            circlesOk: 0,
            circlesTotal: 0,
            records: [],
            error: `paused (${h.reason ?? 'no circles'}) until ${new Date(h.pausedUntil).toISOString()}: ${h.error ?? ''}`.trim(),
          };
        }),
      ), pollFlyItaly(planNow)]);
  const doneNow = Date.now();
  rotation.forEach((r, i) => recordUpstreamHealth(UPSTREAMS[i]!, r, doneNow));
  // FlyItaly records its own health (pollFlyItaly); from here on it is just
  // another upstream.
  const results = fly ? [...rotation, fly] : rotation;

  // Our own ADS-B feeder nodes are just another source: a ~5s cadence against
  // the aggregators' ~15s, covering whatever the circle rotation misses.
  // mergeAircraft resolves any overlap — freshest position wins, metadata
  // backfills from the other record, sources union — so nothing here needs to
  // know which kind of source a record came from.
  // One instant for everything below: every source's age is measured to it,
  // so "freshest wins" compares like with like.
  const mergeNow = Date.now();
  const nodeRecords = nodeAdsbRecords(mergeNow);
  const nodeFeeds = nodeAdsbFeedCount();

  const fresh = mergeAircraft([
    ...ageToNow(results.flatMap((r) => r.records), mergeNow),
    ...nodeRecords,
  ]).filter((a) => inAuBbox(a.lat, a.lon));
  // Only a genuine upstream outage is a failure. With ADSB_DISABLED there are
  // no upstreams to be down, and node data flowing during an aggregator
  // outage means the source is working — neither should trip the backoff.
  const allDown = results.length > 0 && results.every((r) => !r.ok);
  if (allDown && fresh.length === 0) {
    // A total outage is NOT thrown. The poller's backoff starts at five
    // minutes and climbs, so a twenty-second network blip stopped aircraft
    // for five minutes while the last snapshot sat frozen and looked live.
    // Each feed already rests itself when it fails (recordUpstreamHealth), so
    // carrying on costs nothing: the held aircraft age, are estimated and
    // drop out on their own clocks, and the next poll that gets an answer is
    // the recovery. Logged once per outage, not once per poll.
    if (!_allDownLogged) {
      _allDownLogged = true;
      log.warn(
        { upstreams: results.map((r) => `${r.id}: ${r.error ?? 'unknown'}`) },
        'adsb: no source answered; serving held aircraft until one does',
      );
    }
  } else if (_allDownLogged) {
    _allDownLogged = false;
    log.info('adsb: a source is answering again');
  }

  const merged = applyHoldover(fresh, mergeNow);
  // Trails (and, through them, the persisted history) record only OBSERVED
  // positions. A frozen holdover record was harmless here because
  // updateTrails already skips a point identical to the last one, but an
  // estimated record MOVES — without this filter every trail would accumulate
  // fabricated positions indistinguishable from real ones.
  const observed = merged.filter((a) => !a.estimated);
  updateTrails(observed, mergeNow);
  // What each of our receivers should have heard and did not: the coverage
  // view's dark dots. Observed positions only — a dead-reckoned one is a guess.
  noteNetworkAircraft(observed, mergeNow);
  // Persist for the historical view: the observed positions go into the
  // archive's current minute, and any minute that has closed is written.
  // Driven from here rather than its own timer so a write can only ever
  // follow a finished poll.
  noteAdsbPositions(withoutStaleCarriedAlt(observed), mergeNow);
  maybeFlushAdsbTracks(Date.now());

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
    // The instant every ageSec above is measured to.
    fetched_at: new Date(mergeNow).toISOString(),
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
    // The poller re-arms *after* each run completes; 8 s + the staggered
    // sweep (~7.7 s with four upstreams answering, ~15 s with two) lands
    // the effective cadence at ~16-23 s, under 1 req/s on every host.
    intervalMs: 8_000,
    fetch: fetchAdsbAircraft,
  });
}

export function adsbSnapshot(): AdsbSnapshot {
  return liveStore.getData<AdsbSnapshot>('adsb_aircraft') ?? EMPTY_SNAPSHOT;
}
