/**
 * Aircraft snapshots uploaded by ADS-B feeder nodes.
 *
 * Two jobs, deliberately kept apart:
 *
 *  1. **The live picture** — an in-memory, per-node latest snapshot that
 *     `sources/adsb.ts` folds into the same merge the upstream aggregators
 *     feed. Ephemeral by design: aircraft positions are worthless within a
 *     minute, so there is nothing here worth persisting or recovering after a
 *     restart. A node re-uploads within 5 seconds.
 *
 *  2. **The performance record** — a per-node-per-day aggregate for the staff
 *     Data tab, accumulated in memory and flushed to Postgres once a minute.
 *     NOT written per upload: a fleet uploading every 5s is ~17k uploads per
 *     node per day, and this table exists to answer "how is that receiver
 *     doing", not to store every position it ever heard.
 *
 * Why node data is worth merging at all: our own receivers report on a ~5s
 * cadence against the aggregators' ~15s effective cadence, and they cover
 * whatever the aggregator circles miss. `mergeAircraft` already resolves the
 * overlap correctly — freshest position wins, metadata backfills, sources
 * union — so a node simply becomes another upstream.
 */
import { getPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';
import { flushAdsbCoverage, clearAdsbCoverage } from './adsbCoverage.js';
import { formatSydneyNaive } from '../../lib/sydneyTime.js';
// TYPE-ONLY, deliberately: sources/adsb.ts imports this module at runtime to
// read node records, so a runtime import back the other way would be a cycle
// between the upstream poller and the node layer. The wire-format conversion
// lives there (normalizeNodeUpload) for the same reason.
import type { AdsbAircraft } from '../../sources/adsb.js';

/** Decoder statistics accompanying a snapshot (from dump1090's stats.json). */
export interface NodeAdsbStats {
  msgRate?: number | null;
  aircraftTotal?: number | null;
  aircraftWithPos?: number | null;
  tracksAll?: number | null;
  maxRangeKm?: number | null;
  /** Mean and peak receive level over the decoder's last minute, in dBFS —
   *  always negative, closer to zero being stronger. Absent from any receiver
   *  running an agent older than 0.1.6: dump1090 has always reported these and
   *  the agent has always read them for its own gain loop, but they were never
   *  put on the wire. */
  signalDbfs?: number | null;
  signalPeakDbfs?: number | null;
}

/**
 * A node's snapshot is dropped this long after we received it. Two upload
 * cadences (5s) would be too tight — a single slow POST would blink the node
 * out of the picture — while anything beyond the 60s position cutoff is
 * pointless because every record inside would have aged out anyway.
 */
const NODE_SNAPSHOT_TTL_MS = 120_000;

/** Matches MAX_SEEN_POS_SECS in sources/adsb.ts — re-aged records past this
 *  are dropped rather than shown at a stale position. */
const MAX_AGE_SEC = 60;

interface NodeSnapshot {
  name: string;
  receivedAtMs: number;
  records: AdsbAircraft[];
}

const snapshots = new Map<string, NodeSnapshot>();

// ---------------------------------------------------------------------------
// Range, measured here rather than taken from the decoder
// ---------------------------------------------------------------------------
//
// dump1090 computes a max-range statistic of its own, and the agent forwards
// it — but ONLY when the decoder was given --lat/--lon, and only in whatever
// field name that build happens to use. When any link in that chain is missing
// the figure is simply absent, and every range cell on the Data tab reads "—"
// while the coverage map plainly shows aircraft two hundred kilometres out.
//
// We do not need to depend on it. The backend holds both halves already: the
// receiver's exact pin, and every position that receiver just reported. So the
// distance is measured from what arrived. The decoder's own number is still
// taken when present (see accumulateAdsbDaily, which keeps the larger of the
// two), but nothing depends on it any more.

const EARTH_RADIUS_KM = 6371;

/** Great-circle distance in km. */
export function distanceKm(
  lat1: number, lon1: number, lat2: number, lon2: number,
): number {
  const toRad = Math.PI / 180;
  const dLat = (lat2 - lat1) * toRad;
  const dLon = (lon2 - lon1) * toRad;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * toRad) * Math.cos(lat2 * toRad) * Math.sin(dLon / 2) ** 2;
  return 2 * EARTH_RADIUS_KM * Math.asin(Math.min(1, Math.sqrt(a)));
}

/** Nearest and furthest aircraft in one upload, in km from the pin. */
export interface SnapshotRange { minKm: number; maxKm: number }

/**
 * How near and how far this receiver is hearing, in one upload.
 *
 * Both ends, because one number could not say what it was being asked. A lone
 * "range" reads as reach, but the figure moves whenever the furthest aircraft
 * leaves, so it was really "the furthest thing in the sky right now" — which
 * says as much about the traffic as the receiver. Alongside the nearest it is
 * legible: the pair describes the slice of sky currently in view, and a
 * nearest that climbs is the sign of a receiver going deaf close in.
 *
 * Null when the node has no pin — there is nothing to measure from, and the UI
 * already explains an absent range that way ("no pin") rather than as a fault.
 */
export function snapshotRangeKm(
  lat: unknown,
  lon: unknown,
  records: ReadonlyArray<{ lat: number; lon: number }>,
): SnapshotRange | null {
  if (typeof lat !== 'number' || typeof lon !== 'number') return null;
  let min: number | null = null;
  let max: number | null = null;
  for (const r of records) {
    if (!Number.isFinite(r.lat) || !Number.isFinite(r.lon)) continue;
    const d = distanceKm(lat, lon, r.lat, r.lon);
    if (max === null || d > max) max = d;
    if (min === null || d < min) min = d;
  }
  return max === null || min === null ? null : { minKm: min, maxKm: max };
}

/** The furthest aircraft alone, for the callers that only bank the peak. */
export function snapshotMaxRangeKm(
  lat: unknown,
  lon: unknown,
  records: ReadonlyArray<{ lat: number; lon: number }>,
): number | null {
  return snapshotRangeKm(lat, lon, records)?.maxKm ?? null;
}

/**
 * The nearest and furthest aircraft in each node's LATEST upload.
 *
 * Both are "right now", as against the day's peak in node_adsb_daily. Kept
 * beside the snapshots because they have exactly their lifetime: they mean
 * nothing once the upload they were measured from has aged out, and a stale
 * pair is worse than none — it reads as a receiver still hearing.
 */
const observedRangeKm = new Map<string, { range: SnapshotRange; atMs: number }>();

/**
 * Range from this node's most recent upload, or null if it has not reported
 * one (no pin, an empty sky, or the node has gone away).
 *
 * Expiry keys off its OWN timestamp rather than the presence of a live
 * snapshot: a node whose feed is paused still reports, and still has a current
 * range, but deliberately has no snapshot on the map to hang that off.
 */
export function nodeAdsbObservedRange(nodeId: string): SnapshotRange | null {
  const held = observedRangeKm.get(nodeId);
  if (!held) return null;
  if (Date.now() - held.atMs > NODE_SNAPSHOT_TTL_MS) {
    observedRangeKm.delete(nodeId);
    return null;
  }
  return held.range;
}

/** The furthest of the two, for callers that bank a single peak. */
export function nodeAdsbObservedRangeKm(nodeId: string): number | null {
  return nodeAdsbObservedRange(nodeId)?.maxKm ?? null;
}

/** The source id a node's records carry, e.g. `node:adsb-akumascoffin-a3f9`.
 *  Shows up in the aircraft's `sources[]` on the map, so a position that came
 *  from our own hardware is distinguishable from an aggregator's. */
export function adsbNodeSourceId(nodeId: string, name: string | null): string {
  const label = (name ?? '').trim() || nodeId.slice(0, 8);
  return `node:${label}`;
}

/**
 * Store one node's latest snapshot, replacing whatever it sent before.
 *
 * Records arrive already normalized (see normalizeNodeUpload in
 * sources/adsb.ts, which also folds in transit delay). Last-write-wins is
 * correct here: a newer snapshot is a complete restatement of what that
 * receiver can currently see, so merging it with the previous one would
 * resurrect aircraft that have since left its coverage.
 */
export function recordNodeAdsbSnapshot(
  nodeId: string,
  nodeName: string | null,
  records: AdsbAircraft[],
): number {
  snapshots.set(nodeId, {
    name: nodeName ?? '',
    receivedAtMs: Date.now(),
    records,
  });
  return records.length;
}

/**
 * Record what a receiver HEARD, whether or not its feed is on.
 *
 * Split from the snapshot above because the two answer different questions and
 * are gated differently. The snapshot is what reaches the public map, so it is
 * rightly behind the feed gate. Traces, recent aircraft and range-now are the
 * node's OWN performance record — the same category as the daily counters and
 * the coverage envelope, both of which are already accumulated before that
 * gate for exactly this reason.
 *
 * Having this behind the gate meant a receiver with its feed paused showed a
 * coverage envelope (accumulated before the gate) and no tracks at all
 * (recorded after it), which reads as a broken map rather than a paused feed.
 */
export function recordNodeAdsbReception(
  nodeId: string,
  records: AdsbAircraft[],
  range: SnapshotRange | null = null,
): void {
  const nowMs = Date.now();
  if (range !== null) observedRangeKm.set(nodeId, { range, atMs: nowMs });
  recordTraces(nodeId, records, nowMs);
}

/**
 * Every live node record, re-aged to `nowMs`.
 *
 * The poller runs on its own schedule (~8s) independent of when nodes upload,
 * so each record's age has to be advanced by however long its snapshot has
 * been sitting here, and anything that ages past the cutoff is dropped rather
 * than merged at a stale position.
 */
export function nodeAdsbRecords(nowMs: number = Date.now()): AdsbAircraft[] {
  const out: AdsbAircraft[] = [];
  for (const [nodeId, snap] of snapshots) {
    const heldSec = (nowMs - snap.receivedAtMs) / 1000;
    if (heldSec * 1000 > NODE_SNAPSHOT_TTL_MS) {
      // Node went away (offline, disabled, network). Forget it rather than
      // letting its last snapshot linger on the map.
      snapshots.delete(nodeId);
      continue;
    }
    for (const rec of snap.records) {
      const ageSec = rec.ageSec + heldSec;
      if (ageSec > MAX_AGE_SEC) continue;
      out.push({ ...rec, ageSec });
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Recent traces (the coverage picture)
// ---------------------------------------------------------------------------
//
// The paths aircraft took through this receiver's coverage over the last hour
// — tar1090's pTracks view, which is the single most useful diagnostic a
// receiver operator has: it shows at a glance which directions the antenna
// actually hears, and how far.
//
// In memory, not Postgres, and deliberately so. Storing every position would
// be millions of rows a day per node for something only ever read as a picture
// of the last hour; the aggregates in node_adsb_daily already answer every
// question that outlives the hour. The cost is that a backend restart resets
// the picture, which for a live coverage view is acceptable — it refills within
// minutes.
//
// Points are decimated on both time and distance: a receiver reports every 5s,
// but a coverage picture needs nothing like that resolution, and an aircraft
// holding still (on stand, or in a holding pattern) should not accumulate
// hundreds of identical points.

/**
 * How far back traces are kept.
 *
 * Eight hours, matching tar1090's pTracks default, because coverage is a
 * question you cannot answer from an hour of data: a receiver's true range and
 * its blind bearings only emerge once enough aircraft have crossed the area
 * from enough directions. An hour of a quiet afternoon looks identical to a
 * receiver with a broken antenna.
 */
const TRACE_WINDOW_MS = 8 * 60 * 60 * 1000;

/**
 * Shortest gap between stored points for a MOVING aircraft.
 *
 * An airborne transponder reports position about twice a second, but the agent
 * samples the decoder's state file every five, so five seconds IS the finest
 * resolution that can reach us — anything longer here throws away uploads we
 * already paid for and turns a curve back into straight hops. The distance
 * rule below is what stops this becoming twelve points a minute for an
 * aircraft that is not going anywhere.
 */
const TRACE_MIN_GAP_MS = 5_000;

/**
 * Longest gap before a point is stored regardless of movement.
 *
 * A parked aircraft still has to appear somewhere, and its position is the only
 * evidence the receiver is still hearing it at all.
 */
const TRACE_IDLE_GAP_MS = 120_000;

/**
 * Movement that counts as having gone somewhere, in km.
 *
 * TRUE DISTANCE, not each axis separately. It used to be a per-axis threshold
 * of 0.01 degrees, which quietly depended on heading: an aircraft flying due
 * north covered it in one step, while the same aircraft at the same speed
 * heading north-EAST split its movement between the two axes, tripped neither,
 * and fell back on the idle rule. Those tracks were drawn as minute-long
 * straight lines — about thirteen kilometres at cruise — through turns the
 * aircraft actually flew.
 */
const TRACE_MIN_MOVE_KM = 0.15;

/**
 * Per-aircraft point cap.
 *
 * An hour of continuous movement at the minimum gap. Airliners cross a
 * receiver's range in twenty minutes or so, so this bounds the pathological
 * case — a stuck position, a helicopter orbiting all afternoon — rather than
 * normal traffic. Anything evicted here has already been flushed to
 * node_adsb_tracks, so the map still draws it: the archive is what holds the
 * long tail, and memory only has to hold what has not been written yet.
 */
const TRACE_MAX_POINTS = 720;

/**
 * Per-node aircraft cap.
 *
 * A busy metropolitan receiver sees a few hundred distinct aircraft an hour, so
 * eight hours could legitimately be a couple of thousand. Above this the OLDEST
 * traces are evicted rather than new ones refused: the recent picture matters
 * more than the far end of the window, and silently dropping current aircraft
 * would make a busy receiver look like it had stopped hearing.
 */
const TRACE_MAX_AIRCRAFT = 4000;

interface Trace {
  /** Parallel arrays rather than an array of objects: three number arrays cost
   *  a fraction of the memory of hundreds of small objects, and this is the one
   *  structure here that grows with traffic. */
  lat: number[];
  lon: number[];
  t: number[];
  callsign: string | null;
  lastMs: number;
  /** Latest and highest altitude seen, feet.
   *
   *  Two scalars, not a fourth parallel array: 4000 aircraft x 240 altitudes
   *  would be stored purely to render one number per row of a table. */
  lastAltFt: number | null;
  maxAltFt: number | null;
  /**
   * How many reports this aircraft was seen in — including the ones the gap
   * and distance rules below decline to store as points.
   *
   * This is the "how solidly was it held" signal that a point count cannot
   * give: an aircraft parked in view has one point and thousands of reports,
   * and one crossing the edge of coverage has few of both.
   */
  reports: number;
}

const traces = new Map<string, Map<string, Trace>>();

/** Fold one snapshot's positions into the node's traces. */
function recordTraces(nodeId: string, records: AdsbAircraft[], nowMs: number): void {
  let byHex = traces.get(nodeId);
  if (!byHex) {
    byHex = new Map<string, Trace>();
    traces.set(nodeId, byHex);
  }

  for (const r of records) {
    const tr = byHex.get(r.hex);
    if (!tr) {
      if (byHex.size >= TRACE_MAX_AIRCRAFT) evictOldestTrace(byHex);
      byHex.set(r.hex, {
        lat: [r.lat], lon: [r.lon], t: [nowMs],
        callsign: r.callsign, lastMs: nowMs,
        lastAltFt: r.altFt, maxAltFt: r.altFt, reports: 1,
      });
      continue;
    }
    // Keep the callsign once it is known: an aircraft is usually tracked for a
    // while before it transmits one, so the first point rarely has it.
    if (!tr.callsign && r.callsign) tr.callsign = r.callsign;

    // Counted BEFORE the dedupe below, which is the whole point: an aircraft
    // sitting still contributes reports without contributing points.
    tr.reports += 1;
    if (r.altFt !== null) {
      tr.lastAltFt = r.altFt;
      if (tr.maxAltFt === null || r.altFt > tr.maxAltFt) tr.maxAltFt = r.altFt;
    }

    const n = tr.lat.length;
    const dt = nowMs - tr.t[n - 1]!;
    const movedKm = distanceKm(tr.lat[n - 1]!, tr.lon[n - 1]!, r.lat, r.lon);
    // Moving and enough time has passed, OR it has been long enough that even
    // a stationary aircraft is worth a point. The first keeps a track smooth;
    // the second stops an aircraft parked in view filling the buffer.
    const worthStoring =
      (dt >= TRACE_MIN_GAP_MS && movedKm >= TRACE_MIN_MOVE_KM) ||
      dt >= TRACE_IDLE_GAP_MS;
    if (!worthStoring) {
      tr.lastMs = nowMs;
      continue;
    }

    tr.lat.push(r.lat);
    tr.lon.push(r.lon);
    tr.t.push(nowMs);
    tr.lastMs = nowMs;
    if (tr.lat.length > TRACE_MAX_POINTS) {
      tr.lat.shift();
      tr.lon.shift();
      tr.t.shift();
    }
  }
}

/**
 * Make room by forgetting the least recently seen aircraft.
 *
 * Reached only on a receiver busy enough to fill the cap inside the window, and
 * the oldest trace is the right one to lose: the near end of the window is what
 * anyone is looking at, and refusing NEW aircraft instead would make a busy
 * receiver appear to have stopped hearing.
 */
function evictOldestTrace(byHex: Map<string, Trace>): void {
  let oldestHex: string | null = null;
  let oldestMs = Infinity;
  for (const [hex, tr] of byHex) {
    if (tr.lastMs < oldestMs) {
      oldestMs = tr.lastMs;
      oldestHex = hex;
    }
  }
  if (oldestHex) byHex.delete(oldestHex);
}

/** Drop points and aircraft that have aged out of the window. */
function pruneTraces(nodeId: string, nowMs: number): void {
  const byHex = traces.get(nodeId);
  if (!byHex) return;
  const cutoff = nowMs - TRACE_WINDOW_MS;
  for (const [hex, tr] of byHex) {
    // Points are appended in time order, so the expired ones are a prefix.
    let drop = 0;
    while (drop < tr.t.length && tr.t[drop]! < cutoff) drop += 1;
    if (drop > 0) {
      tr.lat.splice(0, drop);
      tr.lon.splice(0, drop);
      tr.t.splice(0, drop);
    }
    if (tr.lat.length === 0) byHex.delete(hex);
  }
  if (byHex.size === 0) traces.delete(nodeId);
}

export interface NodeTrace {
  hex: string;
  callsign: string | null;
  /** [lat, lon] pairs in time order. Timestamps are dropped on the way out —
   *  the view draws paths, and shipping a third number per point would inflate
   *  the response for something nothing renders. */
  points: Array<[number, number]>;
}

/** The same trace with its timings intact, for merging against stored history.
 *  Timestamps are dropped again before the view goes on the wire. */
export interface NodeTraceTimed {
  hex: string;
  callsign: string | null;
  /** [epochMs, lat, lon], oldest first. */
  points: Array<[number, number, number]>;
}

/**
 * One receiver's traces over the last `minutes`, with times.
 *
 * Single-point traces are KEPT here, unlike the view's own output: a lone live
 * point may be the newest position of an aircraft whose earlier path is on
 * disk, and dropping it at this level would lose the join. The caller decides
 * what is too short to draw once both halves are in hand.
 */
export function nodeAdsbTraces(
  nodeId: string,
  minutes: number,
  nowMs: number = Date.now(),
): { traces: NodeTraceTimed[]; aircraft: number; points: number; windowMinutes: number } {
  pruneTraces(nodeId, nowMs);
  const byHex = traces.get(nodeId);
  if (!byHex) return { traces: [], aircraft: 0, points: 0, windowMinutes: minutes };

  const cutoff = nowMs - Math.max(1, minutes) * 60_000;
  const out: NodeTraceTimed[] = [];
  let points = 0;
  let aircraft = 0;

  for (const [hex, tr] of byHex) {
    const pts: Array<[number, number, number]> = [];
    for (let i = 0; i < tr.t.length; i += 1) {
      if (tr.t[i]! < cutoff) continue;
      pts.push([tr.t[i]!, tr.lat[i]!, tr.lon[i]!]);
    }
    if (pts.length === 0) continue;
    aircraft += 1;
    points += pts.length;
    out.push({ hex, callsign: tr.callsign, points: pts });
  }
  return { traces: out, aircraft, points, windowMinutes: minutes };
}

/** One node's raw trace, for services/nodes/nodeTrackArchive.ts. */
export interface NodeArchivableTrack {
  nodeId: string;
  hex: string;
  callsign: string | null;
  reports: number;
  lastAltFt: number | null;
  maxAltFt: number | null;
  /** [epochMs, lat, lon, null], oldest first. The fourth slot keeps the shape
   *  of the global archive's points and is always null: a Trace holds altitude
   *  as two scalars, not per point. */
  points: ReadonlyArray<[number, number, number, number | null]>;
}

/**
 * Every node's live traces, for persistence.
 *
 * Exposed here rather than exporting the map itself so the buffer stays owned
 * by this module — the archive reads, it never appends or prunes, and the
 * decimation rules stay in one place.
 */
export function nodeTracesForArchive(): NodeArchivableTrack[] {
  const out: NodeArchivableTrack[] = [];
  for (const [nodeId, byHex] of traces) {
    for (const [hex, tr] of byHex) {
      if (tr.t.length === 0) continue;
      const points: Array<[number, number, number, number | null]> = [];
      for (let i = 0; i < tr.t.length; i += 1) {
        // null, not tr.lastAltFt: that is the CURRENT altitude, and stamping
        // it onto every historical point would claim the aircraft flew the
        // whole track at whatever height it is at now.
        points.push([tr.t[i]!, tr.lat[i]!, tr.lon[i]!, null]);
      }
      out.push({
        nodeId, hex, callsign: tr.callsign, points,
        reports: tr.reports, lastAltFt: tr.lastAltFt, maxAltFt: tr.maxAltFt,
      });
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Recent aircraft (what this receiver actually heard)
// ---------------------------------------------------------------------------
//
// Derived entirely from the traces above — no second structure, because the
// traces already hold every fact this answers and a parallel "recent" list
// would be one more thing to prune in step with them.

export interface NodeRecentAircraft {
  hex: string;
  callsign: string | null;
  /** Epoch ms. Timestamps are dropped from `NodeTrace` because a path does not
   *  need them; here they ARE the answer, so they stay. */
  firstMs: number;
  lastMs: number;
  /** Stored points versus reports received — see `Trace.reports`. A large gap
   *  between them means an aircraft that barely moved, not a weak signal. */
  points: number;
  reports: number;
  lastAltFt: number | null;
  maxAltFt: number | null;
}

/** Hard ceiling on a `limit` query parameter, so a caller cannot ask for the
 *  whole 4000-aircraft window as JSON. */
export const ADSB_RECENT_MAX = 100;

/**
 * The aircraft this receiver heard most recently, newest first.
 *
 * First-heard comes from `tr.t[0]` rather than a cached `firstMs`, which would
 * go stale twice over: the point cap shifts the oldest point off the front, and
 * the window prune does the same. The array is the truth.
 */
export function nodeAdsbRecentAircraft(
  nodeId: string,
  limit: number,
  nowMs: number = Date.now(),
): NodeRecentAircraft[] {
  pruneTraces(nodeId, nowMs);
  const byHex = traces.get(nodeId);
  if (!byHex) return [];

  const n = Number.isFinite(limit)
    ? Math.min(ADSB_RECENT_MAX, Math.max(1, Math.round(limit)))
    : ADSB_RECENT_MAX;

  const out: NodeRecentAircraft[] = [];
  for (const [hex, tr] of byHex) {
    out.push({
      hex,
      callsign: tr.callsign,
      firstMs: tr.t[0] ?? tr.lastMs,
      lastMs: tr.lastMs,
      points: tr.t.length,
      reports: tr.reports,
      lastAltFt: tr.lastAltFt,
      maxAltFt: tr.maxAltFt,
    });
  }
  out.sort((a, b) => b.lastMs - a.lastMs);
  return out.slice(0, n);
}

/** One machine retrying the same rejected token inside this window bumps a
 *  count rather than filling the ring with identical rows. */
const AUTH_COALESCE_MS = 60_000;

// --- Authentication failures (fleet-wide, staff only) ----------------------
//
// These cannot be attributed to a node: they happen BEFORE the token resolves,
// and the only identifier on the request is the token that just failed. Looking
// one up by prefix to name a node would turn this log into an oracle for
// guessing tokens, so it is kept fleet-wide and keyed on the install id — the
// agent's own machine identifier, which is not a secret.
//
// An owner whose token has died sees their node go offline, which is the
// actionable signal; this ring is for staff working out WHY.

export interface AdsbAuthFailure {
  /** Enough of the install id to tell two machines apart, never the token. */
  install: string;
  reason: string;
  firstMs: number;
  lastMs: number;
  count: number;
}

const AUTH_FAIL_RING = 25;
const authFailures: AdsbAuthFailure[] = [];

export function recordAdsbAuthFailure(
  installId: string | null | undefined,
  reason: string,
  nowMs: number = Date.now(),
): void {
  const install = (installId ?? '').trim().slice(0, 8) || 'unknown';
  const last = authFailures[authFailures.length - 1];
  if (
    last && last.install === install && last.reason === reason &&
    nowMs - last.lastMs < AUTH_COALESCE_MS
  ) {
    last.lastMs = nowMs;
    last.count += 1;
    return;
  }
  authFailures.push({ install, reason, firstMs: nowMs, lastMs: nowMs, count: 1 });
  while (authFailures.length > AUTH_FAIL_RING) authFailures.shift();
}

/** The fleet-wide authentication failure log, newest first. */
export function adsbAuthFailures(): AdsbAuthFailure[] {
  return authFailures.slice().reverse();
}

// ---------------------------------------------------------------------------

/**
 * Forget everything held for one node.
 *
 * Called when a node is deleted, beside `hub.clearNode`. Without it a deleted
 * receiver keeps a live snapshot on the map for up to two minutes and traces
 * for up to eight hours — and if the id is ever reissued, the new node inherits
 * the old one's coverage picture.
 */
export function clearAdsbNodeState(nodeId: string): void {
  snapshots.delete(nodeId);
  observedRangeKm.delete(nodeId);
  traces.delete(nodeId);
  clearAdsbCoverage(nodeId);
}

/** How many nodes currently have a live snapshot (for the upstreams summary). */
export function nodeAdsbFeedCount(): number {
  return snapshots.size;
}

/** Test seam — drops all in-memory state. */
export function _resetAdsbNodeStore(): void {
  snapshots.clear();
  observedRangeKm.clear();
  pending.clear();
  traces.clear();
  authFailures.length = 0;
}

// ---------------------------------------------------------------------------
// Daily aggregates
// ---------------------------------------------------------------------------

interface PendingDay {
  snapshots: number;
  positions: number;
  maxAircraft: number;
  maxRangeKm: number | null;
  msgRateMax: number | null;
  tracksMax: number | null;
}

/** Keyed `${nodeId}|${day}` so a flush spanning local midnight writes both
 *  days correctly rather than attributing the lot to whichever day wins. */
const pending = new Map<string, PendingDay>();

/**
 * The same figures by the hour, for the charts.
 *
 * A separate accumulator rather than a finer `pending`, because the two are
 * bucketed differently: the daily rows are Sydney-local calendar days (that is
 * what an operator means by "yesterday"), while an hour is an hour anywhere and
 * is kept in UTC so a chart does not gain or lose an hour twice a year.
 */
interface PendingHour {
  snapshots: number;
  positions: number;
  maxAircraft: number;
  maxRangeKm: number | null;
  msgRateMax: number | null;
  /** Running sum and count, so the mean can be weighted on read. Averaging a
   *  stream of averages without its weight quietly favours quiet minutes. */
  signalSum: number;
  signalN: number;
  signalPeak: number | null;
}

/** Keyed `${nodeId}|${epochMsOfHour}`. */
const pendingHour = new Map<string, PendingHour>();

function hourStartMs(ms: number): number {
  return Math.floor(ms / 3_600_000) * 3_600_000;
}

const FLUSH_INTERVAL_MS = 60_000;
let flushTimer: NodeJS.Timeout | null = null;

function maxOrNull(a: number | null, b: number | null | undefined): number | null {
  if (b === null || b === undefined || !Number.isFinite(b)) return a;
  return a === null ? b : Math.max(a, b);
}

/** Sydney-local `YYYY-MM-DD` — the day boundary the rest of the site uses. */
function sydneyDay(ms: number): string {
  return formatSydneyNaive(ms).slice(0, 10);
}

/**
 * Fold one accepted upload into the day's aggregate. Called for EVERY accepted
 * upload, including when the node's feed is disabled: reception is the node
 * owner's own performance record, and hiding it when the feed is off would
 * make a healthy-but-paused receiver look dead on the Data tab.
 */
export function accumulateAdsbDaily(
  nodeId: string,
  positions: number,
  stats?: NodeAdsbStats | null,
  observedRange: number | null = null,
): void {
  const key = `${nodeId}|${sydneyDay(Date.now())}`;
  const cur =
    pending.get(key) ??
    { snapshots: 0, positions: 0, maxAircraft: 0, maxRangeKm: null, msgRateMax: null, tracksMax: null };
  cur.snapshots += 1;
  cur.positions += positions;
  cur.maxAircraft = Math.max(cur.maxAircraft, positions);
  // OUR measurement only, deliberately, even though the decoder reports one.
  //
  // The decoder's figure is its max_distance over the `total` window, which
  // means since the decoder process started — not since midnight. Folding it
  // into a per-day row stamped a distance reached last Tuesday onto every day
  // that followed, so "best range today" quietly became "best range ever", and
  // it disagreed with the coverage plot beside it for the same reason.
  //
  // What is lost is the sampling gap: the decoder sees every position it
  // decodes, while we see the aircraft present in the snapshots it uploads, so
  // a distant contact that came and went between two uploads counts for the
  // decoder and not for us. That is a smaller error than attributing a
  // measurement to the wrong day, and it has the merit of being a figure we
  // can actually stand behind — the decoder's lifetime best is still reported
  // live, where "since it started" is what the number means.
  cur.maxRangeKm = maxOrNull(cur.maxRangeKm, observedRange);
  cur.msgRateMax = maxOrNull(cur.msgRateMax, stats?.msgRate);
  cur.tracksMax = maxOrNull(cur.tracksMax, stats?.tracksAll);
  pending.set(key, cur);

  const hKey = `${nodeId}|${hourStartMs(Date.now())}`;
  const h =
    pendingHour.get(hKey) ??
    { snapshots: 0, positions: 0, maxAircraft: 0, maxRangeKm: null,
      msgRateMax: null, signalSum: 0, signalN: 0, signalPeak: null };
  h.snapshots += 1;
  h.positions += positions;
  h.maxAircraft = Math.max(h.maxAircraft, positions);
  // Same reasoning as the daily row above, and more sharply: an hour is a much
  // smaller bucket for a run-total to contaminate.
  h.maxRangeKm = maxOrNull(h.maxRangeKm, observedRange);
  h.msgRateMax = maxOrNull(h.msgRateMax, stats?.msgRate);
  // Signal is dBFS and always negative; 0 is not a plausible reading, so it is
  // treated as absent rather than as a very strong one.
  if (typeof stats?.signalDbfs === 'number' && Number.isFinite(stats.signalDbfs)
      && stats.signalDbfs < 0) {
    h.signalSum += stats.signalDbfs;
    h.signalN += 1;
  }
  if (typeof stats?.signalPeakDbfs === 'number' && Number.isFinite(stats.signalPeakDbfs)
      && stats.signalPeakDbfs < 0) {
    h.signalPeak = h.signalPeak === null
      ? stats.signalPeakDbfs
      : Math.max(h.signalPeak, stats.signalPeakDbfs);
  }
  pendingHour.set(hKey, h);
}

/**
 * Write accumulated counters to Postgres and clear them.
 *
 * Entries are removed from `pending` BEFORE the query runs, so a failed flush
 * loses one minute of counters rather than double-counting them on the next
 * pass. These are performance statistics, not billing — a gap is preferable to
 * an inflation, and the alternative (retry queue) is complexity this does not
 * warrant.
 */
export async function flushAdsbDaily(): Promise<void> {
  await flushAdsbHourly();
  if (pending.size === 0) return;
  const batch = Array.from(pending.entries());
  pending.clear();

  const pool = await getPool();
  if (!pool) return;

  for (const [key, agg] of batch) {
    const sep = key.lastIndexOf('|');
    const nodeId = key.slice(0, sep);
    const day = key.slice(sep + 1);
    try {
      await pool.query(
        `INSERT INTO node_adsb_daily
           (node_id, day, snapshots, positions, max_aircraft, max_range_km, msg_rate_max, tracks_max)
         VALUES ($1, $2::date, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (node_id, day) DO UPDATE SET
           snapshots    = node_adsb_daily.snapshots + EXCLUDED.snapshots,
           positions    = node_adsb_daily.positions + EXCLUDED.positions,
           max_aircraft = GREATEST(node_adsb_daily.max_aircraft, EXCLUDED.max_aircraft),
           max_range_km = GREATEST(COALESCE(node_adsb_daily.max_range_km, 0), COALESCE(EXCLUDED.max_range_km, 0)),
           msg_rate_max = GREATEST(COALESCE(node_adsb_daily.msg_rate_max, 0), COALESCE(EXCLUDED.msg_rate_max, 0)),
           tracks_max   = GREATEST(COALESCE(node_adsb_daily.tracks_max, 0), COALESCE(EXCLUDED.tracks_max, 0))`,
        [
          nodeId, day, agg.snapshots, agg.positions, agg.maxAircraft,
          agg.maxRangeKm, agg.msgRateMax, agg.tracksMax,
        ],
      );
    } catch (err) {
      // A deleted node (FK violation) or a transient DB blip: log once and drop.
      log.debug({ err, nodeId, day }, 'adsb daily flush failed');
    }
  }
}

/** The hourly half of the same flush. Same posture on failure: a lost minute
 *  beats a double count, and these are performance figures, not billing. */
export async function flushAdsbHourly(): Promise<void> {
  if (pendingHour.size === 0) return;
  const batch = Array.from(pendingHour.entries());
  pendingHour.clear();

  const pool = await getPool();
  if (!pool) return;

  for (const [key, agg] of batch) {
    const sep = key.lastIndexOf('|');
    const nodeId = key.slice(0, sep);
    const hourMs = Number(key.slice(sep + 1));
    try {
      await pool.query(
        `INSERT INTO node_adsb_hourly
           (node_id, hour, snapshots, positions, max_aircraft, max_range_km,
            msg_rate_max, signal_sum, signal_n, signal_peak)
         VALUES ($1, $2::timestamptz, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (node_id, hour) DO UPDATE SET
           snapshots    = node_adsb_hourly.snapshots + EXCLUDED.snapshots,
           positions    = node_adsb_hourly.positions + EXCLUDED.positions,
           max_aircraft = GREATEST(node_adsb_hourly.max_aircraft, EXCLUDED.max_aircraft),
           max_range_km = GREATEST(COALESCE(node_adsb_hourly.max_range_km, 0),
                                   COALESCE(EXCLUDED.max_range_km, 0)),
           msg_rate_max = GREATEST(COALESCE(node_adsb_hourly.msg_rate_max, 0),
                                   COALESCE(EXCLUDED.msg_rate_max, 0)),
           -- The mean's two halves accumulate together or the average drifts.
           signal_sum   = COALESCE(node_adsb_hourly.signal_sum, 0) + COALESCE(EXCLUDED.signal_sum, 0),
           signal_n     = COALESCE(node_adsb_hourly.signal_n, 0) + COALESCE(EXCLUDED.signal_n, 0),
           -- dBFS is negative, so the STRONGEST signal is the greatest value.
           signal_peak  = GREATEST(node_adsb_hourly.signal_peak, EXCLUDED.signal_peak)`,
        [
          nodeId, new Date(hourMs).toISOString(), agg.snapshots, agg.positions,
          agg.maxAircraft, agg.maxRangeKm, agg.msgRateMax,
          agg.signalN > 0 ? agg.signalSum : null,
          agg.signalN > 0 ? agg.signalN : null,
          agg.signalPeak,
        ],
      );
    } catch (err) {
      log.debug({ err, nodeId, hourMs }, 'adsb hourly flush failed');
    }
  }
}

/** Start the periodic flush. Idempotent. */
export function startAdsbDailyFlush(): void {
  if (flushTimer) return;
  flushTimer = setInterval(() => {
    void flushAdsbDaily().catch(() => {});
    // Same cadence, same reason: both are running aggregates that nothing
    // reads between passes.
    void flushAdsbCoverage().catch(() => {});
  }, FLUSH_INTERVAL_MS);
  flushTimer.unref?.();
}

/** Stop the flush timer and write out whatever is pending. */
export async function stopAdsbDailyFlush(): Promise<void> {
  if (flushTimer) {
    clearInterval(flushTimer);
    flushTimer = null;
  }
  await flushAdsbDaily();
  await flushAdsbCoverage();
}
