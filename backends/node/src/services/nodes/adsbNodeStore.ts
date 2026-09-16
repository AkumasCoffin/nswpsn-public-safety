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
  const nowMs = Date.now();
  snapshots.set(nodeId, {
    name: nodeName ?? '',
    receivedAtMs: nowMs,
    records,
  });
  recordTraces(nodeId, records, nowMs);
  return records.length;
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
 * Minimum gap between stored points for one aircraft.
 *
 * Coarser than it was, because the window is now eight times longer and this
 * is the one structure here that grows with traffic. A coverage picture is
 * about WHERE the receiver hears, not the shape of any single flight, so a
 * point a minute is ample — and a fast mover still gets extra points from the
 * distance rule below, which is what stops a jet being drawn cutting corners
 * it never flew.
 */
const TRACE_MIN_GAP_MS = 60_000;

/** Minimum movement to store a point sooner than the gap, in degrees —
 *  ~1km, which is a visible step at coverage-map zoom. */
const TRACE_MIN_MOVE_DEG = 0.01;

/**
 * Per-aircraft point cap. Eight hours at the minimum gap is 480, but no real
 * aircraft stays in one receiver's range for eight hours — an airliner crosses
 * in twenty minutes or so. This is the bound for something pathological (a
 * stuck position, a ground vehicle parked in view), not for normal traffic.
 */
const TRACE_MAX_POINTS = 240;

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
      });
      continue;
    }
    // Keep the callsign once it is known: an aircraft is usually tracked for a
    // while before it transmits one, so the first point rarely has it.
    if (!tr.callsign && r.callsign) tr.callsign = r.callsign;

    const n = tr.lat.length;
    const dt = nowMs - tr.t[n - 1]!;
    const moved =
      Math.abs(r.lat - tr.lat[n - 1]!) > TRACE_MIN_MOVE_DEG ||
      Math.abs(r.lon - tr.lon[n - 1]!) > TRACE_MIN_MOVE_DEG;
    if (dt < TRACE_MIN_GAP_MS && !moved) {
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

/**
 * One receiver's traces over the last `minutes`.
 *
 * Single-point traces are omitted: an aircraft caught once is a dot, not a
 * path, and hundreds of them turn the coverage picture into noise. They are
 * still counted in `aircraft` so the total stays honest.
 */
export function nodeAdsbTraces(
  nodeId: string,
  minutes: number,
  nowMs: number = Date.now(),
): { traces: NodeTrace[]; aircraft: number; points: number; windowMinutes: number } {
  pruneTraces(nodeId, nowMs);
  const byHex = traces.get(nodeId);
  if (!byHex) return { traces: [], aircraft: 0, points: 0, windowMinutes: minutes };

  const cutoff = nowMs - Math.max(1, minutes) * 60_000;
  const out: NodeTrace[] = [];
  let points = 0;
  let aircraft = 0;

  for (const [hex, tr] of byHex) {
    const pts: Array<[number, number]> = [];
    for (let i = 0; i < tr.t.length; i += 1) {
      if (tr.t[i]! < cutoff) continue;
      pts.push([tr.lat[i]!, tr.lon[i]!]);
    }
    if (pts.length === 0) continue;
    aircraft += 1;
    points += pts.length;
    if (pts.length < 2) continue;
    out.push({ hex, callsign: tr.callsign, points: pts });
  }
  return { traces: out, aircraft, points, windowMinutes: minutes };
}

/** How many nodes currently have a live snapshot (for the upstreams summary). */
export function nodeAdsbFeedCount(): number {
  return snapshots.size;
}

/** Test seam — drops all in-memory state. */
export function _resetAdsbNodeStore(): void {
  snapshots.clear();
  pending.clear();
  traces.clear();
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
): void {
  const key = `${nodeId}|${sydneyDay(Date.now())}`;
  const cur =
    pending.get(key) ??
    { snapshots: 0, positions: 0, maxAircraft: 0, maxRangeKm: null, msgRateMax: null, tracksMax: null };
  cur.snapshots += 1;
  cur.positions += positions;
  cur.maxAircraft = Math.max(cur.maxAircraft, positions);
  cur.maxRangeKm = maxOrNull(cur.maxRangeKm, stats?.maxRangeKm);
  cur.msgRateMax = maxOrNull(cur.msgRateMax, stats?.msgRate);
  cur.tracksMax = maxOrNull(cur.tracksMax, stats?.tracksAll);
  pending.set(key, cur);
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

/** Start the periodic flush. Idempotent. */
export function startAdsbDailyFlush(): void {
  if (flushTimer) return;
  flushTimer = setInterval(() => {
    void flushAdsbDaily().catch(() => {});
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
}
