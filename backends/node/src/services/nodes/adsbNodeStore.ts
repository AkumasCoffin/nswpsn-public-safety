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
  snapshots.set(nodeId, {
    name: nodeName ?? '',
    receivedAtMs: Date.now(),
    records,
  });
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

/** How many nodes currently have a live snapshot (for the upstreams summary). */
export function nodeAdsbFeedCount(): number {
  return snapshots.size;
}

/** Test seam — drops all in-memory state. */
export function _resetAdsbNodeStore(): void {
  snapshots.clear();
  pending.clear();
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
