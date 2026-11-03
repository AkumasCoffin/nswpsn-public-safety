/**
 * 30-day pruner for the per-event node capture DETAIL tables
 * (node_radio_events / node_pager_events — migration 043).
 *
 * The hourly rollup tables (node_radio_hourly / node_radio_hourly_sys /
 * node_pager_hourly) are kept FOREVER and are deliberately not touched
 * here — bucketing keeps them small.
 *
 * The radio pair is DERIVED from node_radio_events (services/
 * nodeHourlyRollup.ts), so pruning is no longer independent of it: deleting an
 * hour of detail before it has been rolled up destroys that history for good,
 * because the detail table is the only thing it can be derived from. Radio
 * pruning therefore stops at the rollup's high-water mark. See pruneTable.
 *
 * Shape mirrors statsArchiver.ts: setInterval + re-entrancy guard +
 * start/stop exported, wired in src/index.ts. Deletes are batched
 * (5k ids per DELETE, looped until none remain) so a large backlog
 * after downtime can't hold a long row lock / bloat one transaction.
 *
 * RADIO DETAIL IS PARTITIONED BY UTC DAY (migration 135), so its retention
 * is DROP TABLE, not DELETE: one statement a day instead of ~36k single-row
 * deletes an hour, each of which left a dead tuple and a dead entry in every
 * index for autovacuum to find. The same rollup rule bounds it — a day is
 * dropped only once its upper bound is past both the retention line and the
 * rollup's high-water mark. Three more jobs ride the same hourly tick:
 *   - the runway: a partition for each UTC day from yesterday to a week
 *     ahead (ensureNodeEventPartitions, also run at boot before ingest);
 *   - the legacy partition, which is the pre-135 table holding every day
 *     below migration 133's bound: still trimmed by DELETE until its own upper
 *     bound passes the line, then dropped like any other day;
 *   - a daily ANALYZE of the parent, which autovacuum never does for a
 *     partitioned table.
 * Each DROP and ATTACH takes a brief lock on the parent that every insert
 * also needs, so each runs under lock_timeout: queueing behind a long staff
 * read would hold ingest up for as long as the read lasted, and the next
 * hour is soon enough to try again.
 *
 * Until migration 135 has been applied (the boot-time runMigrations() logs a
 * failure and carries on), the radio table is a plain table and the old
 * DELETE path runs against it unchanged.
 */
import { getWriterPool } from '../db/pool.js';
import { partitionsEndingBy } from '../db/partitions.js';
import { log } from '../lib/log.js';
import { rollupHighWater } from './nodeHourlyRollup.js';

const PRUNE_INTERVAL_MS = 60 * 60_000; // hourly
const RETENTION_DAYS = 30;

/** Decode-health samples are kept for a SHORTER window than call detail. They
 *  arrive ~1/minute/site/node regardless of traffic, so they accumulate on a
 *  quiet fleet where call rows don't; and their whole purpose is a recent
 *  trend, not a permanent record. ~1.4k rows/site/day at this cadence. */
const DECODE_SAMPLE_RETENTION_DAYS = 7;

/** Parked call uploads (migration 070) are measured in MINUTES, not days.
 *  A pending row exists only to survive the gap between an upload and the
 *  activity event it belongs to — a few seconds. One still sitting here after
 *  ten minutes has no event coming (the call was never decoded on this node,
 *  or its events were dropped), so it is waste, and this is the one node table
 *  with no natural upper bound on its growth. */
const PENDING_RECORDING_RETENTION_MINUTES = 10;
const BATCH_SIZE = 5000;

const RADIO = 'node_radio_events';
const RADIO_LEGACY = 'node_radio_events_legacy';
/** Days of partitions kept ahead of today. Receptions are accepted up to 48 h
 *  either side of server time (nodeEvents.ts radioReceivedAt), so a week ahead
 *  leaves the hourly job several days of misses before any insert could find
 *  no partition. */
const RUNWAY_DAYS = 7;
/** How long a DROP or ATTACH may wait for the parent's lock. */
const PARTITION_LOCK_TIMEOUT = '3s';
const ANALYZE_EVERY_MS = 24 * 60 * 60_000;
/** Only these names are ever dropped: the legacy table and YYYYMMDD days. */
const RADIO_PARTITION_NAME = /^node_radio_events_(legacy|\d{8})$/;

let timer: NodeJS.Timeout | null = null;
// Guards an overlapping tick when a prune runs longer than the interval.
let tickRunning = false;
let lastAnalyzeAt = 0;

/** Batched delete of rows older than the retention window. Returns rows
 *  removed. Table name comes from the fixed list below — never user input. */
async function pruneTable(
  table:
    | 'node_radio_events'
    | 'node_radio_events_legacy'
    | 'node_pager_events'
    | 'node_site_decode_samples'
    | 'node_pending_recordings',
): Promise<number> {
  const pool = await getWriterPool();
  if (!pool) return 0;
  // Each table brings its own timestamp column and window; pending uploads are
  // the only one measured in minutes.
  const samples = table === 'node_site_decode_samples';
  const pending = table === 'node_pending_recordings';
  const tsCol = pending ? 'created_at' : samples ? 'sampled_at' : 'received_at';
  const age = pending
    ? `${PENDING_RECORDING_RETENTION_MINUTES} minutes`
    : `${samples ? DECODE_SAMPLE_RETENTION_DAYS : RETENTION_DAYS} days`;

  // Radio detail is the ONLY source the hourly rollups can be derived from, so
  // it must never be pruned past what the rollup has already summarised. In
  // normal running the rollup is hours ahead of the 30-day line and this bound
  // never binds; it matters when the rollup has been failing, where without it
  // the pruner would quietly delete history nobody had summarised yet.
  //
  // A null high-water mark means the rollup has never run — nothing may go.
  let ceiling: string | null = null;
  if (table === RADIO || table === RADIO_LEGACY) {
    const water = await rollupHighWater();
    if (water === null) {
      log.warn({ table }, 'node events pruner: rollup has not run, skipping radio prune');
      return 0;
    }
    ceiling = water.toISOString();
  }

  let total = 0;
  for (;;) {
    const r = await pool.query(
      `DELETE FROM ${table} WHERE id IN (
         SELECT id FROM ${table}
          WHERE ${tsCol} < now() - interval '${age}'
            ${ceiling === null ? '' : `AND ${tsCol} < $1::timestamptz`}
          LIMIT ${BATCH_SIZE}
       )`,
      ceiling === null ? undefined : [ceiling],
    );
    const n = r.rowCount ?? 0;
    total += n;
    if (n === 0) break;
  }
  return total;
}

/** Whether migration 135 has run, and whether the legacy partition is still
 *  there. Read every tick: both change underneath a running process (the
 *  deploy migrates before it restarts us; the legacy table is dropped by us). */
async function radioLayout(): Promise<{ partitioned: boolean; legacy: boolean }> {
  const pool = await getWriterPool();
  if (!pool) return { partitioned: false, legacy: false };
  const r = await pool.query<{ partitioned: boolean | null; legacy: boolean }>(
    `SELECT (SELECT relkind = 'p' FROM pg_class WHERE oid = to_regclass($1)) AS partitioned,
            to_regclass($2) IS NOT NULL AS legacy`,
    [RADIO, RADIO_LEGACY],
  );
  const row = r.rows[0];
  return { partitioned: row?.partitioned === true, legacy: row?.legacy === true };
}

/** YYYY-MM-DD of a UTC day `offset` days from now. */
function utcDay(offset: number): string {
  return new Date(Date.now() + offset * 86_400_000).toISOString().slice(0, 10);
}

/**
 * Make sure a radio partition exists for every UTC day from yesterday to
 * RUNWAY_DAYS ahead. Returns how many it created. A no-op before migration
 * 135. Never throws: an insert that finds no partition fails its own event,
 * the agent re-sends it, and the next call here fixes the cause.
 *
 * ensure_node_radio_events_partition (migration 135) skips days that already
 * have a partition and days the legacy partition still covers, and ATTACHes
 * under SHARE UPDATE EXCLUSIVE. One transaction for the whole runway, so a
 * lock timeout leaves nothing half-done.
 */
export async function ensureNodeEventPartitions(): Promise<number> {
  try {
    const pool = await getWriterPool();
    if (!pool) return 0;
    if (!(await radioLayout()).partitioned) return 0;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`SET LOCAL lock_timeout = '${PARTITION_LOCK_TIMEOUT}'`);
      const r = await client.query<{ made: string }>(
        `SELECT count(p)::text AS made
           FROM generate_series($1::date, $2::date, interval '1 day') AS d,
                LATERAL ensure_node_radio_events_partition(d::date) AS p`,
        [utcDay(-1), utcDay(RUNWAY_DAYS)],
      );
      await client.query('COMMIT');
      const made = Number(r.rows[0]?.made ?? 0);
      if (made > 0) log.info({ made }, 'node events: created radio day partitions');
      return made;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'node events: partition runway failed; retrying next hour');
    return 0;
  }
}

let runwayRequested: Promise<number> | null = null;
/** For the ingest path: an insert just found no partition for its day. Runs
 *  the runway now instead of at the next tick, at most once at a time. */
export function requestNodeEventPartitions(): void {
  if (runwayRequested) return;
  runwayRequested = ensureNodeEventPartitions().finally(() => {
    runwayRequested = null;
  });
}

/**
 * Drop radio partitions whose every row is past retention and already rolled
 * up. Returns how many were dropped.
 */
async function dropExpiredRadioPartitions(): Promise<number> {
  const pool = await getWriterPool();
  if (!pool) return 0;
  const water = await rollupHighWater();
  if (water === null) {
    log.warn({ table: RADIO }, 'node events pruner: rollup has not run, skipping radio drop');
    return 0;
  }
  const retention = new Date(Date.now() - RETENTION_DAYS * 86_400_000);
  const cutoff = water < retention ? water : retention;

  const client = await pool.connect();
  let dropped = 0;
  try {
    const expired = await partitionsEndingBy(client, [RADIO], cutoff.toISOString());
    for (const p of expired) {
      if (!RADIO_PARTITION_NAME.test(p.partition)) continue;
      try {
        await client.query('BEGIN');
        await client.query(`SET LOCAL lock_timeout = '${PARTITION_LOCK_TIMEOUT}'`);
        await client.query(`DROP TABLE IF EXISTS ${p.partition}`);
        await client.query('COMMIT');
        dropped += 1;
        log.info(
          { partition: p.partition, range_to: p.range_to },
          'node events pruner: dropped expired radio partition',
        );
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        log.warn(
          { err: (err as Error).message, partition: p.partition },
          'node events pruner: drop failed; retrying next hour',
        );
      }
    }
  } finally {
    client.release();
  }
  return dropped;
}

/** ANALYZE the radio parent, at most daily or right after a drop.
 *  Autovacuum analyses the partitions but never a partitioned parent, and the
 *  parent's statistics are what the planner uses for the grouping lookup. */
async function analyzeRadioParent(force: boolean): Promise<void> {
  if (!force && Date.now() - lastAnalyzeAt < ANALYZE_EVERY_MS) return;
  const pool = await getWriterPool();
  if (!pool) return;
  try {
    await pool.query(`ANALYZE ${RADIO}`);
    lastAnalyzeAt = Date.now();
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'node events pruner: ANALYZE failed');
  }
}

/** One prune pass over every detail table. Never throws. */
export async function pruneNodeEventsOnce(): Promise<void> {
  try {
    let radio = 0;
    let dropped = 0;
    const layout = await radioLayout();
    if (layout.partitioned) {
      await ensureNodeEventPartitions();
      dropped = await dropExpiredRadioPartitions();
      // Trimmed by DELETE until its upper bound passes the line, at which
      // point the drop above has taken it — so look again rather than trust
      // the layout read before the drop.
      if (layout.legacy && (await radioLayout()).legacy) {
        radio = await pruneTable(RADIO_LEGACY);
      }
      await analyzeRadioParent(dropped > 0);
    } else {
      radio = await pruneTable(RADIO);
    }
    const pager = await pruneTable('node_pager_events');
    const decode = await pruneTable('node_site_decode_samples');
    const pending = await pruneTable('node_pending_recordings');
    if (radio > 0 || dropped > 0 || pager > 0 || decode > 0 || pending > 0) {
      log.info(
        { radio, radio_partitions_dropped: dropped, pager, decode, pending },
        'node events pruner: removed expired detail rows',
      );
    }
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'node events pruner: prune failed');
  }
}

/** Start the hourly pruner. Idempotent. */
export function startNodeEventsPruner(intervalMs: number = PRUNE_INTERVAL_MS): void {
  if (timer) return;
  // First pass shortly after boot so a backlog from downtime clears
  // without waiting a full hour.
  setTimeout(() => void pruneNodeEventsOnce(), 60_000).unref?.();
  timer = setInterval(async () => {
    if (tickRunning) return;
    tickRunning = true;
    try {
      await pruneNodeEventsOnce();
    } finally {
      tickRunning = false;
    }
  }, intervalMs);
  timer.unref?.();
}

export function stopNodeEventsPruner(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
    tickRunning = false;
  }
}
