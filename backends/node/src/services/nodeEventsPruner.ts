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
 */
import { getWriterPool } from '../db/pool.js';
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

let timer: NodeJS.Timeout | null = null;
// Guards an overlapping tick when a prune runs longer than the interval.
let tickRunning = false;

/** Batched delete of rows older than the retention window. Returns rows
 *  removed. Table name comes from the fixed list below — never user input. */
async function pruneTable(
  table:
    | 'node_radio_events'
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
  if (table === 'node_radio_events') {
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

/** One prune pass over both detail tables. Never throws. */
export async function pruneNodeEventsOnce(): Promise<void> {
  try {
    const radio = await pruneTable('node_radio_events');
    const pager = await pruneTable('node_pager_events');
    const decode = await pruneTable('node_site_decode_samples');
    const pending = await pruneTable('node_pending_recordings');
    if (radio > 0 || pager > 0 || decode > 0 || pending > 0) {
      log.info(
        { radio, pager, decode, pending },
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
