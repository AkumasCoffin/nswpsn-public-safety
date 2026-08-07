/**
 * 30-day pruner for the per-event node capture DETAIL tables
 * (node_radio_events / node_pager_events — migration 043).
 *
 * The hourly rollup tables (node_radio_hourly / node_radio_hourly_sys /
 * node_pager_hourly) are kept FOREVER and are deliberately not touched
 * here — bucketing keeps them small.
 *
 * Shape mirrors statsArchiver.ts: setInterval + re-entrancy guard +
 * start/stop exported, wired in src/index.ts. Deletes are batched
 * (5k ids per DELETE, looped until none remain) so a large backlog
 * after downtime can't hold a long row lock / bloat one transaction.
 */
import { getWriterPool } from '../db/pool.js';
import { log } from '../lib/log.js';

const PRUNE_INTERVAL_MS = 60 * 60_000; // hourly
const RETENTION_DAYS = 30;
const BATCH_SIZE = 5000;

let timer: NodeJS.Timeout | null = null;
// Guards an overlapping tick when a prune runs longer than the interval.
let tickRunning = false;

/** Batched delete of rows older than the retention window. Returns rows
 *  removed. Table name comes from the fixed list below — never user input. */
async function pruneTable(table: 'node_radio_events' | 'node_pager_events'): Promise<number> {
  const pool = await getWriterPool();
  if (!pool) return 0;
  let total = 0;
  for (;;) {
    const r = await pool.query(
      `DELETE FROM ${table} WHERE id IN (
         SELECT id FROM ${table}
          WHERE received_at < now() - interval '${RETENTION_DAYS} days'
          LIMIT ${BATCH_SIZE}
       )`,
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
    if (radio > 0 || pager > 0) {
      log.info({ radio, pager }, 'node events pruner: removed expired detail rows');
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
