/**
 * Periodic background maintenance for the is_latest column.
 *
 * The archive_* tables now have an is_latest BOOLEAN column (added
 * in migration 012). Idea: writes set new rows is_latest=true; reads
 * filter `WHERE is_latest = true` to short-circuit the per-source_id
 * dedup. Sub-millisecond on the partial index regardless of table size.
 *
 * Earlier revision had the writer flip prior is_latest=true rows to
 * false inside the same transaction as the INSERT. That worked but
 * doubled write amplification — every essential_future poll (1025
 * outages) did 1025 INSERTs + 1025 UPDATEs, hitting the 60s
 * statement_timeout under bloat pressure.
 *
 * This refresher decouples them. Writes stay append-only and fast.
 * Every 5 min this loop flips superseded is_latest=true rows to
 * is_latest=false for each archive table. Reads see eventually-
 * consistent is_latest with at most ~5 min lag — they layer DISTINCT
 * ON over the partial index to dedupe the lag window.
 */
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';

const REFRESH_INTERVAL_MS = 5 * 60_000; // 5 min
const ARCHIVE_TABLES = [
  'archive_waze',
  'archive_traffic',
  'archive_rfs',
  'archive_power',
  'archive_misc',
];

let timer: NodeJS.Timeout | null = null;
let running = false;
let lastRunAt = 0;
let lastDuration = 0;
let lastFlippedTotal = 0;

export interface IsLatestRefresherStats {
  last_run_age_secs: number | null;
  last_duration_ms: number;
  last_flipped: number;
  refresh_interval_secs: number;
}

export function isLatestRefresherStats(): IsLatestRefresherStats {
  return {
    last_run_age_secs: lastRunAt
      ? Math.floor(Date.now() / 1000) - lastRunAt
      : null,
    last_duration_ms: lastDuration,
    last_flipped: lastFlippedTotal,
    refresh_interval_secs: Math.floor(REFRESH_INTERVAL_MS / 1000),
  };
}

/**
 * Single refresh pass. For each archive table, find rows that are
 * marked is_latest=true but have a newer sibling (same source_id,
 * higher id), and flip them to is_latest=false.
 *
 * Idempotent — running back-to-back finds zero candidates the second
 * time. Bounded by table size (single UPDATE per table).
 */
export async function refreshIsLatestOnce(): Promise<{ flipped: number; ms: number }> {
  if (running) return { flipped: 0, ms: 0 };
  running = true;
  const startedAt = Date.now();
  let totalFlipped = 0;
  try {
    const pool = await getPool();
    if (!pool) return { flipped: 0, ms: 0 };
    const client = await pool.connect();
    try {
      // Unlimit timeout — this UPDATE can be heavy when the writer
      // has been writing fast and many rows need flipping. It only
      // runs every 5 min so taking a couple of minutes once isn't
      // user-visible.
      await client.query('SET statement_timeout = 0');
      for (const table of ARCHIVE_TABLES) {
        try {
          const t0 = Date.now();
          // Find rows where there's a newer row with the same
          // (source, source_id), then flip the older ones to
          // is_latest=false. Uses the partial index
          // `idx_<table>_src_sid_latest WHERE is_latest = true`
          // for fast lookup.
          const r = await client.query(
            `WITH stale AS (
               SELECT a.id
               FROM ${table} a
               JOIN ${table} b
                 ON a.source = b.source
                AND a.source_id = b.source_id
                AND b.id > a.id
                AND b.is_latest = true
               WHERE a.is_latest = true
                 AND a.source_id IS NOT NULL
             )
             UPDATE ${table}
                SET is_latest = false
              WHERE id IN (SELECT id FROM stale)`,
          );
          const flipped = r.rowCount ?? 0;
          totalFlipped += flipped;
          if (flipped > 0) {
            log.info(
              { table, flipped, ms: Date.now() - t0 },
              'isLatestRefresher: flipped',
            );
          }
        } catch (err) {
          log.warn(
            { err: (err as Error).message, table },
            'isLatestRefresher: failed for table',
          );
        }
      }
    } finally {
      client.release();
    }
    lastRunAt = Math.floor(Date.now() / 1000);
    lastDuration = Date.now() - startedAt;
    lastFlippedTotal = totalFlipped;
    return { flipped: totalFlipped, ms: lastDuration };
  } finally {
    running = false;
  }
}

/** Start the periodic refresh loop. Idempotent. */
export function startIsLatestRefresher(intervalMs: number = REFRESH_INTERVAL_MS): void {
  if (timer) return;
  // First run 60s after boot — give the indexBuilder its window to
  // create the partial indexes the refresher relies on.
  setTimeout(() => void refreshIsLatestOnce(), 60_000).unref?.();
  timer = setInterval(() => void refreshIsLatestOnce(), intervalMs);
  timer.unref?.();
}

export function stopIsLatestRefresher(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}
