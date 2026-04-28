/**
 * Periodic data cleanup.
 *
 * The new partitioned schema means cleanup is partition-level, not
 * row-level: drop monthly partitions whose entire range is older than
 * the retention window. Way faster than DELETE — partition drop is
 * O(1) regardless of row count.
 *
 * Schedule: runs once an hour (DATA_CLEANUP_INTERVAL_SECS), default
 * 31-day retention. Status surface mirrors python's `cleanup` panel
 * shape so the dashboard renders without missing fields.
 *
 * Stats tracked:
 *   - last_run_age_secs       last successful cleanup tick
 *   - last_history_deleted    rows removed in the most recent run
 *   - last_pager_ended        unused (legacy python field)
 *   - last_waze_ended         unused (legacy python field)
 *   - last_stats_deleted      stats_snapshots rows pruned
 *   - total_history_deleted   running total since process start
 *   - last_vacuum_age_secs    last successful VACUUM
 *   - last_vacuum_ms          duration of that vacuum
 *
 * VACUUM ANALYZE runs after each cleanup so the planner sees the
 * updated row counts.
 */
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { pruneOldSnapshots } from './statsArchiver.js';
import { sweepStaleAsEnded } from './archiveLiveness.js';

const DEFAULT_INTERVAL_SECS = Number.parseInt(
  process.env['DATA_CLEANUP_INTERVAL_SECS'] ?? '3600',
  10,
);
const DEFAULT_RETENTION_DAYS = Number.parseInt(
  process.env['DATA_RETENTION_DAYS'] ?? '31',
  10,
);

const ARCHIVE_TABLES = [
  'archive_waze',
  'archive_traffic',
  'archive_rfs',
  'archive_power',
  'archive_misc',
];

interface CleanupStats {
  lastRunAt: number;
  lastHistoryDeleted: number;
  lastStatsDeleted: number;
  lastStaleSwept: number;
  totalHistoryDeleted: number;
  lastVacuumAt: number;
  lastVacuumMs: number;
}

const stats: CleanupStats = {
  lastRunAt: 0,
  lastHistoryDeleted: 0,
  lastStatsDeleted: 0,
  lastStaleSwept: 0,
  totalHistoryDeleted: 0,
  lastVacuumAt: 0,
  lastVacuumMs: 0,
};

let timer: NodeJS.Timeout | null = null;
let running = false;

interface PartitionRow {
  parent: string;
  partition: string;
  range_to: string;
  row_count: string;
}

/**
 * Find every monthly partition whose upper bound (range_to) is at or
 * before the cutoff. Postgres stores partition bounds in
 * pg_class.relpartbound; we parse the FROM/TO via pg_get_expr.
 */
async function findExpiredPartitions(
  client: { query: (sql: string, params?: unknown[]) => Promise<{ rows: PartitionRow[] }> },
  cutoffIso: string,
): Promise<PartitionRow[]> {
  const sql = `
    WITH parts AS (
      SELECT
        parent.relname::text AS parent,
        child.relname::text AS partition,
        pg_get_expr(child.relpartbound, child.oid) AS bound
      FROM pg_inherits
      JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
      JOIN pg_class child  ON child.oid  = pg_inherits.inhrelid
      WHERE parent.relname = ANY($1::text[])
    )
    SELECT
      parent,
      partition,
      -- bound is "FOR VALUES FROM ('YYYY-MM-DD ...') TO ('YYYY-MM-DD ...')".
      -- Pull the second timestamp.
      regexp_replace(bound, $2, '\\1') AS range_to,
      0::text AS row_count
    FROM parts
    WHERE bound LIKE 'FOR VALUES FROM%'
      AND regexp_replace(bound, $2, '\\1')::timestamptz <= $3::timestamptz
  `;
  const re =
    "FOR VALUES FROM \\(.+\\) TO \\('([^']+)'\\)";
  const r = await client.query(sql, [ARCHIVE_TABLES, re, cutoffIso]);
  return r.rows;
}

/**
 * One cleanup pass. Idempotent — running twice in a row finds nothing
 * to delete the second time.
 */
export async function runCleanupOnce(retentionDays: number = DEFAULT_RETENTION_DAYS): Promise<{
  partitions_dropped: number;
  rows_deleted: number;
  stats_pruned: number;
  stale_swept: number;
  vacuum_ms: number;
}> {
  if (running) {
    return { partitions_dropped: 0, rows_deleted: 0, stats_pruned: 0, stale_swept: 0, vacuum_ms: 0 };
  }
  running = true;
  const startMs = Date.now();
  let partitionsDropped = 0;
  let rowsDeleted = 0;
  let statsPruned = 0;
  let vacuumMs = 0;
  try {
    const pool = await getPool();
    if (!pool) return { partitions_dropped: 0, rows_deleted: 0, stats_pruned: 0, stale_swept: 0, vacuum_ms: 0 };
    const cutoff = new Date(Date.now() - retentionDays * 86_400_000).toISOString();
    const client = await pool.connect();
    try {
      // Long timeout — partition drop is fast but the surrounding
      // SELECT against pg_class can take a moment on busy DBs.
      await client.query('SET statement_timeout = 0');

      // 1. Drop expired archive_* partitions.
      const expired = await findExpiredPartitions(client, cutoff);
      for (const p of expired) {
        try {
          // Count rows BEFORE drop so we can report deletion totals.
          const cnt = await client.query<{ n: string }>(
            `SELECT COUNT(*)::text AS n FROM ${p.partition}`,
          );
          const n = Number(cnt.rows[0]?.n ?? 0);
          await client.query(`DROP TABLE IF EXISTS ${p.partition}`);
          partitionsDropped += 1;
          rowsDeleted += n;
          log.info(
            { parent: p.parent, partition: p.partition, rows: n, range_to: p.range_to },
            'cleanup: dropped expired partition',
          );
        } catch (err) {
          log.warn(
            { err: (err as Error).message, partition: p.partition },
            'cleanup: drop failed',
          );
        }
      }

      // 2. ANALYZE the archive tables — refreshes pg_statistic so the
      // planner stays current after partition drops above. We do NOT
      // VACUUM here: postgres' autovacuum already handles dead-tuple
      // cleanup automatically, and running a second VACUUM in parallel
      // saturates disk I/O. Production saw archive_rfs queries (tiny
      // table) time out at 60s when our hourly VACUUM competed with
      // autovacuum on archive_waze. Manual one-shot VACUUM is still
      // available via /api/admin/db/vacuum when needed.
      const vacStart = Date.now();
      for (const t of ARCHIVE_TABLES) {
        try {
          await client.query(`ANALYZE ${t}`);
        } catch (err) {
          log.warn(
            { err: (err as Error).message, table: t },
            'cleanup: ANALYZE failed',
          );
        }
      }
      vacuumMs = Date.now() - vacStart;
    } finally {
      client.release();
    }

    // 3. Prune stats_snapshots (delegated to the existing helper which
    // already knows the retention policy for that table).
    statsPruned = await pruneOldSnapshots();

    // 4. Tombstone any source_id whose latest live archive row is
    // >1h old. Catches the case where a poller has been failing for
    // an hour+ (so the per-poll diff hasn't run) and disappeared
    // incidents would otherwise stay live until partition drop.
    let staleSwept = 0;
    try {
      const pool = await getPool();
      if (pool) staleSwept = await sweepStaleAsEnded(pool);
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'cleanup: stale-as-ended sweep failed (non-fatal)',
      );
    }

    stats.lastRunAt = Math.floor(Date.now() / 1000);
    stats.lastHistoryDeleted = rowsDeleted;
    stats.lastStatsDeleted = statsPruned;
    stats.lastStaleSwept = staleSwept;
    stats.totalHistoryDeleted += rowsDeleted;
    if (vacuumMs > 0) {
      stats.lastVacuumAt = Math.floor(Date.now() / 1000);
      stats.lastVacuumMs = vacuumMs;
    }

    log.info(
      {
        partitions_dropped: partitionsDropped,
        rows_deleted: rowsDeleted,
        stats_pruned: statsPruned,
        stale_swept: staleSwept,
        ms: Date.now() - startMs,
      },
      'cleanup: done',
    );
    return {
      partitions_dropped: partitionsDropped,
      rows_deleted: rowsDeleted,
      stats_pruned: statsPruned,
      stale_swept: staleSwept,
      vacuum_ms: vacuumMs,
    };
  } catch (err) {
    log.error({ err: (err as Error).message }, 'cleanup: failed');
    return { partitions_dropped: 0, rows_deleted: 0, stats_pruned: 0, stale_swept: 0, vacuum_ms: 0 };
  } finally {
    running = false;
  }
}

/** Stats snapshot for /api/status `cleanup` block. */
export function cleanupStatsForStatus(): {
  last_run_age_secs: number | null;
  last_history_deleted: number;
  last_stats_deleted: number;
  last_pager_ended: number;
  last_waze_ended: number;
  last_stale_swept: number;
  total_history_deleted: number;
  last_vacuum_age_secs: number | null;
  last_vacuum_ms: number;
  retention_days: number;
  cleanup_interval_secs: number;
} {
  const now = Math.floor(Date.now() / 1000);
  return {
    last_run_age_secs: stats.lastRunAt ? now - stats.lastRunAt : null,
    last_history_deleted: stats.lastHistoryDeleted,
    last_stats_deleted: stats.lastStatsDeleted,
    // Legacy fields python tracked separately. The new schema doesn't
    // distinguish pager-ended vs waze-ended (everything's a partition
    // drop) so report 0 — keeps the response shape stable.
    last_pager_ended: 0,
    last_waze_ended: 0,
    // Number of "stale-as-ended" tombstones inserted in the most
    // recent cleanup pass — counts source_ids that the per-poll diff
    // missed because the poller was failing.
    last_stale_swept: stats.lastStaleSwept,
    total_history_deleted: stats.totalHistoryDeleted,
    last_vacuum_age_secs: stats.lastVacuumAt ? now - stats.lastVacuumAt : null,
    last_vacuum_ms: stats.lastVacuumMs,
    retention_days: DEFAULT_RETENTION_DAYS,
    cleanup_interval_secs: DEFAULT_INTERVAL_SECS,
  };
}

/** Start the periodic cleanup loop. Idempotent. */
export function startCleanupLoop(intervalSecs: number = DEFAULT_INTERVAL_SECS): void {
  if (timer) return;
  // First run a few minutes after boot so it doesn't compete with
  // the indexBuilder + initial poll cycles for DB connections.
  setTimeout(() => void runCleanupOnce(), 5 * 60_000).unref?.();
  timer = setInterval(() => void runCleanupOnce(), intervalSecs * 1000);
  timer.unref?.();
}

export function stopCleanupLoop(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}
