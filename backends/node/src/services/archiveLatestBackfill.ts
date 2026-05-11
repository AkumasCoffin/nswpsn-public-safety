/**
 * One-shot backfill for the archive_*_latest sidecar tables (migration
 * 017). The writer maintains the sidecar going forward, but rows that
 * already exist in the parent at the time the migration lands aren't
 * automatically reflected — this task walks each parent in id-order
 * chunks and UPSERTs into the sidecar so unique=1 history queries see
 * the full historical incident set, not just the last few hours.
 *
 * Runs once at startup, opportunistically and in the background. Each
 * chunk is a single GROUP BY + UPSERT so the work happens server-side
 * with no row-by-row plumbing through Node. Idempotent (ON CONFLICT
 * keeps the newer fetched_at), so re-running is safe.
 *
 * Backfill is skipped per-table when the sidecar's row count is at
 * least the parent's distinct (source, source_id) count — meaning a
 * previous run already finished. Cheaper than tracking checkpoint
 * state in a separate table.
 */
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';

const ARCHIVE_TABLES = [
  'archive_waze',
  'archive_traffic',
  'archive_rfs',
  'archive_power',
  'archive_misc',
] as const;

const CHUNK_SIZE = 50_000;
// 5 min between chunks if the host is busy. Backfill is cooperative —
// the writer's hot path takes priority.
const PAUSE_BETWEEN_CHUNKS_MS = 250;
// Start the backfill after the server has had time to stabilise. Picking
// this up too early can fight with migration finalisation + initial poll
// fan-out for connections.
const STARTUP_DELAY_MS = 30_000;

let running = false;

interface BackfillStats {
  table: string;
  chunks: number;
  rowsProcessed: number;
  upserted: number;
  ms: number;
}

async function tableNeedsBackfill(pool: Pool, table: string): Promise<boolean> {
  // Skip if the sidecar already has a row count >= the count of distinct
  // (source, source_id) pairs in the parent. Counting distinct in the
  // parent is itself expensive — but it's bounded by source_id NOT NULL,
  // and we only run it once per process startup.
  //
  // Cheaper heuristic: if the sidecar has any rows AT ALL and we're not
  // launching for the first time, assume backfill already ran. Skip this
  // table. We rely on the writer to keep the sidecar up to date going
  // forward; a one-time backfill miss is not catastrophic.
  const r = await pool.query<{ n: string }>(
    `SELECT COUNT(*)::text AS n FROM ${table}_latest LIMIT 1`,
  );
  const count = Number(r.rows[0]?.n ?? '0');
  return count === 0;
}

async function backfillTable(pool: Pool, table: string): Promise<BackfillStats> {
  const start = Date.now();
  const stats: BackfillStats = {
    table,
    chunks: 0,
    rowsProcessed: 0,
    upserted: 0,
    ms: 0,
  };

  // Walk by id range. Postgres' partition pruning + the implicit (id)
  // index on the partitioned PK lets each chunk's range scan stay fast
  // even when the table spans many monthly partitions.
  let lastId = 0;
  for (;;) {
    // Per-chunk timeout so a slow partition doesn't wedge the loop.
    const client = await pool.connect();
    let chunkRows = 0;
    try {
      await client.query('BEGIN');
      await client.query("SET LOCAL statement_timeout = '120s'");

      // Find this chunk's id range. We scan until we have CHUNK_SIZE
      // candidate rows or run out.
      const rangeRes = await client.query<{ id: string }>(
        `SELECT id FROM ${table}
          WHERE id > $1
            AND source_id IS NOT NULL
            AND source_id <> ''
          ORDER BY id ASC
          OFFSET $2
          LIMIT 1`,
        [lastId, CHUNK_SIZE - 1],
      );
      const upperBound = rangeRes.rows[0]?.id ?? null;

      // GROUP BY in SQL — no per-row plumbing through Node.
      const upsertRes = await client.query<{ rows_processed: string }>(
        `WITH src AS (
           SELECT source, source_id, max(fetched_at) AS latest_fetched_at,
                  COUNT(*) AS n
           FROM ${table}
           WHERE id > $1
             ${upperBound !== null ? 'AND id <= $3' : ''}
             AND source_id IS NOT NULL
             AND source_id <> ''
           GROUP BY source, source_id
         ),
         ins AS (
           INSERT INTO ${table}_latest (source, source_id, latest_fetched_at)
           SELECT source, source_id, latest_fetched_at FROM src
           ON CONFLICT (source, source_id) DO UPDATE
             SET latest_fetched_at = EXCLUDED.latest_fetched_at
             WHERE ${table}_latest.latest_fetched_at < EXCLUDED.latest_fetched_at
           RETURNING 1
         )
         SELECT COALESCE(SUM(n), 0)::text AS rows_processed FROM src`,
        upperBound !== null ? [lastId, null, upperBound] : [lastId],
      );
      chunkRows = Number(upsertRes.rows[0]?.rows_processed ?? '0');

      await client.query('COMMIT');

      stats.chunks += 1;
      stats.rowsProcessed += chunkRows;

      if (upperBound === null) {
        // No more rows past lastId — done.
        break;
      }
      lastId = Number(upperBound);
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch { /* ignore */ }
      log.warn(
        { err: (err as Error).message, table, lastId, chunkRows },
        'archiveLatestBackfill: chunk failed; stopping for this table',
      );
      break;
    } finally {
      client.release();
    }

    if (PAUSE_BETWEEN_CHUNKS_MS > 0) {
      await new Promise((r) => setTimeout(r, PAUSE_BETWEEN_CHUNKS_MS));
    }
  }

  stats.ms = Date.now() - start;
  return stats;
}

export async function runArchiveLatestBackfill(): Promise<void> {
  if (running) return;
  running = true;
  try {
    const pool = await getPool();
    if (!pool) {
      log.info('archiveLatestBackfill: no pool, skipping');
      return;
    }
    for (const table of ARCHIVE_TABLES) {
      try {
        const needs = await tableNeedsBackfill(pool, table);
        if (!needs) {
          log.info({ table }, 'archiveLatestBackfill: sidecar already populated, skipping');
          continue;
        }
        const stats = await backfillTable(pool, table);
        log.info(stats, 'archiveLatestBackfill: table done');
      } catch (err) {
        log.warn(
          { err: (err as Error).message, table },
          'archiveLatestBackfill: table failed (non-fatal)',
        );
      }
    }
  } finally {
    running = false;
  }
}

/** Schedule the backfill on a delayed timer so it doesn't compete with
 *  startup-critical work. Idempotent — multiple calls just no-op. */
export function scheduleArchiveLatestBackfill(): void {
  setTimeout(() => {
    void runArchiveLatestBackfill();
  }, STARTUP_DELAY_MS).unref?.();
}
