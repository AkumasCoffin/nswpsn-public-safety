/**
 * One-shot backfill for sidecar columns promoted from JSONB:
 *   - Migration 021 added category, subcategory
 *   - Migration 022 added title, location_text, status, severity,
 *     is_active
 *
 * Existing sidecar rows have NULL in these columns until the writer
 * naturally re-touches them on the next data_hash change, which could
 * be days for stable incidents. This task copies all of them from the
 * parent row that the sidecar's latest_fetched_at points at.
 *
 * Runs once at startup, opportunistically and in the background. Each
 * chunk is a single UPDATE...FROM so the work happens server-side and
 * Node doesn't carry rows. Idempotent — the WHERE guard skips already-
 * populated rows.
 *
 * Skip behaviour: counts pending rows up-front; if zero, table is done.
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

const CHUNK_SIZE = 5_000;
const PAUSE_BETWEEN_CHUNKS_MS = 500;
const STARTUP_DELAY_MS = 60_000; // start 1 min after boot — well after
                                  // the existing sidecar backfill

let running = false;

interface BackfillStats {
  table: string;
  chunks: number;
  rowsUpdated: number;
  ms: number;
}

// A row needs backfill if ANY of the promoted columns is NULL AND the
// parent has a value for it. Approximated cheaply by checking
// category IS NULL OR title IS NULL — most sources populate at least
// one of these, so this catches both 021 (category) and 022 (title +
// status + severity + ...) backfill targets in one pass.
async function nullCount(pool: Pool, table: string): Promise<number> {
  const r = await pool.query<{ n: string }>(
    `SELECT COUNT(*)::text AS n
       FROM ${table}_latest
      WHERE category IS NULL OR title IS NULL`,
  );
  return Number(r.rows[0]?.n ?? '0');
}

async function backfillTable(pool: Pool, table: string): Promise<BackfillStats> {
  const start = Date.now();
  const stats: BackfillStats = {
    table,
    chunks: 0,
    rowsUpdated: 0,
    ms: 0,
  };

  for (;;) {
    const client = await pool.connect();
    let updated = 0;
    try {
      await client.query('BEGIN');
      await client.query("SET LOCAL statement_timeout = '60s'");

      // CTE picks a chunk of NULL-category sidecar rows, joins to the
      // parent on the latest_fetched_at pointer, then updates the
      // sidecar. The CTE's LIMIT bounds the lock scope so cleanup +
      // writers can interleave. FOR UPDATE SKIP LOCKED would be ideal
      // but pg doesn't support it on the subselect of an UPDATE...FROM;
      // the loop simply re-runs if a row got grabbed concurrently.
      // Backfill both 021 (category/subcategory — parent columns) and
      // 022 (title/location_text/status/severity/is_active — extracted
      // from parent's data JSONB). The CASE on is_active uses the same
      // truthy-string semantics as extractBoolField in archive.ts so
      // backfilled values match what the writer would produce going
      // forward.
      const r = await client.query<{ updated: string }>(
        `WITH batch AS (
           SELECT source, source_id, latest_fetched_at
           FROM ${table}_latest
           WHERE category IS NULL OR title IS NULL
           LIMIT $1
         )
         UPDATE ${table}_latest l
            SET category      = a.category,
                subcategory   = a.subcategory,
                title         = NULLIF(a.data->>'title', ''),
                location_text = NULLIF(a.data->>'location_text', ''),
                status        = NULLIF(a.data->>'status', ''),
                severity      = NULLIF(a.data->>'severity', ''),
                is_active     = CASE
                  WHEN a.data->>'is_active' IN ('1','true','True','TRUE') THEN true
                  WHEN a.data->>'is_active' IN ('0','false','False','FALSE') THEN false
                  ELSE NULL
                END
           FROM batch b
           JOIN ${table} a
             ON a.source = b.source
            AND a.source_id = b.source_id
            AND a.fetched_at = b.latest_fetched_at
          WHERE l.source = b.source
            AND l.source_id = b.source_id
         RETURNING 1`,
        [CHUNK_SIZE],
      );
      updated = r.rowCount ?? 0;
      await client.query('COMMIT');

      stats.chunks += 1;
      stats.rowsUpdated += updated;
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch { /* ignore */ }
      log.warn(
        { err: (err as Error).message, table, chunks: stats.chunks },
        'archiveLatestDimsBackfill: chunk failed; stopping for this table',
      );
      break;
    } finally {
      client.release();
    }

    if (updated === 0) break; // table done

    if (PAUSE_BETWEEN_CHUNKS_MS > 0) {
      await new Promise((r) => setTimeout(r, PAUSE_BETWEEN_CHUNKS_MS));
    }
  }

  stats.ms = Date.now() - start;
  return stats;
}

export async function runArchiveLatestDimsBackfill(): Promise<void> {
  if (running) return;
  running = true;
  try {
    const pool = await getPool();
    if (!pool) {
      log.info('archiveLatestDimsBackfill: no pool, skipping');
      return;
    }
    for (const table of ARCHIVE_TABLES) {
      try {
        const remaining = await nullCount(pool, table);
        if (remaining === 0) {
          log.info({ table }, 'archiveLatestDimsBackfill: no NULL rows, skipping');
          continue;
        }
        log.info({ table, remaining }, 'archiveLatestDimsBackfill: starting');
        const stats = await backfillTable(pool, table);
        log.info(stats, 'archiveLatestDimsBackfill: table done');
      } catch (err) {
        log.warn(
          { err: (err as Error).message, table },
          'archiveLatestDimsBackfill: table failed (non-fatal)',
        );
      }
    }
  } finally {
    running = false;
  }
}

/** Schedule the backfill 60s after boot so it doesn't compete with the
 *  existing archiveLatestBackfill (30s startup delay) or any other
 *  startup work. Idempotent — multiple calls just no-op. */
export function scheduleArchiveLatestDimsBackfill(): void {
  setTimeout(() => {
    void runArchiveLatestDimsBackfill();
  }, STARTUP_DELAY_MS).unref?.();
}
