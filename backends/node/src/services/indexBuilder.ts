/**
 * Background index builder.
 *
 * The perf indexes used to live in a SQL migration (`008_perf_indexes.sql`),
 * but on a multi-million-row archive_waze each CREATE INDEX takes minutes
 * and kept hitting the pool's default 30s statement_timeout — even after
 * we set timeout=0 in the runner, deploys that didn't rebuild dist/
 * carried the old migrate.js without the fix. Index-build failures
 * shouldn't block the entire migration cascade.
 *
 * Solution: build the indexes from a dedicated post-start background job
 * with its own connection (statement_timeout=0). This:
 *   - Doesn't block startup (app serves traffic while indexes build).
 *   - Doesn't depend on migration-runner state.
 *   - Idempotent — `IF NOT EXISTS` makes re-runs cheap.
 *   - Can fail individual indexes without poisoning the rest.
 *
 * Why not CREATE INDEX CONCURRENTLY? Postgres rejects CONCURRENTLY on
 * partitioned tables (archive_* are RANGE-partitioned by month). Plain
 * CREATE INDEX takes a SHARE lock on each partition during the build,
 * blocking writes for the duration. Acceptable since the writer
 * re-queues failed inserts. Long-running but bounded.
 */
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';

interface IndexSpec {
  name: string;
  sql: string;
}

const INDEXES: IndexSpec[] = [
  // Composite (source, source_id, fetched_at DESC) — covering for the
  // unique=1 path's DISTINCT ON. Plain (no INCLUDE) keeps the build
  // faster + index smaller; the ~5% read-side perf loss vs INCLUDE is
  // worth the deploy-time savings.
  {
    name: 'idx_archive_waze_src_sid_ts',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_waze_src_sid_ts
          ON archive_waze (source, source_id, fetched_at DESC)`,
  },
  {
    name: 'idx_archive_traffic_src_sid_ts',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_traffic_src_sid_ts
          ON archive_traffic (source, source_id, fetched_at DESC)`,
  },
  {
    name: 'idx_archive_power_src_sid_ts',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_power_src_sid_ts
          ON archive_power (source, source_id, fetched_at DESC)`,
  },
  {
    name: 'idx_archive_misc_src_sid_ts',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_misc_src_sid_ts
          ON archive_misc (source, source_id, fetched_at DESC)`,
  },
  {
    name: 'idx_archive_rfs_src_sid_ts',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_rfs_src_sid_ts
          ON archive_rfs (source, source_id, fetched_at DESC)`,
  },
  // Partial index on is_live-truthy rows. Halves the row set for the
  // count query's COUNT(*) FILTER aggregate on the default logs.html
  // "live only" filter.
  {
    name: 'idx_archive_waze_live',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_waze_live
          ON archive_waze (source, fetched_at DESC)
          WHERE (data->>'is_live') IN ('1','true','True')`,
  },
  {
    name: 'idx_archive_traffic_live',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_traffic_live
          ON archive_traffic (source, fetched_at DESC)
          WHERE (data->>'is_live') IN ('1','true','True')`,
  },
  {
    name: 'idx_archive_power_live',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_power_live
          ON archive_power (source, fetched_at DESC)
          WHERE (data->>'is_live') IN ('1','true','True')`,
  },
  {
    name: 'idx_archive_misc_live',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_misc_live
          ON archive_misc (source, fetched_at DESC)
          WHERE (data->>'is_live') IN ('1','true','True')`,
  },
  {
    name: 'idx_archive_rfs_live',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_rfs_live
          ON archive_rfs (source, fetched_at DESC)
          WHERE (data->>'is_live') IN ('1','true','True')`,
  },
  // archive_waze heatmap helper: only index rows with non-null coords
  // so the heatmap query's WHERE filter becomes implicit.
  {
    name: 'idx_archive_waze_heatmap',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_waze_heatmap
          ON archive_waze (source, fetched_at DESC)
          WHERE lat IS NOT NULL AND lng IS NOT NULL`,
  },
];

let inFlight: Promise<void> | null = null;

/**
 * Build any missing perf indexes. Idempotent. Returns immediately if
 * already running. Designed to be called from index.ts post-startup
 * with `void ensurePerfIndexes()`.
 */
export async function ensurePerfIndexes(): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const pool = await getPool();
    if (!pool) return;
    let client;
    try {
      client = await pool.connect();
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'indexBuilder: pool acquire failed; skipping',
      );
      return;
    }
    try {
      // Unlimit timeout for this connection — index builds can take
      // minutes per index on large archive_waze partitions.
      await client.query('SET statement_timeout = 0');
      // Skip already-existing indexes cheaply via pg_indexes lookup.
      const existing = await client.query<{ indexname: string }>(
        `SELECT indexname FROM pg_indexes
         WHERE indexname = ANY($1::text[])`,
        [INDEXES.map((i) => i.name)],
      );
      const have = new Set(existing.rows.map((r) => r.indexname));

      let built = 0;
      let skipped = 0;
      let failed = 0;
      for (const ix of INDEXES) {
        if (have.has(ix.name)) {
          skipped += 1;
          continue;
        }
        const t0 = Date.now();
        log.info({ index: ix.name }, 'indexBuilder: building');
        try {
          await client.query(ix.sql);
          built += 1;
          log.info(
            { index: ix.name, ms: Date.now() - t0 },
            'indexBuilder: built',
          );
        } catch (err) {
          failed += 1;
          log.warn(
            { err: (err as Error).message, index: ix.name, ms: Date.now() - t0 },
            'indexBuilder: build failed',
          );
        }
      }
      log.info({ built, skipped, failed }, 'indexBuilder: done');
    } finally {
      client.release();
    }
  })();
  try {
    await inFlight;
  } finally {
    inFlight = null;
  }
}
