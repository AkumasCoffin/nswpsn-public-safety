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
  // archive_waze heatmap helper: only index rows with non-null coords
  // so the heatmap query's WHERE filter becomes implicit. Cheap WHERE
  // (column null check) so write overhead is negligible.
  {
    name: 'idx_archive_waze_heatmap',
    sql: `CREATE INDEX IF NOT EXISTS idx_archive_waze_heatmap
          ON archive_waze (source, fetched_at DESC)
          WHERE lat IS NOT NULL AND lng IS NOT NULL`,
  },
];

/**
 * Indexes to actively DROP. Earlier revisions added partial indexes on
 * `(data->>'is_live') IN ('1','true','True')` to speed up the live-only
 * filter, but the JSONB extraction inside the WHERE clause forced
 * per-row JSON parsing during every INSERT — production showed
 * archive_power INSERTs of 1.3k rows taking 60s+. The indexes weren't
 * pulling their weight on read-side either (the planner often picked
 * the regular `(source, fetched_at DESC)` index with a Filter step
 * anyway). Dropping them frees up write throughput.
 */
const DROP_INDEXES = [
  'idx_archive_waze_live',
  'idx_archive_traffic_live',
  'idx_archive_power_live',
  'idx_archive_misc_live',
  'idx_archive_rfs_live',
];

/**
 * One-shot backfills. Each runs after index work, only if the target
 * row count is non-zero. Idempotent — re-running on a clean table is
 * a no-op (each WHERE clause matches zero rows).
 */
interface BackfillSpec {
  name: string;
  // Cheap COUNT(*) to decide whether to run the heavier UPDATE.
  countSql: string;
  // The actual UPDATE.
  updateSql: string;
}

const BACKFILLS: BackfillSpec[] = [
  // waze_police: rows where subcategory is empty/null but data carries
  // the subtype in the JSONB blob → promote it into the column. Mirrors
  // python's normaliser which sometimes left subcategory empty even
  // when the subtype was right there in `data->>'subtype'`.
  {
    name: 'archive_waze.subcategory ← data->>subtype (waze_police)',
    countSql: `SELECT COUNT(*)::bigint AS n FROM archive_waze
               WHERE source = 'waze_police'
                 AND COALESCE(subcategory, '') = ''
                 AND NULLIF(data->>'subtype', '') IS NOT NULL`,
    updateSql: `UPDATE archive_waze
                   SET subcategory = data->>'subtype'
                 WHERE source = 'waze_police'
                   AND COALESCE(subcategory, '') = ''
                   AND NULLIF(data->>'subtype', '') IS NOT NULL`,
  },
  // waze_police: ALL remaining empty-subcategory rows default to
  // POLICE_VISIBLE. Mirrors python's `eff = sub or 'POLICE_VISIBLE'`
  // (external_api_proxy.py:10167) — Waze police alerts without an
  // explicit subtype are treated as visible. User-confirmed policy
  // ("any police without a subtype should be counted as visible
  // police"). After this, every waze_police row has a non-empty
  // subcategory and downstream filters (/api/data/history,
  // filterCache, heatmap) all see the same value without needing
  // COALESCE chains.
  {
    name: 'archive_waze.subcategory = POLICE_VISIBLE (waze_police default)',
    countSql: `SELECT COUNT(*)::bigint AS n FROM archive_waze
               WHERE source = 'waze_police'
                 AND COALESCE(subcategory, '') = ''`,
    updateSql: `UPDATE archive_waze
                   SET subcategory = 'POLICE_VISIBLE'
                 WHERE source = 'waze_police'
                   AND COALESCE(subcategory, '') = ''`,
  },
  // Other waze types (hazard/roadwork/jam): promote data->>'subtype'
  // into subcategory where the column is empty but JSONB has it. No
  // default for these — leave subcategory empty when nothing is
  // available, since hazard subtypes are too varied to assign a
  // generic one.
  {
    name: 'archive_waze.subcategory ← data->>subtype (other waze)',
    countSql: `SELECT COUNT(*)::bigint AS n FROM archive_waze
               WHERE source IN ('waze_hazard','waze_roadwork','waze_jam')
                 AND COALESCE(subcategory, '') = ''
                 AND NULLIF(data->>'subtype', '') IS NOT NULL`,
    updateSql: `UPDATE archive_waze
                   SET subcategory = data->>'subtype'
                 WHERE source IN ('waze_hazard','waze_roadwork','waze_jam')
                   AND COALESCE(subcategory, '') = ''
                   AND NULLIF(data->>'subtype', '') IS NOT NULL`,
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

      // Drop indexes that are hurting us before building the desired set.
      // The partial-live indexes were causing INSERT timeouts via JSONB
      // parse-on-write (see DROP_INDEXES docstring above).
      let dropped = 0;
      for (const name of DROP_INDEXES) {
        try {
          const before = await client.query<{ indexname: string }>(
            `SELECT indexname FROM pg_indexes WHERE indexname = $1`,
            [name],
          );
          if (before.rowCount === 0) continue;
          await client.query(`DROP INDEX IF EXISTS ${name}`);
          dropped += 1;
          log.info({ index: name }, 'indexBuilder: dropped (write-overhead)');
        } catch (err) {
          log.warn(
            { err: (err as Error).message, index: name },
            'indexBuilder: drop failed',
          );
        }
      }
      if (dropped > 0) log.info({ dropped }, 'indexBuilder: drops complete');

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

      // One-shot data backfills. Each is gated on a cheap COUNT first
      // so on a clean DB this whole pass costs ~5 ms. On the first
      // boot after deploying this code, may UPDATE tens of thousands
      // of rows; subsequent boots are no-ops. Track which tables had
      // a non-trivial UPDATE so we know which to VACUUM after.
      const tablesNeedingVacuum = new Set<string>();
      for (const bf of BACKFILLS) {
        try {
          const cnt = await client.query<{ n: string }>(bf.countSql);
          const n = Number(cnt.rows[0]?.n ?? 0);
          if (n === 0) continue;
          const t0 = Date.now();
          log.info({ backfill: bf.name, candidates: n }, 'indexBuilder: backfilling');
          const r = await client.query(bf.updateSql);
          log.info(
            { backfill: bf.name, updated: r.rowCount, ms: Date.now() - t0 },
            'indexBuilder: backfill done',
          );
          // Heuristic: any backfill name starting with "archive_<table>"
          // implies that table needs VACUUM after.
          const m = /^archive_(\w+)\./.exec(bf.name);
          if (m) tablesNeedingVacuum.add(`archive_${m[1]}`);
        } catch (err) {
          log.warn(
            { err: (err as Error).message, backfill: bf.name },
            'indexBuilder: backfill failed',
          );
        }
      }

      // VACUUM ANALYZE the archive tables. Two reasons:
      //   1. ANALYZE refreshes pg_statistic so the planner picks the new
      //      indexes instead of seq-scanning.
      //   2. VACUUM (non-FULL) reclaims dead tuples — critical after a
      //      bulk UPDATE like the POLICE_VISIBLE backfill which can
      //      leave tens of thousands of dead rows that bloat scans
      //      until autovacuum catches up. Plain VACUUM doesn't take
      //      ACCESS EXCLUSIVE so reads and writes still flow.
      // We only run VACUUM ANALYZE on tables a backfill touched; for
      // the rest, plain ANALYZE is enough.
      const tables = [
        'archive_waze',
        'archive_traffic',
        'archive_rfs',
        'archive_power',
        'archive_misc',
      ];
      for (const t of tables) {
        const t0 = Date.now();
        const cmd = tablesNeedingVacuum.has(t) ? `VACUUM (ANALYZE) ${t}` : `ANALYZE ${t}`;
        try {
          await client.query(cmd);
          log.info(
            { table: t, ms: Date.now() - t0, op: tablesNeedingVacuum.has(t) ? 'vacuum-analyze' : 'analyze' },
            'indexBuilder: stats refresh',
          );
        } catch (err) {
          log.warn(
            { err: (err as Error).message, table: t, cmd },
            'indexBuilder: stats refresh failed',
          );
        }
      }
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
