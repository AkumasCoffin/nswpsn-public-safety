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
  // is_latest experiment fallout — migration 013 drops the columns,
  // but defensively drop the indexes too in case a partition kept
  // them after the column removal cascaded.
  'idx_archive_waze_src_sid_latest',
  'idx_archive_traffic_src_sid_latest',
  'idx_archive_rfs_src_sid_latest',
  'idx_archive_power_src_sid_latest',
  'idx_archive_misc_src_sid_latest',
  'idx_archive_waze_latest',
  'idx_archive_traffic_latest',
  'idx_archive_rfs_latest',
  'idx_archive_power_latest',
  'idx_archive_misc_latest',
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
  // One-shot population of police_heatmap_bin_daily from existing
  // archive_waze rows. The migration creates the table empty; this
  // backfills the trailing 30 days so the heatmap refresh has data
  // to read from on the next cycle. Scoped to fetched_at::date <
  // CURRENT_DATE so it never overlaps with today — the writer is
  // already incrementing today's rows in real time, and overlapping
  // would either double-count or clobber depending on which side
  // ran last. ON CONFLICT DO NOTHING keeps the rerun safe even
  // without the marker (defence-in-depth).
  //
  // Long-running on a slow host (the previous heatmap aggregation
  // sat in IO/AioIoCompletion for 30+ min). indexBuilder runs with
  // statement_timeout=0 so this is allowed to take as long as it
  // needs without blocking startup. The marker ensures re-deploys
  // don't pay the cost twice.
  {
    name: 'archive_waze.police_heatmap_bin_daily backfill',
    countSql: `SELECT COUNT(*)::bigint AS n
                 FROM archive_waze
                WHERE source = 'waze_police'
                  AND lat IS NOT NULL AND lng IS NOT NULL
                  AND fetched_at >= NOW() - INTERVAL '30 days'
                  AND (fetched_at AT TIME ZONE 'UTC')::date < (NOW() AT TIME ZONE 'UTC')::date`,
    updateSql: `INSERT INTO police_heatmap_bin_daily
                  (day, lat_bin, lng_bin, subcategory, count)
                SELECT
                  (fetched_at AT TIME ZONE 'UTC')::date AS day,
                  (FLOOR(lat / 0.001) * 0.001)::float8 AS lat_bin,
                  (FLOOR(lng / 0.001) * 0.001)::float8 AS lng_bin,
                  COALESCE(NULLIF(subcategory, ''), data->>'subtype', 'POLICE_VISIBLE') AS subcategory,
                  COUNT(*)::int AS count
                FROM archive_waze
                WHERE source = 'waze_police'
                  AND lat IS NOT NULL AND lng IS NOT NULL
                  AND fetched_at >= NOW() - INTERVAL '30 days'
                  AND (fetched_at AT TIME ZONE 'UTC')::date < (NOW() AT TIME ZONE 'UTC')::date
                GROUP BY 1, 2, 3, 4
                ON CONFLICT (day, lat_bin, lng_bin, subcategory) DO NOTHING`,
  },
  // One-shot population of filter_facets_daily from each archive_*
  // table (skipping archive_waze — see migration 016 header). Same
  // race-avoidance pattern as the police heatmap backfill: scope to
  // days < today so the writer's same-day increments never collide.
  // ON CONFLICT DO NOTHING for re-run safety. One spec per archive
  // table because aggregating all four in one query would force the
  // single statement_timeout=0 connection to hold its lock for the
  // duration of every scan.
  ...(['archive_misc', 'archive_traffic', 'archive_rfs', 'archive_power'] as const).map(
    (tbl): BackfillSpec => ({
      name: `${tbl}.filter_facets_daily backfill`,
      countSql: `SELECT COUNT(*)::bigint AS n
                   FROM ${tbl}
                  WHERE fetched_at >= NOW() - INTERVAL '7 days'
                    AND (fetched_at AT TIME ZONE 'UTC')::date < (NOW() AT TIME ZONE 'UTC')::date`,
      updateSql: `INSERT INTO filter_facets_daily
                    (day, archive, source, category, subcategory, count)
                  SELECT
                    (fetched_at AT TIME ZONE 'UTC')::date AS day,
                    '${tbl}' AS archive,
                    source,
                    COALESCE(category, '') AS category,
                    COALESCE(subcategory, '') AS subcategory,
                    COUNT(*)::int AS count
                  FROM ${tbl}
                  WHERE fetched_at >= NOW() - INTERVAL '7 days'
                    AND (fetched_at AT TIME ZONE 'UTC')::date < (NOW() AT TIME ZONE 'UTC')::date
                  GROUP BY 1, 2, 3, 4, 5
                  ON CONFLICT (day, archive, source, category, subcategory) DO NOTHING`,
    }),
  ),
  // NOTE: is_live and is_latest backfills disabled. The is_latest
  // approach created untenable I/O contention — the bulk UPDATE
  // creating ~366k dead tuples on archive_waze made every query
  // (writes, reads, heatmap aggregation) compete for disk. Reverted
  // to bounded DISTINCT ON in the read path. Columns still exist
  // in the schema (migration 012) but reads no longer filter them.
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

      // One-shot data backfills. Track completion in schema_migrations
      // so subsequent restarts skip the heavy pre-check entirely. The
      // COUNT scans that gate each backfill can be slow on large tables
      // when they involve JSONB extraction or full-table predicates;
      // running them every restart is wasteful when the backfill has
      // already happened.
      const tablesNeedingVacuum = new Set<string>();
      // Ensure the marker table exists (migrate.ts creates it but be
      // defensive in case indexBuilder runs first).
      await client.query(`
        CREATE TABLE IF NOT EXISTS schema_migrations (
          filename TEXT PRIMARY KEY,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);
      const completed = await client.query<{ filename: string }>(
        `SELECT filename FROM schema_migrations
         WHERE filename LIKE '_backfill_%'`,
      );
      const done = new Set(completed.rows.map((r) => r.filename));

      for (const bf of BACKFILLS) {
        const marker = `_backfill_${bf.name}`;
        if (done.has(marker)) continue;
        try {
          const cnt = await client.query<{ n: string }>(bf.countSql);
          const n = Number(cnt.rows[0]?.n ?? 0);
          if (n === 0) {
            // No work to do — record completion so we never run the
            // expensive COUNT again.
            await client.query(
              `INSERT INTO schema_migrations (filename) VALUES ($1)
               ON CONFLICT (filename) DO NOTHING`,
              [marker],
            );
            continue;
          }
          const t0 = Date.now();
          log.info({ backfill: bf.name, candidates: n }, 'indexBuilder: backfilling');
          const r = await client.query(bf.updateSql);
          log.info(
            { backfill: bf.name, updated: r.rowCount, ms: Date.now() - t0 },
            'indexBuilder: backfill done',
          );
          // Mark complete only after a SUCCESSFUL run. Failed runs
          // re-attempt next boot.
          await client.query(
            `INSERT INTO schema_migrations (filename) VALUES ($1)
             ON CONFLICT (filename) DO NOTHING`,
            [marker],
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

      // VACUUM ANALYZE the archive tables a backfill TOUCHED. Was:
      // unconditional ANALYZE on every archive_* table on every boot.
      // ANALYZE on a partitioned parent recurses into every partition,
      // so on archive_waze that's ~22 minutes of disk hammering at
      // every restart on a saturated host. autovacuum keeps each
      // partition's stats fresh on its own threshold; we only need to
      // step in here when a backfill just rewrote a meaningful chunk of
      // rows (like the POLICE_VISIBLE backfill that creates dead
      // tuples). Tables NOT in tablesNeedingVacuum are now skipped
      // entirely and rely on autovacuum.
      for (const t of tablesNeedingVacuum) {
        const t0 = Date.now();
        const cmd = `VACUUM (ANALYZE) ${t}`;
        try {
          await client.query(cmd);
          log.info(
            { table: t, ms: Date.now() - t0, op: 'vacuum-analyze' },
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
