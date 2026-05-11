/**
 * One-shot Postgres diagnostic for the slow archive_* queries.
 *
 * Run from `backends/node`:
 *   npx tsx --env-file-if-exists=../.env scripts/db-diagnose.ts
 *
 * Prints a single JSON blob covering, per archive_* table:
 *   - partition list with row estimates and on-disk size
 *   - every index attached (parent + per-partition)
 *   - n_live_tup / n_dead_tup / last_autovacuum / last_analyze
 *   - EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) for the two queries that
 *     have been timing out: archiveLiveness DISTINCT ON and
 *     filterCache GROUP BY.
 *
 * Also dumps pg_stat_activity (filtered to long-running queries) so we
 * can see whether anything is holding locks against these tables right
 * now.
 *
 * Read-only — does not modify any data. Safe to run on production.
 */
import { Pool } from 'pg';

const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) {
  console.error('DATABASE_URL not set');
  process.exit(1);
}

const pool = new Pool({
  connectionString: dbUrl,
  max: 4,
  // EXPLAIN ANALYZE actually runs the query; cap at 60s so a single
  // hung partition doesn't black-hole the whole run. We'd rather see
  // "timed out" in the JSON than no output at all.
  statement_timeout: 60_000,
  // 30s connect cap — fast fail if the server is unreachable.
  connectionTimeoutMillis: 30_000,
});

const ARCHIVE_TABLES = ['archive_misc', 'archive_traffic', 'archive_rfs', 'archive_power'];

interface PartitionInfo {
  partition: string;
  size_bytes: string;
  pretty_size: string;
  estimated_rows: number;
}

interface TableStats {
  relname: string;
  n_live_tup: number;
  n_dead_tup: number;
  dead_pct: number;
  last_autovacuum: string | null;
  last_autoanalyze: string | null;
  last_vacuum: string | null;
  last_analyze: string | null;
}

interface IndexInfo {
  partition: string;
  indexname: string;
  indexdef: string;
  size_bytes: string;
  pretty_size: string;
}

async function listPartitions(table: string): Promise<PartitionInfo[]> {
  // pg_inherits + pg_class: every partition inherits from the parent.
  const { rows } = await pool.query<{
    partition: string;
    size_bytes: string;
    pretty_size: string;
    estimated_rows: string;
  }>(
    `
    SELECT child.relname AS partition,
           pg_relation_size(child.oid)::text AS size_bytes,
           pg_size_pretty(pg_relation_size(child.oid)) AS pretty_size,
           child.reltuples::text AS estimated_rows
      FROM pg_inherits
      JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
      JOIN pg_class child  ON child.oid  = pg_inherits.inhrelid
     WHERE parent.relname = $1
     ORDER BY child.relname
    `,
    [table],
  );
  return rows.map((r) => ({
    partition: r.partition,
    size_bytes: r.size_bytes,
    pretty_size: r.pretty_size,
    estimated_rows: Number(r.estimated_rows),
  }));
}

async function listIndexes(table: string): Promise<IndexInfo[]> {
  // Walk parent + every partition. pg_inherits gives us the children;
  // union with the parent itself so the parent-defined indexes show up
  // (Postgres replicates them onto each partition automatically but
  // listing the parent makes the relationship clear).
  const { rows } = await pool.query<{
    partition: string;
    indexname: string;
    indexdef: string;
    size_bytes: string;
    pretty_size: string;
  }>(
    `
    WITH all_relations AS (
      SELECT $1::text AS relname
      UNION ALL
      SELECT child.relname
        FROM pg_inherits
        JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
        JOIN pg_class child  ON child.oid  = pg_inherits.inhrelid
       WHERE parent.relname = $1
    )
    SELECT i.tablename AS partition,
           i.indexname,
           i.indexdef,
           pg_relation_size(c.oid)::text AS size_bytes,
           pg_size_pretty(pg_relation_size(c.oid)) AS pretty_size
      FROM pg_indexes i
      JOIN all_relations r ON i.tablename = r.relname
      LEFT JOIN pg_class c ON c.relname = i.indexname
     WHERE i.schemaname = 'public'
     ORDER BY i.tablename, i.indexname
    `,
    [table],
  );
  return rows;
}

async function tableStats(table: string): Promise<TableStats[]> {
  // Stats live on the partition leaves, not the parent. Sum live/dead
  // across partitions so we can spot tables with bloated children even
  // when the parent looks fine.
  const { rows } = await pool.query<{
    relname: string;
    n_live_tup: string;
    n_dead_tup: string;
    last_autovacuum: string | null;
    last_autoanalyze: string | null;
    last_vacuum: string | null;
    last_analyze: string | null;
  }>(
    `
    WITH partitions AS (
      SELECT child.relname
        FROM pg_inherits
        JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
        JOIN pg_class child  ON child.oid  = pg_inherits.inhrelid
       WHERE parent.relname = $1
      UNION
      SELECT $1
    )
    SELECT s.relname,
           s.n_live_tup::text,
           s.n_dead_tup::text,
           s.last_autovacuum::text,
           s.last_autoanalyze::text,
           s.last_vacuum::text,
           s.last_analyze::text
      FROM pg_stat_user_tables s
      JOIN partitions p ON p.relname = s.relname
     ORDER BY s.relname
    `,
    [table],
  );
  return rows.map((r) => {
    const live = Number(r.n_live_tup);
    const dead = Number(r.n_dead_tup);
    const dead_pct = live + dead > 0 ? (dead / (live + dead)) * 100 : 0;
    return {
      relname: r.relname,
      n_live_tup: live,
      n_dead_tup: dead,
      dead_pct: Number(dead_pct.toFixed(1)),
      last_autovacuum: r.last_autovacuum,
      last_autoanalyze: r.last_autoanalyze,
      last_vacuum: r.last_vacuum,
      last_analyze: r.last_analyze,
    };
  });
}

/**
 * EXPLAIN (ANALYZE, BUFFERS) the archiveLiveness DISTINCT ON query for
 * a single archive table. Mirrors the live SQL in
 * `backends/node/src/services/archiveLiveness.ts:107-119` but with
 * placeholder-substituted source list (every NON-waze source associated
 * with the table family — the values come from the archive writer's
 * ARCHIVE_SOURCE_TABLES map but for diagnostic purposes we dump every
 * source actually present in the table).
 */
async function explainArchiveLiveness(table: string): Promise<unknown> {
  // Pick a real source list from the table itself. Empty array is a
  // valid hint that the table sees no traffic; skip the EXPLAIN since
  // the production path skips empty source lists too.
  const srcs = await pool.query<{ source: string }>(
    `SELECT DISTINCT source FROM ${table}
     WHERE fetched_at >= now() - interval '1 day'
       AND NOT (source LIKE 'waze_%')
     LIMIT 20`,
  );
  const sources = srcs.rows.map((r) => r.source);
  if (sources.length === 0) {
    return { skipped: 'no recent non-waze sources in table' };
  }
  const sql = `
    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
    SELECT source, source_id, lat, lng, category, subcategory, data
      FROM (
        SELECT DISTINCT ON (source, source_id)
               source, source_id, lat, lng, category, subcategory, data, fetched_at
          FROM ${table}
         WHERE source = ANY($1::text[])
           AND source_id IS NOT NULL
           AND fetched_at >= now() - ($2 || ' days')::interval
         ORDER BY source, source_id, fetched_at DESC
      ) latest
     WHERE COALESCE(data->>'is_live', 'true') NOT IN ('0','false','False')
  `;
  const r = await pool.query<{ 'QUERY PLAN': unknown }>(sql, [sources, '7']);
  return { sources_used: sources, plan: r.rows[0]?.['QUERY PLAN'] };
}

/**
 * EXPLAIN the filterCache GROUP BY scan. Same SQL as
 * `backends/node/src/store/filterCache.ts:489-500`.
 */
async function explainFilterCache(table: string, windowDays: number): Promise<unknown> {
  const sql = `
    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
    SELECT source, category, subcategory, COUNT(*)::text AS cnt,
           EXTRACT(EPOCH FROM MIN(fetched_at))::bigint AS oldest,
           EXTRACT(EPOCH FROM MAX(fetched_at))::bigint AS newest
      FROM ${table}
     WHERE fetched_at >= NOW() - ($1 || ' days')::interval
     GROUP BY 1, 2, 3
  `;
  const r = await pool.query<{ 'QUERY PLAN': unknown }>(sql, [String(windowDays)]);
  return { window_days: windowDays, plan: r.rows[0]?.['QUERY PLAN'] };
}

/**
 * EXPLAIN the pager/hits archive_misc query. Different shape from the
 * other two — this is the one currently wrapped in SwrCache after the
 * latest fix. Worth profiling to know whether the underlying query
 * needs an expression index too.
 */
async function explainPagerHits(): Promise<unknown> {
  const cutoff = Math.floor(Date.now() / 1000) - 24 * 3600;
  const sql = `
    EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
    SELECT * FROM (
      SELECT DISTINCT ON (source_id)
        source_id, lat, lng, category, subcategory, fetched_at, data
      FROM archive_misc
      WHERE source = 'pager'
        AND (data->>'timestamp')::bigint >= $1
      ORDER BY source_id, fetched_at DESC
    ) x
    ORDER BY fetched_at DESC
    LIMIT 500
  `;
  const r = await pool.query<{ 'QUERY PLAN': unknown }>(sql, [cutoff]);
  return { cutoff, plan: r.rows[0]?.['QUERY PLAN'] };
}

interface ActivityRow {
  pid: number;
  state: string;
  wait_event_type: string | null;
  wait_event: string | null;
  query_start_age: string;
  query: string;
}

async function longRunningQueries(): Promise<ActivityRow[]> {
  const { rows } = await pool.query<ActivityRow>(
    `
    SELECT pid,
           state,
           wait_event_type,
           wait_event,
           (now() - query_start)::text AS query_start_age,
           regexp_replace(query, '\\s+', ' ', 'g') AS query
      FROM pg_stat_activity
     WHERE state = 'active'
       AND query_start IS NOT NULL
       AND now() - query_start > interval '5 seconds'
       AND query NOT ILIKE '%pg_stat_activity%'
     ORDER BY query_start ASC
    `,
  );
  return rows;
}

/**
 * Run a single diagnostic step and emit a tagged JSON line on stdout.
 * Tagging via NDJSON so a hang in step N doesn't lose steps 1..N-1 —
 * the consumer can recover whatever made it out before the SIGKILL.
 */
async function step<T>(
  tag: string,
  fn: () => Promise<T>,
): Promise<void> {
  const t0 = Date.now();
  process.stderr.write(`[${tag}] starting...\n`);
  try {
    const result = await fn();
    process.stdout.write(
      JSON.stringify({ tag, ok: true, ms: Date.now() - t0, result }) + '\n',
    );
    process.stderr.write(`[${tag}] done in ${Date.now() - t0}ms\n`);
  } catch (err) {
    process.stdout.write(
      JSON.stringify({
        tag,
        ok: false,
        ms: Date.now() - t0,
        error: (err as Error).message,
      }) + '\n',
    );
    process.stderr.write(`[${tag}] FAILED after ${Date.now() - t0}ms: ${(err as Error).message}\n`);
  }
}

async function main(): Promise<void> {
  process.stdout.write(JSON.stringify({ tag: 'meta', captured_at: new Date().toISOString() }) + '\n');

  for (const table of ARCHIVE_TABLES) {
    await step(`${table}.partitions`, () => listPartitions(table));
    await step(`${table}.indexes`, () => listIndexes(table));
    await step(`${table}.stats`, () => tableStats(table));
    await step(`${table}.archive_liveness_plan`, () => explainArchiveLiveness(table));
    const windowDays = table === 'archive_traffic' || table === 'archive_rfs' ? 1 : 7;
    await step(`${table}.filter_cache_plan`, () => explainFilterCache(table, windowDays));
  }

  await step('pager_hits_plan', explainPagerHits);
  await step('long_running_queries', longRunningQueries);
}

main()
  .then(() => pool.end())
  .catch(async (err) => {
    process.stderr.write(`fatal: ${(err as Error).message}\n`);
    await pool.end().catch(() => {});
    process.exit(1);
  });
