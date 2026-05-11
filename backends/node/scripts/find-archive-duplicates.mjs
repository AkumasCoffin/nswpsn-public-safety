#!/usr/bin/env node
/**
 * Archive duplicate checker.
 *
 * The archive_* tables are APPEND-ONLY. Every poll snapshot becomes a
 * row. With one poll every ~30s per source, the natural row count for
 * a given (source, source_id) over N hours is roughly N*120. archive_waze
 * had ~2x that — duplicates leaked in.
 *
 * Two failure modes produce duplicates:
 *
 *   1. Exact dupes — same (source, source_id, fetched_at) tuple twice.
 *      Caused when the writer's re-queue path retried a batch that had
 *      ALREADY made it past the INSERT before the timeout cancelled
 *      the query. The transaction rollback didn't undo the actual
 *      writes (they had committed individual chunks), but the writer
 *      re-queued the whole batch on the timeout.
 *
 *   2. Burst dupes — same (source, source_id) at fetched_at values
 *      within a few seconds of each other. Caused when a single poll
 *      cycle's snapshot got fanned-out into the queue twice (e.g.
 *      both registerAllSources paths running in some race window).
 *
 * Usage:
 *   node --env-file-if-exists=../.env scripts/find-archive-duplicates.mjs           # report only
 *   node --env-file-if-exists=../.env scripts/find-archive-duplicates.mjs --delete  # delete exact dupes
 *
 * --delete keeps the lowest id per (source, source_id, fetched_at) tuple
 * and removes the rest. Burst dupes are reported but never auto-deleted —
 * those need a human to decide whether the second snapshot was meaningful.
 *
 * Per-table: skips empty tables, prints exact-dupe count, burst-dupe
 * count (within 10s window), top-10 worst offenders by source. Safe to
 * run while the service is up — every query is read-only unless --delete
 * is passed; the DELETE in --delete mode runs in a transaction with a
 * 5-min statement_timeout so it can't get stuck.
 */
import pg from 'pg';

const { Pool } = pg;

const TABLES = [
  'archive_waze',
  'archive_traffic',
  'archive_rfs',
  'archive_power',
  'archive_misc',
];

const BURST_WINDOW_SECS = 10;
const SHOULD_DELETE = process.argv.includes('--delete');

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error('DATABASE_URL not set. Run with --env-file-if-exists=../.env');
    process.exit(1);
  }

  const pool = new Pool({
    connectionString: url,
    max: 1,
    statement_timeout: 0,
    idleTimeoutMillis: 0,
  });

  console.log('=== archive duplicate checker ===');
  if (SHOULD_DELETE) {
    console.log('MODE: --delete passed — exact duplicates WILL be removed.');
  } else {
    console.log('MODE: report only. Pass --delete to remove exact duplicates.');
  }
  console.log('');

  const totals = {
    rows: 0,
    exactDupes: 0,
    burstDupes: 0,
    deleted: 0,
  };

  try {
    for (const table of TABLES) {
      const r = await runOne(pool, table);
      totals.rows += r.rows;
      totals.exactDupes += r.exactDupes;
      totals.burstDupes += r.burstDupes;
      totals.deleted += r.deleted;
    }
    console.log('\n=== summary ===');
    console.log(`  total rows:       ${totals.rows.toLocaleString()}`);
    console.log(`  exact duplicates: ${totals.exactDupes.toLocaleString()}`);
    console.log(`  burst duplicates: ${totals.burstDupes.toLocaleString()}`);
    if (SHOULD_DELETE) {
      console.log(`  deleted:          ${totals.deleted.toLocaleString()}`);
    }
  } finally {
    await pool.end();
  }
}

async function runOne(pool, table) {
  console.log(`\n=== ${table} ===`);
  const exists = await pool.query(
    `SELECT 1 FROM pg_class WHERE relname = $1
       AND relnamespace = 'public'::regnamespace`,
    [table],
  );
  if (exists.rowCount === 0) {
    console.log('  (table not found — skipping)');
    return { rows: 0, exactDupes: 0, burstDupes: 0, deleted: 0 };
  }

  const totalRes = await pool.query(`SELECT count(*)::bigint AS n FROM ${table}`);
  const rows = Number(totalRes.rows[0].n);
  console.log(`  rows: ${rows.toLocaleString()}`);
  if (rows === 0) {
    return { rows: 0, exactDupes: 0, burstDupes: 0, deleted: 0 };
  }

  // Exact duplicates: same (source, source_id, fetched_at) more than
  // once. NULL source_id values are folded into a single null-group
  // via IS NOT DISTINCT FROM semantics in the DELETE; here we treat
  // them with regular GROUP BY (postgres groups all NULLs together).
  // sub-query keeps it to a single round-trip.
  const exactRes = await pool.query(`
    WITH dupe_groups AS (
      SELECT count(*) AS n
        FROM ${table}
       GROUP BY source, source_id, fetched_at
      HAVING count(*) > 1
    )
    SELECT
      COALESCE(count(*), 0)::bigint  AS groups,
      COALESCE(sum(n - 1), 0)::bigint AS dupes
    FROM dupe_groups
  `);
  const exactCount = Number(exactRes.rows[0]?.dupes ?? 0);
  const groupCount = Number(exactRes.rows[0]?.groups ?? 0);
  console.log(`  exact duplicates: ${exactCount.toLocaleString()}` +
    (exactCount > 0 ? `  (across ${groupCount.toLocaleString()} groups)` : ''));

  // Burst duplicates: same (source, source_id) appearing more than
  // once within BURST_WINDOW_SECS. Different fetched_at, but suspicious.
  // We use a self-join restricted to the same source_id with a small
  // time delta — bounded to source_id IS NOT NULL because the null
  // case matches too widely (every snapshot for a source).
  const burstRes = await pool.query(
    `
    SELECT count(*)::bigint AS n
      FROM ${table} a
      JOIN ${table} b
        ON a.source = b.source
       AND a.source_id = b.source_id
       AND a.id < b.id
       AND b.fetched_at - a.fetched_at < INTERVAL '${BURST_WINDOW_SECS} seconds'
       AND b.fetched_at >= a.fetched_at
     WHERE a.source_id IS NOT NULL
    `,
  );
  const burstCount = Number(burstRes.rows[0]?.n ?? 0);
  console.log(`  burst duplicates (<${BURST_WINDOW_SECS}s): ${burstCount.toLocaleString()}`);

  // Top offenders — which (source) is responsible for most exact dupes?
  if (exactCount > 0) {
    const top = await pool.query(`
      WITH dupe_groups AS (
        SELECT source, count(*) - 1 AS extras
          FROM ${table}
         GROUP BY source, source_id, fetched_at
        HAVING count(*) > 1
      )
      SELECT source, sum(extras)::bigint AS dupes
        FROM dupe_groups
       GROUP BY source
       ORDER BY dupes DESC
       LIMIT 10
    `);
    console.log('  top offenders by source:');
    for (const r of top.rows) {
      console.log(`    ${r.source.padEnd(30)} ${Number(r.dupes).toLocaleString()}`);
    }
  }

  let deleted = 0;
  if (SHOULD_DELETE && exactCount > 0) {
    console.log(`  deleting ${exactCount.toLocaleString()} exact duplicates…`);
    const t0 = Date.now();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // 5 min cap so a runaway query can't lock the table forever.
      await client.query("SET LOCAL statement_timeout = '300s'");
      // Keep the lowest id per (source, source_id, fetched_at) tuple,
      // delete the rest. Self-join on the keyset so we don't scan
      // every row twice. EXISTS is faster than IN here on big tables.
      const r = await client.query(`
        DELETE FROM ${table} t
         WHERE EXISTS (
           SELECT 1 FROM ${table} k
            WHERE k.source = t.source
              AND (k.source_id IS NOT DISTINCT FROM t.source_id)
              AND k.fetched_at = t.fetched_at
              AND k.id < t.id
         )
      `);
      await client.query('COMMIT');
      deleted = r.rowCount ?? 0;
      console.log(`  deleted ${deleted.toLocaleString()} rows in ${Date.now() - t0} ms`);
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch { /* ignore */ }
      console.error(`  delete failed: ${err.message}`);
      throw err;
    } finally {
      client.release();
    }
  }

  return { rows, exactDupes: exactCount, burstDupes: burstCount, deleted };
}

main().catch((err) => {
  console.error('find-archive-duplicates failed:', err);
  process.exit(1);
});
