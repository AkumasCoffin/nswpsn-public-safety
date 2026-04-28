#!/usr/bin/env node
/**
 * Emergency: VACUUM FULL + REINDEX archive_waze.
 *
 * The is_latest experiment left archive_waze in a bad state — bloat
 * from the bulk UPDATE that touched ~366k rows + index churn from
 * the 7+ indexes per write. Plain VACUUM (autovacuum or our admin
 * endpoint) reclaims dead tuples but doesn't shrink the heap on
 * disk. VACUUM FULL rewrites the table compactly.
 *
 * Trade-off: VACUUM FULL takes ACCESS EXCLUSIVE lock — INSERTs and
 * SELECTs both block during the rewrite. Run with the Node service
 * stopped:
 *
 *   pm2 stop api-node
 *   cd /var/www/nswpsn/backends/node
 *   node --env-file-if-exists=../.env scripts/vacuum-full-archive-waze.mjs
 *   pm2 start api-node
 *
 * Runtime depends on archive_waze size + disk speed. Expect 1-5 min
 * for ~400k rows on SSD.
 *
 * Iterates per-partition (VACUUM FULL on the partitioned parent
 * isn't supported in older PG versions; partition-by-partition is
 * the portable way).
 */
import pg from 'pg';

const { Pool } = pg;

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

  console.log('=== archive_waze emergency recovery ===');
  console.log('VACUUM FULL + REINDEX on every archive_waze partition.');
  console.log('Make sure the Node service is stopped.\n');

  try {
    // Find every child partition of archive_waze.
    const parts = await pool.query<{ partition: string }>(
      `SELECT child.relname::text AS partition
       FROM pg_inherits
       JOIN pg_class child ON child.oid = pg_inherits.inhrelid
       JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
       WHERE parent.relname = 'archive_waze'`,
    );
    if (parts.rows.length === 0) {
      console.log('No archive_waze partitions found. Treating as a non-partitioned table.');
      await runOne(pool, 'archive_waze');
    } else {
      console.log(`Found ${parts.rows.length} partition(s):`);
      for (const r of parts.rows) console.log(`  - ${r.partition}`);
      console.log('');
      for (const r of parts.rows) {
        await runOne(pool, r.partition);
      }
    }

    // Final ANALYZE on the parent so planner stats are fresh.
    console.log('\n[analyze] archive_waze (parent)');
    const t0 = Date.now();
    await pool.query('ANALYZE archive_waze');
    console.log(`  ✓ ${Date.now() - t0} ms`);

    console.log('\n✓ recovery complete');
  } finally {
    await pool.end();
  }
}

async function runOne(pool, table) {
  console.log(`\n[${table}]`);
  // Stats before.
  const before = await pool.query(
    `SELECT n_live_tup, n_dead_tup, pg_size_pretty(pg_total_relation_size($1::regclass)) AS size
     FROM pg_stat_user_tables WHERE relname = $1`,
    [table],
  );
  const b = before.rows[0];
  if (b) {
    console.log(`  before: ${b.n_live_tup} live, ${b.n_dead_tup} dead, ${b.size} on disk`);
  }

  // VACUUM FULL — rewrites the table.
  let t0 = Date.now();
  try {
    await pool.query(`VACUUM (FULL, ANALYZE) ${table}`);
    console.log(`  ✓ VACUUM FULL (${Date.now() - t0} ms)`);
  } catch (err) {
    console.error(`  ✗ VACUUM FULL failed: ${err.message}`);
    throw err;
  }

  // REINDEX — rebuilds indexes from the freshly-rewritten heap.
  t0 = Date.now();
  try {
    await pool.query(`REINDEX TABLE ${table}`);
    console.log(`  ✓ REINDEX (${Date.now() - t0} ms)`);
  } catch (err) {
    console.error(`  ✗ REINDEX failed: ${err.message}`);
    throw err;
  }

  // Stats after.
  const after = await pool.query(
    `SELECT n_live_tup, n_dead_tup, pg_size_pretty(pg_total_relation_size($1::regclass)) AS size
     FROM pg_stat_user_tables WHERE relname = $1`,
    [table],
  );
  const a = after.rows[0];
  if (a) {
    console.log(`  after:  ${a.n_live_tup} live, ${a.n_dead_tup} dead, ${a.size} on disk`);
  }
}

main().catch((err) => {
  console.error('vacuum-full-archive-waze failed:', err);
  process.exit(1);
});
