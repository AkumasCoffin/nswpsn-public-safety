#!/usr/bin/env node
/**
 * Emergency: VACUUM FULL + REINDEX every archive_* parent.
 *
 * The is_latest experiment (migration 012, reverted in 013) added
 * is_latest + is_live columns to ALL archive_* tables and ran a bulk
 * UPDATE on each — which is why production logs show statement
 * timeouts on archive_traffic / archive_rfs / archive_power /
 * archive_misc as well as archive_waze. Migration 013 dropped the
 * columns but only marks them logically dropped; the heap still
 * carries the dead tuples + dropped-column slots until rewritten.
 *
 * Plain VACUUM (autovacuum or our admin endpoint) reclaims dead
 * tuples but doesn't shrink the heap on disk. VACUUM FULL rewrites
 * each table compactly + REINDEX rebuilds every index from the
 * fresh heap.
 *
 * Trade-off: VACUUM FULL takes ACCESS EXCLUSIVE lock — INSERTs and
 * SELECTs both block during the rewrite. Run with the Node service
 * stopped:
 *
 *   pm2 stop api-node
 *   cd /var/www/nswpsn/backends/node
 *   node --env-file-if-exists=../.env scripts/vacuum-full-archive-tables.mjs
 *   pm2 start api-node
 *
 * Runtime depends on table size + disk speed. archive_waze is the
 * heaviest (1-5 min per partition on SSD). The smaller tables
 * (rfs / power / misc / traffic) finish in seconds each.
 *
 * Iterates per-partition because VACUUM FULL on a partitioned parent
 * isn't supported on PG14; partition-by-partition is the portable
 * way and lets us print progress instead of one long blocking call.
 */
import pg from 'pg';

const { Pool } = pg;

const PARENTS = [
  'archive_waze',
  'archive_traffic',
  'archive_rfs',
  'archive_power',
  'archive_misc',
];

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

  console.log('=== archive_* emergency recovery ===');
  console.log('VACUUM FULL + REINDEX on every partition of every archive_* table.');
  console.log('Make sure the Node service is stopped.\n');

  const overallStart = Date.now();
  try {
    for (const parent of PARENTS) {
      console.log(`\n=== ${parent} ===`);
      const parts = await pool.query(
        `SELECT child.relname::text AS partition
         FROM pg_inherits
         JOIN pg_class child ON child.oid = pg_inherits.inhrelid
         JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
         WHERE parent.relname = $1
         ORDER BY child.relname`,
        [parent],
      );
      if (parts.rows.length === 0) {
        console.log(`  (no partitions found — treating as plain table)`);
        await runOne(pool, parent);
      } else {
        console.log(`  ${parts.rows.length} partition(s)`);
        for (const r of parts.rows) {
          await runOne(pool, r.partition);
        }
      }
      // ANALYZE the parent so planner stats are fresh after the rewrite.
      console.log(`  [analyze] ${parent} (parent)`);
      const t0 = Date.now();
      await pool.query(`ANALYZE ${parent}`);
      console.log(`    ${Date.now() - t0} ms`);
    }
    console.log(`\nrecovery complete in ${Math.round((Date.now() - overallStart) / 1000)}s`);
  } finally {
    await pool.end();
  }
}

async function runOne(pool, table) {
  console.log(`\n  [${table}]`);
  const before = await pool.query(
    `SELECT n_live_tup, n_dead_tup, pg_size_pretty(pg_total_relation_size($1::regclass)) AS size
     FROM pg_stat_user_tables WHERE relname = $1`,
    [table],
  );
  const b = before.rows[0];
  if (b) {
    console.log(`    before: ${b.n_live_tup} live, ${b.n_dead_tup} dead, ${b.size} on disk`);
  }

  let t0 = Date.now();
  try {
    await pool.query(`VACUUM (FULL, ANALYZE) ${table}`);
    console.log(`    VACUUM FULL ${Date.now() - t0} ms`);
  } catch (err) {
    console.error(`    VACUUM FULL failed: ${err.message}`);
    throw err;
  }

  t0 = Date.now();
  try {
    await pool.query(`REINDEX TABLE ${table}`);
    console.log(`    REINDEX ${Date.now() - t0} ms`);
  } catch (err) {
    console.error(`    REINDEX failed: ${err.message}`);
    throw err;
  }

  const after = await pool.query(
    `SELECT n_live_tup, n_dead_tup, pg_size_pretty(pg_total_relation_size($1::regclass)) AS size
     FROM pg_stat_user_tables WHERE relname = $1`,
    [table],
  );
  const a = after.rows[0];
  if (a) {
    console.log(`    after:  ${a.n_live_tup} live, ${a.n_dead_tup} dead, ${a.size} on disk`);
  }
}

main().catch((err) => {
  console.error('vacuum-full-archive-tables failed:', err);
  process.exit(1);
});
