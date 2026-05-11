#!/usr/bin/env node
/**
 * One-shot database recovery: dedup, vacuum, reindex, analyze.
 *
 * Run this with the Node service STOPPED so writes can't fight the
 * recovery. Each step prints progress so you can monitor + bail out
 * with Ctrl-C if needed.
 *
 * Steps:
 *   1. Dedup each archive_* table by (source, source_id, fetched_at).
 *      Mirrors `dedup-archive.mjs --include-null-source-id=false`.
 *   2. VACUUM (FULL, ANALYZE) — reclaims dead-tuple space and
 *      refreshes pg_statistic. FULL is heavy (rewrites the table)
 *      but necessary after 29k+ row deletes; otherwise the bloated
 *      heap keeps every scan slow.
 *   3. REINDEX TABLE — rebuilds bloated indexes from the dedup-deleted
 *      rows so the planner picks them.
 *
 * Usage:
 *   pm2 stop api-node
 *   cd /var/www/nswpsn/backends/node
 *   node --env-file-if-exists=../.env scripts/db-recover.mjs
 *   pm2 start api-node
 *
 * Flags:
 *   --skip-dedup        skip step 1 (already deduped)
 *   --skip-vacuum-full  skip VACUUM FULL (do plain VACUUM only — faster
 *                       but doesn't reclaim disk space)
 *   --skip-reindex      skip step 3
 *   --table=<name>      only process this table (default: all 5)
 *
 * Heads-up:
 *   - VACUUM FULL takes an ACCESS EXCLUSIVE lock on each table; nothing
 *     else can read or write it during the rewrite. Acceptable here
 *     because the Node service should be stopped.
 *   - Total runtime on a multi-million-row archive_waze: 5-30 minutes.
 */
import pg from 'pg';

const { Pool } = pg;

const ALL_TABLES = [
  'archive_misc',
  'archive_rfs',
  'archive_power',
  'archive_traffic',
  'archive_waze',
];

const argv = process.argv.slice(2);
const flags = {
  skipDedup: argv.includes('--skip-dedup'),
  skipVacuumFull: argv.includes('--skip-vacuum-full'),
  skipReindex: argv.includes('--skip-reindex'),
  tables: ALL_TABLES,
};
for (const a of argv) {
  if (a.startsWith('--table=')) flags.tables = [a.slice(8)];
}

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error('DATABASE_URL not set. Run with --env-file-if-exists=../.env');
    process.exit(1);
  }

  const pool = new Pool({
    connectionString: url,
    max: 1, // serialise everything
    statement_timeout: 0,
    idleTimeoutMillis: 0,
  });

  console.log('=== DB recovery ===');
  console.log(`tables: ${flags.tables.join(', ')}`);
  console.log(
    `steps: ${[
      !flags.skipDedup && 'dedup',
      'vacuum' + (flags.skipVacuumFull ? '' : '-full'),
      !flags.skipReindex && 'reindex',
      'analyze',
    ]
      .filter(Boolean)
      .join(', ')}`,
  );
  console.log('');

  try {
    if (!flags.skipDedup) {
      for (const t of flags.tables) await dedup(pool, t);
    }
    for (const t of flags.tables) {
      await vacuum(pool, t, !flags.skipVacuumFull);
    }
    if (!flags.skipReindex) {
      for (const t of flags.tables) await reindex(pool, t);
    }
    for (const t of flags.tables) await analyze(pool, t);
    console.log('\n✓ recovery complete');
  } finally {
    await pool.end();
  }
}

async function dedup(pool, table) {
  console.log(`\n[dedup] ${table}`);
  const cte = `
    SELECT id FROM (
      SELECT id, ROW_NUMBER() OVER (
        PARTITION BY source, source_id, fetched_at ORDER BY id ASC
      ) AS rn
      FROM ${table}
      WHERE source_id IS NOT NULL
    ) t WHERE rn > 1
  `;
  const t0 = Date.now();
  const cnt = await pool.query(`SELECT COUNT(*)::bigint AS n FROM (${cte}) d`);
  const total = Number(cnt.rows[0]?.n ?? 0);
  console.log(`  duplicates: ${total} (${Date.now() - t0} ms scan)`);
  if (total === 0) {
    console.log('  ✓ clean');
    return;
  }

  // Batched delete to keep WAL pressure manageable.
  const BATCH = 50_000;
  let deleted = 0;
  for (;;) {
    const t1 = Date.now();
    const r = await pool.query(`
      WITH dupes AS (
        SELECT id FROM (
          SELECT id, ROW_NUMBER() OVER (
            PARTITION BY source, source_id, fetched_at ORDER BY id ASC
          ) AS rn
          FROM ${table}
          WHERE source_id IS NOT NULL
        ) t WHERE rn > 1
        LIMIT ${BATCH}
      )
      DELETE FROM ${table} WHERE id IN (SELECT id FROM dupes)
    `);
    const n = r.rowCount ?? 0;
    deleted += n;
    process.stdout.write(
      `  deleted ${deleted}/${total} (${Date.now() - t1} ms last batch)         \r`,
    );
    if (n === 0) break;
  }
  console.log(`  deleted ${deleted}/${total}                          `);
}

async function vacuum(pool, table, full) {
  const cmd = full ? `VACUUM (FULL, ANALYZE) ${table}` : `VACUUM (ANALYZE) ${table}`;
  console.log(`\n[vacuum${full ? '-full' : ''}] ${table}`);
  const t0 = Date.now();
  try {
    await pool.query(cmd);
    console.log(`  ✓ ${table} (${Date.now() - t0} ms)`);
  } catch (err) {
    // FULL on a partitioned parent isn't supported in older Postgres.
    // Fall back to per-partition.
    if (full && /cannot vacuum.*partitioned/i.test(err.message)) {
      console.log(`  parent unsupported — vacuuming partitions individually`);
      const parts = await pool.query(
        `SELECT inhrelid::regclass::text AS p
         FROM pg_inherits WHERE inhparent = $1::regclass`,
        [table],
      );
      for (const r of parts.rows) {
        const p0 = Date.now();
        await pool.query(`VACUUM (FULL, ANALYZE) ${r.p}`);
        console.log(`    ✓ ${r.p} (${Date.now() - p0} ms)`);
      }
    } else {
      throw err;
    }
  }
}

async function reindex(pool, table) {
  console.log(`\n[reindex] ${table}`);
  const t0 = Date.now();
  try {
    await pool.query(`REINDEX TABLE ${table}`);
    console.log(`  ✓ ${table} (${Date.now() - t0} ms)`);
  } catch (err) {
    if (/cannot reindex.*partitioned/i.test(err.message)) {
      console.log(`  parent unsupported — reindexing partitions`);
      const parts = await pool.query(
        `SELECT inhrelid::regclass::text AS p
         FROM pg_inherits WHERE inhparent = $1::regclass`,
        [table],
      );
      for (const r of parts.rows) {
        const p0 = Date.now();
        await pool.query(`REINDEX TABLE ${r.p}`);
        console.log(`    ✓ ${r.p} (${Date.now() - p0} ms)`);
      }
    } else {
      throw err;
    }
  }
}

async function analyze(pool, table) {
  // Just to be sure post-reindex.
  const t0 = Date.now();
  await pool.query(`ANALYZE ${table}`);
  console.log(`[analyze] ${table} ✓ (${Date.now() - t0} ms)`);
}

main().catch((err) => {
  console.error('db-recover failed:', err);
  process.exit(1);
});
