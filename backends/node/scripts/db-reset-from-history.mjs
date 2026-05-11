#!/usr/bin/env node
/**
 * Nuclear option: TRUNCATE archive_* and re-migrate from python's
 * data_history table. Use when the archive_* tables are too bloated /
 * duplicated to recover via dedup+vacuum.
 *
 * ⚠ WARNING — DATA LOSS
 * This DELETES everything in archive_waze / archive_traffic /
 * archive_rfs / archive_power / archive_misc, including:
 *   - Userscript-ingested Waze data since cutover (`/api/waze/ingest`)
 *   - Any incidents Node polled and archived after data_history froze
 *
 * data_history (python's table) is the authoritative source for what
 * gets re-migrated. Anything Node wrote that python didn't is lost.
 *
 * Steps:
 *   1. TRUNCATE the 5 archive_* tables (instant on empty / fast on full).
 *   2. Clear the `999_data_history_backfill` row from schema_migrations
 *      so the backfill script will run again.
 *   3. Spawn migrate-history.mjs to re-copy data_history → archive_*.
 *   4. ANALYZE the freshly populated tables.
 *
 * Usage (Node service MUST be stopped):
 *   pm2 stop api-node
 *   cd /var/www/nswpsn/backends/node
 *   node --env-file-if-exists=../.env scripts/db-reset-from-history.mjs --confirm
 *   pm2 start api-node
 *
 * Without --confirm the script just prints what it WOULD do.
 *
 * Total runtime: dominated by the migrate-history.mjs pass; depends on
 * data_history size. ~10-20 min for ~5M rows.
 */
import pg from 'pg';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const { Pool } = pg;
const argv = process.argv.slice(2);
const confirmed = argv.includes('--confirm');

const ARCHIVE_TABLES = [
  'archive_waze',
  'archive_traffic',
  'archive_rfs',
  'archive_power',
  'archive_misc',
];

const __dirname = dirname(fileURLToPath(import.meta.url));

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error('DATABASE_URL not set. Run with --env-file-if-exists=../.env');
    process.exit(1);
  }

  console.log('=== DB reset from history ===');
  console.log('This will TRUNCATE the archive_* tables and re-migrate');
  console.log('from data_history. Anything Node wrote since the original');
  console.log('cutover is gone forever.');
  console.log('');

  const pool = new Pool({
    connectionString: url,
    max: 1,
    statement_timeout: 0,
  });

  try {
    // Pre-flight: confirm data_history exists and has rows.
    const dh = await pool.query(
      `SELECT to_regclass('public.data_history') AS reg`,
    );
    if (!dh.rows[0]?.reg) {
      console.error('data_history table not found. Aborting (nothing to migrate from).');
      process.exit(2);
    }
    const dhCount = await pool.query(
      `SELECT COUNT(*)::bigint AS n FROM data_history`,
    );
    const dhRows = Number(dhCount.rows[0]?.n ?? 0);
    console.log(`data_history rows: ${dhRows.toLocaleString()}`);
    if (dhRows === 0) {
      console.error('data_history is empty — refusing to truncate archive tables.');
      process.exit(3);
    }

    // Show current archive state.
    console.log('\nCurrent archive_* state:');
    for (const t of ARCHIVE_TABLES) {
      const r = await pool.query(`SELECT COUNT(*)::bigint AS n FROM ${t}`);
      const n = Number(r.rows[0]?.n ?? 0);
      console.log(`  ${t}: ${n.toLocaleString()} rows`);
    }

    if (!confirmed) {
      console.log('\nDry-run (no --confirm). Re-run with --confirm to execute.');
      return;
    }

    // 1. TRUNCATE.
    console.log('\n[truncate]');
    for (const t of ARCHIVE_TABLES) {
      const t0 = Date.now();
      // CASCADE in case anything else references it (shouldn't be any
      // FKs but defensive). RESTART IDENTITY resets the BIGSERIAL ids.
      await pool.query(`TRUNCATE ${t} RESTART IDENTITY CASCADE`);
      console.log(`  ✓ ${t} (${Date.now() - t0} ms)`);
    }

    // 2. Clear the backfill marker so migrate-history.mjs runs.
    await pool.query(
      `DELETE FROM schema_migrations WHERE filename = '999_data_history_backfill'`,
    );
    console.log(`\n[marker] cleared 999_data_history_backfill`);

    // 3. Spawn migrate-history.mjs as a child process. Streams its
    // output through so the user sees per-batch progress.
    console.log('\n[migrate-history] spawning…');
    const scriptPath = join(__dirname, 'migrate-history.mjs');
    await new Promise((resolve, reject) => {
      const child = spawn(
        'node',
        ['--env-file-if-exists=../.env', scriptPath],
        { stdio: 'inherit' },
      );
      child.on('exit', (code) =>
        code === 0
          ? resolve()
          : reject(new Error(`migrate-history exited with code ${code}`)),
      );
      child.on('error', reject);
    });

    // 4. ANALYZE for fresh stats.
    console.log('\n[analyze]');
    for (const t of ARCHIVE_TABLES) {
      const t0 = Date.now();
      await pool.query(`ANALYZE ${t}`);
      console.log(`  ✓ ${t} (${Date.now() - t0} ms)`);
    }

    // 5. Final state.
    console.log('\nFinal archive_* state:');
    for (const t of ARCHIVE_TABLES) {
      const r = await pool.query(`SELECT COUNT(*)::bigint AS n FROM ${t}`);
      const n = Number(r.rows[0]?.n ?? 0);
      console.log(`  ${t}: ${n.toLocaleString()} rows`);
    }

    console.log('\n✓ reset complete');
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error('db-reset-from-history failed:', err);
  process.exit(1);
});
