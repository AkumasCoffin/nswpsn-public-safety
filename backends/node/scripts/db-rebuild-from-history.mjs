#!/usr/bin/env node
/**
 * Nuclear+: DROP every archive_* table, recreate them clean, then
 * backfill from python's data_history.
 *
 * Difference from db-reset-from-history.mjs (which TRUNCATEs):
 *   - DROP CASCADE wipes the parents AND every partition + index +
 *     stored autovacuum settings. The recreated tables are pristine —
 *     no leftover bloat, no orphan indexes, no autovacuum state.
 *   - Use this when one or more archive_* tables are corrupt, missing,
 *     or have schema drift you can't reason about anymore. (Production
 *     hit this with archive_waze: doubled row count from the is_latest
 *     re-queue cascade made VACUUM FULL impractical.)
 *   - TRUNCATE preserves the schema; this one starts from scratch.
 *
 * The SQL inlined below is the union of every archive-creating
 * migration (002 base table + 005 src_ts index + 010 autovacuum + 014
 * waze recreate logic). Idempotent — re-running on a healthy DB does
 * nothing destructive after the DROP step (which only drops what
 * exists; missing tables are skipped).
 *
 * After recreation we DELETE the 999_data_history_backfill marker
 * from schema_migrations and spawn migrate-history.mjs to copy rows
 * from data_history into the right archive_<family> table per source.
 *
 * ⚠ DATA LOSS — anything Node wrote since the original cutover that
 * isn't in data_history is gone. Userscript Waze ingest, anything
 * archived after data_history froze, etc. data_history is the only
 * source we can rebuild from.
 *
 * Usage (Node service MUST be stopped):
 *   pm2 stop api-node
 *   cd /var/www/nswpsn/backends/node
 *   node --env-file-if-exists=../.env scripts/db-rebuild-from-history.mjs --confirm
 *   pm2 start api-node
 *
 * Without --confirm the script reports what it would do and exits.
 * Total runtime: drop is fast, recreate is fast, migrate-history
 * dominates (~10-20 min for ~5M data_history rows).
 */
import pg from 'pg';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const { Pool } = pg;
const argv = process.argv.slice(2);
const confirmed = argv.includes('--confirm');
const skipMigrate = argv.includes('--no-migrate'); // schema-only rebuild
const __dirname = dirname(fileURLToPath(import.meta.url));

const FAMILIES = ['waze', 'traffic', 'rfs', 'power', 'misc'];
const PARENTS = FAMILIES.map((f) => `archive_${f}`);

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error('DATABASE_URL not set. Run with --env-file-if-exists=../.env');
    process.exit(1);
  }

  console.log('=== DB rebuild from history ===');
  console.log('DROPs every archive_* parent + partitions, recreates the schema,');
  console.log('then backfills from data_history. data_history is the source of');
  console.log('truth — anything Node wrote since the cutover that data_history');
  console.log('does not have is permanently gone.\n');

  const pool = new Pool({
    connectionString: url,
    max: 1,
    statement_timeout: 0,
    idleTimeoutMillis: 0,
  });

  try {
    // Pre-flight: confirm data_history exists and has rows (unless
    // --no-migrate, in which case we're OK with no source data).
    if (!skipMigrate) {
      const dh = await pool.query(
        `SELECT to_regclass('public.data_history') AS reg`,
      );
      if (!dh.rows[0]?.reg) {
        console.error('data_history not found. Aborting (nothing to migrate from).');
        console.error('Pass --no-migrate to rebuild the schema only.');
        process.exit(2);
      }
      const dhCount = await pool.query(
        `SELECT COUNT(*)::bigint AS n FROM data_history`,
      );
      const dhRows = Number(dhCount.rows[0]?.n ?? 0);
      console.log(`data_history rows: ${dhRows.toLocaleString()}`);
      if (dhRows === 0) {
        console.error('data_history is empty — refusing to drop the archive tables.');
        console.error('Pass --no-migrate to rebuild the schema only.');
        process.exit(3);
      }
    } else {
      console.log('--no-migrate: schema-only rebuild, data_history will not be touched.');
    }

    // Show current archive state.
    console.log('\nCurrent archive_* state:');
    for (const t of PARENTS) {
      const r = await pool.query(
        `SELECT to_regclass($1::text) AS reg`,
        [`public.${t}`],
      );
      if (!r.rows[0]?.reg) {
        console.log(`  ${t}: MISSING`);
        continue;
      }
      const c = await pool.query(`SELECT COUNT(*)::bigint AS n FROM ${t}`);
      const n = Number(c.rows[0]?.n ?? 0);
      console.log(`  ${t}: ${n.toLocaleString()} rows`);
    }

    if (!confirmed) {
      console.log('\nDry-run (no --confirm). Re-run with --confirm to execute.');
      return;
    }

    // 1. DROP every parent (CASCADE removes partitions + indexes).
    console.log('\n[drop]');
    for (const t of PARENTS) {
      const t0 = Date.now();
      await pool.query(`DROP TABLE IF EXISTS ${t} CASCADE`);
      console.log(`  dropped ${t} (${Date.now() - t0} ms)`);
    }

    // 2. Recreate every parent + per-family indexes + seed two months
    //    of partitions. ensure_archive_partition is defined by
    //    migration 002/010 — its CREATE OR REPLACE means it's still in
    //    the catalog after we drop the parents (functions live in pg_proc,
    //    not pg_class). If for some reason it's missing, recreate it.
    console.log('\n[recreate]');
    await pool.query(`
      CREATE OR REPLACE FUNCTION ensure_archive_partition(
        parent_table TEXT,
        for_date DATE
      ) RETURNS TEXT AS $fn$
      DECLARE
        start_date DATE := date_trunc('month', for_date)::date;
        end_date   DATE := (date_trunc('month', for_date) + INTERVAL '1 month')::date;
        part_name  TEXT := parent_table || '_' || to_char(start_date, 'YYYY_MM');
      BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
          EXECUTE format(
            'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
            part_name, parent_table, start_date, end_date
          );
          IF parent_table = 'archive_waze' THEN
            EXECUTE format(
              'ALTER TABLE %I SET (
                 autovacuum_vacuum_scale_factor = 0.02,
                 autovacuum_vacuum_threshold = 1000,
                 autovacuum_vacuum_cost_limit = 2000,
                 autovacuum_vacuum_cost_delay = 2,
                 autovacuum_analyze_scale_factor = 0.05
               )', part_name);
          ELSIF parent_table IN ('archive_traffic','archive_misc') THEN
            EXECUTE format(
              'ALTER TABLE %I SET (
                 autovacuum_vacuum_scale_factor = 0.1,
                 autovacuum_vacuum_cost_limit = 500
               )', part_name);
          END IF;
        END IF;
        RETURN part_name;
      END;
      $fn$ LANGUAGE plpgsql;
    `);
    for (const fam of FAMILIES) {
      const parent = `archive_${fam}`;
      const t0 = Date.now();
      await pool.query(`
        CREATE TABLE ${parent} (
          id          BIGSERIAL    NOT NULL,
          source      TEXT         NOT NULL,
          source_id   TEXT,
          fetched_at  TIMESTAMPTZ  NOT NULL,
          lat         DOUBLE PRECISION,
          lng         DOUBLE PRECISION,
          category    TEXT,
          subcategory TEXT,
          data        JSONB        NOT NULL,
          PRIMARY KEY (id, fetched_at)
        ) PARTITION BY RANGE (fetched_at)
      `);
      await pool.query(`
        CREATE INDEX idx_${parent}_src_sid_ts
          ON ${parent} (source, source_id, fetched_at DESC)
      `);
      await pool.query(`
        CREATE INDEX idx_${parent}_ts
          ON ${parent} (fetched_at DESC)
      `);
      await pool.query(`
        CREATE INDEX idx_${parent}_src_ts
          ON ${parent} (source, fetched_at DESC)
      `);
      // Seed current + next month so the writer has somewhere to write
      // immediately after restart. migrate-history.mjs's ensurePartitions
      // pass adds whatever older months data_history needs per family.
      await pool.query(
        `SELECT ensure_archive_partition($1, date_trunc('month', now())::date)`,
        [parent],
      );
      await pool.query(
        `SELECT ensure_archive_partition($1, (date_trunc('month', now()) + INTERVAL '1 month')::date)`,
        [parent],
      );
      console.log(`  ${parent} (${Date.now() - t0} ms)`);
    }

    if (skipMigrate) {
      console.log('\n--no-migrate: skipping data_history backfill.');
    } else {
      // 3. Clear the backfill marker.
      await pool.query(
        `DELETE FROM schema_migrations WHERE filename = '999_data_history_backfill'`,
      );
      console.log(`\n[marker] cleared 999_data_history_backfill`);

      // 4. Spawn migrate-history.mjs.
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
    }

    // 5. ANALYZE for fresh stats so the planner picks the new indexes.
    console.log('\n[analyze]');
    for (const t of PARENTS) {
      const t0 = Date.now();
      await pool.query(`ANALYZE ${t}`);
      console.log(`  ${t} (${Date.now() - t0} ms)`);
    }

    // 6. Final state.
    console.log('\nFinal archive_* state:');
    for (const t of PARENTS) {
      const c = await pool.query(`SELECT COUNT(*)::bigint AS n FROM ${t}`);
      const n = Number(c.rows[0]?.n ?? 0);
      console.log(`  ${t}: ${n.toLocaleString()} rows`);
    }

    console.log('\nrebuild complete');
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error('db-rebuild-from-history failed:', err);
  process.exit(1);
});
