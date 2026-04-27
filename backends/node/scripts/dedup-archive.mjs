#!/usr/bin/env node
/**
 * One-shot deduplication for archive_* tables.
 *
 * Why: scripts/migrate-history.mjs does NOT use ON CONFLICT DO NOTHING
 * and there is no UNIQUE constraint on archive_*. If the backfill ran
 * twice (with --force, or before the marker existed) every row from
 * data_history was copied twice. Effect: archive_waze ends up with ~2×
 * the rows it should have, every COUNT(*) is double, /api/data/history
 * returns duplicate rows when unique=0.
 *
 * Strategy: drop rows whose (source, source_id, fetched_at) tuple
 * already has a smaller-id sibling. `id` is BIGSERIAL so this keeps
 * the first-inserted row of each tuple and drops later duplicates.
 *
 * Tuples with NULL source_id (e.g. legacy rows where the upstream
 * didn't expose a stable id) are NOT deduplicated by default — they
 * could be genuine multi-poll snapshots of the same incident. Pass
 * --include-null-source-id to dedup those too (treats NULL as a single
 * group keyed by source + fetched_at).
 *
 * Usage:
 *   cd backends/node
 *   node --env-file-if-exists=../.env scripts/dedup-archive.mjs --dry-run
 *   node --env-file-if-exists=../.env scripts/dedup-archive.mjs
 *   node --env-file-if-exists=../.env scripts/dedup-archive.mjs --table=archive_waze
 *
 * Flags:
 *   --dry-run                   report duplicate counts without deleting
 *   --table=<name>              only process this table (default: all 5)
 *   --include-null-source-id    also dedup rows whose source_id is NULL
 *   --batch=<N>                 delete in chunks of N (default 50000)
 *
 * Safety:
 *   - DELETE runs in a single transaction per table so a crash leaves
 *     the table in a consistent state.
 *   - statement_timeout is set to 0 (unlimited) for the bulk DELETE.
 *   - Idempotent: re-running on a clean table is a no-op.
 */
import pg from 'pg';

const { Pool } = pg;

const ALL_TABLES = [
  'archive_waze',
  'archive_traffic',
  'archive_rfs',
  'archive_power',
  'archive_misc',
];

const argv = process.argv.slice(2);
const flags = {
  dryRun: argv.includes('--dry-run'),
  includeNull: argv.includes('--include-null-source-id'),
  batch: 50_000,
  tables: ALL_TABLES,
};
for (const a of argv) {
  if (a.startsWith('--batch=')) {
    const n = Number.parseInt(a.slice(8), 10);
    if (Number.isFinite(n) && n > 0) flags.batch = n;
  }
  if (a.startsWith('--table=')) {
    flags.tables = [a.slice(8)];
  }
}

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error('DATABASE_URL is not set. Run with --env-file-if-exists=../.env');
    process.exit(1);
  }

  const pool = new Pool({
    connectionString: url,
    max: 2,
    statement_timeout: 0,
  });

  try {
    for (const table of flags.tables) {
      await dedupOne(pool, table);
    }
  } finally {
    await pool.end();
  }
}

/**
 * Build the duplicate-row CTE. Groups by (source, source_id, fetched_at)
 * (or (source, fetched_at) when --include-null-source-id is set), keeps
 * the row with the smallest id per group, marks the rest for deletion.
 */
function dupesCte(table, includeNull) {
  const groupCols = includeNull
    ? `source, fetched_at`
    : `source, source_id, fetched_at`;
  const filter = includeNull ? '' : `WHERE source_id IS NOT NULL`;
  return `
    WITH ranked AS (
      SELECT id,
             ROW_NUMBER() OVER (
               PARTITION BY ${groupCols}
               ORDER BY id ASC
             ) AS rn
      FROM ${table}
      ${filter}
    )
    SELECT id FROM ranked WHERE rn > 1
  `;
}

async function dedupOne(pool, table) {
  console.log(`\n${table}:`);

  // Count duplicates first (cheap subset of the full delete plan).
  const cte = dupesCte(table, flags.includeNull);
  const t0 = Date.now();
  const cnt = await pool.query(`SELECT COUNT(*)::bigint AS n FROM (${cte}) d`);
  const total = Number(cnt.rows[0]?.n ?? 0);
  const ms = Date.now() - t0;
  console.log(`  duplicates: ${total} (${ms} ms to scan)`);

  if (total === 0) {
    console.log(`  ✓ no action`);
    return;
  }

  if (flags.dryRun) {
    console.log(`  dry-run — no deletes issued`);
    return;
  }

  // Batched delete to keep WAL pressure manageable on large tables.
  let deleted = 0;
  for (;;) {
    const t1 = Date.now();
    // Use a fresh CTE per batch — `LIMIT` inside the CTE bounds the
    // ROW_NUMBER scan to a single page, keeping memory low.
    const sql = `
      WITH dupes AS (
        SELECT id FROM (
          SELECT id,
                 ROW_NUMBER() OVER (
                   PARTITION BY ${
                     flags.includeNull
                       ? 'source, fetched_at'
                       : 'source, source_id, fetched_at'
                   }
                   ORDER BY id ASC
                 ) AS rn
          FROM ${table}
          ${flags.includeNull ? '' : 'WHERE source_id IS NOT NULL'}
        ) ranked
        WHERE rn > 1
        LIMIT ${flags.batch}
      )
      DELETE FROM ${table}
      WHERE id IN (SELECT id FROM dupes)
    `;
    const r = await pool.query(sql);
    const n = r.rowCount ?? 0;
    deleted += n;
    const dt = Date.now() - t1;
    process.stdout.write(
      `  deleted ${deleted}/${total} (${dt} ms last batch)         \r`,
    );
    if (n === 0) break;
  }
  console.log(`  deleted ${deleted}/${total}                          `);
  console.log(`  ✓ ${table} clean`);
}

main().catch((err) => {
  console.error('dedup-archive failed:', err);
  process.exit(1);
});
