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
 * --include-null-source-id to dedup those too, in a SEPARATE pass keyed
 * by (source, fetched_at, md5(data)) — the content hash means only
 * byte-identical rows (the double-backfill signature) collapse; N
 * distinct NULL-id incidents that share one poll's fetched_at are left
 * alone. (An earlier version applied the NULL grouping to EVERY row,
 * which would have deleted N-1 of every poll snapshot's genuine,
 * distinct incidents across the whole table.)
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
 *   - DELETEs run as independent autocommit batches (--batch=N rows per
 *     statement). Each batch is atomic, but a crash mid-run leaves
 *     earlier batches committed — safe here because every batch removes
 *     only rows whose keeper sibling provably exists; just re-run to
 *     finish the remainder.
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
    const t = a.slice(8);
    // Validate against the allowlist before it reaches any SQL string —
    // table names are interpolated directly (can't be parameterized).
    if (!ALL_TABLES.includes(t)) {
      console.error(
        `Unknown --table=${t}. Must be one of: ${ALL_TABLES.join(', ')}`,
      );
      process.exit(1);
    }
    flags.tables = [t];
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
 * Dedup passes. Each pass scopes its scan with `filter` and groups by
 * `groupCols`; the smallest-id row per group is kept, the rest deleted.
 *
 *   notnull — rows WITH a stable id: (source, source_id, fetched_at).
 *             The double-backfill signature exactly.
 *   null    — rows WITHOUT one (only with --include-null-source-id):
 *             (source, fetched_at, md5(data::text)). The content hash is
 *             essential — one poll snapshot stamps every incident with
 *             the same fetched_at, so keying on (source, fetched_at)
 *             alone would collapse N distinct incidents into one.
 */
function dedupPasses() {
  const passes = [
    {
      name: 'source_id rows',
      groupCols: 'source, source_id, fetched_at',
      filter: 'WHERE source_id IS NOT NULL',
    },
  ];
  if (flags.includeNull) {
    passes.push({
      name: 'NULL-source_id rows (content-identical only)',
      groupCols: 'source, fetched_at, md5(data::text)',
      filter: 'WHERE source_id IS NULL',
    });
  }
  return passes;
}

function dupesCte(table, pass) {
  return `
    WITH ranked AS (
      SELECT id,
             ROW_NUMBER() OVER (
               PARTITION BY ${pass.groupCols}
               ORDER BY id ASC
             ) AS rn
      FROM ${table}
      ${pass.filter}
    )
    SELECT id FROM ranked WHERE rn > 1
  `;
}

async function dedupOne(pool, table) {
  console.log(`\n${table}:`);

  for (const pass of dedupPasses()) {
    // Count duplicates first (cheap subset of the full delete plan).
    const cte = dupesCte(table, pass);
    const t0 = Date.now();
    const cnt = await pool.query(`SELECT COUNT(*)::bigint AS n FROM (${cte}) d`);
    const total = Number(cnt.rows[0]?.n ?? 0);
    const ms = Date.now() - t0;
    console.log(`  [${pass.name}] duplicates: ${total} (${ms} ms to scan)`);

    if (total === 0) {
      console.log(`  ✓ no action`);
      continue;
    }

    if (flags.dryRun) {
      console.log(`  dry-run — no deletes issued`);
      continue;
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
                     PARTITION BY ${pass.groupCols}
                     ORDER BY id ASC
                   ) AS rn
            FROM ${table}
            ${pass.filter}
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
  }
  console.log(`  ✓ ${table} clean`);
}

main().catch((err) => {
  console.error('dedup-archive failed:', err);
  process.exit(1);
});
