#!/usr/bin/env node
/**
 * One-shot backfill from python's `data_history` table into the new
 * partitioned `archive_*` tables.
 *
 * Why: python wrote one row per incident to `data_history` with
 * top-level columns (title, severity, status, location_text, latitude,
 * longitude, etc.) plus the original feature blob in `data`. Node now
 * writes to the family-partitioned `archive_waze`, `archive_traffic`,
 * `archive_rfs`, `archive_power`, `archive_misc` — same shape, different
 * tables. Without this script, /api/data/history on Node only sees
 * data since the cutover; everything python collected is invisible.
 *
 * Idempotent: on each run we read the marker row in `schema_migrations`
 * for this script's id. If present, we exit immediately. Use --force
 * to re-run despite a previous completion (useful if you need to
 * backfill again after wiping archive_*).
 *
 * Usage:
 *   cd backends/node
 *   node --env-file-if-exists=../.env scripts/migrate-history.mjs
 *
 * Flags:
 *   --force           re-run even if the marker exists
 *   --batch=<N>       rows per family per round-trip (default 50000)
 *   --dry-run         report counts without inserting
 *   --since=YYYY-MM-DD  only rows with fetched_at >= midnight UTC
 *
 * Source-to-family mapping mirrors src/services/dataHistoryQuery.ts so
 * /api/data/history finds the backfilled rows in the same family the
 * Node poller would have written them to.
 */
import pg from 'pg';

const { Pool } = pg;

const MARKER = '999_data_history_backfill';

const SOURCE_TO_FAMILY = {
  // Waze
  waze_hazard: 'archive_waze',
  waze_jam: 'archive_waze',
  waze_police: 'archive_waze',
  waze_roadwork: 'archive_waze',
  // LiveTraffic NSW
  traffic_incident: 'archive_traffic',
  traffic_roadwork: 'archive_traffic',
  traffic_flood: 'archive_traffic',
  traffic_fire: 'archive_traffic',
  traffic_majorevent: 'archive_traffic',
  livetraffic: 'archive_traffic',
  // RFS
  rfs: 'archive_rfs',
  // Power
  endeavour: 'archive_power',
  endeavour_current: 'archive_power',
  endeavour_planned: 'archive_power',
  endeavour_maintenance: 'archive_power',
  ausgrid: 'archive_power',
  essential: 'archive_power',
  essential_current: 'archive_power',
  essential_planned: 'archive_power',
  essential_future: 'archive_power',
  essential_energy_cancelled: 'archive_power',
  // Misc
  bom: 'archive_misc',
  bom_warning: 'archive_misc',
  bom_land: 'archive_misc',
  bom_marine: 'archive_misc',
  beach: 'archive_misc',
  beachsafe: 'archive_misc',
  beachsafe_details: 'archive_misc',
  beachwatch: 'archive_misc',
  weather: 'archive_misc',
  weather_current: 'archive_misc',
  weather_radar: 'archive_misc',
  pager: 'archive_misc',
  news: 'archive_misc',
  aviation: 'archive_misc',
  centralwatch: 'archive_misc',
};

// Group source names by destination family so we issue one migration
// query per family rather than per source.
function groupByFamily() {
  const out = {};
  for (const [source, family] of Object.entries(SOURCE_TO_FAMILY)) {
    if (!out[family]) out[family] = [];
    out[family].push(source);
  }
  return out;
}

const FAMILIES = groupByFamily();

// -----------------------------------------------------------------------------
// CLI flag parsing
// -----------------------------------------------------------------------------

const argv = process.argv.slice(2);
const flags = {
  force: argv.includes('--force'),
  dryRun: argv.includes('--dry-run'),
  batch: 50_000,
  since: null,
};
for (const a of argv) {
  if (a.startsWith('--batch=')) flags.batch = Number.parseInt(a.slice(8), 10);
  if (a.startsWith('--since=')) flags.since = a.slice(8);
}
if (!Number.isFinite(flags.batch) || flags.batch <= 0) flags.batch = 50_000;

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error('DATABASE_URL is not set. Did you run with --env-file-if-exists=../.env?');
    process.exit(1);
  }

  const pool = new Pool({
    connectionString: url,
    max: 4,
    statement_timeout: 0, // long bulk inserts; no per-statement cap
  });

  try {
    await ensureMarkerTable(pool);

    if (!flags.force && (await alreadyApplied(pool))) {
      console.log(
        `Backfill marker '${MARKER}' already in schema_migrations. ` +
          'Re-run with --force to redo.',
      );
      return;
    }

    const dataHistoryExists = await tableExists(pool, 'data_history');
    if (!dataHistoryExists) {
      console.log("Source table 'data_history' does not exist. Nothing to backfill.");
      if (!flags.dryRun) await markApplied(pool);
      return;
    }

    // Detect whether fetched_at is in seconds or milliseconds. Python
    // wasn't consistent across the codebase, so we sniff at the maximum.
    const unitDivisor = await detectFetchedAtUnit(pool);
    console.log(
      `data_history.fetched_at unit: ${unitDivisor === 1 ? 'seconds' : 'milliseconds'}`,
    );

    let grandTotal = 0;
    for (const [family, sources] of Object.entries(FAMILIES)) {
      const count = await migrateFamily(pool, family, sources, unitDivisor);
      grandTotal += count;
    }

    console.log(
      `\nBackfill complete: ${grandTotal} rows copied into archive_* tables.`,
    );
    if (!flags.dryRun) {
      await markApplied(pool);
      console.log(`Marker '${MARKER}' written to schema_migrations.`);
    }
  } finally {
    await pool.end();
  }
}

async function ensureMarkerTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
}

async function alreadyApplied(pool) {
  const r = await pool.query('SELECT 1 FROM schema_migrations WHERE filename = $1', [
    MARKER,
  ]);
  return r.rows.length > 0;
}

async function markApplied(pool) {
  await pool.query(
    'INSERT INTO schema_migrations (filename) VALUES ($1) ON CONFLICT DO NOTHING',
    [MARKER],
  );
}

async function tableExists(pool, name) {
  const r = await pool.query(
    `SELECT 1 FROM information_schema.tables WHERE table_name = $1 LIMIT 1`,
    [name],
  );
  return r.rows.length > 0;
}

async function detectFetchedAtUnit(pool) {
  const r = await pool.query('SELECT MAX(fetched_at) AS max FROM data_history');
  const max = Number(r.rows[0]?.max ?? 0);
  // Threshold ~1e11: any timestamp post-2001 in ms is > 1e12; in seconds < 2e9.
  // 1e11 is comfortably between, picks up either with no false-positive.
  return max > 1e11 ? 1000 : 1;
}

async function migrateFamily(pool, family, sources, unitDivisor) {
  // Count first so we can show progress.
  const countSql = buildCountSql(sources);
  const countParams = [...sources];
  if (flags.since) countParams.push(flags.since);
  const countRes = await pool.query(countSql, countParams);
  const total = Number(countRes.rows[0]?.n ?? 0);
  if (total === 0) {
    console.log(`  ${family}: no rows in data_history`);
    return 0;
  }
  if (flags.dryRun) {
    console.log(`  ${family}: ${total} rows would be copied (dry-run)`);
    return total;
  }

  console.log(`  ${family}: ${total} rows → copying in batches of ${flags.batch}…`);

  // Page by id ascending so resumed runs (after a crash) can start
  // from the last id by checking MAX(id) in archive_*. We don't bake
  // that resume into this script — for a clean re-run use --force on
  // an empty archive_* table.
  let copied = 0;
  let lastId = 0;
  for (;;) {
    const insertSql = buildInsertSql(family, sources, unitDivisor);
    const params = [...sources, lastId];
    if (flags.since) params.push(flags.since);
    params.push(flags.batch);
    const t0 = Date.now();
    const res = await pool.query(insertSql, params);
    const inserted = res.rowCount ?? 0;
    if (inserted === 0) break;
    copied += inserted;
    // Find the highest id we just touched to advance the cursor.
    const lastRes = await pool.query(
      `SELECT MAX(id) AS m FROM data_history WHERE id > $1 AND id <= $2`,
      [lastId, lastId + flags.batch * 5], // safety cap
    );
    const newLast = Number(lastRes.rows[0]?.m ?? 0);
    if (newLast <= lastId) break;
    lastId = newLast;
    const ms = Date.now() - t0;
    process.stdout.write(
      `    ${family}: ${copied}/${total} (last id ${lastId}, ${ms}ms)        \r`,
    );
  }
  console.log(`    ${family}: ${copied}/${total} done                          `);
  return copied;
}

function buildCountSql(sources) {
  const ph = sources.map((_, i) => `$${i + 1}`).join(',');
  let sql = `SELECT COUNT(*)::bigint AS n FROM data_history WHERE source IN (${ph})`;
  if (flags.since) {
    sql += ` AND fetched_at >= EXTRACT(EPOCH FROM TIMESTAMP $${sources.length + 1})::bigint`;
  }
  return sql;
}

function buildInsertSql(family, sources, unitDivisor) {
  const ph = sources.map((_, i) => `$${i + 1}`).join(',');
  const idParam = sources.length + 1;
  const sinceParam = flags.since ? sources.length + 2 : null;
  const limitParam = flags.since ? sources.length + 3 : sources.length + 2;

  const fetchedAtExpr =
    unitDivisor === 1000
      ? `to_timestamp(fetched_at::double precision / 1000.0)`
      : `to_timestamp(fetched_at::double precision)`;

  const sinceClause = sinceParam
    ? ` AND fetched_at >= EXTRACT(EPOCH FROM TIMESTAMP $${sinceParam})::bigint * ${unitDivisor === 1000 ? '1000' : '1'}`
    : '';

  // Build a flat JSONB blob so /api/data/history's pickStr(data, 'title')
  // (etc.) finds the python columns at the top level. The original
  // python `data` text is parsed and merged on top; if it carries its
  // own title/severity/etc. those win, matching python's behaviour
  // where the data field was the source of truth.
  return `
    INSERT INTO ${family} (
      source, source_id, fetched_at, lat, lng, category, subcategory, data
    )
    SELECT
      source,
      source_id,
      ${fetchedAtExpr} AS fetched_at,
      latitude AS lat,
      longitude AS lng,
      category,
      subcategory,
      jsonb_strip_nulls(jsonb_build_object(
        'title', title,
        'location_text', location_text,
        'status', status,
        'severity', severity,
        'source_timestamp', source_timestamp,
        'source_timestamp_unix', source_timestamp_unix,
        'is_active', CASE WHEN is_active = 1 THEN true WHEN is_active = 0 THEN false END,
        'is_live', CASE WHEN is_live = 1 THEN true WHEN is_live = 0 THEN false END,
        'last_seen', last_seen
      )) || COALESCE(NULLIF(data, '')::jsonb, '{}'::jsonb)
    FROM data_history
    WHERE source IN (${ph})
      AND id > $${idParam}${sinceClause}
    ORDER BY id ASC
    LIMIT $${limitParam}
  `;
}

main().catch((err) => {
  console.error('migrate-history failed:', err);
  process.exit(1);
});
