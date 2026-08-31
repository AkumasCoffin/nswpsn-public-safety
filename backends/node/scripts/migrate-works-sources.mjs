#!/usr/bin/env node
/**
 * Re-file archived `traffic_works` rows under the type their category
 * implies.
 *
 * Why: the works aggregate is a mixed bag — roadwork, construction,
 * utilities, light rail, hazards, flooding, events — and filing it all
 * under one "Works & ACT" type meant the logs page offered a type whose
 * contents were really Incidents, Roadwork, Flooding and Major Events
 * wearing a single label. sources/traffic.ts now files new records by
 * category at ingest; this moves the ones already stored, so the filter
 * doesn't show a hard cutover date.
 *
 * The mapping is `worksArchiveSource()` from the source, restated here
 * so the script has no build dependency — keep the two in step.
 *
 * What it touches: the `source` column on `archive_traffic` and
 * `archive_traffic_latest`, for rows currently marked `traffic_works`.
 * Nothing else is read or written. All four target sources already live
 * in `archive_traffic`, so no row changes table, and works ids are
 * `ltw:`-prefixed so they cannot collide with the state feeds' own ids
 * now sharing those sources.
 *
 * Usage (from backends/node):
 *   node scripts/migrate-works-sources.mjs            # report only
 *   node scripts/migrate-works-sources.mjs --apply    # write
 *   node scripts/migrate-works-sources.mjs --apply --batch 5000
 *
 * Reads DATABASE_URL from backends/.env, or takes it from the
 * environment / `--env-file-if-exists=../.env` if already set.
 */
import pg from 'pg';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const APPLY = process.argv.includes('--apply');
const batchArg = process.argv.indexOf('--batch');
const BATCH = batchArg > -1 ? Math.max(500, Number(process.argv[batchArg + 1]) || 5000) : 5000;

/** Mirror of worksArchiveSource() in src/sources/traffic.ts. */
function worksArchiveSource(mainCategory) {
  const c = String(mainCategory || '').toLowerCase().replace(/[^a-z]/g, '');
  if (c.includes('flood')) return 'traffic_flood';
  if (/roadwork|construction|utilit|lightrail|telecommunication|charitable/.test(c)) {
    return 'traffic_roadwork';
  }
  if (/majorevent|specialevent/.test(c)) return 'traffic_majorevent';
  return 'traffic_incident';
}

function loadEnvFile(path) {
  let text;
  try {
    text = readFileSync(path, 'utf8');
  } catch {
    return false;
  }
  for (const line of text.split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith('#')) continue;
    const eq = t.indexOf('=');
    if (eq < 1) continue;
    const key = t.slice(0, eq).trim();
    let val = t.slice(eq + 1).trim().replace(/\r$/, '');
    if (
      (val.startsWith('"') && val.endsWith('"')) ||
      (val.startsWith("'") && val.endsWith("'"))
    ) {
      val = val.slice(1, -1);
    }
    if (process.env[key] === undefined) process.env[key] = val;
  }
  return true;
}

const ENV_PATH = join(dirname(dirname(fileURLToPath(import.meta.url))), '..', '.env');
if (!process.env.DATABASE_URL) loadEnvFile(ENV_PATH);

const url = process.env.DATABASE_URL;
if (!url) {
  console.error(`DATABASE_URL is not set, and no usable value in ${ENV_PATH}`);
  process.exit(1);
}

const pool = new pg.Pool({ connectionString: url, max: 4 });

async function migrate(table, idColumn) {
  const { rows } = await pool.query(
    `SELECT ${idColumn} AS key, data->>'mainCategory' AS cat
       FROM ${table}
      WHERE source = 'traffic_works'`,
  );
  const byTarget = new Map();
  for (const r of rows) {
    const target = worksArchiveSource(r.cat);
    if (!byTarget.has(target)) byTarget.set(target, []);
    byTarget.get(target).push(r.key);
  }
  const summary = Object.fromEntries([...byTarget].map(([k, v]) => [k, v.length]));
  console.log(`${table}: ${rows.length} rows to re-file ${JSON.stringify(summary)}`);
  if (!APPLY || rows.length === 0) return rows.length;

  for (const [target, keys] of byTarget) {
    // Sorted so concurrent batches take row locks in the same order as
    // the archive writer's own upsert, which sorts by the conflict key.
    keys.sort();
    for (let i = 0; i < keys.length; i += BATCH) {
      const chunk = keys.slice(i, i + BATCH);
      await pool.query(
        `UPDATE ${table} SET source = $1
          WHERE source = 'traffic_works' AND ${idColumn} = ANY($2::text[])`,
        [target, chunk.map(String)],
      );
      console.log(`  ${table} -> ${target}: ${Math.min(i + BATCH, keys.length)}/${keys.length}`);
    }
  }
  return rows.length;
}

async function run() {
  // The sidecar first: its primary key is (source, source_id), so a row
  // whose parent already moved but whose sidecar hasn't would look like
  // a brand-new incident to the next poll.
  const sidecar = await migrate('archive_traffic_latest', 'source_id');
  const parent = await migrate('archive_traffic', 'id');

  console.log(
    `\n${parent} history rows and ${sidecar} sidecar rows ` +
      (APPLY ? 'moved.' : 'would move — re-run with --apply to write.'),
  );
  if (APPLY) {
    const { rows } = await pool.query(
      `SELECT count(*)::int AS n FROM archive_traffic WHERE source = 'traffic_works'`,
    );
    console.log(`traffic_works rows remaining: ${rows[0].n}`);
  }
}

run()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => pool.end());
