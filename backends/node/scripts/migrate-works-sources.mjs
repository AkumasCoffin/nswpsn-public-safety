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
 * category at ingest; this moves the ones already stored so the filter
 * doesn't show a hard cutover date.
 *
 * TWO THINGS TO KNOW BEFORE READING THE SQL.
 *
 * 1. The sidecar has no `data` column. Only the parent carries the JSONB
 *    blob; `archive_*_latest` holds promoted columns. Both tables have
 *    `category`, and on every one of the 26k works rows it is identical
 *    to `data->>'mainCategory'`, so that is what both queries read.
 *
 * 2. The old rows are DUPLICATES, not orphans. Once the new code was
 *    deployed it began writing these same incidents — same `ltw:` ids —
 *    under the four target sources, while the `traffic_works` rows
 *    stopped. So most works ids already exist at their destination, and
 *    the sidecar's primary key is (source, source_id): a plain UPDATE
 *    would collide on ~1,859 of 2,561 rows.
 *
 *    Where the destination already has the row, the live code's version
 *    is the better one (newer last_seen_at, current data_hash and state)
 *    and the works row is redundant, so it is DELETED. Nothing is lost:
 *    the sidecar is only a latest-state pointer, and the incident's
 *    actual history lives in the parent rows, which move either way.
 *
 * The parent needs none of that care: its key is (id, fetched_at), it
 * has no uniqueness on `source`, and it partitions on `fetched_at`, so
 * re-pointing `source` moves nothing between partitions.
 *
 * Both phases are idempotent — they act on whatever still says
 * `traffic_works` — so an interrupted run can simply be run again.
 *
 * Usage (from backends/node):
 *   node scripts/migrate-works-sources.mjs            # report only
 *   node scripts/migrate-works-sources.mjs --apply    # write
 *   node scripts/migrate-works-sources.mjs --apply --batch 5000
 *
 * Reads DATABASE_URL from backends/.env, or takes it from the
 * environment if already set.
 */
import pg from 'pg';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
// The real classifier, not a copy of it — a second regex here could
// drift from the one that files new records and quietly split an
// incident's history across two sources. Importing is safe: the module
// defines constants at import time and only self-registers when
// registerAll calls its default export.
import { worksArchiveSource } from '../dist/sources/traffic.js';

const APPLY = process.argv.includes('--apply');
const batchArg = process.argv.indexOf('--batch');
const BATCH = batchArg > -1 ? Math.max(500, Number(process.argv[batchArg + 1]) || 5000) : 5000;

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

/** Group source_ids by the archive source they should move to. */
function groupByTarget(rows) {
  const byTarget = new Map();
  for (const r of rows) {
    const target = worksArchiveSource(r.category);
    if (!byTarget.has(target)) byTarget.set(target, []);
    byTarget.get(target).push(String(r.source_id));
  }
  // Sorted so concurrent batches take row locks in the same order as the
  // archive writer's own upsert, which sorts by the conflict key.
  for (const ids of byTarget.values()) ids.sort();
  return byTarget;
}

async function runBatched(ids, fn) {
  for (let i = 0; i < ids.length; i += BATCH) {
    await fn(ids.slice(i, i + BATCH));
  }
}

/** Phase 1 — the history rows. No conflicts are possible here. */
async function migrateParent() {
  const { rows } = await pool.query(
    `SELECT DISTINCT source_id, category
       FROM archive_traffic
      WHERE source = 'traffic_works'`,
  );
  const byTarget = groupByTarget(rows);
  const summary = Object.fromEntries([...byTarget].map(([k, v]) => [k, v.length]));
  console.log(`archive_traffic: ${rows.length} incidents to re-file ${JSON.stringify(summary)}`);
  if (!APPLY) return rows.length;

  let moved = 0;
  for (const [target, ids] of byTarget) {
    await runBatched(ids, async (chunk) => {
      const res = await pool.query(
        `UPDATE archive_traffic SET source = $1
          WHERE source = 'traffic_works' AND source_id = ANY($2::text[])`,
        [target, chunk],
      );
      moved += res.rowCount ?? 0;
    });
    console.log(`  archive_traffic -> ${target}: ${ids.length} incidents`);
  }
  console.log(`  archive_traffic: ${moved} history rows moved`);
  return rows.length;
}

/**
 * Phase 2 — the latest-state pointers, where the duplicates live.
 *
 * Split into rows whose destination is free (move) and rows the live
 * code has already re-written there (drop the stale works copy).
 */
async function migrateSidecar() {
  const { rows } = await pool.query(
    `SELECT w.source_id, w.category,
            EXISTS (
              SELECT 1 FROM archive_traffic_latest t
               WHERE t.source_id = w.source_id AND t.source <> 'traffic_works'
            ) AS has_sibling
       FROM archive_traffic_latest w
      WHERE w.source = 'traffic_works'`,
  );

  const toMove = new Map();
  const toDrop = new Map();
  for (const r of rows) {
    const target = worksArchiveSource(r.category);
    // has_sibling is a cheap pre-filter; the authoritative test is
    // whether the row exists at THIS row's own target, checked below.
    const bucket = r.has_sibling ? toDrop : toMove;
    if (!bucket.has(target)) bucket.set(target, []);
    bucket.get(target).push(String(r.source_id));
  }

  // Confirm each "drop" really does have a row at its own target — a
  // sibling under some other source would not be a key conflict, and
  // that row should move, not be deleted.
  for (const [target, ids] of [...toDrop]) {
    const { rows: present } = await pool.query(
      `SELECT source_id FROM archive_traffic_latest
        WHERE source = $1 AND source_id = ANY($2::text[])`,
      [target, ids],
    );
    const occupied = new Set(present.map((p) => String(p.source_id)));
    const reallyDrop = ids.filter((id) => occupied.has(id));
    const actuallyMove = ids.filter((id) => !occupied.has(id));
    toDrop.set(target, reallyDrop);
    if (actuallyMove.length) {
      if (!toMove.has(target)) toMove.set(target, []);
      toMove.get(target).push(...actuallyMove);
    }
  }

  const moveSummary = Object.fromEntries([...toMove].map(([k, v]) => [k, v.length]));
  const dropSummary = Object.fromEntries([...toDrop].filter(([, v]) => v.length).map(([k, v]) => [k, v.length]));
  const moveTotal = [...toMove.values()].reduce((n, v) => n + v.length, 0);
  const dropTotal = [...toDrop.values()].reduce((n, v) => n + v.length, 0);
  console.log(
    `archive_traffic_latest: ${rows.length} rows — ` +
      `${moveTotal} to move ${JSON.stringify(moveSummary)}, ` +
      `${dropTotal} superseded by the live code and deleted ${JSON.stringify(dropSummary)}`,
  );
  if (!APPLY) return rows.length;

  for (const [target, ids] of toMove) {
    if (!ids.length) continue;
    ids.sort();
    await runBatched(ids, (chunk) =>
      pool.query(
        `UPDATE archive_traffic_latest SET source = $1
          WHERE source = 'traffic_works' AND source_id = ANY($2::text[])`,
        [target, chunk],
      ),
    );
    console.log(`  archive_traffic_latest -> ${target}: ${ids.length} moved`);
  }
  for (const [target, ids] of toDrop) {
    if (!ids.length) continue;
    ids.sort();
    await runBatched(ids, (chunk) =>
      pool.query(
        `DELETE FROM archive_traffic_latest
          WHERE source = 'traffic_works' AND source_id = ANY($1::text[])`,
        [chunk],
      ),
    );
    console.log(`  archive_traffic_latest: ${ids.length} stale works rows dropped (live row exists at ${target})`);
  }
  return rows.length;
}

async function run() {
  // History first, then the pointer to it.
  await migrateParent();
  await migrateSidecar();

  if (APPLY) {
    const { rows } = await pool.query(
      `SELECT
         (SELECT count(*)::int FROM archive_traffic WHERE source = 'traffic_works') AS parent_left,
         (SELECT count(*)::int FROM archive_traffic_latest WHERE source = 'traffic_works') AS sidecar_left`,
    );
    const { parent_left: p, sidecar_left: s } = rows[0];
    console.log(`\ntraffic_works rows remaining — history: ${p}, sidecar: ${s}`);
    if (p || s) console.log('Re-run to finish; the script only ever acts on what is still traffic_works.');
  } else {
    console.log('\nDry run — re-run with --apply to write.');
  }
}

run()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => pool.end());
