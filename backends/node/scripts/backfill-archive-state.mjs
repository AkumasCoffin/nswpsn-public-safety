#!/usr/bin/env node
/**
 * One-shot fill of `state` on the archive `_latest` sidecars.
 *
 * Why a script: the state is derived from the row's coordinate by
 * point-in-polygon against real jurisdiction boundaries, which the
 * database cannot do (no PostGIS). Migration 082 adds the column but
 * cannot populate it, so the work happens here, in Node, where the
 * polygons live (src/lib/stateMask.ts).
 *
 * Live rows heal themselves — upsertLatestSidecar refreshes display
 * fields whenever the stored value is NULL, so anything still being
 * polled gets a state within a cycle or two. This exists for the rest:
 * rows whose incident has closed and will never be written again.
 *
 * What it touches: only `state`, only where it is currently NULL, only
 * for rows whose parent snapshot carries a usable coordinate. No other
 * column is read or written, and a row that cannot be placed (BOM
 * warnings are areas, not points) is left NULL rather than guessed at.
 *
 * Usage (from backends/node):
 *   node scripts/backfill-archive-state.mjs            # report only
 *   node scripts/backfill-archive-state.mjs --apply    # write
 *   node scripts/backfill-archive-state.mjs --apply --batch 2000
 *
 * Reads DATABASE_URL from backends/.env. The rest of the project passes
 * that in with `node --env-file-if-exists=../.env`, which also works
 * here; when it isn't given, the file is loaded directly below so the
 * script runs the same either way.
 */
import pg from 'pg';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { stateForRow } from '../dist/lib/stateMask.js';

/**
 * Minimal .env reader — enough for KEY=value with optional quotes and
 * trailing CR (the file is edited on both Windows and the server).
 * Never overwrites a variable that is already set, so an explicit
 * --env-file or a shell export still wins.
 */
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

const APPLY = process.argv.includes('--apply');
const batchArg = process.argv.indexOf('--batch');
const BATCH = batchArg > -1 ? Math.max(100, Number(process.argv[batchArg + 1]) || 1000) : 1000;

const PAIRS = [
  ['archive_traffic', 'archive_traffic_latest'],
  ['archive_rfs', 'archive_rfs_latest'],
  ['archive_power', 'archive_power_latest'],
  ['archive_misc', 'archive_misc_latest'],
];

// backends/.env — one level above backends/node, resolved from this
// file so the script works whatever directory it is invoked from.
const ENV_PATH = join(dirname(dirname(fileURLToPath(import.meta.url))), '..', '.env');
if (!process.env.DATABASE_URL) loadEnvFile(ENV_PATH);

const url = process.env.DATABASE_URL;
if (!url) {
  console.error(`DATABASE_URL is not set, and no usable value in ${ENV_PATH}`);
  process.exit(1);
}

const pool = new pg.Pool({ connectionString: url, max: 4 });

async function run() {
  let grandTotal = 0;
  let grandPlaced = 0;

  for (const [parent, sidecar] of PAIRS) {
    // The sidecar has no coordinate of its own, so pull the most recent
    // parent snapshot for each un-stated row and read the point from it.
    const { rows } = await pool.query(
      `SELECT l.source, l.source_id, p.data
         FROM ${sidecar} l
         JOIN LATERAL (
           SELECT data FROM ${parent} h
            WHERE h.source = l.source AND h.source_id = l.source_id
            ORDER BY h.fetched_at DESC
            LIMIT 1
         ) p ON true
        WHERE l.state IS NULL`,
    );

    const updates = [];
    for (const r of rows) {
      const st = stateForRow(r.data);
      if (st) updates.push([r.source, r.source_id, st]);
    }

    const byState = {};
    for (const [, , st] of updates) byState[st] = (byState[st] ?? 0) + 1;
    console.log(
      `${sidecar}: ${rows.length} without a state, ${updates.length} placeable ` +
        `${JSON.stringify(byState)}`,
    );
    grandTotal += rows.length;
    grandPlaced += updates.length;

    if (!APPLY || updates.length === 0) continue;

    for (let i = 0; i < updates.length; i += BATCH) {
      const chunk = updates.slice(i, i + BATCH);
      // Order by the primary key, the same as upsertLatestSidecar does,
      // so this can't deadlock against a concurrent archive flush.
      chunk.sort((a, b) => (a[0] === b[0] ? (a[1] < b[1] ? -1 : 1) : a[0] < b[0] ? -1 : 1));
      const values = chunk
        .map((_, j) => `($${j * 3 + 1}::text, $${j * 3 + 2}::text, $${j * 3 + 3}::text)`)
        .join(',');
      await pool.query(
        `UPDATE ${sidecar} l
            SET state = v.state
           FROM (VALUES ${values}) AS v(source, source_id, state)
          WHERE l.source = v.source
            AND l.source_id = v.source_id
            AND l.state IS NULL`,
        chunk.flat(),
      );
      console.log(`  ${sidecar}: wrote ${Math.min(i + BATCH, updates.length)}/${updates.length}`);
    }
  }

  console.log(
    `\n${grandPlaced} of ${grandTotal} rows could be placed. ` +
      (APPLY ? 'Written.' : 'Dry run — re-run with --apply to write.'),
  );
  const unplaceable = grandTotal - grandPlaced;
  if (unplaceable > 0) {
    console.log(
      `${unplaceable} left NULL: no usable coordinate, or a point outside ` +
        `every state (BOM warnings are areas, not points).`,
    );
  }
}

run()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => pool.end());
