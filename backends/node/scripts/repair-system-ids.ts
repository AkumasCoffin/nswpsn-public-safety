/**
 * Re-file receptions whose transmitted P25 ids were mis-decoded.
 *
 * WACN and system id are read off the air, so a corrupt control frame yields a
 * plausible-looking pair belonging to no network. Production carries 0xAEE00
 * and 0xBAE03 against a real 0xBEE00 — one bit apart, a handful of receptions
 * each against millions. Every rollup on the Data page groups by
 * (wacn, system), so each bad pair renders as a whole extra system beside the
 * real one, carrying off a few of its talkgroups and calls.
 *
 * The ingest path stops new ones (see canonicalPair in services/nodeEvents),
 * and the 30-day prune eventually takes the old ones. This repairs what is
 * already stored, for when "eventually" is not good enough.
 *
 * It applies the SAME rule the ingest path does, so the two cannot drift:
 * identity is decided by the system LABEL, which the operator configures
 * rather than the radio decoding it, and within one label by the pair the
 * overwhelming majority of receptions carry. Two guards keep it from ever
 * merging two real networks: the label must already have a substantial body of
 * receptions, and the pair being rewritten must be under a thousandth of its
 * traffic. A different network carries a different label and is untouched.
 *
 * It does NOT re-group the calls those receptions forked off into. The ids are
 * put right so the Data page stops showing a phantom system; a dozen historical
 * calls stay split, which is not worth re-running the grouper over.
 *
 * Usage (run from backends/node):
 *   npx tsx --env-file=../.env scripts/repair-system-ids.ts            # dry run
 *   npx tsx --env-file=../.env scripts/repair-system-ids.ts --apply    # write
 *
 * The survey of what to fix reads the whole retention window and takes several
 * minutes on a production-sized table — which is exactly why this is an
 * operator script run when convenient, and not a migration run at boot.
 */
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { getPool, closePool } from '../src/db/pool.js';

/** A label needs at least this many receptions before it is believed. */
const MIN_CANONICAL_EVENTS = 1000;
/** A pair under 1/this of the label's traffic is noise, not a network. */
const NOISE_RATIO = 1000;

interface Deviation {
  system_label: string;
  good_wacn: number | null;
  good_sys: number;
  good_n: string;
  bad_wacn: number | null;
  bad_sys: number;
  bad_n: string;
}

const SURVEY_SQL = `
  WITH pair_counts AS (
    SELECT system_label, wacn, system, COUNT(*)::bigint AS n
      FROM node_radio_events
     WHERE system IS NOT NULL
       AND system_label IS NOT NULL
     GROUP BY system_label, wacn, system
  ),
  canon AS (
    SELECT DISTINCT ON (system_label) system_label, wacn, system, n
      FROM pair_counts
     ORDER BY system_label, n DESC
  )
  SELECT c.system_label,
         c.wacn AS good_wacn, c.system AS good_sys, c.n::text AS good_n,
         p.wacn AS bad_wacn,  p.system AS bad_sys,  p.n::text AS bad_n
    FROM canon c
    JOIN pair_counts p ON p.system_label = c.system_label
   WHERE (p.wacn IS DISTINCT FROM c.wacn OR p.system <> c.system)
     AND c.n >= $1
     AND p.n * $2 <= c.n
   ORDER BY p.n DESC`;

const hex = (v: number | null) => (v === null ? 'null' : `0x${v.toString(16).toUpperCase()}`);

async function main(): Promise<void> {
  const apply = process.argv.slice(2).includes('--apply');
  const pool = await getPool();
  if (!pool) {
    console.error('No database. Run with --env-file=../.env from backends/node.');
    process.exit(1);
  }

  console.log('Surveying stored receptions (several minutes on a large table)…');
  const { rows } = await pool.query<Deviation>(SURVEY_SQL, [MIN_CANONICAL_EVENTS, NOISE_RATIO]);

  if (rows.length === 0) {
    console.log('Nothing to repair: every reception already carries its system’s ids.');
    await closePool();
    return;
  }

  console.log(`\n${rows.length} mis-decoded id pair(s):\n`);
  let total = 0;
  for (const r of rows) {
    total += Number(r.bad_n);
    console.log(
      `  ${r.system_label}: ${r.bad_n.padStart(7)} reception(s) carrying ` +
        `wacn ${hex(r.bad_wacn)} / system ${r.bad_sys}\n` +
        `  ${' '.repeat(r.system_label.length)}  → wacn ${hex(r.good_wacn)} / system ${r.good_sys} ` +
        `(${Number(r.good_n).toLocaleString()} receptions)`,
    );
  }
  console.log(`\n${total} reception(s) would be re-filed.`);

  if (!apply) {
    console.log('\nDry run — nothing was written. Re-run with --apply to repair.');
    await closePool();
    return;
  }

  // One statement per deviating pair: each is a narrow, resumable write, and a
  // failure halfway leaves the pairs it already fixed fixed.
  let repaired = 0;
  for (const r of rows) {
    const res = await pool.query(
      `UPDATE node_radio_events
          SET wacn = $1, system = $2
        WHERE system_label = $3
          AND system = $4
          AND wacn IS NOT DISTINCT FROM $5`,
      [r.good_wacn, r.good_sys, r.system_label, r.bad_sys, r.bad_wacn],
    );
    repaired += res.rowCount ?? 0;
    console.log(`  repaired ${res.rowCount ?? 0} row(s) for ${r.system_label} / ${hex(r.bad_wacn)}`);
  }
  console.log(`\nDone: ${repaired} reception(s) re-filed under their own system.`);
  await closePool();
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  main().then(
    () => process.exit(0),
    (err) => {
      console.error(err);
      process.exit(1);
    },
  );
}
