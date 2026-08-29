#!/usr/bin/env node
/**
 * One-shot repair for Endeavour outages archived as "Unknown".
 *
 * Why: Endeavour's /outage-points enrichment intermittently drops an
 * incident's address partway through its life. The area record keeps
 * reporting the same coordinates, status and customer count, but
 * cityname/street_name/postcode come back empty, and the poller used to
 * write suburb "Unknown" + empty streets over the top. Because the
 * archive's `title` is derived from `streets || suburb`, the incident
 * then showed on the logs page as a nameless "Unknown" outage — even
 * though an earlier snapshot of the SAME incident, at the SAME
 * coordinates, still holds the real street.
 *
 * sources/endeavour.ts now carries the last known address forward so
 * this stops happening. This script repairs the rows written before
 * that fix.
 *
 * Strategy: for each endeavour incident whose latest snapshot has no
 * usable address, find that incident's most recent EARLIER snapshot
 * that did, and merge its suburb/streets/postcode into the latest
 * snapshot's JSONB (plus the derived title/location_text). The
 * sidecar's promoted title/location_text columns are refreshed to
 * match, so the logs page filters agree with what it displays.
 *
 * This edits `data` on rows already in archive_power, which is
 * otherwise append-only. That is deliberate and narrow: the address
 * comes from the same incident's own history, never from anywhere
 * else, and only fields that are blank/"Unknown" are filled. Nothing
 * is invented and no non-address field is touched.
 *
 * Usage:
 *   cd backends/node
 *   node --env-file-if-exists=../.env scripts/backfill-endeavour-addresses.mjs            # dry run
 *   node --env-file-if-exists=../.env scripts/backfill-endeavour-addresses.mjs --apply
 *
 * Flags:
 *   --apply            actually write (default is a dry run that only reports)
 *   --all-snapshots    repair every degraded snapshot, not just the
 *                      latest one per incident. Off by default: the
 *                      latest snapshot is the only one the logs page
 *                      shows in its default unique=1 view.
 *   --days=<N>         only consider incidents whose latest snapshot is
 *                      within N days (default 90)
 *   --batch=<N>        rows per UPDATE statement (default 500)
 *
 * Safety:
 *   - Dry run by default; --apply is required to write.
 *   - Idempotent: a repaired row no longer matches the "degraded"
 *     predicate, so re-running is a no-op.
 *   - Batched autocommit UPDATEs; a crash mid-run leaves earlier
 *     batches committed and re-running finishes the remainder.
 */
import pg from 'pg';

const { Pool } = pg;

const argv = process.argv.slice(2);
const flags = {
  apply: argv.includes('--apply'),
  allSnapshots: argv.includes('--all-snapshots'),
  days: 90,
  batch: 500,
};
for (const a of argv) {
  if (a.startsWith('--days=')) {
    const n = Number.parseInt(a.slice(7), 10);
    if (Number.isFinite(n) && n > 0) flags.days = n;
  }
  if (a.startsWith('--batch=')) {
    const n = Number.parseInt(a.slice(8), 10);
    if (Number.isFinite(n) && n > 0) flags.batch = n;
  }
}

/**
 * A row is "degraded" when it carries no usable street AND no real
 * suburb — exactly the shape the old fallback produced. Mirrors
 * isRealSuburb() in sources/endeavour.ts.
 */
const DEGRADED = `
  COALESCE(NULLIF(TRIM(a.data->>'streets'), ''), '') = ''
  AND LOWER(COALESCE(NULLIF(TRIM(a.data->>'suburb'), ''), 'unknown')) = 'unknown'
`;

/** A row worth copying an address FROM. */
const HAS_ADDRESS = `
  COALESCE(NULLIF(TRIM(h.data->>'streets'), ''), '') <> ''
  OR LOWER(COALESCE(NULLIF(TRIM(h.data->>'suburb'), ''), 'unknown')) <> 'unknown'
`;

/**
 * Candidate rows: degraded snapshots paired with the address recovered
 * from the newest earlier snapshot of the same incident.
 *
 * `scope` restricts which degraded rows are eligible — by default only
 * the one the sidecar points at (what the logs page actually renders).
 */
function candidateSql(scope) {
  return `
    SELECT a.id,
           a.fetched_at,
           a.source,
           a.source_id,
           best.suburb,
           best.streets,
           best.postcode
      FROM archive_power a
      ${scope}
      CROSS JOIN LATERAL (
        SELECT NULLIF(TRIM(h.data->>'suburb'), '')   AS suburb,
               NULLIF(TRIM(h.data->>'streets'), '')  AS streets,
               NULLIF(TRIM(h.data->>'postcode'), '') AS postcode
          FROM archive_power h
         WHERE h.source = a.source
           AND h.source_id = a.source_id
           AND h.fetched_at <= a.fetched_at
           AND (${HAS_ADDRESS})
         ORDER BY h.fetched_at DESC
         LIMIT 1
      ) best
     WHERE a.source LIKE 'endeavour%'
       AND a.fetched_at > now() - interval '${flags.days} days'
       AND (${DEGRADED})
     ORDER BY a.id
  `;
}

/**
 * Default scope: join to the sidecar so only the snapshot the API
 * serves (source, source_id, latest_fetched_at) is considered.
 */
const LATEST_ONLY_SCOPE = `
  JOIN archive_power_latest l
    ON l.source = a.source
   AND l.source_id = a.source_id
   AND l.latest_fetched_at = a.fetched_at
`;

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error('DATABASE_URL is not set. Run with --env-file-if-exists=../.env');
    process.exit(1);
  }

  const pool = new Pool({ connectionString: url, max: 2, statement_timeout: 0 });

  try {
    const scope = flags.allSnapshots ? '' : LATEST_ONLY_SCOPE;
    console.log(
      `[backfill] scope=${flags.allSnapshots ? 'all snapshots' : 'latest snapshot per incident'} ` +
        `days=${flags.days} mode=${flags.apply ? 'APPLY' : 'dry-run'}`,
    );

    const { rows: candidates } = await pool.query(candidateSql(scope));
    console.log(`[backfill] ${candidates.length} degraded row(s) have a recoverable address`);

    if (candidates.length === 0) {
      console.log('[backfill] nothing to do.');
      return;
    }

    // Show a few so the operator can eyeball the merge before applying.
    for (const r of candidates.slice(0, 5)) {
      const label = [r.streets, r.suburb].filter(Boolean).join(', ');
      console.log(`  ${r.source_id.padEnd(16)} → ${label}`);
    }
    if (candidates.length > 5) console.log(`  … and ${candidates.length - 5} more`);

    if (!flags.apply) {
      console.log('[backfill] dry run — re-run with --apply to write these changes.');
      return;
    }

    let written = 0;
    for (let i = 0; i < candidates.length; i += flags.batch) {
      const chunk = candidates.slice(i, i + flags.batch);
      const ids = chunk.map((r) => r.id);
      const suburbs = chunk.map((r) => r.suburb ?? '');
      const streets = chunk.map((r) => r.streets ?? '');
      const postcodes = chunk.map((r) => r.postcode ?? '');

      // jsonb_strip_nulls keeps the blob clean when a field stays empty.
      // title/location_text mirror archiveExtract's `streets || suburb`
      // derivation so the promoted columns and the payload agree.
      const res = await pool.query(
        `UPDATE archive_power a
            SET data = a.data || jsonb_strip_nulls(jsonb_build_object(
                  'suburb',        NULLIF(v.suburb, ''),
                  'streets',       NULLIF(v.streets, ''),
                  'postcode',      NULLIF(v.postcode, ''),
                  'title',         NULLIF(COALESCE(NULLIF(v.streets, ''), v.suburb), ''),
                  'location_text', NULLIF(COALESCE(NULLIF(v.streets, ''), v.suburb), '')
                ))
           FROM (
             SELECT UNNEST($1::bigint[]) AS id,
                    UNNEST($2::text[])   AS suburb,
                    UNNEST($3::text[])   AS streets,
                    UNNEST($4::text[])   AS postcode
           ) v
          WHERE a.id = v.id
            AND (${DEGRADED})`,
        [ids, suburbs, streets, postcodes],
      );
      written += res.rowCount ?? 0;
      console.log(`[backfill] updated ${written}/${candidates.length}`);
    }

    // Re-promote the sidecar's display columns from the rows just fixed
    // so filtering by title/location matches what the page renders.
    const sync = await pool.query(
      `UPDATE archive_power_latest l
          SET title         = a.data->>'title',
              location_text = a.data->>'location_text'
         FROM archive_power a
        WHERE a.source = l.source
          AND a.source_id = l.source_id
          AND a.fetched_at = l.latest_fetched_at
          AND l.source LIKE 'endeavour%'
          AND a.data->>'title' IS NOT NULL
          AND l.title IS DISTINCT FROM a.data->>'title'`,
    );
    console.log(`[backfill] sidecar rows re-promoted: ${sync.rowCount ?? 0}`);
    console.log('[backfill] done.');
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error('[backfill] failed:', err);
  process.exit(1);
});
