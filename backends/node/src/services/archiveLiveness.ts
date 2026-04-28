/**
 * Per-poll "disappeared from upstream" tracking — Node port of python's
 * `store_incidents_batch` diff at external_api_proxy.py:2165-2204.
 *
 * After each successful poll, we want any source_id that was live in
 * the previous cycle but isn't in the current snapshot to be flagged
 * `is_live: false`. Node's archive is append-only, so instead of an
 * UPDATE we INSERT a tombstone row with the previous row's data
 * fields preserved + `is_live: false` overlaid. DISTINCT ON
 * (source, source_id) ORDER BY fetched_at DESC at read time then
 * surfaces the tombstone, so logs.html / data-history see the
 * incident as ended.
 *
 * Edge cases mirrored from python:
 *   - Empty snapshot → skip the diff entirely. An upstream that
 *     briefly returns zero incidents shouldn't tombstone everything
 *     that was live a minute ago. (python: line 2177 — `if
 *     source_type and all_source_ids_in_batch`).
 *   - Waze sources (`waze_*`) → skip the diff. Userscript ingest
 *     rotates through ~190 bbox regions, so any single ingest only
 *     sees a fraction of the live state. The 1h staleness sweep
 *     (`sweepStaleAsEnded`) handles waze instead. (python: line
 *     2174-2176).
 *
 * The fallback sweep at the bottom catches the case where polling
 * gets stuck for a source — without it, an archive row's last live
 * fetch from 6h ago would still report is_live=true. Mirrors python's
 * cleanup_old_data pager-aging at line 2248-2259, generalised.
 */
import type { Pool } from 'pg';
import { log } from '../lib/log.js';
import type { ArchiveRow, ArchiveTable } from '../store/archive.js';

/** Bound on how far back the diff query searches for "previously live"
 *  rows. 7 days is a generous window — anything older than that we
 *  assume has long since aged out via partition drop or staleness sweep
 *  and isn't worth tombstoning. The bound also keeps the DISTINCT ON
 *  query fast on large archive tables. */
const LIVE_LOOKBACK_DAYS = 7;

/** Fallback staleness threshold: a source_id whose latest live row's
 *  fetched_at is older than this gets tombstoned by sweepStaleAsEnded.
 *  1h matches python's pager_cutoff. Set generously — the per-poll diff
 *  is the primary mechanism; this is just for stuck pollers. */
const STALE_LIVE_AGE_SECS = 3600;

/** Sources for which the per-poll diff is unsafe — see file header. */
function isDiffExempt(source: string): boolean {
  return source.startsWith('waze_');
}

/**
 * Latest-live row info needed to build a tombstone that preserves the
 * incident's display fields. Returned by getLiveRowsForSource.
 */
interface LiveRow {
  source_id: string;
  lat: number | null;
  lng: number | null;
  category: string | null;
  subcategory: string | null;
  data: Record<string, unknown> | null;
}

/**
 * Fetch source_ids whose CURRENT latest archive row is "live" — meaning
 * `data->>'is_live'` is missing OR truthy. Bounded to the last
 * LIVE_LOOKBACK_DAYS so the query stays fast on partitioned tables.
 *
 * Returns the latest row's display fields (lat/lng/category/subcategory/data)
 * so a tombstone insert can preserve them.
 */
async function getLiveRowsForSource(
  pool: Pool,
  table: ArchiveTable,
  source: string,
): Promise<Map<string, LiveRow>> {
  const sql = `
    SELECT source_id, lat, lng, category, subcategory, data
      FROM (
        SELECT DISTINCT ON (source_id)
               source_id, lat, lng, category, subcategory, data, fetched_at
          FROM ${table}
         WHERE source = $1
           AND source_id IS NOT NULL
           AND fetched_at >= now() - ($2 || ' days')::interval
         ORDER BY source_id, fetched_at DESC
      ) latest
     WHERE COALESCE(data->>'is_live', 'true') NOT IN ('0','false','False')
  `;
  const r = await pool.query<{
    source_id: string;
    lat: number | null;
    lng: number | null;
    category: string | null;
    subcategory: string | null;
    data: Record<string, unknown> | null;
  }>(sql, [source, String(LIVE_LOOKBACK_DAYS)]);
  const out = new Map<string, LiveRow>();
  for (const row of r.rows) {
    out.set(row.source_id, row);
  }
  return out;
}

/**
 * Build tombstone rows for source_ids that were live before this poll
 * but aren't in the new snapshot. Returns an array (possibly empty);
 * the caller pushes them through ArchiveWriter alongside the live rows.
 *
 * Does NOT consult the DB if `newRows` is empty (upstream blip
 * protection — matches python). Does NOT consult the DB for waze
 * sources (rotation problem — matches python).
 */
export async function computeDisappearedTombstones(opts: {
  pool: Pool;
  table: ArchiveTable;
  source: string;
  newRows: ArchiveRow[];
  fetchedAt: number;
}): Promise<ArchiveRow[]> {
  const { pool, table, source, newRows, fetchedAt } = opts;
  if (newRows.length === 0) return []; // upstream blip
  if (isDiffExempt(source)) return [];

  const newIds = new Set<string>();
  for (const r of newRows) {
    if (r.source_id !== null && r.source_id !== undefined && r.source_id !== '') {
      newIds.add(String(r.source_id));
    }
  }
  // If the snapshot has data but no source_ids at all (sources without
  // stable ids), the diff has no anchor — skip rather than tombstone
  // everything.
  if (newIds.size === 0) return [];

  let liveBefore: Map<string, LiveRow>;
  try {
    liveBefore = await getLiveRowsForSource(pool, table, source);
  } catch (err) {
    // Don't let a transient DB error block the live INSERT. Log and
    // skip the diff for this cycle — next cycle will re-run.
    log.warn(
      { err: (err as Error).message, source, table },
      'archiveLiveness: live-row fetch failed; skipping diff',
    );
    return [];
  }
  const tombstones: ArchiveRow[] = [];
  for (const [sid, prev] of liveBefore) {
    if (newIds.has(sid)) continue;
    // Preserve display fields so logs.html still renders title/etc. on
    // an ended incident. Overlay is_live=false on top of the previous
    // data blob.
    const prevData = (prev.data ?? {}) as Record<string, unknown>;
    tombstones.push({
      source,
      source_id: sid,
      fetched_at: fetchedAt,
      lat: prev.lat,
      lng: prev.lng,
      category: prev.category,
      subcategory: prev.subcategory,
      data: { ...prevData, is_live: false },
    });
  }
  if (tombstones.length > 0) {
    log.info(
      { source, table, count: tombstones.length },
      'archiveLiveness: tombstoning disappeared incidents',
    );
  }
  return tombstones;
}

/**
 * Stamp `is_live: true` into a row's data blob if it isn't already
 * set. Called from the poller before pushing live rows. Without this
 * stamp, the count query at /api/data/history (`(data->>'is_live') IN
 * ('1','true','True')`) wouldn't see them as live.
 */
export function stampLiveRow(row: ArchiveRow): ArchiveRow {
  const data = (row.data ?? {}) as Record<string, unknown>;
  if (data['is_live'] === undefined) {
    return { ...row, data: { ...data, is_live: true } };
  }
  return row;
}

/**
 * Background sweep: for each archive table, find source_ids whose
 * latest LIVE row's fetched_at is older than STALE_LIVE_AGE_SECS, and
 * insert tombstones. Runs from the cleanup loop so a stuck poller
 * doesn't leave records reporting is_live=true forever.
 *
 * Returns the number of tombstones inserted across all tables.
 */
export async function sweepStaleAsEnded(pool: Pool): Promise<number> {
  const tables: ArchiveTable[] = [
    'archive_waze',
    'archive_traffic',
    'archive_rfs',
    'archive_power',
    'archive_misc',
  ];
  let total = 0;
  for (const table of tables) {
    try {
      const sql = `
        WITH stale AS (
          SELECT source, source_id, lat, lng, category, subcategory, data
            FROM (
              SELECT DISTINCT ON (source, source_id)
                     source, source_id, lat, lng, category, subcategory,
                     data, fetched_at
                FROM ${table}
               WHERE source_id IS NOT NULL
                 AND fetched_at >= now() - ($1 || ' days')::interval
               ORDER BY source, source_id, fetched_at DESC
            ) latest
           WHERE COALESCE(data->>'is_live', 'true') NOT IN ('0','false','False')
             AND fetched_at < now() - ($2 || ' seconds')::interval
        )
        INSERT INTO ${table} (source, source_id, fetched_at,
                              lat, lng, category, subcategory, data)
        SELECT source, source_id, EXTRACT(EPOCH FROM now())::bigint,
               lat, lng, category, subcategory,
               COALESCE(data, '{}'::jsonb) || '{"is_live": false}'::jsonb
          FROM stale
      `;
      const r = await pool.query(sql, [
        String(LIVE_LOOKBACK_DAYS),
        String(STALE_LIVE_AGE_SECS),
      ]);
      const inserted = r.rowCount ?? 0;
      if (inserted > 0) {
        log.info(
          { table, count: inserted },
          'archiveLiveness: swept stale as ended',
        );
      }
      total += inserted;
    } catch (err) {
      log.warn(
        { err: (err as Error).message, table },
        'archiveLiveness: stale sweep failed for table',
      );
    }
  }
  return total;
}

// ---------------------------------------------------------------------------
// Test seams
// ---------------------------------------------------------------------------

export const _archiveLivenessTestHooks = {
  isDiffExempt,
  LIVE_LOOKBACK_DAYS,
  STALE_LIVE_AGE_SECS,
};
