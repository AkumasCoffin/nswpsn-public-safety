/**
 * Flush-time "disappeared from upstream" tracking — Node port of
 * python's `store_incidents_batch` diff at external_api_proxy.py:
 * 2165-2204.
 *
 * Once per archive flush (not once per poll — too chatty), we look at
 * the rows queued for each archive_* table, group them by source, and
 * for each non-exempt source compute the set of source_ids that were
 * live in the previous flush window but aren't in the current
 * snapshot. Each disappeared source_id gets a tombstone row appended
 * to the flush — same source_id, fetched_at=now, prior `data` fields
 * preserved with `is_live: false` overlaid. DISTINCT ON
 * (source, source_id) ORDER BY fetched_at DESC at read time then
 * surfaces the tombstone, so logs.html / data-history see the
 * incident as ended.
 *
 * Doing the diff at flush time means **one SELECT per archive table
 * per flush window** instead of one per source per poll — the
 * archive writer's natural batching applies to liveness queries too.
 * On a typical deploy this drops the per-poll DB chatter from ~21
 * SELECT/min down to ~5 SELECT/30s.
 *
 * Edge cases mirrored from python:
 *   - Empty snapshot for a source → skip the diff for that source.
 *     An upstream that briefly returns zero incidents shouldn't
 *     tombstone everything that was live a minute ago. (python: line
 *     2177 — `if source_type and all_source_ids_in_batch`).
 *   - Waze sources (`waze_*`) → skip the diff. Userscript ingest
 *     rotates through ~190 bbox regions, so any single ingest only
 *     sees a fraction of the live state. The 1h staleness sweep
 *     (`sweepStaleAsEnded`) handles waze instead. (python: line
 *     2174-2176).
 *   - Future-scheduled power outages (`endeavour_planned`,
 *     `essential_planned`, `essential_future`) → skip the diff.
 *     Planned outages can be scheduled days in advance and rotate
 *     in/out of the upstream feed as the schedule shifts;
 *     tombstoning them on the first missing poll would mark
 *     genuinely-still-scheduled outages as ended. Note:
 *     `endeavour_planned` doubles as the archive bucket for
 *     `endeavour_maintenance` (currently-active planned
 *     maintenance) — that's the same fold python uses at
 *     external_api_proxy.py:4544-4545. They share the exemption.
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
 *  rows. 1 day is enough — incidents that have been gone from upstream
 *  for more than a day are picked up by the hourly sweepStaleAsEnded
 *  fallback regardless. Was 7 days; on production hosts under disk
 *  pressure the 7-day DISTINCT ON reliably hit the 30s
 *  statement_timeout, blocking every archive flush for 30s × 4 tables. */
const LIVE_LOOKBACK_DAYS = 1;

/** Fallback staleness threshold: a source_id whose latest live row's
 *  fetched_at is older than this gets tombstoned by sweepStaleAsEnded.
 *  1h matches python's pager_cutoff. Set generously — the per-poll diff
 *  is the primary mechanism; this is just for stuck pollers. */
const STALE_LIVE_AGE_SECS = 3600;

/** Sources for which the per-poll diff is unsafe — see file header. */
const FUTURE_DATED_SOURCES = new Set<string>([
  'endeavour_planned',
  'essential_planned',
  'essential_future',
]);
/** Sources where source_id is unique per row (one row = one event), so
 *  DISTINCT ON returns the entire table and the diff is meaningless
 *  work. Pager: every message is fired once and that's it; "disappeared
 *  from upstream" doesn't apply. Skipping pager here is a pure perf
 *  win — the table only contains pager rows, so the per-flush
 *  archive_misc DISTINCT ON over thousands of rows is now skipped
 *  entirely. */
const UNIQUE_SOURCE_ID_SOURCES = new Set<string>(['pager']);
function isDiffExempt(source: string): boolean {
  if (source.startsWith('waze_')) return true;
  if (FUTURE_DATED_SOURCES.has(source)) return true;
  if (UNIQUE_SOURCE_ID_SOURCES.has(source)) return true;
  return false;
}

/**
 * Latest-live row info needed to build a tombstone that preserves the
 * incident's display fields. Returned by getLiveRowsForSources.
 */
interface LiveRow {
  source: string;
  source_id: string;
  lat: number | null;
  lng: number | null;
  category: string | null;
  subcategory: string | null;
  data: Record<string, unknown> | null;
}

/**
 * Single batched SELECT for every (source, source_id) pair that is
 * currently live in the given archive table, scoped to a list of
 * sources. Bounded to LIVE_LOOKBACK_DAYS so the query stays fast on
 * partitioned tables.
 *
 * Result map is keyed by source so the caller can compute the diff
 * per-source against its own incoming batch.
 */
async function getLiveRowsForSources(
  pool: Pool,
  table: ArchiveTable,
  sources: string[],
): Promise<Map<string, Map<string, LiveRow>>> {
  if (sources.length === 0) return new Map();
  const sql = `
    SELECT source, source_id, lat, lng, category, subcategory, data
      FROM (
        SELECT DISTINCT ON (source, source_id)
               source, source_id, lat, lng, category, subcategory, data, fetched_at
          FROM ${table}
         WHERE source = ANY($1::text[])
           AND source_id IS NOT NULL
           AND fetched_at >= now() - ($2 || ' days')::interval
         ORDER BY source, source_id, fetched_at DESC
      ) latest
     WHERE COALESCE(data->>'is_live', 'true') NOT IN ('0','false','False')
  `;
  // Tight 5s timeout so a slow miss can't drag the archive flush down
  // to 30s. The hourly sweepStaleAsEnded covers anything we miss here.
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    try {
      await client.query("SET LOCAL statement_timeout = '5s'");
      const r = await client.query<LiveRow>(sql, [sources, String(LIVE_LOOKBACK_DAYS)]);
      await client.query('COMMIT');
      const out = new Map<string, Map<string, LiveRow>>();
      for (const row of r.rows) {
        let bucket = out.get(row.source);
        if (!bucket) {
          bucket = new Map();
          out.set(row.source, bucket);
        }
        bucket.set(row.source_id, row);
      }
      return out;
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch { /* ignore */ }
      throw err;
    }
  } finally {
    client.release();
  }
}

/**
 * Build tombstone rows for the queued contents of one archive table.
 * Called from the archive writer's flush() once per table per flush —
 * a single SELECT covers every non-exempt source in the bucket.
 *
 * Per source, tombstones cover source_ids that were live in DB but
 * aren't in this flush's incoming rows. Same edge cases as python:
 * empty incoming batch for a source skips the diff for that source,
 * waze_* + future-dated sources skip entirely.
 */
export async function computeFlushTimeTombstones(opts: {
  pool: Pool;
  table: ArchiveTable;
  rows: ArchiveRow[];
  fetchedAt: number;
}): Promise<ArchiveRow[]> {
  const { pool, table, rows, fetchedAt } = opts;
  if (rows.length === 0) return [];

  // Group incoming source_ids by source. Skip exempt sources and
  // sources whose batch has no source_ids (can't anchor a diff).
  const newIdsBySource = new Map<string, Set<string>>();
  for (const r of rows) {
    if (isDiffExempt(r.source)) continue;
    const sid = r.source_id;
    if (sid === null || sid === undefined || sid === '') continue;
    let ids = newIdsBySource.get(r.source);
    if (!ids) {
      ids = new Set();
      newIdsBySource.set(r.source, ids);
    }
    ids.add(String(sid));
  }
  if (newIdsBySource.size === 0) return [];

  const sources = Array.from(newIdsBySource.keys());
  let liveBefore: Map<string, Map<string, LiveRow>>;
  try {
    liveBefore = await getLiveRowsForSources(pool, table, sources);
  } catch (err) {
    log.warn(
      { err: (err as Error).message, table, sources },
      'archiveLiveness: live-rows fetch failed; skipping diff this flush',
    );
    return [];
  }

  const tombstones: ArchiveRow[] = [];
  for (const [source, newIds] of newIdsBySource) {
    const live = liveBefore.get(source);
    if (!live) continue;
    for (const [sid, prev] of live) {
      if (newIds.has(sid)) continue;
      // Preserve display fields so logs.html still renders title/etc.
      // on an ended incident. Overlay is_live=false on top of the
      // previous data blob.
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
  }
  if (tombstones.length > 0) {
    // Log per-source counts so it's clear in the logs which sources
    // saw activity even though it's one flush-time call.
    const bySource: Record<string, number> = {};
    for (const t of tombstones) {
      bySource[t.source] = (bySource[t.source] ?? 0) + 1;
    }
    log.info(
      { table, count: tombstones.length, by_source: bySource },
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
 * archive_waze is deliberately excluded:
 *   - waze_* sources are exempt from the per-flush diff (rotation
 *     can't see all regions per cycle), AND
 *   - the table holds millions of rows, so the DISTINCT ON over a
 *     7-day window contended with userscript ingest and stretched
 *     a single cleanup pass to 4+ minutes in production.
 * Waze records age out via the partition-drop strategy in cleanup.ts
 * — that's enough to prevent overflow, even without per-row tombstones.
 *
 * Returns the number of tombstones inserted across all tables.
 */
export async function sweepStaleAsEnded(pool: Pool): Promise<number> {
  const tables: ArchiveTable[] = [
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
