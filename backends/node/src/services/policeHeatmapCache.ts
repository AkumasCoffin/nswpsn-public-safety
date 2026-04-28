/**
 * Background refresher for the materialised police_heatmap_cache table.
 *
 * Mirrors python's `_refresh_police_heatmap_cache` (external_api_proxy.py
 * around line 10236). Why we need this instead of aggregating live:
 *
 * The heatmap query is `SELECT FLOOR(lat/0.001), FLOOR(lng/0.001),
 * COUNT(*) FROM archive_waze WHERE source='waze_police' AND fetched_at
 * >= now()-30d GROUP BY 1,2`. On 290k rows with ongoing writes and
 * occasional UPDATE-driven dead-tuple bloat, this reliably runs over
 * 60s and hits statement_timeout. Python solved it by pre-aggregating
 * every 10 min in background, serving requests from a tiny indexed
 * cache table — request-side latency drops from 5-60s to sub-ms.
 *
 * Refresh flow:
 *   1. Aggregate from archive_waze with statement_timeout=0 (allowed
 *      to take as long as it needs; only the background path is held
 *      up, not user requests).
 *   2. Build a temp table with the new data.
 *   3. TRUNCATE + INSERT into police_heatmap_cache atomically.
 *   4. The route handler reads from the cache table.
 *
 * If the refresh fails (e.g. archive_waze locked by autovacuum), the
 * existing cache table contents persist — readers keep seeing stale
 * data rather than nothing. Better than the in-memory cache which
 * empties on process restart.
 */
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';

// Same bin grain as the live aggregation (~110m at NSW latitudes).
const BIN_DEG = 0.001;
const WINDOW_DAYS = 30;
const REFRESH_INTERVAL_MS = 10 * 60_000; // 10 min, matches python

let timer: NodeJS.Timeout | null = null;
let refreshing = false;
let lastRefreshAt = 0;
let lastRefreshOk = false;
let lastRefreshMs = 0;
let lastBinCount = 0;

export interface PoliceHeatmapStats {
  last_refresh_age_secs: number | null;
  last_refresh_ms: number;
  last_refresh_ok: boolean;
  bins: number;
}

export function policeHeatmapCacheStats(): PoliceHeatmapStats {
  return {
    last_refresh_age_secs: lastRefreshAt
      ? Math.floor(Date.now() / 1000) - lastRefreshAt
      : null,
    last_refresh_ms: lastRefreshMs,
    last_refresh_ok: lastRefreshOk,
    bins: lastBinCount,
  };
}

/**
 * One refresh pass. Runs the heavy aggregation against archive_waze
 * with statement_timeout=0 (no cap) and atomically swaps the result
 * into police_heatmap_cache. Idempotent — safe to call manually for
 * an immediate refresh.
 */
export async function refreshPoliceHeatmapCache(): Promise<{ ok: boolean; bins: number; ms: number }> {
  if (refreshing) {
    return { ok: false, bins: 0, ms: 0 };
  }
  refreshing = true;
  const startedAt = Date.now();
  try {
    const pool = await getPool();
    if (!pool) return { ok: false, bins: 0, ms: 0 };
    const client = await pool.connect();
    try {
      // Unlimit timeout — this aggregation is the whole point of the
      // background path, no reason to cap it.
      await client.query('SET statement_timeout = 0');

      // Build the new aggregation in a temp table. Use the SAME
      // subcategory normalisation the route handler uses
      // (COALESCE-with-POLICE_VISIBLE-default) so the cache rows are
      // already classified correctly.
      await client.query(`DROP TABLE IF EXISTS police_heatmap_cache_new`);
      await client.query(`
        CREATE TEMP TABLE police_heatmap_cache_new (
          lat_bin     DOUBLE PRECISION NOT NULL,
          lng_bin     DOUBLE PRECISION NOT NULL,
          subcategory TEXT             NOT NULL,
          count       INTEGER          NOT NULL
        )
      `);
      const t0 = Date.now();
      await client.query(
        `INSERT INTO police_heatmap_cache_new (lat_bin, lng_bin, subcategory, count)
         SELECT
           (FLOOR(lat / $1) * $1)::float8 AS lat_bin,
           (FLOOR(lng / $1) * $1)::float8 AS lng_bin,
           COALESCE(NULLIF(subcategory, ''), data->>'subtype', 'POLICE_VISIBLE') AS subcategory,
           COUNT(*)::int AS count
         FROM archive_waze
         WHERE source = 'waze_police'
           AND fetched_at >= NOW() - ($2 || ' days')::interval
           AND lat IS NOT NULL AND lng IS NOT NULL
         GROUP BY 1, 2, 3`,
        [BIN_DEG, String(WINDOW_DAYS)],
      );
      const aggMs = Date.now() - t0;

      // Atomic swap: TRUNCATE + INSERT inside a single transaction so
      // readers either see the old data or the new — never partial.
      await client.query('BEGIN');
      try {
        await client.query('TRUNCATE police_heatmap_cache');
        const r = await client.query<{ n: string }>(
          `INSERT INTO police_heatmap_cache (lat_bin, lng_bin, subcategory, count, updated_at)
           SELECT lat_bin, lng_bin, subcategory, count, NOW()
           FROM police_heatmap_cache_new
           RETURNING 1`,
        );
        const bins = r.rowCount ?? 0;
        await client.query('COMMIT');
        await client.query('DROP TABLE IF EXISTS police_heatmap_cache_new');
        const ms = Date.now() - startedAt;
        lastRefreshAt = Math.floor(Date.now() / 1000);
        lastRefreshOk = true;
        lastRefreshMs = ms;
        lastBinCount = bins;
        log.info(
          { bins, agg_ms: aggMs, total_ms: ms },
          'police-heatmap cache refreshed',
        );
        return { ok: true, bins, ms };
      } catch (err) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw err;
      }
    } finally {
      client.release();
    }
  } catch (err) {
    lastRefreshAt = Math.floor(Date.now() / 1000);
    lastRefreshOk = false;
    lastRefreshMs = Date.now() - startedAt;
    log.warn(
      { err: (err as Error).message, ms: lastRefreshMs },
      'police-heatmap cache refresh failed',
    );
    return { ok: false, bins: 0, ms: lastRefreshMs };
  } finally {
    refreshing = false;
  }
}

interface CacheRow {
  lat_bin: number;
  lng_bin: number;
  subcategory: string;
  count: number;
}

/**
 * Read the heatmap from the cache table, filtered by subtype/bbox.
 * Sub-millisecond on the cache hit — the route handler swaps to this
 * instead of running the live aggregation.
 */
export async function readPoliceHeatmapCache(
  subtypes: string[] | null,
  bbox: [number, number, number, number] | null,
): Promise<{ points: [number, number, number][]; total: number; max: number } | null> {
  const pool = await getPool();
  if (!pool) return null;

  const conditions: string[] = [];
  const params: unknown[] = [];
  if (subtypes && subtypes.length > 0) {
    const placeholders = subtypes.map((_, i) => `$${i + 1}`).join(',');
    conditions.push(`subcategory IN (${placeholders})`);
    for (const s of subtypes) params.push(s);
  }
  if (bbox) {
    const i = params.length;
    conditions.push(
      `lat_bin BETWEEN $${i + 1} AND $${i + 2} AND lng_bin BETWEEN $${i + 3} AND $${i + 4}`,
    );
    params.push(bbox[0], bbox[2], bbox[1], bbox[3]);
  }
  const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  const sql = `
    SELECT lat_bin, lng_bin, subcategory, count
    FROM police_heatmap_cache
    ${where}
  `;

  try {
    const r = await pool.query<CacheRow>(sql, params);
    // Group by (lat_bin, lng_bin), summing counts across subtypes.
    // Most common case: caller asks for one subtype, so this is a
    // pass-through. When asking for multiple subtypes that share a
    // bin location, we sum.
    const merged = new Map<string, [number, number, number]>();
    for (const row of r.rows) {
      const key = `${row.lat_bin}|${row.lng_bin}`;
      const existing = merged.get(key);
      if (existing) existing[2] += row.count;
      else merged.set(key, [row.lat_bin, row.lng_bin, row.count]);
    }
    const points = Array.from(merged.values()).sort((a, b) => b[2] - a[2]);
    return {
      points,
      total: points.reduce((acc, p) => acc + p[2], 0),
      max: points.length > 0 ? (points[0]?.[2] ?? 0) : 0,
    };
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'police-heatmap cache read failed',
    );
    return null;
  }
}

/** Start the periodic refresh loop. Idempotent. */
export function startPoliceHeatmapCacheRefresh(): void {
  if (timer) return;
  // First refresh 30s after boot — give the rest of startup room to
  // run, then kick the heavy aggregation off.
  setTimeout(() => void refreshPoliceHeatmapCache(), 30_000).unref?.();
  timer = setInterval(() => void refreshPoliceHeatmapCache(), REFRESH_INTERVAL_MS);
  timer.unref?.();
}

export function stopPoliceHeatmapCacheRefresh(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}
