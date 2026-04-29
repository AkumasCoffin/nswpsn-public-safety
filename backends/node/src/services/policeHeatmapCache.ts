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
import { brotliCompressSync, constants as zlibConstants } from 'node:zlib';
import { createHash } from 'node:crypto';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';

// Same bin grain as the live aggregation (~110m at NSW latitudes).
const BIN_DEG = 0.001;
const WINDOW_DAYS = 30;
const REFRESH_INTERVAL_MS = 10 * 60_000; // 10 min, matches python
const HEATMAP_MAX_BINS = 60_000;

const POLICE_SUBTYPES_SORTED = [
  'POLICE_HIDING',
  'POLICE_VISIBLE',
  'POLICE_WITH_MOBILE_CAMERA',
] as const;

let timer: NodeJS.Timeout | null = null;
let refreshing = false;
let lastRefreshAt = 0;
let lastRefreshOk = false;
let lastRefreshMs = 0;
let lastBinCount = 0;

// Pre-rendered buffers per common subtype combo. Built at refresh time
// so the route handler can ship a body + Content-Encoding: br straight
// to the client without doing JSON.stringify or Brotli on every GET.
// `null` (any-subtype) maps to the 'ALL' key; explicit combos use the
// sorted-comma-joined subtype list. Stored as ArrayBuffer (not Buffer)
// because Hono's c.body() accepts ArrayBuffer / ReadableStream / string,
// not the Node-flavoured Uint8Array that Buffer is.
export interface PreRenderedHeatmap {
  jsonAB: ArrayBuffer;
  brAB: ArrayBuffer;
  etag: string;
  updatedAt: Date;
  binCount: number;
}
let preRenderedCache: Map<string, PreRenderedHeatmap> = new Map();

function toArrayBuffer(buf: Buffer): ArrayBuffer {
  // Buffers from brotliCompressSync / Buffer.from share their backing
  // ArrayBuffer with the Node Buffer pool — slicing gives us a clean
  // standalone copy that's safe to hand to a web Response.
  const out = new ArrayBuffer(buf.byteLength);
  new Uint8Array(out).set(buf);
  return out;
}

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
 * Cache key for the pre-rendered heatmap. `null`/empty subtypes and the
 * full canonical 3-subtype combo both map to 'ALL' — they're functionally
 * identical reads since the subcategory column only ever holds those
 * three values.
 */
export function preRenderKey(subtypes: string[] | null | undefined): string {
  if (!subtypes || subtypes.length === 0) return 'ALL';
  const sorted = [...subtypes].sort();
  if (
    sorted.length === POLICE_SUBTYPES_SORTED.length &&
    sorted.every((s, i) => s === POLICE_SUBTYPES_SORTED[i])
  ) {
    return 'ALL';
  }
  return sorted.join(',');
}

/**
 * Look up a pre-rendered heatmap by subtype combo. Returns null for
 * combos that weren't pre-rendered (e.g. unusual subtype permutations
 * or any bbox-filtered request). Bbox queries always fall through to
 * the live read path — pre-rendering them would multiply the cache
 * size by every possible bbox.
 */
export function getPreRenderedHeatmap(
  subtypes: string[] | null,
  bbox: [number, number, number, number] | null,
): PreRenderedHeatmap | null {
  if (bbox !== null) return null;
  return preRenderedCache.get(preRenderKey(subtypes)) ?? null;
}

// Minimal pg-client shape. Mirrors the bits of pg.PoolClient we touch
// without pulling the whole type tree into this module.
interface PgClient {
  query: (
    sql: string,
    params?: unknown[],
  ) => Promise<{ rows: unknown[]; rowCount: number | null }>;
}

interface BinRow {
  // pg returns DOUBLE PRECISION as string by default (no parseFloat
  // type-cast configured). We coerce to number when reading rows.
  lat_bin: number | string;
  lng_bin: number | string;
  subcategory: string;
  count: number | string;
}

/**
 * Build the response body + ETag + Brotli payload for one subtype combo.
 * Reads from the just-populated `police_heatmap_cache` table (still on
 * the same client/transaction the refresh used). The result mirrors the
 * shape served by the route handler.
 */
async function buildPreRendered(
  client: PgClient,
  subtypes: readonly string[] | null,
  updatedAt: Date,
): Promise<PreRenderedHeatmap> {
  let sql: string;
  let params: unknown[];
  if (subtypes && subtypes.length > 0) {
    const placeholders = subtypes.map((_, i) => `$${i + 1}`).join(',');
    sql = `SELECT lat_bin, lng_bin, subcategory, count
             FROM police_heatmap_cache
            WHERE subcategory IN (${placeholders})`;
    params = [...subtypes];
  } else {
    sql = `SELECT lat_bin, lng_bin, subcategory, count
             FROM police_heatmap_cache`;
    params = [];
  }
  const r = await client.query(sql, params);

  // Group by (lat_bin, lng_bin), summing across subcategories. Coerce
  // every numeric pg column up front — DOUBLE PRECISION arrives as a
  // string under the default pg type parsers, so subsequent .toFixed()
  // would throw without this.
  const merged = new Map<string, [number, number, number]>();
  for (const raw of r.rows) {
    const row = raw as BinRow;
    const lat = Number(row.lat_bin);
    const lng = Number(row.lng_bin);
    const cnt = Number(row.count);
    const key = `${lat}|${lng}`;
    const existing = merged.get(key);
    if (existing) existing[2] += cnt;
    else merged.set(key, [lat, lng, cnt]);
  }
  const sorted = Array.from(merged.values()).sort((a, b) => b[2] - a[2]);
  const points = sorted.slice(0, HEATMAP_MAX_BINS).map(
    ([lat, lng, cnt]): [number, number, number] => [
      Number(lat.toFixed(5)),
      Number(lng.toFixed(5)),
      cnt,
    ],
  );
  const total_records = sorted.reduce((acc, p) => acc + p[2], 0);
  const max_count = points.length > 0 ? (points[0]?.[2] ?? 0) : 0;

  const responseSubtypes = subtypes
    ? [...subtypes].sort()
    : [...POLICE_SUBTYPES_SORTED];
  const body = {
    points,
    total_records,
    bin_size_deg: BIN_DEG,
    max_count,
    days: WINDOW_DAYS,
    subtypes: responseSubtypes,
    cache_updated_at: updatedAt.toISOString(),
    cache_status: 'materialised' as const,
  };
  const json = JSON.stringify(body);
  const jsonBuf = Buffer.from(json, 'utf8');
  // Brotli quality 6 — good size for the ~10 min refresh budget; q=11
  // is ~3× slower for ~5% smaller output, not worth it.
  const brBuf = brotliCompressSync(jsonBuf, {
    params: { [zlibConstants.BROTLI_PARAM_QUALITY]: 6 },
  });
  const etag = `"${createHash('sha1').update(jsonBuf).digest('hex').slice(0, 16)}"`;
  return {
    jsonAB: toArrayBuffer(jsonBuf),
    brAB: toArrayBuffer(brBuf),
    etag,
    updatedAt,
    binCount: points.length,
  };
}

/**
 * Rebuild the pre-rendered map for every common subtype combo. Built
 * atomically — readers either see the entire previous snapshot or the
 * entire new one, never a partial mix.
 */
async function rebuildPreRenderedCache(
  client: PgClient,
  updatedAt: Date,
): Promise<void> {
  const next = new Map<string, PreRenderedHeatmap>();
  const combos: Array<{ key: string; subtypes: readonly string[] | null }> = [
    { key: 'ALL', subtypes: null },
    { key: 'POLICE_HIDING', subtypes: ['POLICE_HIDING'] },
    { key: 'POLICE_VISIBLE', subtypes: ['POLICE_VISIBLE'] },
    { key: 'POLICE_WITH_MOBILE_CAMERA', subtypes: ['POLICE_WITH_MOBILE_CAMERA'] },
  ];
  for (const combo of combos) {
    next.set(combo.key, await buildPreRendered(client, combo.subtypes, updatedAt));
  }
  preRenderedCache = next;
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
      // Aggregate from the day-bucketed table written incrementally by
      // the archive writer (see store/archive.ts:upsertPoliceHeatmapBin
      // Daily). Replaces the 30-day GROUP BY scan over archive_waze that
      // sat in IO/AioIoCompletion for 30+ minutes per refresh — the
      // pre-aggregated source means each refresh sums at most ~30 ×
      // (cells per day) rows, typically <100k rows total.
      //
      // Fall back to the legacy archive_waze scan if the daily table is
      // empty (i.e. the indexBuilder backfill hasn't completed yet).
      // Keeps the heatmap warm during the first hour or so after deploy.
      let aggMs = 0;
      // SELECT 1 ... LIMIT 1 stops at the first row found — O(1)
      // existence check. The earlier COUNT(*) version full-scanned
      // the table just to know if it had any rows.
      const dailyCheck = await client.query<{ n: number }>(
        `SELECT 1 AS n FROM police_heatmap_bin_daily LIMIT 1`,
      );
      const dailyHasRows = (dailyCheck.rowCount ?? 0) > 0;
      if (dailyHasRows) {
        await client.query(
          `INSERT INTO police_heatmap_cache_new (lat_bin, lng_bin, subcategory, count)
           SELECT lat_bin, lng_bin, subcategory, SUM(count)::int AS count
             FROM police_heatmap_bin_daily
            WHERE day >= (NOW() - ($1 || ' days')::interval)::date
            GROUP BY 1, 2, 3`,
          [String(WINDOW_DAYS)],
        );
        aggMs = Date.now() - t0;
      } else {
        log.warn('police_heatmap_bin_daily empty; falling back to archive_waze scan');
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
        aggMs = Date.now() - t0;
      }

      // Atomic swap: TRUNCATE + INSERT inside a single transaction so
      // readers either see the old data or the new — never partial.
      await client.query('BEGIN');
      let bins = 0;
      const updatedAt = new Date();
      try {
        await client.query('TRUNCATE police_heatmap_cache');
        const ins = await client.query(
          `INSERT INTO police_heatmap_cache (lat_bin, lng_bin, subcategory, count, updated_at)
           SELECT lat_bin, lng_bin, subcategory, count, $1
           FROM police_heatmap_cache_new`,
          [updatedAt],
        );
        bins = ins.rowCount ?? 0;
        await client.query('COMMIT');
      } catch (err) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw err;
      }
      await client.query('DROP TABLE IF EXISTS police_heatmap_cache_new');

      // Build pre-rendered Buffers for the common subtype combos.
      // Reads against the just-populated cache table, so this is cheap
      // (~tens of ms total). Failures here don't roll back the swap —
      // readers fall back to readPoliceHeatmapCache for any combo we
      // couldn't pre-render.
      const preT0 = Date.now();
      try {
        await rebuildPreRenderedCache(client, updatedAt);
      } catch (err) {
        log.warn(
          { err: (err as Error).message },
          'police-heatmap pre-render rebuild failed (cache table still updated)',
        );
      }
      const preMs = Date.now() - preT0;

      const ms = Date.now() - startedAt;
      lastRefreshAt = Math.floor(Date.now() / 1000);
      lastRefreshOk = true;
      lastRefreshMs = ms;
      lastBinCount = bins;
      log.info(
        `police-heatmap cache refreshed: ${bins} bins, agg=${aggMs}ms, prerender=${preMs}ms, total=${ms}ms`,
      );
      return { ok: true, bins, ms };
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
  // DOUBLE PRECISION columns arrive as strings under default pg parsers.
  lat_bin: number | string;
  lng_bin: number | string;
  subcategory: string;
  count: number | string;
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
      const lat = Number(row.lat_bin);
      const lng = Number(row.lng_bin);
      const cnt = Number(row.count);
      const key = `${lat}|${lng}`;
      const existing = merged.get(key);
      if (existing) existing[2] += cnt;
      else merged.set(key, [lat, lng, cnt]);
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
