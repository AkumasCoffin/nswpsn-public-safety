/**
 * GET /api/waze/{police,hazards,roadwork,metrics}
 *
 * All read from the in-memory WazeIngestCache (LiveStore-backed). No DB
 * on the request path. Response shapes match the Python backend so the
 * frontend doesn't need to know which backend served the request.
 */
import { Hono } from 'hono';
import { snapshot } from '../store/wazeIngestCache.js';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import {
  isHazardAlert,
  isPoliceAlert,
  isRoadworkAlert,
  parseWazeAlert,
  parseWazeJam,
} from '../services/wazeAlerts.js';

export const wazeRouter = new Hono();

wazeRouter.get('/api/waze/police', (c) => {
  const { alerts } = snapshot();
  const features = alerts
    .filter(isPoliceAlert)
    .map((a) => parseWazeAlert(a, 'Police'))
    .filter((f) => f !== null);
  return c.json({
    type: 'FeatureCollection',
    features,
    count: features.length,
  });
});

wazeRouter.get('/api/waze/roadwork', (c) => {
  const { alerts } = snapshot();
  const features = alerts
    .filter(isRoadworkAlert)
    .map((a) => parseWazeAlert(a, 'Roadwork'))
    .filter((f) => f !== null);
  return c.json({
    type: 'FeatureCollection',
    features,
    count: features.length,
  });
});

wazeRouter.get('/api/waze/hazards', (c) => {
  const { alerts, jams } = snapshot();
  const features = alerts
    .filter(isHazardAlert)
    .map((a) => parseWazeAlert(a, 'Hazard'))
    .filter((f) => f !== null);
  const jamFeatures = jams
    .map((j) => parseWazeJam(j))
    .filter((f) => f !== null);
  return c.json({
    type: 'FeatureCollection',
    features,
    jams: jamFeatures,
    count: features.length,
    jamCount: jamFeatures.length,
  });
});

/**
 * Lightweight metrics endpoint. Mirrors python /api/waze/metrics.
 * Just exposes the LiveStore counters; no rolling block-rate window
 * yet (the userscript is the only ingest path now and there's no
 * "Waze blocked us" failure mode for this backend to track).
 */
wazeRouter.get('/api/waze/metrics', (c) => {
  const s = snapshot();
  return c.json({
    regions_cached: s.regions_cached,
    last_ingest_age_secs: s.last_ingest_age_secs,
    alert_count: s.alerts.length,
    jam_count: s.jams.length,
  });
});

// /api/waze/alerts — combined categorised view (hazards + police +
// roadwork + jams + other). Mirrors python line 9937-10008. Single
// pass over the snapshot, dispatched by the same isXxxAlert helpers
// used by the per-category endpoints.
wazeRouter.get('/api/waze/alerts', (c) => {
  const { alerts, jams } = snapshot();
  const hazards: ReturnType<typeof parseWazeAlert>[] = [];
  const police: ReturnType<typeof parseWazeAlert>[] = [];
  const roadwork: ReturnType<typeof parseWazeAlert>[] = [];
  const other: ReturnType<typeof parseWazeAlert>[] = [];
  for (const a of alerts) {
    if (isPoliceAlert(a)) {
      const f = parseWazeAlert(a, 'Police');
      if (f) police.push(f);
    } else if (isRoadworkAlert(a)) {
      const f = parseWazeAlert(a, 'Roadwork');
      if (f) roadwork.push(f);
    } else if (isHazardAlert(a)) {
      const f = parseWazeAlert(a, 'Hazard');
      if (f) hazards.push(f);
    } else {
      const f = parseWazeAlert(a, 'Other');
      if (f) other.push(f);
    }
  }
  const jamFeatures = jams.map((j) => parseWazeJam(j)).filter((f) => f !== null);
  return c.json({
    hazards: { type: 'FeatureCollection', features: hazards, count: hazards.length },
    police: { type: 'FeatureCollection', features: police, count: police.length },
    roadwork: { type: 'FeatureCollection', features: roadwork, count: roadwork.length },
    jams: { type: 'FeatureCollection', features: jamFeatures, count: jamFeatures.length },
    other: { type: 'FeatureCollection', features: other, count: other.length },
    totalCount: alerts.length,
    jamCount: jamFeatures.length,
  });
});

// /api/waze/types — debug aggregate of (type, subtype) frequencies in
// the current snapshot. Mirrors python line 10424-10443.
wazeRouter.get('/api/waze/types', (c) => {
  const { alerts, jams, regions_cached } = snapshot();
  const types: Record<string, { count: number; subtypes: Record<string, number> }> = {};
  for (const a of alerts) {
    const raw = a as Record<string, unknown>;
    const t = String(raw['type'] ?? 'UNKNOWN');
    const sub = String(raw['subtype'] ?? '');
    if (!types[t]) types[t] = { count: 0, subtypes: {} };
    types[t].count += 1;
    if (sub) {
      types[t].subtypes[sub] = (types[t].subtypes[sub] ?? 0) + 1;
    }
  }
  return c.json({
    types,
    totalAlerts: alerts.length,
    totalJams: jams.length,
    regionsQueried: regions_cached,
    note: 'Aggregated from userscript ingest snapshot (Node backend)',
  });
});

// /api/waze/raw — raw alerts list from the snapshot. Mirrors python
// line 10446-10451 but serves from the in-memory ingest cache (the
// Node backend never fetches Waze upstream — userscript ingest only).
wazeRouter.get('/api/waze/raw', (c) => {
  const { alerts } = snapshot();
  return c.json({ alerts, count: alerts.length });
});

// /api/waze/debug — informational pointer. Python's debug endpoint pokes
// Waze upstream with curl_cffi to see what they're returning right now;
// the Node backend has no upstream fetch path (userscript-only ingest
// pattern), so we surface the snapshot state instead.
wazeRouter.get('/api/waze/debug', (c) => {
  const s = snapshot();
  return c.json({
    backend: 'node',
    note: 'Node backend has no direct Waze upstream. Userscript-only ingest.',
    last_ingest_age_secs: s.last_ingest_age_secs,
    regions_cached: s.regions_cached,
    alerts_count: s.alerts.length,
    jams_count: s.jams.length,
    sample_alert_keys:
      s.alerts.length > 0
        ? Object.keys(s.alerts[0] as Record<string, unknown>).slice(0, 15)
        : null,
  });
});

// /api/waze/police-heatmap — bbox-binned police alert aggregation
// over a rolling 30-day window. Reads from archive_waze (the
// partitioned table the poller + migration script populate). Cached
// in-process for 60s so dashboard refreshes don't hammer the DB.
//
// Bin/cap sized to match python's settings at external_api_proxy.py:
// 10102-10110. Earlier revisions used a 5 km / 1500-bin grid which
// silently dropped count=1 suburban bins — a pin would render without
// any hex underneath. With BIN_DEG=0.001 (~110 m) and MAX_BINS=60000
// the cache holds the full live picture; bbox filtering still keeps
// per-request payloads small on tight zooms.
const HEATMAP_BIN_DEG = 0.001; // ~110 m at NSW latitudes (matches python)
const HEATMAP_MAX_BINS = 60_000;
const HEATMAP_WINDOW_DAYS = 30;
const HEATMAP_CACHE_TTL_MS = 60_000;
// Python ships POLICE_HIDDEN in the live snapshot but the canonical
// validator set has 3 subtypes — mirror that for query-string parity.
const POLICE_VALID_SUBTYPES = new Set([
  'POLICE_VISIBLE',
  'POLICE_HIDING',
  'POLICE_WITH_MOBILE_CAMERA',
]);

// Periodic background refresh of the unfiltered (no subtype, no bbox)
// heatmap so /api/status can report a meaningful bin count and the
// first request after a cache eviction doesn't block on the SQL. Runs
// every HEATMAP_REFRESH_INTERVAL_MS; result lives in the same
// in-process cache we already use for request-scoped responses.
const HEATMAP_REFRESH_INTERVAL_MS = 5 * 60_000; // 5 min
let heatmapRefreshTimer: NodeJS.Timeout | null = null;
let heatmapLastRefreshTs: number | null = null;
let heatmapLastBinCount = 0;

interface HeatmapBucket {
  result: unknown;
  ts: number;
}
const heatmapCache = new Map<string, HeatmapBucket>();

function _heatmapCacheKey(
  subtypes: string[],
  bbox: [number, number, number, number] | null,
): string {
  return JSON.stringify({ s: subtypes, b: bbox });
}

interface HeatmapRow {
  lat_bin: number;
  lng_bin: number;
  cnt: number;
}

async function buildHeatmapFromArchive(
  subtypes: string[] | null,
  bbox: [number, number, number, number] | null,
): Promise<{
  points: [number, number, number][];
  total_records: number;
  max_count: number;
  cache_updated_at: string;
} | null> {
  const pool = await getPool();
  if (!pool) return null;

  const params: unknown[] = ['waze_police', HEATMAP_WINDOW_DAYS];
  let subtypeClause = '';
  if (subtypes && subtypes.length > 0) {
    const placeholders = subtypes
      .map((_, i) => `$${params.length + i + 1}`)
      .join(',');
    // COALESCE to 'POLICE_VISIBLE' so rows with NULL/missing subtype
    // (Waze sometimes ships bare type='POLICE' with no subtype) still
    // count when the caller asks for POLICE_VISIBLE. Mirrors python's
    // _waze_police_heatmap loop which does `eff = sub or 'POLICE_VISIBLE'`
    // before the filter check (external_api_proxy.py:10166-10169).
    // Without this the bbox-aware heatmap dropped roughly 90% of pings
    // and showed ~6k where it should have shown ~90k+.
    subtypeClause = `AND COALESCE(data->>'subtype', 'POLICE_VISIBLE') IN (${placeholders})`;
    for (const s of subtypes) params.push(s);
  }
  // Bbox filter applies to the COALESCE-resolved coords (lat_v/lng_v),
  // not the raw column, so we don't drop rows whose lat is in the
  // JSONB blob. Built here as a placeholder string and interpolated
  // into the outer WHERE further down.
  let bboxOuterClause = '';
  if (bbox) {
    const i = params.length;
    bboxOuterClause = `AND lat_v BETWEEN $${i + 1} AND $${i + 2} AND lng_v BETWEEN $${i + 3} AND $${i + 4}`;
    params.push(bbox[0], bbox[2], bbox[1], bbox[3]);
  }

  // COALESCE the dedicated lat/lng columns with the JSONB
  // location.y/.x — python's data_history populated the dedicated
  // columns inconsistently (mostly NULL on historical rows) and the
  // migration faithfully copied those NULLs into archive_waze. The
  // 007 backfill migration patches existing rows; the COALESCE here
  // is the safety net so any future-written row that lands without a
  // lat/lng column but with a usable JSONB coordinate still feeds
  // the heatmap. Only valid numeric strings are accepted (the regex
  // mirrors python's float() coercion).
  const sql = `
    WITH coords AS (
      SELECT
        COALESCE(lat,
          CASE WHEN (data->'location'->>'y') ~ '^-?[0-9.]+$'
               THEN (data->'location'->>'y')::float8 END) AS lat_v,
        COALESCE(lng,
          CASE WHEN (data->'location'->>'x') ~ '^-?[0-9.]+$'
               THEN (data->'location'->>'x')::float8 END) AS lng_v
      FROM archive_waze
      WHERE source = $1
        AND fetched_at >= now() - ($2 || ' days')::interval
        ${subtypeClause}
    )
    SELECT
      (FLOOR(lat_v / ${HEATMAP_BIN_DEG}) * ${HEATMAP_BIN_DEG})::float8 AS lat_bin,
      (FLOOR(lng_v / ${HEATMAP_BIN_DEG}) * ${HEATMAP_BIN_DEG})::float8 AS lng_bin,
      COUNT(*)::int AS cnt
    FROM coords
    WHERE lat_v IS NOT NULL AND lng_v IS NOT NULL
      ${bboxOuterClause}
    GROUP BY lat_bin, lng_bin
    ORDER BY cnt DESC
    LIMIT ${HEATMAP_MAX_BINS}
  `;

  const client = await pool.connect();
  try {
    // SET LOCAL is a no-op outside a transaction (pg semantics) — wrap
    // the query in BEGIN/COMMIT so the 20s timeout actually applies.
    await client.query('BEGIN');
    let r: { rows: HeatmapRow[] };
    try {
      await client.query("SET LOCAL statement_timeout = '20s'");
      r = await client.query<HeatmapRow>(sql, params);
      await client.query('COMMIT');
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch { /* ignore */ }
      throw err;
    }
    const points: [number, number, number][] = r.rows.map((row) => [
      Number(row.lat_bin.toFixed(5)),
      Number(row.lng_bin.toFixed(5)),
      row.cnt,
    ]);
    const max_count = points.length > 0 ? (points[0]?.[2] ?? 0) : 0;
    const total_records = points.reduce((n, p) => n + p[2], 0);
    return {
      points,
      total_records,
      max_count,
      cache_updated_at: new Date().toISOString(),
    };
  } finally {
    client.release();
  }
}

wazeRouter.get('/api/waze/police-heatmap', async (c) => {
  const url = new URL(c.req.url);
  const rawSubtypes = (url.searchParams.get('subtypes') ?? '').trim();
  let wanted: string[] | null = null;
  if (rawSubtypes) {
    wanted = rawSubtypes
      .split(',')
      .map((s) => s.trim().toUpperCase())
      .filter((s) => POLICE_VALID_SUBTYPES.has(s));
  }
  const rawBbox = (url.searchParams.get('bbox') ?? '').trim();
  let bbox: [number, number, number, number] | null = null;
  if (rawBbox) {
    const parts = rawBbox.split(',').map((p) => Number(p));
    if (parts.length === 4 && parts.every(Number.isFinite)) {
      const [s, w, n, e] = parts as [number, number, number, number];
      if (s < n && w < e) bbox = [s, w, n, e];
    }
  }

  const cacheKey = _heatmapCacheKey(wanted ?? [], bbox);
  const now = Date.now();
  const hit = heatmapCache.get(cacheKey);
  if (hit && now - hit.ts < HEATMAP_CACHE_TTL_MS) {
    return c.json(hit.result);
  }

  // Fast path: when only a bbox is filter (no subtype), the unfiltered
  // cache already has every bin we need — just pre-filter to the
  // requested viewport instead of re-running the SQL. Eliminates the
  // 6+ second heatmap latency for the common map-pan/zoom case where
  // the front-end pings the heatmap with each viewport change. Subtype
  // filtering requires a fresh aggregate (counts can't be split out
  // of the merged bin) so it still falls through to buildHeatmap.
  if (bbox && (!wanted || wanted.length === 0)) {
    const baseKey = _heatmapCacheKey([], null);
    const base = heatmapCache.get(baseKey);
    if (base && now - base.ts < HEATMAP_CACHE_TTL_MS) {
      const baseResult = base.result as {
        points: [number, number, number][];
        max_count: number;
        days: number;
        subtypes: string[];
        cache_updated_at: string | null;
      };
      const [s, w, n, e] = bbox;
      const filtered: [number, number, number][] = [];
      for (const p of baseResult.points) {
        const [lat, lng, cnt] = p;
        if (lat >= s && lat <= n && lng >= w && lng <= e) {
          filtered.push(p);
          if (filtered.length >= HEATMAP_MAX_BINS) break;
        }
      }
      const result = {
        points: filtered,
        total_records: filtered.reduce((acc, p) => acc + p[2], 0),
        bin_size_deg: HEATMAP_BIN_DEG,
        max_count: filtered.reduce((m, p) => (p[2] > m ? p[2] : m), 0),
        days: baseResult.days,
        subtypes: Array.from(POLICE_VALID_SUBTYPES).sort(),
        cache_updated_at: baseResult.cache_updated_at,
        cache_status: 'bbox-from-base',
      };
      heatmapCache.set(cacheKey, { result, ts: now });
      return c.json(result);
    }
  }

  const aggregated = await buildHeatmapFromArchive(wanted, bbox).catch((err) => {
    log.warn({ err: (err as Error).message }, 'police-heatmap query failed');
    return null;
  });

  // Fallback to the live in-memory snapshot if the archive query failed
  // (DB down, statement timeout, etc.) — same behaviour python had when
  // its data_history-backed cache was warming up.
  if (!aggregated) {
    const { alerts } = snapshot();
    const bins = new Map<string, { lat: number; lng: number; count: number }>();
    const wantedSet = wanted ? new Set(wanted) : null;
    for (const a of alerts) {
      if (!isPoliceAlert(a)) continue;
      const raw = a as Record<string, unknown>;
      const sub = String(raw['subtype'] ?? '').toUpperCase();
      const eff = sub || 'POLICE_VISIBLE';
      if (wantedSet !== null && !wantedSet.has(eff)) continue;
      const loc = (raw['location'] ?? {}) as Record<string, unknown>;
      const lat = typeof loc['y'] === 'number' ? loc['y'] : null;
      const lng = typeof loc['x'] === 'number' ? loc['x'] : null;
      if (lat === null || lng === null) continue;
      if (bbox && (lat < bbox[0] || lat > bbox[2] || lng < bbox[1] || lng > bbox[3])) continue;
      const latBin = Math.round(lat / HEATMAP_BIN_DEG) * HEATMAP_BIN_DEG;
      const lngBin = Math.round(lng / HEATMAP_BIN_DEG) * HEATMAP_BIN_DEG;
      const key = `${latBin.toFixed(5)}|${lngBin.toFixed(5)}`;
      const existing = bins.get(key);
      if (existing) existing.count += 1;
      else bins.set(key, { lat: latBin, lng: lngBin, count: 1 });
    }
    const sorted = Array.from(bins.values())
      .sort((a, b) => b.count - a.count)
      .slice(0, HEATMAP_MAX_BINS);
    const points = sorted.map(
      (b): [number, number, number] => [
        Number(b.lat.toFixed(5)),
        Number(b.lng.toFixed(5)),
        b.count,
      ],
    );
    const result = {
      points,
      total_records: sorted.reduce((n, b) => n + b.count, 0),
      bin_size_deg: HEATMAP_BIN_DEG,
      max_count: sorted[0]?.count ?? 0,
      days: 0,
      subtypes: wanted ?? Array.from(POLICE_VALID_SUBTYPES).sort(),
      cache_updated_at: null,
      cache_status: 'live-fallback',
      note: 'archive query unavailable — serving live snapshot',
    };
    return c.json(result);
  }

  const result = {
    points: aggregated.points,
    total_records: aggregated.total_records,
    bin_size_deg: HEATMAP_BIN_DEG,
    max_count: aggregated.max_count,
    days: HEATMAP_WINDOW_DAYS,
    subtypes: wanted ?? Array.from(POLICE_VALID_SUBTYPES).sort(),
    cache_updated_at: aggregated.cache_updated_at,
    cache_status: 'ok',
  };
  heatmapCache.set(cacheKey, { result, ts: now });
  // If the user-facing request is the unfiltered shape (no subtype, no
  // bbox), also feed the tracking counters that policeHeatmapStatus
  // exposes. Without this, /api/status reports bins=0 until the
  // background loop fires, even though we just answered a real
  // unfiltered query — confusing to operators.
  if ((wanted === null || wanted.length === 0) && bbox === null) {
    heatmapLastRefreshTs = Math.floor(now / 1000);
    heatmapLastBinCount = aggregated.points.length;
  }
  return c.json(result);
});

export function _resetHeatmapCacheForTests(): void {
  heatmapCache.clear();
  heatmapLastRefreshTs = null;
  heatmapLastBinCount = 0;
}

/**
 * Background refresh of the unfiltered (no subtype, no bbox) heatmap
 * into the in-process cache. Surfaces two bits of state for /api/status:
 * the timestamp of the last successful refresh and the resulting bin
 * count. Runs periodically so the panel reflects reality without an
 * on-demand DB query in the status check.
 *
 * Best-effort: failures are logged at warn-level, the previous cache
 * entry is left in place, and the next interval tries again.
 */
async function refreshHeatmapCache(): Promise<void> {
  const startedAt = Date.now();
  try {
    const aggregated = await buildHeatmapFromArchive(null, null);
    if (!aggregated) {
      // pool isn't ready yet (only happens during the first ~1s of
      // boot). Log so we can tell apart "no data" from "couldn't ask."
      log.debug('police-heatmap refresh: pool not ready, skipping tick');
      return;
    }
    const result = {
      points: aggregated.points,
      total_records: aggregated.total_records,
      bin_size_deg: HEATMAP_BIN_DEG,
      max_count: aggregated.max_count,
      days: HEATMAP_WINDOW_DAYS,
      subtypes: Array.from(POLICE_VALID_SUBTYPES).sort(),
      cache_updated_at: aggregated.cache_updated_at,
      cache_status: 'ok',
    };
    heatmapCache.set(_heatmapCacheKey([], null), { result, ts: Date.now() });
    heatmapLastRefreshTs = Math.floor(Date.now() / 1000);
    heatmapLastBinCount = aggregated.points.length;
    log.info(
      {
        bins: aggregated.points.length,
        total_records: aggregated.total_records,
        ms: Date.now() - startedAt,
      },
      'police-heatmap refreshed',
    );
  } catch (err) {
    log.warn(
      {
        err: (err as Error).message,
        ms: Date.now() - startedAt,
      },
      'police-heatmap background refresh failed',
    );
  }
}

/** Start the periodic refresh loop. Idempotent — safe to call twice. */
export function startHeatmapRefreshLoop(): void {
  if (heatmapRefreshTimer) return;
  // Kick once on boot so the cache is populated before the first
  // dashboard refresh — the first user-facing request gets a hit
  // instead of waiting on the SQL.
  void refreshHeatmapCache();
  heatmapRefreshTimer = setInterval(
    () => void refreshHeatmapCache(),
    HEATMAP_REFRESH_INTERVAL_MS,
  );
  heatmapRefreshTimer.unref?.();
}

export function stopHeatmapRefreshLoop(): void {
  if (heatmapRefreshTimer) {
    clearInterval(heatmapRefreshTimer);
    heatmapRefreshTimer = null;
  }
}

/** Snapshot of police-heatmap freshness for /api/status. */
export function policeHeatmapStatus(): {
  bins: number;
  last_refresh_age_secs: number | null;
} {
  const age =
    heatmapLastRefreshTs === null
      ? null
      : Math.floor(Date.now() / 1000) - heatmapLastRefreshTs;
  return { bins: heatmapLastBinCount, last_refresh_age_secs: age };
}
