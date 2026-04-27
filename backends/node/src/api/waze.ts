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
const HEATMAP_BIN_DEG = 0.05; // ~5km at NSW latitudes
const HEATMAP_MAX_BINS = 1500;
const HEATMAP_WINDOW_DAYS = 30;
const HEATMAP_CACHE_TTL_MS = 60_000;
const POLICE_VALID_SUBTYPES = new Set([
  'POLICE_VISIBLE',
  'POLICE_HIDDEN',
  'POLICE_HIDING',
  'POLICE_WITH_MOBILE_CAMERA',
]);

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
    subtypeClause = `AND (data->>'subtype') IN (${placeholders})`;
    for (const s of subtypes) params.push(s);
  }
  let bboxClause = '';
  if (bbox) {
    const i = params.length;
    bboxClause = `AND lat BETWEEN $${i + 1} AND $${i + 2} AND lng BETWEEN $${i + 3} AND $${i + 4}`;
    params.push(bbox[0], bbox[2], bbox[1], bbox[3]);
  }

  const sql = `
    SELECT
      (FLOOR(lat / ${HEATMAP_BIN_DEG}) * ${HEATMAP_BIN_DEG})::float8 AS lat_bin,
      (FLOOR(lng / ${HEATMAP_BIN_DEG}) * ${HEATMAP_BIN_DEG})::float8 AS lng_bin,
      COUNT(*)::int AS cnt
    FROM archive_waze
    WHERE source = $1
      AND fetched_at >= now() - ($2 || ' days')::interval
      AND lat IS NOT NULL AND lng IS NOT NULL
      ${subtypeClause}
      ${bboxClause}
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
  return c.json(result);
});

export function _resetHeatmapCacheForTests(): void {
  heatmapCache.clear();
}
