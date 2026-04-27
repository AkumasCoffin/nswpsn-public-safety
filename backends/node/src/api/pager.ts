/**
 * GET /api/pager/hits — pager hits as a GeoJSON FeatureCollection.
 *
 * Mirrors python's `/api/pager/hits` route at external_api_proxy.py:12449.
 * Queries archive_misc directly (one row per upstream pager message,
 * since the pager source's `archiveItems` extractor fans out per-msg)
 * with the same DISTINCT-ON-source_id-then-newest pattern python uses
 * against data_history. Without this DB pass, earlier revisions only
 * served the latest 100 in-memory messages and silently no-op'd
 * `?capcode=` / `?incident_id=` filters.
 */
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { pagerSnapshot, type PagerMessage } from '../sources/pager.js';

export const pagerRouter = new Hono();

interface PagerFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    id: string;
    pager_msg_id: string | number | null;
    incident_id: string | null;
    capcode: string | null;
    alias: string | null;
    agency: string | null;
    message: string;
    incident_time: string | null;
    fetched_at: number;
    timestamp: number | null;
    is_live: boolean;
    lat: number;
    lon: number;
  };
}

interface PagerArchiveRow {
  source_id: string | null;
  lat: number | null;
  lng: number | null;
  category: string | null;
  subcategory: string | null;
  fetched_at: Date;
  data: Record<string, unknown> | null;
}

/** In-memory fallback: walk the live snapshot like the original Node
 *  port did. Used when the DB pool isn't available (ENV missing, or DB
 *  briefly unreachable) so the route never 500s. */
function snapshotFallback(
  hours: number,
  limit: number,
  capcode: string | null,
  incidentId: string | null,
): PagerFeature[] {
  const cutoff = Math.floor(Date.now() / 1000) - hours * 3600;
  const out: PagerFeature[] = [];
  const snap = pagerSnapshot();
  for (const m of snap.messages as PagerMessage[]) {
    if (m.timestamp !== null && m.timestamp < cutoff) continue;
    if (capcode && m.capcode !== capcode) continue;
    if (incidentId && m.incident_id !== incidentId) continue;
    out.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [m.lon, m.lat] },
      properties: {
        id: String(m.id),
        pager_msg_id: m.id,
        incident_id: m.incident_id,
        capcode: m.capcode,
        alias: m.alias,
        agency: m.agency,
        message: m.message,
        incident_time: m.incident_time,
        fetched_at: m.timestamp ?? Math.floor(Date.now() / 1000),
        timestamp: m.timestamp,
        is_live: true,
        lat: m.lat,
        lon: m.lon,
      },
    });
    if (out.length >= limit) break;
  }
  return out;
}

pagerRouter.get('/api/pager/hits', async (c) => {
  const url = new URL(c.req.url);
  const hoursParam = url.searchParams.get('hours');
  const limitParam = url.searchParams.get('limit');
  const capcode = url.searchParams.get('capcode');
  const incidentId = url.searchParams.get('incident_id');

  let hours = Number.parseInt(hoursParam ?? '24', 10);
  if (!Number.isFinite(hours) || hours <= 0) hours = 24;
  hours = Math.min(hours, 168); // 7-day cap, matches python.

  let limit = Number.parseInt(limitParam ?? '500', 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 500;
  limit = Math.min(limit, 2000);

  const cutoff = Math.floor(Date.now() / 1000) - hours * 3600;

  const pool = await getPool();
  if (!pool) {
    const features = snapshotFallback(hours, limit, capcode, incidentId);
    return c.json({
      type: 'FeatureCollection',
      features,
      count: features.length,
      hours,
    });
  }

  // Build the WHERE clause incrementally so optional filters slot in
  // with stable parameter indexes. Filter on `data->>'timestamp'`
  // (the upstream pager incident-time unix int) rather than fetched_at
  // — fetched_at is when WE polled, which lags the actual incident.
  const where: string[] = [
    "source = 'pager'",
    `(data->>'timestamp')::bigint >= $1`,
  ];
  const params: unknown[] = [cutoff];
  if (capcode) {
    params.push(capcode);
    where.push(`subcategory = $${params.length}`);
  }
  if (incidentId) {
    params.push(incidentId);
    where.push(`data->>'incident_id' = $${params.length}`);
  }
  params.push(limit);
  const limitIdx = params.length;

  // DISTINCT ON (source_id) keeps the newest row per upstream message
  // id; outer ORDER BY then re-sorts by fetched_at DESC. Mirrors
  // python's MAX(fetched_at) self-join trick at external_api_proxy.py:12495.
  const sql = `
    SELECT * FROM (
      SELECT DISTINCT ON (source_id)
        source_id,
        lat,
        lng,
        category,
        subcategory,
        fetched_at,
        data
      FROM archive_misc
      WHERE ${where.join(' AND ')}
      ORDER BY source_id, fetched_at DESC
    ) x
    ORDER BY fetched_at DESC
    LIMIT $${limitIdx}
  `;

  let rows: PagerArchiveRow[] = [];
  try {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      try {
        await client.query("SET LOCAL statement_timeout = '30s'");
        const result = await client.query<PagerArchiveRow>(sql, params);
        rows = result.rows;
        await client.query('COMMIT');
      } catch (err) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw err;
      }
    } finally {
      client.release();
    }
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'pager/hits: archive_misc query failed; falling back to snapshot',
    );
    const features = snapshotFallback(hours, limit, capcode, incidentId);
    return c.json({
      type: 'FeatureCollection',
      features,
      count: features.length,
      hours,
    });
  }

  const features: PagerFeature[] = [];
  for (const r of rows) {
    if (r.lat === null || r.lng === null) continue;
    const data = (r.data ?? {}) as Record<string, unknown>;
    const tsRaw = data['timestamp'];
    const ts = typeof tsRaw === 'number' ? tsRaw : tsRaw != null ? Number(tsRaw) : null;
    const fetchedAtSecs = Math.floor(r.fetched_at.getTime() / 1000);
    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [r.lng, r.lat] },
      properties: {
        id: r.source_id ?? String(data['id'] ?? ''),
        pager_msg_id: data['id'] != null ? (data['id'] as string | number) : null,
        incident_id: typeof data['incident_id'] === 'string' ? data['incident_id'] : null,
        capcode: r.subcategory ?? (typeof data['capcode'] === 'string' ? data['capcode'] : null),
        alias: typeof data['alias'] === 'string' ? data['alias'] : null,
        agency: r.category ?? (typeof data['agency'] === 'string' ? data['agency'] : null),
        message: typeof data['message'] === 'string' ? data['message'] : '',
        incident_time:
          typeof data['incident_time'] === 'string' ? data['incident_time'] : null,
        fetched_at: fetchedAtSecs,
        timestamp: Number.isFinite(ts) ? (ts as number) : null,
        is_live: true,
        lat: r.lat,
        lon: r.lng,
      },
    });
  }

  return c.json({
    type: 'FeatureCollection',
    features,
    count: features.length,
    hours,
  });
});
