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
import { SwrCache } from '../services/swrCache.js';

export const pagerRouter = new Hono();

// SWR cache for the archive_misc-backed query. archive_misc is a heavy
// partitioned table and the JSON-extract WHERE clause regularly takes
// 30 s+ — without this cache the discord-bot polling the route once a
// minute kept the DB pegged AND every other archive_misc consumer
// (archiveLiveness, filterCache) saw cascading statement_timeouts.
//
// Sizing:
//   - fresh = 60 s   → bot poll cadence; almost every call is a hit.
//   - stale = 10 min → background refresh keeps the cache warm; if the
//                       DB times out for ten minutes the entry expires
//                       and the cold path falls back to the snapshot.
const PAGER_HITS_FRESH_MS = 60_000;
const PAGER_HITS_STALE_MS = 10 * 60_000;
const pagerHitsCache = new SwrCache<PagerHitsBody>();

// Circuit breaker for the archive_misc refresh. On disk-saturated
// hosts the (data->>'timestamp')::bigint Seq Scan reliably hits the
// statement timeout. Without this, every cold cache request fires a
// new DB refresh attempt that will fail the same way — flooding the
// logs with `pager/hits: archive_misc refresh failed` warnings and
// burning a connection on each one. After a failure we back off for
// 5 minutes before trying the DB again; in the meantime cold callers
// just get the LiveStore snapshot, which is what the SwrCache fallback
// path already serves them.
let pagerHitsDbBackoffUntil = 0;
const PAGER_HITS_DB_BACKOFF_MS = 5 * 60_000;

interface PagerHitsBody {
  type: 'FeatureCollection';
  features: PagerFeature[];
  count: number;
  hours: number;
}

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
    /** Parsed incident type (e.g. "Bush Fire", "MVA"); '' when not parseable. */
    type: string;
    /** Call class ("FIRECALL"/"INCIDENT CALL"/…); '' when absent. */
    call_class: string;
    /** True when this page is a Stop Message / Stand Down (flag only). */
    is_stop: boolean;
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
    // Coordless rows (FRNSW FRINC turnouts) are logs-only — never mapped.
    if (m.lat === null || m.lon === null) continue;
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
        type: m.type ?? '',
        call_class: m.call_class ?? '',
        is_stop: m.is_stop === true,
        incident_time: m.incident_time,
        fetched_at: m.timestamp ?? Math.floor(Date.now() / 1000),
        timestamp: m.timestamp,
        is_live: true,
        lat: m.lat,
        lon: m.lon,
      },
    });
  }
  // Truncate by INCIDENT time (matching the DB path + the window filter), not by
  // iteration order, so the same `limit` keeps the newest hits and stays monotonic.
  out.sort((a, b) => (b.properties.timestamp ?? -Infinity) - (a.properties.timestamp ?? -Infinity));
  return out.slice(0, limit);
}

/**
 * The DB read itself, factored out so SwrCache can call it. Builds the
 * WHERE clause, runs the DISTINCT-ON query against archive_misc, and
 * marshals rows into PagerFeatures. Throws on DB failure so SwrCache
 * keeps the previous (stale) value instead of caching a fallback.
 */
async function fetchPagerHitsFromDb(opts: {
  hours: number;
  limit: number;
  capcode: string | null;
  incidentId: string | null;
}): Promise<PagerHitsBody> {
  const { hours, limit, capcode, incidentId } = opts;
  const cutoff = Math.floor(Date.now() / 1000) - hours * 3600;
  // archive_misc is PARTITION BY RANGE(fetched_at). Filtering only on the
  // incident time `(data->>'timestamp')` can't prune partitions, so the query
  // Seq-scans ALL history and reliably hits the statement timeout on a busy host
  // → the route falls back to the tiny LiveStore snapshot (~3 incidents). Add a
  // fetched_at lower bound so Postgres prunes old partitions. We always archive a
  // page AFTER it arrives, so fetched_at >= incident_time — any row with
  // timestamp >= cutoff therefore has fetched_at >= cutoff too; the 1-day buffer
  // absorbs clock skew so no valid (in-window) row is ever excluded.
  const fetchedCutoff = cutoff - 86400;

  const pool = await getPool();
  if (!pool) throw new Error('no DB pool');

  // Build the WHERE clause incrementally so optional filters slot in with stable
  // parameter indexes. Visibility is decided by `data->>'timestamp'` (incident
  // time); the fetched_at bound is purely a partition-pruning aid (it never
  // narrows the result beyond the incident-time filter).
  const where: string[] = [
    "source = 'pager'",
    // Coordless pager rows (FRNSW FRINC turnouts) are archived for the logs
    // page but must never draw a map pin — exclude them from the hits query.
    'lat IS NOT NULL',
    'lng IS NOT NULL',
    `fetched_at >= to_timestamp($1)`,
    `(data->>'timestamp')::bigint >= $2`,
  ];
  const params: unknown[] = [fetchedCutoff, cutoff];
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

  // DISTINCT ON (source_id) keeps the newest row per upstream message id (dedup;
  // fetched_at DESC picks the most-recently-polled copy of the same id). The
  // OUTER order + LIMIT must truncate by the SAME column the window filters on —
  // incident time (data->>'timestamp'), NOT fetched_at. Ordering the 500-cap by
  // fetched_at while filtering by incident time made a late-polled/backfilled row
  // (old incident, recent fetched_at) evict genuinely-newer hits non-monotonically
  // as `hours` widened, so some hits flickered in/out at specific slider values.
  // Ordering by incident time makes visibility monotonic: a hit shown at N hours
  // stays shown at every larger N. NULLS LAST so a null-timestamp row can't win.
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
    ORDER BY (data->>'timestamp')::bigint DESC NULLS LAST
    LIMIT $${limitIdx}
  `;

  const client = await pool.connect();
  let rows: PagerArchiveRow[];
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
        type: typeof data['type'] === 'string' ? data['type'] : '',
        call_class: typeof data['call_class'] === 'string' ? data['call_class'] : '',
        is_stop: data['is_stop'] === true,
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
  return { type: 'FeatureCollection', features, count: features.length, hours };
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

  // Cache key — same combination of params returns the same body, so
  // the bot's repeated calls coalesce onto a single DB read per minute.
  const key = `h${hours}|l${limit}|c${capcode ?? ''}|i${incidentId ?? ''}`;
  const inDbBackoff = Date.now() < pagerHitsDbBackoffUntil;

  // Try-DB wrapper that tracks failures so we can back off.
  const tryRefresh = (): Promise<PagerHitsBody> => {
    return fetchPagerHitsFromDb({ hours, limit, capcode, incidentId }).catch((err) => {
      pagerHitsDbBackoffUntil = Date.now() + PAGER_HITS_DB_BACKOFF_MS;
      throw err;
    });
  };
  const swrOpts = {
    fresh: PAGER_HITS_FRESH_MS,
    stale: PAGER_HITS_STALE_MS,
    onError: (err: unknown) =>
      log.warn(
        { err: (err as Error).message },
        'pager/hits: archive_misc refresh failed; backing off 5 min',
      ),
  };

  // Diagnostic: tag every response with the branch that served it and the row
  // count, so "why does the map show N pins at this window" is answerable from
  // the server log alone. `snapshot-*` lines are the degraded paths to watch for.
  const served = (src: string, count: number, extra?: Record<string, unknown>) =>
    log.info({ src, hours, count, key, ...extra }, `pager/hits served (${src})`);

  // Cold path: no cached value for this window yet. The query is now
  // partition-pruned and typically a few ms (was 30 s+, which is why this used
  // to return the tiny LiveStore snapshot and only warm the cache in the
  // background — that made the FIRST request for every slider value show ~3
  // incidents until a second pass warmed the key). Now we AWAIT the DB and serve
  // real data, guarded by a short timeout: if the DB is genuinely slow we fall
  // back to the snapshot for THIS request while the background refresh (still in
  // flight inside .get, since SwrCache coalesces + keeps running) warms the key.
  // Skip the DB entirely during the post-failure backoff window.
  if (!pagerHitsCache.has(key)) {
    if (!inDbBackoff) {
      const COLD_DB_WAIT_MS = 4000;
      let timer: ReturnType<typeof setTimeout> | undefined;
      // .catch → null so a late rejection (after the timeout already won the
      // race) can't surface as an unhandled rejection; backoff is set by
      // tryRefresh either way.
      const dbPromise = pagerHitsCache
        .get(key, tryRefresh, swrOpts)
        .then((r) => r.value)
        .catch(() => null);
      const timeout = new Promise<null>((resolve) => {
        timer = setTimeout(() => resolve(null), COLD_DB_WAIT_MS);
      });
      try {
        const body = await Promise.race([dbPromise, timeout]);
        if (body) {
          served('db-cold', body.count);
          return c.json(body);
        }
      } finally {
        if (timer) clearTimeout(timer);
      }
    }
    const features = snapshotFallback(hours, limit, capcode, incidentId);
    served(inDbBackoff ? 'snapshot-backoff' : 'snapshot-cold-slow', features.length);
    return c.json({
      type: 'FeatureCollection',
      features,
      count: features.length,
      hours,
    });
  }

  // Warm path: cache has a value. If we're in DB backoff, skip the
  // SwrCache.get() (which would trigger a background refresh past
  // `fresh`) and read the cached body directly. Otherwise let SwrCache
  // do its normal stale-while-revalidate dance.
  if (inDbBackoff) {
    const peeked = pagerHitsCache._peek(key);
    if (peeked) {
      served('cache-backoff', peeked.value.count);
      return c.json(peeked.value);
    }
  }
  try {
    const { value, warming } = await pagerHitsCache.get(key, tryRefresh, swrOpts);
    // Only log the interesting warm case (a stale-while-revalidate refresh);
    // plain fresh hits fire ~1/s from the map poll and would bury the log.
    if (warming) served('cache-stale-revalidate', value.count);
    return c.json(value);
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'pager/hits: cache miss + DB refetch failed; falling back to snapshot',
    );
    const features = snapshotFallback(hours, limit, capcode, incidentId);
    served('snapshot-error', features.length);
    return c.json({
      type: 'FeatureCollection',
      features,
      count: features.length,
      hours,
    });
  }
});
