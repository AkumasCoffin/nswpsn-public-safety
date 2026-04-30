/**
 * /api/data/history and friends.
 *
 * The single most-used endpoint in the system — drives the logs page,
 * map proximity lookups, the dashboard preset stats, and the Discord
 * bot. It serves *historical* (snapshot) incident data from the
 * partitioned per-family archive tables.
 *
 * New schema: 5 archive_* tables (waze/traffic/rfs/power/misc), each
 * append-only, monthly RANGE-partitioned by fetched_at, no
 * is_live/is_latest columns. See src/db/migrations/002_archive_partitions.sql.
 *
 * The big handler routes to the minimum set of family tables given the
 * `?source=` filter. Single-family case = one indexed query. Multi-family
 * fans out, sorts in Node, applies LIMIT — no giant cross-table UNION
 * (that's the kind of write-amp problem we're escaping).
 *
 * Cursor pagination is the default path. Offset is honoured for
 * legacy callers but capped at MAX_OFFSET=10_000.
 */
import { Hono } from 'hono';
import type { Pool, QueryResult, QueryResultRow } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import {
  ALL_ARCHIVE_TABLES,
  buildCountSqlForTable,
  buildPlan,
  buildSqlForTable,
  mergeAndPaginate,
  sourceFamilies,
  type ArchiveQueryRow,
  type DataHistoryParams,
  type SortDir,
} from '../services/dataHistoryQuery.js';
import {
  decodeCursor,
  encodeCursor,
  MAX_OFFSET,
} from '../services/cursorPagination.js';
import { getFilterFacets } from '../store/filterCache.js';
import type { ArchiveTable } from '../store/archive.js';
import { sydneyIsoFromUnix, sydneyIsoFromDate } from '../lib/sydneyTime.js';

export const dataHistoryRouter = new Hono();

// ---------------------------------------------------------------------------
// Param parsing helpers
// ---------------------------------------------------------------------------

function commaList(v: string | null | undefined): string[] | null {
  if (!v) return null;
  const parts = v
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  return parts.length > 0 ? parts : null;
}

function intParam(v: string | null | undefined): number | null {
  if (!v) return null;
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}

function floatParam(v: string | null | undefined): number | null {
  if (!v) return null;
  const n = Number.parseFloat(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Parse a date-or-datetime string the same way Python does. Bare date
 * `YYYY-MM-DD` is interpreted as midnight (for `date_from`) or
 * 23:59:59 (for `date_to` — Python special-cases this). Anything with
 * a `T` or space is parsed as ISO.
 */
function parseDateBoundary(v: string | null | undefined, endOfDay: boolean): number | null {
  if (!v) return null;
  const trimmed = v.trim();
  if (!trimmed) return null;
  const hasTime = trimmed.includes('T') || trimmed.includes(' ');
  try {
    if (hasTime) {
      const dt = new Date(trimmed.replace(' ', 'T'));
      const t = dt.getTime();
      return Number.isFinite(t) ? Math.floor(t / 1000) : null;
    }
    // Bare date — pin to local midnight or end-of-day to match Python's
    // datetime.strptime + replace() semantics.
    const dt = new Date(`${trimmed}T${endOfDay ? '23:59:59' : '00:00:00'}`);
    const t = dt.getTime();
    return Number.isFinite(t) ? Math.floor(t / 1000) : null;
  } catch {
    return null;
  }
}

interface ParsedQuery {
  params: DataHistoryParams;
  /** Raw filter view echoed back in `query.filters_applied`. */
  rawFilters: Record<string, unknown>;
}

function parseQuery(url: URL): ParsedQuery | { error: string; status: number } {
  const q = url.searchParams;

  const sources = commaList(q.get('source'));
  const sourceId = q.get('source_id') || null;
  const category = commaList(q.get('category'));
  const subcategory = commaList(q.get('subcategory'));
  const status = commaList(q.get('status'));
  const severity = commaList(q.get('severity'));

  let since = intParam(q.get('since'));
  let until = intParam(q.get('until'));
  const dateFrom = q.get('date_from');
  const dateTo = q.get('date_to');
  const hours = intParam(q.get('hours'));
  const days = intParam(q.get('days'));
  const todayOnly = q.get('today') === '1';

  if (since === null && dateFrom) since = parseDateBoundary(dateFrom, false);
  if (until === null && dateTo) until = parseDateBoundary(dateTo, true);

  const nowSecs = Math.floor(Date.now() / 1000);
  if (since === null && hours !== null) since = nowSecs - hours * 3600;
  if (since === null && days !== null) since = nowSecs - days * 86400;
  if (since === null && todayOnly) {
    const t = new Date();
    t.setHours(0, 0, 0, 0);
    since = Math.floor(t.getTime() / 1000);
  }

  const sinceSource = intParam(q.get('since_source'));
  const untilSource = intParam(q.get('until_source'));

  const rawLimit = intParam(q.get('limit')) ?? 100;
  const limit = Math.max(1, Math.min(rawLimit, 1000));
  const offset = Math.max(0, intParam(q.get('offset')) ?? 0);

  const cursorRaw = (q.get('cursor') ?? '').trim();
  const cursor = cursorRaw ? decodeCursor(cursorRaw) : null;

  if (cursorRaw && cursor === null) {
    return { error: 'invalid_cursor', status: 400 };
  }
  if (!cursor && offset > MAX_OFFSET) {
    return { error: 'offset_too_large', status: 400 };
  }

  const order: SortDir = q.get('order') === 'asc' ? 'ASC' : 'DESC';

  const params: DataHistoryParams = {
    sources,
    sourceId,
    category,
    subcategory,
    status,
    severity,
    since,
    until,
    sinceSource,
    untilSource,
    search: q.get('search') || null,
    title: q.get('title') || null,
    location: q.get('location') || null,
    lat: floatParam(q.get('lat')),
    lng: floatParam(q.get('lon')) ?? floatParam(q.get('lng')),
    radiusKm: floatParam(q.get('radius')) ?? 10,
    unique: q.get('unique') === '1',
    liveOnly: q.get('live_only') === '1',
    historicalOnly: q.get('historical_only') === '1',
    activeOnly: q.get('active_only') === '1',
    order,
    limit,
    offset,
    cursor,
    includeData: q.get('full') === '1',
  };

  // Build the filters_applied echo — Python only includes keys that
  // the user actually passed.
  const rawFilters: Record<string, unknown> = {};
  if (sources) rawFilters['source'] = sources;
  if (category) rawFilters['category'] = category;
  if (subcategory) rawFilters['subcategory'] = subcategory;
  if (status) rawFilters['status'] = status;
  if (severity) rawFilters['severity'] = severity;
  if (params.search) rawFilters['search'] = params.search;
  // Echo as naive Sydney to match python's filters_applied shape — see
  // lib/sydneyTime for why UTC `Z` strings break date-bucketing in the
  // frontend logs page.
  if (since !== null) rawFilters['since'] = sydneyIsoFromUnix(since);
  if (until !== null) rawFilters['until'] = sydneyIsoFromUnix(until);
  if (params.liveOnly) rawFilters['live_only'] = true;
  if (params.historicalOnly) rawFilters['historical_only'] = true;
  if (params.unique) rawFilters['unique'] = true;

  return { params, rawFilters };
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

interface RawArchiveRow extends QueryResultRow {
  id: string | number;
  source: string;
  source_id: string | null;
  fetched_at_epoch: string | number;
  fetched_at: Date | string;
  /** Only set on unique=1 sidecar-driven queries. epoch seconds. */
  last_seen_at_epoch?: string | number | null;
  /** Only set on unique=1 sidecar-driven queries. */
  last_seen_at?: Date | string | null;
  lat: number | null;
  lng: number | null;
  category: string | null;
  subcategory: string | null;
  data: Record<string, unknown> | string;
}

function normaliseRow(r: RawArchiveRow): ArchiveQueryRow {
  // pg returns BIGINT as string by default. Coerce to JS number — incident
  // ids fit comfortably below 2^53 and the cursor encoder needs a number.
  const id = typeof r.id === 'string' ? Number.parseInt(r.id, 10) : r.id;
  const epoch =
    typeof r.fetched_at_epoch === 'string'
      ? Number.parseInt(r.fetched_at_epoch, 10)
      : r.fetched_at_epoch;
  let lastSeenEpoch: number | null = null;
  if (r.last_seen_at_epoch != null) {
    lastSeenEpoch =
      typeof r.last_seen_at_epoch === 'string'
        ? Number.parseInt(r.last_seen_at_epoch, 10)
        : r.last_seen_at_epoch;
  }
  let data: Record<string, unknown>;
  if (typeof r.data === 'string') {
    try {
      data = JSON.parse(r.data) as Record<string, unknown>;
    } catch {
      data = {};
    }
  } else {
    data = r.data ?? {};
  }
  return {
    id,
    source: r.source,
    source_id: r.source_id,
    fetched_at_epoch: epoch,
    fetched_at: r.fetched_at,
    last_seen_at_epoch: lastSeenEpoch,
    last_seen_at: r.last_seen_at ?? null,
    lat: r.lat,
    lng: r.lng,
    category: r.category,
    subcategory: r.subcategory,
    data,
  };
}

async function runWithTimeout(
  pool: Pool,
  sql: string,
  params: unknown[],
  timeoutSecs: number = 60,
): Promise<QueryResult<RawArchiveRow>> {
  const client = await pool.connect();
  try {
    // Default 60s budget for the data query — wider than python's old
    // 25s because the new partitioned schema hasn't accumulated all
    // the indexes yet, and on a freshly-backfilled archive_waze
    // (~257k rows) the DISTINCT ON + COUNT(DISTINCT ...) pair can
    // stretch to 30s+ until the planner caches a stable plan.
    //
    // Count queries override to 15s — they're best-effort (failure
    // returns total=0 to the caller), so a stuck count shouldn't
    // hold the route's connection for 60s. The data query carries
    // the page even when count is unavailable.
    //
    // CRITICAL: SET LOCAL is a no-op outside an explicit transaction
    // block (the pg docs are explicit on this). Earlier revisions
    // skipped the BEGIN/COMMIT pair and the statement_timeout never
    // applied — Postgres fell back to the global default (~30s) and
    // queries hit that ceiling instead of the budget we intended.
    // Wrap in BEGIN/COMMIT so SET LOCAL actually scopes to this
    // query and gets reset before the connection returns to the pool.
    await client.query('BEGIN');
    try {
      await client.query(`SET LOCAL statement_timeout = '${timeoutSecs}s'`);
      const r = await client.query<RawArchiveRow>(sql, params);
      await client.query('COMMIT');
      return r;
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch { /* ignore */ }
      throw err;
    }
  } finally {
    client.release();
  }
}

// ---------------------------------------------------------------------------
// /api/data/history
// ---------------------------------------------------------------------------

dataHistoryRouter.get('/api/data/history', async (c) => {
  const url = new URL(c.req.url);
  const parsed = parseQuery(url);
  if ('error' in parsed) {
    if (parsed.error === 'invalid_cursor') {
      return c.json(
        { error: 'invalid_cursor', message: 'cursor is malformed; drop it and start over' },
        400,
      );
    }
    if (parsed.error === 'offset_too_large') {
      const offset = intParam(url.searchParams.get('offset')) ?? 0;
      return c.json(
        {
          error: 'offset_too_large',
          message:
            `offset=${offset} exceeds ${MAX_OFFSET}. Use ?cursor=<next_cursor from previous response> ` +
            'for forward pagination, or narrow the result set with date_from/hours/source.',
          max_offset: MAX_OFFSET,
        },
        400,
      );
    }
    return c.json({ error: parsed.error }, 400);
  }

  const { params, rawFilters } = parsed;
  const pool = await getPool();
  if (!pool) {
    return c.json({
      records: [],
      total: 0,
      live_count: 0,
      ended_count: 0,
      limit: params.limit,
      offset: params.offset,
      count: 0,
      next_cursor: null,
      query: { filters_applied: rawFilters },
    });
  }

  try {
    const plan = buildPlan(params);

    // Execute the per-family data queries AND a parallel set of count
    // queries against the same WHERE clauses (without LIMIT/cursor) so
    // the response carries a real `total`. The frontend's logs.html
    // pagination uses `total` to compute Math.ceil(total/limit) for
    // the page-jumper; with total=0 it can't navigate to later pages.
    const dataNowMs = Date.now();
    const dataPromise = Promise.all(
      plan.queries.map(async (q) => {
        const dKey = dataCacheKey(q.table, q.sql, q.params);
        const dHit = dataCache.get(dKey);
        if (dHit && dataNowMs - dHit.ts < DATA_CACHE_TTL_MS) {
          return dHit.rows;
        }
        try {
          // 30s timeout (was 60s) — on a saturated host a 60s wait
          // ties up the connection AND the user's tab. Failing at 30
          // lets the next request retry sooner and unblocks the pool.
          const r = await runWithTimeout(pool, q.sql, q.params, 30);
          const rows = r.rows.map(normaliseRow);
          // Bound the cache: drop oldest when over capacity.
          if (dataCache.size >= DATA_CACHE_MAX) {
            const oldestKey = dataCache.keys().next().value;
            if (oldestKey !== undefined) dataCache.delete(oldestKey);
          }
          dataCache.set(dKey, { rows, ts: dataNowMs });
          return rows;
        } catch (err) {
          log.error({ err, table: q.table }, 'data-history query failed');
          return [] as ArchiveQueryRow[];
        }
      }),
    );
    // Per-family count returns total + live_count in one round-trip.
    // ended is derived as `total - live` to match formatRecord's
    // truthy-default semantics for missing is_live.
    //
    // 2-min LRU cache: page-jumper clicks all hit the same WHERE
    // clause (only LIMIT/OFFSET differ, and the count query strips
    // both) so we re-use the previous result instead of re-running
    // the 5-10s aggregate per page click. 30s was too short to span
    // a typical paginating session; bumped to 120s.
    //
    // Count uses a 15s timeout (vs 60s for the data query) so a
    // stuck count never blocks the route. On timeout the route
    // returns the data with total=0; logs.html falls back to "+"
    // pagination instead of jump-to-page.
    const nowMs = Date.now();
    const countPromise = Promise.all(
      plan.queries.map(async (q) => {
        const cq = buildCountSqlForTable(q.table, params);
        const key = countCacheKey(q.table, cq.sql, cq.params);
        const hit = countCache.get(key);
        if (hit && nowMs - hit.ts < COUNT_CACHE_TTL_MS) {
          return hit.count;
        }
        try {
          const r = await runWithTimeout(pool, cq.sql, cq.params, 15);
          const row = r.rows[0] as
            | { total?: string | number; live_count?: string | number }
            | undefined;
          const count = {
            total: Number(row?.total ?? 0),
            live: Number(row?.live_count ?? 0),
          };
          // Bound the cache: drop oldest when over capacity. Map's
          // insertion order makes this O(1) per eviction.
          if (countCache.size >= COUNT_CACHE_MAX) {
            const oldestKey = countCache.keys().next().value;
            if (oldestKey !== undefined) countCache.delete(oldestKey);
          }
          countCache.set(key, { count, ts: nowMs });
          return count;
        } catch (err) {
          log.warn(
            { err: (err as Error).message, table: q.table },
            'data-history count query failed (returning 0)',
          );
          // Negative-cache for 30s so a hot page-load loop doesn't
          // re-run the failing 15s query on every refresh.
          if (countCache.size >= COUNT_CACHE_MAX) {
            const oldestKey = countCache.keys().next().value;
            if (oldestKey !== undefined) countCache.delete(oldestKey);
          }
          countCache.set(key, {
            count: { total: 0, live: 0 },
            ts: nowMs - (COUNT_CACHE_TTL_MS - 30_000),
          });
          return { total: 0, live: 0 };
        }
      }),
    );
    const [results, counts] = await Promise.all([dataPromise, countPromise]);
    const total = counts.reduce((a, b) => a + b.total, 0);
    const liveCount = counts.reduce((a, b) => a + b.live, 0);
    const endedCount = Math.max(0, total - liveCount);

    let merged: ArchiveQueryRow[];
    if (results.length === 1) {
      // Single-family: the per-table SQL already applied limit + offset
      // when no cursor was set. Skip merge sort entirely.
      const rows = results[0] ?? [];
      if (params.cursor) {
        merged = rows;
      } else {
        merged = rows.slice(params.offset, params.offset + params.limit);
      }
    } else {
      // Multi-family: combine, sort by (fetched_at, id), apply offset+limit.
      const all: ArchiveQueryRow[] = [];
      for (const part of results) all.push(...part);
      merged = mergeAndPaginate(all, plan);
    }

    const records = merged.map((r) => formatRecord(r, params.includeData));
    const last = merged[merged.length - 1];
    const nextCursor =
      merged.length >= params.limit && last
        ? encodeCursor(last.fetched_at_epoch, last.id)
        : null;

    return c.json({
      records,
      total,
      // Both numbers come out of the same single count query as `total`
      // via FILTER (WHERE data->>'is_live' IN ('1','true','True')).
      // Capped at the same 50k as `total` per family.
      live_count: liveCount,
      ended_count: endedCount,
      limit: params.limit,
      offset: params.offset,
      count: records.length,
      next_cursor: nextCursor,
      query: { filters_applied: rawFilters },
    });
  } catch (err) {
    log.error({ err }, '/api/data/history error');
    return c.json({ error: (err as Error).message }, 500);
  }
});

interface FormattedRecord {
  id: number;
  source: string;
  source_id: string | null;
  fetched_at: number;
  fetched_at_iso: string | null;
  source_timestamp: string | null;
  source_timestamp_unix: number | null;
  latitude: number | null;
  longitude: number | null;
  location_text: string | null;
  title: string | null;
  category: string | null;
  subcategory: string | null;
  status: string | null;
  severity: string | null;
  data: Record<string, unknown>;
  is_active: boolean;
  is_live: boolean;
  last_seen: number | null;
  last_seen_iso: string | null;
}

function pickStr(d: Record<string, unknown>, k: string): string | null {
  const v = d[k];
  return typeof v === 'string' ? v : null;
}

/**
 * Walk a list of JSONB keys and return the first non-empty string value.
 * Used to derive a record's title from whichever field actually got
 * populated by the upstream — pager rows have data.title, waze rows have
 * data.street, RFS / traffic rows have data.locationDescriptor, etc.
 */
function pickFirstStr(
  d: Record<string, unknown>,
  keys: readonly string[],
): string | null {
  for (const k of keys) {
    const v = d[k];
    if (typeof v === 'string' && v.length > 0) return v;
  }
  return null;
}

function pickNum(d: Record<string, unknown>, k: string): number | null {
  const v = d[k];
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number.parseInt(v, 10);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function pickBool(d: Record<string, unknown>, k: string, fallback: boolean): boolean {
  const v = d[k];
  if (typeof v === 'boolean') return v;
  if (typeof v === 'number') return v !== 0;
  if (typeof v === 'string') {
    if (v === '1' || v.toLowerCase() === 'true') return true;
    if (v === '0' || v.toLowerCase() === 'false') return false;
  }
  return fallback;
}

function formatRecord(r: ArchiveQueryRow, includeData: boolean): FormattedRecord {
  const data = r.data ?? {};
  const ts = r.fetched_at_epoch;
  // last_seen sources, in priority:
  //   1. sidecar.last_seen_at (set on unique=1 results from migration 018) —
  //      authoritative "when did we last see this in any poll" timestamp.
  //   2. data.last_seen — fallback used by sources that recorded their own
  //      last_seen field in JSONB before the sidecar existed.
  const lastSeen = r.last_seen_at_epoch ?? pickNum(data, 'last_seen');
  return {
    id: r.id,
    source: r.source,
    source_id: r.source_id,
    fetched_at: ts,
    fetched_at_iso: sydneyIsoFromUnix(ts),
    source_timestamp: pickStr(data, 'source_timestamp'),
    source_timestamp_unix: pickNum(data, 'source_timestamp_unix'),
    // Schema column is `lat`/`lng` but Python's response shape is
    // `latitude`/`longitude` — keep the legacy names so logs.html etc.
    // don't break.
    latitude: r.lat,
    longitude: r.lng,
    location_text: pickStr(data, 'location_text'),
    // Title fallback chain. Most upstream payloads don't normalise to
    // a single 'title' field, so we pull the most useful per-source
    // alternative if data.title is missing:
    //   pager      → data.title (set by pagerArchiveItems)
    //   waze_*     → data.street ('A1 - Pacific Hwy', 'Combine St', ...)
    //   pager      → data.alias (defensive — backend already promotes
    //                 this to data.title, but covers older rows)
    //   rfs/traffic → data.locationDescriptor or data.location
    // Without this every waze record was returning title=null, which
    // logs.html rendered as "Unknown".
    title: pickFirstStr(data, [
      'title',
      'street',
      'alias',
      'locationDescriptor',
      'location',
    ]),
    category: r.category,
    subcategory: r.subcategory,
    status: pickStr(data, 'status'),
    severity: pickStr(data, 'severity'),
    data: includeData ? data : {},
    is_active: pickBool(data, 'is_active', false),
    // Python defaulted is_live=true when the column was NULL. The new
    // schema doesn't carry an is_live column at all; absence in the JSON
    // means "we never tagged this snapshot", which for the latest row
    // is effectively "still live". Default-true matches Python.
    is_live: pickBool(data, 'is_live', true),
    last_seen: lastSeen,
    last_seen_iso: sydneyIsoFromUnix(lastSeen),
  };
}

// ---------------------------------------------------------------------------
// /api/data/history/filters
// ---------------------------------------------------------------------------

dataHistoryRouter.get('/api/data/history/filters', (c) => {
  const sourceFilter = c.req.query('source');
  return c.json(getFilterFacets(sourceFilter ?? null));
});

// ---------------------------------------------------------------------------
// /api/data/history/sources
// ---------------------------------------------------------------------------

interface SourceCountRow extends QueryResultRow {
  source: string;
  count: string | number;
  oldest: Date | string | null;
  newest: Date | string | null;
}

// 5-min response cache for /sources and /stats. Each runs an
// unbounded COUNT(*) GROUP BY across every monthly partition of all
// 5 archive tables — multi-second on archive_waze. The numbers don't
// change much within 5 min so caching the response makes the dashboard
// load instant on repeat hits. Cleared by /api/cache/clear.
interface SourcesCacheEntry { body: unknown; ts: number; }
let sourcesCache: SourcesCacheEntry | null = null;
let statsCache: SourcesCacheEntry | null = null;
const SOURCES_STATS_TTL_MS = 5 * 60_000;

// 30s LRU-bounded cache for /api/data/history count queries. The page-
// jumper on logs.html re-issues the same WHERE-clause for every page
// click; counts are stable across those clicks so we don't need to
// re-run the 5-10s COUNT(*) FILTER aggregate each time. Bounded at
// 256 entries to avoid runaway growth from heavy filter-permutation
// traffic.
interface CountCacheEntry { count: { total: number; live: number }; ts: number; }
const countCache = new Map<string, CountCacheEntry>();
const COUNT_CACHE_TTL_MS = 120_000;
const COUNT_CACHE_MAX = 256;

function countCacheKey(table: string, sql: string, params: unknown[]): string {
  return `${table}|${sql}|${JSON.stringify(params)}`;
}

// 5-min LRU cache for /api/data/history DATA queries — the rows
// themselves, not just the count. On disk-saturated hosts the per-
// table DISTINCT ON regularly hits the 30s statement_timeout, and
// the underlying archive_waze pages get evicted from cache by the
// concurrent waze ingest writer. Caching the rows means a single
// successful query covers every subsequent identical request inside
// the window; the bot's per-minute polls and the typical "load
// page 1, scroll, page-jump" pattern both warm the cache fast.
//
// TTL bumped from 60s → 5min after observing repeated 30s timeouts
// on archive_waze unique=1 queries during heavy waze ingest bursts:
// each cache miss took 30s, failed, didn't populate the cache, then
// the next request faced the same cold path — every refresh hit
// the timeout. 5 min gives a successful query enough time to absorb
// many subsequent requests, and the underlying data only changes by
// a few rows per minute anyway.
//
// Keyed on (table, sql, params), so the LIMIT/OFFSET/cursor naturally
// shard the cache per page. Capped at 256 entries — each entry holds
// up to ~limit (default 100) ArchiveQueryRows so memory stays bounded
// even at the cap.
interface DataCacheEntry { rows: ArchiveQueryRow[]; ts: number; }
const dataCache = new Map<string, DataCacheEntry>();
const DATA_CACHE_TTL_MS = 5 * 60_000;
const DATA_CACHE_MAX = 256;

function dataCacheKey(table: string, sql: string, params: unknown[]): string {
  return `${table}|${sql}|${JSON.stringify(params)}`;
}

export function _resetDataHistoryAggregateCache(): void {
  sourcesCache = null;
  statsCache = null;
  countCache.clear();
  dataCache.clear();
}

dataHistoryRouter.get('/api/data/history/sources', async (c) => {
  const now = Date.now();
  if (sourcesCache && now - sourcesCache.ts < SOURCES_STATS_TTL_MS) {
    return c.json(sourcesCache.body);
  }
  const pool = await getPool();
  if (!pool) {
    return c.json({ sources: [] });
  }

  const aggregated: Map<string, { count: number; oldest: Date | null; newest: Date | null }> =
    new Map();
  // One client across all 5 tables — previous revision did pool.connect()
  // per iteration which burned 5 pool slots back-to-back. Each query is
  // wrapped in its own BEGIN/COMMIT so SET LOCAL actually applies (the
  // same fix the archive writer needed earlier).
  const client = await pool.connect();
  try {
    for (const table of ALL_ARCHIVE_TABLES) {
      try {
        await client.query('BEGIN');
        try {
          await client.query("SET LOCAL statement_timeout = '25s'");
          const r = await client.query<SourceCountRow>(
            `SELECT source, COUNT(*)::bigint AS count,
                    MIN(fetched_at) AS oldest, MAX(fetched_at) AS newest
             FROM ${table} GROUP BY source`,
          );
          await client.query('COMMIT');
          for (const row of r.rows) {
            const cnt =
              typeof row.count === 'string' ? Number.parseInt(row.count, 10) : row.count;
            const old = row.oldest ? new Date(row.oldest) : null;
            const recent = row.newest ? new Date(row.newest) : null;
            const existing = aggregated.get(row.source);
            if (!existing) {
              aggregated.set(row.source, { count: cnt, oldest: old, newest: recent });
            } else {
              existing.count += cnt;
              if (old && (!existing.oldest || old < existing.oldest)) existing.oldest = old;
              if (recent && (!existing.newest || recent > existing.newest))
                existing.newest = recent;
            }
          }
        } catch (err) {
          try { await client.query('ROLLBACK'); } catch { /* ignore */ }
          throw err;
        }
      } catch (err) {
        log.error({ err, table }, 'data-history/sources query failed');
      }
    }
  } finally {
    client.release();
  }

  const sources = Array.from(aggregated.entries())
    .filter(([source]) => source && source !== 'essential_energy_cancelled')
    .map(([source, info]) => ({
      source,
      count: info.count,
      oldest: sydneyIsoFromDate(info.oldest),
      newest: sydneyIsoFromDate(info.newest),
    }))
    .sort((a, b) => b.count - a.count);

  const body = { sources };
  sourcesCache = { body, ts: now };
  return c.json(body);
});

// ---------------------------------------------------------------------------
// /api/data/history/stats
// ---------------------------------------------------------------------------

interface StatsRow extends QueryResultRow {
  total: string | number;
  oldest: Date | string | null;
  newest: Date | string | null;
}

dataHistoryRouter.get('/api/data/history/stats', async (c) => {
  const now = Date.now();
  if (statsCache && now - statsCache.ts < SOURCES_STATS_TTL_MS) {
    return c.json(statsCache.body);
  }
  const pool = await getPool();
  if (!pool) {
    return c.json({
      total_records: 0,
      oldest: null,
      newest: null,
      tables: {},
    });
  }

  const perTable: Record<string, { count: number; oldest: string | null; newest: string | null }> =
    {};
  let total = 0;
  let oldestOverall: Date | null = null;
  let newestOverall: Date | null = null;

  // Same one-client pattern as /sources above — the per-iteration
  // pool.connect was claiming 5 slots in sequence under load.
  const client = await pool.connect();
  try {
    for (const table of ALL_ARCHIVE_TABLES) {
      try {
        await client.query('BEGIN');
        try {
          await client.query("SET LOCAL statement_timeout = '25s'");
          const r = await client.query<StatsRow>(
            `SELECT COUNT(*)::bigint AS total, MIN(fetched_at) AS oldest, MAX(fetched_at) AS newest FROM ${table}`,
          );
          await client.query('COMMIT');
          const row = r.rows[0];
          if (!row) continue;
          const cnt =
            typeof row.total === 'string' ? Number.parseInt(row.total, 10) : row.total;
          const old = row.oldest ? new Date(row.oldest) : null;
          const recent = row.newest ? new Date(row.newest) : null;
          perTable[table] = {
            count: cnt,
            oldest: sydneyIsoFromDate(old),
            newest: sydneyIsoFromDate(recent),
          };
          total += cnt;
          if (old && (!oldestOverall || old < oldestOverall)) oldestOverall = old;
          if (recent && (!newestOverall || recent > newestOverall)) newestOverall = recent;
        } catch (err) {
          try { await client.query('ROLLBACK'); } catch { /* ignore */ }
          throw err;
        }
      } catch (err) {
        log.error({ err, table }, 'data-history/stats query failed');
      }
    }
  } finally {
    client.release();
  }

  const body = {
    total_records: total,
    oldest: sydneyIsoFromDate(oldestOverall),
    newest: sydneyIsoFromDate(newestOverall),
    tables: perTable,
  };
  statsCache = { body, ts: now };
  return c.json(body);
});

// ---------------------------------------------------------------------------
// /api/data/history/incident/<source>/<source_id>
// ---------------------------------------------------------------------------

dataHistoryRouter.get('/api/data/history/incident/:source/:source_id', async (c) => {
  const source = c.req.param('source');
  const sourceId = c.req.param('source_id');
  if (!source || !sourceId) {
    return c.json({ error: 'source and source_id are required' }, 400);
  }

  const pool = await getPool();
  if (!pool) {
    return c.json({
      source,
      source_id: sourceId,
      is_live: false,
      snapshots: 0,
      history: [],
    });
  }

  const families = sourceFamilies([source]);
  // Trust the first matched family — `source` is unique to a family.
  const table: ArchiveTable = families[0] ?? 'archive_misc';

  try {
    // Reuse buildSqlForTable to get a consistent column list. We want the
    // full snapshot list for this incident, ascending order, with the
    // data blob included; cap matches Python at 5000 to bound payload.
    const built = buildSqlForTable(table, {
      sources: [source],
      sourceId: sourceId,
      category: null,
      subcategory: null,
      status: null,
      severity: null,
      since: null,
      until: null,
      sinceSource: null,
      untilSource: null,
      search: null,
      title: null,
      location: null,
      lat: null,
      lng: null,
      radiusKm: 10,
      unique: false,
      liveOnly: false,
      historicalOnly: false,
      activeOnly: false,
      order: 'ASC',
      limit: 5000,
      offset: 0,
      cursor: null,
      includeData: true,
    });
    const result = await runWithTimeout(pool, built.sql, built.params);
    const rows = result.rows.map(normaliseRow);

    const history = rows.map((r) => ({
      id: r.id,
      fetched_at: r.fetched_at_epoch,
      fetched_at_iso: sydneyIsoFromUnix(r.fetched_at_epoch),
      source_timestamp: pickStr(r.data, 'source_timestamp'),
      source_timestamp_unix: pickNum(r.data, 'source_timestamp_unix'),
      latitude: r.lat,
      longitude: r.lng,
      location_text: pickStr(r.data, 'location_text'),
      title: pickStr(r.data, 'title'),
      category: r.category,
      subcategory: r.subcategory,
      status: pickStr(r.data, 'status'),
      severity: pickStr(r.data, 'severity'),
      data: r.data,
      is_active: pickBool(r.data, 'is_active', false),
      is_live: pickBool(r.data, 'is_live', true),
      last_seen: pickNum(r.data, 'last_seen'),
      last_seen_iso: sydneyIsoFromUnix(pickNum(r.data, 'last_seen')),
    }));

    const isCurrentlyLive =
      history.length > 0 ? (history[history.length - 1]?.is_live ?? false) : false;

    return c.json({
      source,
      source_id: sourceId,
      is_live: isCurrentlyLive,
      snapshots: history.length,
      history,
    });
  } catch (err) {
    log.error({ err, source, sourceId }, '/api/data/history/incident error');
    return c.json({ error: (err as Error).message }, 500);
  }
});
