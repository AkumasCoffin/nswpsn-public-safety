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
  if (since !== null) rawFilters['since'] = new Date(since * 1000).toISOString();
  if (until !== null) rawFilters['until'] = new Date(until * 1000).toISOString();
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
): Promise<QueryResult<RawArchiveRow>> {
  const client = await pool.connect();
  try {
    // Match Python: 25s budget per /api/data/history query.
    await client.query("SET LOCAL statement_timeout = '25s'");
    return await client.query<RawArchiveRow>(sql, params);
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

    // Execute every per-family query. Single-family is the fast path
    // (one round-trip); multi-family runs in parallel and merges.
    const results = await Promise.all(
      plan.queries.map(async (q) => {
        try {
          const r = await runWithTimeout(pool, q.sql, q.params);
          return r.rows.map(normaliseRow);
        } catch (err) {
          log.error({ err, table: q.table }, 'data-history query failed');
          return [] as ArchiveQueryRow[];
        }
      }),
    );

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
      // We intentionally don't run a separate COUNT(*) here — Python
      // had a complicated stale-while-revalidate cache around it because
      // the old monolith couldn't COUNT in time. The new partitioned
      // tables make a true COUNT no cheaper, and the value is only used
      // for "X of Y" pagination text. Returning 0 keeps the response
      // shape stable; the next_cursor + count fields drive real
      // pagination. A future enhancement can wire reltuples-style
      // estimates if the frontend needs the headline number.
      total: 0,
      live_count: 0,
      ended_count: 0,
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
  const lastSeen = pickNum(data, 'last_seen');
  return {
    id: r.id,
    source: r.source,
    source_id: r.source_id,
    fetched_at: ts,
    fetched_at_iso: ts ? new Date(ts * 1000).toISOString() : null,
    source_timestamp: pickStr(data, 'source_timestamp'),
    source_timestamp_unix: pickNum(data, 'source_timestamp_unix'),
    // Schema column is `lat`/`lng` but Python's response shape is
    // `latitude`/`longitude` — keep the legacy names so logs.html etc.
    // don't break.
    latitude: r.lat,
    longitude: r.lng,
    location_text: pickStr(data, 'location_text'),
    title: pickStr(data, 'title'),
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
    last_seen_iso: lastSeen ? new Date(lastSeen * 1000).toISOString() : null,
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

dataHistoryRouter.get('/api/data/history/sources', async (c) => {
  const pool = await getPool();
  if (!pool) {
    return c.json({ sources: [] });
  }

  const aggregated: Map<string, { count: number; oldest: Date | null; newest: Date | null }> =
    new Map();
  for (const table of ALL_ARCHIVE_TABLES) {
    try {
      const client = await pool.connect();
      try {
        await client.query("SET LOCAL statement_timeout = '25s'");
        const r = await client.query<SourceCountRow>(
          `SELECT source, COUNT(*)::bigint AS count,
                  MIN(fetched_at) AS oldest, MAX(fetched_at) AS newest
           FROM ${table} GROUP BY source`,
        );
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
      } finally {
        client.release();
      }
    } catch (err) {
      log.error({ err, table }, 'data-history/sources query failed');
    }
  }

  const sources = Array.from(aggregated.entries())
    .filter(([source]) => source && source !== 'essential_energy_cancelled')
    .map(([source, info]) => ({
      source,
      count: info.count,
      oldest: info.oldest ? info.oldest.toISOString() : null,
      newest: info.newest ? info.newest.toISOString() : null,
    }))
    .sort((a, b) => b.count - a.count);

  return c.json({ sources });
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

  for (const table of ALL_ARCHIVE_TABLES) {
    try {
      const client = await pool.connect();
      try {
        await client.query("SET LOCAL statement_timeout = '25s'");
        const r = await client.query<StatsRow>(
          `SELECT COUNT(*)::bigint AS total, MIN(fetched_at) AS oldest, MAX(fetched_at) AS newest FROM ${table}`,
        );
        const row = r.rows[0];
        if (!row) continue;
        const cnt =
          typeof row.total === 'string' ? Number.parseInt(row.total, 10) : row.total;
        const old = row.oldest ? new Date(row.oldest) : null;
        const recent = row.newest ? new Date(row.newest) : null;
        perTable[table] = {
          count: cnt,
          oldest: old ? old.toISOString() : null,
          newest: recent ? recent.toISOString() : null,
        };
        total += cnt;
        if (old && (!oldestOverall || old < oldestOverall)) oldestOverall = old;
        if (recent && (!newestOverall || recent > newestOverall)) newestOverall = recent;
      } finally {
        client.release();
      }
    } catch (err) {
      log.error({ err, table }, 'data-history/stats query failed');
    }
  }

  return c.json({
    total_records: total,
    oldest: oldestOverall ? oldestOverall.toISOString() : null,
    newest: newestOverall ? newestOverall.toISOString() : null,
    tables: perTable,
  });
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
      fetched_at_iso: r.fetched_at_epoch
        ? new Date(r.fetched_at_epoch * 1000).toISOString()
        : null,
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
      last_seen_iso: (() => {
        const ls = pickNum(r.data, 'last_seen');
        return ls ? new Date(ls * 1000).toISOString() : null;
      })(),
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
