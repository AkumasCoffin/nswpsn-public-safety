/**
 * /api/data/history and friends.
 *
 * The single most-used endpoint in the system — drives the logs page,
 * map proximity lookups, the dashboard preset stats, and the Discord
 * bot. It serves *historical* (snapshot) incident data from the
 * partitioned per-family archive tables.
 *
 * New schema: 5 archive_* tables (waze/traffic/rfs/power/misc), each
 * append-only, monthly RANGE-partitioned by fetched_at. See
 * src/db/migrations/002_archive_partitions.sql.
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
import { AU_STATES } from '../lib/stateMask.js';
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
import {
  sydneyIsoFromUnix,
  sydneyIsoFromDate,
  sydneyUnixFromNaive,
  formatSydneyNaive,
} from '../lib/sydneyTime.js';
import type { Context } from 'hono';
import { z } from 'zod';
import { describeRoute, resolver } from 'hono-openapi';
import { HISTORY_SOURCES, historySourcesFor, scoped } from '../services/auth/apiKeyScopes.js';
import type { FilterFacets } from '../store/filterCache.js';
import { fail, isV1, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  ErrorBody,
  GATED_ERRORS,
  okJson,
  queryValidator,
  type SameShape,
} from './v1/schemas.js';

export const dataHistoryRouter = new Hono();

// What a scoped API key needs to read stored history (services/auth/
// apiKeyScopes.ts). It spans sources and types: a key with window:history
// gets in, and is answered only from the archived sources its scopes cover
// (historySourcesFor). /sources and /stats — counts across every source —
// declare nothing, so no key reaches them; they are not on /api/v1/.
const SCOPE_HISTORY = scoped({ source: '*', type: '*', window: 'history' });

/**
 * The archived sources a request may read: those its key's scopes cover, or
 * null for a caller that is not a named key (this site's pages, the static
 * key), which reads them all, as it always has.
 */
function readableSources(c: Context): string[] | null {
  if (c.get('authClass') !== 'apikey') return null;
  return historySourcesFor(c.get('apiKeyScopes') ?? []);
}

/** A key asked for history its scopes do not cover. */
function refuseSources(c: Context, message: string): Response {
  return fail(c, 403, 'api_key_scope_forbidden', message, { error: 'api_key_scope_forbidden', message });
}

/** Unix seconds → ISO 8601 UTC, as /api/v1/ writes every time. */
function utcIso(unix: number | null | undefined): string | null {
  if (unix === null || unix === undefined || !Number.isFinite(unix)) return null;
  return new Date(unix * 1000).toISOString();
}

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
  // Interpret bare `YYYY-MM-DD` or `YYYY-MM-DD[ T]HH:mm[:ss]` as
  // Australia/Sydney wall-clock — the zone this system stores in.
  // Using `new Date()` here would parse in the server's local zone (UTC
  // on prod), shifting the requested day by the Sydney offset (10–11h)
  // and silently dropping the start/end of the day from results.
  // Optional fractional seconds (`.\d+`) are tolerated and discarded so a
  // bare datetime like `2026-06-27 14:30:00.000` is still read as Sydney
  // wall-clock rather than falling through to the local-zone `new Date()`
  // path below (which would shift it by the Sydney offset). Strings with
  // an explicit offset / `Z` still fail this anchored match and are parsed
  // by `new Date()`, which is correct — they carry their own zone.
  const m = trimmed.match(
    /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?)?$/,
  );
  if (m) {
    const [, y, mo, d, hh, mi, ss] = m;
    const hasTime = hh !== undefined;
    const H = hasTime ? Number(hh) : endOfDay ? 23 : 0;
    const M = hasTime ? Number(mi) : endOfDay ? 59 : 0;
    const S = hasTime ? (ss !== undefined ? Number(ss) : 0) : endOfDay ? 59 : 0;
    return sydneyUnixFromNaive(Number(y), Number(mo), Number(d), H, M, S);
  }
  // Anything else (explicit offset / Z / RFC string) — let Date parse it.
  const t = new Date(trimmed).getTime();
  return Number.isFinite(t) ? Math.floor(t / 1000) : null;
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
  // ?state=NSW,QLD — uppercased so the query string is forgiving, and
  // validated against the known codes so a typo can't silently return
  // nothing (an unknown code would match no rows at all).
  const stateRaw = commaList(q.get('state'));
  const state = stateRaw
    ? stateRaw.map((v) => v.toUpperCase()).filter((v) => (AU_STATES as readonly string[]).includes(v))
    : null;

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
    // "Today" in Sydney, not the server's local zone.
    const sydneyToday = formatSydneyNaive(Date.now()).slice(0, 10);
    since = parseDateBoundary(sydneyToday, false);
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
    state: state && state.length > 0 ? state : null,
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
    // unique defaults to true (one row per incident) since that's what
    // logs.html / dashboard.html and the bot all want. Old default
    // (unique=0, every snapshot) scans the 2.8M-row parent archive
    // when the ~3-77K-row sidecar would do; queries dropped from
    // 5-30s to milliseconds. Callers that genuinely want every poll
    // snapshot must pass ?unique=0 explicitly — logs.html's
    // linked-incident proximity lookups already do.
    unique: q.get('unique') !== '0',
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
  /** Only set on unique=1 sidecar-driven queries. The upstream feed's
   *  own publish/last-updated time, normalised to epoch seconds. */
  source_timestamp_unix?: string | number | null;
  lat: number | null;
  lng: number | null;
  category: string | null;
  subcategory: string | null;
  /** Derived from the coordinate at write time; null when unplaceable. */
  state?: string | null;
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
  let sourceTsUnix: number | null = null;
  if (r.source_timestamp_unix != null) {
    sourceTsUnix =
      typeof r.source_timestamp_unix === 'string'
        ? Number.parseInt(r.source_timestamp_unix, 10)
        : r.source_timestamp_unix;
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
    source_timestamp_unix: sourceTsUnix,
    lat: r.lat,
    lng: r.lng,
    category: r.category,
    subcategory: r.subcategory,
    state: r.state ?? null,
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
      // statement_timeout can't be parameterized (SET LOCAL ... = $1 is a
      // syntax error in Postgres), so the value is interpolated. Callers
      // only ever pass internal literals today, but clamp to a positive
      // integer so a future request-derived value can't inject SQL or
      // produce a malformed interval.
      const timeoutInt = Math.max(1, Math.trunc(timeoutSecs));
      await client.query(`SET LOCAL statement_timeout = '${timeoutInt}s'`);
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

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const UNIX = 'unix seconds';
const SOURCE_NAMES = Object.keys(HISTORY_SOURCES).join(', ');

const historyRecordShape = {
  id: z.number().int().describe("The stored snapshot's id"),
  fetched_at: z.number().int().describe(`When this snapshot was stored, ${UNIX}`),
  fetched_at_iso: z.string().nullable().describe('fetched_at, ISO 8601 UTC'),
  source_timestamp: z.string().nullable().describe("The source's own time for the record, as the source writes it; null when it gives none"),
  source_timestamp_unix: z.number().int().nullable().describe(`The source's own time for the record, ${UNIX}; null when it gives none`),
  latitude: z.number().nullable(),
  longitude: z.number().nullable(),
  location_text: z.string().nullable().describe('Where, in words, when the source gives it'),
  title: z.string().nullable().describe("The record's title, else its street or location, as the source gives it"),
  category: z.string().nullable(),
  subcategory: z.string().nullable(),
  status: z.string().nullable().describe("The record's status, as the source words it"),
  severity: z.string().nullable().describe("The record's severity or alert level, as the source words it"),
  is_active: z.boolean().describe('Whether the source still listed it as active when this snapshot was stored'),
  last_seen: z.number().int().nullable().describe(`The last poll that still found the incident, ${UNIX}`),
  last_seen_iso: z.string().nullable().describe('last_seen, ISO 8601 UTC'),
};

const HistoryRecord = z
  .object({
    ...historyRecordShape,
    source: z.string().describe(`The archived source: one of ${SOURCE_NAMES}`),
    source_id: z.string().nullable().describe("The source's own id for the incident"),
    state: z.string().nullable().describe('State or territory code (NSW, VIC, …), from the position; null when it has none'),
    data: z
      .record(z.string(), z.unknown())
      .describe('The record exactly as the source gave it, with full=1; otherwise empty'),
  })
  .meta({ id: 'HistoryRecord' });
assertShape<SameShape<z.infer<typeof HistoryRecord>, FormattedRecord>>();

const HistoryPage = z
  .object({
    records: z.array(HistoryRecord),
    total: z
      .number()
      .int()
      .describe('How many records match across every page; 0 when they could not be counted, and always 0 on a page fetched by cursor'),
    limit: z.number().int(),
    offset: z.number().int(),
    count: z.number().int().describe('Records on this page'),
    next_cursor: z.string().nullable().describe('Pass as `cursor` for the next page; null on the last page'),
    query: z.object({
      filters_applied: z
        .record(z.string(), z.unknown())
        .describe('The filters given, as understood: lists split on commas, and since/until as ISO 8601 UTC'),
    }),
  })
  .meta({ id: 'HistoryPage' });

const opt = (description: string) => z.string().optional().catch(undefined).describe(description);

// The handler reads the query string itself, exactly as it always has; this
// only describes it. It refuses nothing: the two parameters that can be
// refused (cursor, offset) are refused by the handler, in its own words.
const HistoryQuery = z.object({
  source: opt(
    `Only these archived sources, comma-separated: ${SOURCE_NAMES}. A key reads only the sources its scopes cover; naming another is refused (403), and leaving this out means all of them`,
  ),
  source_id: opt("Only the incident with this id (the source's own)"),
  category: opt('Only these categories, comma-separated'),
  subcategory: opt('Only these subcategories, comma-separated'),
  status: opt('Only these statuses, comma-separated'),
  severity: opt('Only these severities, comma-separated'),
  state: opt('Only these states or territories, comma-separated codes (NSW, VIC, …); unknown codes are ignored'),
  since: opt(`Stored at or after this time, ${UNIX}`),
  until: opt(`Stored at or before this time, ${UNIX}`),
  date_from: opt(
    'Stored on or after this date or time when since is not given: YYYY-MM-DD (midnight) or YYYY-MM-DDTHH:mm[:ss], read as Sydney time; with an offset or Z, as written',
  ),
  date_to: opt('Stored on or before this date or time when until is not given; a bare date means the end of that day, Sydney time'),
  hours: opt('Stored in the last this many hours, when neither since nor date_from is given'),
  days: opt('Stored in the last this many days, when no earlier time parameter is given'),
  today: opt('1: stored today, Sydney time, when no earlier time parameter is given'),
  since_source: opt(`Only records whose own source time is at or after this, ${UNIX}`),
  until_source: opt(`Only records whose own source time is at or before this, ${UNIX}`),
  search: opt('Only records matching this text'),
  title: opt('Only records whose title matches this text'),
  location: opt('Only records whose location matches this text'),
  lat: opt('With lon: only records within radius of this point'),
  lon: opt('With lat: only records within radius of this point (lng works too)'),
  radius: opt('Kilometres around lat/lon; 10 when not given'),
  unique: opt('0: every stored snapshot. Otherwise one record per incident, its latest'),
  active_only: opt('1: only records still active when stored'),
  order: opt('asc: oldest first. Otherwise newest first'),
  limit: opt('Records per page, 1 to 1000; 100 when not given'),
  offset: opt(`Skip this many records, at most ${MAX_OFFSET}; for later pages use cursor`),
  cursor: opt('next_cursor from the previous page'),
  full: opt('1: include each record as the source gave it, in data'),
});

/** A record as /api/v1/ writes it: the ISO times in UTC. */
function utcRecord(r: FormattedRecord): FormattedRecord {
  return { ...r, fetched_at_iso: utcIso(r.fetched_at), last_seen_iso: utcIso(r.last_seen) };
}

dataHistoryRouter.get(
  '/api/data/history',
  SCOPE_HISTORY,
  describeRoute({
    operationId: 'getHistory',
    tags: ['History'],
    summary: 'Stored records',
    description:
      "Every source's records as this server stored them over time: by default one record per incident (its latest), newest first. Filter by source, time, place and text; page with cursor (or offset, up to 10,000). Records are kept for the server's retention window. A key reads only the archived sources its scopes cover — each source's stored history needs that source's own scope and type, with window:history.",
    responses: {
      ...okJson(envelope(HistoryPage), 'A page of records'),
      ...GATED_ERRORS,
      400: {
        description: 'cursor is malformed, or offset is past 10,000 (validation_failed; details.max_offset)',
        content: { 'application/json': { schema: resolver(ErrorBody) } },
      },
      503: {
        description:
          'The archive could not be read, or part of it did not answer in time (service_unavailable; details.failed_tables) — retry, or narrow the window; or (auth_unavailable) the key could not be checked just now',
        content: { 'application/json': { schema: resolver(ErrorBody) } },
      },
    },
  }),
  queryValidator(HistoryQuery),
  async (c) => {
  const url = new URL(c.req.url);
  const parsed = parseQuery(url);
  if ('error' in parsed) {
    if (parsed.error === 'invalid_cursor') {
      const message = 'cursor is malformed; drop it and start over';
      return fail(c, 400, 'validation_failed', message, { error: 'invalid_cursor', message });
    }
    if (parsed.error === 'offset_too_large') {
      const offset = intParam(url.searchParams.get('offset')) ?? 0;
      const message =
        `offset=${offset} exceeds ${MAX_OFFSET}. Use ?cursor=<next_cursor from previous response> ` +
        'for forward pagination, or narrow the result set with date_from/hours/source.';
      return fail(
        c,
        400,
        'validation_failed',
        message,
        { error: 'offset_too_large', message, max_offset: MAX_OFFSET },
        { max_offset: MAX_OFFSET },
      );
    }
    return fail(c, 400, 'validation_failed', parsed.error, { error: parsed.error });
  }

  const { params, rawFilters } = parsed;

  // A named key reads only the archived sources its scopes cover, and its
  // answer is its own: never cached in between.
  const readable = readableSources(c);
  if (readable) {
    c.header('Cache-Control', 'private, no-store');
    if (params.sources) {
      const refused = params.sources.filter((s) => !readable.includes(s));
      if (refused.length) {
        return refuseSources(c, `This key's scopes do not cover the stored history of: ${refused.join(', ')}.`);
      }
    } else {
      if (!readable.length) return refuseSources(c, "This key's scopes cover no stored history.");
      params.sources = readable;
    }
  }

  const pool = await getPool();
  if (!pool) {
    if (isV1(c)) return fail(c, 503, 'service_unavailable', 'Stored history is not available just now.');
    // The old path has always answered an empty page (200) rather than an error.
    return ok(c, {
      records: [],
      total: 0,
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
    // Families whose data query did not answer. Collected rather than
    // swallowed, because a short page is not a smaller true answer — see the
    // 503 below.
    const failedFamilies: ArchiveTable[] = [];
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
          // Tag each row with the family it came from so the multi-family
          // merge can order by (fetched_at, table_rank, id) and the cursor
          // can record the boundary row's table. Constant per cache key
          // (q.table is fixed), so this is safe to do before caching.
          for (const row of rows) row._family = q.table;
          // Bound the cache: drop oldest when over capacity.
          if (dataCache.size >= DATA_CACHE_MAX) {
            const oldestKey = dataCache.keys().next().value;
            if (oldestKey !== undefined) dataCache.delete(oldestKey);
          }
          dataCache.set(dKey, { rows, ts: dataNowMs });
          return rows;
        } catch (err) {
          log.error({ err, table: q.table }, 'data-history query failed');
          failedFamilies.push(q.table);
          return [] as ArchiveQueryRow[];
        }
      }),
    );
    // Per-family count: a single `total` bigint that the route sums
    // across families.
    //
    // 2-min LRU cache: page-jumper clicks all hit the same WHERE
    // clause (only LIMIT/OFFSET differ, and the count query strips
    // both) so we re-use the previous result instead of re-running
    // the 5-10s aggregate per page click.
    //
    // Count uses a 15s timeout (vs 30s for the data query) so a
    // stuck count never blocks the route. On timeout the route
    // returns the data with total=0; logs.html falls back to "+"
    // pagination instead of jump-to-page.
    const nowMs = Date.now();
    // Skip the count query entirely when the caller passed ?cursor=.
    // Cursor pagination doesn't need `total` — the next_cursor in the
    // response is the only signal it uses. Avoids running the 5-10s
    // COUNT aggregate on every cursor-paged page click.
    const skipCount = params.cursor !== null;
    const countPromise: Promise<number[]> = skipCount
      ? Promise.resolve(plan.queries.map(() => 0))
      : Promise.all(
      plan.queries.map(async (q) => {
        const cq = buildCountSqlForTable(q.table, params);
        const key = countCacheKey(q.table, cq.sql, cq.params);
        const hit = countCache.get(key);
        if (hit && nowMs - hit.ts < COUNT_CACHE_TTL_MS) {
          return hit.count;
        }
        try {
          const r = await runWithTimeout(pool, cq.sql, cq.params, 15);
          const row = r.rows[0] as { total?: string | number } | undefined;
          const count = Number(row?.total ?? 0);
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
            count: 0,
            ts: nowMs - (COUNT_CACHE_TTL_MS - 30_000),
          });
          return 0;
        }
      }),
    );
    const [results, counts] = await Promise.all([dataPromise, countPromise]);

    // A family that did not answer makes the whole page unanswerable, so say
    // so rather than serving what the others returned.
    //
    // The count query is genuinely best-effort — total=0 just means "not
    // counted" and the frontend already treats it that way. The DATA query is
    // not: merging the surviving families produces a page that looks complete,
    // reports a `total` that still includes the missing family, and — worse —
    // builds next_cursor from the last SURVIVING row. The next page seeks
    // strictly past that boundary, so the records the failed family owed to
    // this page are never served on any later page either. With one source it
    // degrades to a bare empty 200, which reads as "nothing matched".
    //
    // Nothing in the cursor can repair that: the boundary is one (ts, id,
    // family) triple, not a position per family, so there is no page to resume
    // from. 503 is the retryable answer, and both callers already treat a
    // non-200 as an error instead of as data.
    if (failedFamilies.length > 0) {
      const tables = [...new Set(failedFamilies)];
      log.warn({ tables }, '/api/data/history: refusing a partial page');
      const message =
        'One or more archive tables did not answer in time, so this page would ' +
        'silently omit records. Retry, or narrow the window with hours/date_from.';
      return fail(
        c,
        503,
        'service_unavailable',
        message,
        { error: 'partial_result', message, failed_tables: tables },
        { failed_tables: tables },
      );
    }

    const total = counts.reduce((a, b) => a + b, 0);

    let merged: ArchiveQueryRow[];
    // hasMore is derived from the PRE-slice pool exceeding the page end —
    // not from `merged.length >= limit`, which fired a phantom next_cursor
    // on an exactly-full final page (returning an empty next fetch).
    let hasMore: boolean;
    if (results.length === 1) {
      const rows = results[0] ?? [];
      if (params.cursor) {
        hasMore = rows.length > params.limit;
        merged = rows.slice(0, params.limit);
      } else {
        hasMore = rows.length > params.offset + params.limit;
        merged = rows.slice(params.offset, params.offset + params.limit);
      }
    } else {
      // Multi-family: combine, sort by (fetched_at, id), apply offset+limit.
      const all: ArchiveQueryRow[] = [];
      for (const part of results) all.push(...part);
      hasMore = all.length > plan.effectiveOffset + plan.limit;
      merged = mergeAndPaginate(all, plan);
    }

    const records = merged.map((r) => formatRecord(r, params.includeData));
    const last = merged[merged.length - 1];
    // Multi-family cursors carry the boundary row's table so the next
    // page's seek can reproduce the (ts, table_rank, id) order.
    // Single-family stays byte-identical to the legacy 2-part format.
    // Unique mode encodes the boundary row's effective_ts — the key its
    // SQL actually orders by — instead of fetched_at; the two coincide
    // for sources without their own timestamp, so legacy in-flight
    // cursors keep (approximately) working across a deploy.
    const multiFamily = results.length > 1;
    const cursorTs = last
      ? params.unique
        ? Number(last.source_timestamp_unix ?? last.fetched_at_epoch)
        : Number(last.fetched_at_epoch)
      : 0;
    const nextCursor =
      hasMore && last
        ? encodeCursor(
            cursorTs,
            last.id,
            multiFamily ? last._family : undefined,
          )
        : null;

    // /api/v1/ writes every time in UTC; the old path keeps naive Sydney time.
    const v1 = isV1(c);
    const filtersApplied = v1
      ? {
          ...rawFilters,
          ...(params.since !== null ? { since: utcIso(params.since) } : {}),
          ...(params.until !== null ? { until: utcIso(params.until) } : {}),
        }
      : rawFilters;
    return ok(c, {
      records: v1 ? records.map(utcRecord) : records,
      total,
      limit: params.limit,
      offset: params.offset,
      count: records.length,
      next_cursor: nextCursor,
      query: { filters_applied: filtersApplied },
    } satisfies z.infer<typeof HistoryPage>);
  } catch (err) {
    log.error({ err }, '/api/data/history error');
    return fail(c, 500, 'internal_error', 'Stored history could not be read.', { error: 'internal error' });
  }
  },
);

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
  state: string | null;
  status: string | null;
  severity: string | null;
  data: Record<string, unknown>;
  is_active: boolean;
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
  // source_timestamp sources, in priority:
  //   1. sidecar.source_timestamp_unix (extracted at write time from the
  //      upstream payload — migration 020). Normalised epoch seconds.
  //   2. data.source_timestamp_unix in the JSONB blob (legacy path; not
  //      currently populated by any source but kept for forward-compat).
  //   3. null (frontend can fall back to fetched_at_iso for display).
  const srcTsUnix = r.source_timestamp_unix ?? pickNum(data, 'source_timestamp_unix');
  return {
    id: r.id,
    source: r.source,
    source_id: r.source_id,
    fetched_at: ts,
    fetched_at_iso: sydneyIsoFromUnix(ts),
    source_timestamp: pickStr(data, 'source_timestamp'),
    source_timestamp_unix: srcTsUnix,
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
    // The SQL selects state and normaliseRow carries it, but it was never
    // emitted — so the Logs list's state pill never rendered.
    state: r.state ?? null,
    status: pickStr(data, 'status'),
    severity: pickStr(data, 'severity'),
    data: includeData ? data : {},
    is_active: pickBool(data, 'is_active', false),
    last_seen: lastSeen,
    last_seen_iso: sydneyIsoFromUnix(lastSeen),
  };
}

// ---------------------------------------------------------------------------
// /api/data/history/filters
// ---------------------------------------------------------------------------

// Resolves the time window for /filters from the same params /history
// accepts: explicit since (epoch secs) > date_from (YYYY-MM-DD or ISO) >
// hours > days. Missing all of them = "All" (filterCache defaults to
// DATA_RETENTION_DAYS). Returns the window in hours so the cache key
// stays simple.
function parseWindowHours(q: URLSearchParams): number | null {
  const since = intParam(q.get('since'));
  const dateFrom = q.get('date_from');
  const hours = intParam(q.get('hours'));
  const days = intParam(q.get('days'));
  const nowSecs = Math.floor(Date.now() / 1000);
  if (since !== null && since > 0) {
    const h = Math.ceil((nowSecs - since) / 3600);
    return h > 0 ? h : null;
  }
  if (dateFrom) {
    const sinceFromDate = parseDateBoundary(dateFrom, false);
    if (sinceFromDate !== null && sinceFromDate > 0) {
      const h = Math.ceil((nowSecs - sinceFromDate) / 3600);
      return h > 0 ? h : null;
    }
  }
  if (hours !== null && hours > 0) return hours;
  if (days !== null && days > 0) return days * 24;
  return null;
}

const Facet = z.object({ value: z.string(), count: z.number().int() });

const HistoryFilters = z
  .object({
    providers: z
      .array(
        z.object({
          key: z.string().describe('The provider, e.g. rfs or livetraffic'),
          name: z.string(),
          icon: z.string().describe('A Font Awesome class for it'),
          color: z.string().describe('Its colour on this site'),
          count: z.number().int().describe('Records in the window'),
          types: z.array(
            z.object({
              alert_type: z.string().describe('The archived source: what /data/history takes as source'),
              name: z.string(),
              count: z.number().int(),
              categories: z.array(Facet),
              subcategories: z.array(Facet),
              statuses: z.array(Facet),
              severities: z.array(Facet),
              states: z.array(Facet).describe('Empty for a source with no placeable records'),
            }),
          ),
        }),
      )
      .describe('Every provider with records in the window, and each of its sources with the values found'),
    date_range: z.object({
      oldest: z.string().nullable().describe('The oldest record, ISO 8601 UTC'),
      newest: z.string().nullable().describe('The newest record, ISO 8601 UTC'),
      oldest_unix: z.number().int().nullable(),
      newest_unix: z.number().int().nullable(),
    }),
  })
  .meta({ id: 'HistoryFilters' });
assertShape<SameShape<z.infer<typeof HistoryFilters>, FilterFacets>>();

const HistoryFiltersQuery = z.object({
  source: opt('Only this archived source'),
  since: opt(`The window starts at this time, ${UNIX}`),
  date_from: opt('The window starts on this date or time when since is not given (as /data/history reads it)'),
  hours: opt('The window is the last this many hours, when neither since nor date_from is given'),
  days: opt('The window is the last this many days, when no earlier parameter is given'),
});

dataHistoryRouter.get(
  '/api/data/history/filters',
  describeRoute({
    operationId: 'getHistoryFilters',
    tags: ['History'],
    summary: 'What stored history can be filtered by',
    description:
      "For each provider and archived source with records in the window, the categories, subcategories, statuses, severities and states found, with counts; and the oldest and newest record. The window is the server's whole retention when no parameter narrows it. No key is needed.",
    responses: okJson(envelope(HistoryFilters), 'The filter values'),
  }),
  queryValidator(HistoryFiltersQuery),
  async (c) => {
    const sourceFilter = c.req.query('source');
    const url = new URL(c.req.url);
    const windowHours = parseWindowHours(url.searchParams);
    const facets = await getFilterFacets(sourceFilter ?? null, windowHours);
    if (!isV1(c)) return ok(c, facets);
    const range = facets.date_range;
    return ok(c, {
      ...facets,
      date_range: { ...range, oldest: utcIso(range.oldest_unix), newest: utcIso(range.newest_unix) },
    } satisfies z.infer<typeof HistoryFilters>);
  },
);

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
interface CountCacheEntry { count: number; ts: number; }
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

const HistoryIncident = z
  .object({
    source: z.string().describe('The archived source, as asked for'),
    source_id: z.string().describe("The incident's id, as asked for"),
    snapshots: z.number().int().describe('How many snapshots follow'),
    history: z
      .array(
        z.object({
          ...historyRecordShape,
          data: z.record(z.string(), z.unknown()).describe('The record exactly as the source gave it'),
        }),
      )
      .describe('Every stored snapshot of the incident, oldest first, at most 5,000'),
  })
  .meta({ id: 'HistoryIncident' });

dataHistoryRouter.get(
  '/api/data/history/incident/:source/:source_id',
  SCOPE_HISTORY,
  describeRoute({
    operationId: 'getHistoryIncident',
    tags: ['History'],
    summary: "One incident's stored snapshots",
    description:
      'Every snapshot this server stored of one incident, oldest first, each with the record exactly as the source gave it: how the incident changed over time. {source} is an archived source (as /data/history returns it) and {source_id} the incident\'s id from that source. An incident with no snapshots answers an empty list. A key reads only the sources its scopes cover.',
    responses: {
      ...okJson(envelope(HistoryIncident), "The incident's snapshots"),
      ...GATED_ERRORS,
      503: {
        description:
          'The archive could not be read just now (service_unavailable); or (auth_unavailable) the key could not be checked just now',
        content: { 'application/json': { schema: resolver(ErrorBody) } },
      },
    },
  }),
  async (c) => {
  const source = c.req.param('source');
  const sourceId = c.req.param('source_id');
  if (!source || !sourceId) {
    return fail(c, 400, 'validation_failed', 'source and source_id are required', {
      error: 'source and source_id are required',
    });
  }

  const readable = readableSources(c);
  if (readable) {
    c.header('Cache-Control', 'private, no-store');
    if (!readable.includes(source)) {
      return refuseSources(c, `This key's scopes do not cover the stored history of: ${source}.`);
    }
  }

  const pool = await getPool();
  if (!pool) {
    if (isV1(c)) return fail(c, 503, 'service_unavailable', 'Stored history is not available just now.');
    // The old path has always answered an empty list (200) rather than an error.
    return ok(c, {
      source,
      source_id: sourceId,
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
      state: null,
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
      activeOnly: false,
      order: 'ASC',
      limit: 5000,
      offset: 0,
      cursor: null,
      includeData: true,
    });
    const result = await runWithTimeout(pool, built.sql, built.params);
    const rows = result.rows.map(normaliseRow);

    // /api/v1/ writes every time in UTC; the old path keeps naive Sydney time.
    const iso = isV1(c) ? utcIso : sydneyIsoFromUnix;
    const history = rows.map((r) => ({
      id: r.id,
      fetched_at: r.fetched_at_epoch,
      fetched_at_iso: iso(r.fetched_at_epoch),
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
      last_seen: pickNum(r.data, 'last_seen'),
      last_seen_iso: iso(pickNum(r.data, 'last_seen')),
    }));

    return ok(c, {
      source,
      source_id: sourceId,
      snapshots: history.length,
      history,
    } satisfies z.infer<typeof HistoryIncident>);
  } catch (err) {
    log.error({ err, source, sourceId }, '/api/data/history/incident error');
    return fail(c, 500, 'internal_error', 'Stored history could not be read.', { error: 'internal error' });
  }
  },
);
