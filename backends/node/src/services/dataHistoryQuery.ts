/**
 * Query builder for /api/data/history.
 *
 * Pure logic — no DB, no Hono, no I/O. Takes the parsed query params and
 * produces a list of (table, sql, params) tuples plus the post-merge
 * pagination instructions. Easy to unit-test: feed in params, assert
 * the SQL string + params array.
 *
 * Schema this builds against (per-family archive tables, see
 * src/db/migrations/002_archive_partitions.sql):
 *
 *   id BIGSERIAL, source TEXT, source_id TEXT, fetched_at TIMESTAMPTZ,
 *   lat DOUBLE PRECISION, lng DOUBLE PRECISION, category TEXT,
 *   subcategory TEXT, data JSONB
 *
 * **No is_live / is_latest columns** in the new schema. unique=1 maps to
 * `DISTINCT ON (source, source_id) ORDER BY source, source_id, fetched_at DESC`
 * inside a subquery, then the outer query re-sorts by fetched_at.
 *
 * Several Python filters used to hit indexed top-level columns
 * (status, severity, title, location_text, source_timestamp_unix). In
 * the new schema those live inside the JSONB `data` blob. We translate
 * with `data->>'<field>'` predicates — works, but the Python optimiser
 * comment about "narrow with date_from/source first" is even more true
 * now.
 */
import type { ArchiveTable } from '../store/archive.js';
import type { DecodedCursor } from './cursorPagination.js';

// Canonical source-name -> archive table mapping. Source is provided by
// the caller as a comma-separated list; we resolve each to its family.
// Unknown sources fall back to archive_misc (matches Python's catch-all
// behaviour for unrecognised feeds, e.g. centralwatch / aviation).
const SOURCE_TO_FAMILY: Record<string, ArchiveTable> = {
  // Waze
  waze_hazard: 'archive_waze',
  waze_jam: 'archive_waze',
  waze_police: 'archive_waze',
  waze_roadwork: 'archive_waze',
  // LiveTraffic NSW
  traffic_incident: 'archive_traffic',
  traffic_roadwork: 'archive_traffic',
  traffic_flood: 'archive_traffic',
  traffic_fire: 'archive_traffic',
  traffic_majorevent: 'archive_traffic',
  livetraffic: 'archive_traffic',
  // RFS
  rfs: 'archive_rfs',
  // Power
  endeavour: 'archive_power',
  endeavour_current: 'archive_power',
  endeavour_planned: 'archive_power',
  endeavour_maintenance: 'archive_power',
  ausgrid: 'archive_power',
  essential: 'archive_power',
  essential_current: 'archive_power',
  essential_planned: 'archive_power',
  essential_future: 'archive_power',
  essential_energy_cancelled: 'archive_power',
  // Misc — BoM, beach, weather, pager, news, aviation, centralwatch
  bom: 'archive_misc',
  bom_warning: 'archive_misc',
  bom_land: 'archive_misc',
  bom_marine: 'archive_misc',
  beach: 'archive_misc',
  beachsafe: 'archive_misc',
  beachsafe_details: 'archive_misc',
  beachwatch: 'archive_misc',
  weather: 'archive_misc',
  weather_current: 'archive_misc',
  weather_radar: 'archive_misc',
  pager: 'archive_misc',
  news: 'archive_misc',
  aviation: 'archive_misc',
  centralwatch: 'archive_misc',
};

export const ALL_ARCHIVE_TABLES: ArchiveTable[] = [
  'archive_waze',
  'archive_traffic',
  'archive_rfs',
  'archive_power',
  'archive_misc',
];

/** Sources frontend should never receive in history results. */
export const DEPRECATED_SOURCES = new Set<string>(['essential_energy_cancelled']);

/**
 * Map a list of source names to the minimum set of archive tables
 * needed to satisfy the query. Empty list (or undefined) means "all
 * five families". Unknown sources route to archive_misc — the same
 * conservative fallback Python uses; the WHERE-clause `source IN (...)`
 * still scopes the actual rows.
 */
export function sourceFamilies(sources: string[] | null | undefined): ArchiveTable[] {
  if (!sources || sources.length === 0) return [...ALL_ARCHIVE_TABLES];
  const set = new Set<ArchiveTable>();
  for (const s of sources) {
    const fam = SOURCE_TO_FAMILY[s] ?? 'archive_misc';
    set.add(fam);
  }
  // Stable order so cache keys built from the family list don't churn.
  return ALL_ARCHIVE_TABLES.filter((t) => set.has(t));
}

export type SortDir = 'ASC' | 'DESC';

export interface DataHistoryParams {
  /** Comma-split, already-trimmed list. null = no source filter. */
  sources: string[] | null;
  sourceId: string | null;
  category: string[] | null;
  subcategory: string[] | null;
  status: string[] | null;
  severity: string[] | null;

  /** Time window — epoch seconds. */
  since: number | null;
  until: number | null;
  sinceSource: number | null;
  untilSource: number | null;

  search: string | null;
  title: string | null;
  location: string | null;

  lat: number | null;
  lng: number | null;
  radiusKm: number;

  unique: boolean;
  /** Mirrors Python `live_only` / `historical_only` — implemented best-effort against JSONB. */
  liveOnly: boolean;
  historicalOnly: boolean;
  activeOnly: boolean;

  order: SortDir;
  limit: number;
  offset: number;
  cursor: DecodedCursor | null;
  /** ?full=1 — include the full data JSONB blob in each row. */
  includeData: boolean;
}

export interface BuiltQuery {
  /** SQL text with $1, $2, ... placeholders. */
  sql: string;
  /** Parameter values, in order. */
  params: unknown[];
  /** Tables this single SQL touches. Length 1 for the fast path; >1 SQL strings come from buildPlan. */
  table: ArchiveTable;
}

export interface QueryPlan {
  /** One BuiltQuery per archive table that needs to be hit. */
  queries: BuiltQuery[];
  /** Pagination to apply to the merged result set in Node. */
  effectiveOffset: number;
  limit: number;
  order: SortDir;
}

interface ConditionAcc {
  parts: string[];
  params: unknown[];
}

function pushIn(acc: ConditionAcc, column: string, values: string[]): void {
  if (values.length === 1) {
    acc.parts.push(`${column} = $${acc.params.length + 1}`);
    acc.params.push(values[0]);
    return;
  }
  const placeholders = values.map((_, i) => `$${acc.params.length + i + 1}`);
  acc.parts.push(`${column} IN (${placeholders.join(',')})`);
  for (const v of values) acc.params.push(v);
}

function pushJsonbIn(acc: ConditionAcc, jsonField: string, values: string[]): void {
  // (data->>'<field>') for indexed access is fine; we just don't have
  // an index on it. Still cheaper than the alternative of scanning every
  // row in Node. Casts via TEXT comparison match Python's column-string
  // semantics for status/severity/title.
  const col = `(data->>'${jsonField}')`;
  pushIn(acc, col, values);
}

/**
 * Build the SQL text + params for a single archive table, given the
 * already-validated request params. Exported for unit testing.
 */
export function buildSqlForTable(table: ArchiveTable, p: DataHistoryParams): BuiltQuery {
  const acc: ConditionAcc = { parts: [], params: [] };

  // Source filter — comma-list IN (...).
  if (p.sources && p.sources.length > 0) {
    pushIn(acc, 'source', p.sources);
  }

  // Exclude deprecated sources, unless the caller explicitly listed one.
  const explicitlyAskedForDeprecated =
    p.sources && p.sources.some((s) => DEPRECATED_SOURCES.has(s));
  if (!explicitlyAskedForDeprecated && DEPRECATED_SOURCES.size > 0) {
    const dep = Array.from(DEPRECATED_SOURCES);
    const placeholders = dep.map((_, i) => `$${acc.params.length + i + 1}`);
    acc.parts.push(`source NOT IN (${placeholders.join(',')})`);
    for (const d of dep) acc.params.push(d);
  }

  if (p.sourceId) {
    acc.parts.push(`source_id = $${acc.params.length + 1}`);
    acc.params.push(p.sourceId);
  }

  if (p.category && p.category.length > 0) {
    pushIn(acc, 'category', p.category);
  }
  if (p.subcategory && p.subcategory.length > 0) {
    pushIn(acc, 'subcategory', p.subcategory);
  }

  // status / severity / title / location / source_timestamp_unix all live
  // inside the JSONB blob in the new schema. Use ->> '...' predicates;
  // these aren't indexed, but the (source, source_id, fetched_at) and
  // (fetched_at) indexes still drive the seek so the row-filter is over
  // a small bounded set per query.
  if (p.status && p.status.length > 0) {
    pushJsonbIn(acc, 'status', p.status);
  }
  if (p.severity && p.severity.length > 0) {
    pushJsonbIn(acc, 'severity', p.severity);
  }

  if (p.since !== null) {
    acc.parts.push(`fetched_at >= to_timestamp($${acc.params.length + 1})`);
    acc.params.push(p.since);
  }
  if (p.until !== null) {
    acc.parts.push(`fetched_at <= to_timestamp($${acc.params.length + 1})`);
    acc.params.push(p.until);
  }
  if (p.sinceSource !== null) {
    acc.parts.push(
      `((data->>'source_timestamp_unix')::bigint) >= $${acc.params.length + 1}`,
    );
    acc.params.push(p.sinceSource);
  }
  if (p.untilSource !== null) {
    acc.parts.push(
      `((data->>'source_timestamp_unix')::bigint) <= $${acc.params.length + 1}`,
    );
    acc.params.push(p.untilSource);
  }

  if (p.activeOnly) {
    // Python checked `is_active = 1` on a top-level column; in the new
    // schema we look it up under `data`. Boolean OR int 1.
    acc.parts.push(
      `(data->>'is_active' IN ('1','true','True'))`,
    );
  }

  if (p.liveOnly) {
    acc.parts.push(`(data->>'is_live' IN ('1','true','True'))`);
  } else if (p.historicalOnly) {
    acc.parts.push(`(data->>'is_live' IN ('0','false','False'))`);
  }

  if (p.search) {
    const ph = `$${acc.params.length + 1}`;
    acc.parts.push(`((data->>'title') ILIKE ${ph} OR (data->>'location_text') ILIKE ${ph})`);
    acc.params.push(`%${p.search}%`);
  }
  if (p.title) {
    acc.parts.push(`(data->>'title') ILIKE $${acc.params.length + 1}`);
    acc.params.push(`%${p.title}%`);
  }
  if (p.location) {
    acc.parts.push(`(data->>'location_text') ILIKE $${acc.params.length + 1}`);
    acc.params.push(`%${p.location}%`);
  }

  if (p.lat !== null && p.lng !== null) {
    // Approximate bounding box, identical math to Python (110.574 km / deg
    // latitude, scaled by cos(lat) for longitude). Avoids the Earth-curve
    // exactness of PostGIS — we don't need it for a filter pre-screen, and
    // not requiring PostGIS keeps the schema portable.
    const latDelta = p.radiusKm / 111.0;
    const cosLat = Math.cos((p.lat * Math.PI) / 180);
    const lonDelta = p.radiusKm / (111.0 * Math.max(0.0001, Math.abs(cosLat)));
    const i = acc.params.length;
    acc.parts.push(`lat BETWEEN $${i + 1} AND $${i + 2}`);
    acc.parts.push(`lng BETWEEN $${i + 3} AND $${i + 4}`);
    acc.params.push(p.lat - latDelta, p.lat + latDelta, p.lng - lonDelta, p.lng + lonDelta);
  }

  // Cursor seek clause. (fetched_at, id) row-value comparison — same
  // shape Python uses, but on TIMESTAMPTZ instead of unix int. The
  // index `(fetched_at DESC)` on every archive_* table supports this.
  if (p.cursor) {
    const cmp = p.order === 'DESC' ? '<' : '>';
    const i = acc.params.length;
    acc.parts.push(
      `(fetched_at ${cmp} to_timestamp($${i + 1}) ` +
        `OR (fetched_at = to_timestamp($${i + 2}) AND id ${cmp} $${i + 3}))`,
    );
    acc.params.push(p.cursor.fetchedAt, p.cursor.fetchedAt, p.cursor.rowId);
  }

  const whereClause = acc.parts.length > 0 ? `WHERE ${acc.parts.join(' AND ')}` : '';

  // Project the same column list every family produces so the merge in
  // Node sees a uniform row shape. fetched_at_epoch is a derived int we
  // emit alongside the timestamp so cursor encoding doesn't have to
  // re-parse a string.
  // include_data toggles whether we ship the heavy JSONB blob — list
  // views skip it for ~10× smaller payloads.
  const dataCol = p.includeData ? 'data' : "'{}'::jsonb AS data";

  const select = `
    SELECT id, source, source_id,
           extract(epoch FROM fetched_at)::bigint AS fetched_at_epoch,
           fetched_at,
           lat, lng, category, subcategory,
           ${dataCol}
    FROM ${table}
  `;

  const orderClause = `ORDER BY fetched_at ${p.order}, id ${p.order}`;

  // Multi-table merge does its own LIMIT after sort; per-table query
  // still pulls (offset + limit) so we have enough material to merge.
  // For the single-table fast path the outer plan applies LIMIT/OFFSET
  // directly. Here we always pass through both — the buildPlan helper
  // decides which path to use.
  const fetchSize = p.cursor ? p.limit : p.offset + p.limit;
  const limitClause = `LIMIT $${acc.params.length + 1}`;
  acc.params.push(fetchSize);

  if (p.unique) {
    // unique=1 path. Filter on the is_latest partial index, then
    // DISTINCT ON to dedupe within the small lag window. Writes are
    // append-only (is_latest=true on insert); services/isLatestRefresher
    // periodically flips superseded rows to false. Between refreshes
    // there can be 2-3 is_latest=true rows for the same source_id —
    // DISTINCT ON keeps only the newest.
    //
    // The partial index `idx_<table>_src_sid_latest (source, source_id)
    // WHERE is_latest = true AND source_id IS NOT NULL` makes this
    // sub-ms regardless of table size — only ~one row per source_id
    // qualifies for the partial index, so scanning + DISTINCT ON over
    // that small set is essentially instant.
    const isLatestClause = acc.parts.length > 0
      ? `${whereClause} AND is_latest = true`
      : `WHERE is_latest = true`;
    const inner = `
      SELECT DISTINCT ON (source, source_id)
             id, source, source_id,
             extract(epoch FROM fetched_at)::bigint AS fetched_at_epoch,
             fetched_at,
             lat, lng, category, subcategory,
             ${p.includeData ? 'data' : "'{}'::jsonb AS data"}
      FROM ${table}
      ${isLatestClause}
      ORDER BY source, source_id, fetched_at DESC, id DESC
    `;
    const sql = `
      SELECT * FROM (${inner}) AS u
      ORDER BY fetched_at_epoch ${p.order}, id ${p.order}
      ${limitClause}
    `;
    return { sql, params: acc.params, table };
  }

  return {
    sql: `${select}${whereClause} ${orderClause} ${limitClause}`,
    params: acc.params,
    table,
  };
}

/**
 * Build a COUNT query for the same WHERE clauses as buildSqlForTable,
 * but without LIMIT/OFFSET/ORDER BY and crucially without the cursor
 * seek clause — `total` should reflect "how many rows match the user's
 * filters", not "how many rows after this page". Returns a bigint
 * column `n` as the only output.
 *
 * For unique=1 we count DISTINCT (source, source_id) to match the
 * DISTINCT ON the data query applies, so the page-count math the
 * frontend uses (Math.ceil(total/limit)) lines up with what's actually
 * paginatable.
 */
export function buildCountSqlForTable(
  table: ArchiveTable,
  p: DataHistoryParams,
): BuiltQuery {
  // Same WHERE-builder as buildSqlForTable but skip the cursor clause.
  const acc: ConditionAcc = { parts: [], params: [] };

  if (p.sources && p.sources.length > 0) pushIn(acc, 'source', p.sources);

  const explicitlyAskedForDeprecated =
    p.sources && p.sources.some((s) => DEPRECATED_SOURCES.has(s));
  if (!explicitlyAskedForDeprecated && DEPRECATED_SOURCES.size > 0) {
    const dep = Array.from(DEPRECATED_SOURCES);
    const placeholders = dep.map((_, i) => `$${acc.params.length + i + 1}`);
    acc.parts.push(`source NOT IN (${placeholders.join(',')})`);
    for (const d of dep) acc.params.push(d);
  }

  if (p.sourceId) {
    acc.parts.push(`source_id = $${acc.params.length + 1}`);
    acc.params.push(p.sourceId);
  }
  if (p.category && p.category.length > 0) pushIn(acc, 'category', p.category);
  if (p.subcategory && p.subcategory.length > 0)
    pushIn(acc, 'subcategory', p.subcategory);
  if (p.status && p.status.length > 0) pushJsonbIn(acc, 'status', p.status);
  if (p.severity && p.severity.length > 0)
    pushJsonbIn(acc, 'severity', p.severity);

  if (p.since !== null) {
    acc.parts.push(`fetched_at >= to_timestamp($${acc.params.length + 1})`);
    acc.params.push(p.since);
  }
  if (p.until !== null) {
    acc.parts.push(`fetched_at <= to_timestamp($${acc.params.length + 1})`);
    acc.params.push(p.until);
  }
  if (p.search) {
    acc.parts.push(
      `(data->>'title' ILIKE $${acc.params.length + 1} OR (data->>'location_text') ILIKE $${acc.params.length + 1})`,
    );
    acc.params.push(`%${p.search}%`);
  }
  if (p.title) {
    acc.parts.push(`(data->>'title') ILIKE $${acc.params.length + 1}`);
    acc.params.push(`%${p.title}%`);
  }
  if (p.location) {
    acc.parts.push(`(data->>'location_text') ILIKE $${acc.params.length + 1}`);
    acc.params.push(`%${p.location}%`);
  }
  if (p.lat !== null && p.lng !== null) {
    const latDelta = p.radiusKm / 111.0;
    const cosLat = Math.cos((p.lat * Math.PI) / 180);
    const lonDelta = p.radiusKm / (111.0 * Math.max(0.0001, Math.abs(cosLat)));
    const i = acc.params.length;
    acc.parts.push(`lat BETWEEN $${i + 1} AND $${i + 2}`);
    acc.parts.push(`lng BETWEEN $${i + 3} AND $${i + 4}`);
    acc.params.push(p.lat - latDelta, p.lat + latDelta, p.lng - lonDelta, p.lng + lonDelta);
  }

  const whereClause = acc.parts.length > 0 ? `WHERE ${acc.parts.join(' AND ')}` : '';

  // Cap the count at COUNT_CAP rows so a billion-row archive_waze can't
  // burn the 60s statement_timeout chasing a number the frontend
  // doesn't need exact. The page-jumper on logs.html only navigates
  // the first ~5000 pages anyway (page * limit > 100k is unusable).
  //
  // CRITICAL: for the unique=1 path the LIMIT must be applied BEFORE
  // DISTINCT — `SELECT DISTINCT ... LIMIT N` evaluates DISTINCT first
  // and only then truncates, so the cap doesn't bound the scan. Use a
  // doubly-nested subquery so the LIMIT operates on raw rows.
  //
  // The `ORDER BY fetched_at DESC` in the inner SELECT is what makes
  // the LIMIT actually fast: it forces Postgres to use the
  // idx_archive_*_ts (fetched_at DESC) index instead of doing a
  // partition-by-partition sequential scan. Without it, the planner
  // may walk every partition in storage order looking for matching
  // rows, which on archive_waze (millions of rows across many monthly
  // partitions) blows past the 60s budget even for a 50k cap. With it,
  // the planner walks the index from now backwards and stops at 50k.
  // SELECT shape: total + live_count (FILTER over `data->>'is_live'`).
  // ended_count is computed in JS as `total - live_count` to match
  // formatRecord's truthy-default semantics (missing is_live → live).
  // Inferring ended from `is_live IN ('0','false','False')` would
  // silently drop rows with no is_live key.
  //
  // Both columns ride through the doubly-nested LIMIT pattern so the
  // 50k cap still bounds the index walk before DISTINCT runs.
  const COUNT_CAP = 50_000;
  // is_live is now a real boolean column (migration 012). No more
  // per-row JSONB extraction — direct column read, planner-friendly.
  const aggLine = `SELECT COUNT(*)::bigint AS total, COUNT(*) FILTER (WHERE is_live)::bigint AS live_count`;
  const sql = p.unique
    ? `${aggLine}
       FROM ${table}
       ${whereClause ? whereClause + ' AND is_latest = true' : 'WHERE is_latest = true'}`
    : `${aggLine}
       FROM (
         SELECT is_live FROM ${table} ${whereClause}
         ORDER BY fetched_at DESC
         LIMIT ${COUNT_CAP}
       ) sub`;

  return { sql, params: acc.params, table };
}

/**
 * Build the full query plan: per-family SQL + merge instructions. The
 * single-family case produces one query; multi-family fans out and the
 * caller merges rows in Node.
 */
export function buildPlan(p: DataHistoryParams): QueryPlan {
  const tables = sourceFamilies(p.sources);
  const queries = tables.map((t) => buildSqlForTable(t, p));
  return {
    queries,
    // Cursor pagination supersedes offset (matches Python).
    effectiveOffset: p.cursor ? 0 : p.offset,
    limit: p.limit,
    order: p.order,
  };
}

/**
 * Row shape the per-table queries emit. Keep this in sync with the
 * SELECT clause in buildSqlForTable so consumers have one place to type
 * the merged stream.
 */
export interface ArchiveQueryRow {
  id: number;
  source: string;
  source_id: string | null;
  fetched_at_epoch: number;
  fetched_at: Date | string;
  lat: number | null;
  lng: number | null;
  category: string | null;
  subcategory: string | null;
  data: Record<string, unknown>;
}

/**
 * Sort the merged rows from multiple tables and apply offset+limit in
 * Node. Stable sort by (fetched_at_epoch, id), direction-aware.
 */
export function mergeAndPaginate(
  rows: ArchiveQueryRow[],
  plan: QueryPlan,
): ArchiveQueryRow[] {
  const dir = plan.order === 'DESC' ? -1 : 1;
  rows.sort((a, b) => {
    if (a.fetched_at_epoch !== b.fetched_at_epoch) {
      return (a.fetched_at_epoch - b.fetched_at_epoch) * dir;
    }
    return (a.id - b.id) * dir;
  });
  return rows.slice(plan.effectiveOffset, plan.effectiveOffset + plan.limit);
}
