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
 * Build the unique=1 SQL via the sidecar table. See buildSqlForTable's
 * `if (p.unique)` branch for context.
 *
 * The query has two filter scopes:
 *
 *   sidecarAcc — applied inside the CTE that pulls top-N keys out of
 *     ${table}_latest. Includes filters that map cleanly onto sidecar
 *     columns: source, source_id, time-window via latest_fetched_at.
 *
 *   parentAcc — applied after the JOIN to ${table}. Everything else
 *     (category/subcategory, JSONB fields, lat/lng radius, cursor).
 *
 * The CTE's LIMIT is multiplied by 5 vs. the user's requested limit so
 * post-JOIN filters can drop a few rows without us returning fewer
 * results than requested. For unfiltered ?unique=1 calls the multiplier
 * is wasted but each row is a tiny tuple, so the cost is negligible
 * (~100 rows of a 3-column table).
 */
function buildUniqueQuery(table: ArchiveTable, p: DataHistoryParams): BuiltQuery {
  const sidecarAcc: ConditionAcc = { parts: [], params: [] };
  const parentAcc: ConditionAcc = { parts: [], params: [] };

  // ---- sidecar filters ----
  if (p.sources && p.sources.length > 0) pushIn(sidecarAcc, 'source', p.sources);
  const explicitlyAskedForDeprecated =
    p.sources && p.sources.some((s) => DEPRECATED_SOURCES.has(s));
  if (!explicitlyAskedForDeprecated && DEPRECATED_SOURCES.size > 0) {
    const dep = Array.from(DEPRECATED_SOURCES);
    const placeholders = dep.map(
      (_, i) => `$${sidecarAcc.params.length + i + 1}`,
    );
    sidecarAcc.parts.push(`source NOT IN (${placeholders.join(',')})`);
    for (const d of dep) sidecarAcc.params.push(d);
  }
  if (p.sourceId) {
    sidecarAcc.parts.push(`source_id = $${sidecarAcc.params.length + 1}`);
    sidecarAcc.params.push(p.sourceId);
  }
  // Time-window filter uses the SOURCE'S OWN publish timestamp
  // (extracted at write time into source_timestamp_unix on the
  // sidecar — see migration 020). Falls back to last_seen_at when
  // the upstream payload didn't expose a usable timestamp so rows
  // without source-side time still surface. Backend ingest times
  // (fetched_at / latest_fetched_at) deliberately aren't used for
  // user-facing windows — they describe our polling, not what the
  // user means by "incidents in the last 24h".
  if (p.since !== null) {
    sidecarAcc.parts.push(
      `COALESCE(source_timestamp_unix, EXTRACT(EPOCH FROM last_seen_at)::bigint) >= $${sidecarAcc.params.length + 1}`,
    );
    sidecarAcc.params.push(p.since);
  }
  if (p.until !== null) {
    sidecarAcc.parts.push(
      `COALESCE(source_timestamp_unix, EXTRACT(EPOCH FROM last_seen_at)::bigint) <= $${sidecarAcc.params.length + 1}`,
    );
    sidecarAcc.params.push(p.until);
  }

  // CTE row budget — must cover (offset + limit) plus headroom for
  // post-JOIN filter drops. 2x multiplier on the fetch target handles
  // realistic filter selectivity; cap is just over MAX_OFFSET (api
  // rejects offsets beyond 10k anyway).
  const fetchTarget = p.cursor ? p.limit : p.offset + p.limit;
  const cteLimit = Math.min(Math.max(fetchTarget * 2, 100), 25_000);
  const sidecarWhere =
    sidecarAcc.parts.length > 0 ? `WHERE ${sidecarAcc.parts.join(' AND ')}` : '';

  // ---- parent filters (post-JOIN) ----
  // Renumber placeholders so parent params start AFTER sidecar params.
  // Helper appends to parentAcc with offset.
  const offset = (): number => sidecarAcc.params.length + parentAcc.params.length;

  if (p.category && p.category.length > 0) {
    const placeholders = p.category.map((_, i) => `$${offset() + i + 1}`);
    parentAcc.parts.push(
      p.category.length === 1
        ? `a.category = ${placeholders[0]}`
        : `a.category IN (${placeholders.join(',')})`,
    );
    for (const v of p.category) parentAcc.params.push(v);
  }
  if (p.subcategory && p.subcategory.length > 0) {
    const placeholders = p.subcategory.map((_, i) => `$${offset() + i + 1}`);
    parentAcc.parts.push(
      p.subcategory.length === 1
        ? `a.subcategory = ${placeholders[0]}`
        : `a.subcategory IN (${placeholders.join(',')})`,
    );
    for (const v of p.subcategory) parentAcc.params.push(v);
  }
  if (p.status && p.status.length > 0) {
    const placeholders = p.status.map((_, i) => `$${offset() + i + 1}`);
    parentAcc.parts.push(
      `(a.data->>'status') IN (${placeholders.join(',')})`,
    );
    for (const v of p.status) parentAcc.params.push(v);
  }
  if (p.severity && p.severity.length > 0) {
    const placeholders = p.severity.map((_, i) => `$${offset() + i + 1}`);
    parentAcc.parts.push(
      `(a.data->>'severity') IN (${placeholders.join(',')})`,
    );
    for (const v of p.severity) parentAcc.params.push(v);
  }
  if (p.sinceSource !== null) {
    parentAcc.parts.push(
      `((a.data->>'source_timestamp_unix')::bigint) >= $${offset() + 1}`,
    );
    parentAcc.params.push(p.sinceSource);
  }
  if (p.untilSource !== null) {
    parentAcc.parts.push(
      `((a.data->>'source_timestamp_unix')::bigint) <= $${offset() + 1}`,
    );
    parentAcc.params.push(p.untilSource);
  }
  if (p.activeOnly) {
    parentAcc.parts.push(`(a.data->>'is_active' IN ('1','true','True'))`);
  }
  if (p.liveOnly) {
    parentAcc.parts.push(`(a.data->>'is_live' IN ('1','true','True'))`);
  } else if (p.historicalOnly) {
    parentAcc.parts.push(`(a.data->>'is_live' IN ('0','false','False'))`);
  }
  if (p.search) {
    const ph = `$${offset() + 1}`;
    parentAcc.parts.push(
      `((a.data->>'title') ILIKE ${ph} OR (a.data->>'location_text') ILIKE ${ph})`,
    );
    parentAcc.params.push(`%${p.search}%`);
  }
  if (p.title) {
    parentAcc.parts.push(`(a.data->>'title') ILIKE $${offset() + 1}`);
    parentAcc.params.push(`%${p.title}%`);
  }
  if (p.location) {
    parentAcc.parts.push(`(a.data->>'location_text') ILIKE $${offset() + 1}`);
    parentAcc.params.push(`%${p.location}%`);
  }
  if (p.lat !== null && p.lng !== null) {
    const latDelta = p.radiusKm / 111.0;
    const cosLat = Math.cos((p.lat * Math.PI) / 180);
    const lonDelta = p.radiusKm / (111.0 * Math.max(0.0001, Math.abs(cosLat)));
    const i = offset();
    parentAcc.parts.push(`a.lat BETWEEN $${i + 1} AND $${i + 2}`);
    parentAcc.parts.push(`a.lng BETWEEN $${i + 3} AND $${i + 4}`);
    parentAcc.params.push(
      p.lat - latDelta,
      p.lat + latDelta,
      p.lng - lonDelta,
      p.lng + lonDelta,
    );
  }
  if (p.cursor) {
    const cmp = p.order === 'DESC' ? '<' : '>';
    const i = offset();
    parentAcc.parts.push(
      `(a.fetched_at ${cmp} to_timestamp($${i + 1}) ` +
        `OR (a.fetched_at = to_timestamp($${i + 2}) AND a.id ${cmp} $${i + 3}))`,
    );
    parentAcc.params.push(p.cursor.fetchedAt, p.cursor.fetchedAt, p.cursor.rowId);
  }

  const parentWhere =
    parentAcc.parts.length > 0 ? `AND ${parentAcc.parts.join(' AND ')}` : '';
  const dataCol = p.includeData ? 'a.data' : "'{}'::jsonb AS data";

  const allParams = [...sidecarAcc.params, ...parentAcc.params];
  const fetchSize = p.cursor ? p.limit : p.offset + p.limit;
  allParams.push(fetchSize);
  const limitPlaceholder = `$${allParams.length}`;

  // ORDER BY also uses source_timestamp_unix with last_seen_at fallback,
  // matching the filter semantics. Most-recently-published-by-the-source
  // lands at the top of the list.
  const sql = `
    WITH top_keys AS (
      SELECT source, source_id, latest_fetched_at, last_seen_at,
             source_timestamp_unix,
             COALESCE(source_timestamp_unix,
                      EXTRACT(EPOCH FROM last_seen_at)::bigint) AS effective_ts
      FROM ${table}_latest
      ${sidecarWhere}
      ORDER BY effective_ts ${p.order}
      LIMIT ${cteLimit}
    )
    SELECT a.id, a.source, a.source_id,
           extract(epoch FROM a.fetched_at)::bigint AS fetched_at_epoch,
           a.fetched_at,
           extract(epoch FROM k.last_seen_at)::bigint AS last_seen_at_epoch,
           k.last_seen_at,
           k.source_timestamp_unix,
           a.lat, a.lng, a.category, a.subcategory,
           ${dataCol}
    FROM top_keys k
    JOIN ${table} a
      ON a.source = k.source
     AND a.source_id = k.source_id
     AND a.fetched_at = k.latest_fetched_at
    WHERE TRUE
    ${parentWhere}
    ORDER BY k.effective_ts ${p.order}, a.id ${p.order}
    LIMIT ${limitPlaceholder}
  `;
  return { sql, params: allParams, table };
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
    // unique=1 path — sidecar driven (migration 017).
    //
    // Architecture: ${table}_latest holds (source, source_id,
    // latest_fetched_at) — one row per incident. The writer maintains
    // it via UPSERT on every parent INSERT (see archive.ts
    // upsertLatestSidecar). Reads pull the top N keys by
    // latest_fetched_at DESC out of the sidecar (small, indexed),
    // then JOIN to the parent for full row data.
    //
    // Why this is fast: the prior bounded-DISTINCT-ON pattern scanned
    // up to 50k recent rows and sorted them — under archive_waze
    // ingest pressure this hit the 30s statement_timeout regularly.
    // The sidecar ORDER BY latest_fetched_at DESC LIMIT N is a
    // sub-millisecond index walk; the parent JOIN is N PK-equivalent
    // index lookups via idx_archive_*_src_sid_ts. Total: <100ms cold,
    // single-digit ms warm.
    //
    // Filter routing:
    //   - source / since / until / sourceId — applied on the sidecar
    //     (all columns the sidecar carries).
    //   - everything else (category, subcategory, status, severity,
    //     title/location/search, lat/lng radius, JSONB liveOnly etc.)
    //     — applied post-JOIN on parent columns.
    //
    // For the common ?unique=1&hours=24 workload only sidecar filters
    // apply, so we hit the (latest_fetched_at DESC) index directly
    // and pull just N rows out of the sidecar.
    return buildUniqueQuery(table, p);
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
  // ROLLED BACK from is_live/is_latest filtering — see buildSqlForTable
  // for context. Back to bounded count using the (fetched_at DESC)
  // index. live_count derived from JSONB at extract time; per-row
  // overhead is fine because the LIMIT bounds the scan.
  const IS_LIVE_PRED = `(data->>'is_live') IN ('1','true','True')`;
  const aggLine = `SELECT COUNT(*)::bigint AS total, COUNT(*) FILTER (WHERE is_live_truthy)::bigint AS live_count`;

  if (p.unique) {
    // Sidecar-driven unique count (migration 017). Sidecar carries
    // (source, source_id, latest_fetched_at) — exactly the columns
    // we filter on for time + source. Anything else (category,
    // JSONB fields, lat/lng radius) only the parent has.
    //
    // Trade-off: live_count is set to total here. The previous count
    // path bounded-scanned the parent and FILTERed by data->>'is_live',
    // which gave an accurate live/ended split — but it ran 10s+ on
    // archive_waze under ingest pressure. Counting the sidecar is
    // O(matching_rows) and finishes in milliseconds; the small
    // regression (frontend's "X live, Y ended" pill always shows
    // ended=0 for unique=1 lists) is acceptable for the latency win.
    const sidecarAcc: ConditionAcc = { parts: [], params: [] };
    if (p.sources && p.sources.length > 0) pushIn(sidecarAcc, 'source', p.sources);
    const explicitlyAskedForDeprecated2 =
      p.sources && p.sources.some((s) => DEPRECATED_SOURCES.has(s));
    if (!explicitlyAskedForDeprecated2 && DEPRECATED_SOURCES.size > 0) {
      const dep = Array.from(DEPRECATED_SOURCES);
      const placeholders = dep.map(
        (_, i) => `$${sidecarAcc.params.length + i + 1}`,
      );
      sidecarAcc.parts.push(`source NOT IN (${placeholders.join(',')})`);
      for (const d of dep) sidecarAcc.params.push(d);
    }
    if (p.sourceId) {
      sidecarAcc.parts.push(`source_id = $${sidecarAcc.params.length + 1}`);
      sidecarAcc.params.push(p.sourceId);
    }
    // Match the unique=1 data query — filter on the source's own
    // publish timestamp with last_seen_at fallback.
    if (p.since !== null) {
      sidecarAcc.parts.push(
        `COALESCE(source_timestamp_unix, EXTRACT(EPOCH FROM last_seen_at)::bigint) >= $${sidecarAcc.params.length + 1}`,
      );
      sidecarAcc.params.push(p.since);
    }
    if (p.until !== null) {
      sidecarAcc.parts.push(
        `COALESCE(source_timestamp_unix, EXTRACT(EPOCH FROM last_seen_at)::bigint) <= $${sidecarAcc.params.length + 1}`,
      );
      sidecarAcc.params.push(p.until);
    }
    const sidecarWhere =
      sidecarAcc.parts.length > 0 ? `WHERE ${sidecarAcc.parts.join(' AND ')}` : '';
    const sql = `
      SELECT COUNT(*)::bigint AS total, COUNT(*)::bigint AS live_count
      FROM (
        SELECT 1 FROM ${table}_latest ${sidecarWhere} LIMIT ${COUNT_CAP}
      ) sub
    `;
    return { sql, params: sidecarAcc.params, table };
  }

  const sql = `${aggLine}
       FROM (
         SELECT ${IS_LIVE_PRED} AS is_live_truthy
         FROM ${table} ${whereClause}
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
  /** When the most recently STORED row was inserted (data last changed). */
  fetched_at_epoch: number;
  fetched_at: Date | string;
  /** Only present on unique=1 results. When the incident was last seen
   *  in a poll, regardless of whether the data changed. epoch seconds. */
  last_seen_at_epoch?: number | null;
  last_seen_at?: Date | string | null;
  /** Only present on unique=1 results. Upstream feed's own publish /
   *  last-updated time in epoch seconds — null when the source payload
   *  didn't carry a usable timestamp. */
  source_timestamp_unix?: number | null;
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
