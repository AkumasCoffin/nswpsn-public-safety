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
 * unique=1 maps to `DISTINCT ON (source, source_id) ORDER BY source,
 * source_id, fetched_at DESC` inside a subquery, then the outer query
 * re-sorts by fetched_at.
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

/**
 * Global rank of an archive table — its index in ALL_ARCHIVE_TABLES.
 * Used as the secondary sort key (after fetched_at, before id) when a
 * query merges rows from multiple families, so the cross-table order is
 * deterministic AND reproducible from a cursor. Returns -1 for unknown
 * tables; callers treat that as "no table info" and fall back to a
 * single-table seek.
 */
export function tableRank(table: ArchiveTable): number {
  return ALL_ARCHIVE_TABLES.indexOf(table);
}

type RankRelation = 'equal' | 'after' | 'before';

/**
 * Where `thisRank` sits relative to `cursorRank` in the merged stream's
 * sort order. In ASC order rank ascends (lower rank first); in DESC it
 * descends. "after" = comes later in the stream than the cursor row's
 * table; "before" = already emitted on a prior page.
 */
function rankRelation(
  thisRank: number,
  cursorRank: number,
  order: SortDir,
): RankRelation {
  if (thisRank === cursorRank) return 'equal';
  if (order === 'ASC') return thisRank > cursorRank ? 'after' : 'before';
  return thisRank < cursorRank ? 'after' : 'before';
}

/**
 * Build the keyset seek clause for one table given the cursor position.
 *
 * The merged stream is ordered by (fetched_at, table_rank, id). To fetch
 * everything strictly after the cursor row, each table needs a different
 * clause depending on how its rank compares to the cursor row's table:
 *
 *   - equal  (same table, or a single-family / table-less cursor):
 *       the classic row-value seek on (fetched_at, id).
 *   - after  (this table sorts later at a tied second): include the
 *       boundary second too — `fetched_at <cmp>= X`.
 *   - before (this table sorts earlier at a tied second; its rows at X
 *       were already returned): only strictly-later seconds —
 *       `fetched_at <cmp> X`.
 *
 * `baseParamCount` is how many params already precede these in the final
 * params array, so the $N placeholders line up (the unique=1 path
 * prepends sidecar params, so it can't assume a zero base). Returns the
 * clause text plus the params to append, or null when there's no cursor.
 */
function buildCursorSeek(
  faCol: string,
  idCol: string,
  p: DataHistoryParams,
  table: ArchiveTable,
  baseParamCount: number,
): { clause: string; params: unknown[] } | null {
  if (!p.cursor) return null;
  const cmp = p.order === 'DESC' ? '<' : '>';
  // A cursor without a (valid) table — single-family, a legacy 2-part
  // cursor, or a foreign table name — collapses to equal-rank, i.e. the
  // original single-table seek. tableRank() returns -1 for unknowns.
  const ct = p.cursor.table;
  const cursorRank =
    ct != null && tableRank(ct as ArchiveTable) >= 0
      ? tableRank(ct as ArchiveTable)
      : tableRank(table);
  const rel = rankRelation(tableRank(table), cursorRank, p.order);
  const i = baseParamCount;
  if (rel === 'before') {
    return {
      clause: `${faCol} ${cmp} to_timestamp($${i + 1})`,
      params: [p.cursor.fetchedAt],
    };
  }
  if (rel === 'after') {
    return {
      clause: `${faCol} ${cmp}= to_timestamp($${i + 1})`,
      params: [p.cursor.fetchedAt],
    };
  }
  return {
    clause:
      `(${faCol} ${cmp} to_timestamp($${i + 1}) ` +
      `OR (${faCol} = to_timestamp($${i + 2}) AND ${idCol} ${cmp} $${i + 3}))`,
    params: [p.cursor.fetchedAt, p.cursor.fetchedAt, p.cursor.rowId],
  };
}

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
  // (source_timestamp_unix on the sidecar — see migration 020),
  // falling back to last_seen_at when the upstream payload didn't
  // expose a usable timestamp. last_seen_at = "we still see this
  // incident in our polls", which is the natural meaning of "in the
  // last N hours" — a stable waze incident polled all day stays
  // visible in a 24h window even if its latest_fetched_at is days
  // old.
  //
  // This matches /api/data/history/filters' time filter; previously
  // the data path used latest_fetched_at here, which silently
  // dropped ~15% of waze rows from logs.html's total while /filters
  // still reported them.
  //
  // Note: this is the WHERE filter only. The SELECT's effective_ts
  // (and the ORDER BY built from it below) still falls back to
  // latest_fetched_at so the result isn't a stack of identical
  // "now" timestamps for stable incidents.
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
  // Predicates that were previously post-JOIN on parent JSONB now hit
  // sidecar columns directly (migrations 021 + 022). Applying them in
  // the CTE means the JOIN does less work and the planner can use the
  // partial indexes on (source, status / severity / is_active).
  if (p.category && p.category.length > 0) {
    const placeholders = p.category.map(
      (_, i) => `$${sidecarAcc.params.length + i + 1}`,
    );
    sidecarAcc.parts.push(
      p.category.length === 1
        ? `category = ${placeholders[0]}`
        : `category IN (${placeholders.join(',')})`,
    );
    for (const v of p.category) sidecarAcc.params.push(v);
  }
  if (p.subcategory && p.subcategory.length > 0) {
    const placeholders = p.subcategory.map(
      (_, i) => `$${sidecarAcc.params.length + i + 1}`,
    );
    sidecarAcc.parts.push(
      p.subcategory.length === 1
        ? `subcategory = ${placeholders[0]}`
        : `subcategory IN (${placeholders.join(',')})`,
    );
    for (const v of p.subcategory) sidecarAcc.params.push(v);
  }
  if (p.status && p.status.length > 0) {
    const placeholders = p.status.map(
      (_, i) => `$${sidecarAcc.params.length + i + 1}`,
    );
    sidecarAcc.parts.push(
      p.status.length === 1
        ? `status = ${placeholders[0]}`
        : `status IN (${placeholders.join(',')})`,
    );
    for (const v of p.status) sidecarAcc.params.push(v);
  }
  if (p.severity && p.severity.length > 0) {
    const placeholders = p.severity.map(
      (_, i) => `$${sidecarAcc.params.length + i + 1}`,
    );
    sidecarAcc.parts.push(
      p.severity.length === 1
        ? `severity = ${placeholders[0]}`
        : `severity IN (${placeholders.join(',')})`,
    );
    for (const v of p.severity) sidecarAcc.params.push(v);
  }
  if (p.activeOnly) {
    // Sidecar column. NULL means "writer hasn't backfilled yet" — treat
    // as visible (default-true) so we don't hide incidents during the
    // brief 022 backfill window.
    sidecarAcc.parts.push(`(is_active IS NULL OR is_active = true)`);
  }
  if (p.search) {
    const ph = `$${sidecarAcc.params.length + 1}`;
    sidecarAcc.parts.push(`(title ILIKE ${ph} OR location_text ILIKE ${ph})`);
    sidecarAcc.params.push(`%${p.search}%`);
  }
  if (p.title) {
    sidecarAcc.parts.push(`title ILIKE $${sidecarAcc.params.length + 1}`);
    sidecarAcc.params.push(`%${p.title}%`);
  }
  if (p.location) {
    sidecarAcc.parts.push(
      `location_text ILIKE $${sidecarAcc.params.length + 1}`,
    );
    sidecarAcc.params.push(`%${p.location}%`);
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
  // The only filters that stay post-JOIN are those that need fields
  // not (yet) on the sidecar:
  //   - sinceSource / untilSource (data->>'source_timestamp_unix' —
  //     duplicates the indexed sidecar column source_timestamp_unix
  //     but only when the upstream supplies one)
  //   - lat/lng radius (sidecar doesn't carry coords)
  //   - cursor seek (uses fetched_at + id from parent)
  // Renumber placeholders so parent params start AFTER sidecar params.
  const offset = (): number => sidecarAcc.params.length + parentAcc.params.length;

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
  const uniqueSeek = buildCursorSeek('a.fetched_at', 'a.id', p, table, offset());
  if (uniqueSeek) {
    parentAcc.parts.push(uniqueSeek.clause);
    parentAcc.params.push(...uniqueSeek.params);
  }

  const parentWhere =
    parentAcc.parts.length > 0 ? `AND ${parentAcc.parts.join(' AND ')}` : '';
  const dataCol = p.includeData ? 'a.data' : "'{}'::jsonb AS data";

  const allParams = [...sidecarAcc.params, ...parentAcc.params];
  // Fetch ONE row beyond the page so the route's hasMore check
  // (rows.length > offset+limit) can distinguish "exactly a full page"
  // from "a full page with more behind it" and emit next_cursor only
  // when another row provably exists — matching the non-unique path.
  // The cteLimit above carries 2x headroom so the extra row is covered.
  const fetchSize = (p.cursor ? p.limit : p.offset + p.limit) + 1;
  allParams.push(fetchSize);
  const limitPlaceholder = `$${allParams.length}`;

  // ORDER and JOIN dedup notes:
  //   - effective_ts uses source_timestamp_unix with latest_fetched_at
  //     fallback so the chronological feed is meaningful for sources
  //     without their own pub timestamp (after recompute,
  //     latest_fetched_at points at when the incident's data last
  //     changed in our archive — a real-world chronological signal).
  //   - LATERAL ... LIMIT 1 picks exactly one parent row per sidecar
  //     entry. Pre-dedup-writer data has multiple identical (source,
  //     source_id, fetched_at) rows in the parent (overlapping waze
  //     bbox polls within one flush window), and a regular JOIN would
  //     return all of them, surfacing dupes in the unique=1 list.
  //     Highest a.id wins arbitrarily but deterministically.
  const sql = `
    WITH top_keys AS (
      SELECT source, source_id, latest_fetched_at, last_seen_at,
             source_timestamp_unix,
             COALESCE(source_timestamp_unix,
                      EXTRACT(EPOCH FROM latest_fetched_at)::bigint) AS effective_ts
      FROM ${table}_latest
      ${sidecarWhere}
      ORDER BY effective_ts ${p.order}
      LIMIT ${cteLimit}
    )
    SELECT a.id, k.source, k.source_id,
           extract(epoch FROM a.fetched_at)::bigint AS fetched_at_epoch,
           a.fetched_at,
           extract(epoch FROM k.last_seen_at)::bigint AS last_seen_at_epoch,
           k.last_seen_at,
           k.source_timestamp_unix,
           a.lat, a.lng, a.category, a.subcategory,
           ${dataCol}
    FROM top_keys k
    CROSS JOIN LATERAL (
      SELECT id, fetched_at, lat, lng, category, subcategory, data
        FROM ${table}
       WHERE source = k.source
         AND source_id = k.source_id
         AND fetched_at = k.latest_fetched_at
       ORDER BY id DESC
       LIMIT 1
    ) a
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
  // For multi-family cursors the clause is rank-aware so a per-table id
  // is never compared against another table's id (see buildCursorSeek).
  const seek = buildCursorSeek('fetched_at', 'id', p, table, acc.params.length);
  if (seek) {
    acc.parts.push(seek.clause);
    acc.params.push(...seek.params);
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
  // Fetch ONE row beyond the page so the route can tell "exactly a full
  // page" (no more) from "a full page with more behind it" and only emit
  // a next_cursor when another row provably exists. (The unique=1 path
  // below has its own cteLimit headroom, so this only affects the
  // non-unique SELECT.)
  const fetchSize = (p.cursor ? p.limit : p.offset + p.limit) + 1;
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
    //   - everything else (lat/lng radius, JSONB sinceSource/
    //     untilSource, cursor seek) — applied post-JOIN on parent
    //     columns. Sidecar-resident dims (category, subcategory,
    //     status, severity, title, location_text) hit the sidecar.
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
  // sinceSource / untilSource — match buildSqlForTable. Previously
  // omitted here, causing `total` to over-report when the caller
  // narrowed the data query with these JSONB time filters.
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
  // activeOnly — previously omitted from the count, which left
  // `total` inconsistent with the actual `records` returned for
  // callers passing ?active_only=1.
  if (p.activeOnly) {
    acc.parts.push(`(data->>'is_active' IN ('1','true','True'))`);
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
  // SELECT shape: a single bigint `total`. The 50k cap rides through
  // the doubly-nested LIMIT pattern so the index walk stops at 50k
  // before the outer COUNT runs.
  const COUNT_CAP = 50_000;

  if (p.unique) {
    // Sidecar-driven unique count (migration 017). Sidecar carries
    // (source, source_id, latest_fetched_at) — exactly the columns
    // we filter on for time + source. Anything else (category,
    // JSONB fields, lat/lng radius) only the parent has.
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
    // publish timestamp with last_seen_at fallback. last_seen_at
    // mirrors the /filters facet query (filterCache.ts) so the per-
    // source counts there sum to this `total`.
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
    // Apply the same post-JOIN-filter set the data query (buildUniqueQuery)
    // now applies on the sidecar — so `total` honours them instead of
    // always reporting the unfiltered roll-up.
    if (p.category && p.category.length > 0) {
      const placeholders = p.category.map(
        (_, i) => `$${sidecarAcc.params.length + i + 1}`,
      );
      sidecarAcc.parts.push(
        p.category.length === 1
          ? `category = ${placeholders[0]}`
          : `category IN (${placeholders.join(',')})`,
      );
      for (const v of p.category) sidecarAcc.params.push(v);
    }
    if (p.subcategory && p.subcategory.length > 0) {
      const placeholders = p.subcategory.map(
        (_, i) => `$${sidecarAcc.params.length + i + 1}`,
      );
      sidecarAcc.parts.push(
        p.subcategory.length === 1
          ? `subcategory = ${placeholders[0]}`
          : `subcategory IN (${placeholders.join(',')})`,
      );
      for (const v of p.subcategory) sidecarAcc.params.push(v);
    }
    if (p.status && p.status.length > 0) {
      const placeholders = p.status.map(
        (_, i) => `$${sidecarAcc.params.length + i + 1}`,
      );
      sidecarAcc.parts.push(
        p.status.length === 1
          ? `status = ${placeholders[0]}`
          : `status IN (${placeholders.join(',')})`,
      );
      for (const v of p.status) sidecarAcc.params.push(v);
    }
    if (p.severity && p.severity.length > 0) {
      const placeholders = p.severity.map(
        (_, i) => `$${sidecarAcc.params.length + i + 1}`,
      );
      sidecarAcc.parts.push(
        p.severity.length === 1
          ? `severity = ${placeholders[0]}`
          : `severity IN (${placeholders.join(',')})`,
      );
      for (const v of p.severity) sidecarAcc.params.push(v);
    }
    if (p.activeOnly) {
      sidecarAcc.parts.push(`(is_active IS NULL OR is_active = true)`);
    }
    if (p.search) {
      const ph = `$${sidecarAcc.params.length + 1}`;
      sidecarAcc.parts.push(`(title ILIKE ${ph} OR location_text ILIKE ${ph})`);
      sidecarAcc.params.push(`%${p.search}%`);
    }
    if (p.title) {
      sidecarAcc.parts.push(`title ILIKE $${sidecarAcc.params.length + 1}`);
      sidecarAcc.params.push(`%${p.title}%`);
    }
    if (p.location) {
      sidecarAcc.parts.push(
        `location_text ILIKE $${sidecarAcc.params.length + 1}`,
      );
      sidecarAcc.params.push(`%${p.location}%`);
    }
    const sidecarWhere =
      sidecarAcc.parts.length > 0 ? `WHERE ${sidecarAcc.parts.join(' AND ')}` : '';
    // No COUNT_CAP for the sidecar path — `_latest` carries one row
    // per (source, source_id), bounded by the number of distinct
    // incidents the upstream feed currently knows about (tens of
    // thousands, not the millions in the parent partitions). Counting
    // it directly is sub-second on an indexed table.
    const sql = `SELECT COUNT(*)::bigint AS total FROM ${table}_latest ${sidecarWhere}`;
    return { sql, params: sidecarAcc.params, table };
  }

  // Unique=0 parent count: bounded scan via idx_archive_*_ts so
  // archive_waze (millions of rows across monthly partitions) can't
  // burn the 60s statement_timeout. The frontend's page-jumper only
  // ever navigates the first ~5000 pages anyway.
  const sql = `SELECT COUNT(*)::bigint AS total
       FROM (
         SELECT 1
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
  /** Which archive table this row came from. Tagged by the route after
   *  fetch so the multi-family merge can order by table rank and the
   *  cursor can record the boundary row's table. Not emitted to clients. */
  _family?: ArchiveTable;
}

/**
 * Sort the merged rows from multiple tables and apply offset+limit in
 * Node. Sort key is (fetched_at_epoch, table_rank, id), direction-aware.
 *
 * The table_rank tiebreaker is essential: `id` is a per-table BIGSERIAL,
 * so at a fetched_at tie two families' ids are unrelated. Ordering by
 * rank first makes the cross-table order deterministic and — critically —
 * reproducible from the cursor's table segment, which is what keeps
 * forward pagination from skipping or repeating rows. Rows missing
 * `_family` fall back to rank 0 (only happens if a caller hand-builds
 * rows; the route always tags them).
 */
export function mergeAndPaginate(
  rows: ArchiveQueryRow[],
  plan: QueryPlan,
): ArchiveQueryRow[] {
  const dir = plan.order === 'DESC' ? -1 : 1;
  const rankOf = (r: ArchiveQueryRow): number =>
    r._family ? tableRank(r._family) : 0;
  rows.sort((a, b) => {
    if (a.fetched_at_epoch !== b.fetched_at_epoch) {
      return (a.fetched_at_epoch - b.fetched_at_epoch) * dir;
    }
    const ra = rankOf(a);
    const rb = rankOf(b);
    if (ra !== rb) return (ra - rb) * dir;
    return (a.id - b.id) * dir;
  });
  return rows.slice(plan.effectiveOffset, plan.effectiveOffset + plan.limit);
}
