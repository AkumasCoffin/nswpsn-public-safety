/**
 * Pure SQL builder for the staff Data tab's node-event analytics (radio + pager).
 *
 * Pure logic — no database, no Hono, no I/O. It owns the predicate emitters, the
 * window map, the rollup/live seam emitters, and the one definition each of a
 * Reception and a Logical call, so every route expresses those invariants the
 * same way. The route resolves the Rollup edge (a database read) and passes it in
 * as `edge`; the builder stays pure and deterministic, which makes it testable by
 * asserting the emitted SQL string + params with no database — the same discipline
 * as services/dataHistoryQuery.ts.
 *
 * Vocabulary (see GLOSSARY.md): a **Logical call** is one transmission; a
 * **Reception** is one observed (logical call, node, site[, talkgroup]) tuple,
 * counted not multiplied; the **Rollup edge** is the seam between completed hours
 * (the hourly rollups) and the in-progress hour (the detail table); a **Window**
 * is the look-back span (24h / 7d / 30d / all).
 */

export type Windows = '24h' | '7d' | '30d' | 'all';

/** The look-back interval for each non-`all` Window, as a Postgres interval literal. */
export const WINDOW_INTERVAL: Record<Exclude<Windows, 'all'>, string> = {
  '24h': '24 hours',
  '7d': '7 days',
  '30d': '30 days',
};

/**
 * A CALL talkgroup on this network is 5 digits, so the floor is 10000 and the
 * ceiling is the top of the band the agencies actually allocate — NOT 65535, the
 * 16-bit P25 maximum (everything above TG_MAX is noise, 65535 being the all-ones
 * null talkgroup). Ingest stores every event's `target` in the talkgroup column
 * regardless of kind, so unit/data calls drop 7-digit RADIO IDs there; this gate
 * keeps them out of talkgroup lists/counts. A configured talkgroup outside the
 * band is still honoured — see the route's tgValidConfigured. Prefixable ('' or
 * 'e.'); `TG_VALID` is the bare form.
 */
export const TG_MIN = 10000;
export const TG_MAX = 30500;
export const tgValid = (prefix = ''): string =>
  `${prefix}talkgroup BETWEEN ${TG_MIN} AND ${TG_MAX}`;
export const TG_VALID = tgValid();

/**
 * "This is a real radio id." The RID half of tgValid: a RID on this network is 7
 * digits (6 where a leading zero was stripped), so anything outside 100000..9999999
 * is decode noise, not a radio. Counts of calls stay unfiltered; this gates who
 * appears in a list of radios. Prefixable; `RID_VALID` is the bare form.
 */
export const ridValid = (prefix = ''): string =>
  `${prefix}source_unit BETWEEN 100000 AND 9999999`;
export const RID_VALID = ridValid();

/**
 * "This row is a talkgroup VOICE call." node_radio_events stores every P25 activity
 * event — voice AND data/signaling — so only CALL_GROUP% and CALL_PATCH_GROUP%
 * events are real talkgroup voice calls. CALL_PATCH_GROUP is spelled out separately
 * because it does NOT start with "CALL_GROUP"; a single prefix match silently drops
 * every patched call. Matched directly (not through upper()): ingest uppercases
 * event_type, and wrapping the column made the predicate opaque to the planner.
 * Prefixable; `CALL_GROUP` is the bare form.
 */
export const callGroup = (prefix = ''): string =>
  `(${prefix}event_type LIKE 'CALL_GROUP%'` +
  ` OR ${prefix}event_type LIKE 'CALL_PATCH_GROUP%')`;
export const CALL_GROUP = callGroup();

/**
 * The two halves of a Rollup-edge union, split on the resolved seam.
 *
 * Completed hours come from an hourly rollup (`hour < seam`, windowed back by the
 * `$1::interval` the caller binds); the in-progress hour comes from the detail
 * table (`received_at >= seam`). Both take the seam as a SQL literal — see
 * rollupEdgeSql — so the planner can estimate the live half instead of guessing a
 * third of the table. `rollupHours` assumes `$1` is the window interval parameter.
 */
export const rollupHours = (seam: string): string =>
  `hour >= date_trunc('hour', now()) - $1::interval AND hour < ${seam}`;
export const liveHour = (seam: string): string => `received_at >= ${seam}`;

/**
 * The Rollup edge as SQL. Injection-safe by construction: the text is
 * Date#toISOString() output — a fixed ISO-8601 format — of a Date built from our
 * own database's answer (resolveRollupEdge, in the route layer), never from request
 * input.
 */
export function rollupEdgeSql(edge: Date): string {
  return `'${edge.toISOString()}'::timestamptz`;
}

/**
 * The encrypted / no-tgid outcome predicates over a home-talkgroup column, as SQL
 * fragments. `encParam` / `progParam` are the $N placeholders (or int[] literals)
 * holding the encrypted and programmed talkgroup id arrays. An EMPTY programmed
 * list means "rdio unreachable", not "nothing programmed", so cardinality() = 0
 * skips the no-tgid test rather than condemning the whole network.
 */
export function outcomeSql(
  homeCol: string,
  encParam: string,
  progParam: string,
): { isEnc: string; isNoTgid: string } {
  const isEnc = `${homeCol} = ANY(${encParam}::int[])`;
  const isNoTgid =
    `cardinality(${progParam}::int[]) > 0 AND ${homeCol} <> ALL(${progParam}::int[])`;
  return { isEnc, isNoTgid };
}

/**
 * The one definition of a **Reception** count: distinct (logical call, node, site)
 * tuples. The same logical call heard by three nodes is three receptions of one
 * logical call — counted, never multiplied. COALESCE(-1) folds a null site/rfss
 * into a single sentinel so an unknown-site reception is still one reception.
 * Prefixable ('' or 'e.') to match queries that alias the events table.
 *
 * Consumed by the overview/lists reads (batch 2 of the node-data deepening); kept
 * here as the single source of truth for the tuple so no route re-inlines it.
 */
export const receptionCount = (prefix = ''): string =>
  `COUNT(DISTINCT (${prefix}logical_call_id, ${prefix}node_id,` +
  ` COALESCE(${prefix}site_rfss, -1), COALESCE(${prefix}site_id, -1)))::int`;

/**
 * The one definition of a **Logical call** count: distinct transmissions,
 * regardless of how many nodes received each. Prefixable; `LOGICAL_COUNT` is the
 * bare form.
 */
export const logicalCount = (prefix = ''): string =>
  `COUNT(DISTINCT ${prefix}logical_call_id)::int`;
export const LOGICAL_COUNT = logicalCount();

/**
 * The WHERE-clause accumulator idiom, copied from services/dataHistoryQuery.ts
 * (ConditionAcc / pushIn) so the node-event builder stops re-declaring a local
 * `conds[]` + `add()` per route. Copied, not shared: lifting one shared helper is
 * deferred until both builders are proven (two real consumers then justify the
 * seam). `$N` placeholders are 1-based on `params.length`.
 */
export interface ConditionAcc {
  parts: string[];
  params: unknown[];
}

export function newAcc(initialParams: unknown[] = []): ConditionAcc {
  return { parts: [], params: [...initialParams] };
}

/** Append `column = $N`, binding one value. */
export function pushEq(acc: ConditionAcc, column: string, value: unknown): void {
  acc.params.push(value);
  acc.parts.push(`${column} = $${acc.params.length}`);
}

/** Append `column = $N` for a single value, or `column IN ($a,$b,...)` for many. */
export function pushIn(acc: ConditionAcc, column: string, values: unknown[]): void {
  if (values.length === 1) {
    acc.params.push(values[0]);
    acc.parts.push(`${column} = $${acc.params.length}`);
    return;
  }
  const placeholders = values.map((_, i) => `$${acc.params.length + i + 1}`);
  acc.parts.push(`${column} IN (${placeholders.join(',')})`);
  for (const v of values) acc.params.push(v);
}

/** Render the accumulated conditions as a `WHERE ...` clause (empty string when none). */
export function whereClause(acc: ConditionAcc): string {
  return acc.parts.length > 0 ? `WHERE ${acc.parts.join(' AND ')}` : '';
}

// ---------------------------------------------------------------------------
// Talkgroups route (pilot) — the two query strategies as pure builders.
//
// The route resolves the impure inputs first (the Rollup edge, the encrypted
// talkgroup id set, the agency's talkgroup id set, and the configured-valid
// predicate string — each a database/catalog read) and passes them in; the
// builder then emits SQL + params with no I/O, so a golden test can pin the
// emitted SQL and params with no database. Two strategies, one input contract:
//
//   • buildTalkgroupsRollup — fleet-wide (no node): the hourly rollup below the
//     Rollup edge UNION the in-progress hour from detail, grouped per talkgroup.
//   • buildTalkgroupsRaw    — node-scoped: a two-level grouped scan of detail
//     (the rollups carry no node, and the (node, time) index makes this cheap).
//
// The SQL is semantically identical to what the route emitted before the lift;
// only whitespace is normalised. Params are byte-identical.
// ---------------------------------------------------------------------------

/** One sort key → the grouped expression the talkgroups page orders on. */
export type TalkgroupsSort = 'calls' | 'lastSeen' | 'talkgroup' | 'enc' | 'logical';
export const TALKGROUPS_ORDER_COL: Record<TalkgroupsSort, string> = {
  calls: 'calls',
  lastSeen: 'last_seen',
  talkgroup: 'talkgroup',
  enc: 'enc',
  logical: 'logical',
};

/**
 * The resolved inputs both talkgroups strategies build from. Everything here is
 * already resolved — no promises, no pool. `encTgs` is the encrypted-talkgroup
 * id set (consulted only when `encHide`); `agencyIds` is the agency filter's id
 * set, or `null` for no agency filter (an empty array means "an agency with no
 * talkgroups", which matches nothing — deliberately, not the same as null);
 * `tgConfiguredSql` is the catalog-backed configured-valid predicate.
 */
export interface TalkgroupsQuery {
  windowInterval: string;
  system: number | null;
  rfss: number | null;
  site: number | null;
  nodeId: string | null;
  qRaw: string;
  encHide: boolean;
  encTgs: number[];
  agencyIds: number[] | null;
  tgConfiguredSql: string;
  orderCol: string;
  dir: 'ASC' | 'DESC';
  limit: number;
  offset: number;
}

/** A count query and a page query, each with its own params, ready for the pool. */
export interface BuiltCountPage {
  countSql: string;
  countParams: unknown[];
  pageSql: string;
  pageParams: unknown[];
}

/**
 * Node-scoped talkgroups: a two-level grouped scan of the detail table. The
 * inner group folds each logical call's receptions together so the outer
 * `logical` is a plain COUNT(*) of groups, never a COUNT(DISTINCT) that would
 * sort the whole window. `enc=hide` becomes a per-group HAVING, so the count
 * must group first and count the survivors.
 */
export function buildTalkgroupsRaw(q: TalkgroupsQuery): BuiltCountPage {
  const params: unknown[] = [q.windowInterval];
  // CALL_GROUP restricts to talkgroup voice calls; TG_VALID drops NULL/out-of-
  // range ids; the configured-valid predicate re-admits configured exceptions.
  const conds = ['received_at >= now() - $1::interval', CALL_GROUP, TG_VALID];
  if (q.system !== null) {
    params.push(q.system);
    conds.push(`system = $${params.length}`);
  }
  if (q.rfss !== null) {
    params.push(q.rfss);
    conds.push(`site_rfss = $${params.length}`);
    params.push(q.site);
    conds.push(`site_id = $${params.length}`);
  }
  if (q.nodeId !== null) {
    params.push(q.nodeId);
    conds.push(`node_id = $${params.length}`);
  }
  if (q.qRaw) {
    params.push(`${q.qRaw}%`);
    conds.push(`talkgroup::text LIKE $${params.length}`);
  }
  if (q.encHide && q.encTgs.length) {
    params.push(q.encTgs);
    conds.push(`talkgroup <> ALL($${params.length}::int[])`);
  }
  if (q.agencyIds !== null) {
    params.push(q.agencyIds);
    conds.push(`talkgroup = ANY($${params.length}::int[])`);
  }
  conds.push(q.tgConfiguredSql);
  const where = conds.join(' AND ');
  // Applied to the OUTER aggregate, where a reception's counts are already
  // folded per logical call. The count's HAVING is per-group on the flat form.
  const having = q.encHide ? 'HAVING SUM(g.enc) < SUM(g.calls)' : '';
  const countHaving = q.encHide
    ? 'HAVING COUNT(*) FILTER (WHERE encrypted) < COUNT(*)'
    : '';
  const limIdx = params.length + 1;
  const offIdx = params.length + 2;
  const pageParams = [...params, q.limit, q.offset];

  // Grouped subquery, NOT COUNT(DISTINCT (row)): a composite DISTINCT sorts
  // every row in the window; the group-then-count hash-aggregates.
  const countSql = `SELECT COUNT(*)::int AS n FROM (
  SELECT 1 FROM node_radio_events WHERE ${where}
   GROUP BY wacn, system, talkgroup ${countHaving}
) g`;

  const pageSql = `SELECT wacn, system, talkgroup,
       SUM(g.calls)::int AS calls,
       COUNT(*)::int AS logical,
       SUM(g.enc)::int AS enc,
       MAX(g.last_seen) AS last_seen
  FROM (
    SELECT wacn, system, talkgroup, logical_call_id,
           COUNT(*)::int AS calls,
           (COUNT(*) FILTER (WHERE encrypted))::int AS enc,
           MAX(received_at) AS last_seen
      FROM node_radio_events
     WHERE ${where}
     GROUP BY wacn, system, talkgroup, logical_call_id
  ) g
 GROUP BY wacn, system, talkgroup ${having}
 ORDER BY ${q.orderCol} ${q.dir}, talkgroup ASC
 LIMIT $${limIdx} OFFSET $${offIdx}`;

  return { countSql, countParams: params, pageSql, pageParams };
}

/**
 * Fleet-wide talkgroups across the Rollup edge: completed hours from
 * `node_radio_hourly_sys` (windowed back by `$1::interval`, stopping at the
 * seam) UNION the in-progress hour counted from detail, then grouped per
 * (system, talkgroup). `seam` is the resolved Rollup edge as a SQL literal
 * (rollupEdgeSql). Row filters (system, site) apply to both halves; talkgroup
 * filters (range, prefix, encrypted set, agency, configured) apply once on the
 * union. The live half counts encrypted LOGICAL calls to match the rollup's
 * `encrypted_calls`, so both halves are in one unit.
 */
export function buildTalkgroupsRollup(q: TalkgroupsQuery, seam: string): BuiltCountPage {
  const rp: unknown[] = [q.windowInterval];
  const rowConds: string[] = [];
  if (q.system !== null) {
    rp.push(q.system);
    rowConds.push(`system = $${rp.length}`);
  }
  if (q.rfss !== null) {
    rp.push(q.rfss);
    rowConds.push(`site_rfss = $${rp.length}`);
    rp.push(q.site);
    rowConds.push(`site_id = $${rp.length}`);
  }
  const tgConds: string[] = [TG_VALID];
  if (q.qRaw) {
    rp.push(`${q.qRaw}%`);
    tgConds.push(`talkgroup::text LIKE $${rp.length}`);
  }
  if (q.encHide && q.encTgs.length) {
    rp.push(q.encTgs);
    tgConds.push(`talkgroup <> ALL($${rp.length}::int[])`);
  }
  if (q.agencyIds !== null) {
    rp.push(q.agencyIds);
    tgConds.push(`talkgroup = ANY($${rp.length}::int[])`);
  }
  tgConds.push(q.tgConfiguredSql);

  const rowWhere = rowConds.length ? ' AND ' + rowConds.join(' AND ') : '';
  const liveWhere = [liveHour(seam), CALL_GROUP, 'talkgroup IS NOT NULL', ...rowConds].join(' AND ');
  const tgWhere = tgConds.join(' AND ');
  const rollupHaving = q.encHide ? 'HAVING SUM(enc) < SUM(logical)' : '';

  const cte = `WITH r AS (
  SELECT system, talkgroup, calls,
         logical_calls_tg AS logical, encrypted_calls AS enc, hour AS last_seen
    FROM node_radio_hourly_sys
   WHERE ${rollupHours(seam)} AND talkgroup <> 0${rowWhere}
  UNION ALL
  SELECT COALESCE(system, 0), talkgroup,
         COUNT(*)::int,
         COUNT(DISTINCT logical_call_id)::int,
         (COUNT(DISTINCT logical_call_id) FILTER (WHERE encrypted))::int,
         MAX(received_at)
    FROM node_radio_events
   WHERE ${liveWhere}
   GROUP BY 1, 2
)`;

  const countSql = `${cte}
SELECT COUNT(*)::int AS n FROM (
  SELECT 1 FROM r WHERE ${tgWhere}
   GROUP BY system, talkgroup ${rollupHaving}
) g`;

  const pageSql = `${cte}
SELECT NULL::int AS wacn, NULLIF(system, 0) AS system, talkgroup,
       SUM(calls)::int AS calls,
       SUM(logical)::int AS logical,
       SUM(enc)::int AS enc,
       MAX(last_seen) AS last_seen
  FROM r
 WHERE ${tgWhere}
 GROUP BY system, talkgroup ${rollupHaving}
 ORDER BY ${q.orderCol} ${q.dir}, talkgroup ASC
 LIMIT $${rp.length + 1} OFFSET $${rp.length + 2}`;

  return { countSql, countParams: rp, pageSql, pageParams: [...rp, q.limit, q.offset] };
}

/**
 * The rollup-plus-live union for a drill-down's rows, at the (hour, node,
 * system, site, talkgroup, radio) grain. Completed hours come from
 * `node_radio_hourly_heard` below the seam; the in-progress hour is folded from
 * detail, one reception per (logical call, node, site, talkgroup). `where` is
 * applied to both halves (and again to the folded live rows). This is the one
 * emitter every fleet-wide reception read on the staff Data tab builds on.
 */
export function heardWindowSql(seam: string, where: string): string {
  return `
    SELECT hour, node_id, system, site_rfss, site_id, talkgroup, source_unit,
           receptions, last_heard
      FROM node_radio_hourly_heard
     WHERE ${rollupHours(seam)} AND ${where}
    UNION ALL
    SELECT hour, node_id, system, site_rfss, site_id, talkgroup, source_unit,
           COUNT(*)::int, MAX(last_heard)
      FROM (
        SELECT date_trunc('hour', MIN(received_at)) AS hour, node_id,
               COALESCE(system, 0) AS system,
               COALESCE(site_rfss, -1) AS site_rfss,
               COALESCE(site_id, -1) AS site_id,
               COALESCE(talkgroup, 0) AS talkgroup,
               COALESCE((array_agg(source_unit ORDER BY id)
                           FILTER (WHERE source_unit IS NOT NULL))[1], 0) AS source_unit,
               MAX(received_at) AS last_heard
          FROM node_radio_events
         WHERE ${liveHour(seam)} AND ${CALL_GROUP}
         GROUP BY logical_call_id, node_id, 3, 4, 5, 6
      ) rx
     WHERE ${where}
     GROUP BY 1, 2, 3, 4, 5, 6, 7`;
}

// ---------------------------------------------------------------------------
// Radios route (pilot) — the twin of the talkgroups builders, keyed on radio.
//
// Same pure-in/impure-out boundary: the route resolves the Rollup edge and the
// agency/alias unit-id set first, then the builder emits SQL + params with no
// I/O. Two strategies:
//
//   • buildRadiosRollup — fleet-wide (no node): heardWindowSql below the edge
//     UNION the live hour, collapsed one row per radio, with the radio's
//     LIFETIME over-the-air alias joined from node_radio_aliases.
//   • buildRadiosRaw    — node-scoped: a grouped scan of detail, the alias
//     taken as the most-recent non-null source_alias in the window.
// ---------------------------------------------------------------------------

export type RadiosSort = 'calls' | 'lastSeen' | 'radio' | 'ota';

/** Raw path: the grouped output column each sort key orders on. */
const RADIOS_RAW_ORDER: Record<RadiosSort, string> = {
  calls: 'calls',
  lastSeen: 'last_seen',
  radio: 'radio',
  ota: 'alias',
};
/** Rollup path: the same, over the aliased subqueries `g` (traffic) and `a` (alias). */
const RADIOS_ROLLUP_ORDER: Record<RadiosSort, string> = {
  calls: 'g.calls',
  lastSeen: 'g.last_seen',
  radio: 'g.radio',
  ota: 'a.alias',
};

/**
 * The resolved inputs both radios strategies build from. `unitIds` is the
 * agency/has-alias filter's unit-id set, or `null` for no such filter (an empty
 * array matches nothing, deliberately). `namedOnly` is the named=1 / has=ota
 * gate: only radios that have transmitted an over-the-air alias — a HAVING, not
 * a WHERE, because the alias is an aggregate over the radio's calls.
 */
export interface RadiosQuery {
  windowInterval: string;
  system: number | null;
  rfss: number | null;
  site: number | null;
  nodeId: string | null;
  qRaw: string;
  unitIds: number[] | null;
  namedOnly: boolean;
  sort: RadiosSort;
  dir: 'ASC' | 'DESC';
  limit: number;
  offset: number;
}

/**
 * Node-scoped radios: a grouped scan of the detail table, one row per
 * (wacn, system, source_unit). Without named-only the count is a cheap
 * COUNT(DISTINCT (row)); with it the count must group first and count the
 * survivors, because named-only is a per-group HAVING.
 */
export function buildRadiosRaw(q: RadiosQuery): BuiltCountPage {
  const params: unknown[] = [q.windowInterval];
  // CALL_GROUP: a radio = a unit that made talkgroup voice calls, not one merely
  // seen in signalling. RID_VALID, not just NOT NULL: this IS the Radios list,
  // so a corrupted decode here would become a row claiming to be a radio.
  const conds = ['received_at >= now() - $1::interval', CALL_GROUP, RID_VALID];
  if (q.system !== null) {
    params.push(q.system);
    conds.push(`system = $${params.length}`);
  }
  if (q.rfss !== null) {
    params.push(q.rfss);
    conds.push(`site_rfss = $${params.length}`);
    params.push(q.site);
    conds.push(`site_id = $${params.length}`);
  }
  if (q.nodeId !== null) {
    params.push(q.nodeId);
    conds.push(`node_id = $${params.length}`);
  }
  if (q.qRaw) {
    params.push(`${q.qRaw}%`);
    conds.push(`source_unit::text LIKE $${params.length}`);
  }
  if (q.unitIds !== null) {
    params.push(q.unitIds);
    conds.push(`source_unit = ANY($${params.length}::bigint[])`);
  }
  const where = conds.join(' AND ');
  const orderCol = RADIOS_RAW_ORDER[q.sort];
  const orderNulls = q.sort === 'ota' ? ' NULLS LAST' : '';
  const having = q.namedOnly ? 'HAVING COUNT(source_alias) > 0' : '';
  const limIdx = params.length + 1;
  const offIdx = params.length + 2;
  const pageParams = [...params, q.limit, q.offset];

  const countSql = q.namedOnly
    ? `SELECT COUNT(*)::int AS n FROM (
  SELECT 1 FROM node_radio_events WHERE ${where}
   GROUP BY wacn, system, source_unit ${having}) t`
    : `SELECT COUNT(DISTINCT (wacn, system, source_unit))::int AS n
  FROM node_radio_events WHERE ${where}`;

  // alias: the most-recent non-null OTA/talker alias for this radio
  // (source_alias from vce); null when none was ever captured.
  const pageSql = `SELECT wacn, system, source_unit AS radio,
       (array_agg(source_alias ORDER BY received_at DESC)
          FILTER (WHERE source_alias IS NOT NULL AND source_alias <> source_unit::text))[1] AS alias,
       COUNT(*)::int AS calls,
       MAX(received_at) AS last_seen
  FROM node_radio_events
 WHERE ${where}
 GROUP BY wacn, system, source_unit
 ${having}
 ORDER BY ${orderCol} ${q.dir}${orderNulls}, radio ASC
 LIMIT $${limIdx} OFFSET $${offIdx}`;

  return { countSql, countParams: params, pageSql, pageParams };
}

/**
 * Fleet-wide radios across the Rollup edge. `calls` is receptions (every rollup
 * read counts them); the OTA used to sort and to gate named-only is the radio's
 * LIFETIME alias, joined from node_radio_aliases, the one the row has always
 * displayed — not one sent in-window. `seam` is the resolved edge as a literal.
 */
export function buildRadiosRollup(q: RadiosQuery, seam: string): BuiltCountPage {
  const rp: unknown[] = [q.windowInterval];
  const rowConds: string[] = [];
  if (q.system !== null) {
    rp.push(q.system);
    rowConds.push(`system = $${rp.length}`);
  }
  if (q.rfss !== null) {
    rp.push(q.rfss);
    rowConds.push(`site_rfss = $${rp.length}`);
    rp.push(q.site);
    rowConds.push(`site_id = $${rp.length}`);
  }
  const unitConds: string[] = [RID_VALID];
  if (q.qRaw) {
    rp.push(`${q.qRaw}%`);
    unitConds.push(`source_unit::text LIKE $${rp.length}`);
  }
  if (q.unitIds !== null) {
    rp.push(q.unitIds);
    unitConds.push(`source_unit = ANY($${rp.length}::bigint[])`);
  }
  const h = heardWindowSql(seam, [...rowConds, ...unitConds].join(' AND '));
  // One row per radio, lifetime OTA joined on; echoed ids dropped first.
  const grouped = `(SELECT system, source_unit AS radio,
        SUM(receptions)::int AS calls, MAX(last_heard) AS last_seen
   FROM h GROUP BY system, source_unit) g
   LEFT JOIN (SELECT radio_id, (array_agg(alias ORDER BY last_seen DESC))[1] AS alias
                FROM node_radio_aliases
               WHERE alias <> radio_id::text
               GROUP BY radio_id) a ON a.radio_id = g.radio${
    q.namedOnly ? ' WHERE a.alias IS NOT NULL' : ''}`;
  const rollupOrder = RADIOS_ROLLUP_ORDER[q.sort];
  const orderNulls = q.sort === 'ota' ? ' NULLS LAST' : '';

  const countSql = `WITH h AS (${h}) SELECT COUNT(*)::int AS n FROM ${grouped}`;
  const pageSql = `WITH h AS (${h})
SELECT NULL::int AS wacn, NULLIF(g.system, 0) AS system, g.radio, a.alias,
       g.calls, g.last_seen
  FROM ${grouped}
 ORDER BY ${rollupOrder} ${q.dir}${orderNulls}, g.radio ASC
 LIMIT $${rp.length + 1} OFFSET $${rp.length + 2}`;

  return { countSql, countParams: rp, pageSql, pageParams: [...rp, q.limit, q.offset] };
}
