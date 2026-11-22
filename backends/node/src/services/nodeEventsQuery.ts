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
