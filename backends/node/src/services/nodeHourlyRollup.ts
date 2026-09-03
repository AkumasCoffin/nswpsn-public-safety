/**
 * Hourly radio rollups, DERIVED from the detail table (migration 080).
 *
 * WHY DERIVED AND NOT MAINTAINED
 * These tables used to be bumped once per event inside the ingest transaction.
 * That cannot be right: the truth changes after ingest. mergeAutomaticPatch
 * folds several logical calls into one seconds later, and a late reception
 * joins a group whose first member was already counted — so an incrementally
 * maintained counter drifts from the detail table by construction. It also
 * cost two upserts per event on the hot path, for numbers nothing read.
 *
 * Recomputing a COMPLETED hour has none of those problems: by then every merge
 * and every straggler has settled, and the answer is whatever the detail table
 * says. DELETE-then-INSERT per hour makes it idempotent, so a re-run is a
 * no-op rather than a doubling — the property migration 079 could not have.
 *
 * DEFINITIONS come from the read path (api/node-data.ts radioTotalsSql) so the
 * rollup and a live query cannot disagree:
 *   - a CALL is a CALL_GROUP% / CALL_PATCH_GROUP% event; data and signalling
 *     are not calls;
 *   - a RECEPTION is one (logical call, node, site), not one stored row — vce
 *     emits a GRANT and a CALL for the same receipt and repeats the grant on a
 *     re-grant;
 *   - a call is encrypted/recorded if ANY of its receptions was.
 *
 * The first pass over the 30 days of detail IS the backfill; there is no
 * separate migration step.
 *
 * Shape mirrors nodeEventsPruner.ts / statsArchiver.ts: setInterval with a
 * re-entrancy guard, start/stop exported, wired in src/index.ts.
 */
import { getWriterPool } from '../db/pool.js';
import { log } from '../lib/log.js';

const ROLLUP_INTERVAL_MS = 60 * 60_000; // hourly

/**
 * How far back a cold start will reach. Matches the detail table's retention —
 * there is nothing older to derive from, and asking for more just scans empty
 * range.
 */
const MAX_BACKFILL_HOURS = 24 * 31;

/**
 * Hours per statement. One hour is ~6k detail rows here; batching keeps each
 * transaction short so a cold-start backfill of ~750 hours cannot hold a long
 * write lock or bloat a single transaction.
 */
const BATCH_HOURS = 24;

/**
 * How far back each pass re-derives, on top of the hours it has never seen.
 *
 * Covers the lag between a call and its audio: markRecorded lands after the
 * call ends and can cross an hour boundary, so the hour just closed is not
 * final when it closes. Six hours is far more slack than that needs and costs
 * six DELETE+INSERT pairs an hour against a table of tens of rows per hour.
 */
const REDERIVE_HOURS = 6;

/** A call, and only a call. Kept textually identical to node-data.ts's
 *  callGroup() — if one changes the other must. */
const CALL_GROUP =
  "(event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%')";


/**
 * The hour a CALL belongs to: the hour of its earliest event.
 *
 * Every rollup buckets on this, so a call spanning an hour boundary lands in
 * one bucket rather than being counted in both, and summing any set of hours
 * is exact. The per-node and per-site tables used to bucket each EVENT into
 * its own hour, which ran about one call adrift per boundary -- invisible
 * while nothing read them, and a permanent floor on how closely a window sum
 * could match the detail query it replaces.
 *
 * Ordered by id, not by received_at, matching radioReceptionsSql: the same
 * rule that picks a call's home talkgroup should pick its hour, and id is the
 * tiebreak that makes both deterministic.
 *
 * Expects a preceding CTE named `ev` exposing id, received_at and
 * logical_call_id.
 */
const CALL_HOUR = `SELECT logical_call_id,
                (array_agg(date_trunc('hour', received_at) ORDER BY id))[1] AS hour
           FROM ev GROUP BY logical_call_id`;

let timer: NodeJS.Timeout | null = null;
let tickRunning = false;

/**
 * The exclusive upper bound for rollup: the start of the CURRENT hour.
 *
 * Never rolls up the hour in progress — it would be recomputed as incomplete
 * and then need recomputing again, and the read path can union the current
 * partial hour from detail far more cheaply than this job can chase it.
 */
function currentHourStart(now: Date): Date {
  const d = new Date(now);
  d.setUTCMinutes(0, 0, 0);
  return d;
}

/**
 * Recompute [from, to) into both rollup tables.
 *
 * Both statements DELETE the range first, so calling this twice over the same
 * range leaves the same rows. `-1` is the sentinel for an unknown site, which
 * is what the table's primary key needs in place of a NULL.
 */
async function rollupRange(
  pool: NonNullable<Awaited<ReturnType<typeof getWriterPool>>>,
  from: Date,
  to: Date,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Per-node volume. `calls` is that node's receptions; `logical_calls` is
    // the distinct calls THAT NODE heard.
    //
    // Deliberately not summable across nodes — two nodes hearing one call
    // contribute 1 each — because the question this table answers is "how much
    // did this node hear", which is per node by definition. The network-wide
    // count lives in node_radio_hourly_sys, where it is attributed so that it
    // does sum. Anything totalling calls across the fleet must read that one.
    await client.query(
      `DELETE FROM node_radio_hourly WHERE hour >= $1::timestamptz AND hour < $2::timestamptz`,
      [from.toISOString(), to.toISOString()],
    );
    await client.query(
      `WITH ev AS MATERIALIZED (
         SELECT id, received_at, logical_call_id, node_id,
                COALESCE(system, 0) AS system,
                COALESCE(talkgroup, 0) AS talkgroup,
                COALESCE(site_rfss, -1) AS rfss,
                COALESCE(site_id, -1) AS site,
                audio_bytes
           FROM node_radio_events
          WHERE received_at >= $1::timestamptz AND received_at < $2::timestamptz
            AND ${CALL_GROUP}
       ), ch AS (
         ${CALL_HOUR}
       ), r AS (
         SELECT ch.hour, e.node_id, e.system, e.talkgroup, e.logical_call_id,
                e.rfss, e.site, MAX(e.audio_bytes) AS audio_bytes
           FROM ev e JOIN ch USING (logical_call_id)
          GROUP BY 1, 2, 3, 4, 5, 6, 7
       )
       INSERT INTO node_radio_hourly
         (hour, node_id, system, talkgroup, calls, audio_bytes, logical_calls)
       SELECT r.hour, r.node_id, r.system, r.talkgroup,
              COUNT(*)::int,
              COALESCE(SUM(r.audio_bytes), 0)::bigint,
              COUNT(DISTINCT r.logical_call_id)::int
         FROM r
        GROUP BY r.hour, r.node_id, r.system, r.talkgroup`,
      [from.toISOString(), to.toISOString()],
    );

    // Network-wide volume.
    //
    // THE SUMMABILITY PROBLEM. This table is keyed per SITE, but a call is not
    // a per-site thing — one transmission is simulcast from several, and the
    // whole point of logical_calls is to count it ONCE. Counting distinct
    // calls within each site row and then summing those rows double-counts
    // every simulcast call: measured on one live hour, 406 calls reported as
    // 822, which is the same 2x overcount the per-event counters used to have.
    //
    // So each call is ATTRIBUTED to exactly one of its rows — the lowest
    // (system, talkgroup, rfss, site) it was heard on, picked by rn = 1 — and
    // only that row counts it. The partition is the CALL alone, deliberately:
    // partitioning by talkgroup as well counts a patched call once per member
    // talkgroup, which on the same live hour read 407 against a true 406. Per row the figure then reads "calls whose first site was
    // this one", and summing any set of rows is exact, which is what
    // window=all needs. Receptions stay a plain COUNT(*): those really are
    // per-site, and they already summed correctly.
    //
    // encrypted/recorded are properties of the CALL, so they are resolved
    // across all of its receptions with a window before the attribution
    // filter picks the row to count them in.
    await client.query(
      `DELETE FROM node_radio_hourly_sys WHERE hour >= $1::timestamptz AND hour < $2::timestamptz`,
      [from.toISOString(), to.toISOString()],
    );
    //
    // logical_calls_tg is a SECOND attribution of the same calls, once per
    // (call, system, talkgroup) instead of once per call. The overview's
    // talkgroup card asks COUNT(DISTINCT call) PER TALKGROUP, and a patched
    // call belongs to each of its talkgroups; under the fleet-wide rn = 1 only
    // the lowest of them would count it and the rest would read zero. Summed
    // across a talkgroup's sites this equals the detail query exactly.
    await client.query(
      `WITH ev AS MATERIALIZED (
         SELECT id, received_at, logical_call_id, node_id,
                COALESCE(system, 0) AS system,
                COALESCE(talkgroup, 0) AS talkgroup,
                COALESCE(site_rfss, -1) AS rfss,
                COALESCE(site_id, -1) AS site,
                encrypted, recorded
           FROM node_radio_events
          WHERE received_at >= $1::timestamptz AND received_at < $2::timestamptz
            AND ${CALL_GROUP}
       ), ch AS (
         ${CALL_HOUR}
       ), r AS (
         SELECT ch.hour, e.system, e.talkgroup, e.rfss, e.site,
                e.logical_call_id, e.node_id,
                bool_or(e.encrypted) AS encrypted,
                bool_or(e.recorded) AS recorded
           FROM ev e JOIN ch USING (logical_call_id)
          GROUP BY 1, 2, 3, 4, 5, 6, 7
       ), a AS (
         SELECT r.*,
                bool_or(r.encrypted) OVER w AS call_encrypted,
                bool_or(r.recorded) OVER w AS call_recorded,
                ROW_NUMBER() OVER (
                  PARTITION BY r.hour, r.logical_call_id
                  ORDER BY r.system, r.talkgroup, r.rfss, r.site, r.node_id
                ) AS rn,
                ROW_NUMBER() OVER (
                  PARTITION BY r.hour, r.logical_call_id, r.system, r.talkgroup
                  ORDER BY r.rfss, r.site, r.node_id
                ) AS rn_tg
           FROM r
         WINDOW w AS (PARTITION BY r.hour, r.logical_call_id)
       )
       INSERT INTO node_radio_hourly_sys
         (hour, system, talkgroup, site_rfss, site_id,
          calls, logical_calls, receptions, encrypted_calls, recorded_calls,
          logical_calls_tg)
       SELECT a.hour, a.system, a.talkgroup, a.rfss, a.site,
              COUNT(*)::int,
              (COUNT(*) FILTER (WHERE a.rn = 1))::int,
              COUNT(*)::int,
              (COUNT(*) FILTER (WHERE a.rn = 1 AND a.call_encrypted))::int,
              (COUNT(*) FILTER (WHERE a.rn = 1 AND a.call_recorded))::int,
              (COUNT(*) FILTER (WHERE a.rn_tg = 1))::int
         FROM a
        GROUP BY a.hour, a.system, a.talkgroup, a.rfss, a.site`,
      [from.toISOString(), to.toISOString()],
    );

    // The overview's eight tiles, at the grain they are actually asked for.
    //
    // This is radioReceptionsSql from api/node-data.ts, bucketed by hour and
    // summarised per home talkgroup. It has to stay a mirror of that query:
    // if the two definitions of a reception drift, the page silently reports
    // different numbers depending on which window it is showing.
    //
    // THE CALL IS THE UNIT, and its earliest event defines it. `home` is that
    // event's talkgroup and `hour` is that event's hour, so a call spanning a
    // boundary lands in one bucket rather than being counted in both. Summing
    // rows across hours is then exact, which is the whole point.
    //
    // Ordering is by id, matching the read path: an unnested patch member
    // inherits its parent row's id, so ordering the EXPANDED set would leave
    // ties that resolve arbitrarily and `home` would differ run to run.
    await client.query(
      `DELETE FROM node_radio_hourly_rx WHERE hour >= $1::timestamptz AND hour < $2::timestamptz`,
      [from.toISOString(), to.toISOString()],
    );
    await client.query(
      `WITH ev AS MATERIALIZED (
         SELECT id, logical_call_id, talkgroup, patch_members, node_id,
                COALESCE(site_rfss, -1) AS rfss, COALESCE(site_id, -1) AS site,
                COALESCE(system, 0) AS system,
                recorded, received_at
           FROM node_radio_events
          WHERE received_at >= $1::timestamptz AND received_at < $2::timestamptz
            AND ${CALL_GROUP}
       ), tx AS (
         -- One row per call: when it started, what talkgroup it started on,
         -- and whether anything ever recorded it.
         SELECT logical_call_id,
                bool_or(recorded) AS rec,
                (array_agg(talkgroup ORDER BY id))[1] AS home,
                (array_agg(date_trunc('hour', received_at) ORDER BY id))[1] AS hour
           FROM ev GROUP BY logical_call_id
       ), expanded AS (
         SELECT logical_call_id, node_id, rfss, site, talkgroup AS tg FROM ev
         UNION ALL
         SELECT logical_call_id, node_id, rfss, site, unnest(patch_members)
           FROM ev WHERE patch_members IS NOT NULL
       ), pairs AS (
         SELECT DISTINCT logical_call_id, node_id, rfss, site, tg FROM expanded
       ), rx AS (
         SELECT t.hour, t.home,
                COUNT(*)::int AS receptions,
                (COUNT(*) FILTER (WHERE p.tg IS NOT DISTINCT FROM t.home))::int
                  AS receptions_home
           FROM pairs p JOIN tx t USING (logical_call_id)
          GROUP BY 1, 2
       ), calls AS (
         -- Attribution is unnecessary here BECAUSE the call is already the
         -- grain: it has exactly one hour and one home, so counting distinct
         -- calls per (hour, home) cannot double-count the way the per-site
         -- table could. That is what buying the narrower grain gets us.
         SELECT hour, home,
                COUNT(*)::int AS transmissions,
                (COUNT(*) FILTER (WHERE rec))::int AS recorded
           FROM tx GROUP BY 1, 2
       )
       INSERT INTO node_radio_hourly_rx
         (hour, home_talkgroup, receptions, receptions_home, transmissions, recorded)
       SELECT rx.hour, COALESCE(rx.home, 0),
              rx.receptions, rx.receptions_home,
              COALESCE(c.transmissions, 0), COALESCE(c.recorded, 0)
         FROM rx
         LEFT JOIN calls c ON c.hour = rx.hour AND c.home IS NOT DISTINCT FROM rx.home`,
      [from.toISOString(), to.toISOString()],
    );

    // Top radios, at the grain the card asks for.
    //
    // Receptions here are the same tuple the detail query counts --
    // DISTINCT (call, node, rfss, site, talkgroup) per unit -- so the list can
    // be summed over a window and still agree with the page around it. The
    // talkgroup is IN the tuple deliberately: a radio heard on a patched call
    // was heard on each of its talkgroups, which is how the detail query has
    // always counted it.
    //
    // No RID band filter: RID_VALID is a read-path predicate (see node-data.ts)
    // and applying it here would freeze today's definition of a plausible radio
    // id into stored history. Decode noise is a few dozen rows a week.
    await client.query(
      `DELETE FROM node_radio_hourly_unit WHERE hour >= $1::timestamptz AND hour < $2::timestamptz`,
      [from.toISOString(), to.toISOString()],
    );
    await client.query(
      `WITH ev AS MATERIALIZED (
         SELECT id, received_at, logical_call_id, node_id, source_unit, source_alias,
                COALESCE(talkgroup, 0) AS talkgroup,
                COALESCE(site_rfss, -1) AS rfss,
                COALESCE(site_id, -1) AS site
           FROM node_radio_events
          WHERE received_at >= $1::timestamptz AND received_at < $2::timestamptz
            AND ${CALL_GROUP} AND source_unit IS NOT NULL
       ), ch AS (
         ${CALL_HOUR}
       ), rx AS (
         SELECT DISTINCT ch.hour, e.source_unit, e.logical_call_id, e.node_id,
                e.rfss, e.site, e.talkgroup
           FROM ev e JOIN ch USING (logical_call_id)
       ), al AS (
         -- The alias the detail query picks: most recent, skipping the ones
         -- that only echo the RID back as text.
         SELECT ch.hour, e.source_unit,
                (array_agg(e.source_alias ORDER BY e.received_at DESC)
                   FILTER (WHERE e.source_alias IS NOT NULL
                             AND e.source_alias <> e.source_unit::text))[1] AS alias
           FROM ev e JOIN ch USING (logical_call_id)
          GROUP BY 1, 2
       )
       INSERT INTO node_radio_hourly_unit (hour, source_unit, receptions, alias)
       SELECT rx.hour, rx.source_unit, COUNT(*)::int, al.alias
         FROM rx
         LEFT JOIN al ON al.hour = rx.hour AND al.source_unit = rx.source_unit
        GROUP BY rx.hour, rx.source_unit, al.alias`,
      [from.toISOString(), to.toISOString()],
    );

    await client.query(
      `UPDATE node_rollup_state SET rolled_up_to = $1::timestamptz, updated_at = now() WHERE id`,
      [to.toISOString()],
    );
    await client.query('COMMIT');
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* connection already gone */
    }
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Roll up every completed hour not yet covered. Never throws.
 *
 * Resumes from `node_rollup_state.rolled_up_to`; a cold start reaches back
 * MAX_BACKFILL_HOURS, which is the whole of the detail table.
 */
export async function rollupNodeHourlyOnce(now: Date = new Date()): Promise<number> {
  try {
    const pool = await getWriterPool();
    if (!pool) return 0;

    const upTo = currentHourStart(now);
    const state = await pool.query<{ rolled_up_to: Date | null }>(
      `SELECT rolled_up_to FROM node_rollup_state WHERE id`,
    );
    const earliest = new Date(upTo.getTime() - MAX_BACKFILL_HOURS * 3_600_000);
    const stored = state.rows[0]?.rolled_up_to ?? null;
    let cursor: Date = stored === null || stored < earliest ? earliest : stored;
    if (cursor >= upTo) return 0;

    // Re-derive a few hours BEHIND the cursor as well as ahead of it.
    //
    // `recorded` is not final when an hour ends. markRecorded sets it when the
    // audio arrives, which is routinely after the call — and so, often enough,
    // after the hour it belongs to has already been summarised. A cursor that
    // only moves forward never revisits that hour, so its recorded count stays
    // permanently short. Nothing noticed while nothing read these tables; the
    // moment the ingested tile is served from here it becomes wrong numbers on
    // the page.
    //
    // DELETE-then-INSERT per hour makes re-running free of consequence, so the
    // cheapest correct answer is simply to redo the recent past every pass.
    if (stored !== null) {
      const redoFrom = new Date(
        Math.max(earliest.getTime(), cursor.getTime() - REDERIVE_HOURS * 3_600_000),
      );
      if (redoFrom < cursor) cursor = redoFrom;
    }

    let hours = 0;
    while (cursor < upTo) {
      const next = new Date(
        Math.min(cursor.getTime() + BATCH_HOURS * 3_600_000, upTo.getTime()),
      );
      await rollupRange(pool, cursor, next);
      hours += Math.round((next.getTime() - cursor.getTime()) / 3_600_000);
      cursor = next;
    }
    if (hours > 0) {
      log.info({ hours, upTo: upTo.toISOString() }, 'node hourly rollup: rebuilt');
    }
    return hours;
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'node hourly rollup: failed');
    return 0;
  }
}

/**
 * The oldest instant the pruner must not delete past.
 *
 * The rollup can only derive an hour while its detail rows still exist, so
 * pruning ahead of the rollup would erase history that has not been summarised
 * yet — permanently, since the detail table is the only source. Null means
 * nothing has been rolled up, so nothing may be pruned.
 */
export async function rollupHighWater(): Promise<Date | null> {
  try {
    const pool = await getWriterPool();
    if (!pool) return null;
    const r = await pool.query<{ rolled_up_to: Date | null }>(
      `SELECT rolled_up_to FROM node_rollup_state WHERE id`,
    );
    return r.rows[0]?.rolled_up_to ?? null;
  } catch {
    return null;
  }
}

/** Start the hourly rollup. Idempotent. */
export function startNodeHourlyRollup(intervalMs: number = ROLLUP_INTERVAL_MS): void {
  if (timer) return;
  // Shortly after boot, so a gap from downtime closes without waiting an hour
  // — and, on the first deploy, so the backfill runs immediately.
  setTimeout(() => void rollupNodeHourlyOnce(), 30_000).unref?.();
  timer = setInterval(async () => {
    if (tickRunning) return;
    tickRunning = true;
    try {
      await rollupNodeHourlyOnce();
    } finally {
      tickRunning = false;
    }
  }, intervalMs);
  timer.unref?.();
}

export function stopNodeHourlyRollup(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
    tickRunning = false;
  }
}
