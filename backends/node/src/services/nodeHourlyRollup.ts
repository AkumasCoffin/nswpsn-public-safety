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
 * Hours per statement.
 *
 * This was 24, which is fine for the steady hourly tick (it only ever covers
 * the lookback plus the hour just closed) but not for a catch-up, where every
 * batch is a full slice of the detail table through three window functions.
 * On the Postgres VM as it stands a plain indexed COUNT over one day of detail
 * — about 184k rows — takes ~35s, so a 24-hour batch could not finish inside
 * the writer pool's 60s statement_timeout and the first one failed every pass:
 * the cursor never moved and a month of history stayed unbuilt. Six hours is
 * small enough to finish comfortably, and only a catch-up pays for the extra
 * round trips.
 */
const BATCH_HOURS = 6;

/**
 * How long one batch's statements may run, overriding the writer pool's 60s
 * default for this transaction only.
 *
 * A rollup batch is a background re-derive, not a request: it is allowed to be
 * slow, and the alternative is the silent stall described above. Bounded rather
 * than disabled so a pathological plan still gives up instead of holding a
 * connection all day. Precedent: cleanup.ts and indexBuilder.ts do the same for
 * their own long statements.
 */
const BATCH_STATEMENT_TIMEOUT_MS = 300_000;

/**
 * Batches one tick will run before leaving the rest to the next one.
 *
 * The loop used to run until it caught up, which after a cursor rewind meant a
 * single tick holding a writer connection against a slow database for hours,
 * competing with live ingest. Each batch commits its own cursor advance, so
 * stopping early loses nothing: the next tick resumes where this one stopped.
 * At 6 hours a batch this is two days of history per tick, so even a month-long
 * rebuild is done inside a day without ever monopolising the database.
 */
const MAX_BATCHES_PER_TICK = 8;

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
 * How far BEFORE the range each pass reads detail.
 *
 * A pass re-derives [from, to), and an hour's final derivation is the pass
 * where that hour is the lowest in the range. Scanning detail only from `from`
 * hides the earlier receptions of a call that began just before it: its
 * earliest VISIBLE event then sits in `from`, CALL_HOUR files it there, and
 * the previous pass — which could see the whole call — had already filed the
 * same call in the hour before. That is the boundary double-count migration
 * 085 removed, reintroduced by the scan range rather than by the bucketing.
 *
 * So detail is read an hour early and CALL_HOUR's own filter, not the scan
 * range, decides which calls a pass owns. A group spans at most twice
 * GROUP_WINDOW_SECONDS (nodeEvents.ts), so an hour is orders of magnitude
 * more slack than a straddling call can need, and the extra hour is one more
 * hour of an index scan the statement was doing anyway.
 */
const CALL_LOOKBACK = "interval '1 hour'";

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
 * It also filters to the hours THIS PASS OWNS, which is why every consumer
 * joins it as an inner join: a call whose earliest event predates the range
 * belongs to an hour an earlier pass already filed, and this pass must leave
 * it alone rather than count it again at its own lower boundary. See
 * CALL_LOOKBACK.
 *
 * Expects a preceding CTE named `ev` exposing id, received_at and
 * logical_call_id, and the range as $1/$2.
 */
const CALL_HOUR = `SELECT * FROM (
           SELECT logical_call_id,
                  (array_agg(date_trunc('hour', received_at) ORDER BY id))[1] AS hour
             FROM ev GROUP BY logical_call_id
         ) c
        WHERE c.hour >= $1::timestamptz AND c.hour < $2::timestamptz`;

let timer: NodeJS.Timeout | null = null;
/** The one-shot that waits for the next HH:00:30 before the interval starts. */
let alignTimer: NodeJS.Timeout | null = null;
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
 * How long node_radio_hourly_heard is kept (migration 126).
 *
 * It exists for the system and site drill-downs, whose longest window is 30
 * days, and nothing else reads it. Two days of slack past that so a 30-day
 * window never reaches a pruned hour, whatever the clocks do.
 */
const HEARD_RETENTION_HOURS = 24 * 32;

/**
 * Which cursor a range moves, and how far past it the detail scan reaches.
 *
 * `advance` is the forward pass: rolled_up_to — which is also the read path's
 * seam — moves up to `to`, and the scan stops at `to` as it always has. A call
 * that straddles `to` is completed by the next pass, which re-derives the same
 * hour with REDERIVE_HOURS of slack after it.
 *
 * `rederive` is the backward walk (migration 126): rederive_below moves down to
 * `from`, and rolled_up_to is left alone so the seam does not move. No later
 * pass will revisit the top hour of this range, so the scan reaches a further
 * CALL_LOOKBACK past `to` to see the end of any call that started inside it —
 * the same events that hour's final forward derivation saw. CALL_HOUR still
 * decides ownership, so a call starting after `to` is not counted here.
 * `last` clears the cursor instead, once the walk reaches the oldest hour the
 * detail table still holds in full.
 */
type RangeMode = { kind: 'advance' } | { kind: 'rederive'; last: boolean };

/**
 * Recompute [from, to) into every rollup table.
 *
 * Every statement DELETEs the range first, so calling this twice over the same
 * range leaves the same rows. `-1` is the sentinel for an unknown site, which
 * is what the table's primary key needs in place of a NULL.
 *
 * $1/$2 are the range the pass OWNS; $3 is how far the detail scan reaches.
 */
async function rollupRange(
  pool: NonNullable<Awaited<ReturnType<typeof getWriterPool>>>,
  from: Date,
  to: Date,
  mode: RangeMode = { kind: 'advance' },
): Promise<void> {
  const scanTo = mode.kind === 'rederive' ? new Date(to.getTime() + 3_600_000) : to;
  const range = [from.toISOString(), to.toISOString(), scanTo.toISOString()];
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Transaction-scoped, so it needs no RESET and cannot leak to the next
    // user of this pooled connection. See BATCH_STATEMENT_TIMEOUT_MS.
    await client.query(`SET LOCAL statement_timeout = ${BATCH_STATEMENT_TIMEOUT_MS}`);

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
          WHERE received_at >= $1::timestamptz - ${CALL_LOOKBACK}
            AND received_at < $3::timestamptz
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
      range,
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
    //
    // logical_calls_site is a THIRD, once per (call, system, site), for the
    // one question the other two cannot answer: how many calls did THIS SITE
    // hear. The fleet-wide systems tree summed logical_calls per site, so
    // every site in a simulcast group but the lowest read far below its real
    // figure and contradicted both the node-scoped tree and the site
    // drill-down, which count from raw events. Summed over the (system, site)
    // rows the tree groups by, this equals COUNT(DISTINCT call) per site in
    // the detail table. Talkgroup is deliberately NOT in the partition: a
    // patched call is one call at the site, however many talkgroups carried
    // it. See migration 123.
    //
    // encrypted_calls_site / recorded_calls_site count the same per-site
    // attribution's calls that were encrypted / recorded, for the site
    // drill-down's tiles (migration 126). The fleet-wide pair credits a
    // simulcast call to its lowest site only, so a site total built from them
    // reads near zero at every other site in the group.
    await client.query(
      `WITH ev AS MATERIALIZED (
         SELECT id, received_at, logical_call_id, node_id,
                COALESCE(system, 0) AS system,
                COALESCE(talkgroup, 0) AS talkgroup,
                COALESCE(site_rfss, -1) AS rfss,
                COALESCE(site_id, -1) AS site,
                encrypted, recorded
           FROM node_radio_events
          WHERE received_at >= $1::timestamptz - ${CALL_LOOKBACK}
            AND received_at < $3::timestamptz
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
                ) AS rn_tg,
                ROW_NUMBER() OVER (
                  PARTITION BY r.hour, r.logical_call_id, r.system, r.rfss, r.site
                  ORDER BY r.talkgroup, r.node_id
                ) AS rn_site
           FROM r
         WINDOW w AS (PARTITION BY r.hour, r.logical_call_id)
       )
       INSERT INTO node_radio_hourly_sys
         (hour, system, talkgroup, site_rfss, site_id,
          calls, logical_calls, receptions, encrypted_calls, recorded_calls,
          logical_calls_tg, logical_calls_site,
          encrypted_calls_site, recorded_calls_site)
       SELECT a.hour, a.system, a.talkgroup, a.rfss, a.site,
              COUNT(*)::int,
              (COUNT(*) FILTER (WHERE a.rn = 1))::int,
              COUNT(*)::int,
              (COUNT(*) FILTER (WHERE a.rn = 1 AND a.call_encrypted))::int,
              (COUNT(*) FILTER (WHERE a.rn = 1 AND a.call_recorded))::int,
              (COUNT(*) FILTER (WHERE a.rn_tg = 1))::int,
              (COUNT(*) FILTER (WHERE a.rn_site = 1))::int,
              (COUNT(*) FILTER (WHERE a.rn_site = 1 AND a.call_encrypted))::int,
              (COUNT(*) FILTER (WHERE a.rn_site = 1 AND a.call_recorded))::int
         FROM a
        GROUP BY a.hour, a.system, a.talkgroup, a.rfss, a.site`,
      range,
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
          WHERE received_at >= $1::timestamptz - ${CALL_LOOKBACK}
            AND received_at < $3::timestamptz
            AND ${CALL_GROUP}
       ), tx AS (
         -- One row per call this pass owns: when it started, what talkgroup it
         -- started on, and whether anything ever recorded it. The hour filter
         -- is CALL_HOUR's, spelled out because this one also needs home and
         -- rec; everything downstream joins tx, so a call belonging to an
         -- earlier hour drops out of the whole statement.
         SELECT * FROM (
           SELECT logical_call_id,
                  bool_or(recorded) AS rec,
                  (array_agg(talkgroup ORDER BY id))[1] AS home,
                  (array_agg(date_trunc('hour', received_at) ORDER BY id))[1] AS hour
             FROM ev GROUP BY logical_call_id
         ) t
        WHERE t.hour >= $1::timestamptz AND t.hour < $2::timestamptz
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
      range,
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
          WHERE received_at >= $1::timestamptz - ${CALL_LOOKBACK}
            AND received_at < $3::timestamptz
            AND ${CALL_GROUP} AND source_unit IS NOT NULL
       ), ch AS (
         ${CALL_HOUR}
       ), rx AS (
         SELECT DISTINCT ch.hour, e.source_unit, e.logical_call_id, e.node_id,
                e.rfss, e.site, e.talkgroup
           FROM ev e JOIN ch USING (logical_call_id)
       ), al AS (
         -- The alias the detail query picks: most recent, skipping the ones
         -- that only echo the RID back as text. last_heard (migration 125) is
         -- the Aliases list's "Last heard", which the hour alone would round
         -- down.
         SELECT ch.hour, e.source_unit,
                (array_agg(e.source_alias ORDER BY e.received_at DESC)
                   FILTER (WHERE e.source_alias IS NOT NULL
                             AND e.source_alias <> e.source_unit::text))[1] AS alias,
                MAX(e.received_at) AS last_heard
           FROM ev e JOIN ch USING (logical_call_id)
          GROUP BY 1, 2
       )
       INSERT INTO node_radio_hourly_unit (hour, source_unit, receptions, alias, last_heard)
       SELECT rx.hour, rx.source_unit, COUNT(*)::int, al.alias, al.last_heard
         FROM rx
         LEFT JOIN al ON al.hour = rx.hour AND al.source_unit = rx.source_unit
        GROUP BY rx.hour, rx.source_unit, al.alias, al.last_heard`,
      range,
    );

    // Who heard what, where — the system and site drill-downs (migration 126).
    //
    // rx is node_radio_hourly_sys's grain exactly: one row per reception
    // (call, node, system, site, talkgroup). Each is then filed under ONE
    // radio, the first that keyed it by id, or 0 when none decoded, so that
    // summing receptions over any set of rows gives the same figure the
    // per-site table does. Counting a reception once per radio that appears in
    // it, as node_radio_hourly_unit does, would make a node's or a site's sum
    // exceed its receptions whenever a grant and its call disagree.
    //
    // No RID band filter, for the same reason as node_radio_hourly_unit.
    await client.query(
      `DELETE FROM node_radio_hourly_heard WHERE hour >= $1::timestamptz AND hour < $2::timestamptz`,
      [from.toISOString(), to.toISOString()],
    );
    await client.query(
      `WITH ev AS MATERIALIZED (
         SELECT id, received_at, logical_call_id, node_id, source_unit,
                COALESCE(system, 0) AS system,
                COALESCE(talkgroup, 0) AS talkgroup,
                COALESCE(site_rfss, -1) AS rfss,
                COALESCE(site_id, -1) AS site
           FROM node_radio_events
          WHERE received_at >= $1::timestamptz - ${CALL_LOOKBACK}
            AND received_at < $3::timestamptz
            AND ${CALL_GROUP}
       ), ch AS (
         ${CALL_HOUR}
       ), rx AS (
         SELECT ch.hour, e.node_id, e.system, e.rfss, e.site, e.talkgroup,
                COALESCE((array_agg(e.source_unit ORDER BY e.id)
                            FILTER (WHERE e.source_unit IS NOT NULL))[1], 0) AS source_unit,
                MAX(e.received_at) AS last_heard
           FROM ev e JOIN ch USING (logical_call_id)
          GROUP BY ch.hour, e.logical_call_id, e.node_id, e.system, e.rfss, e.site, e.talkgroup
       )
       INSERT INTO node_radio_hourly_heard
         (hour, node_id, system, site_rfss, site_id, talkgroup, source_unit, receptions, last_heard)
       SELECT hour, node_id, system, rfss, site, talkgroup, source_unit,
              COUNT(*)::int, MAX(last_heard)
         FROM rx
        GROUP BY hour, node_id, system, rfss, site, talkgroup, source_unit`,
      range,
    );

    // The cursor this range moves, in the same transaction as the rows it
    // vouches for — see RangeMode.
    if (mode.kind === 'advance') {
      await client.query(
        `UPDATE node_rollup_state SET rolled_up_to = $1::timestamptz, updated_at = now() WHERE id`,
        [to.toISOString()],
      );
    } else {
      await client.query(
        `UPDATE node_rollup_state SET rederive_below = $1::timestamptz, updated_at = now() WHERE id`,
        [mode.last ? null : from.toISOString()],
      );
    }
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
    const state = await pool.query<{ rolled_up_to: Date | null; rederive_below?: Date | null }>(
      `SELECT rolled_up_to, rederive_below FROM node_rollup_state WHERE id`,
    );
    const earliest = new Date(upTo.getTime() - MAX_BACKFILL_HOURS * 3_600_000);
    const stored = state.rows[0]?.rolled_up_to ?? null;
    let cursor: Date = stored === null || stored < earliest ? earliest : stored;

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
    //
    // Not when the cursor already sits at the current hour: there is nothing
    // new, and the tick that got it there redid the recent past.
    if (stored !== null && cursor < upTo) {
      const redoFrom = new Date(
        Math.max(earliest.getTime(), cursor.getTime() - REDERIVE_HOURS * 3_600_000),
      );
      if (redoFrom < cursor) cursor = redoFrom;
    }

    let hours = 0;
    let batches = 0;
    while (cursor < upTo && batches < MAX_BATCHES_PER_TICK) {
      const next = new Date(
        Math.min(cursor.getTime() + BATCH_HOURS * 3_600_000, upTo.getTime()),
      );
      await rollupRange(pool, cursor, next);
      hours += Math.round((next.getTime() - cursor.getTime()) / 3_600_000);
      cursor = next;
      batches += 1;
    }
    const behindHours = Math.max(0, Math.round((upTo.getTime() - cursor.getTime()) / 3_600_000));
    if (hours > 0) {
      log.info(
        { hours, batches, behindHours, upTo: upTo.toISOString() },
        behindHours > 0
          ? 'node hourly rollup: rebuilt a batch, more to go'
          : 'node hourly rollup: rebuilt',
      );
    }

    // The forward pass has first call on the tick's batches: a seam that falls
    // behind slows every staff read, and a history rebuild slows none.
    const rederived = await rederiveBackwards(
      pool,
      state.rows[0]?.rederive_below ?? null,
      MAX_BATCHES_PER_TICK - batches,
    );
    await pruneHeard(pool, upTo);
    return hours + rederived;
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'node hourly rollup: failed');
    return 0;
  }
}

/**
 * Walk rederive_below down towards the oldest complete hour of detail,
 * re-deriving as it goes (migration 126). Returns the hours re-derived.
 *
 * Newest first, so the windows the staff page reads most become whole first:
 * 24 h after one tick, 30 d within about a day at the batches a tick has spare.
 *
 * THE FLOOR. An hour may only be re-derived while its detail is complete, and
 * the pruner deletes detail from the bottom. Re-deriving a half-pruned hour
 * would DELETE its correct rows and INSERT a fraction of them — permanently,
 * since the detail is gone. The hour holding the oldest row is partial by
 * definition; each range also reads an hour of lookback below itself; and the
 * pruner can take another hour while the walk runs. Hence oldest hour + 3.
 */
async function rederiveBackwards(
  pool: NonNullable<Awaited<ReturnType<typeof getWriterPool>>>,
  below: Date | null,
  budget: number,
): Promise<number> {
  if (below === null || budget <= 0) return 0;
  const f = await pool.query<{ floor: Date | string | null }>(
    `SELECT date_trunc('hour', MIN(received_at)) + interval '3 hours' AS floor
       FROM node_radio_events
      WHERE ${CALL_GROUP}`,
  );
  const rawFloor = f.rows[0]?.floor ?? null;
  const floor = rawFloor === null ? null : new Date(rawFloor).getTime();
  let to = new Date(below).getTime();
  let hours = 0;
  let done = false;
  for (let b = 0; b < budget && !done; b++) {
    if (floor === null || to <= floor) {
      // Nothing left that the detail can vouch for. Cleared on its own: there
      // is no range to carry it.
      await pool.query(
        `UPDATE node_rollup_state SET rederive_below = NULL, updated_at = now() WHERE id`,
      );
      done = true;
      break;
    }
    const from = Math.max(to - BATCH_HOURS * 3_600_000, floor);
    done = from <= floor;
    await rollupRange(pool, new Date(from), new Date(to), { kind: 'rederive', last: done });
    hours += Math.round((to - from) / 3_600_000);
    to = from;
  }
  if (hours > 0 || done) {
    log.info(
      { hours, below: done ? null : new Date(to).toISOString() },
      done ? 'node hourly rollup: history re-derived' : 'node hourly rollup: re-deriving history',
    );
  }
  return hours;
}

/**
 * Drop node_radio_hourly_heard rows past HEARD_RETENTION_HOURS. A tick's worth
 * of rows (~800) each time, so it needs no batching.
 */
async function pruneHeard(
  pool: NonNullable<Awaited<ReturnType<typeof getWriterPool>>>,
  upTo: Date,
): Promise<void> {
  await pool.query(
    `DELETE FROM node_radio_hourly_heard WHERE hour < $1::timestamptz`,
    [new Date(upTo.getTime() - HEARD_RETENTION_HOURS * 3_600_000).toISOString()],
  );
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

/** One tick, guarded against overlapping with the previous one. */
async function tick(): Promise<void> {
  if (tickRunning) return;
  tickRunning = true;
  try {
    await rollupNodeHourlyOnce();
  } finally {
    tickRunning = false;
  }
}

/**
 * Start the hourly rollup. Idempotent.
 *
 * ALIGNED TO THE CLOCK, not to boot. The read path unions rollup rows below
 * the derived seam with detail from the seam onwards (api/node-data.ts
 * ROLLUP_EDGE_EXPR / resolveRollupEdge), and that works wherever the seam is — but the detail half is
 * the expensive one, and on a tick at boot-time + k hours the seam sat on
 * average half an hour behind the clock, so every fleet-wide read paid for
 * half an hour of detail it did not need to. Firing just after the hour turns
 * means the hour that just closed is derived within a minute of closing.
 *
 * The 30s offset keeps the pass off the exact top of the hour, where the
 * other hourly loops (cleanup, the event pruner) already are.
 */
export function startNodeHourlyRollup(intervalMs: number = ROLLUP_INTERVAL_MS): void {
  if (timer || alignTimer) return;
  // Shortly after boot, so a gap from downtime closes without waiting for the
  // hour to turn — and, on the first deploy, so the backfill runs immediately.
  setTimeout(() => void tick(), 30_000).unref?.();
  // Then at HH:00:30. A non-default interval (tests) keeps the plain cadence:
  // aligning to the hour would make a short interval meaningless.
  if (intervalMs === ROLLUP_INTERVAL_MS) {
    const now = Date.now();
    const next = new Date(now);
    next.setUTCMinutes(0, 30, 0);
    if (next.getTime() <= now) next.setUTCHours(next.getUTCHours() + 1);
    alignTimer = setTimeout(() => {
      alignTimer = null;
      void tick();
      timer = setInterval(() => void tick(), intervalMs);
      timer.unref?.();
    }, next.getTime() - now);
    alignTimer.unref?.();
    return;
  }
  timer = setInterval(() => void tick(), intervalMs);
  timer.unref?.();
}

export function stopNodeHourlyRollup(): void {
  if (alignTimer) {
    clearTimeout(alignTimer);
    alignTimer = null;
  }
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
  tickRunning = false;
}
