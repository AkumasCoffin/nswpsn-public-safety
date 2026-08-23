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

/** A call, and only a call. Kept textually identical to node-data.ts's
 *  callGroup() — if one changes the other must. */
const CALL_GROUP =
  "(event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%')";

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
    // the distinct calls it heard, which is NOT summable across nodes — two
    // nodes hearing one call contribute 1 each.
    await client.query(
      `DELETE FROM node_radio_hourly WHERE hour >= $1::timestamptz AND hour < $2::timestamptz`,
      [from.toISOString(), to.toISOString()],
    );
    await client.query(
      `INSERT INTO node_radio_hourly
         (hour, node_id, system, talkgroup, calls, audio_bytes, logical_calls)
       SELECT r.hour, r.node_id, r.system, r.talkgroup,
              COUNT(*)::int,
              COALESCE(SUM(r.audio_bytes), 0)::bigint,
              COUNT(DISTINCT r.logical_call_id)::int
         FROM (
           SELECT date_trunc('hour', received_at) AS hour,
                  node_id,
                  COALESCE(system, 0) AS system,
                  COALESCE(talkgroup, 0) AS talkgroup,
                  logical_call_id,
                  COALESCE(site_rfss, -1) AS rfss,
                  COALESCE(site_id, -1) AS site,
                  MAX(audio_bytes) AS audio_bytes
             FROM node_radio_events
            WHERE received_at >= $1::timestamptz AND received_at < $2::timestamptz
              AND ${CALL_GROUP}
            GROUP BY 1, 2, 3, 4, 5, 6, 7
         ) r
        GROUP BY r.hour, r.node_id, r.system, r.talkgroup`,
      [from.toISOString(), to.toISOString()],
    );

    // Network-wide volume. logical_calls counts each over-the-air call ONCE
    // however many nodes or sites heard it, which is the number the all-time
    // view exists to show.
    await client.query(
      `DELETE FROM node_radio_hourly_sys WHERE hour >= $1::timestamptz AND hour < $2::timestamptz`,
      [from.toISOString(), to.toISOString()],
    );
    await client.query(
      `INSERT INTO node_radio_hourly_sys
         (hour, system, talkgroup, site_rfss, site_id,
          calls, logical_calls, receptions, encrypted_calls, recorded_calls)
       SELECT r.hour, r.system, r.talkgroup, r.rfss, r.site,
              COUNT(*)::int,
              COUNT(DISTINCT r.logical_call_id)::int,
              COUNT(*)::int,
              COUNT(DISTINCT r.logical_call_id) FILTER (WHERE r.encrypted)::int,
              COUNT(DISTINCT r.logical_call_id) FILTER (WHERE r.recorded)::int
         FROM (
           SELECT date_trunc('hour', received_at) AS hour,
                  COALESCE(system, 0) AS system,
                  COALESCE(talkgroup, 0) AS talkgroup,
                  COALESCE(site_rfss, -1) AS rfss,
                  COALESCE(site_id, -1) AS site,
                  logical_call_id,
                  node_id,
                  bool_or(encrypted) AS encrypted,
                  bool_or(recorded) AS recorded
             FROM node_radio_events
            WHERE received_at >= $1::timestamptz AND received_at < $2::timestamptz
              AND ${CALL_GROUP}
            GROUP BY 1, 2, 3, 4, 5, 6, 7
         ) r
        GROUP BY r.hour, r.system, r.talkgroup, r.rfss, r.site`,
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
