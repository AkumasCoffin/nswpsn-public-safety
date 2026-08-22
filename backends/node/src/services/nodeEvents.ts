/**
 * Per-event feeder-node capture + logical-call grouping (migrations 043/044).
 *
 * RADIO (migration 044): rows in node_radio_events come ONLY from vce
 * ACTIVITY events posted by the Go agent (/api/node-ingest/activity →
 * recordActivityEvents). Activity events carry the real P25 identity — the
 * `system` column stores the P25 systemId — plus per-event action/site/
 * encryption. The rdio call-upload relay no longer inserts rows; it calls
 * markRecorded() to flag the closest matching event row recorded=true
 * (audio exists in central rdio) with its audio size.
 *
 * PAGER (unchanged from 043): every relayed page is recorded via
 * recordPagerEvent.
 *
 * Both paths write a DETAIL row (30-day retention — see nodeEventsPruner.ts)
 * and roll into the hourly FOREVER buckets (node_radio_hourly /
 * node_radio_hourly_sys / node_pager_hourly) in the same transaction.
 *
 * Logical grouping: the same over-the-air transmission heard by N nodes
 * arrives as N events. Rows within ±4s on the same (system, talkgroup)
 * [radio: (systemId, target)] or (capcode, sha256(message)) [pager] share
 * one logical id — the detail-row id of the FIRST member of the group.
 * Group assignment is serialised per key with
 * pg_advisory_xact_lock(hashtext(key)) so two concurrent receptions of the
 * same call can't each start their own group.
 *
 * CONTRACT: every exported record- and mark- function is fire-safe — any
 * failure is logged and swallowed; they never throw to the caller. Capture
 * must never affect the relay path.
 */
import { createHash } from 'node:crypto';
import { getWriterPool } from '../db/pool.js';
import { log } from '../lib/log.js';

/** Two receptions of the same call may carry timestamps a few seconds
 *  apart (queueing on the node, clock skew). ±4s matches the shortest
 *  realistic gap between two DISTINCT calls on one talkgroup. */
const GROUP_WINDOW_SECONDS = 4;

/** markRecorded's match window between an rdio call upload's dateTime and
 *  the activity event's received_at. Deliberately TIGHT: the upload's
 *  dateTime is the call's START, not some later finalise time — vce sends
 *  `(int)(audioRecording.getStartTime() / 1E3)` (RdioScannerBroadcaster),
 *  the same clock and the same instant the activity event is stamped from.
 *  The only slack needed is that whole-second truncation (≤1s) plus the gap
 *  between the recorder's first audio segment and the call-grant event.
 *
 *  MEASURED, not guessed (RECORDED_DIAG over a live minute): the skew runs
 *  from ~1.7s to ~5s with a tail past 12s. It is not clock error — it is the
 *  real gap between the call-grant event (which stamps the row) and the
 *  recorder's first audio sample (which stamps the upload), so it is always
 *  positive-ish and varies with how fast the traffic channel comes up.
 *
 *  An earlier 2s value was set from the wrong theory and matched almost
 *  nothing. Width alone can't cause a MISS-storm anyway — recorded=false
 *  means N uploads claim N distinct rows — but it can certainly cause one by
 *  being narrower than the physical skew, which is what happened.
 *
 *  Being generous here is now safe because time is no longer the only
 *  discriminator: source unit and frequency (below) pin the exact call, so a
 *  wide window no longer risks claiming a neighbour's row. */
const RECORDED_WINDOW_SECONDS = Number(process.env['RECORDED_WINDOW_SECONDS'] ?? 10) || 10;

/**
 * Outer bound for an IDENTITY match — same talkgroup, same calling unit, same
 * traffic channel. Beyond the time window above, that combination is the same
 * call by any reasonable reading, so the clock stops being the deciding factor.
 *
 * This exists because the two sides do not share a clock basis and cannot be
 * made to. vce stamps the rdio upload with audioRecording.getStartTime() — the
 * real call start — while the activity event carries observed_at_ms, when the
 * activity logger WROTE the row (ControlActivityLookup: v.observed_at_ms).
 * p25_activity_event has no call-start column, so there is nothing to align to.
 *
 * The identity, though, vce hands us on both sides already: FormField.SOURCE
 * and FormField.FREQUENCY on the upload, source_unit and frequency on the
 * event. Matching on what identifies the call beats matching on two clocks
 * that were never the same clock.
 *
 * Still bounded, not unlimited: traffic channels are reused, so a much older
 * call could otherwise present the same (unit, frequency) pair.
 */
const RECORDED_OUTER_SECONDS = Number(process.env['RECORDED_OUTER_SECONDS'] ?? 90) || 90;

/** Log every markRecorded outcome (HIT with its timestamp skew, MISS with how
 *  near an event actually was). Off by default — one line per call upload is a
 *  lot of log on a busy system — but it is the only way to tell an upload that
 *  never arrived from one that arrived and matched nothing. Enable with
 *  RECORDED_DIAG=1 for a minute, read the answer, turn it back off. */
const RECORDED_DIAG = process.env['RECORDED_DIAG'] === '1';

/** A node with a wildly wrong clock must not scatter rows across the
 *  timeline (they'd never prune / group). Anything outside now±48h is
 *  replaced with the server's now. */
const CLOCK_SANITY_MS = 48 * 60 * 60 * 1000;

/**
 * Minimum spacing between stored decode samples for one (node, site).
 *
 * Migration 069 was written for "~1/min/site/node". Measured on a live site:
 * 14,066 samples in ~20 hours from ONE node — about 11.5/min, because the
 * agent ships site snapshots far more often than once a minute. That is ~11x
 * the rows the retention estimate assumed, for no extra fidelity: the chart
 * buckets to a minute at its finest, so anything denser is averaged away the
 * moment it is read.
 *
 * Throttled in memory rather than by querying the last row — a SELECT per
 * snapshot would cost more than the insert it saves. The map is bounded by
 * (nodes x sites), which is a handful.
 */
const DECODE_SAMPLE_MIN_INTERVAL_MS = 55_000;
const _lastDecodeSampleAt = new Map<string, number>();

/** Parse anything → integer or null (NaN/Infinity/garbage → null). */
export function safeInt(v: unknown): number | null {
  if (v === null || v === undefined || v === '') return null;
  const n = typeof v === 'number' ? v : Number(v);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function clampReceivedAt(d: Date): Date {
  const t = d instanceof Date ? d.getTime() : NaN;
  if (!Number.isFinite(t)) return new Date();
  if (Math.abs(t - Date.now()) > CLOCK_SANITY_MS) return new Date();
  return d;
}

// ---------------------------------------------------------------------------
// Radio: vce activity events
// ---------------------------------------------------------------------------

/** One decoded activity event as shipped by the agent (already zod-validated
 *  by the /activity route). All shipped events are call-ish by contract, so
 *  every inserted event participates in logical grouping. */
export interface ActivityEventInput {
  /** Agent-side monotonically increasing event id, unique per stream. */
  id: number;
  /** Event time, unix ms (clamped to now±48h like all capture times). */
  atMs: number;
  action: string;
  eventType: string;
  /** Source radio unit id. */
  source: number | null;
  /** Target talkgroup. */
  target: number | null;
  frequencyHz: number | null;
  timeslot: number | null;
  encrypted: boolean;
  rfss: number | null;
  site: number | null;
  nac: number | null;
  wacn: number | null;
  /** P25 systemId — stored in the `system` column. */
  systemId: number | null;
  /** Decoder channel label. Validated but NOT stored: talkgroup labels are
   *  planned to resolve from the global agencies config at read time. */
  channelName: string | null;
  /** Channel's configured P25 system name (e.g. "NSWPSN") — stored in the
   *  `system_label` column so the Data tab can show a friendly name for the
   *  numeric systemId. Optional/null: absent from older agents. */
  systemName?: string | null;
  /** Over-the-air talker alias last captured for the source radio — stored in
   *  the `source_alias` column (migration 046). Optional/null: absent from
   *  older agents. */
  sourceAlias?: string | null;
}

/**
 * Record a batch of activity events for one node/stream. Returns how many
 * were NEWLY inserted (deduped re-sends are skipped silently — the unique
 * (node_id, stream_id, source_event_id) index + ON CONFLICT DO NOTHING
 * makes re-posting a batch idempotent).
 *
 * Batching choice: ONE pooled connection for the whole batch, but a
 * SEPARATE transaction per event. Rationale: the advisory xact lock that
 * serialises logical grouping is per (systemId, target) key and releases
 * at COMMIT — per-event transactions keep each lock held only for its own
 * event's group-find + insert instead of pinning up to 500 keys for the
 * whole batch (deadlock-prone across concurrent nodes), and one bad event
 * can't roll back its siblings. Correctness over batch throughput.
 *
 * Fire-safe: never throws; on per-event failure that event is rolled back,
 * logged once at the end, and the rest of the batch continues.
 */
export async function recordActivityEvents(
  nodeId: string,
  streamId: string,
  events: ActivityEventInput[],
): Promise<number> {
  let accepted = 0;
  try {
    const pool = await getWriterPool();
    if (!pool || events.length === 0) return 0;

    const client = await pool.connect();
    let failures = 0;
    let lastErr: unknown = null;
    try {
      for (const ev of events) {
        const receivedAt = clampReceivedAt(new Date(ev.atMs));
        const system = safeInt(ev.systemId);
        const talkgroup = safeInt(ev.target);
        const sourceUnit = safeInt(ev.source);
        const lockKey = `nrc:${system ?? -1}:${talkgroup ?? -1}`;
        try {
          await client.query('BEGIN');
          // Serialise grouping per (systemId, target) so two nodes shipping
          // the same call concurrently can't both "find no group" and fork.
          await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [lockKey]);

          // Existing logical group within the ±4s window (closest first).
          // The unit check tolerates rows with unknown source_unit on either
          // side; a DIFFERENT known unit means a different call.
          const found = await client.query<{ logical_call_id: string }>(
            `SELECT logical_call_id FROM node_radio_events
              WHERE system IS NOT DISTINCT FROM $1
                AND talkgroup IS NOT DISTINCT FROM $2
                AND received_at BETWEEN $3::timestamptz - interval '${GROUP_WINDOW_SECONDS} seconds'
                                   AND $3::timestamptz + interval '${GROUP_WINDOW_SECONDS} seconds'
                AND ($4::integer IS NULL OR source_unit IS NULL OR source_unit = $4::integer)
                AND logical_call_id IS NOT NULL
              ORDER BY abs(extract(epoch FROM (received_at - $3::timestamptz)))
              LIMIT 1`,
            [system, talkgroup, receivedAt.toISOString(), sourceUnit],
          );
          const existingGroup = found.rows[0]?.logical_call_id ?? null;

          // Dedupe: a re-sent event hits the unique (node_id, stream_id,
          // source_event_id) index → no row returned → skip stamps/buckets.
          const ins = await client.query<{ id: string }>(
            `INSERT INTO node_radio_events
               (node_id, received_at, stream_id, source_event_id,
                action, event_type, system, talkgroup, source_unit,
                frequency, timeslot, encrypted,
                site_rfss, site_id, site_nac, wacn,
                system_label, source_alias)
             VALUES ($1, $2::timestamptz, $3, $4, $5, $6, $7, $8, $9,
                     $10, $11, $12, $13, $14, $15, $16, $17, $18)
             ON CONFLICT (node_id, stream_id, source_event_id) DO NOTHING
             RETURNING id`,
            [
              nodeId,
              receivedAt.toISOString(),
              streamId,
              safeInt(ev.id),
              ev.action,
              ev.eventType,
              system,
              talkgroup,
              sourceUnit,
              safeInt(ev.frequencyHz),
              safeInt(ev.timeslot),
              ev.encrypted === true,
              safeInt(ev.rfss),
              safeInt(ev.site),
              safeInt(ev.nac),
              safeInt(ev.wacn),
              ev.systemName ?? null,
              ev.sourceAlias ?? null,
            ],
          );
          const rowId = ins.rows[0]?.id;
          if (rowId === undefined) {
            // Duplicate re-send — nothing changed, but commit to release
            // the advisory lock cleanly.
            await client.query('COMMIT');
            continue;
          }
          await client.query(
            `UPDATE node_radio_events SET logical_call_id = $1 WHERE id = $2`,
            [existingGroup ?? rowId, rowId],
          );

          // The other half of the ordering fix (migration 070): if this call's
          // audio already arrived and found no row to flag, it is parked —
          // claim it now. DELETE ... RETURNING consumes it in the same
          // transaction, so one upload can never flag two calls, and a
          // rollback puts it back for the next attempt.
          //
          // Same ranking as markRecorded: exact frequency, then exact calling
          // unit, then nearest in time. Deliberately identical, because the
          // two sides are choosing between the same candidates from opposite
          // directions and must not disagree about which pairing is right.
          const claimed = await client.query<{ audio_bytes: string }>(
            `DELETE FROM node_pending_recordings
              WHERE id = (
                SELECT id FROM node_pending_recordings
                 WHERE node_id = $1
                   AND talkgroup IS NOT DISTINCT FROM $2
                   AND started_at BETWEEN $3::timestamptz - interval '${RECORDED_WINDOW_SECONDS} seconds'
                                      AND $3::timestamptz + interval '${RECORDED_WINDOW_SECONDS} seconds'
                 ORDER BY COALESCE(frequency IS NOT NULL AND frequency = $4::bigint, false) DESC,
                          COALESCE(source_unit IS NOT NULL AND source_unit = $5::integer, false) DESC,
                          abs(extract(epoch FROM (started_at - $3::timestamptz)))
                 LIMIT 1
              )
              RETURNING audio_bytes`,
            [
              nodeId,
              talkgroup,
              receivedAt.toISOString(),
              safeInt(ev.frequencyHz),
              sourceUnit,
            ],
          );
          const claimedBytes = claimed.rows[0] ? Number(claimed.rows[0].audio_bytes) || 0 : null;
          if (claimedBytes !== null) {
            await client.query(
              `UPDATE node_radio_events SET recorded = true, audio_bytes = $1 WHERE id = $2`,
              [claimedBytes, rowId],
            );
          }

          const isNewGroup = existingGroup === null;
          // Per-node forever bucket. Bytes are normally 0 here and folded in
          // later by markRecorded when the upload lands — except when we just
          // claimed a parked upload above, in which case they are already
          // known and must be counted here or they are lost entirely (the
          // pending row is gone, so nothing will add them later).
          await client.query(
            `INSERT INTO node_radio_hourly (hour, node_id, system, talkgroup, calls, audio_bytes)
             VALUES (date_trunc('hour', $1::timestamptz), $2, $3, $4, 1, $5)
             ON CONFLICT (hour, node_id, system, talkgroup) DO UPDATE
               SET calls = node_radio_hourly.calls + 1,
                   audio_bytes = node_radio_hourly.audio_bytes + EXCLUDED.audio_bytes`,
            [receivedAt.toISOString(), nodeId, system ?? 0, talkgroup ?? 0, claimedBytes ?? 0],
          );
          // Network-wide forever bucket. logical_calls +1 only when this row
          // STARTED a group (each over-the-air call counted once).
          await client.query(
            `INSERT INTO node_radio_hourly_sys
               (hour, system, talkgroup, site_rfss, site_id, calls, logical_calls)
             VALUES (date_trunc('hour', $1::timestamptz), $2, $3, $4, $5, 1, $6)
             ON CONFLICT (hour, system, talkgroup, site_rfss, site_id) DO UPDATE
               SET calls = node_radio_hourly_sys.calls + 1,
                   logical_calls = node_radio_hourly_sys.logical_calls + EXCLUDED.logical_calls`,
            [
              receivedAt.toISOString(),
              system ?? 0,
              talkgroup ?? 0,
              safeInt(ev.rfss) ?? -1,
              safeInt(ev.site) ?? -1,
              isNewGroup ? 1 : 0,
            ],
          );

          await client.query('COMMIT');
          accepted += 1;
        } catch (err) {
          await client.query('ROLLBACK').catch(() => {});
          failures += 1;
          lastErr = err;
        }
      }
    } finally {
      client.release();
    }
    if (failures > 0) {
      log.warn(
        {
          err: (lastErr as Error)?.message,
          failures,
          node: nodeId.slice(0, 8),
        },
        'nodeEvents: recordActivityEvents partial failure',
      );
    }
  } catch (err) {
    log.warn(
      { err: (err as Error).message, node: nodeId.slice(0, 8) },
      'nodeEvents: recordActivityEvents failed',
    );
  }
  return accepted;
}

/**
 * Mark the closest activity-event row for (node, talkgroup) within
 * ±RECORDED_WINDOW_SECONDS of the rdio call upload's start timestamp as
 * recorded (audio exists in central
 * rdio) and stamp its audio size. Only rows with recorded=false are
 * eligible, so N uploads consume N distinct event rows. No-op (silently)
 * when nothing matches — e.g. the agent hasn't shipped that event yet or
 * the call predates the activity stream.
 *
 * Also folds the audio bytes into the matched row's node_radio_hourly
 * bucket (the event insert bucketed 0 bytes since audio size is only known
 * at upload time) so the forever per-node byte rollup stays truthful.
 */
export async function markRecorded(
  nodeId: string,
  talkgroup: number | null,
  atDate: Date,
  audioBytes: number,
  /** Calling radio id (rdio form field `source`) — vce sends it with every
   *  upload. Two back-to-back calls on one talkgroup are almost always
   *  DIFFERENT units, so this is what stops an upload claiming a neighbour's
   *  row once the time window is wide enough to cover the real skew. */
  sourceUnit: number | null = null,
  /** Traffic-channel frequency in Hz (rdio form field `frequency`). Two calls
   *  seconds apart are on different traffic channels, so this pins the exact
   *  reception even when the same unit keys up twice. */
  frequencyHz: number | null = null,
  /** The radio's OVER-THE-AIR alias (rdio form field `talkerAlias`), when the
   *  radio transmitted one.
   *
   *  This is the only path it reaches us on. The activity-event feed carries a
   *  `sourceAlias`, but that is resolved through vce's trunked identity tables
   *  and arrives null in practice, which left every radio unnamed. vce puts the
   *  alias straight onto the audio upload next to `source` and `talkgroup`
   *  (RdioScannerBroadcaster: FormField.TALKER_ALIAS) — the same form we were
   *  already parsing for the recorded flag, and simply discarding this field
   *  from. */
  talkerAlias: string | null = null,
): Promise<void> {
  try {
    const pool = await getWriterPool();
    if (!pool) return;
    const at = clampReceivedAt(atDate);
    const tg = safeInt(talkgroup);
    const unit = safeInt(sourceUnit);
    const freq = safeInt(frequencyHz);
    const bytes = Math.max(0, safeInt(audioBytes) ?? 0);
    // Over-the-air text, so it is attacker-influenced in principle and blank
    // when the radio sent no alias. Trim, drop empties, and bound it to the
    // column rather than trusting the length.
    const alias = ((): string | null => {
      const s = (talkerAlias ?? '').trim();
      return s === '' ? null : s.slice(0, 64);
    })();

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const upd = await client.query<{
        received_at: Date;
        system: number | null;
        talkgroup: number | null;
      }>(
        // Frequency and unit RANK candidates; they never exclude any. That
        // distinction was learned the hard way — as WHERE clauses they cost
        // more matches than they saved (measured 76 misses against 71 hits,
        // nearly all with an eligible unrecorded row well inside the window).
        // One call heard at several sites has a DIFFERENT traffic frequency
        // per site while the upload carries only the recording site's, so a
        // hard frequency predicate throws away the very rows it should be
        // choosing between.
        //
        // As a ranking it is all upside: an exact frequency match wins, then
        // an exact unit match, then proximity in time — and when the upload
        // carries neither (older agent) this degrades cleanly to the original
        // nearest-in-time behaviour. A slightly mis-attributed flag within one
        // talkgroup and ten seconds beats a missing one; a miss is invisible
        // and permanent.
        //
        // The COALESCE is load-bearing, not decoration: `frequency = $5` is
        // NULL (not false) when the ROW's frequency is null, and DESC sorts
        // NULLS FIRST in Postgres — so without it a row that knows nothing
        // would outrank the exact match this whole change is built on.
        // source_alias is filled in only when it is still empty: the alias is a
        // property of the RADIO, so the first sighting is as good as any, and
        // never overwriting means a later call that happened to carry no alias
        // cannot blank one we already know.
        `UPDATE node_radio_events SET recorded = true, audio_bytes = $4,
                source_alias = COALESCE(source_alias, $7::text)
          WHERE id = (
            SELECT id FROM node_radio_events
             WHERE node_id = $1
               AND talkgroup IS NOT DISTINCT FROM $2
               AND recorded = false
               AND received_at BETWEEN $3::timestamptz - interval '${RECORDED_OUTER_SECONDS} seconds'
                                  AND $3::timestamptz + interval '${RECORDED_OUTER_SECONDS} seconds'
               AND (
                 -- Close in time: trust the clock, as before.
                 abs(extract(epoch FROM (received_at - $3::timestamptz)))
                   <= ${RECORDED_WINDOW_SECONDS}
                 -- Further out: only on a full identity match. Talkgroup is
                 -- already fixed above, so agreeing on BOTH the calling unit
                 -- and the traffic channel is the same call by any reasonable
                 -- reading — and it is precisely what the clock cannot tell us.
                 OR ($5::bigint IS NOT NULL AND frequency = $5::bigint
                     AND $6::integer IS NOT NULL AND source_unit = $6::integer)
               )
             ORDER BY COALESCE($5::bigint IS NOT NULL AND frequency = $5::bigint, false) DESC,
                      COALESCE($6::integer IS NOT NULL AND source_unit = $6::integer, false) DESC,
                      abs(extract(epoch FROM (received_at - $3::timestamptz)))
             LIMIT 1
          )
          RETURNING received_at, system, talkgroup`,
        [nodeId, tg, at.toISOString(), bytes, freq, unit, alias],
      );
      const row = upd.rows[0];

      // No event row yet — park the upload so the event can claim it when it
      // arrives. The two feeds race (audio closes in ~1-2s on a short over,
      // activity events ship on a 3-5s tick) and this used to be a one-shot
      // give-up, which permanently lost the flag for exactly the calls that
      // were quickest to upload. See migration 070.
      if (!row) {
        await client.query(
          `INSERT INTO node_pending_recordings
             (node_id, talkgroup, source_unit, frequency, started_at, audio_bytes)
           VALUES ($1, $2, $3, $4, $5::timestamptz, $6)`,
          [nodeId, tg, unit, freq, at.toISOString(), bytes],
        );
      }

      // Diagnostic (see RECORDED_DIAG): this used to no-op silently on a miss,
      // which made "why do most calls show no recorded icon?" unanswerable —
      // an upload that never arrived and one that arrived but matched nothing
      // looked identical. On a HIT we log the skew between the upload's start
      // timestamp and the event's, which is the only way to size the match
      // window from measurements rather than guesswork.
      if (RECORDED_DIAG) {
        if (row) {
          const skewMs = row.received_at instanceof Date
            ? row.received_at.getTime() - at.getTime()
            : null;
          log.info(
            { node: nodeId.slice(0, 8), tg, unit, freq, skewMs, bytes },
            'nodeEvents: markRecorded HIT',
          );
        } else {
          // How many events exist for this (node, talkgroup) in a much WIDER
          // window, and how close the nearest one is: distinguishes "the event
          // is there but outside the window / already claimed" from "no event
          // for this call at all" (i.e. the upload beat the activity stream,
          // or the call was never decoded on this node).
          const near = await client.query<{ n: string; nearest_ms: string | null; unrecorded: string }>(
            `SELECT COUNT(*)::text AS n,
                    MIN(abs(extract(epoch FROM (received_at - $3::timestamptz)) * 1000))::text AS nearest_ms,
                    (COUNT(*) FILTER (WHERE recorded = false))::text AS unrecorded
               FROM node_radio_events
              WHERE node_id = $1
                AND talkgroup IS NOT DISTINCT FROM $2
                AND received_at BETWEEN $3::timestamptz - interval '60 seconds'
                                   AND $3::timestamptz + interval '60 seconds'`,
            [nodeId, tg, at.toISOString()],
          );
          const n = near.rows[0];
          log.info(
            {
              node: nodeId.slice(0, 8),
              tg,
              unit,
              freq,
              bytes,
              within60s: Number(n?.n ?? 0),
              unrecorded60s: Number(n?.unrecorded ?? 0),
              nearestMs: n?.nearest_ms !== null && n?.nearest_ms !== undefined ? Math.round(Number(n.nearest_ms)) : null,
              windowSec: RECORDED_WINDOW_SECONDS,
            },
            'nodeEvents: markRecorded MISS',
          );
        }
      }

      if (row && bytes > 0) {
        await client.query(
          `INSERT INTO node_radio_hourly (hour, node_id, system, talkgroup, calls, audio_bytes)
           VALUES (date_trunc('hour', $1::timestamptz), $2, $3, $4, 0, $5)
           ON CONFLICT (hour, node_id, system, talkgroup) DO UPDATE
             SET audio_bytes = node_radio_hourly.audio_bytes + EXCLUDED.audio_bytes`,
          [
            row.received_at instanceof Date ? row.received_at.toISOString() : row.received_at,
            nodeId,
            row.system ?? 0,
            row.talkgroup ?? 0,
            bytes,
          ],
        );
      }
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    log.warn(
      { err: (err as Error).message, node: nodeId.slice(0, 8) },
      'nodeEvents: markRecorded failed',
    );
  }
}

// ---------------------------------------------------------------------------
// Radio: deep P25 site metadata (migration 047)
// ---------------------------------------------------------------------------

/** One P25 site's deep metadata, as shipped by the agent from the sdrtrunk-vce
 *  GET /site/snapshots feed (already zod-validated by the site-snapshots route).
 *  The nested lists/objects are stored verbatim as JSONB — they are read whole
 *  by the site drill-down, never queried column-wise. */
export interface SiteSnapshotInput {
  /** Numeric P25 systemId (null → -1 "unknown" in the natural key). */
  systemId: number | null;
  /** P25 RFSS (null → -1 "unknown" in the natural key). */
  rfss: number | null;
  /** P25 site id (always present for a known site). */
  siteId: number | null;
  guid: string | null;
  systemName: string | null;
  wacn: number | null;
  nac: number | null;
  lra: number | null;
  channelName: string | null;
  controlFrequencyMhz: number | null;
  controlLcn: string | null;
  affiliatedRadioCount: number | null;
  observationCount: number | null;
  firstSeenMs: number | null;
  lastSeenMs: number | null;
  status: unknown;
  channels: unknown;
  neighbors: unknown;
  bands: unknown;
  patches: unknown;
  quality: unknown;
}

/**
 * Upsert a batch of P25 site snapshots for one node. Idempotent by contract:
 * re-POSTing the same sites UPSERTs on the natural key
 * (node_id, system_id, rfss, site_id) — one row per (node, physical site),
 * never duplicated. Returns how many site rows were written (inserted or
 * updated).
 *
 * Key columns can't be NULL, but vce may not resolve systemId/rfss for a
 * site, so both are coalesced to -1 ("unknown"), the same sentinel
 * node_radio_hourly_sys uses. A site with no resolvable site id is skipped
 * (a "known site" always has one).
 *
 * Fire-safe: never throws; a per-site failure is logged and the rest of the
 * batch continues.
 */
export async function upsertSiteSnapshots(
  nodeId: string,
  sites: SiteSnapshotInput[],
): Promise<number> {
  let written = 0;
  try {
    const pool = await getWriterPool();
    if (!pool || sites.length === 0) return 0;

    const client = await pool.connect();
    let failures = 0;
    let lastErr: unknown = null;
    try {
      for (const s of sites) {
        const siteId = safeInt(s.siteId);
        if (siteId === null) continue; // not a known site — skip
        const systemId = safeInt(s.systemId) ?? -1;
        const rfss = safeInt(s.rfss) ?? -1;
        try {
          const res = await client.query(
            `INSERT INTO node_site_snapshots
               (node_id, system_id, rfss, site_id, guid, system_name, wacn, nac, lra,
                channel_name, control_frequency_mhz, control_lcn, affiliated_radio_count,
                observation_count, site_first_seen_ms, site_last_seen_ms,
                status, channels, neighbors, bands, patches, quality, received_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                     $10, $11, $12, $13, $14, $15, $16,
                     $17::jsonb, $18::jsonb, $19::jsonb, $20::jsonb, $21::jsonb, $22::jsonb, now())
             ON CONFLICT (node_id, system_id, rfss, site_id) DO UPDATE SET
               guid = EXCLUDED.guid,
               system_name = EXCLUDED.system_name,
               wacn = EXCLUDED.wacn,
               nac = EXCLUDED.nac,
               lra = EXCLUDED.lra,
               channel_name = EXCLUDED.channel_name,
               control_frequency_mhz = EXCLUDED.control_frequency_mhz,
               control_lcn = EXCLUDED.control_lcn,
               affiliated_radio_count = EXCLUDED.affiliated_radio_count,
               observation_count = EXCLUDED.observation_count,
               site_first_seen_ms = EXCLUDED.site_first_seen_ms,
               site_last_seen_ms = EXCLUDED.site_last_seen_ms,
               status = EXCLUDED.status,
               channels = EXCLUDED.channels,
               neighbors = EXCLUDED.neighbors,
               bands = EXCLUDED.bands,
               patches = EXCLUDED.patches,
               quality = EXCLUDED.quality,
               received_at = now()`,
            [
              nodeId,
              systemId,
              rfss,
              siteId,
              s.guid ?? null,
              s.systemName ?? null,
              safeInt(s.wacn),
              safeInt(s.nac),
              safeInt(s.lra),
              s.channelName ?? null,
              typeof s.controlFrequencyMhz === 'number' && Number.isFinite(s.controlFrequencyMhz)
                ? s.controlFrequencyMhz
                : null,
              s.controlLcn ?? null,
              safeInt(s.affiliatedRadioCount),
              safeInt(s.observationCount),
              safeInt(s.firstSeenMs),
              safeInt(s.lastSeenMs),
              s.status != null ? JSON.stringify(s.status) : null,
              JSON.stringify(Array.isArray(s.channels) ? s.channels : []),
              JSON.stringify(Array.isArray(s.neighbors) ? s.neighbors : []),
              JSON.stringify(Array.isArray(s.bands) ? s.bands : []),
              JSON.stringify(Array.isArray(s.patches) ? s.patches : []),
              s.quality != null ? JSON.stringify(s.quality) : null,
            ],
          );
          written += res.rowCount ?? 0;

          // Append a decode-health sample. The snapshot row above is an upsert
          // and keeps only the CURRENT reading, so without this there is no
          // history to chart. Skipped entirely when the node reports no
          // quality object — a NULL-only row would draw a gap as if decode had
          // failed, when it only means the runtime didn't report.
          const q = (s.quality ?? null) as Record<string, unknown> | null;
          if (q) {
            const dbl = (v: unknown): number | null => {
              const n = Number(v);
              return Number.isFinite(n) ? n : null;
            };
            // 0 is "not measured", not "0% decode" / "0 dBFS". A sample is written
            // when EITHER field is present, so a node reporting only signal would
            // otherwise store a real decode_pct of 0 — which reads back as a site
            // decoding at 0% and drags every average and minimum taken over it.
            const zeroIsAbsent = (v: number | null): number | null => (v === 0 ? null : v);
            const decodePct = zeroIsAbsent(dbl(q['decodeHealthPct']));
            const signalDbfs = zeroIsAbsent(dbl(q['signalDbfs']));
            const invalid = dbl(q['invalidFrames']);
            // Throttle: see DECODE_SAMPLE_MIN_INTERVAL_MS. Keyed per node+site so a
            // busy site cannot starve a quiet one.
            const sampleKey = `${nodeId}:${systemId}:${rfss}:${siteId}`;
            const lastSampleAt = _lastDecodeSampleAt.get(sampleKey) ?? 0;
            const sampleDue = Date.now() - lastSampleAt >= DECODE_SAMPLE_MIN_INTERVAL_MS;
            if (sampleDue && (decodePct !== null || signalDbfs !== null)) {
              _lastDecodeSampleAt.set(sampleKey, Date.now());
              await client.query(
                `INSERT INTO node_site_decode_samples
                   (node_id, system_id, rfss, site_id, decode_pct, signal_dbfs, invalid_frames)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
                [nodeId, systemId, rfss, siteId, decodePct, signalDbfs,
                 invalid !== null ? Math.round(invalid) : null],
              );
            }
          }
        } catch (err) {
          failures += 1;
          lastErr = err;
        }
      }
    } finally {
      client.release();
    }
    if (failures > 0) {
      log.warn(
        { err: (lastErr as Error)?.message, failures, node: nodeId.slice(0, 8) },
        'nodeEvents: upsertSiteSnapshots partial failure',
      );
    }
  } catch (err) {
    log.warn(
      { err: (err as Error).message, node: nodeId.slice(0, 8) },
      'nodeEvents: upsertSiteSnapshots failed',
    );
  }
  return written;
}

// ---------------------------------------------------------------------------
// Pager (unchanged)
// ---------------------------------------------------------------------------

export interface PagerEventInput {
  nodeId: string;
  receivedAt: Date;
  capcode: string;
  function: number | null;
  freqMhz: number | null;
  message: string;
}

/**
 * Record one pager message reception: advisory-lock the (capcode,
 * message_hash) key, find a ±4s group, insert, stamp logical_id, bump
 * node_pager_hourly.
 */
export async function recordPagerEvent(ev: PagerEventInput): Promise<void> {
  try {
    const pool = await getWriterPool();
    if (!pool) return;

    const receivedAt = clampReceivedAt(ev.receivedAt);
    const message = typeof ev.message === 'string' ? ev.message : '';
    const messageHash = createHash('sha256').update(message.trim()).digest('hex');
    const fn = safeInt(ev.function);
    const freqMhz =
      typeof ev.freqMhz === 'number' && Number.isFinite(ev.freqMhz) ? ev.freqMhz : null;
    const lockKey = `npc:${ev.capcode}:${messageHash}`;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [lockKey]);

      const found = await client.query<{ logical_id: string }>(
        `SELECT logical_id FROM node_pager_events
          WHERE capcode = $1
            AND message_hash = $2
            AND received_at BETWEEN $3::timestamptz - interval '${GROUP_WINDOW_SECONDS} seconds'
                               AND $3::timestamptz + interval '${GROUP_WINDOW_SECONDS} seconds'
            AND logical_id IS NOT NULL
          ORDER BY abs(extract(epoch FROM (received_at - $3::timestamptz)))
          LIMIT 1`,
        [ev.capcode, messageHash, receivedAt.toISOString()],
      );
      const existingGroup = found.rows[0]?.logical_id ?? null;

      const ins = await client.query<{ id: string }>(
        `INSERT INTO node_pager_events
           (node_id, received_at, capcode, "function", freq_mhz, message, message_hash)
         VALUES ($1, $2::timestamptz, $3, $4, $5, $6, $7)
         RETURNING id`,
        [ev.nodeId, receivedAt.toISOString(), ev.capcode, fn, freqMhz, message, messageHash],
      );
      const rowId = ins.rows[0]!.id;
      await client.query(
        `UPDATE node_pager_events SET logical_id = $1 WHERE id = $2`,
        [existingGroup ?? rowId, rowId],
      );

      const isNewGroup = existingGroup === null;
      await client.query(
        `INSERT INTO node_pager_hourly (hour, node_id, capcode, pages, logical_pages)
         VALUES (date_trunc('hour', $1::timestamptz), $2, $3, 1, $4)
         ON CONFLICT (hour, node_id, capcode) DO UPDATE
           SET pages = node_pager_hourly.pages + 1,
               logical_pages = node_pager_hourly.logical_pages + EXCLUDED.logical_pages`,
        [receivedAt.toISOString(), ev.nodeId, ev.capcode, isNewGroup ? 1 : 0],
      );

      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    log.warn(
      { err: (err as Error).message, node: ev.nodeId.slice(0, 8) },
      'nodeEvents: recordPagerEvent failed',
    );
  }
}
