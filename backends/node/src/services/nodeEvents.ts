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
 * arrives as N events. Rows within ±5s on the same (system, talkgroup)
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
import { getPool, getWriterPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { config } from '../config.js';
import { rdioPatches, groupingTalkgroup } from './rdioPatches.js';

/**
 * How far apart two receptions of ONE transmission may be stamped.
 *
 * Five seconds because that is vce's own answer to the same question:
 * CrossSiteCallDeduplicator.SAME_CALL_WINDOW_MILLISECONDS = 5_000, applied to
 * a key of scope + target + source radio, which is the key used here too. That
 * class exists because several sites of one system observe the same call, and
 * it counts the logical call once. It cannot help us — it is per vce instance,
 * and it gates the SUMMARY buckets, not the detailed activity rows we consume
 * — but its window is a measured property of this decoder, so matching it
 * means our idea of "the same call, heard again" is vce's idea of it.
 *
 * One deliberate difference: vce re-anchors when the OWNING context re-observes
 * a call, so a long call's later reports extend its window. We do not. Our
 * observers are separate nodes rather than sites inside one instance, and
 * re-anchoring per node is exactly the chaining this window exists to stop.
 */
const GROUP_WINDOW_SECONDS = 5;

/**
 * The candidate logical call a new reception may join. Shared by both radio
 * ingest paths so the activity feed and the scanner feed group identically.
 *
 * TWO RULES, and both exist because of measured damage:
 *
 * 1. THE WINDOW IS ANCHORED AT THE GROUP'S FIRST ROW, not at its nearest one.
 *    Matching "within 4s of any member" chains: call A ends, call B starts 3s
 *    later and joins A, call C 3s after B joins them both, and a busy talkgroup
 *    grows one group without limit. Measured over 6h of production before this
 *    was fixed: 96 groups spanning more than 30s, the worst 427 receptions over
 *    2m10s filed as a single call. `logical_call_id` IS the id of the group's
 *    first row, so the anchor is a plain join, and a group can now span at most
 *    2x the window.
 *
 * 2. ONE SITE CANNOT CARRY ONE TRANSMISSION ON TWO CHANNELS. A transmission is
 *    granted a single traffic channel per site, so a candidate group that
 *    already holds a reception from THIS site on a DIFFERENT frequency is a
 *    different transmission, whatever the clock says.
 *
 *    Site-scoped, deliberately: the same transmission simulcast from two sites
 *    arrives on two different frequencies, and one node can hear several sites.
 *    So the LCN may only ever SPLIT a group, never merge one — comparing
 *    frequencies across sites would tear every multi-site call in half.
 *
 * Params (identical in both callers): $1 system, $2 candidate talkgroups,
 * $3 this reception's time, $4 source unit, $5 site rfss, $6 site id,
 * $7 frequency. `systemCond` differs only because the scanner feed tolerates a
 * null system on either side while a node event does not.
 */
function groupLookupSql(systemCond: string): string {
  return `SELECT c.logical_call_id
            FROM node_radio_events c
            JOIN node_radio_events anchor ON anchor.id = c.logical_call_id
           WHERE ${systemCond}
             AND c.talkgroup = ANY($2::int[])
             AND c.received_at BETWEEN $3::timestamptz - interval '${GROUP_WINDOW_SECONDS} seconds'
                                   AND $3::timestamptz + interval '${GROUP_WINDOW_SECONDS} seconds'
             AND anchor.received_at BETWEEN $3::timestamptz - interval '${GROUP_WINDOW_SECONDS} seconds'
                                        AND $3::timestamptz + interval '${GROUP_WINDOW_SECONDS} seconds'
             AND ($4::integer IS NULL OR c.source_unit IS NULL OR c.source_unit = $4::integer)
             AND c.logical_call_id IS NOT NULL
             AND NOT EXISTS (
                   SELECT 1
                     FROM node_radio_events x
                    WHERE x.logical_call_id = c.logical_call_id
                      AND $5::integer IS NOT NULL
                      AND $6::integer IS NOT NULL
                      AND $7::bigint IS NOT NULL
                      AND x.site_rfss = $5::integer
                      AND x.site_id = $6::integer
                      AND x.frequency IS NOT NULL
                      AND x.frequency <> $7::bigint)
           ORDER BY abs(extract(epoch FROM (c.received_at - $3::timestamptz)))
           LIMIT 1`;
}

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

/**
 * The patch members worth storing for one reception.
 *
 * Null passes through as null — a control server that does not report members
 * is saying "unknown", which is a different fact from a call having none, and
 * the column keeps that distinction. An empty result also stores as null: a
 * patch of nothing is not a patch, and a stored empty array would claim the
 * question was answered.
 */
export function patchMembersOf(
  members: number[] | null | undefined,
  talkgroup: number | null,
): number[] | null {
  if (members === null || members === undefined) return null;
  const out: number[] = [];
  for (const m of members) {
    if (!Number.isInteger(m) || m <= 0) continue;
    if (talkgroup !== null && m === talkgroup) continue;
    if (!out.includes(m)) out.push(m);
  }
  return out.length > 0 ? out.sort((a, b) => a - b) : null;
}

/** Parse anything → integer or null (NaN/Infinity/garbage → null). */
export function safeInt(v: unknown): number | null {
  if (v === null || v === undefined || v === '') return null;
  const n = typeof v === 'number' ? v : Number(v);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

/**
 * Is this talker alias an actual NAME, or just the radio's own id echoed back?
 *
 * When a radio transmits no alias, the decoder frequently reports its id in the
 * alias field — so "2072676" arrives as the OTA for radio 2072676. Recording it
 * makes a radio look named when it has never announced itself, and the Data tab
 * then shows the same number three times over (UID, OTA, Alias). An alias that
 * is nothing but the id it belongs to carries no information, so it is not an
 * alias. Other numeric strings are kept — a radio may legitimately be named
 * something numeric that isn't its own id.
 */
export function isRealTalkerAlias(alias: string, radioId: number | null): boolean {
  const s = alias.trim();
  if (s === '') return false;
  if (radioId === null) return true;
  // Compare numerically so a zero-padded echo ("0200307" for 200307) is caught.
  return !(/^\d+$/.test(s) && Number(s) === radioId);
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
  /**
   * The talkgroups patched into this call — stored in `patch_members`
   * (migration 078).
   *
   * A patched transmission carries the PATCH GROUP as its `target`, so without
   * these the real channels carrying the conversation are unknowable. Null
   * from a control server that does not report them, which is a DIFFERENT fact
   * from an empty list on a call that simply is not patched.
   */
  patchMembers?: number[] | null;
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
): Promise<{ accepted: number; failed: number }> {
  let accepted = 0;
  let failed = 0;
  try {
    const pool = await getWriterPool();
    if (events.length === 0) return { accepted: 0, failed: 0 };
    // No writer pool = nothing was attempted; the whole batch failed and the
    // route must refuse the ack so the agent retries.
    if (!pool) return { accepted: 0, failed: events.length };

    // Once per batch, not per event: it is a ~60s-cached read of a two-row
    // table, and an empty lookup (central rdio down or unconfigured) simply
    // means every talkgroup groups as itself, exactly as before patches.
    const patches = await rdioPatches();

    const client = await pool.connect();
    let failures = 0;
    let lastErr: unknown = null;
    try {
      for (const ev of events) {
        const receivedAt = clampReceivedAt(new Date(ev.atMs));
        const system = safeInt(ev.systemId);
        const talkgroup = safeInt(ev.target);
        const sourceUnit = safeInt(ev.source);
        // PATCH GROUPING. A patch is several talkgroups carrying ONE
        // conversation, so the same transmission arrives once per member and
        // would otherwise open a rival logical call on each. Group on the
        // patch's highest-ranked member — the talkgroup rdio files the
        // surviving call under — so a patched transmission is one call here and
        // one call there. A non-member resolves to itself, unchanged.
        const groupTg = talkgroup === null ? null : groupingTalkgroup(patches, talkgroup);
        // Every talkgroup whose receptions may join this group: the patch's
        // members, or just this talkgroup.
        const groupMembers =
          talkgroup === null
            ? []
            : (patches.byTalkgroup.get(talkgroup)?.talkgroups ?? [talkgroup]);
        const lockKey = `nrc:${system ?? -1}:${groupTg ?? -1}`;
        try {
          await client.query('BEGIN');
          // Serialise grouping per (systemId, target) so two nodes shipping
          // the same call concurrently can't both "find no group" and fork.
          await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [lockKey]);

          // Existing logical group within the ±5s window (closest first).
          // The unit check tolerates rows with unknown source_unit on either
          // side; a DIFFERENT known unit means a different call.
          const found = await client.query<{ logical_call_id: string }>(
            groupLookupSql('c.system IS NOT DISTINCT FROM $1::integer'),
            // For a patch member this is EVERY member talkgroup, so a copy that
            // arrived on a sibling channel is found and joined. For anything
            // else it is the single talkgroup, exactly as before.
            [
              system,
              groupMembers,
              receivedAt.toISOString(),
              sourceUnit,
              safeInt(ev.rfss),
              safeInt(ev.site),
              safeInt(ev.frequencyHz),
            ],
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
                system_label, source_alias, patch_members)
             VALUES ($1, $2::timestamptz, $3, $4, $5, $6, $7, $8, $9,
                     $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
             ON CONFLICT (node_id, stream_id, source_event_id) DO NOTHING
             RETURNING id`,
            [
              nodeId,
              receivedAt.toISOString(),
              streamId,
              safeInt(ev.id),
              // Normalised on the way in so the read path can match them
              // directly. vce sends Java enum names, which are uppercase by
              // convention rather than by contract, and every rollup filters
              // on event_type — wrapping the COLUMN in upper() to be safe
              // costs a function call on every row of the window and, worse,
              // hides the column's statistics from the planner. Normalising
              // the handful of bytes here buys both back.
              ev.action.toUpperCase(),
              ev.eventType.toUpperCase(),
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
              // Never the talkgroup itself: the call is already filed there,
              // and listing it as one of its own patch members would read as
              // the transmission going out on it twice.
              patchMembersOf(ev.patchMembers, talkgroup),
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

          // Remember the radio's over-the-air name permanently. This is where
          // aliases actually arrive — nearly all of them ride patch calls —
          // and node_radio_events is pruned at 30 days, so without a durable
          // copy a radio that goes quiet for a month becomes an anonymous
          // number again. See migration 072.
          const evAlias = ((): string | null => {
            const s = (ev.sourceAlias ?? '').trim();
            if (s === '' || !isRealTalkerAlias(s, sourceUnit)) return null;
            return s.slice(0, 64);
          })();
          if (evAlias && sourceUnit !== null) {
            await client.query(
              `INSERT INTO node_radio_aliases (system, radio_id, alias)
               VALUES ($1, $2, $3)
               ON CONFLICT (system, radio_id) DO UPDATE
                 SET alias = EXCLUDED.alias,
                     last_seen = now(),
                     times_seen = node_radio_aliases.times_seen + 1`,
              [system ?? 0, sourceUnit, evAlias],
            );
          }

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
          //
          // That includes the patch widening: groupMembers is the same set
          // markRecorded builds from rdioPatches, so a parked upload filed
          // under one member is claimable by a reception announced on another.
          const claimed = await client.query<{ audio_bytes: string }>(
            `DELETE FROM node_pending_recordings
              WHERE id = (
                SELECT id FROM node_pending_recordings
                 WHERE node_id = $1
                   AND ($2::int[] IS NULL OR talkgroup = ANY($2::int[]))
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
              groupMembers.length > 0 ? groupMembers : null,
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
    failed = failures;
  } catch (err) {
    // A failure this early means NO event in the batch was attempted — the
    // pool was unavailable or the connection never opened. Report the whole
    // batch failed so the route refuses the ack and the agent retries, rather
    // than the events silently ceasing to exist.
    failed = events.length;
    log.warn(
      { err: (err as Error).message, node: nodeId.slice(0, 8) },
      'nodeEvents: recordActivityEvents failed',
    );
  }
  return { accepted, failed };
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
 * The hourly rollups need no help from here. They used to be bumped per
 * event, so a late audio size had to be folded into the bucket by hand; since
 * migration 080 they are DERIVED, recomputed from these rows, and
 * nodeHourlyRollup re-derives the last few hours on every pass precisely
 * because an upload can land after the hour it belongs to has closed.
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
   *  radio transmitted one. vce puts it straight onto the audio upload next to
   *  `source` and `talkgroup` (RdioScannerBroadcaster: FormField.TALKER_ALIAS).
   *
   *  SECOND of two paths, kept as a fallback. This was once the only one that
   *  worked — the activity feed's `sourceAlias` resolves through vce's trunked
   *  identity tables and used to arrive null, leaving every radio unnamed. That
   *  is no longer true: measured over 30 days the feed named all 306 radios we
   *  know, the upload contributed no name the feed had missed, and the two
   *  never disagreed. So this path currently adds nothing — but it costs a
   *  single upsert on a form we already parse, and it is the only source left
   *  if the identity join ever regresses, so it stays. */
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

    // The talkgroups this upload may claim: the one it names, plus every other
    // member of its patch.
    //
    // rdio files a patched call under the highest-ranked member that actually
    // received a copy (Patch.homeRank), while our reception rows keep whichever
    // talkgroup each site announced it on. Those disagree constantly, and an
    // exact `talkgroup = $2` then matched nothing: measured over 24h, rdio held
    // 202 calls on TG 20201 against 7 we had flagged, and 1,240 uploads
    // network-wide found no row at all. Widening to the patch lets the upload
    // claim the reception it belongs to whichever member either side picked.
    //
    // Deliberately NOT widened to the whole system: an unpatched talkgroup
    // still resolves to exactly itself, so ordinary traffic is unaffected.
    const patches = await rdioPatches();
    const tgCandidates =
      tg === null ? [] : (patches.byTalkgroup.get(tg)?.talkgroups ?? [tg]);

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
               AND ($2::int[] IS NULL OR talkgroup = ANY($2::int[]))
               AND recorded = false
               AND received_at BETWEEN $3::timestamptz - interval '${RECORDED_OUTER_SECONDS} seconds'
                                  AND $3::timestamptz + interval '${RECORDED_OUTER_SECONDS} seconds'
               AND (
                 -- Close in time: trust the clock, as before.
                 abs(extract(epoch FROM (received_at - $3::timestamptz)))
                   <= ${RECORDED_WINDOW_SECONDS}
                 -- Further out: only on a full identity match. Talkgroup is
                 -- already constrained above, so agreeing on BOTH the calling
                 -- unit and the traffic channel is the same call by any
                 -- reasonable reading — and it is precisely what the clock
                 -- cannot tell us.
                 OR ($5::bigint IS NOT NULL AND frequency = $5::bigint
                     AND $6::integer IS NOT NULL AND source_unit = $6::integer)
               )
             ORDER BY COALESCE($5::bigint IS NOT NULL AND frequency = $5::bigint, false) DESC,
                      COALESCE($6::integer IS NOT NULL AND source_unit = $6::integer, false) DESC,
                      abs(extract(epoch FROM (received_at - $3::timestamptz)))
             LIMIT 1
          )
          RETURNING received_at, system, talkgroup`,
        [nodeId, tgCandidates.length > 0 ? tgCandidates : null, at.toISOString(),
         bytes, freq, unit, alias],
      );
      const row = upd.rows[0];

      // Remember the radio's name permanently, independently of whether the
      // call row above matched. node_radio_events is pruned at 30 days, and an
      // alias is durable identity rather than traffic — forgetting it a month
      // later would lose the most useful thing we know about a radio. The
      // upload alone carries both the id and the alias, so a matched event row
      // is not required. See migration 072.
      if (alias && unit !== null && isRealTalkerAlias(alias, unit)) {
        await client.query(
          `INSERT INTO node_radio_aliases (system, radio_id, alias)
           VALUES ($1, $2, $3)
           ON CONFLICT (system, radio_id) DO UPDATE
             SET alias = EXCLUDED.alias,
                 last_seen = now(),
                 times_seen = node_radio_aliases.times_seen + 1`,
          [row?.system ?? 0, unit, alias],
        );
      }

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

/**
 * Merge the logical calls of an AUTOMATIC patch, using the member list the
 * decoder observed on the air.
 *
 * WHY THIS IS A SEPARATE, LATER STEP
 * There are two kinds of patch and they arrive by different roads. An operator
 * CONFIGURED patch is known up front, so `recordActivityEvents` groups on it
 * the moment the event lands. An AUTOMATIC patch is detected over the air per
 * transmission, and vce's activity feed does not carry it — ControlActivityLookup
 * emits a fixed column set with no patch field. The only place it appears is the
 * `patches` array on the audio upload, which arrives a second or two AFTER the
 * events have already been grouped.
 *
 * So this cannot be a grouping decision; it is a correction. By the time we know
 * the transmission was patched, each member talkgroup has usually opened its own
 * logical call, and the job is to fold them into one.
 *
 * WHICH SURVIVES
 * The numerically smallest id, which is the earliest row — the call that opened
 * first. Arbitrary but stable: every member picks the same winner no matter
 * which upload triggers the merge, so two uploads racing cannot swap them back
 * and forth.
 *
 * Fire-safe: never throws. A failure leaves the calls unmerged, which is exactly
 * the behaviour before automatic patches were honoured at all.
 */
export async function mergeAutomaticPatch(
  nodeId: string,
  /** Talkgroups the decoder saw carrying this transmission. */
  members: number[],
  atDate: Date,
  sourceUnit: number | null = null,
  /** P25 system, when the caller knows it. Null widens the merge to any
   *  system, which is only safe on a single-system deployment. */
  system: number | null = null,
): Promise<void> {
  // A patch of one is not a patch — the decoder reports the lone talkgroup this
  // way constantly, and it means "not currently patched".
  const unique = [...new Set(members.filter((m) => Number.isInteger(m) && m > 0))];
  if (unique.length < 2) return;

  const pool = await getWriterPool();
  if (!pool) return;
  const at = clampReceivedAt(atDate);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Keyed on the PATCH ITSELF — the sorted member set — not on the lowest
    // member of the subset THIS upload happened to report.
    //
    // Two nodes decoding the same patch routinely report different subsets
    // ({10125,10130} vs {10120,10125}); keyed on the minimum they computed
    // different keys, took different locks, and raced on the very rows the
    // lock exists to protect. Sorting first also makes the key independent of
    // the order the decoder listed them in.
    const patchKey = [...unique].sort((a, b) => a - b).join(',');
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`nrc:auto:${patchKey}`]);

    const found = await client.query<{ logical_call_id: string }>(
      // Scoped to the same system and to voice calls. Without the system this
      // could merge two networks' calls that happen to share a talkgroup
      // number; without the call-group filter it could pull in DATA_CALL and
      // signalling rows, whose `talkgroup` column holds a RADIO id.
      `SELECT DISTINCT logical_call_id FROM node_radio_events
        WHERE talkgroup = ANY($1::int[])
          AND received_at BETWEEN $2::timestamptz - interval '${GROUP_WINDOW_SECONDS} seconds'
                             AND $2::timestamptz + interval '${GROUP_WINDOW_SECONDS} seconds'
          AND ($3::integer IS NULL OR source_unit IS NULL OR source_unit = $3::integer)
          AND ($4::integer IS NULL OR system IS NOT DISTINCT FROM $4::integer)
          AND (event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%')
          AND logical_call_id IS NOT NULL`,
      [unique, at.toISOString(), sourceUnit, system],
    );
    const ids = found.rows.map((r) => r.logical_call_id);
    // Nothing to do unless the members really did fork into rival calls.
    if (ids.length < 2) {
      await client.query('COMMIT');
      return;
    }

    const survivor = ids.reduce((a, b) => (BigInt(a) <= BigInt(b) ? a : b));
    const losers = ids.filter((id) => id !== survivor);
    const res = await client.query(
      `UPDATE node_radio_events SET logical_call_id = $1
        WHERE logical_call_id = ANY($2::bigint[])`,
      [survivor, losers],
    );
    await client.query('COMMIT');
    log.info(
      { node: nodeId.slice(0, 8), members: unique, merged: losers.length, rows: res.rowCount },
      'nodeEvents: automatic patch merged',
    );
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* connection already gone */
    }
    log.warn(
      { err: (err as Error).message, node: nodeId.slice(0, 8) },
      'nodeEvents: mergeAutomaticPatch failed',
    );
  } finally {
    client.release();
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
 * message_hash) key, find a ±5s group, insert, stamp logical_id, bump
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

// ---------------------------------------------------------------------------
// Scanner-feed calls (api/scanner-ingest.ts)
// ---------------------------------------------------------------------------

/** The P25 identity a scanner call is filed under — see resolveScannerSystem(). */
interface ScannerSystem {
  system: number | null;
  wacn: number | null;
  label: string | null;
}
let _scannerSystemCache: { at: number; value: ScannerSystem } | null = null;

/**
 * The P25 system a scanner call belongs to — id, WACN and friendly label.
 *
 * A third-party rdio numbers its systems its own way, so the `system` field on
 * their upload is meaningless to us. Every node on this deployment decodes one
 * P25 system, so the answer is simply "the system our own events are on" —
 * resolved from the busiest one seen recently rather than hardcoded, so a
 * different deployment needs no code change.
 *
 * WHY ALL THREE, not just the number: every rollup on the Data page groups by
 * (wacn, system) — systems, talkgroups and radios alike (api/node-data.ts).
 * A scanner row carrying the right `system` but a NULL `wacn` is therefore a
 * DIFFERENT group to a node row on the same system, and one P25 system renders
 * as two: "NSWPSN 721" plus a nameless 721 beside it, with 53 of 402 talkgroups
 * doubled the same way. The scanner hears the same network our nodes do; it has
 * to say so in the same words.
 *
 * Resolved from NODE rows only — a scanner row must never be the thing that
 * teaches the resolver what a system looks like, or the first wrong answer
 * would keep re-electing itself.
 *
 * Nulls stay null when nothing has been observed yet: the row still stores, it
 * just has no system attributed until a node has been heard from.
 */
async function resolveScannerSystem(): Promise<ScannerSystem> {
  if (_scannerSystemCache && Date.now() - _scannerSystemCache.at < 300_000) {
    return _scannerSystemCache.value;
  }
  let value: ScannerSystem = { system: null, wacn: null, label: null };
  try {
    const pool = await getPool();
    if (pool) {
      // The busiest (wacn, system) pair, with the most recent non-null label
      // that pair carried — the same "most recent label wins" rule the systems
      // rollup itself uses, so the two agree by construction.
      const res = await pool.query<{
        system: number;
        wacn: number | null;
        label: string | null;
      }>(
        `SELECT system, wacn,
                (array_agg(system_label ORDER BY received_at DESC)
                   FILTER (WHERE system_label IS NOT NULL))[1] AS label
           FROM node_radio_events
          WHERE received_at >= now() - interval '24 hours'
            AND system IS NOT NULL
            AND stream_id <> 'scanner'
          GROUP BY wacn, system
          ORDER BY count(*) DESC
          LIMIT 1`,
      );
      const row = res.rows[0];
      if (row) value = { system: row.system, wacn: row.wacn ?? null, label: row.label ?? null };
    }
  } catch (err) {
    log.warn({ err }, 'nodeEvents: resolveScannerSystem failed');
  }
  _scannerSystemCache = { at: Date.now(), value };
  return value;
}

export interface ScannerCall {
  nodeId: string;
  /** rdio's `dateTime` — the call's own start, BEFORE the alignment offset. */
  receivedAt: Date;
  talkgroup: number;
  sourceUnit: number | null;
  frequency: number | null;
  talkerAlias: string | null;
  audioBytes: number;
}

/**
 * Record one call from a scanner feed.
 *
 * Unlike the node path there is no activity feed to match against, so this
 * CREATES the event rather than flagging one. It is a single reception with no
 * site: a scanner has no control-channel view.
 *
 * TIME ALIGNMENT. The two sources stamp different moments. A node's activity
 * event carries observed_at_ms — when vce's activity logger wrote the row, at
 * call setup — while an rdio upload carries audioRecording.getStartTime(), when
 * audio began. Measured against production the audio start runs ~1s later, so
 * a scanner call is shifted back by SCANNER_TIME_OFFSET_MS before storing. That
 * is what lets the same transmission heard by BOTH a node and the scanner land
 * in one logical call instead of two, which is what makes the call counts
 * complete rather than double.
 */
export async function recordScannerCall(call: ScannerCall): Promise<boolean> {
  const pool = await getWriterPool();
  if (!pool) return false;

  const shifted = new Date(call.receivedAt.getTime() + config.SCANNER_TIME_OFFSET_MS);
  const receivedAt = clampReceivedAt(shifted);
  const sys = await resolveScannerSystem();
  const system = sys.system;
  const alias =
    call.talkerAlias && isRealTalkerAlias(call.talkerAlias, call.sourceUnit)
      ? call.talkerAlias.trim().slice(0, 64)
      : null;

  // Dedupe key for the (node_id, stream_id, source_event_id) unique index: rdio
  // retries a failed downstream, and a retry must not become a second call.
  // Built from the call's own identity, so a retry hashes identically.
  //
  // The column is a BIGINT — a node event carries vce's own numeric event id —
  // so the digest is folded into 60 bits rather than passed as hex, which
  // Postgres rejects outright (22P02). 15 hex digits max out at ~1.15e18,
  // comfortably inside the signed 64-bit range. Colliding with a node's real
  // event id is harmless: the unique index is scoped by (node_id, stream_id),
  // and the scanner feed has its own node and its own 'scanner' stream.
  const sourceEventId = BigInt(
    '0x' +
      createHash('sha256')
        .update(
          [
            Math.floor(call.receivedAt.getTime() / 1000),
            call.talkgroup,
            call.sourceUnit ?? '',
            call.frequency ?? '',
          ].join('|'),
        )
        .digest('hex')
        .slice(0, 15),
  ).toString();

  // PATCH GROUPING, identical to the activity path's. A patch is several
  // talkgroups carrying ONE conversation, so a scanner that hears a patched
  // transmission reports it on whichever member it was scanning — which is
  // frequently NOT the member a node reported it on. Without this the two
  // recordings of one conversation open rival logical calls, and the feed
  // inflates the very call count it exists to complete.
  //
  // Cheap: a ~60s-cached read of a two-row table, and an empty lookup (central
  // rdio down or unconfigured) degrades to "every talkgroup groups as itself",
  // exactly as before patches existed.
  const patches = await rdioPatches();
  const groupTg = groupingTalkgroup(patches, call.talkgroup);
  const groupMembers = patches.byTalkgroup.get(call.talkgroup)?.talkgroups ?? [call.talkgroup];

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Same advisory lock + grouping the activity path uses, so a scanner
    // reception joins the logical call a node already opened for the same
    // transmission rather than starting a rival one. Keyed on the GROUP
    // talkgroup, not this reception's, or two members of one patch would take
    // different locks and both find no group.
    const lockKey = `nrc:${system ?? -1}:${groupTg}`;
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [lockKey]);

    const found = await client.query<{ logical_call_id: string }>(
      groupLookupSql('($1::integer IS NULL OR c.system IS NULL OR c.system = $1::integer)'),
      // For a patch member this is EVERY member talkgroup, so a copy a node
      // logged on a sibling channel is found and joined. Otherwise it is the
      // single talkgroup, exactly as before.
      //
      // Site is null and stays null: a scanner has no control-channel view, so
      // it can never say WHICH site it heard. That switches the LCN split off
      // for this path, which is the honest answer rather than a guess.
      [system, groupMembers, receivedAt.toISOString(), call.sourceUnit, null, null, call.frequency],
    );
    const existingGroup = found.rows[0]?.logical_call_id ?? null;

    const ins = await client.query<{ id: string }>(
      `INSERT INTO node_radio_events
         (node_id, received_at, stream_id, source_event_id,
          action, event_type, system, wacn, system_label,
          talkgroup, source_unit,
          frequency, encrypted, recorded, audio_bytes, source_alias)
       VALUES ($1, $2::timestamptz, 'scanner', $3,
               'CALL', 'CALL_GROUP', $4, $5, $6,
               $7, $8,
               $9, false, true, $10, $11)
       ON CONFLICT (node_id, stream_id, source_event_id) DO NOTHING
       RETURNING id`,
      [
        call.nodeId,
        receivedAt.toISOString(),
        sourceEventId,
        system,
        // The scanner hears the same network our nodes do, so it files under
        // the same (wacn, system) identity. Without these two the Data page
        // groups it as a second, nameless system. See resolveScannerSystem.
        sys.wacn,
        sys.label,
        call.talkgroup,
        call.sourceUnit,
        call.frequency,
        call.audioBytes,
        alias,
      ],
    );
    const rowId = ins.rows[0]?.id;
    if (rowId === undefined) {
      // Retry of a call already stored — nothing to do, but commit to release
      // the advisory lock cleanly.
      await client.query('COMMIT');
      return true;
    }
    await client.query(`UPDATE node_radio_events SET logical_call_id = $1 WHERE id = $2`, [
      existingGroup ?? rowId,
      rowId,
    ]);

    // The scanner's own OTA is durable identity like any other.
    if (alias && call.sourceUnit !== null) {
      await client.query(
        `INSERT INTO node_radio_aliases (system, radio_id, alias)
         VALUES ($1, $2, $3)
         ON CONFLICT (system, radio_id) DO UPDATE
           SET alias = EXCLUDED.alias,
               last_seen = now(),
               times_seen = node_radio_aliases.times_seen + 1`,
        [system ?? 0, call.sourceUnit, alias],
      );
    }


    await client.query('COMMIT');
    return true;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* connection already gone */
    }
    // Fire-safe like every other exported record-/mark- function in this
    // module (see the header contract): the relay already delivered the
    // audio, so a failed capture is a logged gap, never a thrown error — a
    // 500 here caused rdio to retry a call it had successfully relayed.
    log.error({ err, node: call.nodeId.slice(0, 8) }, 'nodeEvents: recordScannerCall failed');
    return false;
  } finally {
    client.release();
  }
}
