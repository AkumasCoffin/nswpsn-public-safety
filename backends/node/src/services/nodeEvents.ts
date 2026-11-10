/**
 * Per-event feeder-node capture + logical-call grouping (migrations 043/044).
 *
 * RADIO (migration 044): rows in node_radio_events come ONLY from vce
 * ACTIVITY events posted by the Go agent (/api/node-ingest/activity →
 * recordActivityEvents). Activity events carry the real P25 identity — the
 * `system` column stores the P25 systemId — plus per-event action/site/
 * encryption. The rdio call-upload relay inserts nothing and flags nothing; it
 * only folds an automatic patch's calls together (mergeAutomaticPatch).
 *
 * PAGER (unchanged from 043): every relayed page is recorded via
 * recordPagerEvent.
 *
 * Both paths write a DETAIL row (30-day retention — see nodeEventsPruner.ts).
 * The radio hourly FOREVER buckets (node_radio_hourly*) are no longer written
 * here: since migration 080 nodeHourlyRollup.ts derives them from these rows.
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
import { requestNodeEventPartitions } from './nodeEventsPruner.js';

/** Postgres refusing a row because node_radio_events has no partition for its
 *  day (migration 135): SQLSTATE 23514 is any check_violation, so the message
 *  decides. */
function isNoPartitionError(err: unknown): boolean {
  const e = err as { code?: string; message?: string } | null;
  return e?.code === '23514' && /no partition of relation/.test(e.message ?? '');
}

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
 * How far any row of a logical call can sit from a time inside it — the bound
 * every "all rows of this call" lookup carries so it reads one or two day
 * partitions instead of all thirty (node_radio_events is partitioned on
 * received_at; a predicate on logical_call_id alone prunes nothing).
 *
 * A member joins only within GROUP_WINDOW_SECONDS of the group's anchor, so a
 * call spans seconds; an automatic-patch merge folds calls that themselves sat
 * inside that window. Two minutes is an order of magnitude of slack, and keeps
 * the slice small: it lands in one partition, or two across midnight UTC.
 */
const CALL_SPAN_BOUND = "interval '2 minutes'";

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
                      AND x.received_at BETWEEN $3::timestamptz - ${CALL_SPAN_BOUND}
                                            AND $3::timestamptz + ${CALL_SPAN_BOUND}
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

/** A node with a wildly wrong clock must not scatter rows across the
 *  timeline (they'd never prune / group). Anything outside now±48h is
 *  replaced with the server's now on the pager path, and SKIPPED on the two
 *  radio paths — see radioReceivedAt. */
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
  return radioReceivedAt(d) ?? new Date();
}

/**
 * A radio reception's own timestamp, or null when it is unusable (not a date,
 * or more than CLOCK_SANITY_MS from now), in which case the reception is
 * skipped rather than filed at the server's clock.
 *
 * Re-stamping was the pager rule applied here too, and it is wrong for radio
 * twice over. A reception filed at a made-up moment can never join the call it
 * really belonged to, so it only ever forked a phantom call of its own. And the
 * dedup key includes received_at (migration 131, needed to partition the table
 * by day): an agent re-sending a batch after a failure would get a different
 * "now" on each attempt, and the re-send would be stored twice.
 */
function radioReceivedAt(d: Date): Date | null {
  const t = d instanceof Date ? d.getTime() : NaN;
  if (!Number.isFinite(t)) return null;
  if (Math.abs(t - Date.now()) > CLOCK_SANITY_MS) return null;
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

// ---------------------------------------------------------------------------
// Canonical P25 identity
// ---------------------------------------------------------------------------

/** The (wacn, system) pair a system's own receptions overwhelmingly carry. */
interface CanonicalSystem {
  wacn: number | null;
  system: number;
  events: number;
}

/**
 * A system is only as trustworthy as the frames it is read from, and the WACN
 * and system id are READ OFF THE AIR: a single corrupt control frame yields a
 * plausible-looking pair that belongs to no network. Production has them —
 * 0xAEE00 and 0xBAE03 against a real 0xBEE00, one bit apart, a handful of
 * receptions each against 1.2 million.
 *
 * They are not harmless. Every rollup on the Data page groups by
 * (wacn, system), so each bad pair renders as a whole extra system beside the
 * real one, carrying off a few of its talkgroups and calls; worse, logical
 * call grouping keys on the system id too, so a reception read with a corrupt
 * id cannot join the call its siblings are in and splits off as a call of its
 * own.
 *
 * The fix is to decide identity by something that is NOT read off the air:
 * the channel's configured system name, which the operator sets and the agent
 * ships verbatim. Within one label the pair that the overwhelming majority of
 * receptions carry is what the network actually transmits; anything else
 * under that label is a mis-decode and is filed under the real identity.
 *
 * What this deliberately does NOT do is merge two systems. A different
 * network carries a different label — that is how a node is configured — so
 * adding another state's system creates its own canonical pair and is never
 * touched by this. The assumption is only ever "one label, one network", and
 * every correction is counted into the log so a systematic deviation (an
 * operator who labelled two different networks the same) shows up as
 * thousands of corrections rather than passing silently.
 */
let _canonCache: { at: number; byLabel: Map<string, CanonicalSystem> } | null = null;
let _canonRefreshing: Promise<Map<string, CanonicalSystem>> | null = null;
const CANON_TTL_MS = 10 * 60_000;
/**
 * How many receptions a label needs before its busiest pair is taken as
 * authoritative. A young deployment must not canonicalise itself onto the
 * first pair it ever heard, which might be the corrupt one.
 */
const CANON_MIN_EVENTS = 1000;
/**
 * Window the answer is read from. Short on purpose: this table holds millions
 * of rows, and the question — "which pair does this network transmit" — is
 * answered just as well by a few hours of it as by a month. The call-group
 * filter is what lets the partial rollup index carry the scan.
 */
const CANON_WINDOW = '6 hours';

async function refreshCanonicalSystems(): Promise<Map<string, CanonicalSystem>> {
  const byLabel = new Map<string, CanonicalSystem>();
  try {
    const pool = await getPool();
    if (pool) {
      const res = await pool.query<{ system_label: string; wacn: number | null; system: number; n: string }>(
        `SELECT DISTINCT ON (system_label) system_label, wacn, system, COUNT(*)::text AS n
           FROM node_radio_events
          WHERE received_at >= now() - interval '${CANON_WINDOW}'
            AND (event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%')
            AND system IS NOT NULL
            AND system_label IS NOT NULL
          GROUP BY system_label, wacn, system
          ORDER BY system_label, COUNT(*) DESC`,
      );
      for (const r of res.rows) {
        byLabel.set(r.system_label, { wacn: r.wacn ?? null, system: r.system, events: Number(r.n) || 0 });
      }
    }
  } catch (err) {
    // No canonical set = nothing is corrected, which is the old behaviour.
    log.warn({ err }, 'nodeEvents: canonicalSystems failed');
  }
  _canonCache = { at: Date.now(), byLabel };
  return byLabel;
}

/**
 * The canonical identities, without ever making an arriving batch wait on a
 * refresh. The very first call has nothing to serve and awaits one read; from
 * then on the cached answer goes back immediately and a stale one is renewed
 * in the background. The pair a network transmits does not change, so serving
 * a ten-minute-old answer for the length of one query is free.
 */
async function canonicalSystems(): Promise<Map<string, CanonicalSystem>> {
  if (!_canonCache) {
    _canonRefreshing ??= refreshCanonicalSystems().finally(() => { _canonRefreshing = null; });
    return await _canonRefreshing;
  }
  if (Date.now() - _canonCache.at >= CANON_TTL_MS && !_canonRefreshing) {
    _canonRefreshing = refreshCanonicalSystems().finally(() => { _canonRefreshing = null; });
  }
  return _canonCache.byLabel;
}

/**
 * The identity this reception should be filed under: its own, unless its
 * label's network is known to transmit a different pair — or it arrived with
 * no numeric identity at all.
 *
 * That second case used to be skipped ("nothing to correct"), and it is the
 * one that put the same talkgroup on a node's table twice. An agent can emit
 * an event before its control channel has identified the system — the label
 * is there, the numbers are not — and filed under (NULL, NULL, talkgroup) it
 * forms a second group beside the real one. The label names the network; if
 * that network's pair is known, the event belongs under it.
 */
export function canonicalPair(
  canon: Map<string, CanonicalSystem>,
  label: string | null,
  system: number | null,
  wacn: number | null,
): { system: number | null; wacn: number | null; corrected: boolean } {
  if (label === null) return { system, wacn, corrected: false };
  const c = canon.get(label);
  if (!c || c.events < CANON_MIN_EVENTS) return { system, wacn, corrected: false };
  if (c.system === system && c.wacn === wacn) return { system, wacn, corrected: false };
  return { system: c.system, wacn: c.wacn, corrected: true };
}

/** Test seam. */
export function _resetCanonicalSystems(): void {
  _canonCache = null;
  _canonRefreshing = null;
}

/**
 * The activity path's insert. Two things about it are deliberate.
 *
 * THE ROW'S ID IS DRAWN FIRST. The event that opens a call has no existing
 * group to join ($20 is null), and its call's id is its own row id. That used
 * to mean inserting and then `UPDATE ... SET logical_call_id = id`: a second
 * heap tuple per opener, and since logical_call_id is indexed the update could
 * never be HOT, so every index on the table took a second entry. Openers are
 * not rare — 30 % of rows on production (2026-11-03) — so taking nextval() in
 * the CTE and writing it to both columns removes one row version and a dozen
 * index entries per opener. A CTE that calls a volatile function is evaluated
 * once however often it is referenced, so both columns get the same value. A
 * re-send that then hits the dedup index throws its value away; ids only need
 * to be unique and increasing, and gaps are already normal. The sequence name
 * is fixed by migration 130.
 *
 * THE CONFLICT TARGET IS LEFT OPEN. The dedup index is moving from
 * (node_id, stream_id, source_event_id) to the same plus received_at (131),
 * which partitioning on received_at requires. A named target must match one
 * index exactly, and an INVALID index cannot be an arbiter, so naming either
 * would tie this statement to the state of a migration. With no target, any
 * unique violation is a skip; the only other unique index is the id itself,
 * which a sequence cannot collide on.
 */
const INSERT_EVENT_SQL = `
  WITH n AS (SELECT nextval('node_radio_events_id_seq') AS id)
  INSERT INTO node_radio_events
    (id, node_id, received_at, stream_id, source_event_id,
     action, event_type, system, talkgroup, source_unit,
     frequency, timeslot, encrypted,
     site_rfss, site_id, site_nac, wacn,
     system_label, source_alias, patch_members, logical_call_id)
  VALUES ((SELECT id FROM n), $1, $2::timestamptz, $3, $4, $5, $6, $7, $8, $9,
          $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
          COALESCE($20::bigint, (SELECT id FROM n)))
  ON CONFLICT DO NOTHING
  RETURNING id`;

/**
 * Record a batch of activity events for one node/stream. Returns how many
 * were NEWLY inserted (deduped re-sends are skipped silently — the unique
 * dedup index + ON CONFLICT DO NOTHING makes re-posting a batch idempotent)
 * and how many failed. An event whose timestamp is unusable is neither: it is
 * skipped and logged (radioReceivedAt).
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
    // Likewise once per batch: the known-good P25 identity per system label,
    // used to file a reception whose transmitted ids were mis-decoded under
    // the system it actually belongs to.
    const canon = await canonicalSystems();
    let corrected = 0;

    const client = await pool.connect();
    let failures = 0;
    let lastErr: unknown = null;
    let badClock = 0;
    try {
      for (const ev of events) {
        const receivedAt = radioReceivedAt(new Date(ev.atMs));
        if (receivedAt === null) {
          // Neither accepted nor failed: a failure would refuse the ack and
          // have the agent re-send an event that can never be stored, forever.
          badClock++;
          continue;
        }
        const label = ev.systemName ?? null;
        // Identity BEFORE grouping: the system id is half the logical-call
        // key, so a reception read with corrupt ids has to be put right here
        // or it forks a call of its own.
        const id = canonicalPair(canon, label, safeInt(ev.systemId), safeInt(ev.wacn));
        if (id.corrected) corrected++;
        const system = id.system;
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

          // Dedupe: a re-sent event hits the unique dedup index → no row
          // returned → skip stamps/buckets. See INSERT_EVENT_SQL for why the
          // conflict target is left open.
          //
          // logical_call_id is written HERE, never by a follow-up UPDATE. The
          // lookup above already resolved which call this event belongs to,
          // and the event that OPENS a call (existingGroup null) takes the id
          // the insert itself draws — see INSERT_EVENT_SQL.
          const ins = await client.query<{ id: string }>(
            INSERT_EVENT_SQL,
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
              id.wacn,
              label,
              ev.sourceAlias ?? null,
              // Never the talkgroup itself: the call is already filed there,
              // and listing it as one of its own patch members would read as
              // the transmission going out on it twice.
              patchMembersOf(ev.patchMembers, talkgroup),
              existingGroup,
            ],
          );
          const rowId = ins.rows[0]?.id;
          if (rowId === undefined) {
            // Duplicate re-send — nothing changed, but commit to release
            // the advisory lock cleanly.
            await client.query('COMMIT');
            continue;
          }
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

          await client.query('COMMIT');
          accepted += 1;
        } catch (err) {
          await client.query('ROLLBACK').catch(() => {});
          failures += 1;
          lastErr = err;
          // The event fails and the agent re-sends it; this makes sure the
          // day's partition exists by then instead of at the next hourly tick.
          if (isNoPartitionError(err)) requestNodeEventPartitions();
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
    if (badClock > 0) {
      log.warn(
        { skipped: badClock, of: events.length, node: nodeId.slice(0, 8) },
        'nodeEvents: receptions skipped, timestamp more than 48h from server time (node clock?)',
      );
    }
    if (corrected > 0) {
      // Expect a trickle. A steady stream means either a node with a real
      // decode problem or two different networks sharing one system label,
      // and both want looking at rather than quietly papering over.
      log.info(
        { corrected, of: events.length, node: nodeId.slice(0, 8) },
        'nodeEvents: receptions filed under their system despite mis-decoded P25 ids',
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
        WHERE logical_call_id = ANY($2::bigint[])
          AND received_at BETWEEN $3::timestamptz - ${CALL_SPAN_BOUND}
                             AND $3::timestamptz + ${CALL_SPAN_BOUND}`,
      [survivor, losers, at.toISOString()],
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
  const receivedAt = radioReceivedAt(shifted);
  if (receivedAt === null) {
    // See radioReceivedAt. The audio itself was already relayed; only our own
    // record of it is skipped.
    log.warn(
      { node: call.nodeId.slice(0, 8), at: call.receivedAt.toISOString() },
      'nodeEvents: scanner call skipped, timestamp more than 48h from server time',
    );
    return false;
  }
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

    // logical_call_id rides the insert, as on the activity path above, and a
    // reception that opens its call takes the id the insert draws — the same
    // arrangement, for the same reasons, as INSERT_EVENT_SQL.
    const ins = await client.query<{ id: string }>(
      `WITH n AS (SELECT nextval('node_radio_events_id_seq') AS id)
       INSERT INTO node_radio_events
         (id, node_id, received_at, stream_id, source_event_id,
          action, event_type, system, wacn, system_label,
          talkgroup, source_unit,
          frequency, encrypted, source_alias,
          logical_call_id)
       VALUES ((SELECT id FROM n), $1, $2::timestamptz, 'scanner', $3,
               'CALL', 'CALL_GROUP', $4, $5, $6,
               $7, $8,
               $9, false, $10,
               COALESCE($11::bigint, (SELECT id FROM n)))
       ON CONFLICT DO NOTHING
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
        alias,
        existingGroup,
      ],
    );
    const rowId = ins.rows[0]?.id;
    if (rowId === undefined) {
      // Retry of a call already stored — nothing to do, but commit to release
      // the advisory lock cleanly.
      await client.query('COMMIT');
      return true;
    }
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
    if (isNoPartitionError(err)) requestNodeEventPartitions();
    return false;
  } finally {
    client.release();
  }
}
