/**
 * Per-event feeder-node capture + logical-call grouping (migration 043).
 *
 * Every call/page relayed through /api/node-ingest/* is recorded here as a
 * DETAIL row (node_radio_events / node_pager_events, 30-day retention — see
 * nodeEventsPruner.ts) and rolled into the hourly FOREVER buckets
 * (node_radio_hourly / node_radio_hourly_sys / node_pager_hourly) in the
 * same transaction.
 *
 * Logical grouping: the same over-the-air transmission heard by N nodes
 * arrives as N uploads. Rows within ±4s on the same (system, talkgroup)
 * [radio] or (capcode, sha256(message)) [pager] share one logical id — the
 * detail-row id of the FIRST member of the group. Group assignment is
 * serialised per key with pg_advisory_xact_lock(hashtext(key)) so two
 * concurrent uploads of the same call can't each start their own group.
 *
 * CONTRACT: both record* functions are fire-safe — any failure is logged
 * and swallowed; they never throw to the caller. Capture must never affect
 * the relay path.
 */
import { createHash } from 'node:crypto';
import { getWriterPool } from '../db/pool.js';
import { log } from '../lib/log.js';

/** Two receptions of the same call may carry timestamps a few seconds
 *  apart (queueing on the node, clock skew). ±4s matches the shortest
 *  realistic gap between two DISTINCT calls on one talkgroup. */
const GROUP_WINDOW_SECONDS = 4;

/** A node with a wildly wrong clock must not scatter rows across the
 *  timeline (they'd never prune / group). Anything outside now±48h is
 *  replaced with the server's now. */
const CLOCK_SANITY_MS = 48 * 60 * 60 * 1000;

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

export interface RadioEventInput {
  nodeId: string;
  receivedAt: Date;
  system: number | null;
  talkgroup: number | null;
  sourceUnit: number | null;
  frequency: number | null;
  siteRfss: number | null;
  siteId: number | null;
  siteNac: number | null;
  siteSource: string | null; // 'event' | 'channel' | 'context'
  talkgroupLabel: string | null;
  systemLabel: string | null;
  audioBytes: number;
}

/**
 * Record one radio call reception. One transaction: advisory-lock the
 * (system, talkgroup) grouping key, find an existing logical group within
 * ±4s, insert the detail row, stamp its logical_call_id (found group or
 * its own id = new group), and bump both hourly rollups.
 */
export async function recordRadioEvent(ev: RadioEventInput): Promise<void> {
  try {
    const pool = await getWriterPool();
    if (!pool) return;

    const receivedAt = clampReceivedAt(ev.receivedAt);
    const system = safeInt(ev.system);
    const talkgroup = safeInt(ev.talkgroup);
    const sourceUnit = safeInt(ev.sourceUnit);
    const frequency = safeInt(ev.frequency);
    const siteRfss = safeInt(ev.siteRfss);
    const siteId = safeInt(ev.siteId);
    const siteNac = safeInt(ev.siteNac);
    const audioBytes = safeInt(ev.audioBytes) ?? 0;
    const lockKey = `nrc:${system}:${talkgroup}`;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // Serialise grouping per (system, talkgroup) so two nodes uploading
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

      const ins = await client.query<{ id: string }>(
        `INSERT INTO node_radio_events
           (node_id, received_at, system, talkgroup, source_unit, frequency,
            site_rfss, site_id, site_nac, site_source,
            talkgroup_label, system_label, audio_bytes)
         VALUES ($1, $2::timestamptz, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         RETURNING id`,
        [
          ev.nodeId,
          receivedAt.toISOString(),
          system,
          talkgroup,
          sourceUnit,
          frequency,
          siteRfss,
          siteId,
          siteNac,
          ev.siteSource,
          ev.talkgroupLabel,
          ev.systemLabel,
          audioBytes,
        ],
      );
      const rowId = ins.rows[0]!.id;
      await client.query(
        `UPDATE node_radio_events SET logical_call_id = $1 WHERE id = $2`,
        [existingGroup ?? rowId, rowId],
      );

      const isNewGroup = existingGroup === null;
      // Per-node forever bucket (raw receptions + bytes).
      await client.query(
        `INSERT INTO node_radio_hourly (hour, node_id, system, talkgroup, calls, audio_bytes)
         VALUES (date_trunc('hour', $1::timestamptz), $2, $3, $4, 1, $5)
         ON CONFLICT (hour, node_id, system, talkgroup) DO UPDATE
           SET calls = node_radio_hourly.calls + 1,
               audio_bytes = node_radio_hourly.audio_bytes + EXCLUDED.audio_bytes`,
        [receivedAt.toISOString(), ev.nodeId, system ?? 0, talkgroup ?? 0, audioBytes],
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
          siteRfss ?? -1,
          siteId ?? -1,
          isNewGroup ? 1 : 0,
        ],
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
      'nodeEvents: recordRadioEvent failed',
    );
  }
}

export interface PagerEventInput {
  nodeId: string;
  receivedAt: Date;
  capcode: string;
  function: number | null;
  freqMhz: number | null;
  message: string;
}

/**
 * Record one pager message reception. Same shape as recordRadioEvent:
 * advisory-lock the (capcode, message_hash) key, find a ±4s group, insert,
 * stamp logical_id, bump node_pager_hourly.
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
