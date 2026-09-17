/**
 * Persist each receiver's own tracks, so the eight-hour map survives a restart.
 *
 * The tracks were memory-only. Every deploy emptied them, so a node that had
 * heard sixty-odd aircraft during the day would show ONE — whatever had crossed
 * since the backend last came up. The coverage envelope beside it was persisted
 * and looked healthy, which made the map read as broken rather than as young.
 *
 * Not a filter over the global adsb_tracks, which already stores every merged
 * track with its `sources`: those rows only exist for aircraft that reached the
 * public merge, and a node whose feed is PAUSED contributes nothing to it. A
 * receiver's own diagnostics have to be built from its own uploads, which is
 * the same line the feed gate is drawn on everywhere else here.
 */
import { getPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';
import { sliceTrailByHour } from '../adsbTrackArchive.js';
import { nodeTracesForArchive, type NodeArchivableTrack } from './adsbNodeStore.js';

/** Same cadence as the daily counters and the coverage envelope: all three are
 *  running aggregates that nothing reads between passes. */
const FLUSH_INTERVAL_MS = 60_000;

/** Aircraft-hours written per statement. */
const BATCH_SIZE = 200;

const HOUR_MS = 3_600_000;

let lastFlushMs = 0;
let flushing = false;

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

/**
 * Write the current and previous hour for every aircraft each node is tracking.
 *
 * Only those two hours, for the reason the global archive gives: an hour that
 * has closed is finished, and rewriting it would let the upstream decimation —
 * which progressively coarsens points older than ten minutes — degrade history
 * already stored at full resolution. The previous hour is included so a flush
 * landing after the top of the hour does not leave the hour that just closed
 * missing its final minutes.
 */
export async function flushNodeTracks(
  tracks: ReadonlyArray<NodeArchivableTrack>,
  nowMs: number = Date.now(),
): Promise<number> {
  const pool = await getPool();
  if (!pool) return 0;

  interface Row { nodeId: string; hex: string; callsign: string | null;
    hourMs: number; firstMs: number; lastMs: number; reports: number;
    lastAltFt: number | null; maxAltFt: number | null;
    points: Array<[number, number, number, number | null]>; }
  const rows: Row[] = [];
  for (const t of tracks) {
    for (const s of sliceTrailByHour(t.hex, t.points, nowMs)) {
      rows.push({
        nodeId: t.nodeId, hex: t.hex, callsign: t.callsign,
        hourMs: s.hourMs, firstMs: s.firstMs, lastMs: s.lastMs, points: s.points,
        reports: t.reports, lastAltFt: t.lastAltFt, maxAltFt: t.maxAltFt,
      });
    }
  }
  if (rows.length === 0) return 0;

  let written = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const values: unknown[] = [];
    const tuples = batch.map((r) => {
      const b = values.length;
      values.push(r.nodeId, r.hex, iso(r.hourMs), iso(r.firstMs), iso(r.lastMs),
        r.callsign, r.reports, r.lastAltFt, r.maxAltFt, JSON.stringify(r.points));
      return `($${b + 1}, $${b + 2}, $${b + 3}::timestamptz, $${b + 4}::timestamptz,`
        + ` $${b + 5}::timestamptz, $${b + 6}, $${b + 7}, $${b + 8}, $${b + 9},`
        + ` $${b + 10}::jsonb)`;
    });
    try {
      await pool.query(
        `INSERT INTO node_adsb_tracks
           (node_id, hex, hour_bucket, first_seen, last_seen, callsign,
            reports, last_alt_ft, max_alt_ft, points)
         VALUES ${tuples.join(', ')}
         ON CONFLICT (node_id, hex, hour_bucket) DO UPDATE SET
           points = CASE
             WHEN EXCLUDED.first_seen > node_adsb_tracks.last_seen
             THEN node_adsb_tracks.points || EXCLUDED.points
             ELSE EXCLUDED.points
           END,
           first_seen  = LEAST(node_adsb_tracks.first_seen, EXCLUDED.first_seen),
           last_seen   = GREATEST(node_adsb_tracks.last_seen, EXCLUDED.last_seen),
           callsign    = COALESCE(EXCLUDED.callsign, node_adsb_tracks.callsign),
           -- Counters only ever climb, and the peak is a peak.
           reports     = GREATEST(COALESCE(node_adsb_tracks.reports, 0),
                                  COALESCE(EXCLUDED.reports, 0)),
           last_alt_ft = COALESCE(EXCLUDED.last_alt_ft, node_adsb_tracks.last_alt_ft),
           max_alt_ft  = GREATEST(COALESCE(node_adsb_tracks.max_alt_ft, EXCLUDED.max_alt_ft),
                                  COALESCE(EXCLUDED.max_alt_ft, node_adsb_tracks.max_alt_ft))`,
        values,
      );
      written += batch.length;
    } catch (err) {
      // One interval at worst; the next pass re-cuts the same hours from
      // memory. Same posture as the other ADS-B flushes.
      log.debug({ err, rows: batch.length }, 'node track flush failed');
    }
  }
  return written;
}

/** Called once per upload cycle; rate-limits itself. */
export function maybeFlushNodeTracks(nowMs: number = Date.now()): void {
  if (flushing) return;
  if (nowMs - lastFlushMs < FLUSH_INTERVAL_MS) return;
  lastFlushMs = nowMs;
  flushing = true;
  void flushNodeTracks(nodeTracesForArchive(), nowMs)
    .catch((err) => log.debug({ err }, 'node track flush threw'))
    .finally(() => { flushing = false; });
}

export interface StoredTrack {
  hex: string;
  callsign: string | null;
  /** [epochMs, lat, lon] in time order, clipped to the window. */
  points: Array<[number, number, number]>;
  /** The scalars "Recently heard" is built from, so that table survives a
   *  restart too — it reads the same traces the map does. */
  reports: number;
  lastAltFt: number | null;
  maxAltFt: number | null;
  firstMs: number;
  lastMs: number;
}

/**
 * One receiver's stored tracks over a trailing window.
 *
 * Read back and merged with what is in memory by the caller — the DB holds
 * everything up to the last flush (including before a restart), memory holds
 * the current minute.
 */
export async function storedNodeTracks(
  nodeId: string,
  minutes: number,
  nowMs: number = Date.now(),
): Promise<StoredTrack[]> {
  const pool = await getPool();
  if (!pool) return [];

  const t0 = nowMs - Math.max(1, minutes) * 60_000;
  const hourFrom = Math.floor(t0 / HOUR_MS) * HOUR_MS;

  const r = await pool.query<{ hex: string; callsign: string | null;
    hour_bucket: Date; points: unknown; reports: number | null;
    last_alt_ft: number | null; max_alt_ft: number | null;
    first_seen: Date; last_seen: Date }>(
    `SELECT hex, callsign, hour_bucket, points, reports, last_alt_ft, max_alt_ft,
            first_seen, last_seen
       FROM node_adsb_tracks
      WHERE node_id = $1
        AND hour_bucket >= $2::timestamptz
        AND last_seen   >= $3::timestamptz
      ORDER BY hex, hour_bucket`,
    [nodeId, new Date(hourFrom).toISOString(), new Date(t0).toISOString()],
  );

  const byHex = new Map<string, StoredTrack>();
  for (const row of r.rows) {
    const hourMs = row.hour_bucket.getTime();
    let track = byHex.get(row.hex);
    if (!track) {
      track = {
        hex: row.hex, callsign: row.callsign, points: [],
        reports: 0, lastAltFt: null, maxAltFt: null,
        firstMs: row.first_seen.getTime(), lastMs: row.last_seen.getTime(),
      };
      byHex.set(row.hex, track);
    } else if (!track.callsign && row.callsign) {
      track.callsign = row.callsign;
    }
    // Rows arrive hour by hour, so an aircraft heard across three hours has
    // its counters summed and its span widened rather than overwritten.
    track.reports += Number(row.reports ?? 0);
    if (row.last_alt_ft !== null) track.lastAltFt = row.last_alt_ft;
    if (row.max_alt_ft !== null
        && (track.maxAltFt === null || row.max_alt_ft > track.maxAltFt)) {
      track.maxAltFt = row.max_alt_ft;
    }
    track.firstMs = Math.min(track.firstMs, row.first_seen.getTime());
    track.lastMs = Math.max(track.lastMs, row.last_seen.getTime());
    const raw = Array.isArray(row.points) ? row.points : [];
    for (const p of raw as Array<[number, number, number, number | null]>) {
      const tMs = hourMs + p[0] * 1000;
      // Clipped here: the hour bound above can only ever be coarse, since the
      // points live inside a jsonb array.
      if (tMs < t0 || tMs > nowMs) continue;
      track.points.push([tMs, p[1], p[2]]);
    }
  }

  const out: StoredTrack[] = [];
  for (const t of byHex.values()) {
    if (t.points.length === 0) continue;
    t.points.sort((a, b) => a[0] - b[0]);
    out.push(t);
  }
  return out;
}
