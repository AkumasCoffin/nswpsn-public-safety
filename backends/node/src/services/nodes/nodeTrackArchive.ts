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

/**
 * Points per stored hour, as a backstop only.
 *
 * Decimation upstream stores a moving aircraft about every ten seconds, so a
 * full hour in view is around 360. This sits well clear of that: reaching it
 * means something is wrong, and the row stops growing rather than growing
 * without bound.
 */
const MAX_POINTS_PER_HOUR = 1000;

/** How long a high-water mark is worth keeping. Only the current and previous
 *  hours are ever written, so anything older can never be consulted again. */
const HWM_RETENTION_MS = 2 * HOUR_MS;

/**
 * The newest point time CONFIRMED WRITTEN for each `nodeId|hex`.
 *
 * This is what makes the write an append. Re-cutting the whole hour from memory
 * every minute and letting the statement decide append-or-replace could not
 * work: after a restart the second flush's slice starts before what the first
 * flush had already stored, so the replace branch threw the pre-restart half of
 * the hour away sixty seconds after saving it. Sending only what is new sidesteps
 * the choice entirely.
 *
 * Empty after a restart, which is correct rather than merely tolerable: memory
 * then starts at boot time, strictly after whatever the last pre-restart flush
 * stored, so the first delta appends cleanly with nothing duplicated.
 */
const flushedThrough = new Map<string, number>();

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
  /** One track's rows travel together: a flight over the hour boundary yields
   *  two, and advancing the mark when only one of them landed would lose the
   *  other for good. */
  interface Group { key: string; deltaMaxMs: number; rows: Row[] }

  const groups: Group[] = [];
  for (const t of tracks) {
    const key = `${t.nodeId}|${t.hex}`;
    const since = flushedThrough.get(key) ?? -Infinity;
    const fresh = t.points.filter((p) => p[0] > since);
    if (fresh.length === 0) continue;

    const rows: Row[] = [];
    for (const s of sliceTrailByHour(t.hex, fresh, nowMs)) {
      rows.push({
        nodeId: t.nodeId, hex: t.hex, callsign: t.callsign,
        hourMs: s.hourMs, firstMs: s.firstMs, lastMs: s.lastMs, points: s.points,
        reports: t.reports, lastAltFt: t.lastAltFt, maxAltFt: t.maxAltFt,
      });
    }
    if (rows.length === 0) continue;
    groups.push({ key, deltaMaxMs: fresh[fresh.length - 1]![0], rows });
  }
  if (groups.length === 0) return 0;

  // Pack whole groups until the row budget is reached, so no group straddles
  // two statements.
  const batches: Group[][] = [];
  let current: Group[] = [];
  let currentRows = 0;
  for (const g of groups) {
    if (currentRows > 0 && currentRows + g.rows.length > BATCH_SIZE) {
      batches.push(current);
      current = [];
      currentRows = 0;
    }
    current.push(g);
    currentRows += g.rows.length;
  }
  if (current.length > 0) batches.push(current);

  let written = 0;
  for (const group of batches) {
    const batch = group.flatMap((g) => g.rows);
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
           -- Unconditionally an append: the caller only ever sends points it
           -- has not stored before. The guard caps a pathological row rather
           -- than choosing between the two versions — note it keeps the STORED
           -- points, never EXCLUDED, so overflow stops growth instead of
           -- discarding history.
           points = CASE
             WHEN jsonb_array_length(node_adsb_tracks.points) < ${MAX_POINTS_PER_HOUR}
             THEN node_adsb_tracks.points || EXCLUDED.points
             ELSE node_adsb_tracks.points
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
      // Only now is the delta durable, so only now may the mark move. A
      // failure leaves it where it was and the next pass re-sends the same
      // points.
      for (const g of group) {
        flushedThrough.set(g.key, Math.max(flushedThrough.get(g.key) ?? 0, g.deltaMaxMs));
      }
      written += batch.length;
    } catch (err) {
      // Loud, unlike the rest of the ADS-B flushes: this one failing is
      // indistinguishable from the feature working until someone notices the
      // map is empty, which is exactly how a missing column went unseen.
      log.warn({ err, rows: batch.length }, 'node track flush failed');
    }
  }

  for (const [key, at] of flushedThrough) {
    if (at < nowMs - HWM_RETENTION_MS) flushedThrough.delete(key);
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
    .catch((err) => log.warn({ err }, 'node track flush threw'))
    .finally(() => { flushing = false; });
}

/** Test seam: the high-water marks outlive a single test otherwise. */
export function _resetNodeTrackArchive(): void {
  flushedThrough.clear();
  lastFlushMs = 0;
  flushing = false;
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
    // reports is the whole-trace running total stamped into every hour row
    // (GREATEST on write), not a per-hour count — so the largest row IS the
    // trace total. Summing them tripled a three-hour flight.
    track.reports = Math.max(track.reports, Number(row.reports ?? 0));
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
    // A flush that committed but reported failure re-sends its delta, so the
    // same second can land twice. Points are second-granular once sliced, so
    // equal timestamps are the same observation.
    t.points = t.points.filter((p, i) => i === 0 || p[0] !== t.points[i - 1]![0]);
    out.push(t);
  }
  return out;
}
