/**
 * Spill the in-memory aircraft trails to Postgres, so the map can be scrubbed
 * back in time.
 *
 * There is no second ingest here. `sources/adsb.ts` already assembles exactly
 * what history needs — a per-hex, time-stamped, Douglas-Peucker simplified
 * trail, built from the MERGED snapshot and with dead-reckoned positions
 * filtered out — and this module writes that structure down. Which is also why
 * a fleet of feeder nodes needs no special handling: the merge unions every
 * aggregator and every node by hex before a trail point exists, so one
 * aircraft is one track however many receivers heard it.
 *
 * Rows are keyed (hex, hour). Only the current hour and the one before it are
 * ever written, so a closed hour is immutable, the UPDATE churn is bounded to
 * the aircraft currently flying, and the archive never has to read before it
 * writes.
 */
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
// TYPE-ONLY, deliberately — the same rule adsbNodeStore.ts follows.
// sources/adsb.ts imports this module at runtime to hand its trails over, so
// a runtime import back the other way would close a cycle between the poller
// and the archive. The trails are therefore PASSED IN rather than pulled.
import type { AdsbAircraft, ArchivableTrack } from '../sources/adsb.js';

/**
 * How often trails are written down.
 *
 * The poll runs every ~8 s; writing on each one would be six times the work
 * for six times the bloat and no more history, because the trail decimation
 * upstream stores a point a minute at best for anything not manoeuvring.
 * History therefore lags live by up to a minute, which is invisible — the live
 * view already covers the last several minutes.
 */
const FLUSH_INTERVAL_MS = 60_000;

/**
 * Upper bound on the points stored for one aircraft-hour.
 *
 * Only reachable through the disjoint-append path below (a restart mid-hour),
 * since the in-memory buffer is itself capped at 150. A backstop, not a
 * working limit.
 */
const MAX_POINTS_PER_HOUR = 400;

/** Aircraft written per statement. Keeps one flush to a handful of round
 *  trips without building a single multi-megabyte query. */
const BATCH_SIZE = 200;

const HOUR_MS = 3_600_000;

interface Identity {
  callsign: string | null;
  reg: string | null;
  type: string | null;
  esTag: string | null;
  sources: Set<string>;
}

/**
 * Identity per hex, accumulated across polls.
 *
 * Not on the trail: the trail is positions, and this is who they belong to.
 * Kept because identity ARRIVES LATE — an aircraft is usually tracked for a
 * while before it transmits a callsign, and the aggregators differ in how
 * complete their registration/type databases are, so the record that first
 * gave us a position often is not the one that names it.
 */
const identities = new Map<string, Identity>();

let lastFlushMs = 0;
let flushing = false;

/** Fold one poll's OBSERVED records into the identity map. Estimated records
 *  must not be passed in — they are excluded from trails for the same reason,
 *  and an archive entry should only exist for something that was heard. */
export function noteAdsbIdentities(observed: AdsbAircraft[]): void {
  for (const a of observed) {
    let id = identities.get(a.hex);
    if (!id) {
      id = { callsign: null, reg: null, type: null, esTag: null, sources: new Set() };
      identities.set(a.hex, id);
    }
    // Only ever improves: a later record that has forgotten the callsign must
    // not blank out one we already know.
    if (a.callsign) id.callsign = a.callsign;
    if (a.reg) id.reg = a.reg;
    if (a.type) id.type = a.type;
    if (a.esTag) id.esTag = a.esTag;
    for (const s of a.sources) id.sources.add(s);
  }
}

/** Drop identities for aircraft no longer being trailed, so the map cannot
 *  grow without bound across a long uptime. */
function pruneIdentities(liveHexes: Set<string>): void {
  for (const hex of identities) {
    if (!liveHexes.has(hex[0])) identities.delete(hex[0]);
  }
}

interface HourSlice {
  hex: string;
  hourMs: number;
  firstMs: number;
  lastMs: number;
  /** [secondsIntoHour, lat, lon, altFt|null] */
  points: Array<[number, number, number, number | null]>;
}

/**
 * Cut one aircraft's trail into the hour slices worth writing.
 *
 * Only the current hour and the previous one. Earlier hours were written while
 * they were current and are finished; rewriting them would serve no purpose
 * and would let the upstream simplification — which progressively coarsens
 * points older than ten minutes — degrade history that was already stored at
 * full in-window resolution.
 *
 * The previous hour is included, rather than just the current one, because a
 * flush landing shortly after the top of the hour would otherwise leave the
 * hour that just closed missing its final seconds forever.
 */
export function sliceTrailByHour(
  hex: string,
  points: ReadonlyArray<readonly [number, number, number, number | null]>,
  nowMs: number,
): HourSlice[] {
  const currentHour = Math.floor(nowMs / HOUR_MS) * HOUR_MS;
  const previousHour = currentHour - HOUR_MS;
  const byHour = new Map<number, HourSlice>();

  for (const p of points) {
    const t = p[0];
    const hourMs = Math.floor(t / HOUR_MS) * HOUR_MS;
    if (hourMs !== currentHour && hourMs !== previousHour) continue;
    let slice = byHour.get(hourMs);
    if (!slice) {
      slice = { hex, hourMs, firstMs: t, lastMs: t, points: [] };
      byHour.set(hourMs, slice);
    }
    slice.points.push([Math.round((t - hourMs) / 1000), p[1], p[2], p[3]]);
    if (t < slice.firstMs) slice.firstMs = t;
    if (t > slice.lastMs) slice.lastMs = t;
  }

  // A single point is a dot, not a track. It is still enough to say the
  // aircraft was there, so it is kept — the historical view interpolates
  // between points and a lone point simply renders where it is.
  return Array.from(byHour.values());
}

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

/**
 * Write the current and previous hour for every live aircraft.
 *
 * The conflict clause is where the care is:
 *
 *  - `points` normally REPLACES, because the incoming slice is the same hour
 *    re-cut from the authoritative in-memory buffer, including whatever
 *    simplification has since been applied to it.
 *  - It APPENDS instead when the incoming slice begins after the stored one
 *    ended. That only happens when the process restarted mid-hour and started
 *    a fresh buffer: replacing there would throw away the part of the hour
 *    recorded before the restart.
 *  - Identity fields only improve — COALESCE(new, old), never the reverse, or
 *    a record that momentarily forgot a callsign would erase it.
 *  - `sources` unions, so an hour heard by two receivers lists both. Written
 *    with containment operators rather than ARRAY(SELECT DISTINCT unnest(...)):
 *    plain operators, no subquery inside ON CONFLICT, and it converges — the
 *    concat branch can fire at most once, because the next flush's incoming
 *    set is then a subset of what was stored and the first branch keeps it.
 *    The reader de-duplicates, so the single concat cannot show a source
 *    twice.
 */
export async function flushAdsbTracks(
  tracks: ReadonlyArray<ArchivableTrack>,
  nowMs: number = Date.now(),
): Promise<number> {
  const pool = await getPool();
  if (!pool) return 0;

  const live = new Set(tracks.map((t) => t.hex));
  pruneIdentities(live);

  const slices: HourSlice[] = [];
  for (const t of tracks) {
    for (const s of sliceTrailByHour(t.hex, t.points, nowMs)) slices.push(s);
  }
  if (slices.length === 0) return 0;

  let written = 0;
  for (let i = 0; i < slices.length; i += BATCH_SIZE) {
    const batch = slices.slice(i, i + BATCH_SIZE);
    const values: unknown[] = [];
    const tuples = batch.map((s) => {
      const id = identities.get(s.hex);
      const base = values.length;
      values.push(
        s.hex, iso(s.hourMs), iso(s.firstMs), iso(s.lastMs),
        id?.callsign ?? null, id?.reg ?? null, id?.type ?? null, id?.esTag ?? null,
        id ? Array.from(id.sources) : [],
        JSON.stringify(s.points),
      );
      return `($${base + 1}, $${base + 2}::timestamptz, $${base + 3}::timestamptz,`
        + ` $${base + 4}::timestamptz, $${base + 5}, $${base + 6}, $${base + 7},`
        + ` $${base + 8}, $${base + 9}::text[], $${base + 10}::jsonb)`;
    });

    try {
      await pool.query(
        `INSERT INTO adsb_tracks
           (hex, hour_bucket, first_seen, last_seen, callsign, reg, type, es_tag, sources, points)
         VALUES ${tuples.join(', ')}
         ON CONFLICT (hex, hour_bucket) DO UPDATE SET
           points = CASE
             WHEN EXCLUDED.first_seen > adsb_tracks.last_seen
              AND jsonb_array_length(adsb_tracks.points) < ${MAX_POINTS_PER_HOUR}
             THEN adsb_tracks.points || EXCLUDED.points
             ELSE EXCLUDED.points
           END,
           first_seen = LEAST(adsb_tracks.first_seen, EXCLUDED.first_seen),
           last_seen  = GREATEST(adsb_tracks.last_seen, EXCLUDED.last_seen),
           callsign   = COALESCE(EXCLUDED.callsign, adsb_tracks.callsign),
           reg        = COALESCE(EXCLUDED.reg, adsb_tracks.reg),
           type       = COALESCE(EXCLUDED.type, adsb_tracks.type),
           es_tag     = COALESCE(EXCLUDED.es_tag, adsb_tracks.es_tag),
           sources    = CASE
             WHEN EXCLUDED.sources <@ adsb_tracks.sources THEN adsb_tracks.sources
             WHEN adsb_tracks.sources <@ EXCLUDED.sources THEN EXCLUDED.sources
             ELSE adsb_tracks.sources || EXCLUDED.sources
           END`,
        values,
      );
      written += batch.length;
    } catch (err) {
      // One interval of positions, at worst. Same posture as flushAdsbDaily:
      // a gap in a coverage picture is preferable to a retry queue, and the
      // next flush re-cuts the same hour from memory anyway.
      log.debug({ err, rows: batch.length }, 'adsb track flush failed');
    }
  }
  return written;
}

/**
 * Called once per poll. Rate-limits itself to FLUSH_INTERVAL_MS rather than
 * being scheduled separately, so the archive can only ever write trails the
 * poller has finished updating.
 */
export function maybeFlushAdsbTracks(
  tracks: ReadonlyArray<ArchivableTrack>,
  nowMs: number = Date.now(),
): void {
  if (flushing) return;
  if (nowMs - lastFlushMs < FLUSH_INTERVAL_MS) return;
  lastFlushMs = nowMs;
  flushing = true;
  void flushAdsbTracks(tracks, nowMs)
    .catch((err) => log.debug({ err }, 'adsb track flush threw'))
    .finally(() => { flushing = false; });
}

/** TEST-ONLY. */
export function _resetAdsbTrackArchive(): void {
  identities.clear();
  lastFlushMs = 0;
  flushing = false;
}
