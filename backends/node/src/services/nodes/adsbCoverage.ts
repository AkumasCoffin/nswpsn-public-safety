/**
 * Each ADS-B receiver's coverage envelope: how far it hears, per bearing.
 *
 * The picture the staff card and the owner modal have always been trying to
 * draw, finally stored rather than rebuilt from scratch on every restart. Its
 * own caption states the question — "which bearings this antenna actually
 * hears, and how far. A gap in the fan is an obstruction; a short radius on one
 * bearing is terrain" — and the answer to that is one maximum per bearing, not
 * a track archive.
 *
 * Accumulation is essentially free. The ingest path already walks every
 * aircraft in every upload measuring a great-circle distance from the pin (see
 * snapshotMaxRangeKm); this adds a bearing to the same loop and keeps the
 * furthest per 5-degree slice.
 */
import { getPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';
import { formatSydneyNaive } from '../../lib/sydneyTime.js';
import { distanceKm } from './adsbNodeStore.js';

/** 5-degree slices. Fine enough to show a blocked lobe, coarse enough that a
 *  single hour of ordinary traffic starts to fill the circle in. */
export const COVERAGE_BUCKETS = 72;
const DEGREES_PER_BUCKET = 360 / COVERAGE_BUCKETS;

/** One day's envelope: km per bearing, null where nothing was heard. A null is
 *  the gap in the fan, so it is never collapsed to zero. */
export type Envelope = Array<number | null>;

export function emptyEnvelope(): Envelope {
  return new Array<number | null>(COVERAGE_BUCKETS).fill(null);
}

/**
 * Initial great-circle bearing from the receiver to the aircraft, in degrees
 * clockwise from true north.
 *
 * Great-circle rather than flat: at 250 nm the two disagree by enough to put a
 * target a whole bucket out, which on a picture whose entire purpose is
 * "which direction" is the one error worth avoiding.
 */
export function bearingDeg(
  lat1: number, lon1: number, lat2: number, lon2: number,
): number {
  const toRad = Math.PI / 180;
  const φ1 = lat1 * toRad, φ2 = lat2 * toRad;
  const Δλ = (lon2 - lon1) * toRad;
  const y = Math.sin(Δλ) * Math.cos(φ2);
  const x = Math.cos(φ1) * Math.sin(φ2) - Math.sin(φ1) * Math.cos(φ2) * Math.cos(Δλ);
  return (Math.atan2(y, x) / toRad + 360) % 360;
}

/** Which slice a bearing falls in. 359.9° must land in the last bucket, not
 *  one past the end of the array. */
export function bucketOf(bearing: number): number {
  const i = Math.floor(((bearing % 360) + 360) % 360 / DEGREES_PER_BUCKET);
  return Math.min(COVERAGE_BUCKETS - 1, Math.max(0, i));
}

/** Element-wise max. Used to merge a day's rows into one picture, and to merge
 *  what the database already holds back into memory after a restart. */
export function mergeEnvelopes(into: Envelope, from: readonly (number | null)[]): Envelope {
  for (let i = 0; i < COVERAGE_BUCKETS; i += 1) {
    const v = from[i];
    if (typeof v !== 'number' || !Number.isFinite(v)) continue;
    const cur = into[i];
    if (cur === null || cur === undefined || v > cur) into[i] = v;
  }
  return into;
}

interface Pending {
  /** The CORROBORATED picture: what has been written, plus what this process
   *  has seen twice. This is what gets stored and drawn. */
  envelope: Envelope;
  /** Highest and second-highest single observation per bearing, this process,
   *  today. Only the second-highest is ever promoted into `envelope`.
   *
   *  A running maximum over single observations is not robust, and ADS-B gives
   *  it plenty to go wrong with: a CPR decode error puts an aircraft tens or
   *  hundreds of kilometres from where it is, once. Taking the max meant one
   *  such fix set that bearing's reach permanently, with nothing able to lower
   *  it again — a lone spike over airspace the receiver has never heard.
   *
   *  Requiring a second observation costs almost nothing real: an aircraft in
   *  range reports every few seconds, so genuine reach is corroborated within
   *  moments of being achieved. A decode error is not. */
  top1: Envelope;
  top2: Envelope;
  /** False until the row already in Postgres has been merged in. A restart
   *  mid-day starts with an empty envelope, and writing that straight out
   *  would replace the morning's picture with the afternoon's. */
  hydrated: boolean;
}

/** Keyed `${nodeId}|${day}`, the same shape accumulateAdsbDaily uses. */
const pending = new Map<string, Pending>();

/** Sydney-local `YYYY-MM-DD` — the day boundary the rest of the site uses. */
function sydneyDay(ms: number): string {
  return formatSydneyNaive(ms).slice(0, 10);
}

/**
 * Fold one upload into the receiver's envelope, and return the furthest
 * aircraft in it.
 *
 * Returns null — and records nothing — when the node has no antenna pin. There
 * is nothing to measure a bearing or a distance FROM, and the UI already
 * explains that case as "no pin" rather than as a fault.
 */
export function foldCoverage(
  nodeId: string,
  lat: unknown,
  lon: unknown,
  records: ReadonlyArray<{ lat: number; lon: number }>,
  nowMs: number = Date.now(),
): number | null {
  if (typeof lat !== 'number' || typeof lon !== 'number') return null;

  const key = `${nodeId}|${sydneyDay(nowMs)}`;
  let cur = pending.get(key);
  if (!cur) {
    cur = {
      envelope: emptyEnvelope(),
      top1: emptyEnvelope(),
      top2: emptyEnvelope(),
      hydrated: false,
    };
    pending.set(key, cur);
  }

  let max: number | null = null;
  for (const r of records) {
    if (!Number.isFinite(r.lat) || !Number.isFinite(r.lon)) continue;
    const km = distanceKm(lat, lon, r.lat, r.lon);
    if (max === null || km > max) max = km;
    const i = bucketOf(bearingDeg(lat, lon, r.lat, r.lon));
    // Keep the top two observations for this bearing; only the runner-up is
    // ever believed. See Pending.top1 for why.
    const t1 = cur.top1[i];
    if (t1 === null || t1 === undefined || km > t1) {
      cur.top2[i] = t1 ?? null;
      cur.top1[i] = km;
    } else {
      const t2 = cur.top2[i];
      if (t2 === null || t2 === undefined || km > t2) cur.top2[i] = km;
    }
  }
  return max;
}

/**
 * Write the accumulated envelopes.
 *
 * Unlike the daily counters, entries are NOT dropped from `pending` after a
 * flush: the envelope is a running maximum, so memory stays the authoritative
 * copy for the rest of the day and each flush simply restates it. That also
 * makes a failed write cost nothing — the next pass writes the same picture,
 * only better.
 *
 * The hydrate step is the one subtlety. Before the first write of a given
 * (node, day) this process performs, whatever Postgres already holds for that
 * row is merged in, so restarting the backend at noon cannot overwrite the
 * morning with the afternoon. After that, memory is a superset of the row and
 * a plain upsert-replace is correct — which is why there is no element-wise
 * max in SQL here, and specifically no EXCLUDED inside a subquery, the same
 * construct deliberately avoided in the track archive.
 */
export async function flushAdsbCoverage(): Promise<number> {
  if (pending.size === 0) return 0;
  const pool = await getPool();
  if (!pool) return 0;

  let written = 0;
  for (const [key, cur] of Array.from(pending.entries())) {
    const sep = key.lastIndexOf('|');
    const nodeId = key.slice(0, sep);
    const day = key.slice(sep + 1);
    try {
      if (!cur.hydrated) {
        const prev = await pool.query<{ buckets: unknown }>(
          'SELECT buckets FROM node_adsb_coverage WHERE node_id = $1 AND day = $2::date',
          [nodeId, day],
        );
        const held = prev.rows[0]?.buckets;
        if (Array.isArray(held)) mergeEnvelopes(cur.envelope, held as (number | null)[]);
        cur.hydrated = true;
      }
      // Promote only what has been seen twice. An observation still sitting in
      // top1 alone is uncorroborated and is not written.
      mergeEnvelopes(cur.envelope, cur.top2);
      await pool.query(
        `INSERT INTO node_adsb_coverage (node_id, day, buckets)
         VALUES ($1, $2::date, $3::jsonb)
         ON CONFLICT (node_id, day) DO UPDATE SET buckets = EXCLUDED.buckets`,
        [nodeId, day, JSON.stringify(cur.envelope)],
      );
      written += 1;
    } catch (err) {
      // A deleted node (FK violation) or a transient blip. Left in `pending`,
      // so the next pass retries with the same or a better picture.
      log.debug({ err, nodeId, day }, 'adsb coverage flush failed');
    }
  }

  // Yesterday's entries are finished once the day has rolled over; dropping
  // them is what stops this map growing for the life of the process.
  const today = sydneyDay(Date.now());
  for (const key of Array.from(pending.keys())) {
    if (!key.endsWith(`|${today}`)) pending.delete(key);
  }
  return written;
}

export interface CoverageView {
  /** Union over the whole window — the receiver's demonstrated reach. */
  buckets: Envelope;
  /** Union over the last 24 hours. Drawn against `buckets`, a bearing that
   *  appears in one and not the other is a lobe that has stopped working. */
  recent: Envelope;
  /** How many days the envelope is built from. One quiet day is not a
   *  coverage picture, and the caption needs to be able to say so. */
  daysCovered: number;
  /** Furthest bearing on the long envelope, km. Null if nothing is stored. */
  maxKm: number | null;
}

/**
 * One receiver's stored envelope over a trailing window.
 *
 * Reads the day rows and unions them here rather than in SQL: 72-element
 * arrays across at most a month is a trivial amount of work, and doing it in
 * TypeScript keeps the null-versus-zero distinction — a bearing nothing was
 * ever heard on is not a bearing with a range of zero — which a SQL GREATEST
 * over jsonb would quietly flatten.
 */
export async function adsbCoverageFor(
  nodeId: string,
  days: number,
  nowMs: number = Date.now(),
): Promise<CoverageView | null> {
  const pool = await getPool();
  if (!pool) return null;

  const r = await pool.query<{ day: string; buckets: unknown }>(
    `SELECT to_char(day, 'YYYY-MM-DD') AS day, buckets
       FROM node_adsb_coverage
      WHERE node_id = $1
        AND day > (now() AT TIME ZONE 'Australia/Sydney')::date - $2::int
      ORDER BY day`,
    [nodeId, days],
  );

  const buckets = emptyEnvelope();
  const recent = emptyEnvelope();
  const today = sydneyDay(nowMs);
  const yesterday = sydneyDay(nowMs - 86_400_000);

  for (const row of r.rows) {
    if (!Array.isArray(row.buckets)) continue;
    const day = row.buckets as (number | null)[];
    mergeEnvelopes(buckets, day);
    if (row.day === today || row.day === yesterday) mergeEnvelopes(recent, day);
  }

  // Today's picture is still accumulating in memory and has not necessarily
  // been flushed yet, so fold it in — otherwise a freshly-started receiver
  // shows nothing for its first minute.
  const live = pending.get(`${nodeId}|${today}`);
  if (live) {
    // top2, not top1: the read applies the same corroboration the write does,
    // so a bad fix cannot show for the minute before the next flush either.
    for (const env of [live.envelope, live.top2]) {
      mergeEnvelopes(buckets, env);
      mergeEnvelopes(recent, env);
    }
  }

  let maxKm: number | null = null;
  for (const v of buckets) {
    if (v !== null && (maxKm === null || v > maxKm)) maxKm = v;
  }

  return { buckets, recent, daysCovered: r.rows.length, maxKm };
}

/** Forget a deleted node's in-flight envelope. The stored rows go with the
 *  node itself, by ON DELETE CASCADE. */
export function clearAdsbCoverage(nodeId: string): void {
  for (const key of Array.from(pending.keys())) {
    if (key.startsWith(`${nodeId}|`)) pending.delete(key);
  }
}

/** TEST-ONLY. */
export function _resetAdsbCoverage(): void {
  pending.clear();
}
