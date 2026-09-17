/**
 * Reading the persisted aircraft tracks back — the historical view's query.
 *
 * Answers one question: "where was everything at time T, and how did it get
 * there over the preceding N minutes". Nothing else, and deliberately nothing
 * per-user.
 *
 * THE MULTI-USER PROPERTY IS THE BUCKETING. `at` is snapped to a fixed grid
 * before anything else happens, so ten people scrubbing to the same minute
 * produce the same request and the same cacheable URL. A per-viewer response —
 * bbox-scoped, or snapped to whatever millisecond the slider happened to
 * land on — would give every one of them a private cache entry and turn a
 * shared read into N reads. The client interpolates within the bucket, which
 * is why the coarse grid costs nothing visually.
 */
import { getPool } from '../db/pool.js';
import { DATA_RETENTION_DAYS } from '../lib/retention.js';

/**
 * Grid the requested instant is snapped to.
 *
 * Sixty seconds gives at most 1440 distinct URLs a day. It does NOT limit how
 * finely the view can be scrubbed: the response carries the points either side
 * of the instant and the client interpolates, so a user dragging through a
 * minute reuses one response and still sees continuous movement.
 */
export const HISTORY_BUCKET_MS = 60_000;

/** Default and maximum trailing-track window, minutes. */
export const HISTORY_TRAIL_DEFAULT_MIN = 15;
export const HISTORY_TRAIL_MAX_MIN = 30;

/**
 * Most tracks one response may carry.
 *
 * Australia-wide at a busy hour is well under this; the cap exists so a
 * pathological hour cannot produce a response nobody can render. When it
 * bites, the response says so rather than quietly returning less.
 */
export const HISTORY_MAX_TRACKS = 4000;

const HOUR_MS = 3_600_000;

export interface HistoryTrack {
  hex: string;
  callsign: string | null;
  reg: string | null;
  type: string | null;
  esTag: string | null;
  /** Source ids, including `node:<name>`. Carried so the map's "only my
   *  receiver" filter still works when scrubbing back. */
  sources: string[];
  /** [epochMs, lat, lon, altFt|null], oldest first, clipped to the window. */
  points: Array<[number, number, number, number | null]>;
}

export interface AdsbHistory {
  /** The instant actually served — the request's `at`, snapped. */
  atMs: number;
  trailMinutes: number;
  retentionDays: number;
  /** Oldest instant that can be asked for. */
  oldestMs: number;
  count: number;
  /** True when HISTORY_MAX_TRACKS clipped the result. Never silent. */
  truncated: boolean;
  aircraft: HistoryTrack[];
}

/** Snap an instant to the shared grid. Exported because the route echoes it
 *  and the cache headers key off how old the bucket is. */
export function snapHistoryBucket(atMs: number): number {
  return Math.floor(atMs / HISTORY_BUCKET_MS) * HISTORY_BUCKET_MS;
}

export function clampTrailMinutes(minutes: number): number {
  if (!Number.isFinite(minutes)) return HISTORY_TRAIL_DEFAULT_MIN;
  return Math.min(HISTORY_TRAIL_MAX_MIN, Math.max(0, Math.round(minutes)));
}

export function adsbHistoryOldestMs(nowMs: number = Date.now()): number {
  return nowMs - DATA_RETENTION_DAYS * 86_400_000;
}

interface Row {
  hex: string;
  hour_bucket: Date;
  callsign: string | null;
  reg: string | null;
  type: string | null;
  es_tag: string | null;
  sources: string[] | null;
  points: unknown;
}

/**
 * Every aircraft present between `at - trail` and `at`.
 *
 * Rows are stored one per aircraft per hour, so a window can span two or three
 * buckets and one aircraft can contribute a row from each. They are stitched
 * back into a single track per hex here rather than in the client, because the
 * hour boundary is a storage detail and nothing downstream should have to know
 * about it.
 */
export async function adsbHistoryAt(
  atMsRaw: number,
  trailMinutesRaw: number,
  nowMs: number = Date.now(),
): Promise<AdsbHistory | null> {
  const pool = await getPool();
  const atMs = snapHistoryBucket(atMsRaw);
  const trailMinutes = clampTrailMinutes(trailMinutesRaw);
  const base: Omit<AdsbHistory, 'aircraft' | 'count' | 'truncated'> = {
    atMs,
    trailMinutes,
    retentionDays: DATA_RETENTION_DAYS,
    oldestMs: adsbHistoryOldestMs(nowMs),
  };
  if (!pool) return null;

  const t0 = atMs - trailMinutes * 60_000;
  const t1 = atMs;

  // The hour bound is what makes this an index range scan instead of a table
  // sweep: it is the leading column of idx_adsb_tracks_hour, and it is always
  // one to three values wide. last_seen/first_seen then discard the aircraft
  // that were present in those hours but not during the window itself.
  const hourFrom = Math.floor(t0 / HOUR_MS) * HOUR_MS;
  const hourTo = Math.floor(t1 / HOUR_MS) * HOUR_MS;

  const r = await pool.query<Row>(
    `SELECT hex, hour_bucket, callsign, reg, type, es_tag, sources, points
       FROM adsb_tracks
      WHERE hour_bucket >= $1::timestamptz
        AND hour_bucket <= $2::timestamptz
        AND last_seen  >= $3::timestamptz
        AND first_seen <= $4::timestamptz
      ORDER BY hex, hour_bucket`,
    [
      new Date(hourFrom).toISOString(),
      new Date(hourTo).toISOString(),
      new Date(t0).toISOString(),
      new Date(t1).toISOString(),
    ],
  );

  // Which hour the viewed instant falls in. Sources are recorded per hour, so
  // this is the row whose attribution actually describes the moment on screen.
  const viewedHour = Math.floor(atMs / HOUR_MS) * HOUR_MS;

  const byHex = new Map<string, HistoryTrack>();
  /** Sources from the viewed hour alone, where that hour has a row. */
  const viewedHourSources = new Map<string, string[]>();
  for (const row of r.rows) {
    const hourMs = row.hour_bucket.getTime();
    const raw = Array.isArray(row.points) ? row.points : [];
    if (hourMs === viewedHour && row.sources) {
      viewedHourSources.set(row.hex, Array.from(new Set(row.sources)));
    }

    let track = byHex.get(row.hex);
    if (!track) {
      track = {
        hex: row.hex,
        callsign: row.callsign,
        reg: row.reg,
        type: row.type,
        esTag: row.es_tag,
        // De-duplicated here rather than in SQL: the writer's union can leave
        // one repeat behind (see the CASE in adsbTrackArchive), and a repeated
        // source would double a count nobody wants doubled.
        sources: Array.from(new Set(row.sources ?? [])),
        points: [],
      };
      byHex.set(row.hex, track);
    } else {
      // A later hour may know the callsign the earlier one did not.
      track.callsign = track.callsign ?? row.callsign;
      track.reg = track.reg ?? row.reg;
      track.type = track.type ?? row.type;
      track.esTag = track.esTag ?? row.es_tag;
      if (row.sources) {
        track.sources = Array.from(new Set([...track.sources, ...row.sources]));
      }
    }

    for (const p of raw as Array<[number, number, number, number | null]>) {
      const tMs = hourMs + p[0] * 1000;
      // Clipped here rather than in SQL: the points live inside a jsonb array,
      // so the row-level bounds above can only ever be a coarse filter.
      if (tMs < t0 || tMs > t1) continue;
      track.points.push([tMs, p[1], p[2], p[3]]);
    }
  }

  // Narrow each track's sources to the hour being VIEWED.
  //
  // The union above spans every hour the trail touches, which is up to three.
  // That made the map's receiver filter show aircraft a node had heard at some
  // other point in the window — scrub to 14:05 and something the receiver only
  // caught at 14:55 was still attributed to it, because both instants live in
  // the same union.
  //
  // Per-hour is as fine as this can get: the stored points carry no source of
  // their own, so which receiver heard any individual position is not
  // recoverable. Where the viewed hour has no row for an aircraft — its trail
  // reaches into the window from an earlier hour — the union stays, being the
  // only attribution there is.
  for (const [hex, t] of byHex) {
    const narrowed = viewedHourSources.get(hex);
    if (narrowed) t.sources = narrowed;
  }

  // An aircraft whose every point fell outside the window contributed a row
  // (its hour overlapped) but nothing to draw.
  const all: HistoryTrack[] = [];
  for (const t of byHex.values()) {
    if (t.points.length === 0) continue;
    t.points.sort((a, b) => a[0] - b[0]);
    all.push(t);
  }

  // Truncation keeps whatever was heard most recently, so what survives is the
  // picture closest to the instant asked for.
  const truncated = all.length > HISTORY_MAX_TRACKS;
  if (truncated) {
    all.sort((a, b) => (b.points[b.points.length - 1]![0]) - (a.points[a.points.length - 1]![0]));
    all.length = HISTORY_MAX_TRACKS;
  }

  return { ...base, count: all.length, truncated, aircraft: all };
}
