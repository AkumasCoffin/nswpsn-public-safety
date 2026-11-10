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

const MINUTE_MS = 60_000;

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
  minute: Date;
  aircraft: unknown;
}

/** One aircraft's entry in a stored minute (migration 139). */
interface StoredAircraft {
  c?: string | null;
  r?: string | null;
  t?: string | null;
  e?: string | null;
  s?: string[] | null;
  p?: Array<[number, number, number, number | null]> | null;
}

/**
 * Every aircraft present between `at - trail` and `at`.
 *
 * Rows are one per minute (plus the odd second row for a minute two processes
 * shared across a restart), each holding every aircraft heard in it. A window
 * is at most 31 of them, read by primary key, and each aircraft is stitched
 * back into one track here: the minute boundary is a storage detail and
 * nothing downstream should have to know about it.
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

  const r = await pool.query<Row>(
    `SELECT minute, aircraft
       FROM adsb_track_chunks
      WHERE minute >= $1::timestamptz
        AND minute <= $2::timestamptz
      ORDER BY minute, seq`,
    [
      new Date(Math.floor(t0 / MINUTE_MS) * MINUTE_MS).toISOString(),
      new Date(t1).toISOString(),
    ],
  );

  const byHex = new Map<string, HistoryTrack>();
  /** Per hex: sources of the newest minute it was heard in at or before `at`,
   *  and that minute. Rows arrive oldest first, so the last one wins. */
  const latestSources = new Map<string, { minute: number; sources: Set<string> }>();
  for (const row of r.rows) {
    const minuteMs = row.minute.getTime();
    const raw = row.aircraft;
    if (!raw || typeof raw !== 'object') continue;
    for (const [hex, entry] of Object.entries(raw as Record<string, StoredAircraft>)) {
      if (!entry) continue;
      let track = byHex.get(hex);
      if (!track) {
        track = {
          hex, callsign: null, reg: null, type: null, esTag: null, sources: [], points: [],
        };
        byHex.set(hex, track);
      }
      // The newest non-null wins: identity arrives late and only improves.
      track.callsign = entry.c ?? track.callsign;
      track.reg = entry.r ?? track.reg;
      track.type = entry.t ?? track.type;
      track.esTag = entry.e ?? track.esTag;

      let heardBy = false;
      for (const p of entry.p ?? []) {
        const tMs = minuteMs + p[0];
        if (tMs < t0 || tMs > t1) continue;
        track.points.push([tMs, p[1], p[2], p[3]]);
        heardBy = true;
      }
      // Attribution is per minute: which receivers heard this aircraft in the
      // minute closest to the instant on screen. A minute's second row (two
      // processes sharing it) adds to it rather than replacing it.
      if (heardBy && entry.s && minuteMs <= t1) {
        const cur = latestSources.get(hex);
        if (!cur || minuteMs > cur.minute) {
          latestSources.set(hex, { minute: minuteMs, sources: new Set(entry.s) });
        } else if (minuteMs === cur.minute) {
          for (const s of entry.s) cur.sources.add(s);
        }
      }
    }
  }

  // An aircraft whose every point fell outside the window contributed a row
  // but nothing to draw.
  const all: HistoryTrack[] = [];
  for (const t of byHex.values()) {
    if (t.points.length === 0) continue;
    t.points.sort((a, b) => a[0] - b[0]);
    // Two rows of one minute can both hold a fix seen by both processes.
    t.points = t.points.filter((p, i, arr) => i === 0 || p[0] !== arr[i - 1]![0]);
    t.sources = Array.from(latestSources.get(t.hex)?.sources ?? []);
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
