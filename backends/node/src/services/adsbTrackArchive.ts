/**
 * Write aircraft positions down, one row per minute, so the map can be scrubbed
 * back in time (migration 139).
 *
 * Positions are collected here as they are OBSERVED — once per poll from the
 * merged picture, and at full rate from our own receivers' uploads — into a
 * per-minute accumulator. Shortly after a minute closes it is reduced and
 * inserted as one row, and never touched again.
 *
 * It deliberately does NOT read the live trails in sources/adsb.ts. Those are a
 * display structure: they thin everything older than ten minutes and crush a
 * trail past 150 points, so re-cutting history from them (as the previous
 * hour-row archive did every minute) stored a picture that got coarser after it
 * was written, and lost the start of the hour for anything orbiting or fed at
 * 1 Hz. Collecting separately means what is stored is what was heard.
 *
 * Multiple feeder nodes still need no special handling: the poll merges every
 * aggregator and every node by hex before a position is noted, so one aircraft
 * is one track however many receivers heard it.
 */
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { simplifyTrail, type TrackPoint } from '../lib/simplifyTrail.js';
// TYPE-ONLY, deliberately: sources/adsb.ts imports this module at runtime to
// hand positions over, so a runtime import back the other way would close a
// cycle between the poller and the archive.
import type { AdsbAircraft } from '../sources/adsb.js';

const MINUTE_MS = 60_000;

/**
 * How long after a minute closes it is written. Node uploads carry fixes
 * stamped when they were taken, a few seconds before they arrive; the grace
 * lets those land in their own minute. Positions from the poll are stamped
 * when the poll looked, so they are never late.
 */
const WRITE_GRACE_MS = 15_000;

/**
 * A closed minute that failed to write is retried on each poll for this long,
 * then dropped. A gap in history is preferable to a backlog that grows for as
 * long as the database is away.
 */
const RETRY_FOR_MS = 10 * MINUTE_MS;

/**
 * Within a minute, a position is kept when dropping it would move the path by
 * more than this (~50 m), or when 30 s have passed since the last one kept. The
 * second rule is readsb's: a point at least every 30 s whatever the aircraft is
 * doing, so playback can always place it. First and last are always kept.
 */
const REDUCE_EPSILON_DEG = 0.0005;
const REDUCE_MAX_GAP_MS = 30_000;

/** Two fixes this close are the same fix seen twice (matches the trails). */
const SAME_FIX_MS = 400;

/** An identity not heard from in this long is forgotten. */
const IDENTITY_TTL_MS = 30 * MINUTE_MS;

/** adsb_identities rows are re-sent at least this often, so the retention
 *  prune (on updated_at) never takes an aircraft that is still flying. */
const IDENTITY_REFRESH_MS = 24 * 60 * MINUTE_MS;

interface Identity {
  callsign: string | null;
  reg: string | null;
  type: string | null;
  esTag: string | null;
  lastSeenMs: number;
}

interface MinuteAircraft {
  points: TrackPoint[];
  sources: Set<string>;
}

/**
 * Identity per hex, carried across minutes. Identity ARRIVES LATE — an aircraft
 * is usually tracked for a while before it transmits a callsign, and the
 * aggregators differ in how complete their registration databases are — so a
 * minute is stamped with the best identity known when it is written.
 */
const identities = new Map<string, Identity>();

/** minute start (ms) → hex → what was heard in that minute. */
const minutes = new Map<number, Map<string, MinuteAircraft>>();

/** End (exclusive) of the newest minute this process has written. Positions
 *  older than this are late and are dropped. Null until the first write. */
let writtenThroughMs: number | null = null;

/** What this process last wrote to adsb_identities per hex, and when. */
const identitiesWritten = new Map<string, { key: string; atMs: number }>();

let lateDropped = 0;
let flushing: Promise<void> | null = null;
let stopped = false;

function minuteOf(ms: number): number {
  return Math.floor(ms / MINUTE_MS) * MINUTE_MS;
}

/** Add one position to its minute, with the sources that reported it. */
function addPoint(hex: string, p: TrackPoint, sources: readonly string[]): void {
  const m = minuteOf(p[0]);
  if (writtenThroughMs !== null && m < writtenThroughMs) {
    lateDropped += 1;
    return;
  }
  let byHex = minutes.get(m);
  if (!byHex) {
    byHex = new Map();
    minutes.set(m, byHex);
  }
  let ac = byHex.get(hex);
  if (!ac) {
    ac = { points: [], sources: new Set() };
    byHex.set(hex, ac);
  }
  for (const s of sources) ac.sources.add(s);
  ac.points.push(p);
}

/**
 * Fold one poll's OBSERVED aircraft in: identity, sources and position.
 * Estimated (dead-reckoned) records must not be passed in — they move, and an
 * archived position must be one that was heard.
 */
export function noteAdsbPositions(observed: AdsbAircraft[], nowMs: number): void {
  if (stopped) return;
  for (const a of observed) {
    let id = identities.get(a.hex);
    if (!id) {
      id = { callsign: null, reg: null, type: null, esTag: null, lastSeenMs: nowMs };
      identities.set(a.hex, id);
    }
    // Only ever improves: a record that has forgotten the callsign must not
    // blank out one already known.
    if (a.callsign) id.callsign = a.callsign;
    if (a.reg) id.reg = a.reg;
    if (a.type) id.type = a.type;
    if (a.esTag) id.esTag = a.esTag;
    id.lastSeenMs = nowMs;
    // Stamped with when the fix was heard, not when the poll finished: an
    // aggregator position can be ~20 s old by then, and our receivers' points
    // (noteAdsbNodePoints) carry their real times, so a poll-time stamp put
    // it after newer points and the replayed track doubled back.
    const fixMs = nowMs - Math.max(0, Number.isFinite(a.ageSec) ? a.ageSec : 0) * 1000;
    addPoint(a.hex, [Math.round(fixMs), a.lat, a.lon, a.altFt], a.sources);
  }
}

/**
 * Positions one of our receivers carried between its uploads, at the rate its
 * decoder saw them. Stamped when each fix was taken, so they can belong to a
 * minute the poll has moved past. Sources come from the poll, which sees the
 * same node in its merged records.
 */
export function noteAdsbNodePoints(hex: string, points: ReadonlyArray<TrackPoint>): void {
  if (stopped) return;
  for (const p of points) addPoint(hex, [p[0], p[1], p[2], p[3]], []);
}

/**
 * Reduce one aircraft's minute to the points worth storing: time order, one
 * point per fix, then Douglas-Peucker at ~50 m with a point forced at least
 * every 30 s.
 */
export function reduceMinute(points: TrackPoint[]): TrackPoint[] {
  const sorted = [...points].sort((a, b) => a[0] - b[0]);
  const unique: TrackPoint[] = [];
  for (const p of sorted) {
    const prev = unique[unique.length - 1];
    if (prev && (p[0] - prev[0] <= SAME_FIX_MS || (prev[1] === p[1] && prev[2] === p[2]))) {
      continue;
    }
    unique.push(p);
  }
  if (unique.length <= 2) return unique;
  const kept = new Set(simplifyTrail(unique, REDUCE_EPSILON_DEG));
  const out: TrackPoint[] = [];
  let lastKept = -Infinity;
  for (const p of unique) {
    if (kept.has(p) || p[0] - lastKept >= REDUCE_MAX_GAP_MS) {
      out.push(p);
      lastKept = p[0];
    }
  }
  return out;
}

const r5 = (v: number): number => Math.round(v * 1e5) / 1e5;

/** The json stored for one minute. Exported for the backfill's tests. */
export function minuteJson(
  minuteMs: number,
  byHex: Map<string, MinuteAircraft>,
  identityOf: (hex: string) => Identity | undefined,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [hex, ac] of byHex) {
    const pts = reduceMinute(ac.points);
    if (pts.length === 0) continue;
    const id = identityOf(hex);
    out[hex] = {
      c: id?.callsign ?? null,
      r: id?.reg ?? null,
      t: id?.type ?? null,
      e: id?.esTag ?? null,
      s: Array.from(ac.sources),
      p: pts.map((p) => [
        p[0] - minuteMs,
        r5(p[1]),
        r5(p[2]),
        p[3] === null ? null : Math.round(p[3]),
      ]),
    };
  }
  return out;
}

/** Insert one minute under the next free seq. */
async function insertMinute(
  pool: NonNullable<Awaited<ReturnType<typeof getPool>>>,
  minuteMs: number,
  json: Record<string, unknown>,
): Promise<void> {
  await pool.query(
    `INSERT INTO adsb_track_chunks (minute, seq, aircraft)
     SELECT $1::timestamptz, COALESCE(MAX(seq) + 1, 0), $2::json
       FROM adsb_track_chunks
      WHERE minute = $1::timestamptz AND seq >= 0`,
    [new Date(minuteMs).toISOString(), JSON.stringify(json)],
  );
}

/**
 * Send registration/type for aircraft written this pass, but only when they
 * changed since this process last sent them or a day has passed.
 */
async function upsertIdentities(
  pool: NonNullable<Awaited<ReturnType<typeof getPool>>>,
  hexes: Iterable<string>,
  nowMs: number,
): Promise<void> {
  const rows: Array<[string, string | null, string | null]> = [];
  for (const hex of hexes) {
    const id = identities.get(hex);
    if (!id || (!id.reg && !id.type)) continue;
    const key = `${id.reg ?? ''}|${id.type ?? ''}`;
    const prev = identitiesWritten.get(hex);
    if (prev && prev.key === key && nowMs - prev.atMs < IDENTITY_REFRESH_MS) continue;
    rows.push([hex, id.reg, id.type]);
  }
  if (rows.length === 0) return;
  await pool.query(
    `INSERT INTO adsb_identities (hex, reg, type, updated_at)
     SELECT u.hex, u.reg, u.type, now()
       FROM unnest($1::text[], $2::text[], $3::text[]) AS u(hex, reg, type)
     ON CONFLICT (hex) DO UPDATE SET
       reg = COALESCE(EXCLUDED.reg, adsb_identities.reg),
       type = COALESCE(EXCLUDED.type, adsb_identities.type),
       updated_at = now()
     WHERE (adsb_identities.reg, adsb_identities.type)
             IS DISTINCT FROM (COALESCE(EXCLUDED.reg, adsb_identities.reg),
                               COALESCE(EXCLUDED.type, adsb_identities.type))
        OR adsb_identities.updated_at < now() - interval '1 day'`,
    [rows.map((r) => r[0]), rows.map((r) => r[1]), rows.map((r) => r[2])],
  );
  for (const [hex, reg, type] of rows) {
    identitiesWritten.set(hex, { key: `${reg ?? ''}|${type ?? ''}`, atMs: nowMs });
  }
}

/**
 * Write every minute that is ready: closed and past its grace, or — at
 * shutdown — every minute held, including the open one.
 */
async function writeReady(nowMs: number, all: boolean): Promise<void> {
  const pool = await getPool();
  const ready = Array.from(minutes.keys())
    .filter((m) => all || m + MINUTE_MS + WRITE_GRACE_MS <= nowMs)
    .sort((a, b) => a - b);
  if (ready.length === 0) return;
  if (!pool) {
    // Nothing to write to: drop rather than hold positions forever.
    for (const m of ready) minutes.delete(m);
    return;
  }

  const written = new Set<string>();
  for (const m of ready) {
    const byHex = minutes.get(m);
    if (!byHex) continue;
    try {
      const json = minuteJson(m, byHex, (hex) => identities.get(hex));
      if (Object.keys(json).length > 0) await insertMinute(pool, m, json);
      for (const hex of byHex.keys()) written.add(hex);
      minutes.delete(m);
      // Monotonic: a minute written at shutdown, or after a clock step, never
      // moves it back.
      const end = m + MINUTE_MS;
      if (!all && (writtenThroughMs === null || end > writtenThroughMs)) writtenThroughMs = end;
    } catch (err) {
      if (nowMs - (m + MINUTE_MS) > RETRY_FOR_MS) {
        minutes.delete(m);
        log.warn(
          { err: (err as Error).message, minute: new Date(m).toISOString() },
          'adsb track archive: minute dropped after retries',
        );
      } else {
        log.debug({ err: (err as Error).message }, 'adsb track archive: write failed, retrying');
      }
      // Later minutes wait: written out of order, a retry would land behind
      // writtenThroughMs and its positions would read as late.
      break;
    }
  }

  try {
    await upsertIdentities(pool, written, nowMs);
  } catch (err) {
    log.debug({ err: (err as Error).message }, 'adsb identity upsert failed');
  }

  for (const [hex, id] of identities) {
    if (nowMs - id.lastSeenMs > IDENTITY_TTL_MS) {
      identities.delete(hex);
      identitiesWritten.delete(hex);
    }
  }
}

/**
 * Called once per poll. Writes whatever minutes have closed; at most one write
 * pass runs at a time.
 */
export function maybeFlushAdsbTracks(nowMs: number = Date.now()): void {
  if (flushing || stopped) return;
  flushing = writeReady(nowMs, false)
    .catch((err) => log.debug({ err }, 'adsb track archive pass threw'))
    .finally(() => { flushing = null; });
}

/**
 * The end of the newest minute on disk, for the history route's cache rule: a
 * minute is final once written, and not before. Null until the first write.
 */
export function adsbArchivedThroughMs(): number | null {
  return writtenThroughMs;
}

/** Positions that arrived for a minute already written, since start. */
export function adsbArchiveLateDropped(): number {
  return lateDropped;
}

/**
 * Shutdown: wait for any pass in flight, then write everything held, the open
 * minute included. The next process writes its part of that minute under the
 * next seq and the reader unions them.
 */
export async function stopAdsbTrackArchive(): Promise<void> {
  stopped = true;
  if (flushing) await flushing;
  await writeReady(Date.now(), true).catch((err) =>
    log.warn({ err: (err as Error).message }, 'adsb track archive: shutdown write failed'),
  );
}

/** TEST-ONLY. */
export function _resetAdsbTrackArchive(): void {
  identities.clear();
  minutes.clear();
  identitiesWritten.clear();
  writtenThroughMs = null;
  lateDropped = 0;
  flushing = null;
  stopped = false;
}

/** TEST-ONLY: run one write pass and wait for it. */
export async function _flushAdsbTracksNow(nowMs: number, all = false): Promise<void> {
  await writeReady(nowMs, all);
}
