/**
 * How much of the time a node was actually there.
 *
 * Every kind reports the same way — the WS status heartbeat is common to radio,
 * pager and ADS-B — so one presence record serves all of them, and ADS-B adds
 * its HTTP uploads on top because a receiver can be uploading perfectly with a
 * dropped socket.
 *
 * Presence is stored as a sixty-bit mask per node-hour (migration 108), one bit
 * per minute. Merging is then an OR, which matters more than it looks: a
 * counter would have to know which minutes it had already counted, and after a
 * backend restart the in-memory tally starts again at zero. Adding it would
 * double-count the overlap and taking the larger would discard everything
 * before the restart. Setting the same bit twice is just the same bit.
 *
 * Two questions get answered from the one record: what fraction of a window the
 * node was up, and how long it has been up right now. Deriving the second from
 * the same stored minutes — rather than from the live socket's connect time —
 * is deliberate: a backend deploy drops every socket, and an uptime that resets
 * whenever WE restart is measuring the wrong machine.
 */
import { getPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';

const HOUR_MS = 3_600_000;
const MINUTE_MS = 60_000;

/** Pending bits, keyed `${nodeId}|${hourStartMs}`. */
const pending = new Map<string, bigint>();

function hourStartMs(ms: number): number {
  return Math.floor(ms / HOUR_MS) * HOUR_MS;
}

/**
 * Record that a node was heard from.
 *
 * Cheap enough to call on every heartbeat and every upload: it sets one bit in
 * a map. The write happens on the flush timer.
 */
export function markNodeSeen(nodeId: string, atMs: number = Date.now()): void {
  if (!nodeId) return;
  const hour = hourStartMs(atMs);
  const minute = Math.floor((atMs - hour) / MINUTE_MS);
  const key = `${nodeId}|${hour}`;
  pending.set(key, (pending.get(key) ?? 0n) | (1n << BigInt(minute)));
}

/**
 * Merge the pending bits into Postgres.
 *
 * Cleared before the query, like the other ADS-B flushes: losing a minute of
 * presence is a rounding error on an availability figure, and the OR makes a
 * re-send harmless anyway if one ever happens.
 */
export async function flushNodeUptime(): Promise<void> {
  if (pending.size === 0) return;
  const batch = Array.from(pending.entries());
  pending.clear();

  const pool = await getPool();
  if (!pool) return;

  for (const [key, mask] of batch) {
    const sep = key.lastIndexOf('|');
    const nodeId = key.slice(0, sep);
    const hourMs = Number(key.slice(sep + 1));
    try {
      await pool.query(
        `INSERT INTO node_uptime_hourly (node_id, hour, seen_mask)
         VALUES ($1, $2::timestamptz, $3::bigint)
         ON CONFLICT (node_id, hour) DO UPDATE SET
           seen_mask = node_uptime_hourly.seen_mask | EXCLUDED.seen_mask`,
        // A bigint crosses the wire as text: 60 bits does not survive a
        // round trip through a JS number.
        [nodeId, new Date(hourMs).toISOString(), mask.toString()],
      );
    } catch (err) {
      // A deleted node (FK violation) or a transient blip.
      log.debug({ err, nodeId, hourMs }, 'node uptime flush failed');
    }
  }
}

/** Minutes present in a mask. */
function popcount(mask: bigint): number {
  let n = 0;
  let m = mask;
  while (m > 0n) {
    if (m & 1n) n += 1;
    m >>= 1n;
  }
  return n;
}

export interface NodeUptime {
  /** Fraction of the window the node was present, 0-1. Null when the window
   *  holds no observation at all — a node enrolled ten minutes ago has no
   *  availability, which is not the same as 0%. */
  pct: number | null;
  minutesUp: number;
  minutesWindow: number;
  /** How long the node has been continuously present, in ms. Null when it is
   *  not present now. */
  currentRunMs: number | null;
  /** Per-hour availability, oldest first, for a chart or a sparkline. */
  series: Array<{ hour: string; pct: number }>;
}

const WINDOW_HOURS: Record<string, number> = {
  '24h': 24,
  '7d': 24 * 7,
  '30d': 24 * 30,
};

export function uptimeWindowHours(window: string): number {
  return WINDOW_HOURS[window] ?? WINDOW_HOURS['24h']!;
}

/**
 * Fold stored masks (plus whatever has not been flushed yet) into an answer.
 *
 * Exported for the tests, and because the many-node reader below shares it.
 */
export function foldUptime(
  rows: Array<{ hourMs: number; mask: bigint }>,
  hours: number,
  nowMs: number,
  nodeId?: string,
): NodeUptime {
  const endHour = hourStartMs(nowMs);
  const startHour = endHour - (hours - 1) * HOUR_MS;

  const byHour = new Map<number, bigint>();
  for (const r of rows) byHour.set(r.hourMs, (byHour.get(r.hourMs) ?? 0n) | r.mask);
  // Fold in this minute's own bits, so a freshly started node is not reported
  // as having never been seen for up to a minute after it arrives.
  if (nodeId) {
    for (const [key, mask] of pending) {
      const sep = key.lastIndexOf('|');
      if (key.slice(0, sep) !== nodeId) continue;
      const h = Number(key.slice(sep + 1));
      byHour.set(h, (byHour.get(h) ?? 0n) | mask);
    }
  }

  // The window's last hour is only partly elapsed, so counting its unreached
  // minutes as downtime would peg every node below 100%.
  const elapsedInThisHour = Math.floor((nowMs - endHour) / MINUTE_MS) + 1;

  let minutesUp = 0;
  let minutesWindow = 0;
  const series: Array<{ hour: string; pct: number }> = [];
  for (let h = startHour; h <= endHour; h += HOUR_MS) {
    const cap = h === endHour ? Math.min(60, Math.max(1, elapsedInThisHour)) : 60;
    const mask = byHour.get(h) ?? 0n;
    // Ignore bits beyond the elapsed part of the current hour.
    const capped = h === endHour ? mask & ((1n << BigInt(cap)) - 1n) : mask;
    const up = popcount(capped);
    minutesUp += up;
    minutesWindow += cap;
    series.push({ hour: new Date(h).toISOString(), pct: up / cap });
  }

  // Walk back minute by minute from now for the current unbroken run. One
  // missed minute ends it: heartbeats are every fifteen seconds, so a minute
  // with nothing in it is a node that was genuinely not there.
  let currentRunMs: number | null = null;
  const nowMinute = Math.floor(nowMs / MINUTE_MS);
  const seenAt = (minuteIdx: number): boolean => {
    const ms = minuteIdx * MINUTE_MS;
    const h = hourStartMs(ms);
    const bit = BigInt(Math.floor((ms - h) / MINUTE_MS));
    return ((byHour.get(h) ?? 0n) >> bit & 1n) === 1n;
  };
  // The current minute may legitimately be empty for a few seconds, so the run
  // is allowed to start at the previous one.
  let cursor = seenAt(nowMinute) ? nowMinute : (seenAt(nowMinute - 1) ? nowMinute - 1 : NaN);
  if (!Number.isNaN(cursor)) {
    const runEnd = cursor;
    while (seenAt(cursor - 1) && runEnd - cursor < hours * 60) cursor -= 1;
    currentRunMs = (runEnd - cursor + 1) * MINUTE_MS;
  }

  return {
    // No observation at all is not the same as nought per cent: a node enrolled
    // ten minutes ago has no availability figure yet, and showing it as 0%
    // would accuse it of being down.
    pct: byHour.size === 0 || minutesWindow === 0 ? null : minutesUp / minutesWindow,
    minutesUp,
    minutesWindow,
    currentRunMs,
    series,
  };
}

interface Row { hour: Date; seen_mask: string | number | bigint }

function toRows(rows: Row[]): Array<{ hourMs: number; mask: bigint }> {
  return rows.map((r) => ({ hourMs: r.hour.getTime(), mask: BigInt(r.seen_mask ?? 0) }));
}

/** One node's uptime over a window. */
export async function nodeUptime(
  nodeId: string,
  window: string,
  nowMs: number = Date.now(),
): Promise<NodeUptime | null> {
  const pool = await getPool();
  if (!pool) return null;
  const hours = uptimeWindowHours(window);
  try {
    const r = await pool.query<Row>(
      `SELECT hour, seen_mask FROM node_uptime_hourly
        WHERE node_id = $1 AND hour >= $2::timestamptz
        ORDER BY hour`,
      [nodeId, new Date(nowMs - hours * HOUR_MS).toISOString()],
    );
    return foldUptime(toRows(r.rows), hours, nowMs, nodeId);
  } catch (err) {
    log.warn({ err, nodeId }, 'node uptime unavailable');
    return null;
  }
}

/**
 * Uptime for several nodes at once, for the list views.
 *
 * One query rather than one per node: the feeder page draws every node a user
 * owns, and the staff list draws the fleet.
 */
export async function nodeUptimeMany(
  nodeIds: readonly string[],
  window: string,
  nowMs: number = Date.now(),
): Promise<Map<string, NodeUptime>> {
  const out = new Map<string, NodeUptime>();
  if (nodeIds.length === 0) return out;
  const pool = await getPool();
  if (!pool) return out;
  const hours = uptimeWindowHours(window);
  try {
    const r = await pool.query<Row & { node_id: string }>(
      `SELECT node_id, hour, seen_mask FROM node_uptime_hourly
        WHERE node_id = ANY($1::text[]) AND hour >= $2::timestamptz
        ORDER BY node_id, hour`,
      [nodeIds as string[], new Date(nowMs - hours * HOUR_MS).toISOString()],
    );
    const byNode = new Map<string, Array<{ hourMs: number; mask: bigint }>>();
    for (const row of r.rows) {
      const list = byNode.get(row.node_id) ?? [];
      list.push({ hourMs: row.hour.getTime(), mask: BigInt(row.seen_mask ?? 0) });
      byNode.set(row.node_id, list);
    }
    for (const id of nodeIds) {
      out.set(id, foldUptime(byNode.get(id) ?? [], hours, nowMs, id));
    }
  } catch (err) {
    log.warn({ err }, 'node uptime (many) unavailable');
  }
  return out;
}

/** Same cadence as the other running aggregates: nothing reads these between
 *  passes, and a minute's resolution is the point of the mask. */
const FLUSH_INTERVAL_MS = 60_000;
let flushTimer: NodeJS.Timeout | null = null;

export function startNodeUptimeFlush(): void {
  if (flushTimer) return;
  flushTimer = setInterval(() => {
    void flushNodeUptime().catch(() => {});
  }, FLUSH_INTERVAL_MS);
  flushTimer.unref?.();
}

export async function stopNodeUptimeFlush(): Promise<void> {
  if (flushTimer) {
    clearInterval(flushTimer);
    flushTimer = null;
  }
  // One last write, so a clean shutdown does not drop the minutes since the
  // last pass.
  await flushNodeUptime().catch(() => {});
}

/** Test seam. */
export function _resetNodeUptime(): void {
  pending.clear();
}
