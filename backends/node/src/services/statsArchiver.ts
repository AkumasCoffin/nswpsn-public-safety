/**
 * Periodic snapshot writer for `stats_snapshots`.
 *
 * Mirrors python's `archive_stats` + `collect_stats_for_archive` at
 * external_api_proxy.py:2725, 2755. Every 5 min while at least one
 * page is being viewed (or unconditionally idle, depending on mode),
 * we capture the same `{power, traffic, emergency, environment}`
 * blob /api/stats/summary returns and persist it. /api/stats/history
 * reads back from this table for the dashboard time-series chart —
 * empty until this writer runs at least once.
 *
 * Cadence: 5 min — matches python. Adds negligible DB load (one
 * INSERT every 300s) and gives the chart 24h × 12 = 288 datapoints
 * over a day, which is plenty for a sparse time-series.
 *
 * Old rows are pruned after 7 days so the table doesn't grow
 * unbounded; 7×288 = ~2k rows is a comfortable upper bound.
 */
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import {
  summariseEmergency,
  summariseEnvironment,
  summarisePower,
  summariseTraffic,
} from '../api/stats.js';

let timer: NodeJS.Timeout | null = null;
let lastWriteAt = 0;
let consecutiveErrors = 0;

const ARCHIVE_INTERVAL_MS = 5 * 60_000; // 5 min, matches python
const RETENTION_DAYS = 7;

export interface StatsSnapshot {
  power: ReturnType<typeof summarisePower>;
  traffic: ReturnType<typeof summariseTraffic>;
  emergency: ReturnType<typeof summariseEmergency>;
  environment: ReturnType<typeof summariseEnvironment>;
}

/** Build a snapshot from current LiveStore. Same shape as
 *  /api/stats/summary's response (minus the `timestamp` field, which
 *  is the row's own `ts` column). */
export function buildStatsSnapshot(): StatsSnapshot {
  return {
    power: summarisePower(),
    traffic: summariseTraffic(),
    emergency: summariseEmergency(),
    environment: summariseEnvironment(),
  };
}

/** Insert one snapshot row. Schema matches python's
 *  `(timestamp BIGINT ms, data JSONB)` — see migration 009 for the
 *  rationale. */
export async function writeStatsSnapshot(): Promise<boolean> {
  const pool = await getPool();
  if (!pool) return false;
  const data = buildStatsSnapshot();
  const tsMs = Date.now();
  try {
    await pool.query(
      `INSERT INTO stats_snapshots ("timestamp", data) VALUES ($1, $2::jsonb)`,
      [tsMs, JSON.stringify(data)],
    );
    lastWriteAt = Math.floor(tsMs / 1000);
    consecutiveErrors = 0;
    return true;
  } catch (err) {
    consecutiveErrors += 1;
    if (consecutiveErrors === 1 || consecutiveErrors % 10 === 0) {
      log.warn(
        { err: (err as Error).message, consecutiveErrors },
        'stats archiver: write failed',
      );
    }
    return false;
  }
}

/** Drop snapshots older than the retention window. Keeps the table
 *  bounded (~2k rows steady-state). */
export async function pruneOldSnapshots(): Promise<number> {
  const pool = await getPool();
  if (!pool) return 0;
  const cutoffMs = Date.now() - RETENTION_DAYS * 86_400_000;
  try {
    const result = await pool.query(
      `DELETE FROM stats_snapshots WHERE "timestamp" < $1`,
      [cutoffMs],
    );
    return result.rowCount ?? 0;
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'stats archiver: prune failed',
    );
    return 0;
  }
}

/** Start the periodic archiver. Idempotent. */
export function startStatsArchiver(intervalMs: number = ARCHIVE_INTERVAL_MS): void {
  if (timer) return;
  // First write a few seconds after boot so the chart isn't empty
  // through the first 5-min window.
  setTimeout(() => void writeStatsSnapshot(), 30_000).unref?.();
  timer = setInterval(async () => {
    await writeStatsSnapshot();
    // Prune once an hour-ish (every 12 ticks at 5 min cadence).
    if (Math.floor(Date.now() / intervalMs) % 12 === 0) {
      const dropped = await pruneOldSnapshots();
      if (dropped > 0) {
        log.info({ dropped }, 'stats archiver: pruned old snapshots');
      }
    }
  }, intervalMs);
  timer.unref?.();
}

export function stopStatsArchiver(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}

export function statsArchiverLastWriteAt(): number {
  return lastWriteAt;
}
