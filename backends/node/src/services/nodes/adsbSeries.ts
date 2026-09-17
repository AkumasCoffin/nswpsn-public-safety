/**
 * ADS-B performance as a time series, for the charts.
 *
 * node_adsb_daily answers "how has this receiver been doing lately" and is the
 * wrong shape for a graph: on a 24-hour window it is a single point. This reads
 * node_adsb_hourly instead (migration 107), which the store accumulates beside
 * the daily counters.
 *
 * Four metrics, matching what a receiver operator actually tunes against:
 * aircraft seen, how far it reached, how hard it was working, and how loud the
 * traffic was. The first three have always been collected; signal arrives only
 * from agent 0.1.6 on, so it is null on older history and on any receiver still
 * running an older build — the chart has to cope with that rather than draw a
 * line at zero, which in dBFS would mean a deafeningly strong signal.
 */
import { getPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';

/** One bucket. Every metric is nullable: a receiver that was off has an hour
 *  with nothing in it, and a gap is the honest way to draw that. */
export interface AdsbSeriesPoint {
  /** ISO timestamp of the hour's start, UTC. */
  hour: string;
  /** Most aircraft seen at once in the hour. */
  aircraft: number | null;
  /** Furthest aircraft heard in the hour, km. */
  rangeKm: number | null;
  /** Peak decoder message rate in the hour, per second. */
  msgRate: number | null;
  /** Mean receive level over the hour, dBFS (negative). */
  signalDbfs: number | null;
  /** Strongest receive level in the hour, dBFS (negative). */
  signalPeakDbfs: number | null;
  /** Position reports accepted in the hour. */
  positions: number;
}

/** Hours covered by each window pill. */
const WINDOW_HOURS: Record<string, number> = {
  '24h': 24,
  '7d': 24 * 7,
  '30d': 24 * 30,
};

export function adsbSeriesHours(window: string): number {
  return WINDOW_HOURS[window] ?? WINDOW_HOURS['24h']!;
}

interface Row {
  hour: Date;
  max_aircraft: number | null;
  max_range_km: number | null;
  msg_rate_max: number | null;
  signal_sum: number | null;
  signal_n: number | null;
  signal_peak: number | null;
  positions: string | number | null;
}

function toPoint(r: Row): AdsbSeriesPoint {
  const n = Number(r.signal_n ?? 0);
  return {
    hour: r.hour.toISOString(),
    aircraft: r.max_aircraft ?? null,
    rangeKm: r.max_range_km ?? null,
    msgRate: r.msg_rate_max ?? null,
    // The mean is reconstructed from its two halves. Storing an average of
    // averages would have weighted a quiet minute the same as a busy one.
    signalDbfs: n > 0 && r.signal_sum !== null ? Number(r.signal_sum) / n : null,
    signalPeakDbfs: r.signal_peak ?? null,
    positions: Number(r.positions ?? 0),
  };
}

/**
 * Fill the window's missing hours with empty buckets.
 *
 * Without this a receiver that was down for six hours draws a straight line
 * across the gap, which reads as six hours of steady performance. The chart
 * spans a fixed window either way, so the axis stays stable as data arrives.
 */
function padHours(rows: AdsbSeriesPoint[], hours: number, nowMs: number): AdsbSeriesPoint[] {
  const HOUR = 3_600_000;
  const end = Math.floor(nowMs / HOUR) * HOUR;
  const start = end - (hours - 1) * HOUR;
  const byHour = new Map(rows.map((p) => [Date.parse(p.hour), p]));
  const out: AdsbSeriesPoint[] = [];
  for (let t = start; t <= end; t += HOUR) {
    out.push(byHour.get(t) ?? {
      hour: new Date(t).toISOString(),
      aircraft: null, rangeKm: null, msgRate: null,
      signalDbfs: null, signalPeakDbfs: null, positions: 0,
    });
  }
  return out;
}

/** One receiver's series over the window. */
export async function adsbNodeSeries(
  nodeId: string,
  window: string,
  nowMs: number = Date.now(),
): Promise<AdsbSeriesPoint[]> {
  const pool = await getPool();
  if (!pool) return [];
  const hours = adsbSeriesHours(window);
  try {
    const r = await pool.query<Row>(
      `SELECT hour, max_aircraft, max_range_km, msg_rate_max,
              signal_sum, signal_n, signal_peak, positions
         FROM node_adsb_hourly
        WHERE node_id = $1
          AND hour >= $2::timestamptz
        ORDER BY hour`,
      [nodeId, new Date(nowMs - hours * 3_600_000).toISOString()],
    );
    return padHours(r.rows.map(toPoint), hours, nowMs);
  } catch (err) {
    log.warn({ err, nodeId }, 'adsb node series unavailable');
    return [];
  }
}

/**
 * The fleet's series: every ADS-B receiver, one line.
 *
 * Counters sum across receivers but maxima do not — two receivers each reaching
 * 200 km have not between them reached 400, and the same two both seeing the
 * same aircraft have not seen two. So aircraft and range take the largest
 * across the fleet, which is what "best any receiver managed" means, while
 * positions add up. Signal is averaged over the receivers that reported one,
 * weighted by how many readings each contributed.
 */
export async function adsbFleetSeries(
  window: string,
  nowMs: number = Date.now(),
): Promise<AdsbSeriesPoint[]> {
  const pool = await getPool();
  if (!pool) return [];
  const hours = adsbSeriesHours(window);
  try {
    const r = await pool.query<Row>(
      `SELECT hour,
              MAX(max_aircraft)              AS max_aircraft,
              MAX(max_range_km)              AS max_range_km,
              MAX(msg_rate_max)              AS msg_rate_max,
              SUM(signal_sum)                AS signal_sum,
              SUM(signal_n)                  AS signal_n,
              MAX(signal_peak)               AS signal_peak,
              SUM(positions)                 AS positions
         FROM node_adsb_hourly
        WHERE hour >= $1::timestamptz
        GROUP BY hour
        ORDER BY hour`,
      [new Date(nowMs - hours * 3_600_000).toISOString()],
    );
    return padHours(r.rows.map(toPoint), hours, nowMs);
  } catch (err) {
    log.warn({ err }, 'adsb fleet series unavailable');
    return [];
  }
}
