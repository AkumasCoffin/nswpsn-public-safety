/**
 * One ADS-B receiver's view of itself — performance, and coverage.
 *
 * Extracted from the two staff routes in api/node-data.ts so that the owner
 * routes under /api/feeder can answer the same questions from the same code.
 * The alternative was a second copy of these queries behind a different gate,
 * and this codebase has already been bitten twice by exactly that: roleForKind
 * lived in three files until a stale copy revoked healthy pager nodes every
 * sixty seconds, and the feed-target label lived in three places until two of
 * them told pager operators their pages went to rdio.
 *
 * So: one implementation, two gates. The staff routes keep
 * requireRole(canViewNodeData); the owner routes check ownership. Neither owns
 * the logic.
 */
import { getPool } from '../../db/pool.js';
import { log } from '../../lib/log.js';
import { hub } from './hub.js';
import { adsbCoverageFor, type CoverageView } from './adsbCoverage.js';
import { storedNodeTracks } from './nodeTrackArchive.js';
import { DATA_RETENTION_DAYS } from '../../lib/retention.js';
import {
  nodeAdsbTraces,
  nodeAdsbRecentAircraft,
  nodeAdsbObservedRangeKm,
  type NodeTrace,
  type NodeRecentAircraft,
} from './adsbNodeStore.js';

/** How many recent aircraft the view carries. Enough to fill a scrollable
 *  panel; the full 100-row ceiling is for a caller that asks explicitly. */
const VIEW_RECENT_LIMIT = 40;

export type AdsbWindow = '24h' | '7d' | '30d';

/** Days covered by each window, for the daily-aggregate queries. */
function windowDays(window: AdsbWindow): number {
  return window === '24h' ? 1 : window === '7d' ? 7 : 30;
}

/** Postgres returns bigint as a string; Number() handles both. */
function num(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

/** Same, but preserving the difference between "no rows" and "zero".
 *  A receiver that has never reported a range must read as "—", not "0 km". */
function numOrNull(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

export interface AdsbNodeView {
  window: AdsbWindow;
  node: {
    id: string;
    name: string | null;
    kind: string | null;
    /**
     * Whether an antenna position is set — NOT the position itself.
     *
     * Callers only ever need to know whether range figures can exist, because
     * the decoder cannot compute a distance without --lat/--lon. The exact
     * coordinates are somebody's home address and have no business in a view
     * that is scanned, screenshotted and shared; the precise pin stays behind
     * the node's Location control, where opening it is a deliberate act.
     */
    hasPosition: boolean;
  };
  online: boolean;
  live: {
    aircraftNow: number | null;
    msgRate: number | null;
    maxRangeKm: number | null;
    gainNow: number | null;
    queueDepth: number | null;
    uploadsExpired: number | null;
    decoder: string | null;
    configVersion: string | null;
  };
  totals: {
    snapshots: number;
    positions: number;
    maxAircraft: number;
    maxRangeKm: number | null;
    msgRateMax: number | null;
    tracksMax: number | null;
    daysReporting: number;
  };
  days: Array<{
    day: string;
    snapshots: number;
    positions: number;
    maxAircraft: number;
    maxRangeKm: number | null;
    msgRateMax: number | null;
  }>;
  /**
   * The middle tier the totals cannot show: what this receiver heard in the
   * last eight hours.
   *
   * From memory, and folded in here rather than given its own route so the
   * Data tab and the owner modal each stay one request. Empty after a backend
   * restart, which is the accepted cost of not persisting it — see
   * adsbNodeStore.
   */
  recent: NodeRecentAircraft[];
}

/**
 * One receiver's performance: what it is doing right now (live, from the
 * agent's status heartbeat) and how it has been doing (historical, from the
 * node_adsb_daily aggregate).
 *
 * The split matters. Individual aircraft positions are never stored — a fleet
 * uploading every 5s would write millions of rows a day for numbers nobody
 * queries one at a time — so anything instantaneous comes from the heartbeat
 * and anything historical from the daily rollup. There is no middle tier.
 *
 * `positions` counts position REPORTS, not distinct aircraft: one aircraft in
 * range for ten minutes contributes ~120 of them. `maxAircraft` is the honest
 * "how busy did it get" figure because it is unaffected by upload cadence.
 *
 * Returns null only when there is no database. A node that has been DELETED
 * still returns a view — the registry row supplies identity alone, and the
 * figures are worth seeing after the row is gone.
 */
export async function adsbNodeView(
  nodeId: string,
  window: AdsbWindow,
): Promise<AdsbNodeView | null> {
  const pool = await getPool();
  if (!pool) return null;
  const days = windowDays(window);

  const [nodeQ, totalsQ, seriesQ] = await Promise.all([
    pool.query<{ id: string; name: string | null; kind: string | null; lat: unknown; lon: unknown }>(
      'SELECT id, name, kind, lat, lon FROM nodes WHERE id = $1',
      [nodeId],
    ),
    pool.query<{
      snapshots: unknown; positions: unknown; max_aircraft: unknown;
      max_range_km: unknown; msg_rate_max: unknown; tracks_max: unknown; days: unknown;
    }>(
      `SELECT COALESCE(SUM(snapshots), 0)::int    AS snapshots,
              COALESCE(SUM(positions), 0)::bigint AS positions,
              COALESCE(MAX(max_aircraft), 0)::int AS max_aircraft,
              MAX(max_range_km)                   AS max_range_km,
              MAX(msg_rate_max)                   AS msg_rate_max,
              MAX(tracks_max)::int                AS tracks_max,
              COUNT(*)::int                       AS days
         FROM node_adsb_daily
        WHERE node_id = $1 AND day > (now() AT TIME ZONE 'Australia/Sydney')::date - $2::int`,
      [nodeId, days],
    ),
    pool.query<{
      day: string; snapshots: unknown; positions: unknown;
      max_aircraft: unknown; max_range_km: unknown; msg_rate_max: unknown;
    }>(
      // to_char, not the raw date: node-postgres parses a DATE column into a JS
      // Date at LOCAL midnight, so toISOString().slice(0,10) on a server east of
      // UTC yields the previous day. Formatting in SQL removes the server
      // timezone from the answer entirely.
      `SELECT to_char(day, 'YYYY-MM-DD') AS day,
              snapshots, positions, max_aircraft, max_range_km, msg_rate_max
         FROM node_adsb_daily
        WHERE node_id = $1 AND day > (now() AT TIME ZONE 'Australia/Sydney')::date - $2::int
        ORDER BY day`,
      [nodeId, days],
    ),
  ]);

  // The registry row supplies only identity, so a node that has been deleted is
  // still inspectable rather than a 404 — same reasoning as the pager view.
  const node = nodeQ.rows[0] ?? { id: nodeId, name: null, kind: null, lat: null, lon: null };
  const t = totalsQ.rows[0];

  // Live figures come from the agent's 15s heartbeat, so they are absent
  // whenever the node is offline — deliberately null rather than zero, so the
  // UI can say "offline" instead of claiming it is hearing nothing.
  const st = hub.liveStatus(nodeId).status;

  return {
    window,
    node: {
      id: node.id,
      name: node.name,
      kind: node.kind,
      hasPosition: typeof node.lat === 'number' && typeof node.lon === 'number',
    },
    online: hub.isOnline(nodeId),
    live: {
      aircraftNow: st?.adsbAircraftNow ?? null,
      msgRate: st?.adsbMsgRate ?? null,
      // Ours when the decoder does not report one, which is most of the time:
      // dump1090 only computes range with --lat/--lon, and the figure vanishes
      // entirely if anything in that chain is missing. Measured from the last
      // upload, so it is "range now" rather than the decoder's run total.
      maxRangeKm: st?.adsbMaxRangeKm ?? nodeAdsbObservedRangeKm(nodeId),
      gainNow: st?.adsbGainNow ?? null,
      queueDepth: st?.queueDepth ?? null,
      uploadsExpired: st?.uploadsExpired ?? null,
      decoder: st?.components?.['dump1090'] ?? null,
      configVersion: st?.configVersion ?? null,
    },
    totals: {
      snapshots: num(t?.snapshots),
      positions: num(t?.positions),
      maxAircraft: num(t?.max_aircraft),
      maxRangeKm: numOrNull(t?.max_range_km),
      msgRateMax: numOrNull(t?.msg_rate_max),
      tracksMax: numOrNull(t?.tracks_max),
      daysReporting: num(t?.days),
    },
    recent: nodeAdsbRecentAircraft(nodeId, VIEW_RECENT_LIMIT),
    days: seriesQ.rows.map((r) => ({
      day: r.day,
      snapshots: num(r.snapshots),
      positions: num(r.positions),
      maxAircraft: num(r.max_aircraft),
      maxRangeKm: numOrNull(r.max_range_km),
      msgRateMax: numOrNull(r.msg_rate_max),
    })),
  };
}

export interface AdsbNodeTracks {
  nodeId: string;
  site: { lat: number; lon: number; name: string | null; approx: true } | null;
  windowMinutes: number;
  aircraft: number;
  points: number;
  traces: NodeTrace[];
  /**
   * The persisted coverage envelope — the half of this response that survives
   * a restart. `traces` is the live picture and is deliberately NOT stored;
   * this is the accumulated one. Null only when there is no database.
   */
  coverage: CoverageView | null;
}

/** The store retains 8 hours; asking for more gets the full window rather than
 *  a silently short answer. */
export const ADSB_TRACKS_MAX_MINUTES = 480;

/**
 * The paths aircraft took through one receiver's coverage — tar1090's pTracks
 * view, and the most direct answer to "is this antenna hearing in every
 * direction, and how far".
 *
 * THE SITE ROUNDING LIVES HERE, not in the callers. A coverage map needs a
 * centre or a gap in the fan cannot be read as a direction, but that centre
 * must never be the exact antenna position. Putting the rounding inside the
 * shared function is what makes it impossible for a new caller to be written
 * without it — and there is deliberately no flag to skip it. The owner already
 * knows their own pin, so it would buy nothing, and a "give me the real one"
 * parameter is exactly what later gets passed true from somewhere it should
 * not be.
 */
export async function adsbNodeTracks(
  nodeId: string,
  minutes: number,
): Promise<AdsbNodeTracks> {
  const clamped = Number.isFinite(minutes)
    ? Math.min(ADSB_TRACKS_MAX_MINUTES, Math.max(1, Math.round(minutes)))
    : ADSB_TRACKS_MAX_MINUTES;

  const t = nodeAdsbTraces(nodeId, clamped);

  // The tracks were memory-only, so a deploy emptied the eight-hour window and
  // a receiver that had heard sixty aircraft that day showed one. What is on
  // disk covers everything up to the last flush — including before a restart —
  // and memory covers the current minute, so the answer is the union.
  let merged = t.traces;
  let aircraft = t.aircraft;
  let points = t.points;
  try {
    const stored = await storedNodeTracks(nodeId, clamped);
    if (stored.length > 0) {
      const byHex = new Map<string, NodeTrace>();
      for (const s of stored) {
        byHex.set(s.hex, {
          hex: s.hex,
          callsign: s.callsign,
          points: s.points.map((p) => [p[1], p[2]] as [number, number]),
        });
      }
      // Memory wins for an aircraft in both: it is the same data, plus
      // whatever has arrived since the last flush.
      for (const live of t.traces) byHex.set(live.hex, live);
      merged = Array.from(byHex.values()).filter((x) => x.points.length >= 2);
      aircraft = byHex.size;
      points = 0;
      for (const x of byHex.values()) points += x.points.length;
    }
  } catch (err) {
    // A read failure costs the older half of the picture, not the view.
    log.debug({ err, nodeId }, 'stored node tracks unavailable');
  }

  const pool = await getPool();
  let site: AdsbNodeTracks['site'] = null;
  if (pool) {
    const r = await pool.query<{ name: string | null; lat: unknown; lon: unknown }>(
      'SELECT name, lat, lon FROM nodes WHERE id = $1',
      [nodeId],
    );
    const row = r.rows[0];
    if (row && typeof row.lat === 'number' && typeof row.lon === 'number') {
      // ~1 km — far finer than the tens of kilometres a coverage plot is read
      // at, and far coarser than a street.
      site = {
        lat: Math.round(row.lat * 100) / 100,
        lon: Math.round(row.lon * 100) / 100,
        name: row.name,
        approx: true,
      };
    }
  }

  // The envelope spans the whole retention window, not `minutes`: the tracks
  // are "what crossed recently" and the coverage is "what this antenna has
  // ever demonstrated it can hear". Conflating their windows was the original
  // mistake — it is why a restart looked like a receiver that had gone deaf.
  const coverage = await adsbCoverageFor(nodeId, DATA_RETENTION_DAYS);

  return {
    nodeId,
    site,
    windowMinutes: t.windowMinutes,
    aircraft,
    points,
    traces: merged,
    coverage,
  };
}
