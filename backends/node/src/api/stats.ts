/**
 * Stats / cache / collection-status routes.
 *
 * Combines several "what's in memory right now?" views into a small
 * router. None of these touch Postgres on the request path — they all
 * roll up data already sitting in the LiveStore + poller-health +
 * archive-writer + activity-mode singletons.
 *
 * Routes (mirrors Python in external_api_proxy.py):
 *   /api/stats/history       → line 6250-6290.
 *     Python reads the `stats_snapshots` table for historical chart data.
 *     We don't write that table from Node yet (no archiver for snapshot
 *     summaries). Returning `[]` is the same shape Python emits when the
 *     DB query errors. See TODO at the bottom.
 *   /api/stats/summary       → line 6497-onwards.
 *     A nested rollup keyed by power/traffic/emergency/environment. We
 *     build the power section directly from LiveStore data we own; the
 *     traffic/emergency/environment branches stay zeroed because the
 *     other agent is wiring those sources. They'll fill in as those
 *     LiveStore keys appear — no contract change needed.
 *   /api/cache/status        → line 11675-11694.
 *   /api/cache/stats         → alias of /api/cache/status (Python aliases
 *                              both to the same handler).
 *   /api/collection/status   → line 6467-6494.
 */
import { Hono } from 'hono';
import { liveStore } from '../store/live.js';
import { pollerHealth } from '../services/poller.js';
import { archiveWriter } from '../store/archive.js';
import {
  activeViewerCount,
  dataViewerCount,
  listSessions,
} from '../services/activityMode.js';
import type { EndeavourOutage } from '../sources/endeavour.js';
import type { AusgridOutagesPayload } from '../sources/ausgrid.js';
import type { EssentialOutage } from '../sources/essential.js';
import type { TrafficSnapshot, TrafficCamerasSnapshot } from '../sources/traffic.js';
import type { RfsSnapshot } from '../sources/rfs.js';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import type { BomSnapshot } from '../sources/bom.js';
import type { BeachsafeBeach } from '../sources/beach.js';

export const statsRouter = new Hono();

const ACTIVE_INTERVAL_SECS = 60;
const IDLE_INTERVAL_SECS = 300;
const PAGE_SESSION_TIMEOUT_SECS = 120;

// ---------------------------------------------------------------------------
// /api/stats/history — read 5-min snapshots from stats_snapshots.
// Mirrors python external_api_proxy.py:6250-6290.
// ---------------------------------------------------------------------------

interface StatsHistoryRow {
  ts: Date;
  data: Record<string, unknown>;
}

statsRouter.get('/api/stats/history', async (c) => {
  const url = new URL(c.req.url);
  const hoursParam = url.searchParams.get('hours');
  let hours = Number.parseInt(hoursParam ?? '1', 10);
  if (!Number.isFinite(hours) || hours <= 0) hours = 1;
  hours = Math.min(hours, 168); // 7-day cap, matches python.

  const pool = await getPool();
  if (!pool) return c.json([]);

  try {
    const result = await pool.query<StatsHistoryRow>(
      `SELECT ts, data FROM stats_snapshots
       WHERE ts >= NOW() - ($1 || ' hours')::interval
       ORDER BY ts ASC`,
      [String(hours)],
    );
    // Python emits `timestamp` in JS-compatible milliseconds; preserve
    // that contract so the frontend chart code keeps working.
    return c.json(
      result.rows.map((r) => ({
        timestamp: r.ts.getTime(),
        data: r.data,
      })),
    );
  } catch (err) {
    log.warn({ err: (err as Error).message }, '/api/stats/history failed');
    return c.json({ error: (err as Error).message, data: [] }, 200);
  }
});

// ---------------------------------------------------------------------------
// /api/stats/summary — power roll-up from LiveStore.
// ---------------------------------------------------------------------------

interface PowerSummary {
  ausgrid: { unplanned: number; planned: number; customers_affected: number; total: number };
  endeavour: {
    current: number;
    current_active: number;
    future: number;
    customers_affected: number;
    current_maintenance: number;
  };
  essential: {
    unplanned: number;
    planned: number;
    future: number;
    total: number;
    customers_affected: number;
  };
  total_outages: number;
  total_customers: number;
}

export function summarisePower(): PowerSummary {
  const power: PowerSummary = {
    ausgrid: { unplanned: 0, planned: 0, customers_affected: 0, total: 0 },
    endeavour: {
      current: 0,
      current_active: 0,
      future: 0,
      customers_affected: 0,
      current_maintenance: 0,
    },
    essential: { unplanned: 0, planned: 0, future: 0, total: 0, customers_affected: 0 },
    total_outages: 0,
    total_customers: 0,
  };

  // --- Ausgrid: derived from the markers payload. We don't have the
  // upstream `GetCurrentOutageStats` aggregate banner here, so we
  // approximate from the markers themselves: outageType + customers.
  const ag = liveStore.getData<AusgridOutagesPayload>('ausgrid');
  if (ag && Array.isArray(ag.Markers)) {
    for (const m of ag.Markers) {
      if (m.outageType === 'Planned') power.ausgrid.planned += 1;
      else power.ausgrid.unplanned += 1;
      power.ausgrid.customers_affected += m.customersAffected ?? 0;
    }
    power.ausgrid.total = power.ausgrid.planned + power.ausgrid.unplanned;
    power.total_outages += power.ausgrid.total;
    power.total_customers += power.ausgrid.customers_affected;
  }

  // --- Endeavour: three buckets in LiveStore, summed as Python does.
  const endeavCur = liveStore.getData<EndeavourOutage[]>('endeavour_current') ?? [];
  const endeavMaint = liveStore.getData<EndeavourOutage[]>('endeavour_maintenance') ?? [];
  const endeavFut = liveStore.getData<EndeavourOutage[]>('endeavour_planned') ?? [];
  power.endeavour.current = Array.isArray(endeavCur) ? endeavCur.length : 0;
  power.endeavour.current_maintenance = Array.isArray(endeavMaint) ? endeavMaint.length : 0;
  power.endeavour.future = Array.isArray(endeavFut) ? endeavFut.length : 0;
  // Python's `current_active` reflects the Supabase /rpc/get_outage_statistics
  // active_outages number. Without that call we use the sum of unplanned +
  // current-maintenance as a close proxy.
  power.endeavour.current_active = power.endeavour.current + power.endeavour.current_maintenance;
  // Customers affected: only count CURRENT outages (not future planned).
  // Earlier code summed all three buckets, which over-counts by including
  // future outages that aren't affecting anyone yet, and double-counts
  // when the same id appears in both current and maintenance. Dedup by
  // EndeavourOutage.id and sum across current + currentMaintenance only.
  const seen = new Set<string>();
  let endeavCustomers = 0;
  for (const o of [...endeavCur, ...endeavMaint]) {
    const id = o.id || '';
    if (id && seen.has(id)) continue;
    if (id) seen.add(id);
    endeavCustomers += o.customersAffected || 0;
  }
  power.endeavour.customers_affected = endeavCustomers;
  power.total_outages += power.endeavour.current_active;
  power.total_customers += endeavCustomers;

  // --- Essential: union of the two feeds, classified by outageType.
  const essCurrent = liveStore.getData<EssentialOutage[]>('essential_current') ?? [];
  const essFuture = liveStore.getData<EssentialOutage[]>('essential_future') ?? [];
  const essAll = [...essCurrent, ...essFuture];
  for (const o of essAll) {
    if (o.outageType === 'planned') power.essential.planned += 1;
    else power.essential.unplanned += 1;
    if (o.feedType === 'future') power.essential.future += 1;
    power.essential.customers_affected += o.customersAffected || 0;
  }
  power.essential.total = essAll.length;
  power.total_outages += essAll.length;
  power.total_customers += power.essential.customers_affected;

  return power;
}

// ---------------------------------------------------------------------------
// /api/stats/summary — traffic / emergency / environment roll-ups.
// All three read straight from LiveStore — the upstream calls already
// happened on the poller and the snapshots carry the fields python's
// per-call fetch was extracting (external_api_proxy.py:6634-6776).
// ---------------------------------------------------------------------------

interface TrafficSummary {
  incidents: number;
  crashes: number;
  hazards: number;
  breakdowns: number;
  changed_conditions: number;
  roadwork: number;
  fires: number;
  floods: number;
  major_events: number;
  lga_incidents: number;
  cameras: number;
  total: number;
}

function classifyTrafficIncident(
  props: Record<string, unknown>,
): 'crash' | 'breakdown' | 'hazard' | 'changed_conditions' | null {
  const pickStr = (k: string): string =>
    typeof props[k] === 'string' ? (props[k] as string).toUpperCase() : '';
  const all = [
    pickStr('mainCategory'),
    pickStr('subCategory'),
    pickStr('subCategoryA'),
    pickStr('headline'),
    pickStr('displayName'),
    pickStr('CategoryIcon'),
  ].join(' ');
  if (
    all.includes('CRASH') ||
    all.includes('COLLISION') ||
    all.includes('ROLLOVER')
  ) {
    return 'crash';
  }
  if (
    all.includes('BREAKDOWN') ||
    all.includes('BROKEN DOWN') ||
    all.includes('DISABLED') ||
    all.includes('STALLED')
  ) {
    return 'breakdown';
  }
  if (
    all.includes('HAZARD') ||
    all.includes('DEBRIS') ||
    all.includes('OBSTRUCTION') ||
    all.includes('ANIMAL') ||
    all.includes('OBJECT')
  ) {
    return 'hazard';
  }
  if (all.includes('CHANGED TRAFFIC CONDITIONS')) {
    return 'changed_conditions';
  }
  return null;
}

function isFeatureCollection(
  v: unknown,
): v is { features: Array<Record<string, unknown>> } {
  return (
    !!v &&
    typeof v === 'object' &&
    Array.isArray((v as { features?: unknown[] }).features)
  );
}

/** Count features in a TrafficSnapshot whose properties.ended is falsy
 *  — matches python's `not f.get('properties', f).get('ended', False)`
 *  filter at external_api_proxy.py:6679. */
function countActive(snap: TrafficSnapshot | null): number {
  if (!snap || !Array.isArray(snap.features)) return 0;
  let n = 0;
  for (const f of snap.features) {
    const props = (f as { properties?: Record<string, unknown> }).properties ?? {};
    if (!props['ended']) n += 1;
  }
  return n;
}

export function summariseTraffic(): TrafficSummary {
  const out: TrafficSummary = {
    incidents: 0,
    crashes: 0,
    hazards: 0,
    breakdowns: 0,
    changed_conditions: 0,
    roadwork: 0,
    fires: 0,
    floods: 0,
    major_events: 0,
    lga_incidents: 0,
    cameras: 0,
    total: 0,
  };

  const incidents = liveStore.getData<TrafficSnapshot>('traffic_incidents');
  if (incidents && Array.isArray(incidents.features)) {
    for (const f of incidents.features) {
      const props =
        ((f as unknown as { properties?: Record<string, unknown> }).properties ??
          (f as unknown as Record<string, unknown>));
      if (props['ended']) continue;
      out.incidents += 1;
      const cls = classifyTrafficIncident(props);
      if (cls === 'crash') out.crashes += 1;
      else if (cls === 'breakdown') out.breakdowns += 1;
      else if (cls === 'hazard') out.hazards += 1;
      else if (cls === 'changed_conditions') out.changed_conditions += 1;
    }
  }

  out.roadwork = countActive(liveStore.getData<TrafficSnapshot>('traffic_roadwork'));
  out.fires = countActive(liveStore.getData<TrafficSnapshot>('traffic_fire'));
  out.floods = countActive(liveStore.getData<TrafficSnapshot>('traffic_flood'));
  out.major_events = countActive(
    liveStore.getData<TrafficSnapshot>('traffic_majorevent'),
  );

  const cams = liveStore.getData<TrafficCamerasSnapshot>('traffic_cameras');
  out.cameras = cams && Array.isArray(cams.features) ? cams.features.length : 0;

  out.total =
    out.incidents +
    out.roadwork +
    out.fires +
    out.floods +
    out.major_events;
  return out;
}

interface EmergencySummary {
  rfs_incidents: number;
  rfs_by_level: { emergency_warning: number; watch_and_act: number; advice: number };
  bom_warnings: { land: number; marine: number; total: number };
}

export function summariseEmergency(): EmergencySummary {
  const out: EmergencySummary = {
    rfs_incidents: 0,
    rfs_by_level: { emergency_warning: 0, watch_and_act: 0, advice: 0 },
    bom_warnings: { land: 0, marine: 0, total: 0 },
  };

  const rfs = liveStore.getData<RfsSnapshot>('rfs_incidents');
  if (rfs && Array.isArray(rfs.features)) {
    out.rfs_incidents = rfs.features.length;
    for (const f of rfs.features) {
      const lvl = (f.properties?.alertLevel ?? '').toLowerCase();
      if (lvl.includes('emergency')) out.rfs_by_level.emergency_warning += 1;
      else if (lvl.includes('watch')) out.rfs_by_level.watch_and_act += 1;
      else out.rfs_by_level.advice += 1;
    }
  }

  // BOM snapshot already has a counts object — single read, no walk.
  const bom = liveStore.getData<BomSnapshot>('bom_warnings');
  if (bom && bom.counts) {
    out.bom_warnings.land = bom.counts.land ?? 0;
    out.bom_warnings.marine = bom.counts.marine ?? 0;
    out.bom_warnings.total = out.bom_warnings.land + out.bom_warnings.marine;
  }

  return out;
}

interface EnvironmentSummary {
  beaches_monitored: number;
  beaches_good: number;
  beaches_poor: number;
  beachsafe_patrolled: number;
}

export function summariseEnvironment(): EnvironmentSummary {
  const out: EnvironmentSummary = {
    beaches_monitored: 0,
    beaches_good: 0,
    beaches_poor: 0,
    beachsafe_patrolled: 0,
  };

  const bw = liveStore.getData<unknown>('beachwatch');
  if (isFeatureCollection(bw)) {
    out.beaches_monitored = bw.features.length;
    for (const f of bw.features) {
      const props = (f['properties'] as Record<string, unknown> | undefined) ?? {};
      const result = String(props['latestResult'] ?? '').toLowerCase();
      if (result === 'good' || result === 'excellent') out.beaches_good += 1;
      else if (result === 'bad' || result === 'poor') out.beaches_poor += 1;
    }
  }

  const bs = liveStore.getData<BeachsafeBeach[]>('beachsafe');
  if (Array.isArray(bs)) {
    for (const b of bs) {
      if (b.isPatrolledToday) out.beachsafe_patrolled += 1;
    }
  }

  return out;
}

statsRouter.get('/api/stats/summary', (c) =>
  c.json({
    timestamp: new Date().toISOString().replace(/\.\d+Z$/, 'Z'),
    power: summarisePower(),
    traffic: summariseTraffic(),
    emergency: summariseEmergency(),
    environment: summariseEnvironment(),
  }),
);

// ---------------------------------------------------------------------------
// /api/cache/status (and /api/cache/stats alias) — what's in LiveStore right
// now, plus archive-writer queue depth and recent flush stats.
// ---------------------------------------------------------------------------

interface CacheEntry {
  key: string;
  age_seconds: number | null;
  status: 'fresh' | 'stale';
  size: number;
}

function cacheEntries(): CacheEntry[] {
  const now = Math.floor(Date.now() / 1000);
  const out: CacheEntry[] = [];
  for (const key of liveStore.keys()) {
    const snap = liveStore.get(key);
    if (!snap) continue;
    const age = now - snap.ts;
    out.push({
      key,
      age_seconds: age,
      // No per-key TTL on the Node side (LiveStore is "latest wins") —
      // anything older than 10 minutes counts as stale for this readout.
      status: age <= 600 ? 'fresh' : 'stale',
      // Approx size in bytes, since the snapshot is whatever the source
      // emits. Cheap-and-rough enough for a debug surface.
      size: JSON.stringify(snap.data).length,
    });
  }
  return out.sort((a, b) => a.key.localeCompare(b.key));
}

function cacheStatusBody() {
  const entries = cacheEntries();
  const fresh_count = entries.filter((e) => e.status === 'fresh').length;
  const stale_count = entries.length - fresh_count;
  return {
    prewarm_running: true,
    total_endpoints: entries.length,
    fresh_count,
    stale_count,
    total_fetch_time_ms: 0,
    db_path: 'postgres',
    entries,
    archive: archiveWriter.metrics(),
    poller: pollerHealth(),
  };
}

statsRouter.get('/api/cache/status', (c) => c.json(cacheStatusBody()));
statsRouter.get('/api/cache/stats', (c) => c.json(cacheStatusBody()));

// ---------------------------------------------------------------------------
// /api/collection/status — heartbeat-driven "are we collecting actively"
// view. Mirrors the Python handler at line 6467-6494.
// ---------------------------------------------------------------------------

statsRouter.get('/api/collection/status', (c) => {
  const data = dataViewerCount();
  const total = activeViewerCount();
  const active = data > 0;
  const sessions = listSessions().map(({ id, session }) => ({
    // First 8 chars of id followed by "..." — same short form Python
    // uses (line 6481). Trims the response without losing readability.
    id: id.length > 8 ? `${id.slice(0, 8)}...` : id,
    page_type: session.pageType,
    is_data_page: session.isDataPage,
    ip: session.ip,
    age_seconds: Math.floor((Date.now() - session.openedAt) / 1000),
  }));
  return c.json({
    mode: active ? 'active' : 'idle',
    interval_seconds: active ? ACTIVE_INTERVAL_SECS : IDLE_INTERVAL_SECS,
    total_viewers: total,
    data_viewers: data,
    sessions,
    last_heartbeat: null,
    idle_interval: IDLE_INTERVAL_SECS,
    active_interval: ACTIVE_INTERVAL_SECS,
    session_timeout: PAGE_SESSION_TIMEOUT_SECS,
    data_retention_days: 7,
  });
});

// ---------------------------------------------------------------------------
// /api/stats/archive/status + /api/stats/archive/trigger
// Mirrors python external_api_proxy.py:6293-6345. Reads stats_snapshots
// row counts (the same table /api/stats/history queries) so the response
// keys match python's contract: total_records, records_last_hour,
// oldest_record, newest_record, db_path, active_sessions.
// ---------------------------------------------------------------------------

import { writeStatsSnapshot } from '../services/statsArchiver.js';
import { sydneyIsoFromDate } from '../lib/sydneyTime.js';

interface ArchiveStatusRow {
  total: string | number;
  last_hour: string | number;
  oldest: Date | null;
  newest: Date | null;
}

statsRouter.get('/api/stats/archive/status', async (c) => {
  const data_viewers = dataViewerCount();
  const mode = data_viewers > 0 ? 'active' : 'idle';
  const currentInterval = data_viewers > 0 ? ACTIVE_INTERVAL_SECS : IDLE_INTERVAL_SECS;
  // Match python's response shape exactly. ArchiveWriter metrics are
  // additionally exposed under `archive_writer` so existing operator
  // tooling that checked Node-specific keys still has them.
  const m = archiveWriter.metrics();
  const sessions = listSessions().map(({ id }) =>
    id.length > 8 ? `${id.slice(0, 8)}...` : id,
  );

  let total_records = 0;
  let records_last_hour = 0;
  let oldest: Date | null = null;
  let newest: Date | null = null;

  const pool = await getPool();
  if (pool) {
    try {
      const r = await pool.query<ArchiveStatusRow>(
        `SELECT
           COUNT(*)::bigint AS total,
           COUNT(*) FILTER (WHERE ts >= NOW() - INTERVAL '1 hour')::bigint AS last_hour,
           MIN(ts) AS oldest,
           MAX(ts) AS newest
         FROM stats_snapshots`,
      );
      const row = r.rows[0];
      if (row) {
        total_records = Number(row.total ?? 0);
        records_last_hour = Number(row.last_hour ?? 0);
        oldest = row.oldest;
        newest = row.newest;
      }
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        '/api/stats/archive/status read failed',
      );
    }
  }

  return c.json({
    status: 'running',
    collection_mode: mode,
    current_interval_seconds: currentInterval,
    idle_interval_seconds: IDLE_INTERVAL_SECS,
    active_interval_seconds: ACTIVE_INTERVAL_SECS,
    active_pages: data_viewers,
    active_sessions: sessions,
    total_records,
    records_last_hour,
    oldest_record: sydneyIsoFromDate(oldest),
    newest_record: sydneyIsoFromDate(newest),
    db_path: 'postgres',
    // Node-only extras kept so older tooling that reads them still works.
    archive_writer: {
      queue_size: m.queue_size,
      dropped: m.dropped,
      total_written: m.total_written,
      last_flush_age_secs: m.last_flush_age_secs,
      live_keys: liveStore.keys().length,
    },
  });
});

// /api/stats/archive/trigger — write a stats_snapshots row right now,
// then drain the archive writer queue. Mirrors python's `archive_current_stats`
// at external_api_proxy.py:6341, which also calls back into the snapshot
// writer to populate the table /api/stats/history reads from.
statsRouter.get('/api/stats/archive/trigger', async (c) => {
  try {
    const wrote = await writeStatsSnapshot();
    const flushed = await archiveWriter.flush();
    return c.json({
      success: true,
      timestamp: new Date().toISOString(),
      stats_snapshot_written: wrote,
      flushed,
      after_flush: archiveWriter.metrics(),
    });
  } catch (err) {
    return c.json(
      { success: false, error: (err as Error).message },
      500,
    );
  }
});
