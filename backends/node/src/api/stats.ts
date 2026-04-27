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

export const statsRouter = new Hono();

const ACTIVE_INTERVAL_SECS = 60;
const IDLE_INTERVAL_SECS = 300;
const PAGE_SESSION_TIMEOUT_SECS = 120;

// ---------------------------------------------------------------------------
// /api/stats/history — Python reads from stats_snapshots; we don't yet.
// ---------------------------------------------------------------------------

statsRouter.get('/api/stats/history', (c) => c.json([]));

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

function summarisePower(): PowerSummary {
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
  let endeavCustomers = 0;
  for (const o of [...endeavCur, ...endeavMaint, ...endeavFut]) {
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

statsRouter.get('/api/stats/summary', (c) =>
  c.json({
    timestamp: new Date().toISOString().replace(/\.\d+Z$/, 'Z'),
    power: summarisePower(),
    // The other agent owns traffic/emergency/environment — left zeroed
    // until those LiveStore keys land. Python uses the same nested
    // shape so the frontend's reader is happy with all-zeros today.
    traffic: {
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
    },
    emergency: {
      rfs_incidents: 0,
      rfs_by_level: { emergency_warning: 0, watch_and_act: 0, advice: 0 },
      bom_warnings: { land: 0, marine: 0, total: 0 },
    },
    environment: {
      beaches_monitored: 0,
      beaches_good: 0,
      beaches_poor: 0,
      beachsafe_patrolled: 0,
    },
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
// /api/stats/archive/status + /api/stats/archive/trigger — surface and
// pulse the ArchiveWriter from the outside. Mirrors python lines
// 6293-6345. Note: python's status reads stats_snapshots row counts;
// we expose archive_writer metrics + LiveStore key counts because Node
// archive metadata lives in those primitives.
// ---------------------------------------------------------------------------

statsRouter.get('/api/stats/archive/status', (c) => {
  const m = archiveWriter.metrics();
  const data_viewers = dataViewerCount();
  return c.json({
    status: 'running',
    collection_mode: data_viewers > 0 ? 'active' : 'idle',
    current_interval_seconds: data_viewers > 0
      ? ACTIVE_INTERVAL_SECS
      : IDLE_INTERVAL_SECS,
    idle_interval_seconds: IDLE_INTERVAL_SECS,
    active_interval_seconds: ACTIVE_INTERVAL_SECS,
    active_pages: data_viewers,
    queue_size: m.queue_size,
    dropped: m.dropped,
    total_written: m.total_written,
    last_flush_age_secs: m.last_flush_age_secs,
    live_keys: liveStore.keys().length,
  });
});

// /api/stats/archive/trigger — kicks the archive writer to flush its
// queue immediately. Returns the post-flush metrics so callers can
// confirm the trigger actually drained data.
statsRouter.get('/api/stats/archive/trigger', async (c) => {
  try {
    const result = await archiveWriter.flush();
    return c.json({
      success: true,
      timestamp: new Date().toISOString(),
      flushed: result,
      after_flush: archiveWriter.metrics(),
    });
  } catch (err) {
    return c.json(
      { success: false, error: (err as Error).message },
      500,
    );
  }
});

// TODO(stats-snapshots): Python's /api/stats/history reads pre-computed
// 5-min snapshots from a `stats_snapshots` Postgres table. To bring
// parity to Node we'd need an archiver that periodically writes the
// /api/stats/summary blob into that table. Punted from W4 — frontend
// chart degrades gracefully on `[]`.
