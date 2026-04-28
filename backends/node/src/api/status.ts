/**
 * GET /api/status — Uptime Kuma / JSONata-friendly health endpoint.
 *
 * Mirrors python external_api_proxy.py:11354. The HTTP code reflects
 * critical backend state only (200 healthy or degraded, 503 down). The
 * JSON body has finer status (`ok | degraded | down`) plus a per-check
 * breakdown so monitor JSONata expressions stay short:
 *
 *   status                                      → 'ok' | 'degraded' | 'down'
 *   checks.database.ok                          → true / false
 *   checks.database.latency_ms < 200            → boolean
 *   checks.archive_writer.ok                    → true / false
 *   checks.waze_ingest.regions_cached >= 150    → boolean
 *
 * Shape parity with python's response — every check key python's
 * dashboard panels read is present here, even when the underlying
 * Node primitive doesn't exist (those report null/zero so the panels
 * render gracefully instead of as `—`). Specifically:
 *   - police_heatmap, cleanup, ram_cache: Node doesn't have these
 *     subsystems; the blocks are present with informational fields
 *     and `ok: true` so they don't trip the overall status.
 *   - ingest: pulled from archiveWriter.metrics().
 *   - sources rollup: derived from LiveStore presence (a registered
 *     source whose snapshot is stored is 'ok'; missing = 'unknown').
 *
 * Cached for 5s in-process to absorb monitor + dev-tab refresh storms.
 */
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { liveStore } from '../store/live.js';
import { archiveWriter } from '../store/archive.js';
import { snapshot as wazeSnapshot } from '../store/wazeIngestCache.js';
import { filterCacheLastRefreshAt } from '../store/filterCache.js';
import { policeHeatmapStatus } from './waze.js';
import { allSources } from '../services/sourceRegistry.js';
import { getSourceMetrics } from '../services/poller.js';
import { activeViewerCount } from '../services/activityMode.js';
import { cleanupStatsForStatus } from '../services/cleanup.js';
import { rdioSchedulerStats } from '../services/llm.js';
import { config, modeLabel } from '../config.js';
import { log } from '../lib/log.js';

// Thresholds — match python defaults at lines 11020-11037.
const STATUS_DB_TIMEOUT_SECS = 5;
const STATUS_CACHE_TTL_MS = 5_000;
const STATUS_WRITER_STALE_SECS = 300;
const STATUS_WAZE_STALE_SECS = 900;
const STATUS_FILTER_CACHE_STALE_SECS = 1800;
const STATUS_HEATMAP_STALE_SECS = 1800;
const STATUS_BUFFER_WARN_RECORDS = 10_000;
const FILTER_CACHE_REFRESH_INTERVAL_SECS = 60;
const ARCHIVE_FLUSH_INTERVAL_SECS_FALLBACK = 30;
const DATA_RETENTION_DAYS = Number.parseInt(
  process.env['DATA_RETENTION_DAYS'] ?? '7',
  10,
);
const DATA_CLEANUP_INTERVAL_SECS = Number.parseInt(
  process.env['DATA_CLEANUP_INTERVAL'] ?? '3600',
  10,
);
// Boot grace windows — first heartbeat hasn't fired yet.
const WRITER_BOOT_GRACE_SECS = 60;
const WAZE_BOOT_GRACE_SECS = 120;
const FILTER_CACHE_BOOT_GRACE_SECS = 120;

const PROCESS_START_MS = Date.now();

interface StatusPayload {
  status: 'ok' | 'degraded' | 'down';
  uptime_secs: number;
  started_at: number;
  now: number;
  mode: 'dev' | 'production';
  active_viewers: number;
  summary: {
    checks_failed: number;
    sources_total: number;
    sources_ok: number;
    sources_unknown: number;
  };
  checks: Record<string, Record<string, unknown>>;
}

interface CacheEntry {
  data: StatusPayload;
  httpCode: number;
  ts: number;
}
let statusCache: CacheEntry | null = null;

async function checkDatabase(): Promise<{
  block: Record<string, unknown>;
  critical: boolean;
  degraded: boolean;
}> {
  const t0 = Date.now();
  let pool;
  try {
    pool = await getPool();
  } catch (err) {
    return {
      block: {
        ok: false,
        latency_ms: null,
        error: (err as Error).message.slice(0, 200),
      },
      critical: true,
      degraded: false,
    };
  }
  if (!pool) {
    // No DATABASE_URL configured — treat as ok-but-informational. Most
    // routes that need the DB return 503 themselves; not a hard outage
    // from a status standpoint.
    return {
      block: { ok: true, configured: false, latency_ms: null },
      critical: false,
      degraded: false,
    };
  }
  const client = await pool.connect().catch((err) => err as Error);
  if (client instanceof Error) {
    return {
      block: { ok: false, latency_ms: null, error: client.message.slice(0, 200) },
      critical: true,
      degraded: false,
    };
  }
  try {
    await client.query(
      `SET LOCAL statement_timeout = '${STATUS_DB_TIMEOUT_SECS * 1000}ms'`,
    );
    await client.query('SELECT 1');
    const latency_ms = Date.now() - t0;
    return {
      block: {
        ok: true,
        latency_ms,
        // Field names mirror python's _db_pool_stats output so
        // dashboard panels render without aliasing.
        pool_in_use: Math.max(0, pool.totalCount - pool.idleCount),
        pool_idle: pool.idleCount,
        pool_max: 20,
        pool_waiting: pool.waitingCount,
        error: null,
      },
      critical: false,
      degraded: false,
    };
  } catch (err) {
    const msg = (err as Error).message;
    // Postgres SQLSTATE 57014 = query_canceled (statement_timeout).
    // pg surfaces it as code === '57014' on the error.
    const code = (err as { code?: string }).code;
    const isTimeout = code === '57014';
    const latency_ms = Date.now() - t0;
    return {
      block: {
        ok: false,
        latency_ms,
        error: msg.slice(0, 200),
        code: code ?? null,
      },
      critical: !isTimeout,
      degraded: isTimeout,
    };
  } finally {
    client.release();
  }
}

function checkArchiveWriter(now: number): {
  block: Record<string, unknown>;
  critical: boolean;
} {
  const m = archiveWriter.metrics();
  const age = m.last_flush_age_secs;
  const upSecs = (now - PROCESS_START_MS) / 1000;
  // Boot grace: first ARCHIVE_FLUSH_INTERVAL hasn't elapsed yet.
  const stale = age !== null && age > STATUS_WRITER_STALE_SECS;
  const inGrace = age === null && upSecs < WRITER_BOOT_GRACE_SECS;
  const ok = !stale && (age !== null || inGrace);
  return {
    block: {
      ok,
      last_flush_age_secs: age,
      threshold_secs: STATUS_WRITER_STALE_SECS,
      // python's field name is flush_interval_secs; expose both for
      // dashboard frontends that read either.
      flush_interval_secs: Math.round(
        config.ARCHIVE_FLUSH_INTERVAL_MS / 1000,
      ) || ARCHIVE_FLUSH_INTERVAL_SECS_FALLBACK,
      queue_size: m.queue_size,
      dropped: m.dropped,
      total_written: m.total_written,
    },
    critical: !ok,
  };
}

function checkArchiveBuffer(): {
  block: Record<string, unknown>;
  degraded: boolean;
} {
  const m = archiveWriter.metrics();
  const overWarn = m.queue_size > STATUS_BUFFER_WARN_RECORDS;
  return {
    block: {
      ok: !overWarn,
      records: m.queue_size,
      warn_threshold: STATUS_BUFFER_WARN_RECORDS,
      hard_cap: 50_000,
    },
    degraded: overWarn,
  };
}

function checkWazeIngest(now: number): {
  block: Record<string, unknown>;
  degraded: boolean;
} {
  if (!config.WAZE_INGEST_KEY) {
    return {
      block: {
        ok: true,
        enabled: false,
        // python emits these as null when ingest is disabled; doing the
        // same so the dashboard panel shows "—" not undefined.
        last_ingest_age_secs: null,
        threshold_secs: STATUS_WAZE_STALE_SECS,
        regions_cached: null,
        block_rate_pct: null,
        gate_active: false,
      },
      degraded: false,
    };
  }
  const s = wazeSnapshot();
  const upSecs = (now - PROCESS_START_MS) / 1000;
  const age = s.last_ingest_age_secs;
  const inGrace = age === null && upSecs < WAZE_BOOT_GRACE_SECS;
  const stale = age !== null && age > STATUS_WAZE_STALE_SECS;
  const ok = !stale && (age !== null || inGrace);
  return {
    block: {
      ok,
      enabled: true,
      last_ingest_age_secs: age,
      threshold_secs: STATUS_WAZE_STALE_SECS,
      regions_cached: s.regions_cached,
      // Node doesn't track userscript block-rate yet (no equivalent of
      // python's _waze_metrics_snapshot rolling counter). Surface as
      // null so the panel renders "—" rather than 0% (which would be
      // misleading: 0% looks like "no blocks ever" not "unknown").
      block_rate_pct: null,
      gate_active: false,
    },
    degraded: !ok,
  };
}

function checkFilterCache(now: number): {
  block: Record<string, unknown>;
  degraded: boolean;
} {
  // filterCacheLastRefreshAt() returns epoch SECONDS (per its docstring).
  // `now` is epoch milliseconds. Converting before subtracting otherwise
  // age comes out as ~1.7e9 seconds (~493000 hours, the bug the dashboard
  // surfaced as "Filter Cache 493205h").
  const lastTsSecs = filterCacheLastRefreshAt();
  const nowSecs = Math.floor(now / 1000);
  const upSecs = (now - PROCESS_START_MS) / 1000;
  const age = lastTsSecs > 0 ? Math.max(0, nowSecs - lastTsSecs) : null;
  const inGrace = age === null && upSecs < FILTER_CACHE_BOOT_GRACE_SECS;
  const stale = age !== null && age > STATUS_FILTER_CACHE_STALE_SECS;
  const ok = !stale && (age !== null || inGrace);
  return {
    block: {
      ok,
      last_refresh_age_secs: age,
      threshold_secs: STATUS_FILTER_CACHE_STALE_SECS,
      refresh_interval_secs: FILTER_CACHE_REFRESH_INTERVAL_SECS,
    },
    degraded: !ok,
  };
}

// ---------------------------------------------------------------------------
// Sections python has that Node doesn't natively track. We expose the
// shape so dashboard panels render gracefully (no `—` placeholders),
// using sentinel values (null / 0) where there's no Node equivalent yet.
// ---------------------------------------------------------------------------

/** Police-heatmap freshness. The Node backend runs a 5-min background
 *  refresh into an in-process cache; this reads its last-known state. */
function checkPoliceHeatmap(): Record<string, unknown> {
  const s = policeHeatmapStatus();
  const stale =
    s.last_refresh_age_secs !== null &&
    s.last_refresh_age_secs > STATUS_HEATMAP_STALE_SECS;
  return {
    ok: !stale,
    bins: s.bins,
    last_refresh_age_secs: s.last_refresh_age_secs,
    threshold_secs: STATUS_HEATMAP_STALE_SECS,
  };
}

/** Ingest activity — pulled from the archive writer's flush counters. */
function checkIngest(): Record<string, unknown> {
  const m = archiveWriter.metrics();
  return {
    last_flush_age_secs: m.last_flush_age_secs,
    last_flush_records: m.last_flush_records,
    last_flush_sources: m.last_flush_tables,
    last_flush_ms: m.last_flush_ms,
    total_records_flushed: m.total_written,
    total_flushes: m.total_flushes,
  };
}

/** Cleanup loop — partition drops + stats_snapshots prune. Real
 *  values from services/cleanup.ts. */
function checkCleanup(): Record<string, unknown> {
  return cleanupStatsForStatus();
}

/** RAM-cache hit-rate — LiveStore tracks hits/misses across the
 *  process lifetime. */
function checkRamCache(): Record<string, unknown> {
  return liveStore.cacheStats();
}

/** Hourly rdio summary scheduler. Surfaces enabled/reason/next-fire/
 *  last-fire so the dashboard can show whether the scheduler is alive
 *  without grepping logs. ok=false when enabled but the next fire is
 *  more than 75 min away (means the chain dropped) or the last run
 *  recorded an error. */
function checkRdioScheduler(nowMs: number): Record<string, unknown> {
  const s = rdioSchedulerStats();
  const nowSec = Math.floor(nowMs / 1000);
  const nextAge = s.next_fire_at != null ? s.next_fire_at - nowSec : null;
  // 75 min = one full hour + the worst case where last fire just landed.
  const armedOk = s.next_fire_at == null ? false : nextAge != null && nextAge < 75 * 60;
  return {
    ok: s.enabled ? armedOk && !s.last_error : true, // disabled = informational, not failing
    enabled: s.enabled,
    reason: s.reason,
    next_fire_at: s.next_fire_at,
    next_fire_in_secs: nextAge,
    last_fire_at: s.last_fire_at,
    last_fire_age_secs: s.last_fire_at != null ? nowSec - s.last_fire_at : null,
    last_run_ms: s.last_run_ms,
    last_result: s.last_result,
    last_error: s.last_error,
    total_fires: s.total_fires,
  };
}

function summariseSources(nowMs: number): {
  block: Record<string, Record<string, unknown>>;
  counts: { ok: number; unknown: number; total: number };
} {
  const sources = allSources();
  const metrics = new Map(getSourceMetrics().map((m) => [m.name, m]));
  const nowSec = Math.floor(nowMs / 1000);
  const block: Record<string, Record<string, unknown>> = {};
  const counts = { ok: 0, unknown: 0, total: sources.length };
  for (const s of sources) {
    const m = metrics.get(s.name);
    const has = liveStore.getData(s.name) !== undefined;
    const status = has ? 'ok' : 'unknown';
    // Active poll cadence in seconds (matches what editor-requests
    // shows under `thresh`). intervalIdleMs gives the soft threshold
    // we tolerate before the source looks stale; intervalActiveMs is
    // the active-mode polling target so the UI can compare the two.
    const softSec = Math.round(s.intervalActiveMs / 1000);
    const hardSec = Math.round(s.intervalIdleMs / 1000);
    block[s.name] = {
      ok: has,
      status,
      family: s.family,
      last_success_age_secs:
        m?.last_ok_at != null ? nowSec - m.last_ok_at : null,
      last_error_age_secs:
        m?.last_error_at != null ? nowSec - m.last_error_at : null,
      last_error: m?.last_error ?? null,
      consec_fails: m?.consec_fails ?? 0,
      soft_threshold_secs: softSec,
      hard_threshold_secs: hardSec,
    };
    if (has) counts.ok += 1;
    else counts.unknown += 1;
  }
  return { block, counts };
}

async function computeStatus(): Promise<{
  payload: StatusPayload;
  httpCode: number;
}> {
  const nowMs = Date.now();
  const checks: Record<string, Record<string, unknown>> = {};
  let critical = false;
  let degraded = false;

  const db = await checkDatabase();
  checks['database'] = db.block;
  if (db.critical) critical = true;
  if (db.degraded) degraded = true;

  const writer = checkArchiveWriter(nowMs);
  checks['archive_writer'] = writer.block;
  if (writer.critical) critical = true;

  const buffer = checkArchiveBuffer();
  checks['archive_buffer'] = buffer.block;
  if (buffer.degraded) degraded = true;

  const waze = checkWazeIngest(nowMs);
  checks['waze_ingest'] = waze.block;
  if (waze.degraded) degraded = true;

  const fc = checkFilterCache(nowMs);
  checks['filter_cache'] = fc.block;
  if (fc.degraded) degraded = true;

  // Sections python had that Node doesn't track yet — present so the
  // dashboard panels render with sensible zeros instead of "—".
  checks['police_heatmap'] = checkPoliceHeatmap();
  checks['ingest'] = checkIngest();
  checks['cleanup'] = checkCleanup();
  checks['ram_cache'] = checkRamCache();
  checks['rdio_scheduler'] = checkRdioScheduler(nowMs);

  const sources = summariseSources(nowMs);
  checks['sources'] = sources.block as unknown as Record<string, unknown>;
  if (sources.counts.unknown > 0 && sources.counts.total > 0) {
    // Boot grace handled per-source via the 'unknown' status; not flipped
    // to degraded here because every source registers its own freshness
    // story via individual checks.
  }

  const failedChecks = Object.values(checks).reduce(
    (n, c) => n + (c['ok'] === false ? 1 : 0),
    0,
  );

  let overall: 'ok' | 'degraded' | 'down';
  let httpCode: number;
  if (critical) {
    overall = 'down';
    httpCode = 503;
  } else if (degraded) {
    overall = 'degraded';
    httpCode = 200;
  } else {
    overall = 'ok';
    httpCode = 200;
  }

  const payload: StatusPayload = {
    status: overall,
    uptime_secs: Math.floor((nowMs - PROCESS_START_MS) / 1000),
    started_at: Math.floor(PROCESS_START_MS / 1000),
    now: Math.floor(nowMs / 1000),
    mode: modeLabel(),
    active_viewers: activeViewerCount(),
    summary: {
      checks_failed: failedChecks,
      sources_total: sources.counts.total,
      sources_ok: sources.counts.ok,
      sources_unknown: sources.counts.unknown,
    },
    checks,
  };
  return { payload, httpCode };
}

export const statusRouter = new Hono();

statusRouter.get('/api/status', async (c) => {
  const nowMs = Date.now();
  if (statusCache && nowMs - statusCache.ts < STATUS_CACHE_TTL_MS) {
    // Re-stamp the volatile fields so cached responses don't visibly drift.
    const fresh: StatusPayload = {
      ...statusCache.data,
      now: Math.floor(nowMs / 1000),
      uptime_secs: Math.floor((nowMs - PROCESS_START_MS) / 1000),
      active_viewers: activeViewerCount(),
    };
    return c.json(fresh, statusCache.httpCode as 200 | 503);
  }
  try {
    const { payload, httpCode } = await computeStatus();
    statusCache = { data: payload, httpCode, ts: nowMs };
    return c.json(payload, httpCode as 200 | 503);
  } catch (err) {
    log.error({ err }, '/api/status compute failed');
    return c.json(
      {
        status: 'down',
        error: (err as Error).message,
        now: Math.floor(nowMs / 1000),
      },
      503,
    );
  }
});

export function _resetStatusCacheForTests(): void {
  statusCache = null;
}
