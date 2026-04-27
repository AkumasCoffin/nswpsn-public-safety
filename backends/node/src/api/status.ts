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
 * Differences from python (the Node service has different internals so a
 * bug-for-bug clone isn't possible, only shape parity):
 *   - No police_heatmap, ram_cache, cleanup, or per-source consec_fail
 *     blocks — those primitives don't exist on the Node side. The check
 *     keys are simply omitted; JSONata's `ok = ($exists($.checks.foo) ?
 *     $.checks.foo.ok : true)` already handles this.
 *   - Sources rollup is derived from LiveStore presence: a registered
 *     source whose snapshot is stored is 'ok'; a registered source with
 *     no snapshot yet (boot) is 'unknown'.
 *
 * Cached for 5s in-process to absorb monitor + dev-tab refresh storms.
 */
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { liveStore } from '../store/live.js';
import { archiveWriter } from '../store/archive.js';
import { snapshot as wazeSnapshot } from '../store/wazeIngestCache.js';
import { filterCacheLastRefreshAt } from '../store/filterCache.js';
import { allSources } from '../services/sourceRegistry.js';
import { activeViewerCount } from '../services/activityMode.js';
import { config, modeLabel } from '../config.js';
import { log } from '../lib/log.js';

// Thresholds — match python defaults at lines 11020-11037.
const STATUS_DB_TIMEOUT_SECS = 5;
const STATUS_CACHE_TTL_MS = 5_000;
const STATUS_WRITER_STALE_SECS = 300;
const STATUS_WAZE_STALE_SECS = 900;
const STATUS_FILTER_CACHE_STALE_SECS = 1800;
const STATUS_BUFFER_WARN_RECORDS = 10_000;
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
        pool_total: pool.totalCount,
        pool_idle: pool.idleCount,
        pool_waiting: pool.waitingCount,
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
      flush_interval_ms: config.ARCHIVE_FLUSH_INTERVAL_MS,
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
      block: { ok: true, enabled: false },
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
    },
    degraded: !ok,
  };
}

function checkFilterCache(now: number): {
  block: Record<string, unknown>;
  degraded: boolean;
} {
  const lastTs = filterCacheLastRefreshAt();
  const upSecs = (now - PROCESS_START_MS) / 1000;
  const age =
    lastTs > 0 ? Math.floor((now - lastTs) / 1000) : null;
  const inGrace = age === null && upSecs < FILTER_CACHE_BOOT_GRACE_SECS;
  const stale = age !== null && age > STATUS_FILTER_CACHE_STALE_SECS;
  const ok = !stale && (age !== null || inGrace);
  return {
    block: {
      ok,
      last_refresh_age_secs: age,
      threshold_secs: STATUS_FILTER_CACHE_STALE_SECS,
    },
    degraded: !ok,
  };
}

function summariseSources(): {
  block: Record<string, { ok: boolean; status: string }>;
  counts: { ok: number; unknown: number; total: number };
} {
  const sources = allSources();
  const block: Record<string, { ok: boolean; status: string }> = {};
  const counts = { ok: 0, unknown: 0, total: sources.length };
  for (const s of sources) {
    const has = liveStore.getData(s.name) !== undefined;
    if (has) {
      block[s.name] = { ok: true, status: 'ok' };
      counts.ok += 1;
    } else {
      block[s.name] = { ok: false, status: 'unknown' };
      counts.unknown += 1;
    }
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

  const sources = summariseSources();
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
