/**
 * System / debug / admin endpoints.
 *
 * Mirrors python external_api_proxy.py:
 *   - /api/cache/clear            (line 11425)
 *   - /api/debug/sessions          (line 11559)
 *   - /api/debug/heartbeat-test    (line 11593)
 *   - /api/debug/ratelimit         (line 11528) — Node has no rate-limit
 *                                    middleware so this is informational
 *   - /api/debug/traffic-raw       (line 11637)
 *   - /api/debug/test-all          (line 10929) — degraded version
 *   - /api/admin/db/stats          (line 13226) — adapted to partitioned
 *                                    archive_* schema
 *   - /api/admin/db/vacuum         (line 13327) — runs VACUUM ANALYZE on
 *                                    archive tables
 *   - /api/admin/db/cleanup-duplicates (line 13170) — 503 stub. The
 *                                    deprecated dedup logic targeted the
 *                                    old data_history table; the new
 *                                    append-only archive tables don't
 *                                    have a duplicate problem.
 */
import { Hono } from 'hono';
import { liveStore } from '../store/live.js';
import { archiveWriter } from '../store/archive.js';
import { listSessions, activeViewerCount, dataViewerCount } from '../services/activityMode.js';
import { getPool } from '../db/pool.js';
import { fetchJson } from '../sources/shared/http.js';
import { _resetStatusCacheForTests } from './status.js';
import { _resetW3wCacheForTests } from './w3w.js';
import { _resetEndeavourPostcodesCacheForTests } from './endeavour.js';
import { _resetHeatmapCacheForTests } from './waze.js';
import { _resetNewsCacheForTests } from './news.js';
import { _resetDataHistoryAggregateCache } from './data-history.js';
import { _resetCentralwatchCacheForTests } from '../sources/centralwatch.js';
import {
  _resetNonceCacheForTests as _resetAviationNonceForTests,
  _resetDetailCacheForTests as _resetAviationDetailForTests,
} from '../sources/aviation.js';
import { log } from '../lib/log.js';

export const systemRouter = new Hono();

// /api/cache/clear — drop the various in-process caches we maintain on
// the Node side. LiveStore stays (it's the source of truth for live
// data) but the response-side caches reset. Mirrors python's blanket
// cache.clear() at line 11428.
systemRouter.get('/api/cache/clear', (c) => {
  // Each cache module exposes a *ForTests reset for unit-test bleed
  // prevention; we reuse them here as the production cache-clear path
  // since they do exactly what cache/clear is supposed to do.
  // tryEach swallows per-cache errors so one missing module doesn't
  // leave the others uncleared (matches python's blanket cache.clear()
  // semantics — best effort, not all-or-nothing).
  const tryEach = (fn: () => void): void => {
    try { fn(); } catch { /* module may not be loaded yet */ }
  };
  tryEach(_resetStatusCacheForTests);
  tryEach(_resetW3wCacheForTests);
  tryEach(_resetEndeavourPostcodesCacheForTests);
  // Heatmap cache (5-min TTL on /api/waze/police-heatmap) — operators
  // hitting cache/clear to force-refresh expect this to drop too.
  tryEach(_resetHeatmapCacheForTests);
  // News RSS aggregator (5-min cache).
  tryEach(_resetNewsCacheForTests);
  // Centralwatch file reader (1-min mtime-keyed cache).
  tryEach(_resetCentralwatchCacheForTests);
  // Aviation nonce + per-airport modal detail caches.
  tryEach(_resetAviationNonceForTests);
  tryEach(_resetAviationDetailForTests);
  // /api/data/history/sources and /stats cached aggregates (5-min TTL)
  tryEach(_resetDataHistoryAggregateCache);
  return c.json({
    status: 'ok',
    message: 'Response-side caches cleared (LiveStore retained)',
  });
});

// /api/debug/sessions — list active heartbeat sessions. Mirrors python
// line 11559-11590. Field names match python so any debug tooling
// hitting either backend keeps reading the same shape.
systemRouter.get('/api/debug/sessions', (c) => {
  const sessions = listSessions();
  const now = Date.now();
  const detail = sessions.map(({ id, session }) => ({
    page_id: id,
    page_type: session.pageType,
    is_data_page: session.isDataPage,
    ip: session.ip,
    last_seen_seconds_ago: Math.floor((now - session.lastSeen) / 1000),
    session_age_seconds: Math.floor((now - session.openedAt) / 1000),
  }));
  return c.json({
    active_count: activeViewerCount(),
    data_page_count: dataViewerCount(),
    session_timeout_seconds: 120,
    heartbeat_timeout_seconds: 120,
    is_page_active: dataViewerCount() > 0,
    collection_mode: dataViewerCount() > 0 ? 'active' : 'idle',
    current_interval_seconds: dataViewerCount() > 0 ? 60 : 300,
    sessions: detail,
  });
});

// /api/debug/heartbeat-test — echo endpoint that shows what the server
// sees when the frontend sends a heartbeat. Mirrors python line
// 11593-11634 in payload shape.
systemRouter.get('/api/debug/heartbeat-test', (c) => {
  const url = new URL(c.req.url);
  const action = url.searchParams.get('action') ?? 'ping';
  const page_id = url.searchParams.get('page_id') ?? '';
  const page_type = url.searchParams.get('page_type') ?? 'unknown';
  const data_page_raw = url.searchParams.get('data_page') ?? 'false';
  const is_data_page = ['true', '1', 'yes'].includes(data_page_raw.toLowerCase());
  const client_ip = c.req.header('x-forwarded-for') ?? '';
  const user_agent = (c.req.header('user-agent') ?? 'unknown').slice(0, 100);
  const short_id = page_id.length > 6 ? page_id.slice(-6) : page_id;
  const sessions = listSessions();
  const session_exists = sessions.some((s) => s.id === page_id);
  return c.json({
    received_params: {
      action,
      page_id,
      page_id_short: short_id,
      page_type,
      data_page_raw,
      is_data_page_parsed: is_data_page,
    },
    headers: { client_ip, user_agent },
    session_info: {
      session_exists,
      total_sessions: sessions.length,
      data_sessions: dataViewerCount(),
    },
    state: {
      is_page_active: dataViewerCount() > 0,
      collection_mode: dataViewerCount() > 0 ? 'active' : 'idle',
      current_interval: dataViewerCount() > 0 ? 60 : 300,
    },
    test_result: 'OK - Use /api/heartbeat to actually register a heartbeat',
  });
});

// /api/debug/ratelimit — Node backend doesn't have rate limiting (Apache
// handles that upstream). Endpoint exists so tooling that polls it
// against either backend gets a deterministic shape.
systemRouter.get('/api/debug/ratelimit', (c) => {
  const ip = c.req.header('x-forwarded-for') ?? c.req.header('cf-connecting-ip') ?? '';
  return c.json({
    your_ip: ip,
    your_requests: 0,
    your_burst_used: 0,
    limit: null,
    burst_limit: null,
    window_seconds: null,
    window_remaining: null,
    is_limited: false,
    total_tracked_ips: 0,
    currently_limited: [],
    note: 'Node backend has no in-process rate limiting (Apache handles upstream).',
  });
});

// /api/debug/traffic-raw — sample of raw upstream traffic-incident
// payload. Mirrors python line 11637-11673.
systemRouter.get('/api/debug/traffic-raw', async (c) => {
  try {
    const data = await fetchJson<unknown>(
      'https://www.livetraffic.com/traffic/hazards/incident.json',
      { timeoutMs: 15_000, headers: { 'User-Agent': 'Mozilla/5.0' } },
    );
    const features = Array.isArray(data)
      ? data
      : Array.isArray((data as { features?: unknown[] })?.features)
        ? (data as { features: unknown[] }).features
        : [];
    const sample = features.slice(0, 5).map((f, i) => {
      const fObj = (f ?? {}) as Record<string, unknown>;
      const props = (fObj['properties'] ?? fObj) as Record<string, unknown>;
      return {
        index: i,
        all_keys: Object.keys(fObj),
        properties_keys: Object.keys(props),
        mainCategory: props['mainCategory'] ?? fObj['mainCategory'] ?? null,
        subCategory: props['subCategory'] ?? fObj['subCategory'] ?? null,
        headline: props['headline'] ?? fObj['headline'] ?? null,
        displayName: props['displayName'] ?? fObj['displayName'] ?? null,
        incidentKind: props['incidentKind'] ?? fObj['incidentKind'] ?? null,
        type: props['type'] ?? fObj['type'] ?? null,
        full_properties: props,
      };
    });
    return c.json({ sample, total: features.length });
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      '/api/debug/traffic-raw failed',
    );
    return c.json({ error: (err as Error).message, sample: [], total: 0 });
  }
});

// /api/debug/test-all — quick smoke harness that hits a handful of the
// most-used routes on this backend itself. Mirrors python's spirit
// without porting all 250 lines: just reports per-key liveness in
// LiveStore so an operator can tell at a glance which sources are
// populated. The python implementation makes outbound HTTP calls and
// times them; that level of fidelity isn't needed during cutover.
systemRouter.get('/api/debug/test-all', (c) => {
  const keys = liveStore.keys();
  const now = Math.floor(Date.now() / 1000);
  const report: Record<string, { ok: boolean; age_secs: number | null }> = {};
  for (const k of keys) {
    const snap = liveStore.get(k);
    report[k] = {
      ok: snap !== null,
      age_secs: snap ? now - snap.ts : null,
    };
  }
  return c.json({
    backend: 'node',
    keys_present: keys.length,
    report,
    archive: archiveWriter.metrics(),
  });
});

// /api/admin/db/stats — adapted to the partitioned archive_* schema.
// Python's version queried the legacy `data_history` table; on Node we
// query each archive_* table for row count + size.
systemRouter.get('/api/admin/db/stats', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database not configured' }, 503);

  const ARCHIVE_TABLES = [
    'archive_waze',
    'archive_traffic',
    'archive_rfs',
    'archive_power',
    'archive_misc',
  ];
  const SUPPORT_TABLES = [
    'incidents',
    'incident_updates',
    'user_roles',
    'editor_requests',
    'rdio_summaries',
    'stats_snapshots',
  ];

  const stats: Record<string, unknown> = {};
  // archive_waze can be multi-million rows — an unbounded COUNT(*) can
  // park a connection for tens of seconds and starve the writer pool.
  // Wrap each query in BEGIN/SET LOCAL/COMMIT so it's bounded at 30s.
  const client = await pool.connect();
  try {
    const countWithTimeout = async <T extends Record<string, unknown>>(
      sql: string,
      params: unknown[] = [],
    ): Promise<T[]> => {
      await client.query('BEGIN');
      try {
        await client.query("SET LOCAL statement_timeout = '30s'");
        const r = await client.query<T>(sql, params);
        await client.query('COMMIT');
        return r.rows;
      } catch (err) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw err;
      }
    };

    for (const t of ARCHIVE_TABLES) {
      try {
        const rows = await countWithTimeout<{ rows: number; bytes: number }>(
          `SELECT COUNT(*)::bigint AS rows, ` +
            `pg_total_relation_size($1::regclass)::bigint AS bytes ` +
            `FROM ${t}`,
          [t],
        );
        const row = rows[0];
        stats[t] = {
          rows: Number(row?.rows ?? 0),
          size_mb: row?.bytes
            ? Math.round((Number(row.bytes) / (1024 * 1024)) * 100) / 100
            : 0,
        };
      } catch (err) {
        stats[t] = { error: (err as Error).message };
      }
    }
    for (const t of SUPPORT_TABLES) {
      try {
        const rows = await countWithTimeout<{ rows: number }>(
          `SELECT COUNT(*)::bigint AS rows FROM ${t}`,
        );
        stats[t] = { rows: Number(rows[0]?.rows ?? 0) };
      } catch (err) {
        stats[t] = { error: (err as Error).message };
      }
    }
    return c.json(stats);
  } catch (err) {
    return c.json({ error: (err as Error).message }, 500);
  } finally {
    client.release();
  }
});

// /api/admin/db/vacuum — runs VACUUM ANALYZE on the archive tables.
// VACUUM in postgres can't run inside a transaction, so we use a
// dedicated client with autocommit (the default outside an explicit
// BEGIN). Returns per-table outcome.
systemRouter.post('/api/admin/db/vacuum', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database not configured' }, 503);
  const tables = [
    'archive_waze',
    'archive_traffic',
    'archive_rfs',
    'archive_power',
    'archive_misc',
  ];
  const out: Record<string, { ok: boolean; ms: number; error?: string }> = {};
  const client = await pool.connect();
  try {
    for (const t of tables) {
      const t0 = Date.now();
      try {
        await client.query(`VACUUM ANALYZE ${t}`);
        out[t] = { ok: true, ms: Date.now() - t0 };
      } catch (err) {
        out[t] = { ok: false, ms: Date.now() - t0, error: (err as Error).message };
      }
    }
    return c.json({ status: 'ok', tables: out });
  } finally {
    client.release();
  }
});

// /api/admin/db/waze-subtypes — diagnostic for the heatmap subtype
// filter. Shows where waze_police rows store their subtype (column vs
// JSONB) and how many of each. Useful when the heatmap shows
// suspicious counts (e.g. speed-camera filter returning very few rows).
systemRouter.get('/api/admin/db/waze-subtypes', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database not configured' }, 503);
  const url = new URL(c.req.url);
  const days = Math.max(1, Math.min(90, Number.parseInt(url.searchParams.get('days') ?? '30', 10) || 30));
  try {
    const r = await pool.query<{
      subcategory: string | null;
      jsonb_subtype: string | null;
      cnt: string;
    }>(
      `SELECT
         subcategory,
         data->>'subtype' AS jsonb_subtype,
         COUNT(*)::text AS cnt
       FROM archive_waze
       WHERE source = 'waze_police'
         AND fetched_at >= NOW() - ($1 || ' days')::interval
       GROUP BY 1, 2
       ORDER BY COUNT(*) DESC
       LIMIT 50`,
      [String(days)],
    );
    return c.json({
      window_days: days,
      total_groups: r.rows.length,
      rows: r.rows.map((row) => ({
        subcategory: row.subcategory,
        jsonb_subtype: row.jsonb_subtype,
        count: Number(row.cnt),
      })),
    });
  } catch (err) {
    return c.json({ error: (err as Error).message }, 500);
  }
});

// /api/admin/db/cleanup-duplicates — python's implementation worked on
// the deprecated `data_history` table (which is_live=1 dedup needed).
// The new partitioned archive_* schema is append-only with no
// duplicate-row problem, so this endpoint is a no-op stub on Node.
systemRouter.post('/api/admin/db/cleanup-duplicates', (c) =>
  c.json(
    {
      status: 'no-op',
      reason:
        'archive_* tables are append-only; the legacy data_history dedup ' +
        'pass does not apply on the Node backend.',
    },
    200,
  ),
);
