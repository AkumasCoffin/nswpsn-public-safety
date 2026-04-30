/**
 * Entry point.
 *
 * Starts the HTTP server, wires up graceful shutdown for SIGTERM/SIGINT,
 * and logs a banner so it's obvious in pm2 logs that the Node backend
 * came up.
 */
import { setDefaultResultOrder } from 'node:dns';
// Prefer IPv4 DNS results. Node 17+ defaults to 'verbatim' (IPv6 first
// when the OS returns it), but several upstream sources are US-hosted
// (RainViewer, NASA FIRMS) and the IPv6 route from this AU host has
// been timing out at the SYN stage — every poll dies with ETIMEDOUT.
// Forcing IPv4 makes those endpoints reachable. Has no effect on
// IPv4-only or AU-local upstreams.
setDefaultResultOrder('ipv4first');

import { serve } from '@hono/node-server';
import { config } from './config.js';
import { log } from './lib/log.js';
import { closePool } from './db/pool.js';
import { runMigrations } from './db/migrate.js';
import { createApp } from './server.js';
import { liveStore } from './store/live.js';
import { archiveWriter } from './store/archive.js';
import {
  startFilterCacheRefresh,
  stopFilterCacheRefresh,
} from './store/filterCache.js';
import { closeRdioPool, ensureUnitLabelsLoaded } from './services/rdio.js';
import {
  startRdioSummaryScheduler,
  stopRdioSummaryScheduler,
} from './services/llm.js';
import { ensureSessionTables } from './services/dashboardSession.js';
import { closeBotDbPool } from './services/botDb.js';
import { centralwatchBrowser } from './services/centralwatchBrowser.js';
import {
  startCentralwatchRefreshLoop,
  stopCentralwatchRefreshLoop,
} from './sources/centralwatch.js';
import {
  startCentralwatchImageBatchLoop,
  stopCentralwatchImageBatchLoop,
} from './services/centralwatchImageCache.js';
import { prewarmAll, startPolling, stopPolling } from './services/poller.js';
import {
  startHeatmapRefreshLoop,
  stopHeatmapRefreshLoop,
} from './api/waze.js';
import {
  startStatsArchiver,
  stopStatsArchiver,
} from './services/statsArchiver.js';
import { ensurePerfIndexes } from './services/indexBuilder.js';
import { startCleanupLoop, stopCleanupLoop } from './services/cleanup.js';
import { scheduleArchiveLatestBackfill } from './services/archiveLatestBackfill.js';
import {
  startPoliceHeatmapCacheRefresh,
  stopPoliceHeatmapCacheRefresh,
} from './services/policeHeatmapCache.js';
import {
  start as startActivityMode,
  stop as stopActivityMode,
} from './services/activityMode.js';
import { registerAllSources } from './sources/registerAll.js';
import { registerAllPowerSources } from './sources/registerPower.js';

// Pre-flight: hydrate the live store, run migrations, register every
// source, and start the persist + flush + poll + activity-mode loops
// before binding the port. Each step is best-effort and logged — a
// missing DATABASE_URL or empty STATE_DIR shouldn't block startup.
async function preflight(): Promise<void> {
  try {
    await liveStore.hydrateFromDisk();
  } catch (err) {
    log.error({ err }, 'liveStore hydrate failed');
  }
  try {
    await runMigrations();
  } catch (err) {
    // Migration failure is more serious — log but still start the
    // server so /api/health stays observable. Subsequent boot will
    // retry; persistent failure shows up loudly in logs.
    log.error({ err }, 'migration failed');
  }

  // Register every source with the source registry. The poller walks
  // the registry; nothing happens until startPolling() fires.
  registerAllSources(); // rfs, bom, traffic, beach, weather, pager
  registerAllPowerSources(); // endeavour, ausgrid, essential

  liveStore.startPersistLoop();
  archiveWriter.startFlushLoop();
  startActivityMode(); // sweeper for stale heartbeats; toggles polling cadence
  startFilterCacheRefresh(); // 5-min archive-backed facet refresh for /api/data/history/filters
  startHeatmapRefreshLoop(); // 5-min background refresh of police heatmap RAM cache
  startStatsArchiver(); // 5-min snapshots into stats_snapshots for /api/stats/history
  startCleanupLoop(); // hourly partition-drop + stats-snapshot prune
  startPoliceHeatmapCacheRefresh(); // 10-min materialised heatmap (mirrors python)
  scheduleArchiveLatestBackfill(); // one-shot backfill of archive_*_latest sidecars (migration 017)

  // Prewarm: fire every source's first poll in parallel and await with
  // a bounded timeout. Mirrors python's `prewarm_loop` initial pass —
  // without this, the first request after a restart can serve an empty
  // LiveStore for whatever interval the slowest source takes to fetch
  // for the first time. 30s cap means a hung upstream can't block boot
  // beyond that — its first tick keeps running in the background.
  try {
    await prewarmAll(30_000);
  } catch (err) {
    log.warn({ err }, 'prewarm failed (non-fatal — pollers will still arm)');
  }
  startPolling(); // walks the registry, schedules each source's setInterval

  // Background perf-index build. Runs on its own connection with
  // statement_timeout=0 so each CREATE INDEX can take as long as it
  // needs without blocking startup. Idempotent — re-runs are cheap
  // (pg_indexes lookup, then IF NOT EXISTS).
  setTimeout(() => void ensurePerfIndexes(), 5_000).unref?.();

  // Best-effort warm of the rdio unit-label CSV. Routes call
  // ensureUnitLabelsLoaded() lazily but doing it here means the first
  // /api/rdio/transcripts/search request doesn't pay the disk read.
  try {
    await ensureUnitLabelsLoaded();
  } catch (err) {
    log.warn({ err }, 'rdio unit-labels preload failed (non-fatal)');
  }

  // Optional in-process hourly summary scheduler. Default OFF so a Node
  // restart on a host that already runs python's scheduler doesn't
  // double-spend Gemini quota. Flip NODE_RDIO_SCHEDULER=true once the
  // python scheduler thread is stopped.
  startRdioSummaryScheduler();

  // Dashboard session table hydration — best effort. The router calls
  // this lazily too, but doing it here means the first OAuth callback
  // doesn't pay the CREATE TABLE / row-load.
  try {
    await ensureSessionTables();
  } catch (err) {
    log.warn({ err }, 'dashboard session table hydrate failed (non-fatal)');
  }

  // Centralwatch: kick the Playwright worker init off (non-blocking;
  // the loops below no-op until it reports ready) then start the
  // refresh + image batch loops. Honour CENTRALWATCH_DISABLED so a
  // deploy without chromium can still serve the last-good JSON via
  // /api/centralwatch/cameras.
  if (!config.CENTRALWATCH_DISABLED) {
    void centralwatchBrowser.init();
    startCentralwatchRefreshLoop();
    startCentralwatchImageBatchLoop();
  } else {
    log.info('centralwatch disabled (CENTRALWATCH_DISABLED=true)');
  }
}

await preflight();
const app = createApp();

const server = serve(
  {
    fetch: app.fetch,
    port: config.PORT,
    hostname: '0.0.0.0',
  },
  (info) => {
    log.info(
      { port: info.port, mode: config.NODE_ENV },
      `nswpsn-api-node listening on :${info.port}`,
    );
  },
);

// Drain in-flight requests, close DB pool, exit cleanly. PM2 sends
// SIGINT first (graceful) then SIGKILL after a timeout.
async function shutdown(signal: string) {
  log.info({ signal }, 'shutdown requested');
  try {
    // Stop accepting new connections; finish in-flight ones.
    await new Promise<void>((resolve) => server.close(() => resolve()));
    // Stop background loops in dependency order: pollers first (so
    // they don't enqueue more archive rows), then activity sweeper,
    // then drain the LiveStore + ArchiveWriter, then close the pool.
    stopPolling();
    stopActivityMode();
    stopFilterCacheRefresh();
    stopHeatmapRefreshLoop();
    stopPoliceHeatmapCacheRefresh();
    stopStatsArchiver();
    stopCleanupLoop();
    stopRdioSummaryScheduler();
    // Centralwatch: stop the loops first so they don't enqueue more
    // browser jobs, then close the browser worker.
    stopCentralwatchRefreshLoop();
    stopCentralwatchImageBatchLoop();
    await centralwatchBrowser.shutdown();
    await liveStore.stopAndFlush();
    await archiveWriter.stopAndFlush();
    await closePool();
    await closeRdioPool();
    await closeBotDbPool();
    log.info('shutdown complete');
    process.exit(0);
  } catch (err) {
    log.error({ err }, 'shutdown failed');
    process.exit(1);
  }
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

// Surface unhandled errors instead of letting Node print a stack and
// silently leak the process. The pino logger gives us structured output
// that's easier to grep than a raw stack dump.
process.on('uncaughtException', (err) => {
  log.fatal({ err }, 'uncaughtException — exiting');
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  log.fatal({ reason }, 'unhandledRejection — exiting');
  process.exit(1);
});
