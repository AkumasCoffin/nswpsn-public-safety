/**
 * Entry point.
 *
 * Starts the HTTP server, wires up graceful shutdown for SIGTERM/SIGINT,
 * and logs a banner so it's obvious in pm2 logs that the Node backend
 * came up.
 */
import { serve } from '@hono/node-server';
import { config } from './config.js';
import { log } from './lib/log.js';
import { closePool } from './db/pool.js';
import { runMigrations } from './db/migrate.js';
import { createApp } from './server.js';
import { liveStore } from './store/live.js';
import { archiveWriter } from './store/archive.js';

// Pre-flight: hydrate the live store so /api/waze/* serves something
// useful immediately after restart, and run any pending DB migrations
// before the server binds. Each step is best-effort and logged — a
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
  liveStore.startPersistLoop();
  archiveWriter.startFlushLoop();
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
    // Drain LiveStore + ArchiveWriter so we don't lose buffered work.
    await liveStore.stopAndFlush();
    await archiveWriter.stopAndFlush();
    await closePool();
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
