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
import { createApp } from './server.js';

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
