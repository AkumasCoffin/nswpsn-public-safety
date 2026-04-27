/**
 * App factory — composes route modules into a Hono instance.
 *
 * Separated from src/index.ts so tests can spin up an app without
 * binding a port (`createApp().fetch(req)` returns a Response).
 *
 * As more routes get ported in W2+, register them here.
 */
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { logger as honoLogger } from 'hono/logger';
import { healthRouter } from './api/health.js';
import { configRouter } from './api/config.js';
import { log } from './lib/log.js';

export function createApp() {
  const app = new Hono();

  // Hono's built-in logger pipes through to our pino instance so request
  // logs show up structured rather than as console.log lines. Keeps the
  // log stream uniform with everything else the service emits.
  app.use('*', honoLogger((msg) => log.info(msg)));

  // Permissive CORS for now. Locks down (origin allowlist) in W4 once
  // the heartbeat + auth middleware lands and we know exactly which
  // origins are real.
  app.use(
    '*',
    cors({
      origin: '*',
      allowHeaders: ['Authorization', 'Content-Type', 'X-API-Key'],
      maxAge: 600,
    }),
  );

  // Register route modules. Each router defines its own paths under
  // /api/...; mounting at '/' keeps the handlers' URLs identical to
  // the Python equivalents.
  app.route('/', healthRouter);
  app.route('/', configRouter);

  // Root route — useful for "is this the right backend?" smoke tests
  // when both Python and Node are running side by side.
  app.get('/', (c) =>
    c.json({
      service: 'nswpsn-api-node',
      // Surfaces "I'm the Node one" in case Apache routes the wrong
      // backend during the strangler-fig cutover.
      runtime: 'node',
      docs: 'See backends/external_api_proxy.py for the live Python backend',
    }),
  );

  return app;
}
