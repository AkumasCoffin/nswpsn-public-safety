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
// Core endpoints (W1).
import { healthRouter } from './api/health.js';
import { configRouter } from './api/config.js';
// Waze (W2).
import { wazeRouter } from './api/waze.js';
import { wazeIngestRouter } from './api/waze-ingest.js';
// Simple sources (W3).
import { rfsRouter } from './api/rfs.js';
import { bomRouter } from './api/bom.js';
import { trafficRouter } from './api/traffic.js';
import { beachRouter } from './api/beach.js';
import { weatherRouter } from './api/weather.js';
import { pagerRouter } from './api/pager.js';
// Power sources + heartbeat + stats (W4).
import { endeavourRouter } from './api/endeavour.js';
import { ausgridRouter } from './api/ausgrid.js';
import { essentialRouter } from './api/essential.js';
import { heartbeatRouter } from './api/heartbeat.js';
import { statsRouter } from './api/stats.js';
// Incidents / editor / users (W5).
import { incidentsRouter } from './api/incidents.js';
import { editorRouter } from './api/editor.js';
import { usersRouter } from './api/users.js';
// Data-history archive reads (W6).
import { dataHistoryRouter } from './api/data-history.js';
// Aviation / news / summaries / transcripts (W7).
import { aviationRouter } from './api/aviation.js';
import { newsRouter } from './api/news.js';
import { summariesRouter } from './api/summaries.js';
import { transcriptsRouter } from './api/transcripts.js';
// Centralwatch + dashboard (W8). Image proxy + dashboard endpoints
// are 503 stubs — Apache routes those prefixes to python.
import { centralwatchRouter } from './api/centralwatch.js';
import { dashboardRouter } from './api/dashboard.js';
// Uptime Kuma-shaped /api/status (cutover blocker for monitor flips).
import { statusRouter } from './api/status.js';
// What3Words proxy + system/debug/admin.
import { w3wRouter } from './api/w3w.js';
import { systemRouter } from './api/system.js';
import { requireApiKey } from './services/auth/apiKey.js';
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

  // Global NSWPSN_API_KEY gate. The middleware itself short-circuits for
  // OPTIONS preflights, public endpoints (/api/health, /api/config,
  // /api/heartbeat, POST /api/editor-requests, POST /api/waze/ingest,
  // /api/check-editor/*, etc.), and any non-/api path. Mirrors Python's
  // global @app.before_request hook.
  app.use('*', requireApiKey);

  // Register route modules. Each router defines its own paths under
  // /api/...; mounting at '/' keeps the handlers' URLs identical to
  // the Python equivalents.
  app.route('/', healthRouter);
  app.route('/', configRouter);
  // Waze
  app.route('/', wazeRouter);
  app.route('/', wazeIngestRouter);
  // Simple sources
  app.route('/', rfsRouter);
  app.route('/', bomRouter);
  app.route('/', trafficRouter);
  app.route('/', beachRouter);
  app.route('/', weatherRouter);
  app.route('/', pagerRouter);
  // Power + heartbeat + stats
  app.route('/', endeavourRouter);
  app.route('/', ausgridRouter);
  app.route('/', essentialRouter);
  app.route('/', heartbeatRouter);
  app.route('/', statsRouter);
  // Incidents + editor + users (W5)
  app.route('/', incidentsRouter);
  app.route('/', editorRouter);
  app.route('/', usersRouter);
  // Data-history archive reads (W6)
  app.route('/', dataHistoryRouter);
  // Aviation cameras + news RSS + summaries (W7)
  app.route('/', aviationRouter);
  app.route('/', newsRouter);
  app.route('/', summariesRouter);
  app.route('/', transcriptsRouter);
  // Centralwatch reads + dashboard 503 stubs (W8)
  app.route('/', centralwatchRouter);
  app.route('/', dashboardRouter);
  // Uptime-Kuma-shaped status endpoint
  app.route('/', statusRouter);
  // What3Words proxy
  app.route('/', w3wRouter);
  // System / debug / admin (cache clear, debug/* echoes, admin/db/*)
  app.route('/', systemRouter);

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
