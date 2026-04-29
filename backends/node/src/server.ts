/**
 * App factory — composes route modules into a Hono instance.
 *
 * Separated from src/index.ts so tests can spin up an app without
 * binding a port (`createApp().fetch(req)` returns a Response).
 *
 * As more routes get ported in W2+, register them here.
 */
import { Hono } from 'hono';
import type { MiddlewareHandler } from 'hono';
import { cors } from 'hono/cors';
import { compress } from 'hono/compress';
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

// Paths that should never appear in the request log even on
// success — they fire constantly and drown out everything else.
// Failures still show up because the success-only filter below
// only short-circuits on 2xx/3xx.
const QUIET_PATHS_RE =
  /^\/(?:api\/heartbeat|api\/check-editor\/|api\/config|api\/health|api\/status)/;

const SLOW_REQUEST_MS = 500;

/**
 * Replacement for Hono's built-in logger. Differences:
 *   - 2xx + 3xx on QUIET_PATHS_RE are silent
 *   - 2xx + 3xx slower than SLOW_REQUEST_MS log at info ("slow")
 *   - other 2xx + 3xx log at debug (silent unless LOG_LEVEL=debug)
 *   - 4xx logs at info ("client error")
 *   - 5xx logs at warn ("server error")
 *   - OPTIONS preflights are silent (huge volume, no signal)
 */
const requestLogger: MiddlewareHandler = async (c, next) => {
  const start = Date.now();
  await next();
  const ms = Date.now() - start;
  const status = c.res.status;
  const method = c.req.method;
  const path = new URL(c.req.url).pathname;

  if (method === 'OPTIONS') return;

  if (status >= 500) {
    log.warn({ method, path, status, ms }, 'request 5xx');
    return;
  }
  if (status >= 400) {
    log.info({ method, path, status, ms }, 'request 4xx');
    return;
  }
  if (QUIET_PATHS_RE.test(path)) return;
  if (ms >= SLOW_REQUEST_MS) {
    log.info({ method, path, status, ms }, 'slow request');
    return;
  }
  log.debug({ method, path, status, ms }, 'request');
};

export function createApp() {
  const app = new Hono();

  // Custom request logger — see comment on requestLogger above.
  app.use('*', requestLogger);

  // Brotli/gzip compression. The /api/data/history full payloads and
  // /api/waze/police-heatmap (~1-2 MB raw) compress ~10×. Cloudflare/
  // Apache may also gzip in front; double-compression is detected by
  // hono/compress (skips when Content-Encoding already set).
  app.use('*', compress());

  // Short-lived public cache for cheap GET reads. Browsers + CDNs can
  // serve repeat hits without round-tripping to Node. 30s is short
  // enough that live data stays "live", but covers rapid double-clicks
  // and dashboard panel re-renders. SWR=300s lets stale responses tide
  // a request over while we revalidate in the background.
  // Skipped for auth-sensitive endpoints (/api/config, /api/dashboard/*),
  // per-user state (/api/heartbeat), and POST/PUT/DELETE.
  const CACHEABLE_PATHS = [
    '/api/data/history',
    '/api/data/history/filters',
    '/api/data/history/sources',
    '/api/data/history/stats',
    '/api/waze/police-heatmap',
    '/api/waze/police',
    '/api/waze/hazards',
    '/api/waze/roadwork',
    '/api/news/rss',
    '/api/news/sources',
    '/api/stats/summary',
    '/api/stats/history',
    '/api/centralwatch/cameras',
    '/api/centralwatch/sites',
    '/api/aviation/cameras',
    '/api/bom/warnings',
    '/api/rfs/incidents',
    '/api/traffic/incidents',
    '/api/traffic/cameras',
    '/api/beachwatch',
    '/api/beachsafe',
    '/api/pager/hits',
  ];
  app.use('*', async (c, next) => {
    await next();
    if (c.req.method !== 'GET') return;
    const path = new URL(c.req.url).pathname;
    if (!CACHEABLE_PATHS.includes(path)) return;
    if (!c.res.headers.has('Cache-Control')) {
      c.res.headers.set(
        'Cache-Control',
        'public, max-age=30, stale-while-revalidate=300',
      );
    }
  });

  // Origin allowlist. Wildcard `*` was unsafe — `/api/config` returns
  // the API key in its body, so any cross-origin script could read it
  // from a logged-in user's browser. Restrict to the production domain
  // and its dev/preview subdomains. Local dev traffic comes through
  // `null` Origin (file://) or localhost, both allowed below.
  //
  // credentials:true is required because dashboard.html uses
  // `fetch(..., { credentials: 'include' })` to send the
  // nswpsn_dash_sess cookie cross-origin (frontend at
  // nswpsn.forcequit.xyz, API at api.forcequit.xyz). Without
  // `Access-Control-Allow-Credentials: true` in the response, the
  // browser drops the response and the dashboard can't sign in.
  // The `Access-Control-Allow-Origin: *` + credentials combination
  // is forbidden by the CORS spec, but the origin callback only
  // returns '*' for null Origin (curl, file://, server-to-server)
  // where CORS isn't enforced anyway — browsers always send an
  // Origin on cross-origin requests, so the regex-matched specific
  // origin is what flows back to them.
  const ALLOWED_ORIGIN_RE =
    /^https?:\/\/(localhost(:\d+)?|127\.0\.0\.1(:\d+)?|([a-z0-9-]+\.)*forcequit\.xyz|([a-z0-9-]+\.)*nswpsn\.org)$/i;
  app.use(
    '*',
    cors({
      origin: (origin) => {
        if (!origin) return '*'; // file://, curl, server-to-server
        return ALLOWED_ORIGIN_RE.test(origin) ? origin : null;
      },
      credentials: true,
      allowHeaders: ['Authorization', 'Content-Type', 'X-API-Key', 'Accept'],
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

  // Root route — endpoint catalogue. Mirrors python's `/` response at
  // external_api_proxy.py:11431 so any monitoring / smoke-test relying
  // on the catalogue shape keeps working.
  app.get('/', (c) =>
    c.json({
      name: 'NSW Public Safety Network API',
      version: '2.0',
      runtime: 'node',
      mode: process.env['NODE_ENV'] === 'production' ? 'production' : 'dev',
      features: [
        'live-data',
        'data-history',
        'pager-hits',
        'police-heatmap',
        'discord-dashboard',
        'rdio-transcripts',
        'editor-requests',
      ],
      endpoints: {
        live: [
          '/api/rfs/incidents',
          '/api/bom/warnings',
          '/api/traffic/{incidents,roadwork,flood,fire,majorevent,cameras}',
          '/api/waze/{police,hazards,roadwork,alerts,police-heatmap}',
          '/api/endeavour/{current,planned,future}',
          '/api/ausgrid/outages',
          '/api/essential/{outages,planned,future}',
          '/api/beachwatch',
          '/api/beachsafe',
          '/api/aviation/cameras',
          '/api/centralwatch/cameras',
          '/api/news/rss',
          '/api/pager/hits',
        ],
        history: [
          '/api/data/history',
          '/api/data/history/{filters,sources,stats}',
          '/api/data/history/incident/{source}/{source_id}',
          '/api/stats/{summary,history,archive/status}',
        ],
        admin: [
          '/api/admin/db/{stats,vacuum}',
          '/api/cache/{clear,status,stats}',
          '/api/debug/{sessions,heartbeat-test,traffic-raw,test-all}',
        ],
        dashboard: [
          '/api/dashboard/auth/{login,callback,logout}',
          '/api/dashboard/me',
          '/api/dashboard/guilds/{guildId}/{channels,roles,presets,mute-state}',
          '/api/dashboard/admin/{overview,broadcast,cleanup,bot-actions}',
        ],
        editor: [
          '/api/editor-requests',
          '/api/editor-requests/{id}/{approve,reject}',
          '/api/incidents',
          '/api/users',
          '/api/check-editor/{userId}',
          '/api/check-admin/{userId}',
        ],
        rdio: [
          '/api/rdio/transcripts/search',
          '/api/rdio/calls/{callId}',
          '/api/summaries/{latest,trigger}',
        ],
        utility: [
          '/api/health',
          '/api/config',
          '/api/heartbeat',
          '/api/status',
          '/api/collection/status',
          '/api/w3w/{convert-to-coordinates,convert-to-3wa,grid-section}',
        ],
      },
    }),
  );

  return app;
}
