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
// Simple sources (W3).
import { rfsRouter } from './api/rfs.js';
import { actAmbulanceRouter } from './api/act-ambulance.js';
import { bomRouter } from './api/bom.js';
import { trafficRouter } from './api/traffic.js';
import { beachRouter } from './api/beach.js';
import { dorsalRouter } from './api/dorsal.js';
import { weatherRouter } from './api/weather.js';
import { pagerRouter } from './api/pager.js';
import { firmsRouter } from './api/firms.js';
import { adsbRouter } from './api/adsb.js';
import { radioPublicRouter } from './api/radio-public.js';
// Power sources + heartbeat + stats (W4).
import { endeavourRouter } from './api/endeavour.js';
import { ausgridRouter } from './api/ausgrid.js';
import { essentialRouter } from './api/essential.js';
import { outagesRouter } from './api/outages.js';
import { heartbeatRouter } from './api/heartbeat.js';
import { statsRouter } from './api/stats.js';
// Incidents / editor / users (W5).
import { incidentsRouter } from './api/incidents.js';
import { wireRouter } from './api/wire.js';
import { wireCommentsRouter } from './api/wireComments.js';
import { fleetRouter } from './api/fleet.js';
import { profilesRouter } from './api/profiles.js';
import { editorRouter } from './api/editor.js';
import { ticketsRouter } from './api/tickets.js';
import { feedbackRouter } from './api/feedback.js';
import { staffNotifyRouter } from './api/staffNotify.js';
import { staffNoticesRouter } from './api/staffNotices.js';
import { referralsRouter } from './api/referrals.js';
import { usersRouter } from './api/users.js';
// Data-history archive reads (W6).
import { dataHistoryRouter } from './api/data-history.js';
// Aviation / news / summaries / transcripts (W7).
import { aviationRouter } from './api/aviation.js';
import { qldCamerasRouter } from './api/qld-cameras.js';
import { ntFireRouter } from './api/nt-fire.js';
import { boundariesRouter } from './api/boundaries.js';
import { qldFireRouter } from './api/qld-fire.js';
import { vicEmergencyRouter } from './api/vic-emergency.js';
import { waEmergencyRouter } from './api/wa-emergency.js';
import { saFireRouter } from './api/sa-fire.js';
import { firesRouter } from './api/fires.js';
import { agencyRouter } from './api/agency.js';
import { marinetrafficRouter } from './api/marinetraffic.js';
import { newsRouter } from './api/news.js';
import { summariesRouter } from './api/summaries.js';
import { transcriptsRouter } from './api/transcripts.js';
import { whisperRouter } from './api/whisper.js';
// Centralwatch + dashboard (W8). Image proxy + dashboard endpoints
// are 503 stubs — Apache routes those prefixes to python.
import { centralwatchRouter } from './api/centralwatch.js';
import { dashboardRouter } from './api/dashboard.js';
// Uptime Kuma-shaped /api/status (cutover blocker for monitor flips).
import { statusRouter } from './api/status.js';
// What3Words proxy + system/debug/admin.
import { w3wRouter } from './api/w3w.js';
import { transportRouter } from './api/transport.js';
import { systemRouter } from './api/system.js';
// Feeder nodes
import { nodesRouter } from './api/nodes.js';
import { nodeUpdatesRouter } from './api/node-updates.js';
import { feederRouter } from './api/feeder.js';
import { nodeEnrolRouter } from './api/node-enrol.js';
import { nodeIngestRouter } from './api/node-ingest.js';
import { scannerIngestRouter } from './api/scanner-ingest.js';
import { nodeDataRouter } from './api/node-data.js';
import { requireApiKey } from './services/auth/apiKey.js';
import { apiKeyUsage } from './services/auth/apiKeyUsage.js';
import { isFirstPartyOrigin } from './services/auth/firstParty.js';
import { optionalSupabaseJwt } from './services/auth/supabaseJwt.js';
import { sessionRouter } from './api/session.js';
import { apiKeysRouter } from './api/apiKeys.js';
// The versioned public data API (/api/v1/) and its description. The reference
// page that renders it is on the site (api-docs.html), not here.
import { mirror } from './api/v1/mirror.js';
import { notFound, onError } from './api/v1/respond.js';
import { openapiRouter } from './api/v1/openapi.js';
import { unversionedPath } from './lib/apiVersion.js';
import { log } from './lib/log.js';

// Paths that should never appear in the request log even on
// success — they fire constantly and drown out everything else.
// Failures still show up because the success-only filter below
// only short-circuits on 2xx/3xx. Their /api/v1/ twins are just as quiet.
const QUIET_PATHS_RE =
  /^\/api\/(?:v1\/)?(?:heartbeat|check-editor\/|config|health|status)/;

const SLOW_REQUEST_MS = 500;

// In dev, log every 2xx/3xx request at `info` so operators see the live
// stream. In production the default LOG_LEVEL=info would flood the log
// with browser polls + dashboard fetches, so successful requests stay at
// `debug` (set LOG_LEVEL=debug to surface them when needed).
const VERBOSE_REQUEST_LOG = process.env['NODE_ENV'] !== 'production';

/**
 * Classify the request origin, so the log answers "who is doing this" at a
 * glance instead of lumping every machine caller into "other".
 *
 * Identity comes from whatever each caller can actually carry:
 *
 *   discord-bot   UA `AusAwareBot/…` (NSWPSNBot pre-rebrand) or
 *                 X-Client-Type: discord-bot — bot.py sets both.
 *   node          UA `NSWPSN-NodeAgent/…` — every feeder agent (radio and
 *                 pager alike) sends it on every request (version.go).
 *   rdio          the PATH. rdio's downstream sender cannot set custom
 *                 headers (the reason scanner-ingest auths by form field),
 *                 so /api/scanner-ingest/ itself is the identity.
 *   whisper       the path again: /api/whisper/v1/ is only ever rdio's
 *                 transcripts plugin asking for a transcription.
 *   whisper-node  X-Client-Type: whisper-node — the PC's idle watcher
 *                 (whisper_watch.ps1). Without the header its
 *                 Invoke-RestMethod UA reads as a browser, which had its
 *                 status polls masquerading as dashboard traffic.
 *   browser       Mozilla/Chrome/Safari UA.
 *   other         what remains: curl, probes, unknown callers.
 *
 * Paths win over headers for rdio/whisper because they are the stronger
 * claim: nothing else posts to those routes, and the callers that do
 * cannot carry anything else.
 */
type ClientKind =
  | 'browser'
  | 'discord-bot'
  | 'node'
  | 'rdio'
  | 'whisper'
  | 'whisper-node'
  | 'other';

function classifyClient(ua: string, xClient: string, path: string): ClientKind {
  if (path.startsWith('/api/whisper/v1/')) return 'whisper';
  if (path.startsWith('/api/scanner-ingest/')) return 'rdio';
  const x = xClient.toLowerCase();
  if (x === 'discord-bot') return 'discord-bot';
  if (x === 'whisper-node') return 'whisper-node';
  if (!ua) return 'other';
  if (/^NSWPSN-NodeAgent\//.test(ua)) return 'node';
  // AusAwareBot is the current UA; NSWPSNBot is the pre-rebrand one, kept so a
  // bot that hasn't restarted yet is still classified as a bot, not "other".
  if (/^(?:AusAware|NSWPSN)Bot/i.test(ua) || /\bdiscord(?:bot)?\b/i.test(ua)) return 'discord-bot';
  if (/Mozilla\/|Chrome\/|Safari\/|Firefox\/|Edge\/|Edg\//.test(ua)) return 'browser';
  return 'other';
}

/**
 * The client tag is plain text in the message, plus a `client` field. The
 * colour is added by the pretty-printer (lib/log.ts), not here: pino-pretty
 * strips control characters out of every message, so colour codes written
 * into the text reached `pm2 logs` with their ESC removed, as literal
 * "[93mwhisper-node[0m".
 */
function clientTag(client: ClientKind): string {
  return `[${client}]`;
}

/**
 * Replacement for Hono's built-in logger. Differences:
 *   - 2xx + 3xx on QUIET_PATHS_RE are silent
 *   - 2xx + 3xx slower than SLOW_REQUEST_MS log at info ("slow")
 *   - other 2xx + 3xx log at info in dev, debug in production
 *     (production-default LOG_LEVEL=info would otherwise flood the log)
 *   - 4xx logs at info ("client error")
 *   - 5xx logs at warn ("server error")
 *   - OPTIONS preflights are silent (huge volume, no signal)
 *   - every line tags the client (browser, discord-bot, node, rdio,
 *     whisper, whisper-node, other), colour-coded outside production,
 *     so the streams separate at a glance — see classifyClient
 */
const requestLogger: MiddlewareHandler = async (c, next) => {
  const start = Date.now();
  await next();
  const ms = Date.now() - start;
  const status = c.res.status;
  const method = c.req.method;
  const path = new URL(c.req.url).pathname;

  if (method === 'OPTIONS') return;

  const ua = c.req.header('User-Agent') ?? '';
  const xClient = c.req.header('X-Client-Type') ?? '';
  const client = classifyClient(ua, xClient, path);

  // One-line human-readable format. Earlier versions passed the fields as
  // a structured object so pino-pretty would render them inline, but
  // `singleLine: true` stringified them as JSON, drowning the actual
  // signal under {"method":"GET",…} on every line. Embed everything in
  // the message string instead — production ndjson keeps it grep-able,
  // dev pretty-print shows the same clean line.
  // Which credential class got the request through the gate (apiKey.ts). A
  // named key is tagged by its lookup prefix — enough to tell keys apart in
  // a tail, never the key itself. Absent on public paths and refusals.
  const authClass = c.get('authClass');
  const authTag = !authClass
    ? ''
    : authClass === 'apikey'
      ? ` auth=key:${c.get('apiKeyPrefix') ?? '?'}`
      : ` auth=${authClass}`;
  const line = `${method} ${path} → ${status} ${ms}ms ${clientTag(client)}${authTag}`;

  const f = { client };
  if (status >= 500) {
    log.warn(f, `5xx ${line}`);
    return;
  }
  if (status >= 400) {
    log.info(f, `4xx ${line}`);
    return;
  }
  if (QUIET_PATHS_RE.test(path)) return;
  if (ms >= SLOW_REQUEST_MS) {
    log.info(f, `slow ${line}`);
    return;
  }
  if (VERBOSE_REQUEST_LOG) {
    log.info(f, line);
    return;
  }
  log.debug(f, line);
};

export function createApp() {
  const app = new Hono();

  // Custom request logger — see comment on requestLogger above.
  app.use('*', requestLogger);

  // Per-key usage counts (apiKeyUsage.ts). Outside the credential gate, so
  // it sees the gate's refusals of a key as well as what the route answered.
  app.use('*', apiKeyUsage);

  // Brotli/gzip compression. The /api/data/history full payloads and
  // (~1-2 MB raw) compress ~10×. Cloudflare/
  // Apache may also gzip in front; double-compression is detected by
  // hono/compress (skips when Content-Encoding already set).
  app.use('*', compress());

  // Short-lived public cache for cheap GET reads. Browsers + CDNs can
  // serve repeat hits without round-tripping to Node. 30s is short
  // enough that live data stays "live", but covers rapid double-clicks
  // and dashboard panel re-renders. SWR=300s lets stale responses tide
  // a request over while we revalidate in the background.
  // Skipped for auth-sensitive endpoints (/api/config, /api/dashboard/*),
  // per-user state (/api/heartbeat), and POST/PUT/DELETE. A path's /api/v1/
  // twin (api/v1/mirror.ts) is cached alike.
  const CACHEABLE_PATHS = [
    '/api/data/history',
    '/api/data/history/filters',
    '/api/data/history/sources',
    '/api/data/history/stats',
    '/api/news/rss',
    '/api/news/sources',
    '/api/stats/summary',
    '/api/stats/history',
    '/api/radio/monitored-sites',
    '/api/radio/grn-sites',
    '/api/centralwatch/cameras',
    '/api/centralwatch/sites',
    '/api/aviation/cameras',
    '/api/bom/warnings',
    '/api/rfs/incidents',
    '/api/fires',
    '/api/outages',
    '/api/traffic/incidents',
    '/api/traffic/cameras',
    '/api/beachwatch',
    '/api/beachsafe',
    '/api/pager/hits',
    '/api/firms/hotspots',
    // Aircraft trails: prebuilt per poll, frontend fetches at the same
    // 30s cadence as this cache window — the CDN absorbs all clients.
    '/api/adsb/trails',
    // TfNSW service alerts: fixed path, no viewport scoping.
    '/api/transport/alerts',
  ];
  app.use('*', async (c, next) => {
    await next();
    if (c.req.method !== 'GET') return;
    // Only an answer worth repeating. A browser that kept a 401 for 30 s
    // (and 300 more stale) would hand it straight back to the retry that
    // follows it — api-session.js re-mints after a 401 and asks again at
    // once — and a kept 503 would outlast the outage it reports.
    if (c.res.status < 200 || c.res.status >= 300) return;
    const path = unversionedPath(new URL(c.req.url).pathname);
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
  // Deliberately NARROW: an explicit allow-list of first-party frontend
  // origins only — NOT arbitrary subdomains. The old regex matched every
  // *.forcequit.xyz / *.nswpsn.org host; combined with credentials:true
  // below, that let a compromised sibling subdomain (radio.forcequit.xyz,
  // pager.forcequit.xyz — third-party self-hosted apps) make credentialed
  // cross-origin reads of the dashboard API using a logged-in user's
  // cookie. Listing exact origins removes that pivot. The public
  // map/live/logs pages are served from nswpsn.forcequit.xyz, which is
  // included, so they keep working.
  //
  // The allowlist itself (the regex plus CORS_ALLOWED_ORIGINS) lives in
  // services/auth/firstParty.ts, shared with the browser-token gate: the set
  // of origins that may read responses must be the set that may hold a
  // session token, or a page CORS admits would fail every fetch.
  app.use(
    '*',
    cors({
      origin: (origin) => {
        if (!origin) return '*'; // file://, curl, server-to-server
        return isFirstPartyOrigin(origin) ? origin : null;
      },
      credentials: true,
      allowHeaders: ['Authorization', 'Content-Type', 'X-API-Key', 'Accept'],
      // feeder.html fetches the installer as a blob and reads its filename
      // (which carries the node token for Windows) from Content-Disposition;
      // that header is only visible to cross-origin JS when explicitly
      // exposed. Frontend is on nswpsn.forcequit.xyz, API on api.forcequit.xyz.
      exposeHeaders: ['Content-Disposition'],
      maxAge: 600,
    }),
  );

  // Identify the caller from a Supabase JWT (if one is presented) BEFORE
  // the API-key gate, so privileged routes can role-check the real user
  // and the key gate can let a logged-in user through. Purely additive:
  // opaque NSWPSN_API_KEY bearer tokens aren't JWT-shaped so this is a
  // no-op for them.
  app.use('*', optionalSupabaseJwt);

  // Global NSWPSN_API_KEY gate. The middleware itself short-circuits for
  // OPTIONS preflights, public endpoints (/api/health, /api/config,
  // /api/heartbeat, POST /api/editor-requests,
  // /api/check-editor/*, etc.), any non-/api path, and any request already
  // authenticated as a Supabase user (a logged-in user is at least as
  // trusted as the public key). Mirrors Python's global before_request.
  app.use('*', requireApiKey);

  // Register route modules. Each router defines its own paths under
  // /api/...; mounting at '/' keeps the handlers' URLs identical to
  // the Python equivalents.
  //
  // mirror() serves the routes it names under /api/v1/ as well, with the
  // same handler chain (api/v1/mirror.ts). Only the public data API is
  // versioned — docs/api.md has the list and the policy.
  app.route('/', healthRouter);
  mirror(app, healthRouter, ['/api/health']);
  app.route('/', configRouter);
  app.route('/', sessionRouter);
  // Simple sources
  app.route('/', rfsRouter);
  // Not the incidents: API keys read every fire service together, at /api/v1/fires.
  mirror(app, rfsRouter, ['/api/rfs/fdr']);
  app.route('/', actAmbulanceRouter);
  mirror(app, actAmbulanceRouter, ['/api/act-ambulance/incidents']);
  app.route('/', bomRouter);
  mirror(app, bomRouter, ['/api/bom/warnings']);
  app.route('/', trafficRouter);
  // Not /api/traffic/cameras: camera images are not part of the API.
  mirror(app, trafficRouter, [
    '/api/traffic/incidents', '/api/traffic/incidents/raw',
    '/api/traffic/roadwork', '/api/traffic/roadwork/raw',
    '/api/traffic/flood', '/api/traffic/flood/raw',
    '/api/traffic/fire', '/api/traffic/fire/raw',
    '/api/traffic/majorevent', '/api/traffic/majorevent/raw',
    '/api/traffic/alpine', '/api/traffic/alpine/raw',
    '/api/traffic/lga', '/api/traffic/lga/raw',
    '/api/traffic/lga-incidents', '/api/traffic/all-feeds',
  ]);
  app.route('/', beachRouter);
  // Not /api/beachsafe/details: nothing fills its store; it goes when old paths are retired.
  mirror(app, beachRouter, ['/api/beachwatch', '/api/beachsafe', '/api/beachsafe/beach/:slug']);
  app.route('/', dorsalRouter);
  mirror(app, dorsalRouter, ['/api/dorsal/reports']);
  app.route('/', weatherRouter);
  mirror(app, weatherRouter, [
    '/api/weather/current', '/api/weather/radar', '/api/weather/grid/manifest', '/api/weather/grid', '/api/weather/point',
  ]);
  app.route('/', pagerRouter);
  mirror(app, pagerRouter, ['/api/pager/hits']);
  app.route('/', firmsRouter);
  mirror(app, firmsRouter, ['/api/firms/hotspots']);
  app.route('/', adsbRouter);
  // Not /api/adsb/receivers: node status, never part of the public API.
  mirror(app, adsbRouter, ['/api/adsb/aircraft', '/api/adsb/history', '/api/adsb/trails', '/api/adsb/photo/:hex', '/api/adsb/info/:hex']);
  app.route('/', radioPublicRouter);
  // Not on /api/v1/: the site list is this site's own reference data, and
  // monitored-sites is node status.
  // Power + heartbeat + stats. The per-distributor outage routes are this
  // site's and the bot's; API keys read every distributor together, at
  // /api/v1/outages (api/outages.ts).
  app.route('/', endeavourRouter);
  app.route('/', ausgridRouter);
  app.route('/', essentialRouter);
  app.route('/', outagesRouter);
  mirror(app, outagesRouter, ['/api/outages']);
  app.route('/', heartbeatRouter);
  app.route('/', statsRouter);
  // Incidents + editor + users (W5)
  app.route('/', incidentsRouter);
  app.route('/', wireRouter);
  app.route('/', wireCommentsRouter);
  app.route('/', fleetRouter);
  app.route('/', profilesRouter);
  app.route('/', editorRouter);
  app.route('/', ticketsRouter);
  app.route('/', feedbackRouter);
  app.route('/', staffNotifyRouter);
  app.route('/', staffNoticesRouter);
  app.route('/', referralsRouter);
  app.route('/', usersRouter);
  app.route('/', apiKeysRouter);
  // Data-history archive reads (W6). On /api/v1/ these are /api/v1/history…
  // (lib/apiVersion.ts). Not /sources or /stats: counts across every source,
  // for this site's own pages; no key reaches them.
  app.route('/', dataHistoryRouter);
  mirror(app, dataHistoryRouter, [
    '/api/data/history', '/api/data/history/filters', '/api/data/history/incident/:source/:source_id',
  ]);
  // Aviation cameras + news RSS + summaries (W7). No camera and no news
  // route is on /api/v1/: camera images and other outlets' news are not part
  // of the API.
  app.route('/', aviationRouter);
  app.route('/', qldCamerasRouter);
  app.route('/', ntFireRouter);
  app.route('/', boundariesRouter);
  // Not on /api/v1/: boundaries are this site's own reference data, which API keys do not read.
  app.route('/', whisperRouter);
  app.route('/', qldFireRouter);
  app.route('/', vicEmergencyRouter);
  app.route('/', waEmergencyRouter);
  app.route('/', saFireRouter);
  // Every fire service in one feed (api/fires.ts), with the QFD and DFES
  // warnings. The per-agency incident and warning routes above are this
  // site's and the bot's; API keys read this one. The NT
  // feed asks not to be re-published (sources/ntFire.ts); the owner decided to
  // carry it, with NTFRS credited in the description.
  app.route('/', firesRouter);
  mirror(app, firesRouter, ['/api/fires']);
  app.route('/', agencyRouter);
  mirror(app, agencyRouter, ['/api/agency/extended', '/api/agency/extended/:slug']);
  app.route('/', marinetrafficRouter);
  // Not /api/marinetraffic/vessel-image/:id: nothing uses it, and it goes when old paths are retired.
  mirror(app, marinetrafficRouter, ['/api/marinetraffic/vessels', '/api/marinetraffic/vessel/:id']);
  app.route('/', newsRouter);
  app.route('/', summariesRouter);
  // Not /api/summaries/trigger: owner tooling.
  mirror(app, summariesRouter, ['/api/summaries/latest', '/api/summaries']);
  app.route('/', transcriptsRouter);
  // Not on /api/v1/: API keys read the summaries, not transcripts.
  // Centralwatch reads + dashboard 503 stubs (W8)
  app.route('/', centralwatchRouter);
  app.route('/', dashboardRouter);
  // Uptime-Kuma-shaped status endpoint
  app.route('/', statusRouter);
  mirror(app, statusRouter, ['/api/status']);
  // What3Words proxy
  app.route('/', w3wRouter);
  // AnyTrip public-transport proxy. Deliberately NOT in CACHEABLE_PATHS —
  // that middleware ignores query strings, so a CDN would cross-serve one
  // viewport's vehicles to every viewport.
  app.route('/', transportRouter);
  mirror(app, transportRouter, [
    '/api/transport/vehicles',
    '/api/transport/shape/:id',
    '/api/transport/trip/:date/:tripId/:instance',
    '/api/transport/departures/:stopId',
    '/api/transport/lines',
    '/api/transport/stops',
    '/api/transport/alerts',
  ]);
  // System / debug / admin (cache clear, debug/* echoes, admin/db/*)
  app.route('/', systemRouter);
  // Feeder nodes. node-updates FIRST so its exact `/api/nodes/versions`
  // matches before nodesRouter's `/api/nodes/:id` treats "versions" as an id.
  app.route('/', nodeUpdatesRouter);
  app.route('/', nodesRouter);
  app.route('/', feederRouter);
  // Feeder node call relay
  app.route('/', nodeEnrolRouter);
  app.route('/', nodeIngestRouter);
  app.route('/', scannerIngestRouter);
  // Feeder node event analytics (staff Data tab, owner|dev only)
  app.route('/', nodeDataRouter);

  // The API description, filtered per key.
  app.route('/', openapiRouter);

  // JSON for /api/v1/; every other path keeps Hono's plain-text answers.
  app.notFound(notFound);
  app.onError(onError);

  // Root route — endpoint catalogue. Mirrors python's `/` response at
  // external_api_proxy.py:11431 so any monitoring / smoke-test relying
  // on the catalogue shape keeps working.
  app.get('/', (c) =>
    c.json({
      name: 'AusAware API',
      version: '2.0',
      runtime: 'node',
      mode: process.env['NODE_ENV'] === 'production' ? 'production' : 'dev',
      features: [
        'live-data',
        'data-history',
        'pager-hits',
        // 'police-heatmap' was advertised here after its routes were retired
        // with the Waze ingest; the frontend's loaders are hard-guarded off.
        'discord-dashboard',
        'rdio-transcripts',
        'editor-requests',
      ],
      endpoints: {
        live: [
          '/api/rfs/incidents',
          '/api/act-ambulance/incidents',
          '/api/bom/warnings',
          '/api/traffic/{incidents,roadwork,flood,fire,majorevent,cameras}',
          '/api/endeavour/{current,planned,future}',
          '/api/ausgrid/outages',
          '/api/essential/{outages,planned,future}',
          '/api/beachwatch',
          '/api/beachsafe',
          '/api/dorsal/reports',
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
