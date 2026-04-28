/**
 * Single source of truth for typed environment config.
 *
 * Parsed once at startup with zod; throws a descriptive error if anything
 * required is missing or malformed. Every other module imports `config`
 * from here — no `process.env` reads anywhere else.
 *
 * Mirrors the env vars the Python backend reads. As more endpoints get
 * ported, more vars get added here. Defaults match Python's defaults so
 * a single .env can drive both backends during the strangler-fig phase.
 */
import { z } from 'zod';

const Schema = z.object({
  // Port the Node server binds. Defaults to 3000, the same port the
  // python service ran on — Cloudflare Tunnel fronts api.forcequit.xyz
  // and points at this single port. Override via `PORT=` in the env.
  PORT: z
    .string()
    .default('3000')
    .transform((s) => Number.parseInt(s, 10))
    .refine((n) => Number.isFinite(n) && n > 0 && n < 65536, {
      message: 'PORT must be a valid port number',
    }),

  // dev | production | test. 'test' is added because Vitest sets
  // NODE_ENV=test by default and we want the test run to exercise the
  // real config path; tests get bucketed alongside dev for the
  // /api/health mode label.
  // The default reads Python's DEV_MODE as a fallback so the existing
  // backends/.env (which only defines DEV_MODE) drives both backends
  // without the operator setting NODE_ENV separately.
  NODE_ENV: z
    .enum(['dev', 'production', 'test'])
    .default(process.env['DEV_MODE'] === 'false' ? 'production' : 'dev'),

  // The shared API key clients send via Authorization: Bearer / X-API-Key
  // / ?api_key=. Same value the Python backend uses. /api/config returns
  // it to the frontend so map.html etc. don't need it baked in.
  NSWPSN_API_KEY: z
    .string()
    .min(1, 'NSWPSN_API_KEY is required')
    .default('nswpsn-live-2024-secure'),

  // Postgres connection string. Optional during W1 because /api/health
  // and /api/config don't touch the DB; gets enforced in later weeks
  // when the archive layer comes online.
  DATABASE_URL: z.string().url().optional(),

  // Tunable log level for pino. trace|debug|info|warn|error|fatal.
  LOG_LEVEL: z
    .enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal'])
    .default('info'),

  // Where LiveStore dumps its in-memory snapshots so a restart doesn't
  // empty the live cache. One JSON file per source under this dir.
  // Atomic writes via temp-file + rename, so the file on disk is always
  // either the previous snapshot or a complete current one.
  STATE_DIR: z.string().default('./state'),

  // ArchiveWriter flush cadence in ms. Falls back to Python's
  // seconds-based ARCHIVE_FLUSH_INTERVAL when only the legacy var is
  // set in the shared .env, so a single env source drives both backends.
  ARCHIVE_FLUSH_INTERVAL_MS: z
    .string()
    .default(
      process.env['ARCHIVE_FLUSH_INTERVAL']
        ? String(Number(process.env['ARCHIVE_FLUSH_INTERVAL']) * 1000)
        : '30000',
    )
    .transform((s) => Number.parseInt(s, 10)),

  // LiveStore persist cadence — how often disk dumps happen for each
  // source that has new data since the last dump.
  LIVE_PERSIST_INTERVAL_MS: z
    .string()
    .default('30000')
    .transform((s) => Number.parseInt(s, 10)),

  // Waze userscript posts auth via X-Ingest-Key. Same value the Python
  // backend uses; .env carries it.
  WAZE_INGEST_KEY: z.string().min(1).optional(),

  // How long an ingested Waze bbox snapshot stays "live" before it's
  // pruned. Falls back to Python's WAZE_INGEST_MAX_AGE so the same .env
  // drives both backends. Default 40 min — userscript's ~16 min
  // rotation gets 2x headroom.
  WAZE_INGEST_MAX_AGE_SECS: z
    .string()
    .default(process.env['WAZE_INGEST_MAX_AGE'] ?? '2400')
    .transform((s) => Number.parseInt(s, 10)),

  // Endeavour Energy switched from Sitecore to a public Supabase
  // project for outage data. Their anon key is published in their
  // own frontend bundle but we plumb both URL + key through env so
  // they're not committed and so we can rotate without a redeploy.
  // Same env var names the Python backend reads (line 809-810).
  ENDEAVOUR_SUPABASE_URL: z.string().url().optional(),
  ENDEAVOUR_SUPABASE_KEY: z.string().optional(),

  // Self-hosted Pagermon /api/messages endpoint. Optional — if unset,
  // the pager source skips polling and `/api/pager/hits` returns an
  // empty FeatureCollection. Mirrors Python's PAGERMON_URL +
  // PAGERMON_API_KEY at external_api_proxy.py:791-792.
  PAGERMON_URL: z.string().optional(),
  PAGERMON_API_KEY: z.string().optional(),

  // Supabase project URL — used by /api/users (Auth Admin API listing)
  // and the JWT issuer-claim check. Mirrors Python's SUPABASE_URL.
  SUPABASE_URL: z.string().url().optional(),
  // Supabase service-role key — required by /api/users. Mirrors
  // Python's SUPABASE_SERVICE_ROLE_KEY at external_api_proxy.py.
  SUPABASE_SERVICE_ROLE_KEY: z.string().optional(),
  // HS256 signing secret used to verify Supabase-issued JWTs on
  // editor/admin routes (Authorization: Bearer <jwt>). Find it in
  // Supabase Dashboard > Settings > API > JWT Secret. Optional in tests
  // — the JWT middleware reports 503 when unset so handlers don't
  // accidentally accept unverified tokens.
  SUPABASE_JWT_SECRET: z.string().optional(),

  // rdio-scanner self-hosted Postgres URL. Required by /api/rdio/*
  // routes (transcripts search + call detail). When unset those
  // routes return 503 with a clear "not configured" body, matching
  // python's _rdio_is_configured() check.
  RDIO_DATABASE_URL: z.string().optional(),

  // Timezone for rdio summary windows + transcripts/search day-bounds.
  // Mirrors python's SUMMARY_TZ env var; default to Sydney since this
  // is an NSW-focused service.
  SUMMARY_TZ: z.string().default('Australia/Sydney'),

  // Public URL prefix for rdio-scanner call links. Mirrors python's
  // hardcoded https://radio.forcequit.xyz/?call= but exposed as env
  // so dev/staging deployments can override without a code change.
  RDIO_CALL_URL_BASE: z
    .string()
    .default('https://radio.forcequit.xyz/?call='),

  // Gemini API key for /api/summaries/trigger and the optional hourly
  // scheduler. Mirrors python's GEMINI_API_KEY. When unset, the
  // trigger endpoint returns 503 and the scheduler refuses to start.
  GEMINI_API_KEY: z.string().optional(),

  // Gemini model id. Default matches python's _LLM_DEFAULT_MODEL.
  LLM_MODEL: z.string().default('gemini-2.5-flash'),

  // Gate the in-process hourly summary scheduler. Defaulted OFF
  // during the python→Node cutover to avoid double-spending Gemini
  // quota with two schedulers running. Now defaults ON since python
  // is stopped — host operators can still set NODE_RDIO_SCHEDULER=false
  // explicitly to keep it disabled (e.g. on a node serving as a
  // read-only mirror).
  NODE_RDIO_SCHEDULER: z
    .enum(['true', 'false'])
    .default('true')
    .transform((s) => s === 'true'),

  // Discord OAuth dashboard. All optional — when unset the dashboard
  // routes return 503 with a clear error body. Mirrors python's
  // _dash_enabled() check.
  DISCORD_CLIENT_ID: z.string().optional(),
  DISCORD_CLIENT_SECRET: z.string().optional(),
  DISCORD_BOT_TOKEN: z.string().optional(),
  DASHBOARD_SESSION_SECRET: z.string().optional(),
  DASHBOARD_REDIRECT_URI: z.string().optional(),
  DASHBOARD_FRONTEND_URL: z.string().optional(),
  DASHBOARD_FRONTEND_BASE: z.string().optional(),
  DASHBOARD_COOKIE_DOMAIN: z.string().default('.forcequit.xyz'),
  DASHBOARD_ADMIN_IDS: z.string().optional(),
  // Bot-data Postgres (the discord-bot's separate Postgres cluster).
  // Holds dash_sessions + alert_presets + mute_state + bot_actions.
  // Optional — when unset the dashboard returns 503 on all routes
  // that touch this pool.
  BOT_DATA_DATABASE_URL: z.string().optional(),
  // Public base URL of the API itself (used in OAuth redirect_uri).
  PUBLIC_BASE_URL: z.string().optional(),

  // Kill switch for the centralwatch Playwright browser worker. When
  // true, the worker doesn't init, the JSON refresh loop is skipped,
  // and the image batch loop stays idle. Reads use the last-good JSON
  // file. Mirrors python's _playwright_available short-circuit.
  CENTRALWATCH_DISABLED: z
    .enum(['true', 'false'])
    .default('false')
    .transform((s) => s === 'true'),
});

const parsed = Schema.safeParse(process.env);

if (!parsed.success) {
  // Print a flat list of issues — much easier to read than the default
  // ZodError tree dump in a startup log.
  const issues = parsed.error.issues
    .map((i) => `  - ${i.path.join('.')}: ${i.message}`)
    .join('\n');
  console.error(`Invalid environment configuration:\n${issues}`);
  process.exit(1);
}

export const config = parsed.data;
export type Config = typeof config;

// Convenience for the response shape on /api/health, which Python returns
// as 'dev' | 'production'. Centralised here so the eventual contract
// tests have one place to assert.
export const modeLabel = (): 'dev' | 'production' =>
  config.NODE_ENV === 'production' ? 'production' : 'dev';
