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
  // Port the Node server binds. Defaults to 3001 so it sits next to the
  // Python backend (which holds the real production port) during the
  // migration. Apache routes per-path to whichever backend owns the
  // endpoint.
  PORT: z
    .string()
    .default('3001')
    .transform((s) => Number.parseInt(s, 10))
    .refine((n) => Number.isFinite(n) && n > 0 && n < 65536, {
      message: 'PORT must be a valid port number',
    }),

  // dev | production | test. Mirrors Python's DEV_MODE flag (line 19063
  // area). 'test' is added because Vitest sets NODE_ENV=test by default
  // and we want the test run to exercise the real config path; tests
  // get bucketed alongside dev for the /api/health mode label.
  NODE_ENV: z.enum(['dev', 'production', 'test']).default('dev'),

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

  // ArchiveWriter flush cadence — how often the in-RAM queue gets
  // written to Postgres. Mirrors Python's ARCHIVE_FLUSH_INTERVAL.
  ARCHIVE_FLUSH_INTERVAL_MS: z
    .string()
    .default('30000')
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
  // pruned from the in-memory cache. Mirrors Python's
  // WAZE_INGEST_MAX_AGE — defaults to 40 min so the userscript's
  // ~16 min full rotation has 2x headroom.
  WAZE_INGEST_MAX_AGE_SECS: z
    .string()
    .default('2400')
    .transform((s) => Number.parseInt(s, 10)),
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
