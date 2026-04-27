import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Env vars exposed to ALL tests before any module is imported.
    // Critical because src/config.ts parses process.env at module load
    // time — anything set in a beforeAll() is too late.
    env: {
      // WAZE_INGEST_KEY enables the /api/waze/ingest auth middleware in
      // tests. Without it the middleware returns 403 "ingest disabled".
      WAZE_INGEST_KEY: 'test-ingest-key',
      // Stable temp-ish state dir so LiveStore disk writes don't bleed
      // into the project tree during tests.
      STATE_DIR: './test/.tmp-state',
      // Quieter logs in CI; debug only when running locally.
      LOG_LEVEL: 'warn',
    },
  },
});
