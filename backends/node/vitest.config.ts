import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Several suites open with `await import('../../../src/server.js')`, which
    // cold-transforms a 59-import module graph. Alone that is fast, but with
    // 100+ test files running in parallel it intermittently passed vitest's
    // 5s default and timed out — the suite failed roughly one run in four,
    // always on whichever file happened to import the server first.
    //
    // The cost is transform time in the dev/CI environment, not anything the
    // product does, so the timeout is the thing that was wrong. 20s still
    // catches a genuine hang by a wide margin.
    testTimeout: 20_000,
    hookTimeout: 20_000,
    // Env vars exposed to ALL tests before any module is imported.
    // Critical because src/config.ts parses process.env at module load
    // time — anything set in a beforeAll() is too late.
    env: {
      // WAZE_INGEST_KEY enables the /api/waze/ingest auth middleware in
      // tests. Without it the middleware returns 403 "ingest disabled".
      WAZE_INGEST_KEY: 'test-ingest-key',
      // Stable NSWPSN_API_KEY so tests hitting private endpoints can
      // pass it via X-API-Key without depending on the prod default.
      NSWPSN_API_KEY: 'test-api-key',
      // HS256 secret so optionalSupabaseJwt/requireSupabaseJwt can verify
      // tokens signed by the tests. SUPABASE_URL is intentionally left
      // unset so iss/aud aren't enforced — tests just need sub.
      SUPABASE_JWT_SECRET: 'test-jwt-secret-0123456789',
      // Stable temp-ish state dir so LiveStore disk writes don't bleed
      // into the project tree during tests.
      STATE_DIR: './test/.tmp-state',
      // Quieter logs in CI; debug only when running locally.
      LOG_LEVEL: 'warn',
      // Stub Endeavour Supabase config so the source's config-presence
      // check doesn't trip its mocked fetch tests. The values are never
      // hit upstream — every test that exercises the fetcher mocks
      // shared/http.js to return canned payloads.
      ENDEAVOUR_SUPABASE_URL: 'https://test.supabase.co',
      ENDEAVOUR_SUPABASE_KEY: 'test-key',
    },
  },
});
