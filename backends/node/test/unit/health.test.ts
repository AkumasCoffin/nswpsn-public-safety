/**
 * Sanity test for the W1 endpoints. Verifies:
 *   - /api/health returns the same shape Python does
 *   - /api/config returns apiKey + version
 *   - Unknown routes 404 (Hono default)
 *
 * Once W2 lands and we have prod-captured contract fixtures, these
 * become full byte-for-byte diffs against captured JSON. For W1 we just
 * assert the shape so the scaffold is verified.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { createApp } from '../../src/server.js';

describe('W1 endpoints', () => {
  let app: ReturnType<typeof createApp>;

  beforeAll(() => {
    // Default env is fine for these tests — config.ts has defaults for
    // NSWPSN_API_KEY and PORT, and DATABASE_URL is optional in W1.
    app = createApp();
  });

  it('GET /api/health returns ok', async () => {
    const res = await app.request('/api/health');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['status']).toBe('ok');
    expect(['dev', 'production']).toContain(body['mode']);
    expect(Array.isArray(body['cache_keys'])).toBe(true);
    expect(typeof body['active_viewers']).toBe('number');
  });

  it('GET /api/config returns apiKey + version', async () => {
    const res = await app.request('/api/config');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(typeof body['apiKey']).toBe('string');
    expect((body['apiKey'] as string).length).toBeGreaterThan(0);
    expect(typeof body['version']).toBe('string');
  });

  it('unknown /api route is gated by the API-key middleware (401)', async () => {
    // The global requireApiKey middleware runs before route lookup, so
    // unknown /api/* paths return 401 rather than 404 — bug-for-bug
    // compatible with Python's @app.before_request hook (which fires
    // before Flask's URL map resolves the view function).
    const res = await app.request('/api/this-does-not-exist');
    expect(res.status).toBe(401);
  });

  it('unknown non-/api route 404s', async () => {
    // Non-/api paths bypass the API-key gate and hit Hono's default 404.
    const res = await app.request('/not-an-api-path');
    expect(res.status).toBe(404);
  });
});
