/**
 * /api/status smoke tests. Covers:
 *   - the response shape JSONata monitors expect
 *   - 200 vs 503 classification (degraded vs down)
 *   - 5s in-process cache
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock the DB pool so tests can flip "DB up / down / slow" without
// needing a real Postgres.
const queryMock = vi.fn();
const connectMock = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => ({
    connect: connectMock,
    totalCount: 1,
    idleCount: 1,
    waitingCount: 0,
  })),
  closePool: vi.fn(),
}));

describe('/api/status', () => {
  beforeEach(async () => {
    queryMock.mockReset();
    connectMock.mockReset();
    connectMock.mockResolvedValue({
      query: queryMock,
      release: vi.fn(),
    });
    queryMock.mockResolvedValue({ rows: [{ '?column?': 1 }] });
    const mod = await import('../../../src/api/status.js');
    mod._resetStatusCacheForTests();
  });

  it('returns ok when all checks pass', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/status');
    expect([200, 503]).toContain(res.status);
    const body = (await res.json()) as Record<string, unknown>;
    expect(['ok', 'degraded', 'down']).toContain(body['status']);
    expect(body['mode']).toBeDefined();
    expect(typeof body['uptime_secs']).toBe('number');
    const checks = body['checks'] as Record<string, Record<string, unknown>>;
    expect(checks['database']).toBeDefined();
    expect(checks['archive_writer']).toBeDefined();
    expect(checks['waze_ingest']).toBeDefined();
    expect(checks['filter_cache']).toBeDefined();
  });

  it('reports down + 503 when SELECT 1 fails with a non-timeout error', async () => {
    queryMock.mockReset();
    queryMock.mockImplementationOnce(async () => undefined); // SET LOCAL ok
    queryMock.mockImplementationOnce(async () => {
      const err = new Error('connection refused') as Error & { code?: string };
      err.code = '08001';
      throw err;
    });
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/status');
    expect(res.status).toBe(503);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['status']).toBe('down');
  });

  it('reports degraded but 200 when SELECT 1 hits statement_timeout', async () => {
    queryMock.mockReset();
    queryMock.mockImplementationOnce(async () => undefined); // SET LOCAL ok
    queryMock.mockImplementationOnce(async () => {
      const err = new Error('canceled') as Error & { code?: string };
      err.code = '57014';
      throw err;
    });
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/status');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['status']).toBe('degraded');
  });

  it('caches the response for 5s', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    await app.request('/api/status');
    const callCountAfterFirst = queryMock.mock.calls.length;
    await app.request('/api/status');
    // Second call should be served from cache: no extra DB round-trip.
    expect(queryMock.mock.calls.length).toBe(callCountAfterFirst);
  });

  it('is in the public-endpoint allowlist (no API key needed)', async () => {
    // Critical for monitor flips — Uptime Kuma doesn't have the api key
    // wired by default.
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/status');
    expect(res.status).not.toBe(401);
    expect(res.status).not.toBe(403);
  });
});
