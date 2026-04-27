/**
 * System / debug / admin endpoint smoke tests.
 *
 * Covers the public surfaces of /api/cache/clear, /api/debug/* and
 * /api/admin/db/* without firing any real upstream HTTP. The
 * traffic-raw + admin DB routes mock undici / pg respectively.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchJsonMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', async () => {
  const actual = await vi.importActual<typeof import('../../../src/sources/shared/http.js')>(
    '../../../src/sources/shared/http.js',
  );
  return {
    ...actual,
    fetchJson: (...args: unknown[]) => fetchJsonMock(...args),
  };
});

const queryMock = vi.fn();
const connectMock = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => ({
    connect: connectMock,
    query: queryMock,
    totalCount: 1,
    idleCount: 1,
    waitingCount: 0,
  })),
  closePool: vi.fn(),
}));

const API_KEY = 'test-api-key';
const AUTH = { 'X-API-Key': API_KEY } as const;

describe('/api/cache/clear', () => {
  it('returns ok and clears response-side caches', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/cache/clear', { headers: AUTH });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['status']).toBe('ok');
  });
});

describe('/api/debug/sessions', () => {
  it('returns the activity-mode shape', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/debug/sessions', { headers: AUTH });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['active_count']).toBeDefined();
    expect(body['data_page_count']).toBeDefined();
    expect(Array.isArray(body['sessions'])).toBe(true);
  });
});

describe('/api/debug/heartbeat-test', () => {
  it('echoes received parameters', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request(
      '/api/debug/heartbeat-test?action=ping&page_id=abc-123&data_page=true',
      { headers: AUTH },
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { received_params: Record<string, unknown> };
    expect(body.received_params['action']).toBe('ping');
    // page_id_short is the last 6 chars of `abc-123` → `bc-123`.
    expect(body.received_params['page_id_short']).toBe('bc-123');
    expect(body.received_params['is_data_page_parsed']).toBe(true);
  });
});

describe('/api/debug/ratelimit', () => {
  it('returns informational stub', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/debug/ratelimit', { headers: AUTH });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['is_limited']).toBe(false);
    expect(body['note']).toContain('Apache');
  });
});

describe('/api/debug/traffic-raw', () => {
  beforeEach(() => {
    fetchJsonMock.mockReset();
  });

  it('parses upstream features and returns sample', async () => {
    fetchJsonMock.mockResolvedValueOnce({
      features: [
        {
          properties: {
            mainCategory: 'Crash',
            subCategory: 'Major',
            headline: 'Crash on M1',
            displayName: 'M1 Pacific',
            incidentKind: 'crash',
            type: 'incident',
          },
        },
      ],
    });
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/debug/traffic-raw', { headers: AUTH });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { sample: unknown[]; total: number };
    expect(body.total).toBe(1);
    expect(body.sample.length).toBe(1);
  });

  it('degrades gracefully on upstream failure', async () => {
    fetchJsonMock.mockRejectedValueOnce(new Error('502'));
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/debug/traffic-raw', { headers: AUTH });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['total']).toBe(0);
    expect(body['error']).toContain('502');
  });
});

describe('/api/debug/test-all', () => {
  it('reports keys-present and per-key liveness', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/debug/test-all', { headers: AUTH });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['backend']).toBe('node');
    expect(typeof body['keys_present']).toBe('number');
    expect(body['report']).toBeDefined();
  });
});

describe('/api/admin/db/stats', () => {
  beforeEach(() => {
    queryMock.mockReset();
    queryMock.mockResolvedValue({ rows: [{ rows: 100, bytes: 1024 * 1024 }] });
  });

  it('reports per-table row counts and sizes', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/admin/db/stats', { headers: AUTH });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, Record<string, unknown>>;
    expect(body['archive_waze']).toBeDefined();
    expect(body['archive_traffic']).toBeDefined();
    expect(body['incidents']).toBeDefined();
  });
});

describe('/api/admin/db/cleanup-duplicates', () => {
  it('returns no-op note (append-only schema)', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/admin/db/cleanup-duplicates', {
      method: 'POST',
      headers: AUTH,
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['status']).toBe('no-op');
  });
});
