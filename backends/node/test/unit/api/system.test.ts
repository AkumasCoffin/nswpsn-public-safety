/**
 * System / debug / admin endpoint smoke tests.
 *
 * Covers the public surfaces of /api/cache/clear, /api/debug/* and
 * /api/admin/db/* without firing any real upstream HTTP. The
 * traffic-raw + admin DB routes mock undici / pg respectively.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { SignJWT } from 'jose';

// The /api/admin/db/* routes are owner-gated. Stub the DB-backed isOwner
// check so the smoke tests don't need a real user_roles table; the JWT
// (signed below with the test secret) is still verified by the real
// optionalSupabaseJwt so a valid `userId` is set.
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, isOwner: vi.fn(async () => true) };
});

// Owner Authorization header — a real HS256 JWT signed with the test
// SUPABASE_JWT_SECRET (vitest.config) so optionalSupabaseJwt verifies it.
async function ownerHeaders(): Promise<Record<string, string>> {
  const jwt = await new SignJWT({})
    .setProtectedHeader({ alg: 'HS256' })
    .setSubject('owner-1')
    .setAudience('authenticated') // real Supabase access tokens carry this aud
    .sign(new TextEncoder().encode('test-jwt-secret-0123456789'));
  return { Authorization: `Bearer ${jwt}` };
}

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
    const res = await app.request('/api/cache/clear', { headers: await ownerHeaders() });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['status']).toBe('ok');
  });
});

// The site key is handed to every visitor via /api/config and a page token is
// mintable by any script that sends a first-party Origin, so the credential
// gate these routes sit behind is not a role check. Owner-only, like
// /api/admin/db/*.
describe('operator routes are owner-only', () => {
  it.each([
    '/api/cache/clear',
    '/api/debug/sessions',
    '/api/debug/test-all',
  ])('refuses %s to a key-only caller', async (path) => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request(path, { headers: AUTH });
    expect(res.status).toBe(401);
  });
});

describe('/api/debug/sessions', () => {
  it('returns the activity-mode shape, without any client IP', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/debug/sessions', { headers: await ownerHeaders() });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['active_count']).toBeDefined();
    expect(body['data_page_count']).toBeDefined();
    expect(Array.isArray(body['sessions'])).toBe(true);
    // Visitor addresses are not what "is collection active?" needs.
    expect(JSON.stringify(body)).not.toContain('"ip"');
  });
});

describe('/api/debug/heartbeat-test', () => {
  it('echoes received parameters', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request(
      '/api/debug/heartbeat-test?action=ping&page_id=abc-123&data_page=true',
      { headers: await ownerHeaders() },
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
    const res = await app.request('/api/debug/ratelimit', { headers: await ownerHeaders() });
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
    const res = await app.request('/api/debug/traffic-raw', { headers: await ownerHeaders() });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { sample: unknown[]; total: number };
    expect(body.total).toBe(1);
    expect(body.sample.length).toBe(1);
  });

  it('degrades gracefully on upstream failure', async () => {
    fetchJsonMock.mockRejectedValueOnce(new Error('502'));
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/debug/traffic-raw', { headers: await ownerHeaders() });
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
    const res = await app.request('/api/debug/test-all', { headers: await ownerHeaders() });
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
    queryMock.mockImplementation((sql: string) => {
      // Wrapped in BEGIN/SET LOCAL/COMMIT now that we wrap the COUNT(*)
      // queries in a transaction so SET LOCAL statement_timeout applies.
      if (
        sql === 'BEGIN' ||
        sql === 'COMMIT' ||
        sql === 'ROLLBACK' ||
        sql.includes('statement_timeout')
      ) {
        return { rows: [] };
      }
      return { rows: [{ rows: 100, bytes: 1024 * 1024 }] };
    });
    connectMock.mockReset();
    connectMock.mockResolvedValue({
      query: queryMock,
      release: vi.fn(),
    });
  });

  it('reports per-table row counts and sizes', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/admin/db/stats', { headers: await ownerHeaders() });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, Record<string, unknown>>;
    expect(body['archive_traffic']).toBeDefined();
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
      headers: await ownerHeaders(),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['status']).toBe('no-op');
  });
});
