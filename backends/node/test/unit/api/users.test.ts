/**
 * Users router tests. Mocks getPool() and the global fetch (Supabase
 * Auth Admin API listing) to keep the suite offline.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
const calls: Call[] = [];
const txCalls: Call[] = [];
let resultQueue: Array<{ rows: unknown[] }> = [];
let getPoolReturn: 'pool' | 'null' = 'pool';

const fakeClient = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    txCalls.push({ sql, ...(params ? { params } : {}) });
    return resultQueue.shift() ?? { rows: [] };
  }),
  release: vi.fn(),
};

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return resultQueue.shift() ?? { rows: [] };
  }),
  connect: vi.fn(async () => fakeClient),
};

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => (getPoolReturn === 'pool' ? fakePool : null)),
}));

// Keep requireRole real (it's the gate under test) but stub the DB-backed
// role checks so the happy-path tests don't need a live user_roles table.
// vi.fn so individual tests can force a 403 via mockResolvedValueOnce.
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return {
    ...actual,
    canManageUsers: vi.fn(async () => true),
    canAssignPrivilegedRoles: vi.fn(async () => true),
    isOwner: vi.fn(async () => true),
  };
});

const { usersRouter } = await import('../../../src/api/users.js');
const roles = await import('../../../src/services/auth/roles.js');
const { _resetRolesCacheForTests } = roles;

// Build the test app. By default it injects a verified Supabase user id
// (as the global optionalSupabaseJwt would after a valid JWT) so requireRole
// can pass; pass {authed:false} to exercise the unauthenticated 401 path.
function makeApp(opts: { authed?: boolean } = {}) {
  const app = new Hono();
  if (opts.authed !== false) {
    app.use('*', async (c, next) => {
      c.set('userId', 'owner-1');
      await next();
    });
  }
  app.route('/', usersRouter);
  return app;
}

beforeEach(() => {
  calls.length = 0;
  txCalls.length = 0;
  resultQueue = [];
  getPoolReturn = 'pool';
  fakePool.query.mockClear();
  fakeClient.query.mockClear();
  fakeClient.release.mockClear();
  _resetRolesCacheForTests();
  vi.unstubAllGlobals();
});

describe('GET /api/users', () => {
  it('returns 503 when SUPABASE config missing', async () => {
    // SUPABASE_URL/KEY are unset by default in vitest env; we just verify
    // the handler short-circuits.
    const app = makeApp();
    const res = await app.request('/api/users');
    expect(res.status).toBe(503);
    expect(await res.json()).toEqual({ error: 'Supabase not configured' });
  });
});

describe('PUT /api/users/:userId/roles', () => {
  it('replaces all roles atomically (BEGIN/DELETE/INSERTs/COMMIT)', async () => {
    // 4 results consumed: BEGIN, DELETE, INSERT x2, COMMIT — only the
    // INSERTs return rows in production but the queue serves all.
    resultQueue = [{ rows: [] }, { rows: [] }, { rows: [] }, { rows: [] }, { rows: [] }];
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ roles: ['map_editor', 'pager_contributor'] }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['user_id']).toBe('abc');
    expect(body['roles']).toEqual(['map_editor', 'pager_contributor']);

    // Verify the transaction shape.
    expect(txCalls[0]?.sql).toBe('BEGIN');
    expect(txCalls[1]?.sql).toContain('DELETE FROM user_roles WHERE user_id = $1');
    expect(txCalls[2]?.sql).toContain('INSERT INTO user_roles');
    expect(txCalls[3]?.sql).toContain('INSERT INTO user_roles');
    expect(txCalls[4]?.sql).toBe('COMMIT');
    expect(fakeClient.release).toHaveBeenCalledOnce();
  });

  it('rolls back on error', async () => {
    fakeClient.query.mockImplementationOnce(async (sql: string) => {
      txCalls.push({ sql });
      return { rows: [] };
    }); // BEGIN ok
    fakeClient.query.mockImplementationOnce(async () => {
      throw new Error('boom');
    });
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ roles: ['map_editor'] }),
    });
    expect(res.status).toBe(500);
    expect(fakeClient.release).toHaveBeenCalledOnce();
  });
});

describe('POST /api/users/:userId/roles', () => {
  it('400 when role missing', async () => {
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: 'Role is required' });
  });

  it('inserts role with ON CONFLICT DO NOTHING', async () => {
    resultQueue = [{ rows: [] }];
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ role: 'map_editor' }),
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ success: true, user_id: 'abc', added_role: 'map_editor' });
    expect(calls[0]?.sql).toContain('INSERT INTO user_roles');
    expect(calls[0]?.sql).toContain('ON CONFLICT (user_id, role) DO NOTHING');
    expect(calls[0]?.params).toEqual(['abc', 'map_editor']);
  });
});

describe('DELETE /api/users/:userId/roles/:role', () => {
  it('issues a single-row delete', async () => {
    resultQueue = [{ rows: [] }];
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles/map_editor', {
      method: 'DELETE',
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ success: true, user_id: 'abc', removed_role: 'map_editor' });
    expect(calls[0]?.sql).toContain('DELETE FROM user_roles WHERE user_id = $1 AND role = $2');
    expect(calls[0]?.params).toEqual(['abc', 'map_editor']);
  });
});

describe('503 when DB unavailable', () => {
  it('PUT returns 503', async () => {
    getPoolReturn = 'null';
    const app = makeApp();
    const res = await app.request('/api/users/x/roles', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ roles: [] }),
    });
    expect(res.status).toBe(503);
  });
});

describe('auth gate', () => {
  it('401 when no authenticated user (public api key alone is not enough)', async () => {
    const app = makeApp({ authed: false });
    const res = await app.request('/api/users/abc/roles', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ role: 'map_editor' }),
    });
    expect(res.status).toBe(401);
  });

  it('403 when authenticated but not an owner', async () => {
    vi.mocked(roles.canAssignPrivilegedRoles).mockResolvedValueOnce(false);
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ role: 'map_editor' }),
    });
    expect(res.status).toBe(403);
  });

  it('GET /api/users 403 when authenticated but lacks canManageUsers', async () => {
    vi.mocked(roles.canManageUsers).mockResolvedValueOnce(false);
    const app = makeApp();
    const res = await app.request('/api/users');
    expect(res.status).toBe(403);
  });
});
