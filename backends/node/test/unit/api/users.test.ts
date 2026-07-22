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

const { usersRouter, discordInfo } = await import('../../../src/api/users.js');
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

describe('discordInfo', () => {
  it('reports an OAuth-linked Discord identity with its provider id', () => {
    expect(
      discordInfo({
        identities: [
          { provider: 'email', identity_data: {} },
          { provider: 'discord', identity_data: { provider_id: '123456789', sub: '123456789' } },
        ],
      }),
    ).toEqual({ discord_linked: true, discord_id: '123456789' });
  });

  it('falls back to sub, then to the metadata discord_id (unlinked)', () => {
    expect(
      discordInfo({ identities: [{ provider: 'discord', identity_data: { sub: '42' } }] }),
    ).toEqual({ discord_linked: true, discord_id: '42' });
    expect(
      discordInfo({ identities: [], user_metadata: { discord_id: ' 987 ' } }),
    ).toEqual({ discord_linked: false, discord_id: '987' });
  });

  it('returns null id when nothing is recorded', () => {
    expect(discordInfo({})).toEqual({ discord_linked: false, discord_id: null });
  });
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

  it('403 when authenticated but lacks canManageUsers', async () => {
    vi.mocked(roles.canManageUsers).mockResolvedValueOnce(false);
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

// Team members (canManageUsers=true, canAssignPrivilegedRoles=false) may
// only touch the feature roles; the privileged trio is owner-only.
describe('team member role tiering', () => {
  it('POST: team member can add a feature role', async () => {
    vi.mocked(roles.canAssignPrivilegedRoles).mockResolvedValue(false);
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ role: 'pager_contributor' }),
    });
    expect(res.status).toBe(200);
    vi.mocked(roles.canAssignPrivilegedRoles).mockResolvedValue(true);
  });

  it('POST: team member cannot add a privileged role', async () => {
    vi.mocked(roles.canAssignPrivilegedRoles).mockResolvedValueOnce(false);
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ role: 'dev' }),
    });
    expect(res.status).toBe(403);
    expect(((await res.json()) as { error: string }).error).toContain('Only owners');
    expect(calls.some((q) => q.sql.includes('INSERT INTO user_roles'))).toBe(false);
  });

  it('DELETE: team member can remove a feature role but not a privileged one', async () => {
    vi.mocked(roles.canAssignPrivilegedRoles).mockResolvedValue(false);
    const app = makeApp();
    const ok = await app.request('/api/users/abc/roles/radio_contributor', { method: 'DELETE' });
    expect(ok.status).toBe(200);
    const denied = await app.request('/api/users/abc/roles/owner', { method: 'DELETE' });
    expect(denied.status).toBe(403);
    expect(calls.filter((q) => q.sql.includes('DELETE FROM user_roles'))).toHaveLength(1);
    vi.mocked(roles.canAssignPrivilegedRoles).mockResolvedValue(true);
  });

  it('PUT: team member replace succeeds when the privileged set is unchanged', async () => {
    vi.mocked(roles.canAssignPrivilegedRoles).mockResolvedValueOnce(false);
    // First pool.query = current-roles SELECT; target already holds team_member.
    resultQueue = [{ rows: [{ role: 'team_member' }, { role: 'map_editor' }] }];
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ roles: ['team_member', 'pager_contributor'] }),
    });
    expect(res.status).toBe(200);
    expect(txCalls[0]?.sql).toBe('BEGIN');
    expect(txCalls.at(-1)?.sql).toBe('COMMIT');
  });

  it('PUT: team member replace 403s when it would drop a privileged role', async () => {
    vi.mocked(roles.canAssignPrivilegedRoles).mockResolvedValueOnce(false);
    resultQueue = [{ rows: [{ role: 'dev' }, { role: 'map_editor' }] }];
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ roles: ['map_editor'] }),
    });
    expect(res.status).toBe(403);
    expect(txCalls).toHaveLength(0);
  });

  it('PUT: team member replace 403s when it would add a privileged role', async () => {
    vi.mocked(roles.canAssignPrivilegedRoles).mockResolvedValueOnce(false);
    resultQueue = [{ rows: [{ role: 'map_editor' }] }];
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ roles: ['map_editor', 'owner'] }),
    });
    expect(res.status).toBe(403);
    expect(txCalls).toHaveLength(0);
  });

  it('PUT: owner replace skips the current-roles lookup entirely', async () => {
    const app = makeApp();
    const res = await app.request('/api/users/abc/roles', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ roles: ['owner', 'dev'] }),
    });
    expect(res.status).toBe(200);
    expect(calls.some((q) => q.sql.includes('SELECT role FROM user_roles'))).toBe(false);
  });
});
