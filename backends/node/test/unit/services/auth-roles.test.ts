/**
 * Tests for the role helpers (services/auth/roles.ts).
 *
 * Mocks getPool() so we can drive `getUserRoles` and the higher-level
 * checks against a fake pg query without spinning up a real DB.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

let resultQueue: Array<{ rows: unknown[] }> = [];
let queryCallCount = 0;
let getPoolReturn: 'pool' | 'null' = 'pool';

const fakePool = {
  query: vi.fn(async () => {
    queryCallCount += 1;
    return resultQueue.shift() ?? { rows: [] };
  }),
};

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => (getPoolReturn === 'pool' ? fakePool : null)),
}));

const {
  getUserRoles,
  hasRole,
  isOwner,
  canManageUsers,
  canAssignPrivilegedRoles,
  isPrivilegedRole,
  requireRole,
  invalidateUserRolesCache,
  _resetRolesCacheForTests,
} = await import('../../../src/services/auth/roles.js');

beforeEach(() => {
  resultQueue = [];
  queryCallCount = 0;
  getPoolReturn = 'pool';
  fakePool.query.mockClear();
  _resetRolesCacheForTests();
});

describe('getUserRoles', () => {
  it('returns role strings from the user_roles table', async () => {
    resultQueue = [{ rows: [{ role: 'owner' }, { role: 'map_editor' }] }];
    const roles = await getUserRoles('abc');
    expect(roles).toEqual(['owner', 'map_editor']);
  });

  it('returns [] when DB is unavailable', async () => {
    getPoolReturn = 'null';
    const roles = await getUserRoles('abc');
    expect(roles).toEqual([]);
  });

  it('returns [] for empty userId without hitting the DB', async () => {
    const roles = await getUserRoles('');
    expect(roles).toEqual([]);
    expect(queryCallCount).toBe(0);
  });

  it('caches results within the TTL', async () => {
    resultQueue = [{ rows: [{ role: 'dev' }] }];
    await getUserRoles('u1');
    await getUserRoles('u1');
    expect(queryCallCount).toBe(1);
  });

  it('invalidate clears the cache', async () => {
    resultQueue = [{ rows: [{ role: 'dev' }] }, { rows: [{ role: 'owner' }] }];
    await getUserRoles('u1');
    invalidateUserRolesCache('u1');
    const roles = await getUserRoles('u1');
    expect(roles).toEqual(['owner']);
    expect(queryCallCount).toBe(2);
  });
});

describe('hasRole / isOwner / canManageUsers / canAssignPrivilegedRoles', () => {
  it('isOwner is true only when "owner" is present', async () => {
    resultQueue = [{ rows: [{ role: 'owner' }] }];
    expect(await isOwner('a')).toBe(true);
    _resetRolesCacheForTests();
    resultQueue = [{ rows: [{ role: 'team_member' }] }];
    expect(await isOwner('b')).toBe(false);
  });

  it('canManageUsers includes team_member', async () => {
    resultQueue = [{ rows: [{ role: 'team_member' }] }];
    expect(await canManageUsers('tm')).toBe(true);
    _resetRolesCacheForTests();
    resultQueue = [{ rows: [{ role: 'map_editor' }] }];
    expect(await canManageUsers('me')).toBe(false);
  });

  it('canAssignPrivilegedRoles is owner-only (team_member can NOT)', async () => {
    resultQueue = [{ rows: [{ role: 'team_member' }] }];
    expect(await canAssignPrivilegedRoles('tm')).toBe(false);
    _resetRolesCacheForTests();
    resultQueue = [{ rows: [{ role: 'owner' }] }];
    expect(await canAssignPrivilegedRoles('o')).toBe(true);
  });

  it('hasRole accepts a list and returns true on any match', async () => {
    resultQueue = [{ rows: [{ role: 'dev' }] }];
    expect(await hasRole('u', ['owner', 'dev'])).toBe(true);
    _resetRolesCacheForTests();
    resultQueue = [{ rows: [{ role: 'map_editor' }] }];
    expect(await hasRole('u', ['owner', 'team_member'])).toBe(false);
  });
});

describe('requireRole middleware', () => {
  // Build a tiny app: an optional pre-middleware sets userId (as the real
  // optionalSupabaseJwt would), then the role-gated route.
  function app(opts: { userId?: string } = {}) {
    const a = new Hono();
    if (opts.userId) {
      a.use('*', async (c, next) => {
        c.set('userId', opts.userId!);
        await next();
      });
    }
    a.get('/x', requireRole(isOwner), (c) => c.json({ ok: true }));
    return a;
  }

  it('401 when no verified user is present', async () => {
    const res = await app().request('/x');
    expect(res.status).toBe(401);
  });

  it('403 when the user lacks the role', async () => {
    resultQueue = [{ rows: [{ role: 'map_editor' }] }];
    const res = await app({ userId: 'u' }).request('/x');
    expect(res.status).toBe(403);
  });

  it('passes through when the role check succeeds', async () => {
    resultQueue = [{ rows: [{ role: 'owner' }] }];
    const res = await app({ userId: 'u' }).request('/x');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
  });

  it('403 (fails closed) when the check throws', async () => {
    const res = await app({ userId: 'u' })
      .get('/y', requireRole(async () => { throw new Error('db down'); }), (c) =>
        c.json({ ok: true }),
      )
      .request('/y');
    expect(res.status).toBe(403);
  });
});

describe('isPrivilegedRole', () => {
  it('flags owner / team_member / dev as privileged', () => {
    expect(isPrivilegedRole('owner')).toBe(true);
    expect(isPrivilegedRole('team_member')).toBe(true);
    expect(isPrivilegedRole('dev')).toBe(true);
  });
  it('does NOT flag feature roles as privileged', () => {
    expect(isPrivilegedRole('map_editor')).toBe(false);
    expect(isPrivilegedRole('pager_contributor')).toBe(false);
    expect(isPrivilegedRole('radio_contributor')).toBe(false);
    expect(isPrivilegedRole('unknown')).toBe(false);
  });
});
