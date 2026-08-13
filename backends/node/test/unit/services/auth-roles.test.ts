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
  canonicalRole,
  canonicalRoles,
  isKnownRole,
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
    resultQueue = [{ rows: [{ role: 'owner' }, { role: 'map:editor' }] }];
    const roles = await getUserRoles('abc');
    // Alias expansion also surfaces the legacy name so old checks still pass.
    expect(roles).toEqual(['owner', 'map:editor', 'map_editor']);
  });

  it('expands legacy names to current ones (migration-059 cutover)', async () => {
    resultQueue = [{ rows: [{ role: 'team_member' }] }];
    const roles = await getUserRoles('legacy');
    expect(roles).toContain('team_member'); // as stored
    expect(roles).toContain('staff');       // current equivalent resolves too
  });

  it('canonicalRoles() drops legacy names for display', () => {
    expect(canonicalRoles(['owner', 'map:editor', 'map_editor'])).toEqual(['owner', 'map:editor']);
    expect(canonicalRoles(['team_member', 'staff'])).toEqual(['staff']);
  });

  it('canonicalRole() maps legacy → current, leaves current alone', () => {
    expect(canonicalRole('media_feeder')).toBe('wire:contributor');
    expect(canonicalRole('team_member')).toBe('staff');
    expect(canonicalRole('wire:manager')).toBe('wire:manager');
    expect(canonicalRole('owner')).toBe('owner');
  });

  it('isKnownRole() accepts current + legacy names, rejects junk', () => {
    expect(isKnownRole('wire:contributor')).toBe(true);
    expect(isKnownRole('media_feeder')).toBe(true); // legacy alias
    expect(isKnownRole('feeder:manager')).toBe(true);
    expect(isKnownRole('authed')).toBe(true);
    expect(isKnownRole('dev')).toBe(false);         // role removed
    expect(isKnownRole('superuser')).toBe(false);   // junk
    expect(isKnownRole('')).toBe(false);
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
    resultQueue = [{ rows: [{ role: 'staff' }] }];
    await getUserRoles('u1');
    await getUserRoles('u1');
    expect(queryCallCount).toBe(1);
  });

  it('invalidate clears the cache', async () => {
    resultQueue = [{ rows: [{ role: 'staff' }] }, { rows: [{ role: 'owner' }] }];
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
    resultQueue = [{ rows: [{ role: 'staff' }] }];
    expect(await isOwner('b')).toBe(false);
  });

  it('canManageUsers includes staff (and the legacy team_member)', async () => {
    resultQueue = [{ rows: [{ role: 'staff' }] }];
    expect(await canManageUsers('st')).toBe(true);
    _resetRolesCacheForTests();
    resultQueue = [{ rows: [{ role: 'team_member' }] }];
    expect(await canManageUsers('tm')).toBe(true);
    _resetRolesCacheForTests();
    resultQueue = [{ rows: [{ role: 'map:editor' }] }];
    expect(await canManageUsers('me')).toBe(false);
  });

  it('canAssignPrivilegedRoles is owner-only (staff can NOT)', async () => {
    resultQueue = [{ rows: [{ role: 'staff' }] }];
    expect(await canAssignPrivilegedRoles('tm')).toBe(false);
    _resetRolesCacheForTests();
    resultQueue = [{ rows: [{ role: 'owner' }] }];
    expect(await canAssignPrivilegedRoles('o')).toBe(true);
  });

  it('hasRole accepts a list and returns true on any match', async () => {
    resultQueue = [{ rows: [{ role: 'staff' }] }];
    expect(await hasRole('u', ['owner', 'staff'])).toBe(true);
    _resetRolesCacheForTests();
    resultQueue = [{ rows: [{ role: 'map:editor' }] }];
    expect(await hasRole('u', ['owner', 'staff'])).toBe(false);
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
    resultQueue = [{ rows: [{ role: 'map:editor' }] }];
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
  it('flags owner / staff (and legacy team_member) as privileged', () => {
    expect(isPrivilegedRole('owner')).toBe(true);
    expect(isPrivilegedRole('staff')).toBe(true);
    expect(isPrivilegedRole('team_member')).toBe(true); // legacy name
  });
  it('does NOT flag feature or manager roles as privileged', () => {
    expect(isPrivilegedRole('map:editor')).toBe(false);
    expect(isPrivilegedRole('feeder:pager')).toBe(false);
    expect(isPrivilegedRole('feeder:radio')).toBe(false);
    // Managers run their own area but can't grant roles, so they're not
    // privileged — any staff may assign them.
    expect(isPrivilegedRole('wire:manager')).toBe(false);
    expect(isPrivilegedRole('feeder:manager')).toBe(false);
    expect(isPrivilegedRole('map:manager')).toBe(false);
    expect(isPrivilegedRole('unknown')).toBe(false);
  });
});
