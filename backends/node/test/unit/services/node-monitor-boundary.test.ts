/**
 * SECURITY boundary test for the view-only `node_monitor` role.
 *
 * NOTE (migration-059 role refactor): this suite deliberately still drives the
 * LEGACY role name. `node_monitor` was renamed to `feeder:monitor`, and these
 * tests passing unchanged is the end-to-end proof that the ROLE_ALIASES shim in
 * services/auth/roles.ts resolves old names through a real router — i.e. an
 * un-migrated row or stale cache can't silently lock a monitor out.
 *
 * node_monitor grants READ access to the staff Data + Nodes pages and NOTHING
 * else. This test pins the read/write split:
 *   - permission fns:  canViewNodeData(node_monitor) === true
 *                      canManageNodes(node_monitor)  === false
 *   - integration:     against the REAL requireRole + the REAL nodesRouter, a
 *                      node_monitor user gets a node-data READ (GET /api/nodes)
 *                      through the gate (NOT 401/403) while the WRITES
 *                      PUT /api/nodes/global-config and POST /api/nodes/:id/cmd
 *                      are rejected 403.
 *
 * getPool() is mocked so the user_roles lookup resolves to node_monitor; any
 * other query returns empty rows (enough for the read handler to 200).
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

let getPoolReturn: 'pool' | 'null' = 'pool';

const fakePool = {
  query: vi.fn(async (sql: string) => {
    // The role lookup drives every requireRole decision under test.
    if (/from\s+user_roles/i.test(sql)) {
      return { rows: [{ role: 'node_monitor' }] };
    }
    // Everything else (listNodes, getUsernameMap, …) — empty is fine; we only
    // care that the READ handler runs past the gate.
    return { rows: [] };
  }),
};

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => (getPoolReturn === 'pool' ? fakePool : null)),
}));

const roles = await import('../../../src/services/auth/roles.js');
const { canViewNodeData, canManageNodes, _resetRolesCacheForTests } = roles;
const { nodesRouter } = await import('../../../src/api/nodes.js');

// Mount the REAL router behind a middleware that injects a verified node_monitor
// user id, exactly as optionalSupabaseJwt would upstream.
function app() {
  const a = new Hono();
  a.use('*', async (c, next) => {
    c.set('userId', 'nm-user');
    await next();
  });
  a.route('/', nodesRouter);
  return a;
}

beforeEach(() => {
  getPoolReturn = 'pool';
  fakePool.query.mockClear();
  _resetRolesCacheForTests();
});

describe('node_monitor permission functions', () => {
  it('canViewNodeData(node_monitor) === true (reads allowed)', async () => {
    expect(await canViewNodeData('nm-user')).toBe(true);
  });

  it('canManageNodes(node_monitor) === false (writes denied)', async () => {
    expect(await canManageNodes('nm-user')).toBe(false);
  });
});

describe('node_monitor read/write boundary (real requireRole + nodesRouter)', () => {
  it('ALLOWS a read: GET /api/nodes is not gated out (not 401/403)', async () => {
    const res = await app().request('/api/nodes');
    expect(res.status).not.toBe(401);
    expect(res.status).not.toBe(403);
    // Handler runs past the gate and returns the node list.
    expect(res.status).toBe(200);
  });

  it('FORBIDS a write: PUT /api/nodes/global-config → 403', async () => {
    const res = await app().request('/api/nodes/global-config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ agencies: [] }),
    });
    expect(res.status).toBe(403);
  });

  it('FORBIDS a write: POST /api/nodes/:id/cmd → 403', async () => {
    const res = await app().request('/api/nodes/some-node/cmd', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'restart' }),
    });
    expect(res.status).toBe(403);
  });

  it('FORBIDS the destructive write: DELETE /api/nodes/:id → 403 (owner-only)', async () => {
    const res = await app().request('/api/nodes/some-node', { method: 'DELETE' });
    expect(res.status).toBe(403);
  });
});
