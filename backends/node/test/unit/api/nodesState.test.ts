/**
 * Per-state pager node routes on the staff nodes router:
 *   - POST /api/nodes            — per-kind location rule (radio: zone; pager: state+lga)
 *   - PUT  /api/nodes/:id/pager-primary — label must belong to the node's STATE's plan
 *   - PUT  /api/nodes/:id/state  — move a pager node between states (re-pushes config)
 *
 * Registry + config push are mocked; requireRole stays real with canManageNodes
 * stubbed true (the gate itself is covered by auth-roles tests).
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';
import type { NodeRow } from '../../../src/services/nodes/registry.js';

function nodeRow(overrides: Partial<NodeRow> = {}): NodeRow {
  return {
    id: 'node-1',
    kind: 'pager',
    user_id: 'user-1',
    install_id: null,
    name: 'pager-user-abcd1234',
    enabled: true,
    feed_enabled: true,
    config_override: {},
    config_version: null,
    agent_version: null,
    sdrtrunk_version: null,
    rdio_version: null,
    os: null,
    arch: null,
    last_seen_at: null,
    notes: null,
    created_at: '2026-01-01T00:00:00Z',
    token_prefix: null,
    lat: null,
    lon: null,
    zone: null,
    state: 'NSW',
    lga: 'Newcastle',
    ...overrides,
  } as NodeRow;
}

vi.mock('../../../src/services/nodes/registry.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodes/registry.js')>();
  return {
    ...actual,
    getNode: vi.fn(async () => nodeRow()),
    setNodeLocation: vi.fn(async () => nodeRow()),
    setPagerPrimary: vi.fn(async () => nodeRow()),
    createNode: vi.fn(async () => nodeRow()),
    countNodesForUser: vi.fn(async () => 0),
  };
});

vi.mock('../../../src/services/nodes/configPush.js', () => ({
  pushConfigToNode: vi.fn(async () => ({ pushed: true })),
  pushConfigToAllNodes: vi.fn(async () => []),
}));

vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canManageNodes: vi.fn(async () => true) };
});

vi.mock('../../../src/api/users.js', () => ({
  getUsername: vi.fn(async () => 'someuser'),
  getUsernameMap: vi.fn(async () => new Map()),
}));

const { nodesRouter } = await import('../../../src/api/nodes.js');
const registry = await import('../../../src/services/nodes/registry.js');
const configPush = await import('../../../src/services/nodes/configPush.js');

function makeApp() {
  const app = new Hono();
  app.use('*', async (c, next) => {
    c.set('userId', 'staff-1');
    await next();
  });
  app.route('/', nodesRouter);
  return app;
}

function put(path: string, body: unknown) {
  return makeApp().request(path, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

function post(path: string, body: unknown) {
  return makeApp().request(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

beforeEach(() => {
  vi.mocked(registry.getNode).mockClear().mockResolvedValue(nodeRow());
  vi.mocked(registry.setNodeLocation).mockClear();
  vi.mocked(registry.setPagerPrimary).mockClear();
  vi.mocked(registry.createNode).mockClear();
  vi.mocked(configPush.pushConfigToNode).mockClear();
});

describe('PUT /api/nodes/:id/pager-primary (per-state labels)', () => {
  it('accepts a label from the node state plan', async () => {
    const res = await put('/api/nodes/node-1/pager-primary', { primary: 'FRNSW' });
    expect(res.status).toBe(200);
    expect(registry.setPagerPrimary).toHaveBeenCalledWith('node-1', 'FRNSW');
  });

  it('rejects a label outside the node state plan (FRNSW on a QLD node)', async () => {
    vi.mocked(registry.getNode).mockResolvedValue(nodeRow({ state: 'QLD' }));
    const res = await put('/api/nodes/node-1/pager-primary', { primary: 'FRNSW' });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toContain('QLD');
    expect(registry.setPagerPrimary).not.toHaveBeenCalled();
  });

  it('accepts QFES on a QLD node', async () => {
    vi.mocked(registry.getNode).mockResolvedValue(nodeRow({ state: 'QLD' }));
    const res = await put('/api/nodes/node-1/pager-primary', { primary: 'QFES' });
    expect(res.status).toBe(200);
  });
});

describe('PUT /api/nodes/:id/state', () => {
  it('moves a pager node to another state and re-pushes its config', async () => {
    const res = await put('/api/nodes/node-1/state', { state: 'QLD', lga: 'Brisbane' });
    expect(res.status).toBe(200);
    expect(registry.setNodeLocation).toHaveBeenCalledWith('node-1', { state: 'QLD', lga: 'Brisbane' });
    expect(configPush.pushConfigToNode).toHaveBeenCalledWith('node-1');
  });

  it('rejects a non-pager node', async () => {
    vi.mocked(registry.getNode).mockResolvedValue(nodeRow({ kind: 'radio' }));
    const res = await put('/api/nodes/node-1/state', { state: 'QLD', lga: 'Brisbane' });
    expect(res.status).toBe(400);
  });

  it('rejects an unknown state and a missing lga', async () => {
    expect((await put('/api/nodes/node-1/state', { state: 'ZZZ', lga: 'Brisbane' })).status).toBe(400);
    expect((await put('/api/nodes/node-1/state', { state: 'QLD' })).status).toBe(400);
  });
});

describe('POST /api/nodes (per-kind location rule)', () => {
  it('pager create requires state + lga', async () => {
    const missing = await post('/api/nodes', { userId: 'u1', kind: 'pager' });
    expect(missing.status).toBe(400);
    const ok = await post('/api/nodes', { userId: 'u1', kind: 'pager', state: 'QLD', lga: 'Cairns' });
    expect(ok.status).toBe(200);
    const loc = vi.mocked(registry.createNode).mock.calls[0]?.[5];
    expect(loc).toEqual({ zone: null, state: 'QLD', lga: 'Cairns' });
  });

  it('radio create still requires the RFS zone and stays NSW', async () => {
    const missing = await post('/api/nodes', { userId: 'u1', kind: 'radio' });
    expect(missing.status).toBe(400);
  });
});
