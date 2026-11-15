/**
 * A radio node's listener page on its owner's network: the owner's feeder view
 * carries the link, the staff node view never does (staff are not on that
 * network). Staff keep the node's local IP, as before.
 *
 * The hub is the real one with a stand-in agent socket, so the link takes the
 * same path from the status frame to each view as it does in production.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';
import type { WebSocket } from 'ws';
import type { NodeRow } from '../../../src/services/nodes/registry.js';

const NODE: Partial<NodeRow> = {
  id: 'node-radio-1',
  kind: 'radio',
  user_id: 'user-owner',
  name: 'radio-01',
  enabled: true,
  feed_enabled: true,
  config_override: {},
  state: 'NSW',
  lga: 'Newcastle',
  local_ip: '192.168.1.50',
  created_at: '2026-01-01T00:00:00Z',
};

vi.mock('../../../src/services/nodes/registry.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodes/registry.js')>();
  return {
    ...actual,
    getNode: vi.fn(async () => NODE),
    listNodesForUser: vi.fn(async () => [NODE]),
  };
});

vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return {
    ...actual,
    hasRole: vi.fn(async () => true),
    canViewNodeData: vi.fn(async () => true),
    canManageNodes: vi.fn(async () => true),
  };
});

vi.mock('../../../src/services/auth/supabaseJwt.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/supabaseJwt.js')>();
  return {
    ...actual,
    requireSupabaseJwt: async (_c: unknown, next: () => Promise<void>) => { await next(); },
  };
});

vi.mock('../../../src/services/siteSurvey.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/siteSurvey.js')>();
  return { ...actual, runningSurveys: vi.fn(async () => new Map()) };
});

vi.mock('../../../src/services/nodes/nodeUptime.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodes/nodeUptime.js')>();
  return { ...actual, nodeUptimeMany: vi.fn(async () => new Map()) };
});

vi.mock('../../../src/api/users.js', () => ({
  getUsername: vi.fn(async () => 'someuser'),
  getUsernameMap: vi.fn(async () => new Map()),
}));

vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

const { hub } = await import('../../../src/services/nodes/hub.js');
const { feederRouter } = await import('../../../src/api/feeder.js');
const { nodesRouter } = await import('../../../src/api/nodes.js');

const fakeSocket = { send: () => {}, close: () => {}, readyState: 1 } as unknown as WebSocket;

function app(router: Hono, userId: string) {
  const a = new Hono();
  a.use('*', async (c, next) => { c.set('userId', userId); await next(); });
  a.route('/', router);
  return a;
}

beforeEach(() => {
  hub.registerAgent(NODE.id!, fakeSocket, NODE.user_id!, 'install-1');
  hub.recordStatus(NODE.id!, { components: {} });
  hub.setLocalListen(NODE.id!, { port: 17380, ip: '192.168.1.50' });
});

afterEach(() => {
  hub.unregisterAgent(NODE.id!, fakeSocket);
});

describe('local listener link', () => {
  it('is on the owner’s feeder view', async () => {
    const res = await app(feederRouter, 'user-owner').request('/api/feeder/me');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { nodes: Array<Record<string, unknown>> };
    expect(body.nodes[0]!['localListen']).toEqual({ url: 'http://192.168.1.50:17380' });
  });

  it('is gone once the node goes offline', async () => {
    hub.unregisterAgent(NODE.id!, fakeSocket);
    const res = await app(feederRouter, 'user-owner').request('/api/feeder/me');
    const body = (await res.json()) as { nodes: Array<Record<string, unknown>> };
    expect(body.nodes[0]!['localListen']).toBeNull();
  });

  it('brackets an IPv6 address', async () => {
    const { localListenLinks } = await import('../../../src/services/nodes/localListen.js');
    expect(localListenLinks({ port: 17380, ip: 'fe80::1' })).toEqual({ url: 'http://[fe80::1]:17380' });
  });

  it('never reaches the staff view, which still shows the IP', async () => {
    const res = await app(nodesRouter, 'staff-1').request(`/api/nodes/${NODE.id}`);
    expect(res.status).toBe(200);
    const text = await res.text();
    expect(text).not.toContain('17380');
    expect(text).not.toContain('localListen');
    expect((JSON.parse(text) as Record<string, unknown>)['localIp']).toBe('192.168.1.50');
  });
});
