/**
 * Per-state pager node routes on the staff nodes router:
 *   - POST /api/nodes            — per-kind location rule (radio: zone; pager: state+lga)
 *   - PUT  /api/nodes/:id/state  — move a pager node between states (re-pushes config)
 *   - PATCH /api/nodes/:id       — compare-and-set on config_override
 *
 * Registry + config push are mocked; requireRole stays real with the role
 * predicates stubbed true (the gate itself is covered by auth-roles tests).
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
    updateNode: vi.fn(async () => nodeRow()),
    setNodeLocation: vi.fn(async () => nodeRow()),
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
  return {
    ...actual,
    canManageNodes: vi.fn(async () => true),
    canViewNodeData: vi.fn(async () => true),
  };
});

vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

vi.mock('../../../src/api/users.js', () => ({
  getUsername: vi.fn(async () => 'someuser'),
  getUsernameMap: vi.fn(async () => new Map()),
}));

const { nodesRouter } = await import('../../../src/api/nodes.js');
const registry = await import('../../../src/services/nodes/registry.js');
const configPush = await import('../../../src/services/nodes/configPush.js');
const { log } = await import('../../../src/lib/log.js');

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

function patch(path: string, body: unknown) {
  return makeApp().request(path, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

beforeEach(() => {
  vi.mocked(registry.getNode).mockClear().mockResolvedValue(nodeRow());
  vi.mocked(registry.updateNode).mockClear().mockResolvedValue(nodeRow());
  vi.mocked(log.warn).mockClear();
  vi.mocked(registry.setNodeLocation).mockClear();
  vi.mocked(registry.createNode).mockClear();
  vi.mocked(configPush.pushConfigToNode).mockClear();
});

describe('GET /api/nodes/:id (pager)', () => {
  it("hands the staff drawer the node's state plan", async () => {
    vi.mocked(registry.getNode).mockResolvedValue(nodeRow({ state: 'QLD' }));
    const res = await makeApp().request('/api/nodes/node-1');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['pagerFrequencies']).toEqual([{ label: 'QFES', mhz: 148.6375 }]);
    expect(body).not.toHaveProperty('pagerPrimary');
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
    expect(loc).toEqual({ zone: null, state: 'QLD', lga: 'Cairns', suburb: null });
  });

  it('radio create requires state + lga too — the survey searches from the LGA', async () => {
    expect((await post('/api/nodes', { userId: 'u1', kind: 'radio' })).status).toBe(400);
    expect((await post('/api/nodes', { userId: 'u1', kind: 'radio', state: 'NSW' })).status).toBe(400);
    const ok = await post('/api/nodes', {
      userId: 'u1', kind: 'radio', state: 'NSW', lga: 'Blue Mountains', suburb: 'Katoomba',
    });
    expect(ok.status).toBe(200);
    const loc = vi.mocked(registry.createNode).mock.calls[0]?.[5];
    expect(loc).toEqual({ zone: null, state: 'NSW', lga: 'Blue Mountains', suburb: 'Katoomba' });
  });
});

// ---------------------------------------------------------------------------
// PATCH /api/nodes/:id — config_override is a whole-document replace, and the
// document moves underneath the editor: appendWinnerChannels adds a site
// survey's winners server-side while the drawer that started the survey stays
// open for the 45 minutes it runs. The next save from that drawer used to
// PATCH the pre-survey channel list straight back, and pushConfigToNode then
// stopped those channels on the node — with "Config saved" on screen and the
// survey record still claiming N added.
// ---------------------------------------------------------------------------
describe('PATCH /api/nodes/:id config_override concurrency', () => {
  /** A radio node with the one channel a survey has just added. */
  const surveyed = () =>
    nodeRow({
      kind: 'radio',
      config_override: {
        channels: [{ name: 'Mount Boyce', frequency: 419_912_500 }],
        tuners: [],
      },
    });

  async function storedVersion(): Promise<string> {
    const res = await makeApp().request('/api/nodes/node-1');
    const body = (await res.json()) as { configOverrideVersion: string };
    return body.configOverrideVersion;
  }

  it('hands the client the version of the override it is about to edit', async () => {
    vi.mocked(registry.getNode).mockResolvedValue(surveyed());
    const v = await storedVersion();
    expect(v).toMatch(/^[0-9a-f]{16}$/);
  });

  it('409s a save built from a stale read, and hands back what to rebuild from', async () => {
    vi.mocked(registry.getNode).mockResolvedValue(surveyed());
    const res = await patch('/api/nodes/node-1', {
      // The channel list as the drawer saw it BEFORE the survey finished.
      config_override: { channels: [], tuners: [{ serial: 'abc', gain: 30 }] },
      config_override_version: 'stale0000000000',
    });
    expect(res.status).toBe(409);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['error']).toBe('config_changed');
    // The current document, so the client can re-apply its edit to it.
    expect(body['configOverride']).toEqual(surveyed().config_override);
    expect(body['configOverrideVersion']).toBe(await storedVersion());
    // Nothing written: the survey's channel is still there.
    expect(registry.updateNode).not.toHaveBeenCalled();
  });

  it('applies the save when the version still matches', async () => {
    vi.mocked(registry.getNode).mockResolvedValue(surveyed());
    const res = await patch('/api/nodes/node-1', {
      config_override: { channels: [{ name: 'Mount Boyce', frequency: 419_912_500 }], tuners: [] },
      config_override_version: await storedVersion(),
    });
    expect(res.status).toBe(200);
    expect(registry.updateNode).toHaveBeenCalledTimes(1);
  });

  it('still accepts a save with no version, but says what it discarded', async () => {
    // The staff drawer does not send a version yet, so refusing every
    // versionless PATCH would break channel editing outright. Log what goes
    // instead, so the loss stops being invisible.
    vi.mocked(registry.getNode).mockResolvedValue(surveyed());
    const res = await patch('/api/nodes/node-1', {
      config_override: { channels: [], tuners: [] },
    });
    expect(res.status).toBe(200);
    expect(registry.updateNode).toHaveBeenCalledTimes(1);
    const warned = vi.mocked(log.warn).mock.calls.find((call) =>
      String(call[1]).includes('drops stored channels'),
    );
    expect(warned).toBeDefined();
    expect((warned?.[0] as { dropped?: string[] }).dropped).toEqual(['mount boyce']);
  });

  it('says nothing when the save keeps every stored channel', async () => {
    vi.mocked(registry.getNode).mockResolvedValue(surveyed());
    const res = await patch('/api/nodes/node-1', {
      config_override: {
        channels: [{ name: 'Mount Boyce', frequency: 419_912_500 }, { name: 'Katoomba', frequency: 419_925_000 }],
        tuners: [],
      },
    });
    expect(res.status).toBe(200);
    expect(
      vi.mocked(log.warn).mock.calls.some((call) => String(call[1]).includes('drops stored channels')),
    ).toBe(false);
  });
});
