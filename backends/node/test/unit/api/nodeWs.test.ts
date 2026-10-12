/**
 * Node WebSocket endpoints, driven over a real socket on an ephemeral port.
 *
 * Three properties here are the difference between a bad minute and a dead
 * API, so they are pinned against the real `ws` plumbing rather than a stub:
 *
 *   - a frame whose handler rejects is logged, not fatal. index.ts exits the
 *     process on any unhandledRejection, and these handlers are started
 *     fire-and-forget, so one Postgres blip on the per-frame last-seen write
 *     used to take every agent and staff socket down with it.
 *   - agent-supplied metadata is NUL-free. Postgres rejects U+0000 with 22021,
 *     and the agent re-sends hello on every reconnect, so any contributor
 *     could have held the process in a restart loop.
 *   - a staff socket that goes away stops the audio tap it started. Nothing
 *     else ever told the agent to stop, so a closed tab left a volunteer's
 *     node streaming PCM upstream indefinitely.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { WebSocket } from 'ws';

vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn(), fatal: vi.fn() },
}));

const NODE_ID = 'node-1';
const nodeRow = { id: NODE_ID, kind: 'pager', config_version: 'cv1', enabled: true };

const refreshNodeOnHello = vi.fn(async () => nodeRow);
const touchNodeSeenThrottled = vi.fn(async () => undefined);
const touchNodeSeen = vi.fn(async () => undefined);
const updateNode = vi.fn(async () => nodeRow);

vi.mock('../../../src/services/nodes/registry.js', () => ({
  refreshNodeOnHello: (...a: unknown[]) => refreshNodeOnHello(...(a as [])),
  bindInstallId: vi.fn(async () => 'bound'),
  touchNodeSeenThrottled: (...a: unknown[]) => touchNodeSeenThrottled(...(a as [])),
  touchNodeSeen: (...a: unknown[]) => touchNodeSeen(...(a as [])),
  updateNode: (...a: unknown[]) => updateNode(...(a as [])),
  getNode: vi.fn(async () => nodeRow),
  roleForKind: () => 'feeder:pager',
}));

vi.mock('../../../src/services/auth/nodeToken.js', () => ({
  resolveNodeToken: vi.fn(async () => ({
    ok: true,
    nodeId: NODE_ID,
    userId: 'u1',
    kind: 'pager',
  })),
}));

vi.mock('../../../src/services/auth/supabaseJwt.js', () => ({
  verifySupabaseToken: vi.fn(async (t: string) =>
    t === 'good' ? { userId: 'staff-1', name: 'Staff' } : null,
  ),
}));

vi.mock('../../../src/services/auth/roles.js', () => ({
  hasRole: vi.fn(async () => true),
}));

vi.mock('../../../src/services/nodes/configMerge.js', () => ({
  buildConfigPayload: vi.fn(async () => ({ configVersion: 'cv1' })),
}));

vi.mock('../../../src/services/nodeLive.js', () => ({
  shapeNodeLive: vi.fn(async () => null),
}));

vi.mock('../../../src/services/nodes/chanmgrLog.js', () => ({
  ingestChanMgrLog: vi.fn(async () => undefined),
}));

vi.mock('../../../src/services/nodes/nodeUptime.js', () => ({
  markNodeSeen: vi.fn(),
}));

vi.mock('../../../src/services/siteSurvey.js', () => ({
  maybeStartInstallSurvey: vi.fn(async () => undefined),
  handleSurveyCommand: vi.fn(async () => null),
  noteSurveyStatus: vi.fn(),
}));

const { attachNodeWebSockets, closeNodeWebSockets } = await import(
  '../../../src/api/node-ws.js'
);
const { log } = await import('../../../src/lib/log.js');

let server: Server;
let base: string;
const open: WebSocket[] = [];

/** Resolve once the socket is open, or reject on a handshake failure. */
function connect(path: string, headers?: Record<string, string>): Promise<WebSocket> {
  const ws = new WebSocket(`${base}${path}`, headers ? { headers } : undefined);
  open.push(ws);
  return new Promise((resolve, reject) => {
    ws.once('open', () => resolve(ws));
    ws.once('error', reject);
  });
}

/** The next text frame whose envelope type is `t`. */
function waitFor(ws: WebSocket, t: string, timeoutMs = 2_000): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.off('message', onMsg);
      reject(new Error(`no ${t} frame within ${timeoutMs}ms`));
    }, timeoutMs);
    function onMsg(raw: Buffer, isBinary: boolean): void {
      if (isBinary) return;
      const env = JSON.parse(raw.toString('utf8')) as { t: string };
      if (env.t !== t) return;
      clearTimeout(timer);
      ws.off('message', onMsg);
      resolve(env as Record<string, unknown>);
    }
    ws.on('message', onMsg);
  });
}

const agentHeaders = {
  'x-node-token': 'tok',
  'x-node-install': 'install-abcdef01',
};

/** Let the fire-and-forget handlers settle. */
const settle = () => new Promise((r) => setTimeout(r, 30));

beforeEach(async () => {
  vi.clearAllMocks();
  refreshNodeOnHello.mockResolvedValue(nodeRow);
  server = createServer();
  attachNodeWebSockets(server);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  base = `ws://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterEach(async () => {
  for (const ws of open.splice(0)) {
    try { ws.terminate(); } catch { /* already gone */ }
  }
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

describe('agent frames', () => {
  it('strips NUL from hello metadata before it reaches the registry', async () => {
    const ws = await connect('/api/node-ws/agent', agentHeaders);
    ws.send(JSON.stringify({
      t: 'hello',
      data: { agentVersion: '0.7.9\u0000', hostname: '\u0000', kind: 'pager' },
    }));
    await waitFor(ws, 'helloAck');
    expect(refreshNodeOnHello).toHaveBeenCalledTimes(1);
    const meta = refreshNodeOnHello.mock.calls[0]![1] as Record<string, unknown>;
    expect(meta['agentVersion']).toBe('0.7.9');
    // A field that was NOTHING BUT NUL is nothing at all, not an empty string.
    expect(meta['hostname']).toBeNull();
  });

  it('strips NUL from the configApplied version too', async () => {
    const ws = await connect('/api/node-ws/agent', agentHeaders);
    ws.send(JSON.stringify({ t: 'configApplied', data: { configVersion: 'cv2\u0000' } }));
    await settle();
    expect(updateNode).toHaveBeenCalledWith(NODE_ID, { config_version: 'cv2' });
  });

  it('logs a rejected handler instead of letting it reach unhandledRejection', async () => {
    refreshNodeOnHello.mockRejectedValueOnce(new Error('22021 invalid byte'));
    const onUnhandled = vi.fn();
    process.on('unhandledRejection', onUnhandled);
    try {
      const ws = await connect('/api/node-ws/agent', agentHeaders);
      ws.send(JSON.stringify({ t: 'hello', data: { agentVersion: '0.7.9' } }));
      await settle();
      expect(onUnhandled).not.toHaveBeenCalled();
      expect(vi.mocked(log.warn)).toHaveBeenCalledWith(
        expect.objectContaining({ nodeId: NODE_ID, t: 'hello' }),
        'agent message handler failed',
      );
      // The socket survives a failed frame — a dropped agent would have to
      // re-enrol its whole session over one bad write.
      expect(ws.readyState).toBe(WebSocket.OPEN);
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });

  it('writes last-seen through the throttled path, not once per frame', async () => {
    const ws = await connect('/api/node-ws/agent', agentHeaders);
    for (let i = 0; i < 3; i += 1) {
      ws.send(JSON.stringify({ t: 'status', data: { components: {} } }));
    }
    await settle();
    expect(touchNodeSeenThrottled).toHaveBeenCalledTimes(3);
    expect(touchNodeSeen).not.toHaveBeenCalled();
  });
});

describe('pager audio taps', () => {
  /** An authenticated staff socket subscribed to the node. */
  async function staffSocket(): Promise<WebSocket> {
    const ws = await connect('/api/node-ws/staff');
    ws.send(JSON.stringify({ t: 'auth', data: { token: 'good' } }));
    await waitFor(ws, 'authOk');
    ws.send(JSON.stringify({ t: 'subscribeNode', data: { nodeId: NODE_ID } }));
    await waitFor(ws, 'nodeStatus');
    return ws;
  }

  it('tells the agent to stop when the listening socket goes away', async () => {
    const agent = await connect('/api/node-ws/agent', agentHeaders);
    const staff = await staffSocket();
    staff.send(JSON.stringify({ t: 'audioStart', data: { nodeId: NODE_ID, label: 'NSWRFS' } }));
    await waitFor(agent, 'audioStart');

    const stopped = waitFor(agent, 'audioStop');
    staff.close();
    await expect(stopped).resolves.toMatchObject({ t: 'audioStop' });
  });

  it('keeps the tap alive while another listener still wants it', async () => {
    const agent = await connect('/api/node-ws/agent', agentHeaders);
    const a = await staffSocket();
    const b = await staffSocket();
    a.send(JSON.stringify({ t: 'audioStart', data: { nodeId: NODE_ID, label: 'NSWRFS' } }));
    await waitFor(agent, 'audioStart');
    b.send(JSON.stringify({ t: 'audioStart', data: { nodeId: NODE_ID, label: 'NSWRFS' } }));
    await waitFor(agent, 'audioStart');

    // One listener leaving must not cut the other's audio.
    const stopped = waitFor(agent, 'audioStop', 300).then(() => 'stopped', () => 'still running');
    a.close();
    await expect(stopped).resolves.toBe('still running');

    const finalStop = waitFor(agent, 'audioStop');
    b.close();
    await expect(finalStop).resolves.toMatchObject({ t: 'audioStop' });
  });
});

describe('closeNodeWebSockets', () => {
  it('closes connected sockets with 1001 so shutdown can finish', async () => {
    const agent = await connect('/api/node-ws/agent', agentHeaders);
    const closed = new Promise<number>((resolve) => agent.once('close', resolve));
    closeNodeWebSockets();
    expect(await closed).toBe(1001);
  });
});
