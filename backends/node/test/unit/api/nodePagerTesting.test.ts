/**
 * Pager testing mode on the relay.
 *
 * A node in testing mode is on hand-programmed frequencies, so its pages are
 * shown in the staff drawer only: never forwarded to Pagermon, never stored in
 * node_pager_events. They are acked like Feed off so the agent dequeues them.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

// Hoisted: the vi.mock factories below run before this file's own top level.
const { fetchMock, recordPagerEvent, nodeRow } = vi.hoisted(() => ({
  fetchMock: vi.fn(async () => new Response('{}', { status: 200 })),
  recordPagerEvent: vi.fn(async () => undefined),
  nodeRow: { current: {} as Record<string, unknown> },
}));

vi.mock('../../../src/services/auth/nodeToken.js', () => ({
  resolveNodeToken: vi.fn(async () => ({
    ok: true, nodeId: 'node-pager-1111', userId: 'u1', kind: 'pager',
    enabled: true, feedEnabled: true, installId: 'install-1',
  })),
}));
vi.mock('../../../src/services/nodes/globalConfig.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodes/globalConfig.js')>();
  return { ...actual, getPagerIngest: vi.fn(async () => ({ url: 'http://pagermon.test', apiKey: 'k' })) };
});
vi.mock('../../../src/services/nodeEvents.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodeEvents.js')>();
  return { ...actual, recordPagerEvent };
});
vi.mock('../../../src/services/nodes/registry.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodes/registry.js')>();
  return {
    ...actual,
    getNode: vi.fn(async () => ({ id: 'node-pager-1111', kind: 'pager', name: 'pager-test', state: 'NSW', ...nodeRow.current })),
    bumpNodeCallStat: vi.fn(async () => undefined),
  };
});

const { hub } = await import('../../../src/services/nodes/hub.js');

async function upload() {
  const { nodeIngestRouter } = await import('../../../src/api/node-ingest.js');
  const body = JSON.stringify({
    address: '1234567', function: 0, message: 'TEST PAGE ON A BENCH FREQUENCY', source: 'BENCH', freqMhz: 153.35,
  });
  return nodeIngestRouter.request('/api/node-ingest/pager-upload', {
    method: 'POST',
    body,
    headers: {
      'content-type': 'application/json',
      'content-length': String(Buffer.byteLength(body)),
      'X-Node-Token': 'nsn_test',
      'X-Node-Install': 'install-1',
    },
  });
}

describe('POST /api/node-ingest/pager-upload: testing mode', () => {
  beforeEach(() => {
    fetchMock.mockClear();
    recordPagerEvent.mockClear();
    vi.stubGlobal('fetch', fetchMock);
    hub.clearNode('node-pager-1111');
  });

  it('keeps a testing node\'s page in the drawer only', async () => {
    nodeRow.current = { config_override: { pagerTesting: true, pagerTestFrequencies: [{ label: 'BENCH', mhz: 153.35 }] } };
    const spy = vi.spyOn(hub, 'recordPagerMessage');
    const res = await upload();
    // 200, so the agent dequeues it instead of retrying forever.
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ ok: true, fed: false });
    expect(fetchMock).not.toHaveBeenCalled();
    expect(recordPagerEvent).not.toHaveBeenCalled();
    expect(spy).toHaveBeenCalledWith('node-pager-1111', expect.objectContaining({ testing: true, source: 'BENCH' }));
    spy.mockRestore();
  });

  it('forwards and stores as normal once testing is off', async () => {
    // The test list is kept while off; only pagerTesting decides.
    nodeRow.current = { config_override: { pagerTestFrequencies: [{ label: 'BENCH', mhz: 153.35 }] } };
    const res = await upload();
    expect(res.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(recordPagerEvent).toHaveBeenCalledTimes(1);
  });
});
