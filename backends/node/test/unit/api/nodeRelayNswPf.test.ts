/**
 * NSW PF calls never reach central rdio.
 *
 * The force is encrypted end to end, but a node still uploads one of its calls
 * now and then (encryption missed by the decoder, audio is noise). Central
 * rdio has none of these talkgroups, so it answered 200 and discarded each
 * one, and the relay took that 200 as delivery and flagged the call recorded.
 * The relay now acks them without forwarding, counting or stamping.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

// Hoisted: the vi.mock factories below run before this file's own top level.
const { fetchMock, markRecorded, bumpNodeCallStat } = vi.hoisted(() => ({
  fetchMock: vi.fn(async () => new Response('Call imported successfully.', { status: 200 })),
  markRecorded: vi.fn(async () => undefined),
  bumpNodeCallStat: vi.fn(async () => undefined),
}));

vi.mock('../../../src/config.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/config.js')>();
  return {
    ...actual,
    config: { ...actual.config, RDIO_INTERNAL_URL: 'http://rdio.test', RDIO_INTERNAL_API_KEY: 'internal' },
  };
});
vi.mock('../../../src/services/auth/nodeToken.js', () => ({
  resolveNodeToken: vi.fn(async () => ({
    ok: true, nodeId: 'node-aaaa-bbbb', userId: 'u1', kind: 'radio',
    enabled: true, feedEnabled: true, installId: 'install-1',
  })),
}));
vi.mock('../../../src/services/nodeEvents.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodeEvents.js')>();
  return { ...actual, markRecorded, mergeAutomaticPatch: vi.fn(async () => undefined) };
});
vi.mock('../../../src/services/nodes/registry.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodes/registry.js')>();
  return { ...actual, bumpNodeCallStat };
});

import { isNswPfTalkgroup } from '../../../src/api/node-ingest.js';

describe('isNswPfTalkgroup', () => {
  it('covers every NSW PF range, ends included', () => {
    for (const tg of [10450, 10467, 10485, 11701, 11708, 12001, 12065, 12275, 12501, 12502, 12515]) {
      expect(isNswPfTalkgroup(tg)).toBe(true);
    }
  });

  it('leaves the talkgroups either side of each range alone', () => {
    for (const tg of [10449, 10486, 11700, 11709, 12000, 12276, 12500, 12516]) {
      expect(isNswPfTalkgroup(tg)).toBe(false);
    }
    // Real traffic that must keep flowing: RFS, Marine Rescue, ambulance.
    for (const tg of [30003, 30013, 12543, 10076]) expect(isNswPfTalkgroup(tg)).toBe(false);
  });

  it('is false for a call with no talkgroup', () => {
    expect(isNswPfTalkgroup(null)).toBe(false);
  });
});

async function upload(talkgroup: string) {
  const { nodeIngestRouter } = await import('../../../src/api/node-ingest.js');
  const fd = new FormData();
  fd.set('key', 'node-local-key');
  fd.set('talkgroup', talkgroup);
  fd.set('dateTime', '1792000000');
  fd.set('source', '2019977');
  fd.set('audio', new File([new Uint8Array(512)], 'call.m4a', { type: 'audio/mp4' }));
  // A real Content-Length, which the relay requires. One encoding for both the
  // body and its content-type, or the multipart boundaries disagree.
  const encoded = new Response(fd);
  const type = encoded.headers.get('content-type')!;
  const body = await encoded.arrayBuffer();
  return nodeIngestRouter.request('/api/node-ingest/call-upload', {
    method: 'POST',
    body,
    headers: {
      'content-type': type,
      'content-length': String(body.byteLength),
      'X-Node-Token': 'nsn_test',
      'X-Node-Install': 'install-1',
    },
  });
}

describe('POST /api/node-ingest/call-upload: NSW PF', () => {
  beforeEach(() => {
    fetchMock.mockClear();
    markRecorded.mockClear();
    bumpNodeCallStat.mockClear();
    vi.stubGlobal('fetch', fetchMock);
  });

  it('acks an NSW PF call without forwarding, counting or flagging it recorded', async () => {
    const res = await upload('12502');
    // 200, so the agent dequeues it instead of retrying forever.
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ ok: true, fed: false });
    expect(fetchMock).not.toHaveBeenCalled();
    expect(bumpNodeCallStat).not.toHaveBeenCalled();
    expect(markRecorded).not.toHaveBeenCalled();
  });

  it('still forwards every other talkgroup', async () => {
    const res = await upload('30003');
    expect(res.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(markRecorded).toHaveBeenCalledTimes(1);
  });
});
