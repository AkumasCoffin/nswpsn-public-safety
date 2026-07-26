import { describe, it, expect } from 'vitest';
import { buildConfigPayload } from '../../../src/services/nodes/configMerge.js';
import type { NodeRow } from '../../../src/services/nodes/registry.js';

function pagerNode(overrides: Partial<NodeRow> = {}): NodeRow {
  return {
    id: 'node-1',
    kind: 'pager',
    user_id: 'user-1',
    install_id: null,
    name: 'pager-user-abcd1234',
    enabled: true,
    feed_enabled: false,
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
    ...overrides,
  } as NodeRow;
}

describe('buildConfigPayload (pager)', () => {
  it('returns a lean pager payload: NSW RFS + FRNSW frequencies, POCSAG rates, no rdio doc', async () => {
    const p = await buildConfigPayload(pagerNode());
    expect(p.pager).toBeDefined();
    expect(p.pager!.frequencies.map((f) => f.mhz)).toEqual([148.5875, 148.9625]);
    expect(p.pager!.frequencies[0]!.label).toBe('NSWRFS');
    expect(p.pager!.protocols).toEqual(['POCSAG512', 'POCSAG1200', 'POCSAG2400']);
    // No radio payload leaks into a pager node.
    expect(p.channels).toEqual([]);
    expect(p.tuners).toEqual([]);
    expect(p.aliases).toEqual([]);
    expect(p.rdioConfig).toEqual({});
    expect(p.streamTargets).toEqual([]);
    expect(p.configVersion).toMatch(/^[0-9a-f]{64}$/);
  });

  it('capture/feed reflect the node flags and change the config version', async () => {
    const on = await buildConfigPayload(pagerNode({ enabled: true, feed_enabled: true }));
    const capOff = await buildConfigPayload(pagerNode({ enabled: false, feed_enabled: true }));
    expect(on.captureEnabled).toBe(true);
    expect(on.feedEnabled).toBe(true);
    expect(capOff.captureEnabled).toBe(false);
    // A toggle must bump the version so the agent re-applies.
    expect(on.configVersion).not.toEqual(capOff.configVersion);
  });
});
