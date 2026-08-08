import { describe, it, expect } from 'vitest';
import { buildConfigPayload } from '../../../src/services/nodes/configMerge.js';
import {
  SdrtrunkConfigSchema,
  type GlobalConfig,
} from '../../../src/services/nodes/globalConfig.js';
import type { NodeRow } from '../../../src/services/nodes/registry.js';

function radioNode(overrides: Partial<NodeRow> = {}): NodeRow {
  return {
    id: 'node-1',
    kind: 'radio',
    user_id: 'user-1',
    install_id: null,
    name: 'radio-node',
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
    ...overrides,
  } as NodeRow;
}

function globalWith(sdrtrunkConfig: GlobalConfig['sdrtrunkConfig']): GlobalConfig {
  return {
    agencies: [
      { systemId: 2, name: 'Fire and Rescue', talkgroups: [], units: [], aliasIds: [] },
    ] as unknown as GlobalConfig['agencies'],
    rdioGroups: [],
    rdioTags: [],
    sdrtrunkConfig,
    version: 'test',
    updatedAt: null,
    updatedBy: null,
    streamNames: [],
  };
}

describe('SdrtrunkConfigSchema', () => {
  it('round-trips alias lists, aliases and streams; defaults empty', () => {
    const parsed = SdrtrunkConfigSchema.parse({
      aliasLists: [{ name: 'NSWPSN' }],
      aliases: [
        {
          name: '101 ME1', list: 'NSWPSN', group: 'FRNSW',
          ids: [
            { type: 'broadcastChannel', attrs: { channel: 'FRNSW' } },
            { type: 'talkgroup', attrs: { protocol: 'APCO25', value: '10101' } },
          ],
        },
      ],
      streams: [{ name: 'FRNSW', systemId: 2 }],
    });
    expect(parsed.aliasLists[0]).toEqual({ name: 'NSWPSN', family: 'P25' }); // family defaults
    expect(parsed.streams[0]).toEqual({ name: 'FRNSW', systemId: 2, enabled: true }); // enabled defaults
    expect(SdrtrunkConfigSchema.parse({})).toEqual({ aliasLists: [], aliases: [], streams: [] });
  });
});

describe('buildConfigPayload (radio) uses the imported sdrtrunk config', () => {
  it('pushes imported aliases (collapsed to the primary list) + imported streams', async () => {
    const global = globalWith({
      aliasLists: [{ name: 'NSWPSN', family: 'P25' }, { name: 'catch all PSN', family: 'P25' }],
      aliases: [
        {
          name: '101 ME1', list: 'NSWPSN', group: 'FRNSW',
          ids: [
            { type: 'broadcastChannel', attrs: { channel: 'FRNSW' } },
            { type: 'talkgroup', attrs: { protocol: 'APCO25', value: '10101' } },
          ],
        },
        {
          name: 'Fire & Rescue', list: 'catch all PSN', group: 'FRNSW',
          ids: [
            { type: 'broadcastChannel', attrs: { channel: 'FRNSW' } },
            { type: 'talkgroupRange', attrs: { protocol: 'APCO25', min: '10101', max: '10199' } },
          ],
        },
      ],
      streams: [{ name: 'FRNSW', systemId: 2, enabled: true }],
    });
    const p = await buildConfigPayload(radioNode(), global);
    // Every imported alias survives, collapsed into the single primary list.
    expect(p.aliases).toHaveLength(2);
    expect(new Set(p.aliases.map((a) => a.list))).toEqual(new Set(['NSWPSN']));
    expect(p.aliases[0]!.name).toBe('101 ME1');
    // Streams come from the import (name = broadcastChannel route; systemId links rdio).
    expect(p.streamTargets).toEqual([{ systemId: 2, name: 'FRNSW' }]);
    // rdio apiKey exists for that systemId (agent fills the key), linked by systemId.
    const apiKeys = (p.rdioConfig as { apiKeys?: Array<{ systems: Array<{ id: number }> }> }).apiKeys ?? [];
    expect(apiKeys.some((k) => k.systems?.[0]?.id === 2)).toBe(true);
    // rdio system 2 is present (from agencies), so the uploaded call has a home.
    const systems = (p.rdioConfig as { systems?: Array<{ id: number }> }).systems ?? [];
    expect(systems.some((s) => s.id === 2)).toBe(true);
  });

  it('empty sdrtrunk config → no aliases; streams fall back to rdio systems', async () => {
    const p = await buildConfigPayload(radioNode(), globalWith({ aliasLists: [], aliases: [], streams: [] }));
    expect(p.aliases).toEqual([]);
    // Fallback keeps a pre-import config from having zero streams (rdio system 2).
    expect(p.streamTargets.some((t) => t.systemId === 2)).toBe(true);
  });
});
