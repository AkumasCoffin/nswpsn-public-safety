// ADS-B feeder-node plumbing: the config payload a node receives, and the
// store that folds its snapshots into the same picture the aggregators feed.

import { describe, it, expect, beforeEach } from 'vitest';
import { buildConfigPayload, adsbGainOf, adsbPpmOf } from '../../../src/services/nodes/configMerge.js';
import {
  recordNodeAdsbSnapshot,
  recordNodeAdsbReception,
  nodeAdsbRecords,
  nodeAdsbFeedCount,
  adsbNodeSourceId,
  _resetAdsbNodeStore,
} from '../../../src/services/nodes/adsbNodeStore.js';
import { normalizeNodeUpload, mergeAircraft } from '../../../src/sources/adsb.js';
import type { NodeRow } from '../../../src/services/nodes/registry.js';

function adsbNode(over: Partial<NodeRow> = {}): NodeRow {
  return {
    id: '11111111-1111-1111-1111-111111111111',
    kind: 'adsb',
    name: 'adsb-test-1111',
    enabled: true,
    feed_enabled: true,
    config_override: {},
    lat: -33.8688,
    lon: 151.2093,
    ...over,
  } as NodeRow;
}

describe('adsb config payload', () => {
  it('is radio-free: no presets, rdio doc or stream targets', async () => {
    const p = await buildConfigPayload(adsbNode());
    expect(p.channels).toEqual([]);
    expect(p.tuners).toEqual([]);
    expect(p.aliases).toEqual([]);
    expect(p.rdioConfig).toEqual({});
    expect(p.streamTargets).toEqual([]);
    expect(p.pager).toBeUndefined();
  });

  it('carries the exact antenna position', async () => {
    // Not decoration: dump1090 is launched with --lat/--lon and cannot report
    // range at all without them.
    const p = await buildConfigPayload(adsbNode());
    expect(p.adsb?.lat).toBe(-33.8688);
    expect(p.adsb?.lon).toBe(151.2093);
  });

  it('omits gain and ppm when unset, so configVersion stays stable', async () => {
    const a = await buildConfigPayload(adsbNode());
    const b = await buildConfigPayload(adsbNode());
    expect(a.adsb?.gain).toBeUndefined();
    expect(a.adsb?.ppm).toBeUndefined();
    expect(a.configVersion).toBe(b.configVersion);
  });

  it('changes configVersion when the pin moves', async () => {
    // The whole point of hashing lat/lon in: moving the pin must retune the
    // decoder live rather than waiting for some unrelated edit.
    const before = await buildConfigPayload(adsbNode());
    const after = await buildConfigPayload(adsbNode({ lat: -27.4698, lon: 153.0251 }));
    expect(after.configVersion).not.toBe(before.configVersion);
  });

  it('changes configVersion when gain changes, and carries it', async () => {
    const auto = await buildConfigPayload(adsbNode({ config_override: { adsbGain: 'auto' } }));
    const fixed = await buildConfigPayload(adsbNode({ config_override: { adsbGain: '42.1' } }));
    expect(auto.adsb?.gain).toBe('auto');
    expect(fixed.adsb?.gain).toBe('42.1');
    expect(auto.configVersion).not.toBe(fixed.configVersion);
  });

  it('reads only well-formed overrides', () => {
    expect(adsbGainOf(adsbNode({ config_override: { adsbGain: 'auto' } }))).toBe('auto');
    expect(adsbGainOf(adsbNode({ config_override: { adsbGain: 'banana' } }))).toBeUndefined();
    expect(adsbPpmOf(adsbNode({ config_override: { adsbPpm: 5 } }))).toBe(5);
    expect(adsbPpmOf(adsbNode({ config_override: { adsbPpm: 'five' } }))).toBeUndefined();
  });

  it('reflects capture/feed toggles', async () => {
    const off = await buildConfigPayload(adsbNode({ enabled: false, feed_enabled: false }));
    expect(off.captureEnabled).toBe(false);
    expect(off.feedEnabled).toBe(false);
  });
});

describe('adsb node store', () => {
  beforeEach(() => _resetAdsbNodeStore());

  const upload = (over: Record<string, unknown> = {}) => ({
    at: new Date().toISOString(),
    aircraft: [
      { hex: '7c6db8', flight: 'QFA123 ', lat: -33.7, lon: 150.9, alt_baro: 3500, gs: 210, track: 118, seen_pos: 1 },
      { hex: 'abc123', lat: -33.9, lon: 151.1, alt_baro: 'ground' as const, seen_pos: 2 },
    ],
    ...over,
  });

  function store(nodeId: string, name: string, u = upload()): number {
    const records = normalizeNodeUpload(u, adsbNodeSourceId(nodeId, name));
    recordNodeAdsbReception(nodeId, records);
    return recordNodeAdsbSnapshot(nodeId, name, records);
  }

  it('tags records with the node as their source', () => {
    store('node-a', 'adsb-syd-1');
    const recs = nodeAdsbRecords();
    expect(recs).toHaveLength(2);
    expect(recs[0]!.sources).toEqual(['node:adsb-syd-1']);
  });

  it('falls back to a short id when the node has no name', () => {
    expect(adsbNodeSourceId('deadbeef-cafe', null)).toBe('node:deadbeef');
  });

  it('replaces a node\'s previous snapshot rather than merging it', () => {
    // A snapshot is a complete restatement of what that receiver can see;
    // merging would resurrect aircraft that have left its coverage.
    store('node-a', 'n1');
    store('node-a', 'n1', { at: new Date().toISOString(), aircraft: [
      { hex: '7c6db8', lat: -33.7, lon: 150.9, seen_pos: 1 },
    ] });
    expect(nodeAdsbRecords()).toHaveLength(1);
  });

  it('ages records forward as the snapshot sits unread', () => {
    store('node-a', 'n1');
    const later = nodeAdsbRecords(Date.now() + 20_000);
    expect(later[0]!.ageSec).toBeGreaterThan(19);
  });

  it('drops records that age past the 60s position cutoff', () => {
    store('node-a', 'n1');
    expect(nodeAdsbRecords(Date.now() + 90_000)).toEqual([]);
  });

  it('forgets a node whose snapshot is older than the TTL', () => {
    store('node-a', 'n1');
    expect(nodeAdsbFeedCount()).toBe(1);
    nodeAdsbRecords(Date.now() + 130_000);
    expect(nodeAdsbFeedCount()).toBe(0);
  });

  it('folds transit delay into the reported age', () => {
    // seen_pos is relative to the snapshot's own `at`, so a delayed upload
    // must not present minute-old positions as fresh.
    const old = new Date(Date.now() - 30_000).toISOString();
    store('node-a', 'n1', { at: old, aircraft: [{ hex: 'aaa111', lat: -33, lon: 151, seen_pos: 1 }] });
    expect(nodeAdsbRecords()[0]!.ageSec).toBeGreaterThan(30);
  });

  it('never treats a future-dated snapshot as fresher than now', () => {
    const future = new Date(Date.now() + 600_000).toISOString();
    store('node-a', 'n1', { at: future, aircraft: [{ hex: 'aaa111', lat: -33, lon: 151, seen_pos: 5 }] });
    expect(nodeAdsbRecords()[0]!.ageSec).toBeCloseTo(5, 0);
  });

  it('keeps nodes separate', () => {
    store('node-a', 'syd');
    store('node-b', 'bne', { at: new Date().toISOString(), aircraft: [
      { hex: 'ddd444', lat: -27.4, lon: 153.0, seen_pos: 1 },
    ] });
    expect(nodeAdsbFeedCount()).toBe(2);
    expect(nodeAdsbRecords()).toHaveLength(3);
  });
});

describe('node records merged against upstream records', () => {
  beforeEach(() => _resetAdsbNodeStore());

  it('lets the fresher node position win but keeps upstream metadata', () => {
    // This is why nodes are worth running: ~5s cadence against the
    // aggregators' ~15s. The aggregator still knows the registration.
    const nodeRecs = normalizeNodeUpload(
      { at: new Date().toISOString(), aircraft: [{ hex: '7c6db8', lat: -33.5, lon: 150.5, seen_pos: 1 }] },
      'node:syd',
    );
    const upstream = normalizeNodeUpload(
      { at: new Date().toISOString(), aircraft: [{ hex: '7c6db8', lat: -33.9, lon: 151.9, seen_pos: 20, r: 'VH-ABC', t: 'B738' }] },
      'adsb_lol',
    );
    const [merged] = mergeAircraft([...upstream, ...nodeRecs]);
    expect(merged!.lat).toBe(-33.5);
    expect(merged!.reg).toBe('VH-ABC');
    expect(merged!.type).toBe('B738');
    expect(merged!.sources.sort()).toEqual(['adsb_lol', 'node:syd']);
    expect(merged!.sourceCount).toBe(2);
  });

  it('keeps an aircraft only our node can see', () => {
    const nodeRecs = normalizeNodeUpload(
      { at: new Date().toISOString(), aircraft: [{ hex: 'fff999', lat: -31.2, lon: 146.8, seen_pos: 2 }] },
      'node:remote',
    );
    const merged = mergeAircraft([...nodeRecs]);
    expect(merged).toHaveLength(1);
    expect(merged[0]!.sources).toEqual(['node:remote']);
  });
});
