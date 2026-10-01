import { describe, it, expect } from 'vitest';
import { parseRing } from '../../../src/services/nodes/chanmgrLog.js';

describe('chanmgr log ring parsing', () => {
  it('accepts well-formed entries and drops garbage', () => {
    const ring = parseRing({
      enabled: true,
      log: [
        { atMs: 1760000000000, channel: 'GRN Site A', kind: 'stopped', text: 'auto-stopped' },
        { atMs: 'not a number', channel: 'X', kind: 'stopped', text: '' },
        { channel: 'missing-at', kind: 'stopped' },
        null,
        'nonsense',
        { atMs: 1760000050000, channel: 'GRN Site A', kind: 'probeFail', text: 'test failed' },
      ],
    });
    expect(ring).toHaveLength(2);
    expect(ring[0]).toEqual({ atMs: 1760000000000, channel: 'GRN Site A', kind: 'stopped', text: 'auto-stopped' });
    expect(ring[1]!.kind).toBe('probeFail');
  });

  it('tolerates absent / malformed channelManager shapes', () => {
    expect(parseRing(null)).toEqual([]);
    expect(parseRing(undefined)).toEqual([]);
    expect(parseRing('x')).toEqual([]);
    expect(parseRing({})).toEqual([]);
    expect(parseRing({ log: 'not-an-array' })).toEqual([]);
  });

  it('bounds field lengths so a hostile agent cannot bloat rows', () => {
    const ring = parseRing({ log: [{ atMs: 1, channel: 'c'.repeat(999), kind: 'k'.repeat(999), text: 't'.repeat(9999) }] });
    expect(ring[0]!.channel).toHaveLength(200);
    expect(ring[0]!.kind).toHaveLength(40);
    expect(ring[0]!.text).toHaveLength(500);
  });
});
