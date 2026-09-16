// Which agent binary each node kind is offered.
//
// Every agent's self-updater updates the component literally named "agent",
// so the backend remaps each kind's own binary onto that name. Getting this
// wrong does not fail loudly: the node downloads, sha256-verifies and installs
// a perfectly valid binary — for different hardware. An adsb node had no
// branch at all and fell through to the radio manifest, so it would have
// replaced its own agent with the radio one on first update.

import { describe, it, expect } from 'vitest';
import { manifestForKind, type Manifest } from '../../../src/api/node-updates.js';

const FULL: Manifest = {
  _comment: 'ignored',
  agent: { version: '0.2.25', urls: { 'linux-amd64': 'radio' }, sha256: { 'linux-amd64': 'r' } },
  'pager-agent': { version: '0.1.17', urls: { 'linux-amd64': 'pager' }, sha256: { 'linux-amd64': 'p' } },
  'adsb-agent': { version: '0.1.0', urls: { 'linux-amd64': 'adsb' }, sha256: { 'linux-amd64': 'a' } },
  sdrtrunk: { version: '0.7.15', urls: {}, sha256: {} },
  rdio: { version: '1.0.0', urls: {}, sha256: {} },
};

const versionOf = (m: Manifest, key: string): string | undefined => {
  const v = m[key];
  return typeof v === 'string' || v === undefined ? undefined : v.version;
};

describe('manifestForKind', () => {
  it('serves a pager node its own binary as "agent", and nothing else', () => {
    const m = manifestForKind(FULL, 'pager');
    expect(versionOf(m, 'agent')).toBe('0.1.17');
    expect(Object.keys(m)).toEqual(['agent']);
  });

  it('serves an adsb node its own binary as "agent", and nothing else', () => {
    // The bug: this used to return the radio manifest.
    const m = manifestForKind(FULL, 'adsb');
    expect(versionOf(m, 'agent')).toBe('0.1.0');
    expect(Object.keys(m)).toEqual(['agent']);
  });

  it('never offers a radio node another kind\'s agent', () => {
    const m = manifestForKind(FULL, 'radio');
    expect(versionOf(m, 'agent')).toBe('0.2.25');
    expect(m['pager-agent']).toBeUndefined();
    expect(m['adsb-agent']).toBeUndefined();
    // ...while keeping the components a radio node actually runs.
    expect(versionOf(m, 'sdrtrunk')).toBe('0.7.15');
    expect(versionOf(m, 'rdio')).toBe('1.0.0');
  });

  it('offers no agent at all when that kind\'s binary is not published yet', () => {
    // An empty result is the safe outcome: the agent reads "no update".
    const { 'adsb-agent': _drop, ...withoutAdsb } = FULL;
    void _drop;
    expect(manifestForKind(withoutAdsb, 'adsb')).toEqual({});
  });

  it('treats an unknown kind as radio rather than leaking per-kind agents', () => {
    const m = manifestForKind(FULL, 'something-new');
    expect(m['pager-agent']).toBeUndefined();
    expect(m['adsb-agent']).toBeUndefined();
  });
});
