// What a receiver heard, and why an upload was refused.
//
// Both are in-memory rings whose whole design problem is the 5-second upload
// cadence: recorded naively, a healthy node's own success messages would flush
// the last real fault out of the buffer in about three minutes. So the tests
// that matter here are the ones about what is deliberately NOT recorded.

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  recordNodeAdsbSnapshot,
  nodeAdsbRecentAircraft,
  recordAdsbIngestOutcome,
  nodeAdsbIssues,
  recordAdsbAuthFailure,
  adsbAuthFailures,
  clearAdsbNodeState,
  adsbNodeSourceId,
  nodeAdsbTraces,
  ADSB_RECENT_MAX,
  _resetAdsbNodeStore,
} from '../../../src/services/nodes/adsbNodeStore.js';
import { normalizeNodeUpload } from '../../../src/sources/adsb.js';

const NODE = 'node-logs-1';

function feed(
  aircraft: Array<{ hex: string; lat: number; lon: number; flight?: string; alt_baro?: number }>,
) {
  recordNodeAdsbSnapshot(
    NODE,
    'adsb-test',
    normalizeNodeUpload(
      { at: new Date().toISOString(), aircraft: aircraft.map((a) => ({ ...a, seen_pos: 1 })) },
      adsbNodeSourceId(NODE, 'adsb-test'),
    ),
  );
}

describe('recent aircraft', () => {
  beforeEach(() => {
    _resetAdsbNodeStore();
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-16T00:00:00Z'));
  });
  afterEach(() => vi.useRealTimers());

  it('counts reports even where it declines to store a point', () => {
    // The whole reason `reports` exists. An aircraft holding still is deduped
    // down to one point, but it was heard every single upload — and a point
    // count alone would read as a receiver that barely caught it.
    feed([{ hex: 'aaa111', lat: -33.0, lon: 151.0 }]);
    for (let i = 0; i < 9; i += 1) {
      vi.advanceTimersByTime(5_000);
      feed([{ hex: 'aaa111', lat: -33.0, lon: 151.0 }]);
    }
    const [a] = nodeAdsbRecentAircraft(NODE, 10);
    expect(a!.reports).toBe(10);
    expect(a!.points).toBe(1);
  });

  it('keeps the highest altitude after a descent', () => {
    feed([{ hex: 'bbb222', lat: -33.0, lon: 151.0, alt_baro: 12000 }]);
    vi.advanceTimersByTime(60_000);
    feed([{ hex: 'bbb222', lat: -33.4, lon: 151.4, alt_baro: 34000 }]);
    vi.advanceTimersByTime(60_000);
    feed([{ hex: 'bbb222', lat: -33.8, lon: 151.8, alt_baro: 9000 }]);
    const [a] = nodeAdsbRecentAircraft(NODE, 10);
    expect(a!.maxAltFt).toBe(34000);
    expect(a!.lastAltFt).toBe(9000);
  });

  it('takes first-heard from the points, so a pruned prefix moves it', () => {
    // A cached firstMs would still claim 00:00 here, eight hours after the
    // point it referred to was thrown away.
    feed([{ hex: 'ccc333', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(7 * 60 * 60 * 1000);
    feed([{ hex: 'ccc333', lat: -34.0, lon: 152.0 }]);
    const early = nodeAdsbRecentAircraft(NODE, 10)[0]!;
    expect(new Date(early.firstMs).toISOString()).toBe('2026-09-16T00:00:00.000Z');

    vi.advanceTimersByTime(2 * 60 * 60 * 1000);
    const late = nodeAdsbRecentAircraft(NODE, 10)[0]!;
    expect(new Date(late.firstMs).toISOString()).toBe('2026-09-16T07:00:00.000Z');
  });

  it('drops an aircraft that has aged out of the window entirely', () => {
    feed([{ hex: 'ddd444', lat: -33.0, lon: 151.0 }]);
    expect(nodeAdsbRecentAircraft(NODE, 10)).toHaveLength(1);
    vi.advanceTimersByTime(9 * 60 * 60 * 1000);
    expect(nodeAdsbRecentAircraft(NODE, 10)).toHaveLength(0);
  });

  it('sorts newest first and clamps the limit', () => {
    feed([{ hex: 'e00001', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(60_000);
    feed([{ hex: 'e00002', lat: -33.5, lon: 151.5 }]);
    expect(nodeAdsbRecentAircraft(NODE, 10).map((a) => a.hex)).toEqual(['e00002', 'e00001']);
    expect(nodeAdsbRecentAircraft(NODE, 1)).toHaveLength(1);
    // A caller cannot ask for the whole 4000-aircraft window as JSON.
    expect(nodeAdsbRecentAircraft(NODE, 99_999)).toHaveLength(2);
    expect(ADSB_RECENT_MAX).toBe(100);
  });
});

describe('ingest issues', () => {
  beforeEach(() => {
    _resetAdsbNodeStore();
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-16T00:00:00Z'));
  });
  afterEach(() => vi.useRealTimers());

  it('does not let a healthy node flush its own fault history', () => {
    // 200 uploads at the real 5s cadence is nearly 17 minutes — long enough to
    // cross the heartbeat once, and that is the only new entry allowed.
    for (let i = 0; i < 200; i += 1) {
      recordAdsbIngestOutcome(NODE, 'ok');
      vi.advanceTimersByTime(5_000);
    }
    const log = nodeAdsbIssues(NODE);
    expect(log.length).toBeLessThanOrEqual(2);
    expect(log.every((e) => e.outcome === 'ok')).toBe(true);
    expect(log.reduce((n, e) => n + e.count, 0)).toBe(200);
  });

  it('coalesces a failure loop into one entry with a count', () => {
    for (let i = 0; i < 100; i += 1) {
      recordAdsbIngestOutcome(NODE, 'rate_limited');
      vi.advanceTimersByTime(500);
    }
    const log = nodeAdsbIssues(NODE);
    expect(log).toHaveLength(1);
    expect(log[0]!.count).toBe(100);
    expect(log[0]!.lastMs - log[0]!.firstMs).toBe(99 * 500);
  });

  it('reads a fault and its recovery as exactly two entries', () => {
    recordAdsbIngestOutcome(NODE, 'ok');
    vi.advanceTimersByTime(5_000);
    recordAdsbIngestOutcome(NODE, 'too_large', '900000 bytes, cap 512000');
    vi.advanceTimersByTime(5_000);
    recordAdsbIngestOutcome(NODE, 'ok');
    // Newest first.
    expect(nodeAdsbIssues(NODE).map((e) => e.outcome)).toEqual(['ok', 'too_large', 'ok']);
    expect(nodeAdsbIssues(NODE)[1]!.detail).toBe('900000 bytes, cap 512000');
  });

  it('starts a new episode when the same fault returns much later', () => {
    recordAdsbIngestOutcome(NODE, 'bad_body');
    vi.advanceTimersByTime(10 * 60_000);
    recordAdsbIngestOutcome(NODE, 'bad_body');
    expect(nodeAdsbIssues(NODE)).toHaveLength(2);
  });

  it('caps the ring at 40', () => {
    for (let i = 0; i < 100; i += 1) {
      recordAdsbIngestOutcome(NODE, i % 2 ? 'bad_body' : 'feed_off');
      vi.advanceTimersByTime(1_000);
    }
    expect(nodeAdsbIssues(NODE)).toHaveLength(40);
  });

  it('keeps one node out of another node’s log', () => {
    recordAdsbIngestOutcome(NODE, 'feed_off');
    recordAdsbIngestOutcome('other-node', 'bad_body');
    expect(nodeAdsbIssues(NODE).map((e) => e.outcome)).toEqual(['feed_off']);
  });
});

describe('auth failures', () => {
  beforeEach(() => {
    _resetAdsbNodeStore();
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-16T00:00:00Z'));
  });
  afterEach(() => vi.useRealTimers());

  it('records the install id and never the token', () => {
    recordAdsbAuthFailure('7f3a90c1-2b44-4e10-9a77-0d5c1e2f3b48', 'bad_token');
    const [f] = adsbAuthFailures();
    expect(f!.install).toBe('7f3a90c1');
    expect(JSON.stringify(f)).not.toContain('token=');
    expect(f!.reason).toBe('bad_token');
  });

  it('separates two machines, and coalesces one machine retrying', () => {
    for (let i = 0; i < 20; i += 1) {
      recordAdsbAuthFailure('aaaaaaaa-1111', 'bad_token');
      vi.advanceTimersByTime(1_000);
    }
    recordAdsbAuthFailure('bbbbbbbb-2222', 'no_role');
    const log = adsbAuthFailures();
    expect(log).toHaveLength(2);
    expect(log[0]!.install).toBe('bbbbbbbb');
    expect(log[1]!.count).toBe(20);
  });

  it('caps at 25 and tolerates a missing header', () => {
    for (let i = 0; i < 40; i += 1) {
      // Distinct in the first 8 characters, because that is all the ring keeps
      // — `install-0` and `install-1` are the same machine as far as it knows.
      recordAdsbAuthFailure(`${String(i).padStart(8, '0')}-rest`, 'bad_token');
    }
    expect(adsbAuthFailures()).toHaveLength(25);
    recordAdsbAuthFailure(null, 'bad_token');
    expect(adsbAuthFailures()[0]!.install).toBe('unknown');
  });
});

describe('clearAdsbNodeState', () => {
  beforeEach(() => {
    _resetAdsbNodeStore();
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-16T00:00:00Z'));
  });
  afterEach(() => vi.useRealTimers());

  it('forgets a deleted node’s snapshot, traces and issues at once', () => {
    feed([{ hex: 'fff555', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(60_000);
    feed([{ hex: 'fff555', lat: -33.5, lon: 151.5 }]);
    recordAdsbIngestOutcome(NODE, 'feed_off');
    expect(nodeAdsbTraces(NODE, 60).aircraft).toBe(1);

    clearAdsbNodeState(NODE);

    expect(nodeAdsbTraces(NODE, 60).aircraft).toBe(0);
    expect(nodeAdsbRecentAircraft(NODE, 10)).toHaveLength(0);
    expect(nodeAdsbIssues(NODE)).toHaveLength(0);
  });
});
