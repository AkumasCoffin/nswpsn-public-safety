// The coverage picture: which paths a receiver actually heard, over the last
// hour. Held in memory and decimated, so the tests that matter are about what
// gets kept, what gets thrown away, and that neither grows without bound.

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  recordNodeAdsbSnapshot,
  recordNodeAdsbReception,
  nodeAdsbRecords,
  nodeAdsbObservedRangeKm,
  nodeAdsbTraces,
  adsbNodeSourceId,
  _resetAdsbNodeStore,
  _adsbTraceSizeForTests,
} from '../../../src/services/nodes/adsbNodeStore.js';
import { normalizeNodeUpload } from '../../../src/sources/adsb.js';

const NODE = 'node-trace-1';

/** Feed one snapshot in, as the ingest route would. */
function feed(aircraft: Array<{ hex: string; lat: number; lon: number; flight?: string }>) {
  const upload = {
    at: new Date().toISOString(),
    aircraft: aircraft.map((a) => ({ ...a, seen_pos: 1 })),
  };
  const records = normalizeNodeUpload(upload, adsbNodeSourceId(NODE, 'adsb-test'));
  // Mirrors the ingest route: reception (traces, range) is recorded before the
  // feed gate, the live snapshot after it.
  recordNodeAdsbReception(NODE, records);
  recordNodeAdsbSnapshot(NODE, 'adsb-test', records);
}

describe('a paused feed leaves the receiver diagnostics intact', () => {
  // The reported fault: an ADS-B node's coverage map showed the envelope ring
  // but no tracks at all.
  //
  // recordTraces lived inside recordNodeAdsbSnapshot, which sits AFTER the
  // feed gate, while the coverage envelope and the daily counters are folded
  // in before it. So a receiver with its feed paused accumulated a ring with
  // nothing inside it — which reads as a broken map rather than a paused feed.
  //
  // Traces are the receiver's own record, not something it publishes. Only the
  // live snapshot belongs behind the gate.
  beforeEach(() => {
    _resetAdsbNodeStore();
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-17T00:00:00Z'));
  });
  afterEach(() => vi.useRealTimers());

  /** An upload arriving at a node whose feed is OFF: reception only. */
  function receiveUnfed(
    aircraft: Array<{ hex: string; lat: number; lon: number }>,
  ): void {
    const records = normalizeNodeUpload(
      { at: new Date().toISOString(), aircraft: aircraft.map((a) => ({ ...a, seen_pos: 1 })) },
      adsbNodeSourceId(NODE, 'adsb-test'),
    );
    recordNodeAdsbReception(NODE, records);
  }

  it('records tracks even while the feed is off', () => {
    receiveUnfed([{ hex: 'aaa111', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(90_000);
    receiveUnfed([{ hex: 'aaa111', lat: -33.3, lon: 151.3 }]);

    const t = nodeAdsbTraces(NODE, 480);
    expect(t.aircraft).toBe(1);
    expect(t.traces).toHaveLength(1);
    expect(t.traces[0]!.points.length).toBeGreaterThanOrEqual(2);
  });

  it('keeps that node off the live map, which is what the gate is for', () => {
    receiveUnfed([{ hex: 'aaa111', lat: -33.0, lon: 151.0 }]);
    expect(nodeAdsbRecords()).toHaveLength(0);
  });

  it('still reports range now, which has no snapshot to hang off', () => {
    // The liveness guard used to key off the presence of a live snapshot, so a
    // paused node reported no range even though it had just measured one.
    const records = normalizeNodeUpload(
      { at: new Date().toISOString(),
        aircraft: [{ hex: 'aaa111', lat: -33.0, lon: 151.0, seen_pos: 1 }] },
      adsbNodeSourceId(NODE, 'adsb-test'),
    );
    recordNodeAdsbReception(NODE, records, { minKm: 18.2, maxKm: 212.5 });
    expect(nodeAdsbObservedRangeKm(NODE)).toBe(212.5);
  });

  it('but range now still expires when the node goes quiet', () => {
    const records = normalizeNodeUpload(
      { at: new Date().toISOString(),
        aircraft: [{ hex: 'aaa111', lat: -33.0, lon: 151.0, seen_pos: 1 }] },
      adsbNodeSourceId(NODE, 'adsb-test'),
    );
    recordNodeAdsbReception(NODE, records, 212.5);
    vi.advanceTimersByTime(5 * 60_000);
    expect(nodeAdsbObservedRangeKm(NODE)).toBeNull();
  });
});

describe('adsb traces', () => {
  beforeEach(() => {
    _resetAdsbNodeStore();
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-16T00:00:00Z'));
  });
  afterEach(() => vi.useRealTimers());

  it('builds a path from successive positions', () => {
    feed([{ hex: 'aaa111', lat: -33.0, lon: 151.0, flight: 'QFA1' }]);
    vi.advanceTimersByTime(30_000);
    feed([{ hex: 'aaa111', lat: -33.1, lon: 151.1 }]);
    vi.advanceTimersByTime(30_000);
    feed([{ hex: 'aaa111', lat: -33.2, lon: 151.2 }]);

    const { traces, aircraft } = nodeAdsbTraces(NODE, 60);
    expect(aircraft).toBe(1);
    expect(traces).toHaveLength(1);
    expect(traces[0]!.points).toHaveLength(3);
    // [epochMs, lat, lon] — the time is what lets the view dedupe a live point
    // against the same point already flushed to disk.
    expect(traces[0]!.points[0]!.slice(1)).toEqual([-33.0, 151.0]);
    expect(traces[0]!.points[0]![0]).toBeLessThan(traces[0]!.points[2]![0]);
    // The callsign arrives on the first report here, but often does not — it is
    // kept once seen rather than overwritten with null by later reports.
    expect(traces[0]!.callsign).toBe('QFA1');
  });

  it('keeps a callsign first seen part-way through a track', () => {
    feed([{ hex: 'bbb222', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(30_000);
    feed([{ hex: 'bbb222', lat: -33.1, lon: 151.1, flight: 'JST9 ' }]);
    vi.advanceTimersByTime(30_000);
    feed([{ hex: 'bbb222', lat: -33.2, lon: 151.2 }]);
    expect(nodeAdsbTraces(NODE, 60).traces[0]!.callsign).toBe('JST9');
  });

  it('does not store a point for an aircraft going nowhere', () => {
    // 12 uploads a minute for an aircraft barely moving must not become 12
    // points a minute. Movement, not the clock, is what earns a point — this
    // one drifts about a metre per upload and never travels far enough, so
    // only the idle rule fires and it records presence rather than a path.
    feed([{ hex: 'ccc333', lat: -33.0, lon: 151.0 }]);
    for (let i = 0; i < 120; i += 1) {
      vi.advanceTimersByTime(5_000);
      feed([{ hex: 'ccc333', lat: -33.0 + i * 0.00001, lon: 151.0 }]);
    }
    // 600s at the 120s idle gap: five or six points, not 120.
    const pts = nodeAdsbTraces(NODE, 480).traces[0]!.points;
    expect(pts.length).toBeLessThanOrEqual(7);
    expect(pts.length).toBeGreaterThanOrEqual(4);
  });

  it('keeps every upload for an aircraft that is actually moving', () => {
    // The reported fault: tracks drawn as long straight hops. A transponder
    // reports twice a second and the agent samples every five, so five seconds
    // is the finest resolution that can reach us and it must not be thrown
    // away — at cruise a minute between points is a thirteen-kilometre chord
    // straight through whatever turn the aircraft actually flew.
    feed([{ hex: 'ccc444', lat: -33.0, lon: 151.0 }]);
    for (let i = 1; i <= 20; i += 1) {
      vi.advanceTimersByTime(5_000);
      feed([{ hex: 'ccc444', lat: -33.0 + i * 0.012, lon: 151.0 }]);
    }
    expect(nodeAdsbTraces(NODE, 480).traces[0]!.points).toHaveLength(21);
  });

  it('measures movement as distance, not per axis', () => {
    // The actual bug. The old test was two thresholds, one per axis, so an
    // aircraft flying due north tripped it in one step while the SAME aircraft
    // at the same speed heading north-east split its movement between the two,
    // tripped neither, and fell back on the idle rule. Diagonal tracks were
    // the chunky ones.
    feed([{ hex: 'ccc555', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(5_000);
    // ~0.8 km on each axis: under a per-axis 1 km threshold, over it as a
    // diagonal distance.
    feed([{ hex: 'ccc555', lat: -33.0 + 0.0072, lon: 151.0 + 0.0086 }]);
    expect(nodeAdsbTraces(NODE, 480).traces[0]!.points).toHaveLength(2);
  });

  it('keeps eight hours, because an hour cannot show coverage', () => {
    // Coverage is about which bearings a receiver hears and how far. One quiet
    // afternoon hour looks identical to a broken antenna; it takes a shift's
    // worth of traffic before the shape means anything.
    feed([{ hex: 'aaa777', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(7 * 60 * 60_000);
    feed([{ hex: 'aaa777', lat: -34.0, lon: 152.0 }]);

    const eight = nodeAdsbTraces(NODE, 480);
    expect(eight.traces[0]!.points).toHaveLength(2);
    // ...and a shorter window still trims to itself: one point left, not two.
    expect(nodeAdsbTraces(NODE, 60).traces[0]!.points).toHaveLength(1);
  });

  it('forgets aircraft older than the eight-hour window', () => {
    feed([{ hex: 'bbb888', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(9 * 60 * 60_000);
    expect(nodeAdsbTraces(NODE, 480).aircraft).toBe(0);
  });

  it('records a fast mover on the very next upload', () => {
    // A jet crossing the coverage would otherwise be drawn as a few long
    // straight hops, cutting corners the aircraft never flew.
    feed([{ hex: 'ddd444', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(5_000);
    feed([{ hex: 'ddd444', lat: -33.5, lon: 151.0 }]);
    expect(nodeAdsbTraces(NODE, 480).traces[0]!.points).toHaveLength(2);
  });

  it('will not store two points for the same instant', () => {
    // Two uploads landing in the same second is a retry or a double-poll, not
    // motion, however far apart the positions claim to be.
    feed([{ hex: 'ddd555', lat: -33.0, lon: 151.0 }]);
    feed([{ hex: 'ddd555', lat: -33.5, lon: 151.0 }]);
    expect(nodeAdsbTraces(NODE, 480).traces[0]!.points).toHaveLength(1);
  });

  it('drops points older than the requested window', () => {
    feed([{ hex: 'eee555', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(30 * 60_000);
    feed([{ hex: 'eee555', lat: -33.5, lon: 151.5 }]);

    // A 10-minute window keeps only the recent point. It is still reported
    // here — a lone live point may be the newest leg of a flight whose earlier
    // path is on disk, so only the view, holding both halves, can decide it is
    // a dot rather than a path.
    expect(nodeAdsbTraces(NODE, 10).traces[0]!.points).toHaveLength(1);
    expect(nodeAdsbTraces(NODE, 10).aircraft).toBe(1);
    // The full window still has both.
    expect(nodeAdsbTraces(NODE, 480).traces[0]!.points).toHaveLength(2);
  });

  it('reports nothing for a window with no points in it', () => {
    feed([{ hex: 'fff666', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(61 * 60_000);
    const t = nodeAdsbTraces(NODE, 60);
    expect(t.aircraft).toBe(0);
    expect(t.points).toBe(0);
  });

  it('reports a single-point aircraft and leaves the drawing decision upstream', () => {
    // One catch is a dot, not a path, and hundreds of them would bury the
    // picture — but that filter moved to adsbNodeTracks, which is the only
    // caller that can tell a dot from the live tip of a stored track.
    feed([{ hex: '111aaa', lat: -33.0, lon: 151.0 }]);
    const t = nodeAdsbTraces(NODE, 60);
    expect(t.aircraft).toBe(1);
    expect(t.traces).toHaveLength(1);
    expect(t.traces[0]!.points).toHaveLength(1);
  });

  it('caps points per aircraft rather than growing without bound', () => {
    feed([{ hex: '222bbb', lat: -33.0, lon: 151.0 }]);
    // Large jumps: every one qualifies on distance, so only the cap stops this
    // accumulating forever. Models something pathological — a stuck position,
    // or a ground vehicle parked in view — not real traffic.
    for (let i = 0; i < 900; i += 1) {
      vi.advanceTimersByTime(20_000);
      feed([{ hex: '222bbb', lat: -33.0 + (i % 2 ? 0.5 : -0.5), lon: 151.0 }]);
    }
    const pts = nodeAdsbTraces(NODE, 480).traces[0]!.points;
    expect(pts.length).toBeLessThanOrEqual(720);
  });

  it('keeps each receiver separate', () => {
    feed([{ hex: '333ccc', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(30_000);
    feed([{ hex: '333ccc', lat: -33.2, lon: 151.2 }]);
    expect(nodeAdsbTraces(NODE, 60).traces).toHaveLength(1);
    expect(nodeAdsbTraces('some-other-node', 60).traces).toHaveLength(0);
  });
});

describe('memory tracks the window, not the caps', () => {
  // A receiver nobody is looking at is the common case, and it was the one
  // that kept the most: the prune ran only from the two READ paths, so with
  // nothing reading, points never aged out and a node sat at its CAPS —
  // 4000 aircraft x 720 points x three arrays — instead of at its 8-hour
  // window. Several such nodes is hundreds of megabytes of live heap held for
  // no reason on a process with a ~2GB ceiling.
  beforeEach(() => {
    _resetAdsbNodeStore();
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('ages points out without anything reading the node', () => {
    vi.setSystemTime(new Date('2026-09-21T00:00:00Z'));
    feed([{ hex: 'aaa111', lat: -33.86, lon: 151.20 }]);

    // Nine hours later, past the 8-hour window. Move it far enough each time
    // that the distance rule keeps the point.
    for (let i = 1; i <= 9; i += 1) {
      vi.setSystemTime(new Date(`2026-09-21T0${i}:00:00Z`));
      feed([{ hex: 'aaa111', lat: -33.86 + i * 0.05, lon: 151.20 }]);
    }

    // Measured WITHOUT reading through a public accessor. Both of those
    // prune on the way out, so a test that went through one would pass
    // whether or not the write path prunes — which is the regression.
    const held = _adsbTraceSizeForTests(NODE);
    // Ten feeds an hour apart; only those inside the eight-hour window may
    // still be held. Before the fix all ten were, for ever.
    expect(held.points).toBeLessThanOrEqual(9);
    expect(held.aircraft).toBe(1);
  });

  it('drops an aircraft that has not been heard for a window', () => {
    vi.setSystemTime(new Date('2026-09-21T00:00:00Z'));
    feed([{ hex: 'bbb222', lat: -33.86, lon: 151.20 }]);
    expect(nodeAdsbTraces(NODE, 540).traces.length).toBe(1);

    // A different aircraft keeps the node's write path alive; the first one is
    // never heard again and must not be held for ever.
    for (let i = 1; i <= 9; i += 1) {
      vi.setSystemTime(new Date(`2026-09-21T0${i}:00:00Z`));
      feed([{ hex: 'ccc333', lat: -34.00 + i * 0.05, lon: 151.50 }]);
    }
    // Again without reading through a pruning accessor.
    expect(_adsbTraceSizeForTests(NODE).aircraft).toBe(1);
    const hexes = nodeAdsbTraces(NODE, 540).traces.map((t) => t.hex);
    expect(hexes).not.toContain('bbb222');
    expect(hexes).toContain('ccc333');
  });
});
