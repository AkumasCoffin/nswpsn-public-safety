// The coverage picture: which paths a receiver actually heard, over the last
// hour. Held in memory and decimated, so the tests that matter are about what
// gets kept, what gets thrown away, and that neither grows without bound.

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  recordNodeAdsbSnapshot,
  nodeAdsbTraces,
  adsbNodeSourceId,
  _resetAdsbNodeStore,
} from '../../../src/services/nodes/adsbNodeStore.js';
import { normalizeNodeUpload } from '../../../src/sources/adsb.js';

const NODE = 'node-trace-1';

/** Feed one snapshot in, as the ingest route would. */
function feed(aircraft: Array<{ hex: string; lat: number; lon: number; flight?: string }>) {
  const upload = {
    at: new Date().toISOString(),
    aircraft: aircraft.map((a) => ({ ...a, seen_pos: 1 })),
  };
  recordNodeAdsbSnapshot(
    NODE,
    'adsb-test',
    normalizeNodeUpload(upload, adsbNodeSourceId(NODE, 'adsb-test')),
  );
}

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
    expect(traces[0]!.points[0]).toEqual([-33.0, 151.0]);
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

  it('decimates a receiver reporting every 5 seconds', () => {
    // 12 snapshots a minute for an aircraft barely moving must not become 12
    // points a minute. The window is 8 hours, so this is the one structure
    // that grows with traffic, and a coverage picture needs nothing like that
    // resolution — a point a minute is ample.
    feed([{ hex: 'ccc333', lat: -33.0, lon: 151.0 }]);
    for (let i = 0; i < 120; i += 1) {
      vi.advanceTimersByTime(5_000);
      feed([{ hex: 'ccc333', lat: -33.0 + i * 0.00001, lon: 151.0 }]);
    }
    // 600s of snapshots (120 of them) at a 60s minimum gap.
    const pts = nodeAdsbTraces(NODE, 480).traces[0]!.points;
    expect(pts.length).toBeGreaterThan(8);
    expect(pts.length).toBeLessThanOrEqual(13);
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
    // ...and a shorter window still trims to itself.
    expect(nodeAdsbTraces(NODE, 60).traces).toHaveLength(0);
  });

  it('forgets aircraft older than the eight-hour window', () => {
    feed([{ hex: 'bbb888', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(9 * 60 * 60_000);
    expect(nodeAdsbTraces(NODE, 480).aircraft).toBe(0);
  });

  it('records a fast mover sooner than the time gap', () => {
    // A jet crossing the coverage would otherwise be drawn as a few long
    // straight hops, cutting corners the aircraft never flew.
    feed([{ hex: 'ddd444', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(5_000);
    feed([{ hex: 'ddd444', lat: -33.5, lon: 151.0 }]);
    expect(nodeAdsbTraces(NODE, 480).traces[0]!.points).toHaveLength(2);
  });

  it('drops points older than the requested window', () => {
    feed([{ hex: 'eee555', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(30 * 60_000);
    feed([{ hex: 'eee555', lat: -33.5, lon: 151.5 }]);

    // A 10-minute window keeps only the recent point, so this is a dot, not a
    // path — and single-point traces are not drawn.
    expect(nodeAdsbTraces(NODE, 10).traces).toHaveLength(0);
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

  it('counts a single-point aircraft but does not draw it', () => {
    // One catch is a dot, not a path; hundreds of them would bury the picture.
    feed([{ hex: '111aaa', lat: -33.0, lon: 151.0 }]);
    const t = nodeAdsbTraces(NODE, 60);
    expect(t.aircraft).toBe(1);
    expect(t.traces).toHaveLength(0);
  });

  it('caps points per aircraft rather than growing without bound', () => {
    feed([{ hex: '222bbb', lat: -33.0, lon: 151.0 }]);
    // Large jumps: every one qualifies on distance, so only the cap stops this
    // accumulating forever. Models something pathological — a stuck position,
    // or a ground vehicle parked in view — not real traffic.
    for (let i = 0; i < 400; i += 1) {
      vi.advanceTimersByTime(20_000);
      feed([{ hex: '222bbb', lat: -33.0 + (i % 2 ? 0.5 : -0.5), lon: 151.0 }]);
    }
    const pts = nodeAdsbTraces(NODE, 480).traces[0]!.points;
    expect(pts.length).toBeLessThanOrEqual(240);
  });

  it('keeps each receiver separate', () => {
    feed([{ hex: '333ccc', lat: -33.0, lon: 151.0 }]);
    vi.advanceTimersByTime(30_000);
    feed([{ hex: '333ccc', lat: -33.2, lon: 151.2 }]);
    expect(nodeAdsbTraces(NODE, 60).traces).toHaveLength(1);
    expect(nodeAdsbTraces('some-other-node', 60).traces).toHaveLength(0);
  });
});
