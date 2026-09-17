/**
 * Max range, measured server-side.
 *
 * dump1090 computes a range statistic of its own, but only when it was handed
 * --lat/--lon, and the agent can only forward what the decoder put in
 * stats.json. When any link in that chain is missing the figure is simply
 * absent — which is what left every range cell on the Data tab reading "—"
 * while the coverage map showed aircraft two hundred kilometres out.
 *
 * The backend holds both halves anyway: the receiver's pin, and the positions
 * that receiver just reported. These tests are about measuring from those, and
 * about still preferring the decoder's number when it is larger.
 */
import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  distanceKm,
  snapshotMaxRangeKm,
  recordNodeAdsbSnapshot,
  nodeAdsbRecords,
  nodeAdsbObservedRangeKm,
  accumulateAdsbDaily,
  flushAdsbDaily,
  clearAdsbNodeState,
  adsbNodeSourceId,
  _resetAdsbNodeStore,
} from '../../../src/services/nodes/adsbNodeStore.js';
import { normalizeNodeUpload } from '../../../src/sources/adsb.js';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));

/** Sydney Airport, and a point almost exactly 100 km due north of it. */
const SITE = { lat: -33.9461, lon: 151.1772 };
const NODE = 'node-range-1';

beforeEach(() => {
  _resetAdsbNodeStore();
  queryMock.mockReset();
  queryMock.mockResolvedValue({ rows: [] });
});

describe('distanceKm', () => {
  it('measures a known separation', () => {
    // Sydney to Melbourne is ~713 km great-circle.
    const d = distanceKm(-33.8688, 151.2093, -37.8136, 144.9631);
    expect(d).toBeGreaterThan(700);
    expect(d).toBeLessThan(725);
  });

  it('is zero at the same point, and symmetric', () => {
    expect(distanceKm(SITE.lat, SITE.lon, SITE.lat, SITE.lon)).toBe(0);
    const a = distanceKm(-33, 151, -34, 152);
    const b = distanceKm(-34, 152, -33, 151);
    expect(a).toBeCloseTo(b, 9);
  });

  it('gets a degree of latitude right', () => {
    // One degree of latitude is ~111.19 km anywhere on the globe.
    expect(distanceKm(-33, 151, -34, 151)).toBeCloseTo(111.19, 1);
  });
});

describe('snapshotMaxRangeKm', () => {
  it('returns the furthest aircraft in the upload', () => {
    const km = snapshotMaxRangeKm(SITE.lat, SITE.lon, [
      { lat: SITE.lat + 0.5, lon: SITE.lon },   // ~55 km
      { lat: SITE.lat - 1.8, lon: SITE.lon },   // ~200 km
      { lat: SITE.lat + 0.1, lon: SITE.lon },   // ~11 km
    ]);
    expect(km).toBeGreaterThan(195);
    expect(km).toBeLessThan(205);
  });

  it('is null without an antenna pin', () => {
    // Nothing to measure from. The UI already explains this as "no pin"
    // rather than showing it as a fault.
    expect(snapshotMaxRangeKm(null, null, [{ lat: -33, lon: 151 }])).toBeNull();
    expect(snapshotMaxRangeKm(undefined, undefined, [{ lat: -33, lon: 151 }])).toBeNull();
    // A numeric column that came back as a string is not a position either.
    expect(snapshotMaxRangeKm('-33.9' as unknown, '151.1' as unknown, [{ lat: -33, lon: 151 }]))
      .toBeNull();
  });

  it('is null for an empty sky, but zero is a real answer', () => {
    expect(snapshotMaxRangeKm(SITE.lat, SITE.lon, [])).toBeNull();
    expect(snapshotMaxRangeKm(SITE.lat, SITE.lon, [{ lat: SITE.lat, lon: SITE.lon }])).toBe(0);
  });

  it('skips records with an unusable position', () => {
    const km = snapshotMaxRangeKm(SITE.lat, SITE.lon, [
      { lat: Number.NaN, lon: 151 },
      { lat: SITE.lat + 0.5, lon: SITE.lon },
    ]);
    expect(km).toBeGreaterThan(50);
    expect(km).toBeLessThan(60);
  });
});

describe('range now', () => {
  function feed(rangeKm: number | null, aircraft: Array<{ hex: string; lat: number; lon: number }>) {
    recordNodeAdsbSnapshot(
      NODE, 'adsb-test',
      normalizeNodeUpload(
        { at: new Date().toISOString(), aircraft: aircraft.map((a) => ({ ...a, seen_pos: 1 })) },
        adsbNodeSourceId(NODE, 'adsb-test'),
      ),
      rangeKm,
    );
  }

  it('reports the latest upload, not a running peak', () => {
    // "Range now" is the point: a receiver whose range collapsed should show
    // that it has, and the day's peak lives in node_adsb_daily instead.
    feed(210, [{ hex: 'a1', lat: -33, lon: 151 }]);
    expect(nodeAdsbObservedRangeKm(NODE)).toBe(210);
    feed(40, [{ hex: 'a1', lat: -33.9, lon: 151.1 }]);
    expect(nodeAdsbObservedRangeKm(NODE)).toBe(40);
  });

  it('is null for a node that has not reported', () => {
    expect(nodeAdsbObservedRangeKm('never-seen')).toBeNull();
  });

  it('goes away with the snapshot it was measured from', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-17T00:00:00Z'));
    feed(210, [{ hex: 'a1', lat: -33, lon: 151 }]);
    expect(nodeAdsbObservedRangeKm(NODE)).toBe(210);

    // Past the snapshot TTL the node is no longer reporting anything, so a
    // stale range would be claiming current coverage it no longer has.
    vi.advanceTimersByTime(5 * 60_000);
    nodeAdsbRecords(Date.now());   // the sweep that expires stale snapshots
    expect(nodeAdsbObservedRangeKm(NODE)).toBeNull();
    vi.useRealTimers();
  });

  it('is forgotten when the node is deleted', () => {
    feed(210, [{ hex: 'a1', lat: -33, lon: 151 }]);
    clearAdsbNodeState(NODE);
    expect(nodeAdsbObservedRangeKm(NODE)).toBeNull();
  });
});

describe('the daily peak takes whichever is larger', () => {
  /** Flush and read back the max_range_km parameter that was written. */
  async function writtenRange(): Promise<number | null> {
    await flushAdsbDaily();
    const call = queryMock.mock.calls.find((c) => String(c[0]).includes('node_adsb_daily'));
    return call ? (call[1] as unknown[])[5] as number | null : null;
  }

  it('uses our measurement when the decoder reports none', () => {
    // The actual bug: stats arrive with a message rate and a track count but
    // no range at all, so every range cell read "—".
    accumulateAdsbDaily(NODE, 12, { msgRate: 8.2, tracksAll: 61 }, 187.4);
    return writtenRange().then((v) => expect(v).toBe(187.4));
  });

  it('keeps the decoder’s when it is larger', async () => {
    // It measures every position it DECODED, which can beat what reached us.
    accumulateAdsbDaily(NODE, 12, { maxRangeKm: 240 }, 187.4);
    expect(await writtenRange()).toBe(240);
  });

  it('keeps ours when it is larger', async () => {
    accumulateAdsbDaily(NODE, 12, { maxRangeKm: 90 }, 187.4);
    expect(await writtenRange()).toBe(187.4);
  });

  it('stays null when neither has one', async () => {
    accumulateAdsbDaily(NODE, 12, { msgRate: 8.2 }, null);
    expect(await writtenRange()).toBeNull();
  });

  it('holds the peak across a day of uploads', async () => {
    accumulateAdsbDaily(NODE, 12, null, 80);
    accumulateAdsbDaily(NODE, 12, null, 210);
    accumulateAdsbDaily(NODE, 12, null, 55);
    expect(await writtenRange()).toBe(210);
  });
});
