/**
 * The 3D coverage view: terrain line of sight, and the heard / missed cells.
 *
 * The terrain maths is checked against cases with a known answer (flat ground
 * matches the radio horizon; a ridge cuts low slices short and leaves high
 * ones alone). The cells are checked for the one rule that makes "missed"
 * mean anything: an aircraft is only missed when the receiver has not
 * reported it AND it was inside the line-of-sight limit.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));

const {
  bearingLimits, horizonKm, destination, PROFILE_KM, SLICES, SLICE_M,
} = await import('../../../src/services/nodes/adsbTerrain.js');
const {
  sliceOf, cellOf, foldLightcone, noteNetworkAircraft, adsbLightconeFor,
  flushAdsbLightcone, _resetAdsbLightcone, RINGS,
} = await import('../../../src/services/nodes/adsbLightcone.js');

const SITE = { lat: -33.8688, lon: 151.2093 };
const NODE = 'node-lc-1';

function ac(hex: string, lat: number, lon: number, altFt: number, extra: Record<string, unknown> = {}) {
  return {
    hex, callsign: null, reg: null, type: null, lat, lon, altFt, onGround: false,
    gsKt: null, trackDeg: null, category: null, squawk: null, emergencySquawk: false,
    esTag: null, ageSec: 1, sourceCount: 1, sources: ['x'], estimated: false, estimatedSec: null,
    ...extra,
  } as never;
}

describe('terrain line of sight', () => {
  it('matches the smooth-earth horizon over flat ground', () => {
    const flat = PROFILE_KM.map(() => 0);
    const lim = bearingLimits(0, 10, flat);
    expect(lim).toHaveLength(SLICES);
    for (let k = 0; k < SLICES; k += 1) {
      const want = horizonKm(10, (k + 1) * SLICE_M);
      expect(Math.abs(lim[k]! - want)).toBeLessThan(1);
    }
  });

  it('a ridge cuts low slices short and leaves high ones nearly untouched', () => {
    // A 200 m ridge 10 km out, flat elsewhere.
    const ridge = PROFILE_KM.map((d) => (d === 10 ? 200 : 0));
    const lim = bearingLimits(0, 10, ridge);
    const flat = bearingLimits(0, 10, PROFILE_KM.map(() => 0));
    expect(lim[0]!).toBeLessThan(flat[0]! * 0.5);      // 500 m: well short
    expect(lim[SLICES - 1]!).toBeGreaterThan(flat[SLICES - 1]! * 0.7);
    // And a ridge higher than the slice hides it entirely past the ridge.
    const wall = bearingLimits(0, 10, PROFILE_KM.map((d) => (d === 10 ? 800 : 0)));
    expect(wall[0]!).toBeLessThanOrEqual(10);
  });

  it('limits never grow as the ground rises in front', () => {
    const hill = PROFILE_KM.map((d) => (d < 20 ? d * 20 : 400));
    const lim = bearingLimits(0, 10, hill);
    for (let k = 1; k < SLICES; k += 1) expect(lim[k]!).toBeGreaterThanOrEqual(lim[k - 1]!);
  });

  it('destination goes the right way', () => {
    const north = destination(SITE.lat, SITE.lon, 0, 111.2);
    expect(north.lat - SITE.lat).toBeCloseTo(1, 1);
    const east = destination(SITE.lat, SITE.lon, 90, 50);
    expect(east.lon).toBeGreaterThan(SITE.lon);
  });
});

describe('lightcone cells', () => {
  beforeEach(() => {
    _resetAdsbLightcone();
    queryMock.mockReset();
  });

  it('slices altitude in 500 m steps and caps at the top', () => {
    expect(sliceOf(null)).toBeNull();
    expect(sliceOf(0)).toBe(0);
    expect(sliceOf(1700)).toBe(1);      // 518 m
    expect(sliceOf(60_000)).toBe(SLICES - 1);
    expect(cellOf(1, 2, 3)).toBe((1 * 72 + 2) * RINGS + 3);
  });

  it('counts heard positions and only counts missed for unreported aircraft in range', async () => {
    const t = Date.now();
    const near = destination(SITE.lat, SITE.lon, 45, 50);
    const far = destination(SITE.lat, SITE.lon, 45, 550);
    // The receiver hears ABC twice (two uploads) at 10,000 ft.
    foldLightcone(NODE, SITE.lat, SITE.lon, 10, [ac('abc', near.lat, near.lon, 10_000)], new Map(), t);
    foldLightcone(NODE, SITE.lat, SITE.lon, 10, [ac('abc', near.lat, near.lon, 10_000)], new Map(), t + 5_000);
    // The public feed sees ABC (heard: not missed), DEF nearby (missed), and
    // GHI far beyond the horizon at 3,000 ft (not expected: not missed).
    const feed = [
      ac('abc', near.lat, near.lon, 10_000),
      ac('def', near.lat, near.lon, 20_000),
      ac('ghi', far.lat, far.lon, 3_000),
    ];
    noteNetworkAircraft(feed, t + 6_000);
    noteNetworkAircraft(feed, t + 20_000);

    queryMock.mockResolvedValueOnce({ rows: [] });
    const v = await adsbLightconeFor(NODE, 30, t + 21_000);
    expect(v).not.toBeNull();
    expect(v!.heard).toHaveLength(1);
    expect(v!.heardN[0]).toBe(2);
    expect(v!.missed).toHaveLength(1);
    const slice = Math.floor(v!.missed[0]! / (72 * RINGS));
    expect(slice).toBe(sliceOf(20_000));
  });

  it('ignores a receiver that has stopped uploading', async () => {
    const t = Date.now();
    const near = destination(SITE.lat, SITE.lon, 90, 40);
    foldLightcone(NODE, SITE.lat, SITE.lon, 10, [], new Map(), t);
    const feed = [ac('def', near.lat, near.lon, 20_000)];
    noteNetworkAircraft(feed, t + 120_000);
    noteNetworkAircraft(feed, t + 140_000);
    queryMock.mockResolvedValueOnce({ rows: [] });
    const v = await adsbLightconeFor(NODE, 30, t + 141_000);
    expect(v!.missed).toHaveLength(0);
  });

  it('flush folds the stored day in once, then replaces', async () => {
    const t = Date.now();
    const near = destination(SITE.lat, SITE.lon, 180, 30);
    foldLightcone(NODE, SITE.lat, SITE.lon, 10, [ac('abc', near.lat, near.lon, 5_000)], new Map(), t);
    const cell = cellOf(sliceOf(5_000)!, 36, 3);
    queryMock
      .mockResolvedValueOnce({ rows: [{ heard: { [cell]: 4 }, missed: null }] })
      .mockResolvedValueOnce({ rows: [] });
    await flushAdsbLightcone();
    const upsert = queryMock.mock.calls[1]!;
    expect(String(upsert[0])).toContain('ON CONFLICT');
    const heard = JSON.parse(upsert[1][3] as string) as Record<string, number>;
    expect(Object.values(heard).reduce((a, b) => a + b, 0)).toBe(5);
  });

  it('writes nothing when nothing changed, and a busy day at most every ten minutes', async () => {
    // Each write replaces two large JSONB values. Every minute, changed or
    // not, that was the table's whole churn.
    // Midday in Sydney, so no step below crosses the day boundary.
    const t = Date.UTC(2026, 5, 10, 2, 0, 0);
    const near = destination(SITE.lat, SITE.lon, 180, 30);
    const fold = (at: number) =>
      foldLightcone(NODE, SITE.lat, SITE.lon, 10, [ac('abc', near.lat, near.lon, 5_000)], new Map(), at);
    queryMock.mockResolvedValue({ rows: [] });
    fold(t);
    expect(await flushAdsbLightcone(false, t)).toBe(1);
    // Nothing new: no write, however long it has been.
    expect(await flushAdsbLightcone(false, t + 3_600_000)).toBe(0);
    // Changed a minute later: held back.
    fold(t + 60_000);
    expect(await flushAdsbLightcone(false, t + 61_000)).toBe(0);
    // ...unless shutting down, or ten minutes have passed.
    expect(await flushAdsbLightcone(true, t + 62_000)).toBe(1);
    fold(t + 120_000);
    expect(await flushAdsbLightcone(false, t + 62_000 + 601_000)).toBe(1);
  });
});
