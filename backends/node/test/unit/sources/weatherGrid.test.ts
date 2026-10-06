/**
 * The weather grid's geometry, packing, and — the one that matters — its bill.
 *
 * Open-Meteo's free tier is the binding constraint on this whole feature, and
 * the cost is invisible in the code: it falls out of three config numbers
 * multiplied together. The budget tests below are here so that tightening the
 * grid for a nicer-looking field cannot silently exceed the quota and take the
 * layer down for everyone.
 */
import { describe, it, expect, beforeEach } from 'vitest';

const {
  AU_BBOX, FREE_TIER, LAND_VARS, NODATA,
  gridGeometry, cellCount, cellLatLon, allCells, MARINE_VARS, AIR_VARS, FLOOD_VARS,
  estimateSpend, quantise, dequantise, batchCells, VAR_SCALE, marineGeometry,
  reserveLocations, locationsUsedInLastMinute, _resetRateWindow,
} = await import('../../../src/sources/weatherGrid.js');

describe('grid geometry', () => {
  it('covers Australia inclusive of both edges', () => {
    // 42 degrees of longitude at 0.5 is 84 intervals, which is 85 cells. Off
    // by one here drops a half-cell strip of the east coast, where most of the
    // audience lives.
    const g = gridGeometry(0.5);
    expect(g.cols).toBe(85);
    expect(g.rows).toBe(69);
    expect(cellCount(g)).toBe(5865);

    const first = cellLatLon(g, 0);
    expect(first).toEqual({ lat: AU_BBOX.south, lon: AU_BBOX.west });

    const last = cellLatLon(g, cellCount(g) - 1);
    expect(last.lon).toBeCloseTo(AU_BBOX.east, 6);
    expect(last.lat).toBeCloseTo(AU_BBOX.north, 6);
  });

  it('runs row-major from the south-west corner', () => {
    const g = gridGeometry(1);
    expect(cellLatLon(g, 1).lon).toBeGreaterThan(cellLatLon(g, 0).lon);
    expect(cellLatLon(g, 1).lat).toBe(cellLatLon(g, 0).lat);
    // One full row on takes us north.
    expect(cellLatLon(g, g.cols).lat).toBeGreaterThan(cellLatLon(g, 0).lat);
    expect(cellLatLon(g, g.cols).lon).toBe(cellLatLon(g, 0).lon);
  });

  it('keeps every cell inside the bounding box', () => {
    const g = gridGeometry(2);
    for (const { lat, lon } of allCells(g)) {
      expect(lon).toBeGreaterThanOrEqual(AU_BBOX.west);
      expect(lon).toBeLessThanOrEqual(AU_BBOX.east);
      expect(lat).toBeGreaterThanOrEqual(AU_BBOX.south);
      expect(lat).toBeLessThanOrEqual(AU_BBOX.north);
    }
  });

  it('refuses a nonsensical step rather than producing an empty grid', () => {
    expect(() => gridGeometry(0)).toThrow(/positive/);
    expect(() => gridGeometry(-1)).toThrow(/positive/);
  });

  it('puts recognisable places in the right cells', () => {
    // Sanity that the box is actually over Australia and not, say, mirrored.
    const g = gridGeometry(0.5);
    const cells = allCells(g);
    const near = (lat: number, lon: number) =>
      cells.some((c) => Math.abs(c.lat - lat) <= 0.5 && Math.abs(c.lon - lon) <= 0.5);
    expect(near(-33.87, 151.21)).toBe(true); // Sydney
    expect(near(-42.88, 147.33)).toBe(true); // Hobart
    expect(near(-12.46, 130.84)).toBe(true); // Darwin
    expect(near(-31.95, 115.86)).toBe(true); // Perth
  });
});

describe('the Open-Meteo bill', () => {
  it('the shipped defaults sit inside the free tier', () => {
    // The guard rail. If this fails, the configured grid cannot be afforded
    // and the layer will start erroring in production once the quota trips.
    const est = estimateSpend();
    expect(est.withinFreeTier).toBe(true);
    expect(est.locationsPerDay).toBeLessThanOrEqual(FREE_TIER.perDay);
    expect(est.locationsPerMonth).toBeLessThanOrEqual(FREE_TIER.perMonth);
  });

  it('is safe under the pessimistic reading, not just the optimistic one', () => {
    // Open-Meteo documents weighting by variables and time span but says
    // nothing about whether multiple coordinates in one request count once or
    // once each. The defaults have to survive the worse answer.
    const est = estimateSpend();
    // Land at 0.5 deg plus marine at its own coarser 1 deg grid, with marine
    // counted at its WORST case (every cell ocean) so the budget does not
    // depend on how the coastline happens to fall.
    expect(est.cells).toBe(5865);
    expect(est.marineCells).toBe(1505);
    expect(est.airCells).toBe(667);
    expect(est.locationsPerDay).toBe(5865 + 1505 + 667);
    expect(est.requestsPerDay).toBeLessThan(est.locationsPerDay);
  });

  it('stays at or under ten variables, so a request still weighs one call', () => {
    // "More than 10 weather variables ... are considered multiple API calls."
    // The land list is deliberately AT the limit, so this is the test that
    // stops an eleventh being added without anyone noticing it doubles the
    // cost of all 5,865 cells.
    expect(LAND_VARS.length).toBe(10);
    expect(MARINE_VARS.length).toBeLessThanOrEqual(10);
    expect(AIR_VARS.length).toBeLessThanOrEqual(10);
  });

  it('every variable has a scale, or it silently renders unscaled', () => {
    for (const v of [...LAND_VARS, ...MARINE_VARS, ...AIR_VARS, ...FLOOD_VARS]) {
      expect(VAR_SCALE[v], `no scale for ${v}`).toBeGreaterThan(0);
    }
  });

  it('catches a resolution bump that would blow the quota', () => {
    // Proof the guard rail actually fires: 0.1 degrees is a far prettier field
    // and about 143,000 cells a day.
    const est = estimateSpend(gridGeometry(0.1), 250, 1);
    expect(est.withinFreeTier).toBe(false);
    expect(est.locationsPerDay).toBeGreaterThan(FREE_TIER.perDay);
  });

  it('catches refreshing too often at the shipped resolution', () => {
    const est = estimateSpend(gridGeometry(0.5), 250, 4);
    expect(est.withinFreeTier).toBe(false);
  });

  it('counts a part-full final batch', () => {
    const est = estimateSpend(gridGeometry(0.5), 250, 1, null, null);
    expect(est.requestsPerRefresh).toBe(Math.ceil(5865 / 250));
    expect(est.requestsPerRefresh).toBe(24);
  });

  it('marine on the land grid would break the budget, which is why it is coarser', () => {
    // The reason marineGeometry exists. Sharing the 0.5 deg grid costs another
    // 5,865 locations a day and 11,730 combined is past the ceiling before a
    // single retry — so this must be caught rather than discovered in
    // production when the quota trips mid-afternoon.
    const shared = estimateSpend(gridGeometry(0.5), 250, 1, gridGeometry(0.5), null);
    expect(shared.locationsPerDay).toBe(11730);
    expect(shared.withinFreeTier).toBe(false);

    // The shipped pairing fits, with room.
    const shipped = estimateSpend(gridGeometry(0.5), 250, 1, marineGeometry(1), null);
    expect(shipped.withinFreeTier).toBe(true);
    expect(shipped.locationsPerMonth).toBeLessThanOrEqual(FREE_TIER.perMonth);
  });

  it('the marine grid is coarser than the land grid', () => {
    expect(marineGeometry().stepDeg).toBeGreaterThan(gridGeometry().stepDeg);
    expect(cellCount(marineGeometry())).toBeLessThan(cellCount(gridGeometry()));
  });
});

describe('quantise / dequantise', () => {
  it('round-trips within half a step of the variable scale', () => {
    // The guarantee is the scale, not an arbitrary number of decimals: at
    // scale 10 a value can move by at most 0.05. Asserting against the scale
    // keeps this honest if a scale is ever changed, and avoids pinning the
    // test to values that sit exactly on a rounding boundary.
    const cases: Array<[typeof LAND_VARS[number], number[]]> = [
      ['temperature_2m', [-8.4, 0, 17.25, 46.9]],
      ['precipitation', [0, 0.125, 12.7]],
      ['wind_direction_10m', [0, 180.04, 359.9]],
    ];
    for (const [v, vals] of cases) {
      // Half a step, plus slack for the fact that neither the scaled value nor
      // the divided-back result is exactly representable in binary floating
      // point — 17.25 packs to 173 and unpacks to 17.299999999999997.
      const tolerance = 1 / (2 * VAR_SCALE[v]) + 1e-9;
      const back = dequantise(quantise(vals, v), v);
      for (let i = 0; i < vals.length; i += 1) {
        expect(Math.abs((back[i] as number) - vals[i]!)).toBeLessThanOrEqual(tolerance);
      }
    }
  });

  it('keeps absent distinct from zero', () => {
    // Wave height inland is absent, not calm. Rendering them the same way
    // paints a flat sea across the Nullarbor.
    const packed = quantise([null, 0, undefined, NaN, 1.5], 'wave_height');
    expect(Array.from(packed.slice(0, 1))).toEqual([NODATA]);
    expect(packed[1]).toBe(0);
    expect(packed[2]).toBe(NODATA);
    expect(packed[3]).toBe(NODATA);

    const back = dequantise(packed, 'wave_height');
    expect(back[0]).toBeNull();
    expect(back[1]).toBe(0);
    expect(back[4]).toBeCloseTo(1.5, 2);
  });

  it('clamps instead of wrapping', () => {
    // A wrapped Int16 shows up as a wildly wrong value in the middle of the
    // field; a clipped one just reads as the extreme it already was.
    const packed = quantise([1e9, -1e9], 'temperature_2m');
    expect(packed[0]).toBe(32767);
    expect(packed[1]).toBe(-32767);
    expect(packed[0]).toBeGreaterThan(0);
    expect(packed[1]).toBeLessThan(0);
  });

  it('holds the full plausible range of every variable', () => {
    const extremes: Record<string, number[]> = {
      temperature_2m: [-20, 55],
      apparent_temperature: [-25, 60],
      precipitation: [0, 300],
      wind_speed_10m: [0, 300],
      wind_direction_10m: [0, 360],
      wind_gusts_10m: [0, 400],
      wave_height: [0, 30],
      wave_direction: [0, 360],
      wave_period: [0, 30],
      swell_wave_height: [0, 25],
    };
    for (const [v, vals] of Object.entries(extremes)) {
      const back = dequantise(quantise(vals, v as never), v as never);
      for (let i = 0; i < vals.length; i += 1) {
        // Nothing clipped: the packed value survived the round trip.
        expect(back[i]).toBeCloseTo(vals[i]!, 1);
      }
    }
  });

  it('produces one Int16 per cell', () => {
    const g = gridGeometry(2);
    const packed = quantise(new Array(cellCount(g)).fill(20), 'temperature_2m');
    expect(packed.length).toBe(cellCount(g));
    expect(packed.BYTES_PER_ELEMENT).toBe(2);
  });
});

describe('batching', () => {
  it('never exceeds the batch size', () => {
    const cells = allCells(gridGeometry(0.5));
    const batches = batchCells(cells, 250);
    expect(batches.every((b) => b.length <= 250)).toBe(true);
    expect(batches.flat()).toHaveLength(cells.length);
  });

  it('keeps cells in index order across batches', () => {
    const batches = batchCells([0, 1, 2, 3, 4, 5, 6], 3);
    expect(batches).toEqual([[0, 1, 2], [3, 4, 5], [6]]);
  });

  it('refuses a zero batch size instead of looping forever', () => {
    expect(() => batchCells([1, 2, 3], 0)).toThrow(/batch size/);
  });
});

describe('per-minute pacing', () => {
  // The constraint that actually took the first production run down. The daily
  // ceiling was never the problem: 5,865 locations went out in about six
  // seconds across 24 requests, roughly sixty times the 600/minute allowance,
  // and Open-Meteo returned 429 partway through the grid.
  const LIMIT = 400;

  beforeEach(() => { _resetRateWindow(); });

  it('lets a batch through while the window has room', async () => {
    const waits: number[] = [];
    await reserveLocations(250, async (ms) => { waits.push(ms); });
    expect(waits).toEqual([]);
    expect(locationsUsedInLastMinute()).toBe(250);
  });

  it('makes the caller wait once the minute is spent', async () => {
    const waits: number[] = [];
    // A fake clock, so the test does not actually sit through a minute.
    let now = 1_000_000;
    const realNow = Date.now;
    Date.now = () => now;
    try {
      await reserveLocations(LIMIT, async () => {});
      // The allowance is gone; the next batch has to wait for the window.
      const p = reserveLocations(100, async (ms) => {
        waits.push(ms);
        now += ms;
      });
      await p;
      expect(waits.length).toBeGreaterThan(0);
      expect(waits[0]).toBeGreaterThan(50_000);
    } finally {
      Date.now = realNow;
    }
  });

  it('paces a full grid refresh below the per-minute ceiling', async () => {
    // The regression test for the 429. Walk every batch of a real refresh
    // through the pacer on a fake clock and assert the rate never exceeds the
    // limit in any 60-second window.
    let now = 1_000_000;
    const realNow = Date.now;
    Date.now = () => now;
    const sent: Array<{ at: number; n: number }> = [];
    try {
      const batches = batchCells(allCells(gridGeometry(0.5)), 250);
      for (const b of batches) {
        await reserveLocations(b.length, async (ms) => { now += ms; });
        sent.push({ at: now, n: b.length });
      }
    } finally {
      Date.now = realNow;
    }

    expect(sent.length).toBe(24);
    for (const probe of sent) {
      const inWindow = sent
        .filter((s) => s.at > probe.at - 60_000 && s.at <= probe.at)
        .reduce((sum, s) => sum + s.n, 0);
      expect(inWindow).toBeLessThanOrEqual(LIMIT);
    }

    // And it is the pacing, not luck: unpaced, the same 24 batches would have
    // put 5,865 locations into one window.
    const total = sent.reduce((s, x) => s + x.n, 0);
    expect(total).toBe(5865);
    expect(total).toBeGreaterThan(LIMIT * 10);
  });

  it('does not deadlock on a batch larger than the whole allowance', async () => {
    // Misconfiguration must degrade to "send it and let the upstream judge",
    // not to a refresh that silently never finishes.
    const waits: number[] = [];
    await reserveLocations(LIMIT * 3, async (ms) => { waits.push(ms); });
    expect(waits).toEqual([]);
  });
});
