/**
 * River discharge: the request window, land-only selection, and cell alignment.
 *
 * The window test is the one that matters. Open-Meteo's flood API defaults
 * `forecast_days` to 92, and a request spanning more than two weeks counts as
 * multiple API calls — so taking the default would make every location weigh
 * roughly six and a half, quietly putting the whole weather feature an order of
 * magnitude over budget with nothing failing until the quota tripped.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const files = new Map<string, Uint8Array | string>();

const fetchJsonMock = vi.fn();
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson: (...args: unknown[]) => fetchJsonMock(...args),
}));

// The mask is built from the Elevation API; this suite is about flood, so the
// classification is supplied rather than fetched.
const maskMock = vi.fn();
vi.mock('../../../src/sources/weatherMask.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/sources/weatherMask.js')>();
  return { ...actual, loadMask: (...a: unknown[]) => maskMock(...a) };
});

vi.mock('node:fs/promises', () => ({
  mkdir: vi.fn(async () => undefined),
  readFile: vi.fn(async (p: string) => {
    const hit = files.get(String(p));
    if (hit === undefined) throw Object.assign(new Error('ENOENT'), { code: 'ENOENT' });
    return typeof hit === 'string' ? hit : Buffer.from(hit);
  }),
  writeFile: vi.fn(async (p: string, d: Uint8Array | string) => {
    files.set(String(p), typeof d === 'string' ? d : new Uint8Array(d));
  }),
  rename: vi.fn(async (a: string, b: string) => {
    const v = files.get(String(a));
    if (v !== undefined) { files.set(String(b), v); files.delete(String(a)); }
  }),
  readdir: vi.fn(async () => [] as string[]),
  unlink: vi.fn(async (p: string) => { files.delete(String(p)); }),
}));

const { refreshFloodGrid, floodWindow, MAX_WINDOW_DAYS } =
  await import('../../../src/sources/weatherFloodSource.js');
const { marineGeometry, cellCount, dequantise } =
  await import('../../../src/sources/weatherGrid.js');
const { writeManifest, readManifest, readGridBytes } =
  await import('../../../src/services/weatherStore.js');
const { OCEAN, LAND } = await import('../../../src/sources/weatherMask.js');

const DAYS = ['2026-10-05', '2026-10-06', '2026-10-07'];

/** A flood point whose discharge identifies the cell it came from. */
function point(discharge: number | null) {
  return {
    daily: {
      time: DAYS,
      river_discharge: DAYS.map(() => discharge),
      river_discharge_median: DAYS.map(() => (discharge === null ? null : 10)),
    },
  };
}

function stubUpstream() {
  let served = 0;
  const calls: string[] = [];
  fetchJsonMock.mockImplementation(async (url: string) => {
    calls.push(String(url));
    const n = decodeURIComponent(
      String(url).split('latitude=')[1]!.split('&')[0]!,
    ).split(',').length;
    const batch = [];
    for (let i = 0; i < n; i += 1) batch.push(point(served + i));
    served += n;
    return batch;
  });
  return calls;
}

/** A mask where a known handful of cells are land and the rest ocean. */
function maskWith(landIndices: number[]) {
  const g = marineGeometry();
  const m = new Uint8Array(cellCount(g)).fill(OCEAN);
  for (const i of landIndices) m[i] = LAND;
  maskMock.mockResolvedValue(m);
  return m;
}

async function seedLandManifest() {
  await writeManifest({
    issuedAt: new Date().toISOString(),
    geometry: { west: 112, south: -44, stepDeg: 0.5, cols: 85, rows: 69 },
    timesteps: ['2026-10-05T00:00:00.000Z'],
    vars: [{ name: 'temperature_2m', scale: 10, unit: '°C', marine: false }],
    nodata: -32768,
  });
}

beforeEach(() => {
  files.clear();
  fetchJsonMock.mockReset();
  maskMock.mockReset();
  vi.stubGlobal('fetch', vi.fn(() => {
    throw new Error('a test tried to reach the real network');
  }));
});

describe('the request window', () => {
  it('never spans more than two weeks, so a request still weighs one call', () => {
    const { pastDays, forecastDays } = floodWindow();
    expect(pastDays + forecastDays).toBeLessThanOrEqual(MAX_WINDOW_DAYS);
    expect(forecastDays).toBeGreaterThan(0);
  });

  it('clamps a window someone widens past two weeks', () => {
    // The clamp has to hold against config, not just against today's values —
    // widening WEATHER_FORECAST_DAYS for the land field must not silently
    // multiply what flood costs.
    expect(floodWindow(2, 90)).toEqual({ pastDays: 2, forecastDays: 12 });
    expect(floodWindow(0, 92)).toEqual({ pastDays: 0, forecastDays: 14 });
    expect(floodWindow(30, 30)).toEqual({ pastDays: 13, forecastDays: 1 });

    for (const [p, f] of [[2, 90], [0, 92], [30, 30], [7, 7], [1, 3]]) {
      const w = floodWindow(p, f);
      expect(w.pastDays + w.forecastDays).toBeLessThanOrEqual(MAX_WINDOW_DAYS);
      expect(w.forecastDays).toBeGreaterThanOrEqual(1);
    }
  });

  it('leaves a window already inside the limit alone', () => {
    expect(floodWindow(2, 7)).toEqual({ pastDays: 2, forecastDays: 7 });
  });

  it('pins forecast_days explicitly rather than taking the 92-day default', async () => {
    await seedLandManifest();
    maskWith([0, 1, 2]);
    const calls = stubUpstream();
    await refreshFloodGrid(true);

    const url = calls[0]!;
    expect(url).toContain('forecast_days=');
    const forecast = Number(/forecast_days=(\d+)/.exec(url)![1]);
    const past = Number(/past_days=(\d+)/.exec(url)![1]);
    // 92 is the API default and would cost about 6.5 calls per location.
    expect(forecast).not.toBe(92);
    expect(past + forecast).toBeLessThanOrEqual(MAX_WINDOW_DAYS);
  });

  it('asks the flood endpoint for daily values in UTC', async () => {
    await seedLandManifest();
    maskWith([0]);
    const calls = stubUpstream();
    await refreshFloodGrid(true);
    expect(calls[0]).toContain('flood-api.open-meteo.com');
    expect(calls[0]).toContain('daily=river_discharge');
    expect(calls[0]).toContain('timezone=UTC');
    // Daily data has no hourly block; asking for one would be a different API.
    expect(calls[0]).not.toContain('hourly=');
  });
});

describe('land-only selection', () => {
  it('asks only about land cells, and writes them to their own indices', async () => {
    await seedLandManifest();
    const land = [5, 9, 40];
    maskWith(land);
    const calls = stubUpstream();

    const m = await refreshFloodGrid(true);
    expect(m).not.toBeNull();

    // One request, carrying exactly the land cells — not the whole grid.
    const asked = decodeURIComponent(
      String(calls[0]).split('latitude=')[1]!.split('&')[0]!,
    ).split(',').length;
    expect(asked).toBe(land.length);

    const bytes = await readGridBytes('river_discharge', m!.floodTimesteps![0]!);
    const packed = new Int16Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength / 2);
    const vals = dequantise(packed, 'river_discharge');

    // Served 0,1,2 in land order -> must land on grid indices 5,9,40.
    expect(vals[5]).toBe(0);
    expect(vals[9]).toBe(1);
    expect(vals[40]).toBe(2);
  });

  it('leaves the ocean absent rather than zero', async () => {
    // A river discharge of 0 in the Tasman would read as a real measurement of
    // no flow. Absent is the truth: there is no river there to measure.
    await seedLandManifest();
    maskWith([5]);
    stubUpstream();
    const m = await refreshFloodGrid(true);

    const bytes = await readGridBytes('river_discharge', m!.floodTimesteps![0]!);
    const packed = new Int16Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength / 2);
    const vals = dequantise(packed, 'river_discharge');
    expect(vals[5]).toBe(0);          // the land cell, a real zero
    expect(vals[6]).toBeNull();       // ocean, absent
    expect(vals[0]).toBeNull();
  });

  it('refuses to build when the mask finds no land', async () => {
    await seedLandManifest();
    maskWith([]);
    const calls = stubUpstream();
    const m = await refreshFloodGrid(true);
    expect(calls).toHaveLength(0);
    expect(m!.vars.some((v) => v.flood)).toBe(false);
  });
});

describe('the manifest', () => {
  it('carries a daily axis of its own, separate from the land one', async () => {
    await seedLandManifest();
    maskWith([1, 2]);
    stubUpstream();
    const m = await refreshFloodGrid(true);

    expect(m!.floodTimesteps).toHaveLength(DAYS.length);
    expect(m!.floodTimesteps![0]).toBe('2026-10-05T00:00:00.000Z');
    // Daily, so consecutive steps are 24 hours apart — not the 3 the land
    // field uses.
    const gapHours =
      (Date.parse(m!.floodTimesteps![1]!) - Date.parse(m!.floodTimesteps![0]!)) / 3_600_000;
    expect(gapHours).toBe(24);
    expect(m!.floodTimesteps).not.toEqual(m!.timesteps);
  });

  it('keeps the land variables it was folded into', async () => {
    await seedLandManifest();
    maskWith([1]);
    stubUpstream();
    const m = await refreshFloodGrid(true);
    expect(m!.vars.some((v) => v.name === 'temperature_2m')).toBe(true);
    expect(m!.vars.filter((v) => v.flood).map((v) => v.name))
      .toEqual(['river_discharge', 'river_discharge_median']);
  });

  it('does nothing at all without a land manifest to fold into', async () => {
    const calls = stubUpstream();
    expect(await refreshFloodGrid(true)).toBeNull();
    expect(calls).toHaveLength(0);
  });

  it('does not refetch while the stored dataset is current', async () => {
    await seedLandManifest();
    maskWith([1]);
    const calls = stubUpstream();
    await refreshFloodGrid(true);
    const first = calls.length;
    await refreshFloodGrid(false);
    expect(calls.length).toBe(first);
  });
});
