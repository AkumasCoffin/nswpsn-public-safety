/**
 * Air quality: cell alignment, its own time axis, and the pacer.
 *
 * The pacing test is the one worth having. Air is the fourth source prewarmed
 * alongside land, marine and flood, and they share a single per-minute
 * allowance — a source that reached for `fetchJson` itself would stay invisible
 * in every other test here and take the whole refresh down on an HTTP 429, which
 * has happened once already.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const files = new Map<string, Uint8Array | string>();

// fetchJson wraps undici, not globalThis.fetch, so mocking the module is what
// actually keeps this file offline. The global stub below is only a tripwire.
const fetchJsonMock = vi.fn();
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson: (...args: unknown[]) => fetchJsonMock(...args),
}));

// The real pacer, with a spy in front of it: this file has to prove air goes
// through `pacedFetch` and not around it, so the module is mocked only enough to
// observe the call and then delegates to the genuine implementation.
const pacedSpy = vi.fn();
vi.mock('../../../src/sources/weatherGridSource.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/sources/weatherGridSource.js')>();
  return {
    ...actual,
    pacedFetch: (url: string, locations: number) => {
      pacedSpy(url, locations);
      return actual.pacedFetch(url, locations);
    },
  };
});

// Lift the per-minute ceiling for this file only. The real allowance would make
// a full grid a quarter-hour of deliberate waiting, which is correct in
// production and absurd here; the pacer itself is covered in weatherGrid.test.ts
// against a fake clock.
vi.mock('../../../src/config.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/config.js')>();
  return { ...actual, config: { ...actual.config, WEATHER_LOCATIONS_PER_MIN: 1_000_000 } };
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

const { refreshAirGrid } = await import('../../../src/sources/weatherAirSource.js');
const { AIR_VARS, airGeometry, cellCount, dequantise } =
  await import('../../../src/sources/weatherGrid.js');
const { writeManifest, readGridBytes } = await import('../../../src/services/weatherStore.js');

const HOURS = 6;
const TIMES = Array.from({ length: HOURS }, (_, i) =>
  `2026-10-06T${String(i).padStart(2, '0')}:00`);

/**
 * One air-quality point whose readings identify the cell it came from.
 *
 * Tenths for the scale-10 particulates, not the raw index: cell 5864 as a PM2.5
 * reading packs to 58,640 and clamps at the Int16 ceiling, so the marker would
 * stop being unique exactly where the alignment check matters most. The AQI is
 * scale 1 and carries the index whole.
 */
function point(idx: number | null) {
  return {
    hourly: {
      time: TIMES,
      pm2_5: TIMES.map(() => (idx === null ? null : idx / 10)),
      pm10: TIMES.map(() => (idx === null ? null : idx / 10)),
      us_aqi: TIMES.map(() => idx),
    },
  };
}

/** Stub the upstream so each cell's reading equals its own index. */
function stubUpstream(opts: { short?: boolean } = {}) {
  let served = 0;
  const calls: string[] = [];
  fetchJsonMock.mockImplementation(async (url: string) => {
    calls.push(String(url));
    // URLSearchParams percent-encodes the separating commas, so the coordinate
    // list has to be decoded before it can be counted.
    const n = decodeURIComponent(
      String(url).split('latitude=')[1]!.split('&')[0]!,
    ).split(',').length;
    const batch = [];
    for (let i = 0; i < n; i += 1) batch.push(point(served + i));
    served += n;
    if (opts.short) batch.pop();
    return batch;
  });
  return calls;
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

async function readVar(name: (typeof AIR_VARS)[number], timestep: string) {
  const bytes = await readGridBytes(name, timestep);
  expect(bytes).not.toBeNull();
  const packed = new Int16Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength / 2);
  return { packed, vals: dequantise(packed, name) };
}

beforeEach(() => {
  files.clear();
  fetchJsonMock.mockReset();
  pacedSpy.mockClear();
  vi.stubGlobal('fetch', vi.fn(() => {
    throw new Error('a test tried to reach the real network');
  }));
});

describe('the request', () => {
  it('asks the air-quality endpoint for the three variables, in UTC', async () => {
    await seedLandManifest();
    const calls = stubUpstream();
    await refreshAirGrid(true);

    const url = calls[0]!;
    expect(url).toContain('air-quality-api.open-meteo.com/v1/air-quality');
    expect(url).toContain('timezone=UTC');
    const hourly = decodeURIComponent(url.split('hourly=')[1]!.split('&')[0]!);
    expect(hourly.split(',')).toEqual(['pm2_5', 'pm10', 'us_aqi']);
    expect(url).toContain('past_days=');
    expect(url).toContain('forecast_days=');
  });

  it('goes through the shared pacer rather than fetching directly', async () => {
    await seedLandManifest();
    const calls = stubUpstream();
    await refreshAirGrid(true);

    // Every upstream request came from pacedFetch, and each declared the number
    // of LOCATIONS it was about to spend — not one call per request, which is
    // the mistake that blew the per-minute allowance.
    expect(pacedSpy).toHaveBeenCalledTimes(calls.length);
    let declared = 0;
    for (const [url, locations] of pacedSpy.mock.calls as Array<[string, number]>) {
      const asked = decodeURIComponent(
        String(url).split('latitude=')[1]!.split('&')[0]!,
      ).split(',').length;
      expect(locations).toBe(asked);
      declared += locations;
    }
    expect(declared).toBe(cellCount(airGeometry()));
  });

  it('batches rather than making one request per cell', async () => {
    await seedLandManifest();
    const calls = stubUpstream();
    await refreshAirGrid(true);
    const cells = cellCount(airGeometry());
    expect(calls.length).toBe(pacedSpy.mock.calls.length);
    expect(calls.length).toBeLessThan(cells);
    expect(calls.length).toBeGreaterThan(0);
  });

  it('refuses a short batch instead of skewing the field', async () => {
    await seedLandManifest();
    stubUpstream({ short: true });
    await expect(refreshAirGrid(true)).rejects.toThrow(/asked for .* got/);
  });
});

describe('cell alignment', () => {
  it('writes every cell to its own index, in grid order', async () => {
    await seedLandManifest();
    stubUpstream();
    const m = await refreshAirGrid(true);
    expect(m).not.toBeNull();

    const total = cellCount(airGeometry());
    const { packed, vals } = await readVar('us_aqi', m!.airTimesteps![0]!);
    expect(packed.length).toBe(total);

    // Cell N was served N, so the field reads back as its own index. A slip in
    // batching or indexing shows here, and the last cell catches an off-by-one
    // at the far edge of the box.
    expect(vals[0]).toBe(0);
    expect(vals[1]).toBe(1);
    expect(vals[total - 1]).toBe(total - 1);

    const particulate = await readVar('pm2_5', m!.airTimesteps![0]!);
    expect(particulate.vals[0]).toBeCloseTo(0, 3);
    expect(particulate.vals[7]).toBeCloseTo(0.7, 3);
    expect(particulate.vals[total - 1]).toBeCloseTo((total - 1) / 10, 3);
  });

  it('covers every cell in the box, with no masked-out holes', async () => {
    // Air is not marine: there is air over the Tasman and over the Simpson, and
    // the smoke blowing offshore is the part a coastal reader wants. Nothing
    // here may read as absent.
    await seedLandManifest();
    stubUpstream();
    const m = await refreshAirGrid(true);
    const { vals } = await readVar('us_aqi', m!.airTimesteps![0]!);
    expect(vals.some((v) => v === null)).toBe(false);
  });
});

describe('the manifest', () => {
  it('carries an axis of its own, separate from the land one', async () => {
    await seedLandManifest();
    stubUpstream();
    const m = await refreshAirGrid(true);

    expect(m!.airTimesteps).toHaveLength(2); // 6 hours at 3-hourly
    expect(m!.airTimesteps![0]).toBe('2026-10-06T00:00:00.000Z');
    const gapHours =
      (Date.parse(m!.airTimesteps![1]!) - Date.parse(m!.airTimesteps![0]!)) / 3_600_000;
    expect(gapHours).toBe(3);
    expect(m!.airTimesteps).not.toEqual(m!.timesteps);
    expect(m!.airGeometry!.stepDeg).toBe(airGeometry().stepDeg);
  });

  it('keeps the land variables it was folded into, and flags its own', async () => {
    await seedLandManifest();
    stubUpstream();
    const m = await refreshAirGrid(true);
    expect(m!.vars.some((v) => v.name === 'temperature_2m')).toBe(true);
    expect(m!.vars.filter((v) => v.air).map((v) => v.name))
      .toEqual(['pm2_5', 'pm10', 'us_aqi']);
    // Not marine and not flood: the client picks an axis off these flags, and a
    // var claiming two would be read against the wrong grid.
    expect(m!.vars.filter((v) => v.air).every((v) => !v.marine && !v.flood)).toBe(true);
  });

  it('does nothing at all without a land manifest to fold into', async () => {
    const calls = stubUpstream();
    expect(await refreshAirGrid(true)).toBeNull();
    expect(calls).toHaveLength(0);
    expect(pacedSpy).not.toHaveBeenCalled();
  });

  it('does not refetch while the stored dataset is current', async () => {
    await seedLandManifest();
    const calls = stubUpstream();
    await refreshAirGrid(true);
    const first = calls.length;

    // A restart fires every source immediately; without this guard a redeploy
    // would re-spend the air budget on each one.
    await refreshAirGrid(false);
    expect(calls.length).toBe(first);
  });
});
