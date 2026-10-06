/**
 * The grid fetcher: the time axis, cell alignment, and the restart guard.
 *
 * Two of these matter more than they look. The time axis is parsed from naive
 * strings with no zone, so a mistake shifts the whole field by the server's UTC
 * offset without failing. And cell alignment has no natural symptom either — a
 * one-cell slip renders a plausible map of the wrong places.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const files = new Map<string, Uint8Array | string>();

// fetchJson wraps undici's fetch, not globalThis.fetch — stubbing the global
// leaves the real network wide open, which is how an earlier version of this
// file sent two dozen live requests to Open-Meteo and got itself rate limited.
// Mock the module that actually performs the request, and keep a tripwire on
// the global so a future change of transport fails loudly instead of silently
// going online.
const fetchJsonMock = vi.fn();
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson: (...args: unknown[]) => fetchJsonMock(...args),
}));

// Lift the per-minute pacing ceiling for this file only. A real refresh is
// 5,865 locations at 400/minute — about a quarter of an hour of deliberate
// waiting, which is correct in production and absurd in a unit test. The pacer
// itself is covered properly in weatherGrid.test.ts, against a fake clock.
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

const { pickTimesteps, toUtcIso, refreshWeatherGrid } =
  await import('../../../src/sources/weatherGridSource.js');
const { gridGeometry, cellCount, dequantise } =
  await import('../../../src/sources/weatherGrid.js');
const { readManifest } = await import('../../../src/services/weatherStore.js');

beforeEach(() => {
  files.clear();
  fetchJsonMock.mockReset();
  vi.stubGlobal('fetch', vi.fn(() => {
    throw new Error('a test tried to reach the real network');
  }));
});

describe('time axis', () => {
  it('reads a naive stamp as UTC, not as server-local', () => {
    // The bug this prevents is invisible: Open-Meteo with timezone=UTC returns
    // "2026-10-06T00:00" with no Z, and `new Date()` would read that as local
    // time — shifting every label by 10 or 11 hours in Sydney.
    expect(toUtcIso('2026-10-06T00:00')).toBe('2026-10-06T00:00:00.000Z');
    expect(toUtcIso('2026-10-06T13:00:00')).toBe('2026-10-06T13:00:00.000Z');
  });

  it('leaves an already-zoned stamp alone', () => {
    expect(toUtcIso('2026-10-06T00:00:00Z')).toBe('2026-10-06T00:00:00.000Z');
    expect(toUtcIso('2026-10-06T10:00:00+10:00')).toBe('2026-10-06T00:00:00.000Z');
  });

  it('rejects junk rather than inventing a date', () => {
    expect(toUtcIso('not a time')).toBeNull();
    expect(toUtcIso('')).toBeNull();
  });

  it('takes every third hour', () => {
    const times = Array.from({ length: 24 }, (_, i) =>
      `2026-10-06T${String(i).padStart(2, '0')}:00`);
    const { indices, timesteps } = pickTimesteps(times, 3);
    expect(indices).toEqual([0, 3, 6, 9, 12, 15, 18, 21]);
    expect(timesteps).toHaveLength(8);
    expect(timesteps[1]).toBe('2026-10-06T03:00:00.000Z');
  });
});

// --- the full refresh, against a stubbed upstream ---------------------------

const HOURS = 6;
const TIMES = Array.from({ length: HOURS }, (_, i) =>
  `2026-10-06T${String(i).padStart(2, '0')}:00`);

/** One Open-Meteo point whose temperature is a constant, so cells are telling. */
function point(temp: number | null) {
  return {
    hourly: {
      time: TIMES,
      temperature_2m: TIMES.map(() => temp),
      apparent_temperature: TIMES.map(() => temp),
      precipitation: TIMES.map(() => 0),
      wind_speed_10m: TIMES.map(() => 10),
      wind_direction_10m: TIMES.map(() => 180),
      wind_gusts_10m: TIMES.map(() => 15),
    },
  };
}

/** Stub fetch so each cell's temperature equals its own index. */
function stubUpstream(opts: { drop?: number; short?: boolean } = {}) {
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
    for (let i = 0; i < n; i += 1) {
      const idx = served + i;
      // Tenths, not the raw index: cell 5864 as a temperature would pack to
      // 58,640 and clamp at Int16, so the marker would stop being unique
      // exactly where the alignment check matters most. A tenth per cell is
      // exactly representable at scale 10 and stays in range.
      batch.push(idx === opts.drop ? {} : point(idx / 10));
    }
    served += n;
    if (opts.short) batch.pop();
    return batch;
  });
  return calls;
}

describe('refreshWeatherGrid', () => {
  it('writes every cell to its own index, in grid order', async () => {
    stubUpstream();
    const m = await refreshWeatherGrid(true);
    const g = gridGeometry();

    expect(m.geometry.cols).toBe(g.cols);
    expect(m.timesteps).toHaveLength(2); // 6 hours at 3-hourly

    const { readGridBytes } = await import('../../../src/services/weatherStore.js');
    const bytes = await readGridBytes('temperature_2m', m.timesteps[0]!);
    expect(bytes).not.toBeNull();

    const packed = new Int16Array(
      bytes!.buffer, bytes!.byteOffset, bytes!.byteLength / 2,
    );
    expect(packed.length).toBe(cellCount(g));

    // Cell N was served N/10, so the field reads back as its own index scaled
    // down. Any slip in batching or indexing shows up immediately here, and
    // the last cell is the one that catches an off-by-one at the far edge.
    const vals = dequantise(packed, 'temperature_2m');
    expect(vals[0]).toBeCloseTo(0, 3);
    expect(vals[1]).toBeCloseTo(0.1, 3);
    expect(vals[500]).toBeCloseTo(50, 3);
    expect(vals[cellCount(g) - 1]).toBeCloseTo((cellCount(g) - 1) / 10, 3);
  });

  it('a point with no data stays absent without shifting its neighbours', async () => {
    // The subtle one: skipping a bad point must not slide every later cell one
    // place west. Cell 2 is dropped; 1 and 3 must still be themselves.
    stubUpstream({ drop: 2 });
    const m = await refreshWeatherGrid(true);
    const { readGridBytes } = await import('../../../src/services/weatherStore.js');
    const bytes = await readGridBytes('temperature_2m', m.timesteps[0]!);
    const packed = new Int16Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength / 2);
    const vals = dequantise(packed, 'temperature_2m');

    expect(vals[2]).toBeNull();
    expect(vals[1]).toBeCloseTo(0.1, 3);
    expect(vals[3]).toBeCloseTo(0.3, 3);
  });

  it('refuses a short batch instead of skewing the field', async () => {
    stubUpstream({ short: true });
    await expect(refreshWeatherGrid(true)).rejects.toThrow(/asked for .* got/);
  });

  it('batches rather than making one request per cell', async () => {
    const calls = stubUpstream();
    await refreshWeatherGrid(true);
    // 5,865 cells at 250 per request. The whole affordability argument rests
    // on this being ~24 and not ~5,865.
    expect(calls.length).toBe(24);
    expect(calls.length).toBeLessThan(cellCount(gridGeometry()) / 100);
  });

  it('does not refetch when the stored dataset is still current', async () => {
    const calls = stubUpstream();
    await refreshWeatherGrid(true);
    const first = calls.length;

    // A restart fires sources immediately. Without the freshness guard this
    // would re-spend a full day of quota on every redeploy.
    const again = await refreshWeatherGrid(false);
    expect(calls.length).toBe(first);
    expect(again.issuedAt).toBe((await readManifest())!.issuedAt);
  });

  it('asks for UTC and the configured window', async () => {
    const calls = stubUpstream();
    await refreshWeatherGrid(true);
    const url = calls[0]!;
    expect(url).toContain('timezone=UTC');
    expect(url).toContain('past_days=2');
    expect(url).toContain('forecast_days=7');
    expect(url).toContain('temperature_2m');
  });
});

describe('a deploy that adds a variable', () => {
  // THE PRODUCTION BUG. The land list grew from six variables to ten. The new
  // build went out, the refresh found a six-variable manifest under a day old,
  // called it current, and skipped — so four layers sat in the UI with no
  // grids behind them for twenty-four hours. Worse, a missing grid makes the
  // client bail out silently and leave the PREVIOUS layer on screen, so it
  // looked like those layers were broken rather than absent.
  it('refetches when the stored manifest is missing a variable', async () => {
    const { writeManifest } = await import('../../../src/services/weatherStore.js');
    const { LAND_VARS } = await import('../../../src/sources/weatherGrid.js');

    // A manifest written minutes ago, but by an older build.
    await writeManifest({
      issuedAt: new Date().toISOString(),
      geometry: { west: 112, south: -44, stepDeg: 0.5, cols: 85, rows: 69 },
      timesteps: ['2026-10-05T00:00:00.000Z'],
      vars: [{ name: 'temperature_2m', scale: 10, unit: '°C', marine: false }],
      nodata: -32768,
    });

    const calls = stubUpstream();
    const m = await refreshWeatherGrid(false);

    expect(calls.length).toBeGreaterThan(0);
    const names = m.vars.map((v) => v.name);
    for (const want of LAND_VARS) expect(names).toContain(want);
  });

  it('still skips when the manifest is both current AND complete', async () => {
    // The guard must not become "always refetch" — that would re-spend the
    // whole daily budget on every restart.
    const calls = stubUpstream();
    await refreshWeatherGrid(true);
    const first = calls.length;
    await refreshWeatherGrid(false);
    expect(calls.length).toBe(first);
  });
});

describe('pacedFetch retry classification', () => {
  // One dropped socket used to kill a whole 575-batch refresh and restart it
  // from batch zero — across a ten-minute run it might never complete. The
  // self-hosted container closes connections while it rewrites multi-GB chunk
  // files, so transient failures are a certainty, not an edge case.
  it('retries a dropped socket instead of failing the whole refresh', async () => {
    vi.useFakeTimers();
    try {
      fetchJsonMock
        .mockRejectedValueOnce(Object.assign(
          new Error('fetch failed: UND_ERR_SOCKET other side closed'), { status: null }))
        .mockResolvedValueOnce({ ok: true });
      const { pacedFetch } = await import('../../../src/sources/weatherGridSource.js');
      const p = pacedFetch('http://10.1.0.135:8081/v1/forecast?x', 10);
      await vi.advanceTimersByTimeAsync(10_000);
      await expect(p).resolves.toEqual({ ok: true });
      expect(fetchJsonMock).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not retry a 400 — that request can never succeed', async () => {
    fetchJsonMock.mockRejectedValueOnce(Object.assign(new Error('HTTP 400 for u'), { status: 400 }));
    const { pacedFetch } = await import('../../../src/sources/weatherGridSource.js');
    await expect(pacedFetch('http://10.1.0.135:8081/v1/forecast?x', 10)).rejects.toThrow('HTTP 400');
    expect(fetchJsonMock).toHaveBeenCalledTimes(1);
  });
});
