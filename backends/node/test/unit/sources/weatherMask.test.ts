/**
 * The land/sea mask: the caching contract and the classification.
 *
 * Two kinds of mistake are expensive here and neither announces itself. The
 * first is refetching — the mask is supposed to cost 59 requests once in the
 * lifetime of the deployment, so a cache that silently misses turns a one-time
 * cost into a daily one. The second is reusing a mask that doesn't belong to
 * this grid, which produces a complete, plausible, entirely wrong coastline.
 * Both are asserted against here rather than left to be noticed in production.
 *
 * Nothing in this file touches the network: `fetchJson` is mocked and the
 * global `fetch` is replaced with a tripwire that throws if anything reaches
 * for it.
 */
import { describe, it, expect, beforeEach, afterAll, vi } from 'vitest';
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
// The mask now goes through the shared per-minute pacer, and this file builds
// a 5,865-cell mask — at the real 400/minute that is a quarter of an hour of
// deliberate waiting, so every test here timed out. Stub the pacer itself
// rather than the config value behind it: this file is about mask logic, and
// the pacer has its own coverage in weatherGrid.test.ts against a fake clock.
vi.mock('../../../src/sources/weatherGrid.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/sources/weatherGrid.js')>();
  return { ...actual, reserveLocations: async () => undefined };
});

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

// The mask writes under STATE_DIR, so point it somewhere disposable and
// per-process, so a parallel suite can't collide on the cache file. Everything
// else about the config stays real — the batch size in particular, since one of
// the tests is about honouring it. The path is built by string concatenation
// because vi.hoisted runs before this file's imports exist.
const { stateDir } = vi.hoisted(() => ({
  stateDir: `./test/.tmp-state/weatherMask-${process.pid}`,
}));
vi.mock('../../../src/config.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/config.js')>();
  return { ...actual, config: { ...actual.config, STATE_DIR: stateDir } };
});

const { config } = await import('../../../src/config.js');
const { gridGeometry, allCells, cellCount } = await import('../../../src/sources/weatherGrid.js');
const {
  loadMask, isOcean, oceanCellIndices, classifyOcean, maskPath, _resetMaskCache,
  OCEAN, LAND, ELEVATION_MAX_COORDS,
} = await import('../../../src/sources/weatherMask.js');

const G = gridGeometry(0.5);
const CELLS = allCells(G);

/** Cell index nearest a real-world point. */
function indexOf(lat: number, lon: number): number {
  let best = 0;
  let bestD = Infinity;
  for (let i = 0; i < CELLS.length; i += 1) {
    const c = CELLS[i]!;
    const d = (c.lat - lat) ** 2 + (c.lon - lon) ** 2;
    if (d < bestD) {
      bestD = d;
      best = i;
    }
  }
  return best;
}

const OFF_SYDNEY = indexOf(-34.0, 152.0); // ~60 km east of the heads, abyssal.
const ALICE_SPRINGS = indexOf(-23.7, 133.88);
const LAKE_EYRE = indexOf(-28.37, 137.36);

/**
 * A synthetic DEM, not a real one: the mainland is a single rectangular blob of
 * +300 m sitting strictly inside AU_BBOX, the rest of the box is sea level, and
 * the three cells under test carry their real elevations. That is enough to
 * exercise what the classifier actually decides, including the case the simple
 * "at or below sea level is water" rule gets wrong — Lake Eyre's floor is
 * genuinely about 15 m BELOW sea level while being 700 km from the coast.
 */
const BLOB = { west: 113, east: 151.5, south: -38.5, north: -11 };
const key = (lat: number, lon: number) => `${lat.toFixed(4)},${lon.toFixed(4)}`;
const REAL_ELEVATIONS = new Map<string, number>([
  [key(CELLS[OFF_SYDNEY]!.lat, CELLS[OFF_SYDNEY]!.lon), -4000],
  [key(CELLS[ALICE_SPRINGS]!.lat, CELLS[ALICE_SPRINGS]!.lon), 576],
  [key(CELLS[LAKE_EYRE]!.lat, CELLS[LAKE_EYRE]!.lon), -12],
]);

function demElevation(latStr: string, lonStr: string): number {
  const override = REAL_ELEVATIONS.get(`${latStr},${lonStr}`);
  if (override !== undefined) return override;
  const lat = Number(latStr);
  const lon = Number(lonStr);
  const inland =
    lon >= BLOB.west && lon <= BLOB.east && lat >= BLOB.south && lat <= BLOB.north;
  return inland ? 300 : 0;
}

/** Coordinate counts seen per request, so batching can be inspected. */
let requestedSizes: number[] = [];

/** Answer every batch from the synthetic DEM, positionally. */
function answerFromDem(): void {
  fetchJson.mockImplementation(async (url: string) => {
    const u = new URL(url);
    const lats = (u.searchParams.get('latitude') ?? '').split(',');
    const lons = (u.searchParams.get('longitude') ?? '').split(',');
    requestedSizes.push(lats.length);
    return { elevation: lats.map((la, i) => demElevation(la, lons[i] ?? '')) };
  });
}

beforeEach(async () => {
  vi.clearAllMocks();
  requestedSizes = [];
  _resetMaskCache();
  await rm(join(stateDir, 'weather'), { recursive: true, force: true });
  // Tripwire: nothing in this module should ever reach the real network, and a
  // mocked fetchJson is only half of that guarantee.
  vi.stubGlobal('fetch', vi.fn(() => {
    throw new Error('test made a real network call');
  }));
  answerFromDem();
});

afterAll(async () => {
  await rm(stateDir, { recursive: true, force: true });
});

/** Write a mask file straight to the cache path. */
async function seedCache(bytes: Uint8Array): Promise<string> {
  const path = maskPath(G);
  await mkdir(join(stateDir, 'weather'), { recursive: true });
  await writeFile(path, bytes);
  return path;
}

describe('mask caching', () => {
  it('uses a cached mask without fetching anything', async () => {
    // The whole point of the feature: this is a one-time cost. If the cache
    // path misses, the continent gets refetched on every boot.
    const seeded = new Uint8Array(cellCount(G));
    seeded[7] = OCEAN;
    await seedCache(seeded);

    const mask = await loadMask(G, { batchPauseMs: 0 });

    expect(fetchJson).not.toHaveBeenCalled();
    expect(mask.length).toBe(cellCount(G));
    expect(isOcean(mask, 7)).toBe(true);
    expect(isOcean(mask, 8)).toBe(false);
  });

  it('fetches and writes the mask when the cache is missing', async () => {
    const mask = await loadMask(G, { batchPauseMs: 0 });

    expect(fetchJson).toHaveBeenCalled();
    expect(mask.length).toBe(cellCount(G));

    const onDisk = await readFile(maskPath(G));
    expect(onDisk.length).toBe(cellCount(G));
    expect(Array.from(onDisk)).toEqual(Array.from(mask));
  });

  it('puts the grid step in the filename', async () => {
    // A different step is a different mask. Sharing one filename would hand a
    // coarse mask to a fine grid and misclassify every cell on the map.
    expect(maskPath(gridGeometry(0.5))).toContain('mask-0.5.bin');
    expect(maskPath(gridGeometry(1))).toContain('mask-1.bin');
    expect(maskPath(gridGeometry(0.5))).not.toBe(maskPath(gridGeometry(1)));
  });

  it('rejects and rebuilds a cached mask of the wrong length', async () => {
    // What a step change leaves behind. Truncating or padding it would be
    // worse than refetching: the cells it does cover would be read off by an
    // offset and the coastline would land in the wrong place.
    await seedCache(new Uint8Array(cellCount(gridGeometry(1))));

    const mask = await loadMask(G, { batchPauseMs: 0 });

    expect(fetchJson).toHaveBeenCalled();
    expect(mask.length).toBe(cellCount(G));
    expect((await readFile(maskPath(G))).length).toBe(cellCount(G));
  });

  it('builds once even when two callers race a cold start', async () => {
    const [a, b] = await Promise.all([
      loadMask(G, { batchPauseMs: 0 }),
      loadMask(G, { batchPauseMs: 0 }),
    ]);
    expect(Array.from(a!)).toEqual(Array.from(b!));
    // 5,865 cells at the ELEVATION api's own 100-coordinate ceiling is 59.
    // Not 24: that was this file assuming the forecast API's batch of 250,
    // which the elevation endpoint rejects outright with HTTP 400.
    expect(fetchJson).toHaveBeenCalledTimes(Math.ceil(cellCount(G) / ELEVATION_MAX_COORDS));
  });
});

describe('classification', () => {
  it('calls deep ocean off Sydney ocean, and the inland points land', async () => {
    const mask = await loadMask(G, { batchPauseMs: 0 });

    expect(isOcean(mask, OFF_SYDNEY)).toBe(true);
    expect(isOcean(mask, ALICE_SPRINGS)).toBe(false);
    // Lake Eyre is below sea level and still not ocean: elevation alone would
    // get this wrong and spend marine requests on a salt pan.
    expect(isOcean(mask, LAKE_EYRE)).toBe(false);
  });

  it('keeps a landlocked basin land but a coastal depression sea', async () => {
    // Straight at the classifier, on a grid small enough to read. The fill is
    // 8-connected, so the basin has to be walled diagonally too — which is
    // what being 700 km inland means.
    const g = { west: 0, south: 0, stepDeg: 1, cols: 5, rows: 4 };
    //  row 3:  0  0   0  0  0     open sea along the north edge
    //  row 2:  0  9   9  9  0
    //  row 1:  0  9  -5  9  0     -5 is a basin walled in by +9 land
    //  row 0:  0  9   9  9  0
    const mask = classifyOcean(g, [
      0, 9, 9, 9, 0,
      0, 9, -5, 9, 0,
      0, 9, 9, 9, 0,
      0, 0, 0, 0, 0,
    ]);
    expect(mask[7]).toBe(LAND); // the walled basin
    expect(mask[0]).toBe(OCEAN); // box corner
    expect(mask[17]).toBe(OCEAN); // open north edge
    expect(mask[5]).toBe(OCEAN); // west edge, one row up
  });

  it('refuses an elevation list that is not one value per cell', () => {
    expect(() => classifyOcean(G, [0, 0, 0])).toThrow(/does not match/);
  });

  it('reports ocean cells in index order, and only ocean cells', async () => {
    const mask = await loadMask(G, { batchPauseMs: 0 });
    const idx = oceanCellIndices(mask);

    expect(idx.length).toBeGreaterThan(0);
    expect(idx.length).toBeLessThan(cellCount(G));
    expect(idx.every((i) => isOcean(mask, i))).toBe(true);
    expect([...idx].sort((a, b) => a - b)).toEqual(idx);
    expect(idx).toContain(OFF_SYDNEY);
    expect(idx).not.toContain(ALICE_SPRINGS);
  });

  it('treats an out-of-range index as land rather than throwing', () => {
    // The marine fetcher indexes this with whatever the caller hands it; the
    // safe wrong answer is to drop the cell, not to spend a request on it.
    const mask = new Uint8Array(4).fill(OCEAN);
    expect(isOcean(mask, 4)).toBe(false);
    expect(isOcean(mask, -1)).toBe(false);
  });
});

describe('fetching', () => {
  it('never asks for more locations than the configured batch size', async () => {
    await loadMask(G, { batchPauseMs: 0 });

    // The ELEVATION limit, which is lower than the forecast batch size and is
    // what actually bounds these requests.
    expect(requestedSizes.length).toBe(
      Math.ceil(cellCount(G) / ELEVATION_MAX_COORDS),
    );
    expect(Math.max(...requestedSizes)).toBeLessThanOrEqual(ELEVATION_MAX_COORDS);
    expect(requestedSizes.reduce((a, b) => a + b, 0)).toBe(cellCount(G));
  });

  it('honours an explicit batch size', async () => {
    await loadMask(G, { batchSize: 100, batchPauseMs: 0 });
    expect(Math.max(...requestedSizes)).toBeLessThanOrEqual(100);
    expect(requestedSizes.length).toBe(Math.ceil(cellCount(G) / 100));
  });

  it('throws when a batch comes back the wrong length', async () => {
    // Positional response, so a short array is an OFFSET, not a gap: every
    // cell after it inherits its neighbour's elevation and the whole coastline
    // shifts. A cached mask like that is wrong forever.
    fetchJson.mockImplementation(async (url: string) => {
      const n = (new URL(url).searchParams.get('latitude') ?? '').split(',').length;
      return { elevation: new Array(n - 1).fill(0) };
    });

    await expect(loadMask(G, { batchPauseMs: 0 })).rejects.toThrow(/misaligned mask/);
    // Nothing half-built left behind for the next boot to trust.
    await expect(readFile(maskPath(G))).rejects.toThrow();
  });

  it('throws when the response has no elevation array at all', async () => {
    fetchJson.mockImplementation(async () => ({ reason: 'rate limited' }));
    await expect(loadMask(G, { batchPauseMs: 0 })).rejects.toThrow(/no elevation array/);
  });

  it('cools down after a failure instead of retrying immediately', async () => {
    // Deliberately NOT instantly retryable any more. The dependent sources
    // poll every ten minutes, so an unbuildable mask was re-attempted six
    // times an hour by EACH of marine and flood — thousands of locations an
    // hour on requests that could not succeed, which starved the land grid
    // into HTTP 429 as well. A build that just failed will fail again.
    fetchJson.mockImplementationOnce(async () => {
      throw new Error('boom');
    });
    await expect(loadMask(G, { batchPauseMs: 0 })).rejects.toThrow(/boom/);

    answerFromDem();
    await expect(loadMask(G, { batchPauseMs: 0 })).rejects.toThrow(/cooling down/);

    // Once the cooldown lapses it is retryable, so a transient outage does not
    // poison the mask for the life of the process.
    _resetMaskCache();
    const mask = await loadMask(G, { batchPauseMs: 0 });
    expect(mask.length).toBe(cellCount(G));
  });
});
