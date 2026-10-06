/**
 * The grid's land/sea mask — fetched once, ever.
 *
 * `weatherGrid.ts` explains why this feature's whole design is the Open-Meteo
 * budget. The mask is the other half of that arithmetic. Land variables are
 * meaningful everywhere, but the MARINE variables (wave height, swell) only
 * exist over water, and roughly two thirds of the 5,865 cells are inland. Sent
 * to the Marine API as-is, two thirds of every marine refresh would be spent
 * asking the Nullarbor how big the waves are and getting nulls back.
 *
 * So every cell is classified once and the answer is cached to disk forever.
 * The grid is fixed by `AU_BBOX` and the step, so the mask cannot go stale:
 * the only thing that invalidates it is a different step, which is why the step
 * is in the cache filename. Reusing a 1-degree mask for a 0.5-degree grid would
 * not fail loudly — it would silently misclassify every cell on the map.
 *
 * Open-Meteo's elevation endpoint gives this away cheaply, in the same
 * comma-separated multi-coordinate form the forecast API uses, so the whole
 * continent costs a couple of dozen requests one time.
 */
import { mkdir, readFile, rename, unlink, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { fetchJson } from './shared/http.js';
import {
  allCells,
  batchCells,
  reserveLocations,
  cellCount,
  gridGeometry,
  type GridGeometry,
} from './weatherGrid.js';

const ELEVATION_URL = config.OPEN_METEO_ELEVATION_URL;

/**
 * The elevation API's own coordinate ceiling, which is NOT the forecast API's.
 *
 * "Up to 100 coordinates can be requested at once." The forecast endpoint
 * takes 250 happily, so reusing WEATHER_GRID_BATCH sent 250 and earned a flat
 * HTTP 400 on every request — which failed the mask, which failed marine and
 * flood together, with nothing in the error naming the real cause.
 *
 * Exported so tests derive their expected request count from the real
 * constraint rather than restating a number that can drift from it.
 */
export const ELEVATION_MAX_COORDS = 100;

/** Mask byte values. One byte per cell, index-aligned with the grid. */
export const OCEAN = 1;
export const LAND = 0;

/**
 * Pause between elevation batches on the one cold build. ~24 requests back to
 * back from a fresh boot is exactly the shape of burst that earns a 429, and
 * this path has no retry — a rejected batch throws the whole build away.
 */
const BATCH_PAUSE_MS = 250;

export interface MaskOptions {
  /** Locations per request. Defaults to the configured grid batch size. */
  batchSize?: number;
  /** Override the inter-batch pause. Tests set 0; nothing else should. */
  batchPauseMs?: number;
}

/** Where the mask for a given geometry lives. The step is load-bearing. */
export function maskPath(g: GridGeometry = gridGeometry()): string {
  return join(config.STATE_DIR, 'weather', `mask-${g.stepDeg}.bin`);
}

/**
 * Ocean lookup. An out-of-range index reads as land, which is the safe wrong
 * answer: it drops a cell from the marine request rather than spending budget
 * on a cell that isn't in the grid.
 */
export function isOcean(mask: Uint8Array, index: number): boolean {
  return mask[index] === OCEAN;
}

/** Indices the marine fetcher should ask about, in cell order. */
export function oceanCellIndices(mask: Uint8Array): number[] {
  const out: number[] = [];
  for (let i = 0; i < mask.length; i += 1) {
    if (mask[i] === OCEAN) out.push(i);
  }
  return out;
}

/**
 * Elevations -> mask, in two passes.
 *
 * Pass one is the documented rule: at or below sea level is water. On its own
 * that rule is wrong for Australia specifically, because the continent has
 * inland basins BELOW sea level — the Lake Eyre floor sits around -15 m — and
 * a desert salt pan classified as ocean is precisely the wasted marine request
 * this mask exists to avoid.
 *
 * Pass two fixes it with the one piece of information the grid already has:
 * real ocean is CONNECTED to the edge of the bounding box. Every boundary cell
 * of AU_BBOX is open water (Indian, Southern, Pacific, Arafura), so a flood
 * fill inward from the edge reaches the sea and nothing else. Below-sea-level
 * cells it never reaches are landlocked by definition, and stay land.
 *
 * The fill is 8-connected so a one-cell-wide diagonal strait still counts as
 * sea; at half a degree the alternative loses whole gulf mouths. A diagonal
 * leak into an inland basin would need that basin to touch the coast, at which
 * point it is tidal and marine data is the right answer anyway.
 *
 * A non-finite elevation (a null slot in the upstream array) fails the `<= 0`
 * test and lands as land, deliberately: the cost of that mistake is one
 * missing marine cell, not a wasted request every refresh forever.
 */
export function classifyOcean(g: GridGeometry, elevations: readonly number[]): Uint8Array {
  const n = cellCount(g);
  if (elevations.length !== n) {
    throw new Error(`elevation count ${elevations.length} does not match ${n} grid cells`);
  }

  const atOrBelowSeaLevel = new Uint8Array(n);
  for (let i = 0; i < n; i += 1) {
    atOrBelowSeaLevel[i] = (elevations[i] as number) <= 0 ? 1 : 0;
  }

  const mask = new Uint8Array(n);
  const stack: number[] = [];
  const flood = (i: number): void => {
    if (atOrBelowSeaLevel[i] === 1 && mask[i] !== OCEAN) {
      mask[i] = OCEAN;
      stack.push(i);
    }
  };

  // Seed from the whole perimeter of the box.
  for (let col = 0; col < g.cols; col += 1) {
    flood(col);
    flood((g.rows - 1) * g.cols + col);
  }
  for (let row = 0; row < g.rows; row += 1) {
    flood(row * g.cols);
    flood(row * g.cols + g.cols - 1);
  }

  while (stack.length > 0) {
    const i = stack.pop() as number;
    const row = Math.floor(i / g.cols);
    const col = i % g.cols;
    for (let dr = -1; dr <= 1; dr += 1) {
      for (let dc = -1; dc <= 1; dc += 1) {
        if (dr === 0 && dc === 0) continue;
        const r = row + dr;
        const c = col + dc;
        if (r < 0 || r >= g.rows || c < 0 || c >= g.cols) continue;
        flood(r * g.cols + c);
      }
    }
  }

  return mask;
}

interface ElevationResponse {
  elevation?: unknown;
}

/**
 * One elevation request for one batch of cells.
 *
 * The length check is the important line. Open-Meteo answers positionally, so
 * a short or long array is not a missing value — it is an OFFSET, and every
 * cell after the gap would be given its neighbour's elevation. That shifts the
 * entire coastline by however many slots were dropped, and the result still
 * looks like a plausible mask. Throwing loses the cold build; the alternative
 * caches a wrong mask to disk permanently.
 */
async function fetchElevations(
  cells: ReadonlyArray<{ lat: number; lon: number }>,
): Promise<number[]> {
  const latitude = cells.map((c) => c.lat.toFixed(4)).join(',');
  const longitude = cells.map((c) => c.lon.toFixed(4)).join(',');
  const url = `${ELEVATION_URL}?latitude=${latitude}&longitude=${longitude}`;
  const res = await fetchJson<ElevationResponse>(url, { timeoutMs: 30_000 });
  const elevation = res.elevation;
  if (!Array.isArray(elevation)) {
    throw new Error('elevation API returned no elevation array');
  }
  if (elevation.length !== cells.length) {
    throw new Error(
      `elevation API returned ${elevation.length} values for ${cells.length} ` +
        'requested cells — refusing to build a misaligned mask',
    );
  }
  return (elevation as unknown[]).map((v) => (typeof v === 'number' ? v : Number.NaN));
}

/**
 * Temp file + rename, as LiveStore does: the rename is atomic, so a crash
 * mid-write leaves either no mask or a complete one. A truncated mask would
 * otherwise pass a plain existence check and be rejected only by the length
 * test — or worse, be exactly the right length and wrong.
 */
async function writeMask(path: string, mask: Uint8Array): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const tempPath = `${path}.${process.pid}.tmp`;
  try {
    await writeFile(tempPath, mask);
    await rename(tempPath, path);
  } catch (err) {
    try {
      await unlink(tempPath);
    } catch {
      /* already gone */
    }
    throw err;
  }
}

/**
 * The cached mask, or null. A wrong-length file is treated as absent rather
 * than repaired: it means the grid changed under a stale cache, and half a
 * mask misclassifies the cells it does cover.
 */
async function readCachedMask(path: string, expected: number): Promise<Uint8Array | null> {
  let buf: Buffer;
  try {
    buf = await readFile(path);
  } catch {
    return null; // Never built, or STATE_DIR is new.
  }
  if (buf.length !== expected) {
    log.warn(
      { path, found: buf.length, expected },
      'weather mask: cached mask is the wrong size for this grid, rebuilding',
    );
    return null;
  }
  // Copy out of the Buffer so nothing downstream holds Node's read pool.
  return new Uint8Array(buf);
}

/**
 * In-flight and completed builds, keyed by cache path.
 *
 * Without this, two callers racing on a cold start each fetch the whole
 * continent — the one request burst this module is meant to make unrepeatable.
 * Rejections are evicted so a transient upstream failure doesn't poison the
 * key for the lifetime of the process.
 */
const inFlight = new Map<string, Promise<Uint8Array>>();

/**
 * How long to leave a failed build alone before trying again.
 *
 * Retrying immediately turned one bug into a quota fire. The dependent sources
 * poll every ten minutes, so a mask that could not build was re-attempted six
 * times an hour by EACH of marine and flood — thousands of locations an hour
 * spent on requests that could never succeed, which then starved the land grid
 * into HTTP 429 as well. A build that just failed will almost certainly fail
 * again a minute later; what is worth protecting is everything else sharing
 * the allowance.
 */
const FAILURE_COOLDOWN_MS = 30 * 60_000;
const failedAt = new Map<string, number>();

/** Drop the memoised masks. Tests only. */
export function _resetMaskCache(): void {
  inFlight.clear();
  failedAt.clear();
}

/** One byte per cell: 1 = ocean, 0 = land, in cell-index order. */
export async function loadMask(
  g: GridGeometry = gridGeometry(),
  opts: MaskOptions = {},
): Promise<Uint8Array> {
  const path = maskPath(g);
  const existing = inFlight.get(path);
  if (existing) return existing;

  const failed = failedAt.get(path);
  if (failed !== undefined && Date.now() - failed < FAILURE_COOLDOWN_MS) {
    throw new Error('weather mask: last build failed, cooling down before retrying');
  }

  const build = buildMask(g, path, opts);
  inFlight.set(path, build);
  build.then(
    () => { failedAt.delete(path); },
    () => { inFlight.delete(path); failedAt.set(path, Date.now()); },
  );
  return build;
}

async function buildMask(
  g: GridGeometry,
  path: string,
  opts: MaskOptions,
): Promise<Uint8Array> {
  const n = cellCount(g);
  const cached = await readCachedMask(path, n);
  if (cached) return cached;

  // Clamped to the elevation limit even when a caller asks for more.
  const requested = opts.batchSize ?? config.WEATHER_GRID_BATCH;
  const batches = batchCells(allCells(g), Math.min(requested, ELEVATION_MAX_COORDS));
  const pauseMs = opts.batchPauseMs ?? BATCH_PAUSE_MS;
  const startedAt = Date.now();

  const elevations: number[] = [];
  for (let b = 0; b < batches.length; b += 1) {
    if (b > 0 && pauseMs > 0) {
      await new Promise<void>((resolve) => {
        setTimeout(resolve, pauseMs);
      });
    }
    const batch = batches[b] as Array<{ lat: number; lon: number }>;
    // Through the shared per-minute allowance like every other weather fetch.
    // This module predates the pacer and was burst-sending ~5,900 locations.
    await reserveLocations(batch.length);
    elevations.push(...(await fetchElevations(batch)));
  }

  const mask = classifyOcean(g, elevations);
  const ocean = oceanCellIndices(mask).length;

  try {
    await writeMask(path, mask);
  } catch (err) {
    // The mask is already in memory and correct, so the field still works this
    // run. Failing the whole load over a disk problem would take the layer
    // down for a fault whose only real cost is rebuilding on the next boot.
    log.error({ err, path }, 'weather mask: built the mask but could not cache it');
  }

  log.info(
    {
      cells: n,
      ocean,
      land: n - ocean,
      stepDeg: g.stepDeg,
      requests: batches.length,
      ms: Date.now() - startedAt,
      path,
    },
    'weather mask: built the land/sea mask (one time)',
  );
  return mask;
}
