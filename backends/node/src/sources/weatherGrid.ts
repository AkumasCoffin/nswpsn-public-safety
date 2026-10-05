/**
 * Gridded weather for the Windy-style map layer.
 *
 * The existing `weather_current` source fetches ~100 named NSW towns and
 * renders them as discrete pins. This is the other shape of the same upstream:
 * a regular lat/lon grid over Australia, every cell carrying a time series, so
 * the client can draw a continuous field and scrub through it.
 *
 * THE BUDGET IS THE DESIGN
 * Open-Meteo's free tier is 10,000 calls/day and 300,000/month. A field is just
 * a grid of points, so resolution x refresh rate is the entire budget. Two
 * things keep this affordable:
 *
 *   1. One request per point returns the WHOLE time series (past_days +
 *      forecast_days). The timeline is therefore free — lengthening it costs
 *      nothing. Spend is driven only by how many points and how often.
 *   2. Points are batched into few HTTP requests via Open-Meteo's
 *      comma-separated coordinate form.
 *
 * Open-Meteo weights a call by variables and time span — "more than 10 weather
 * variables or ... more than 2 weeks for a single location" counts as multiple.
 * We request 6 variables over 9 days, so each request weighs one call. What the
 * docs do NOT say is whether a multi-location request counts once or once per
 * location. The defaults here are deliberately safe under BOTH readings:
 *
 *   per-location:  5,865/day, 176k/month   (vs 10k/day, 300k/month)
 *   per-request:   ~6/day
 *
 * `estimateSpend()` reports both so the real number is observable rather than
 * assumed, and GRID_STEP_DEG is config so the grid can be tightened once actual
 * usage is known. Never raise the resolution without re-running that estimate —
 * `weatherGrid.test.ts` asserts it against the ceiling for exactly this reason.
 */
import { config } from '../config.js';
import { log } from '../lib/log.js';

/** Australia, generously bounded: Cape York to Tasmania, Shark Bay to Norfolk. */
export const AU_BBOX = {
  west: 112,
  east: 154,
  south: -44,
  north: -10,
} as const;

/** Open-Meteo free-tier ceilings, for the budget estimate. */
export const FREE_TIER = { perDay: 10_000, perMonth: 300_000 } as const;

/**
 * Variables pulled for every land cell. Six, deliberately: at eleven the
 * request starts counting as more than one call.
 */
export const LAND_VARS = [
  'temperature_2m',
  'apparent_temperature',
  'precipitation',
  'wind_speed_10m',
  'wind_direction_10m',
  'wind_gusts_10m',
] as const;

/** Marine variables, ocean cells only. */
export const MARINE_VARS = [
  'wave_height',
  'wave_direction',
  'wave_period',
  'swell_wave_height',
] as const;

export type LandVar = (typeof LAND_VARS)[number];
export type MarineVar = (typeof MARINE_VARS)[number];
export type GridVar = LandVar | MarineVar;

/**
 * How each variable is packed into an Int16.
 *
 * `stored = round(real * scale)`, so `scale` sets the precision and has to keep
 * the whole plausible range inside +/-32767. Temperature at scale 10 covers
 * -3276..3276 degrees; direction at scale 10 covers 0..360 to a tenth. A value
 * that is genuinely absent (wave height inland) is NODATA, which is distinct
 * from a real zero — rendering must treat them differently.
 */
export const NODATA = -32768;

export const VAR_SCALE: Readonly<Record<GridVar, number>> = {
  temperature_2m: 10,
  apparent_temperature: 10,
  precipitation: 100,
  wind_speed_10m: 10,
  wind_direction_10m: 10,
  wind_gusts_10m: 10,
  wave_height: 100,
  wave_direction: 10,
  wave_period: 10,
  swell_wave_height: 100,
};

export interface GridGeometry {
  west: number;
  south: number;
  stepDeg: number;
  /** Cells across (longitude). */
  cols: number;
  /** Cells down (latitude). */
  rows: number;
}

/**
 * Grid geometry from the configured step.
 *
 * Inclusive of both edges: a 42-degree span at 0.5 gives 85 columns, not 84.
 * Dropping the far edge would leave a half-cell strip of Australia's east coast
 * outside the field, which is where most of the audience lives.
 */
export function gridGeometry(stepDeg = config.WEATHER_GRID_STEP): GridGeometry {
  if (!(stepDeg > 0)) throw new Error(`weather grid step must be positive, got ${stepDeg}`);
  const cols = Math.floor((AU_BBOX.east - AU_BBOX.west) / stepDeg) + 1;
  const rows = Math.floor((AU_BBOX.north - AU_BBOX.south) / stepDeg) + 1;
  return { west: AU_BBOX.west, south: AU_BBOX.south, stepDeg, cols, rows };
}

export function cellCount(g: GridGeometry): number {
  return g.cols * g.rows;
}

/**
 * The marine grid, which is deliberately coarser than the land grid.
 *
 * Not a shortcut — two reasons. Budget: marine on the same 0.5 degree grid
 * would cost up to another 5,865 locations a day, and 11,730 combined is past
 * the free tier even before a single retry. And physics: swell is a far
 * smoother field than land temperature. It varies over hundreds of kilometres,
 * so the detail a tighter grid would buy is mostly interpolation of itself.
 *
 * Worst case at 1 degree — every cell ocean — is 1,505, which keeps the
 * combined daily spend inside the ceiling without relying on the land/sea mask
 * to come out any particular way.
 */
export function marineGeometry(stepDeg = config.WEATHER_MARINE_STEP): GridGeometry {
  return gridGeometry(stepDeg);
}

/**
 * Cell index -> coordinates. Row-major from the SOUTH-WEST corner, so index 0
 * is the bottom-left and rows run north. The client's renderer flips this when
 * it writes image rows, since canvas y grows downward.
 */
export function cellLatLon(g: GridGeometry, index: number): { lat: number; lon: number } {
  const row = Math.floor(index / g.cols);
  const col = index % g.cols;
  return {
    lat: g.south + row * g.stepDeg,
    lon: g.west + col * g.stepDeg,
  };
}

/** Every cell's coordinates, in index order. */
export function allCells(g: GridGeometry): Array<{ lat: number; lon: number }> {
  const out: Array<{ lat: number; lon: number }> = [];
  for (let i = 0; i < cellCount(g); i += 1) out.push(cellLatLon(g, i));
  return out;
}

export interface SpendEstimate {
  cells: number;
  /** Marine cells, counted at their WORST case — every cell ocean. */
  marineCells: number;
  /** HTTP requests per full refresh, after batching. */
  requestsPerRefresh: number;
  refreshesPerDay: number;
  /** If Open-Meteo counts each location. The conservative reading. */
  locationsPerDay: number;
  locationsPerMonth: number;
  /** If it counts each HTTP request. The optimistic reading. */
  requestsPerDay: number;
  /** True only when the conservative reading also fits. */
  withinFreeTier: boolean;
}

/**
 * What a given configuration actually costs, under both readings of the quota.
 *
 * Reported on startup and asserted in tests. The point is that raising the
 * resolution can never silently blow the quota: the number has to be looked at.
 */
export function estimateSpend(
  g: GridGeometry = gridGeometry(),
  batchSize = config.WEATHER_GRID_BATCH,
  refreshesPerDay = 86_400_000 / config.WEATHER_GRID_INTERVAL_MS,
  marine: GridGeometry | null = marineGeometry(),
): SpendEstimate {
  const cells = cellCount(g);
  // Counted at the worst case — every marine cell ocean. The real number is
  // lower because the land/sea mask excludes inland cells, but a budget that
  // only holds if the coastline comes out a particular way is not a budget.
  const marineCells = marine ? cellCount(marine) : 0;
  const perRefresh = cells + marineCells;
  const requestsPerRefresh = Math.ceil(cells / batchSize) + Math.ceil(marineCells / batchSize);
  const locationsPerDay = Math.ceil(perRefresh * refreshesPerDay);
  // 31 so a long month cannot be the thing that tips it over.
  const locationsPerMonth = locationsPerDay * 31;
  return {
    cells,
    marineCells,
    requestsPerRefresh,
    refreshesPerDay,
    locationsPerDay,
    locationsPerMonth,
    requestsPerDay: Math.ceil(requestsPerRefresh * refreshesPerDay),
    withinFreeTier:
      locationsPerDay <= FREE_TIER.perDay && locationsPerMonth <= FREE_TIER.perMonth,
  };
}

/** Log the budget on boot, loudly if the configuration does not fit. */
export function reportSpend(): SpendEstimate {
  const est = estimateSpend();
  const detail = {
    cells: est.cells,
    marineCells: est.marineCells,
    locationsPerDay: est.locationsPerDay,
    locationsPerMonth: est.locationsPerMonth,
    requestsPerDay: est.requestsPerDay,
    stepDeg: config.WEATHER_GRID_STEP,
    marineStepDeg: config.WEATHER_MARINE_STEP,
  };
  if (est.withinFreeTier) {
    log.info(detail, 'weather grid: within the Open-Meteo free tier');
  } else {
    log.warn(
      detail,
      'weather grid: configuration EXCEEDS the Open-Meteo free tier under ' +
        'per-location counting — widen WEATHER_GRID_STEP or lengthen ' +
        'WEATHER_GRID_INTERVAL_MS',
    );
  }
  return est;
}

/**
 * Pack one real into Int16. The scalar form of `quantise`.
 *
 * The grid fetcher calls this a few million times per refresh (cells x
 * timesteps x variables), so it exists to avoid allocating a one-element array
 * for each of them.
 */
export function quantiseOne(raw: number | null | undefined, v: GridVar): number {
  if (raw === null || raw === undefined || !Number.isFinite(raw)) return NODATA;
  const packed = Math.round(raw * VAR_SCALE[v]);
  return packed > 32767 ? 32767 : packed < -32767 ? -32767 : packed;
}

/** Pack reals into Int16 with the variable's scale. `null`/non-finite -> NODATA. */
export function quantise(values: ReadonlyArray<number | null | undefined>, v: GridVar): Int16Array {
  const scale = VAR_SCALE[v];
  const out = new Int16Array(values.length);
  for (let i = 0; i < values.length; i += 1) {
    const raw = values[i];
    if (raw === null || raw === undefined || !Number.isFinite(raw)) {
      out[i] = NODATA;
      continue;
    }
    const packed = Math.round(raw * scale);
    // Clamp rather than let it wrap: a wrapped Int16 renders as a wildly wrong
    // value in the middle of the field, which is worse than a clipped one.
    out[i] = packed > 32767 ? 32767 : packed < -32767 ? -32767 : packed;
  }
  return out;
}

/** Unpack, with NODATA becoming null. The inverse of `quantise`. */
export function dequantise(packed: Int16Array, v: GridVar): Array<number | null> {
  const scale = VAR_SCALE[v];
  const out: Array<number | null> = new Array(packed.length);
  for (let i = 0; i < packed.length; i += 1) {
    const raw = packed[i]!;
    out[i] = raw === NODATA ? null : raw / scale;
  }
  return out;
}

/** Split cells into batches for the comma-separated coordinate form. */
export function batchCells<T>(cells: readonly T[], batchSize = config.WEATHER_GRID_BATCH): T[][] {
  if (batchSize < 1) throw new Error(`batch size must be >= 1, got ${batchSize}`);
  const out: T[][] = [];
  for (let i = 0; i < cells.length; i += batchSize) {
    out.push(cells.slice(i, i + batchSize) as T[]);
  }
  return out;
}
