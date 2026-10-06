/**
 * River discharge from Open-Meteo's flood API (GloFAS).
 *
 * Flooding is one of the things this site exists to watch, so this is the one
 * weather layer whose point is not "what will it be like" but "is water coming".
 *
 * THREE THINGS ABOUT THIS API THAT SHAPE THE CODE
 *
 * 1. `forecast_days` DEFAULTS TO 92. Open-Meteo weights a call by time span —
 *    past two weeks a single location counts as multiple calls — so taking the
 *    default would make every location weigh roughly six and a half calls and
 *    put the whole feature an order of magnitude over budget. It is pinned
 *    explicitly below, and a test asserts the window stays inside two weeks.
 *
 * 2. It is DAILY, not hourly. So it carries its own time axis, like marine, and
 *    cannot be read against the 3-hourly land one.
 *
 * 3. It is a catchment quantity on a 5 km river network, sampled here at one
 *    degree. That is deliberately coarse for budget reasons, and it means a
 *    sample lands on whatever river happens to be nearest — Open-Meteo warns
 *    about exactly this. The client therefore draws discrete markers rather
 *    than a smooth field: smearing one river's flow across 110 km of landscape
 *    would look authoritative and be wrong.
 *
 * It runs on the LAND cells of the same grid marine takes the ocean cells of,
 * so the two partition one grid rather than costing two. That is what keeps the
 * combined daily spend flat at 7,370 instead of 8,875.
 */
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import {
  FLOOD_VARS, VAR_SCALE, NODATA, type GridVar,
  allCells, batchCells, cellCount, marineGeometry, quantiseOne,
} from './weatherGrid.js';
import { loadMask, LAND } from './weatherMask.js';
import {
  mergeManifest,
  readManifest, writeGrid, writeManifest, manifestIsFresh, manifestCovers,
  type ManifestVar, type WeatherManifest,
} from '../services/weatherStore.js';
import { pacedFetch, toUtcIso } from './weatherGridSource.js';

const FLOOD_URL = config.OPEN_METEO_FLOOD_URL;

/**
 * Hard ceiling on the requested window, in days.
 *
 * Open-Meteo: "more than 2 weeks for a single location are considered multiple
 * API calls". Staying at or under this is what keeps each request weighing one.
 */
export const MAX_WINDOW_DAYS = 14;

const UNITS: Readonly<Record<string, string>> = {
  river_discharge: 'm³/s',
  river_discharge_median: 'm³/s',
};

interface DailyBlock { time?: unknown; [key: string]: unknown }
interface FloodPoint { daily?: DailyBlock }

const sleep = (ms: number) => new Promise<void>((r) => { setTimeout(r, ms); });

function asNumber(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

/**
 * The window actually requested, clamped to stay inside one call's worth.
 *
 * Exported so the test can assert the clamp rather than trusting the config to
 * stay sane — this is the number that decides whether the layer costs 1x or 7x.
 */
export function floodWindow(
  wantPast = config.WEATHER_PAST_DAYS,
  wantForecast = config.WEATHER_FORECAST_DAYS,
): { pastDays: number; forecastDays: number } {
  const past = Math.max(0, Math.min(wantPast, MAX_WINDOW_DAYS - 1));
  const forecast = Math.max(1, Math.min(wantForecast, MAX_WINDOW_DAYS - past));
  return { pastDays: past, forecastDays: forecast };
}

function buildUrl(cells: ReadonlyArray<{ lat: number; lon: number }>): string {
  const { pastDays, forecastDays } = floodWindow();
  const params = new URLSearchParams({
    latitude: cells.map((c) => c.lat.toFixed(4)).join(','),
    longitude: cells.map((c) => c.lon.toFixed(4)).join(','),
    daily: FLOOD_VARS.join(','),
    timezone: 'UTC',
    past_days: String(pastDays),
    // Never omitted. The API default is 92 days, which would silently multiply
    // the cost of every single location by about six and a half.
    forecast_days: String(forecastDays),
  });
  return `${FLOOD_URL}?${params.toString()}`;
}

export async function refreshFloodGrid(force = false): Promise<WeatherManifest | null> {
  const base = await readManifest();
  if (!base) {
    log.info('flood grid: no land manifest yet, skipping until the land field exists');
    return null;
  }

  const alreadyHasFlood = manifestCovers(base, FLOOD_VARS, (v) => !!v.flood);
  if (!force && alreadyHasFlood && manifestIsFresh(base, config.WEATHER_GRID_INTERVAL_MS)) {
    log.info({ issuedAt: base.issuedAt }, 'flood grid: stored dataset still current');
    return base;
  }

  const geometry = marineGeometry();
  const total = cellCount(geometry);
  const cells = allCells(geometry);

  const mask = await loadMask(geometry);
  // The complement of the marine selection: rivers are on land, and asking the
  // flood API about open ocean spends budget on guaranteed nulls.
  const landIdx: number[] = [];
  for (let i = 0; i < mask.length; i += 1) if (mask[i] === LAND) landIdx.push(i);
  if (landIdx.length === 0) {
    log.warn('flood grid: mask found no land cells, refusing to build an empty field');
    return base;
  }

  const planes = new Map<GridVar, Int16Array[]>();
  let timesteps: string[] = [];
  const landCells = landIdx.map((i) => cells[i]!);
  const batches = batchCells(landCells, config.WEATHER_GRID_BATCH);
  let seen = 0;

  for (let b = 0; b < batches.length; b += 1) {
    const batch = batches[b]!;
    const data = await pacedFetch<FloodPoint | FloodPoint[]>(buildUrl(batch), batch.length);
    const points = Array.isArray(data) ? data : [data];
    if (points.length !== batch.length) {
      throw new Error(
        `flood grid: batch ${b} asked for ${batch.length} locations, got ${points.length}`,
      );
    }

    for (let i = 0; i < points.length; i += 1) {
      // Position within the LAND sequence maps back through the mask. Using the
      // loop counter would write river flows into the ocean.
      const gridIndex = landIdx[seen + i]!;
      const daily = points[i]?.daily;
      if (!daily) continue;

      if (timesteps.length === 0) {
        const times = Array.isArray(daily.time) ? (daily.time as string[]) : [];
        // Daily stamps are plain dates ("2026-10-06"); every day is kept, since
        // there is no sub-daily detail to thin out.
        timesteps = times
          .map((t) => toUtcIso(typeof t === 'string' && t.length === 10 ? `${t}T00:00` : String(t)))
          .filter((t): t is string => t !== null);
        for (const v of FLOOD_VARS) {
          planes.set(v, timesteps.map(() => new Int16Array(total).fill(NODATA)));
        }
      }

      for (const v of FLOOD_VARS) {
        const series = daily[v];
        if (!Array.isArray(series)) continue;
        const arrays = planes.get(v);
        if (!arrays) continue;
        for (let t = 0; t < timesteps.length; t += 1) {
          arrays[t]![gridIndex] = quantiseOne(asNumber(series[t]), v as GridVar);
        }
      }
    }

    seen += batch.length;
  }

  if (timesteps.length === 0) {
    log.warn('flood grid: upstream returned no time axis');
    return base;
  }

  for (const [v, arrays] of planes) {
    for (let t = 0; t < arrays.length; t += 1) {
      await writeGrid(v, timesteps[t]!, arrays[t]!);
    }
  }

  const floodVars: ManifestVar[] = FLOOD_VARS.map((name) => ({
    name,
    scale: VAR_SCALE[name],
    unit: UNITS[name] ?? '',
    marine: false,
    flood: true,
  }));

  // Merged against the manifest AS IT IS NOW — see marine for why.
  const merged = await mergeManifest((current) => {
    const b = current ?? base;
    return {
      ...b,
      floodGeometry: geometry,
      floodTimesteps: timesteps,
      vars: [...b.vars.filter((v) => !v.flood), ...floodVars],
    };
  });

  log.info(
    {
      landCells: landIdx.length, of: total, days: timesteps.length,
      requests: batches.length, ...floodWindow(),
    },
    'flood grid: refreshed',
  );
  return merged;
}

export function registerFloodGridSource(): void {
  registerSource<WeatherManifest | null>({
    name: 'weather_flood',
    family: 'misc',
    // Short, NOT the daily interval. This source needs the land manifest to
    // exist, and on a cold boot it does not for the first quarter of an hour —
    // at a daily cadence, missing that window means missing the whole day. The
    // coverage + freshness guard makes every call after the first a cheap
    // no-op, so polling often costs nothing.
    intervalMs: config.WEATHER_DEPENDENT_INTERVAL_MS,
    fetch: () => refreshFloodGrid(false),
  });
}
