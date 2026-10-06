/**
 * Marine wave fields: wave height, direction, period and swell.
 *
 * Deliberately a separate source from the land grid, for two reasons.
 *
 * It runs on its own, COARSER grid (see `marineGeometry`): matching the land
 * grid would roughly double the daily spend and leave the free tier, and swell
 * varies over hundreds of kilometres anyway, so the detail would be mostly
 * interpolation of itself.
 *
 * And it only asks about OCEAN cells, using the land/sea mask. Two-thirds of
 * the bounding box is land, and the Marine API returns nothing useful for a
 * point in the middle of the desert — asking anyway would spend a third of the
 * budget on guaranteed nulls.
 *
 * Registered separately so that marine failing (a mask that cannot be built, an
 * upstream outage) leaves the land field — the part most people came for —
 * running untouched.
 */
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import {
  MARINE_VARS, VAR_SCALE, NODATA, type GridVar,
  allCells, batchCells, cellCount, marineGeometry, quantiseOne,
} from './weatherGrid.js';
import { loadMask, oceanCellIndices } from './weatherMask.js';
import {
  mergeManifest,
  readManifest, writeGrid, writeManifest, manifestIsFresh, manifestCovers,
  type ManifestVar, type WeatherManifest,
} from '../services/weatherStore.js';
import { pacedFetch, pickTimesteps, toUtcIso, runBatches, batchConcurrency } from './weatherGridSource.js';

const MARINE_URL = config.OPEN_METEO_MARINE_URL;

const UNITS: Readonly<Record<string, string>> = {
  wave_height: 'm',
  wave_direction: '°',
  wave_period: 's',
  swell_wave_height: 'm',
  swell_wave_direction: '°',
  swell_wave_period: 's',
  ocean_current_velocity: 'm/s',
  ocean_current_direction: '°',
};

interface HourlyBlock { time?: unknown; [key: string]: unknown }
interface MarinePoint { hourly?: HourlyBlock }

const sleep = (ms: number) => new Promise<void>((r) => { setTimeout(r, ms); });

function asNumber(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

function buildUrl(cells: ReadonlyArray<{ lat: number; lon: number }>): string {
  const params = new URLSearchParams({
    latitude: cells.map((c) => c.lat.toFixed(4)).join(','),
    longitude: cells.map((c) => c.lon.toFixed(4)).join(','),
    hourly: MARINE_VARS.join(','),
    timezone: 'UTC',
    past_days: String(config.WEATHER_PAST_DAYS),
    forecast_days: String(config.WEATHER_FORECAST_DAYS),
  });
  if (config.OPEN_METEO_MARINE_MODELS) params.set('models', config.OPEN_METEO_MARINE_MODELS);
  return `${MARINE_URL}?${params.toString()}`;
}

/**
 * Fetch and store the marine grids, then fold them into the existing manifest.
 *
 * Marine runs AFTER the land grid and amends its manifest rather than writing
 * its own, so the client makes one manifest request and gets one time axis.
 * Without a land manifest there is nothing to amend and nothing to align to, so
 * this does nothing rather than inventing a second axis that might not match.
 */
export async function refreshMarineGrid(force = false): Promise<WeatherManifest | null> {
  const base = await readManifest();
  if (!base) {
    log.info('marine grid: no land manifest yet, skipping until the land field exists');
    return null;
  }

  const alreadyHasMarine = manifestCovers(base, MARINE_VARS, (v) => !!v.marine);
  if (!force && alreadyHasMarine && manifestIsFresh(base, config.WEATHER_GRID_INTERVAL_MS)) {
    log.info({ issuedAt: base.issuedAt }, 'marine grid: stored dataset still current');
    return base;
  }

  const geometry = marineGeometry();
  const total = cellCount(geometry);
  const cells = allCells(geometry);

  const mask = await loadMask(geometry);
  const oceanIdx = oceanCellIndices(mask);
  if (oceanIdx.length === 0) {
    log.warn('marine grid: mask found no ocean cells, refusing to build an empty field');
    return base;
  }

  // Full-size planes pre-filled with NODATA: inland cells are never fetched and
  // must read as absent, not as a flat calm sea across the continent.
  const planes = new Map<GridVar, Int16Array[]>();
  let timesteps: string[] = [];
  let pick: number[] = [];

  const oceanCells = oceanIdx.map((i) => cells[i]!);
  const batches = batchCells(oceanCells, config.WEATHER_GRID_BATCH);

  await runBatches(batches, batchConcurrency(MARINE_URL), async (batch, b, offset) => {
    const data = await pacedFetch<MarinePoint | MarinePoint[]>(buildUrl(batch), batch.length);
    const points = Array.isArray(data) ? data : [data];
    if (points.length !== batch.length) {
      throw new Error(
        `marine grid: batch ${b} asked for ${batch.length} locations, got ${points.length}`,
      );
    }

    for (let i = 0; i < points.length; i += 1) {
      // Position within the OCEAN sequence maps back to a grid index via the
      // mask. Using the loop counter directly would write wave data into land
      // cells and leave the sea empty.
      const gridIndex = oceanIdx[offset + i]!;
      const hourly = points[i]?.hourly;
      if (!hourly) continue;

      if (timesteps.length === 0) {
        const times = Array.isArray(hourly.time) ? (hourly.time as string[]) : [];
        const picked = pickTimesteps(times);
        pick = picked.indices;
        timesteps = picked.timesteps;
        for (const v of MARINE_VARS) {
          planes.set(v, timesteps.map(() => new Int16Array(total).fill(NODATA)));
        }
      }

      for (const v of MARINE_VARS) {
        const series = hourly[v];
        if (!Array.isArray(series)) continue;
        const arrays = planes.get(v);
        if (!arrays) continue;
        for (let t = 0; t < pick.length; t += 1) {
          arrays[t]![gridIndex] = quantiseOne(asNumber(series[pick[t]!]), v as GridVar);
        }
      }
    }

  });

  if (timesteps.length === 0) {
    log.warn('marine grid: upstream returned no time axis');
    return base;
  }

  for (const [v, arrays] of planes) {
    for (let t = 0; t < arrays.length; t += 1) {
      await writeGrid(v, timesteps[t]!, arrays[t]!);
    }
  }

  const marineVars: ManifestVar[] = MARINE_VARS.map((name) => ({
    name,
    scale: VAR_SCALE[name],
    unit: UNITS[name] ?? '',
    marine: true,
    flood: false,
    air: false,
  }));

  // Merged against the manifest AS IT IS NOW, not the `base` read before the
  // fetches: another source may have written while this one was running, and
  // building on the stale copy erased its section.
  const merged = await mergeManifest((current) => {
    const b = current ?? base;
    return {
      ...b,
      marineGeometry: geometry,
      // Marine timesteps are its own: the two APIs are asked for the same
      // window but a mismatch must not silently make the client read a land
      // timestep off a marine grid.
      marineTimesteps: timesteps,
      vars: [...b.vars.filter((v) => !v.marine), ...marineVars],
    };
  });

  log.info(
    { oceanCells: oceanIdx.length, of: total, timesteps: timesteps.length, requests: batches.length },
    'marine grid: refreshed',
  );
  return merged;
}

export function registerMarineGridSource(): void {
  registerSource<WeatherManifest | null>({
    name: 'weather_marine',
    family: 'misc',
    // Short, NOT the daily interval. This source needs the land manifest to
    // exist, and on a cold boot it does not for the first quarter of an hour —
    // at a daily cadence, missing that window means missing the whole day. The
    // coverage + freshness guard makes every call after the first a cheap
    // no-op, so polling often costs nothing.
    intervalMs: config.WEATHER_DEPENDENT_INTERVAL_MS,
    fetch: () => refreshMarineGrid(false),
  });
}

export { toUtcIso };
