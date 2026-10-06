/**
 * Air quality: particulates and the US AQI, from Open-Meteo's air-quality API.
 *
 * A separate source from the land grid for the same reasons marine is, with one
 * difference that matters.
 *
 * It runs on its own, COARSE grid (`airGeometry`) — coarser even than marine.
 * Smoke, dust and haze are regional: a plume that matters is hundreds of
 * kilometres across, and nobody reads a smoke layer to find out which paddock
 * the haze starts in. Matching the land grid would multiply the daily spend for
 * detail that would be interpolation of itself, and the whole point of the
 * budget arithmetic in weatherGrid.ts is that resolution is the bill.
 *
 * But unlike marine and flood, it uses NO land/sea mask. Marine asks only about
 * ocean and flood only about land because each is meaningless on the other half;
 * air is meaningful everywhere in the box, including over water where the smoke
 * blowing out to sea is exactly what a coastal reader wants to see. Masking here
 * would carve holes in a field that genuinely is continuous.
 *
 * Registered separately so that air failing — an upstream outage, a changed
 * variable name — leaves the land, marine and flood fields running.
 */
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { registerSource } from '../services/sourceRegistry.js';
import {
  AIR_VARS, VAR_SCALE, NODATA, type GridVar,
  airGeometry, allCells, batchCells, cellCount, quantiseOne,
} from './weatherGrid.js';
import {
  geometryMatches, sectionIsFresh, manifestCovers,
  mergeManifest,
  readManifest, writeGrid, writeManifest, manifestIsFresh,
  type ManifestVar, type WeatherManifest,
} from '../services/weatherStore.js';
import { pacedFetch, pickTimesteps, runBatches, batchConcurrency } from './weatherGridSource.js';

const AIR_URL = config.OPEN_METEO_AIR_URL;

const UNITS: Readonly<Record<string, string>> = {
  pm2_5: 'µg/m³',
  pm10: 'µg/m³',
  us_aqi: 'AQI',
  uv_index: '',
};

interface HourlyBlock { time?: unknown; [key: string]: unknown }
interface AirPoint { hourly?: HourlyBlock }

function asNumber(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

function buildUrl(cells: ReadonlyArray<{ lat: number; lon: number }>): string {
  const params = new URLSearchParams({
    latitude: cells.map((c) => c.lat.toFixed(4)).join(','),
    longitude: cells.map((c) => c.lon.toFixed(4)).join(','),
    hourly: AIR_VARS.join(','),
    timezone: 'UTC',
    past_days: String(config.WEATHER_PAST_DAYS),
    forecast_days: String(config.WEATHER_FORECAST_DAYS),
  });
  if (config.OPEN_METEO_AIR_MODELS) params.set('models', config.OPEN_METEO_AIR_MODELS);
  return `${AIR_URL}?${params.toString()}`;
}

/**
 * Fetch and store the air grids, then fold them into the existing manifest.
 *
 * Like marine, this amends the land manifest rather than writing its own, so the
 * client makes one manifest request. Without a land manifest there is nothing to
 * amend, so this does nothing rather than inventing a half-manifest describing
 * only air.
 */
export async function refreshAirGrid(force = false): Promise<WeatherManifest | null> {
  const base = await readManifest();
  if (!base) {
    log.info('air grid: no land manifest yet, skipping until the land field exists');
    return null;
  }

  // Coverage of the whole list, not "has any air variable": when uv_index
  // joined this source, an any-check would have called the old three-variable
  // dataset complete and left the UV layer with no grids behind it.
  const alreadyHasAir = manifestCovers(base, AIR_VARS, (v) => !!v.air);
  const geometry = airGeometry();
  if (!force && alreadyHasAir && geometryMatches(base.airGeometry, geometry)
    && sectionIsFresh(base.airIssuedAt, config.WEATHER_GRID_INTERVAL_MS)) {
    log.info({ issuedAt: base.airIssuedAt }, 'air grid: stored dataset still current');
    return base;
  }

  const total = cellCount(geometry);
  const cells = allCells(geometry);

  const planes = new Map<GridVar, Int16Array[]>();
  let timesteps: string[] = [];
  let pick: number[] = [];

  const batches = batchCells(cells, config.WEATHER_GRID_BATCH);

  await runBatches(batches, batchConcurrency(AIR_URL), async (batch, b, offset) => {
    // Always through the pacer, never fetchJson directly: land, marine, flood
    // and air are prewarmed together and share one per-minute allowance. The
    // first production run learned what happens when a source spends it alone.
    const data = await pacedFetch<AirPoint | AirPoint[]>(buildUrl(batch), batch.length);
    const points = Array.isArray(data) ? data : [data];
    if (points.length !== batch.length) {
      // A short response accepted silently would shift every later cell one
      // place west — a plausible-looking map of the wrong places.
      throw new Error(
        `air grid: batch ${b} asked for ${batch.length} locations, got ${points.length}`,
      );
    }

    for (let i = 0; i < points.length; i += 1) {
      // Position in the overall sequence, not a count of successes: a point that
      // comes back empty must still consume its index and stay NODATA.
      const cellIndex = offset + i;
      const hourly = points[i]?.hourly;
      if (!hourly) continue;

      if (timesteps.length === 0) {
        const times = Array.isArray(hourly.time) ? (hourly.time as string[]) : [];
        const picked = pickTimesteps(times);
        pick = picked.indices;
        timesteps = picked.timesteps;
        // Pre-filled with NODATA so a cell the upstream declines to answer for
        // reads as absent rather than as pristine air.
        for (const v of AIR_VARS) {
          planes.set(v, timesteps.map(() => new Int16Array(total).fill(NODATA)));
        }
      }

      for (const v of AIR_VARS) {
        const series = hourly[v];
        if (!Array.isArray(series)) continue;
        const arrays = planes.get(v);
        if (!arrays) continue;
        for (let t = 0; t < pick.length; t += 1) {
          arrays[t]![cellIndex] = quantiseOne(asNumber(series[pick[t]!]), v);
        }
      }
    }

  });

  if (timesteps.length === 0) {
    log.warn('air grid: upstream returned no time axis');
    return base;
  }

  for (const [v, arrays] of planes) {
    for (let t = 0; t < arrays.length; t += 1) {
      await writeGrid(v, timesteps[t]!, arrays[t]!);
    }
  }

  const airVars: ManifestVar[] = AIR_VARS.map((name) => ({
    name,
    scale: VAR_SCALE[name],
    unit: UNITS[name] ?? '',
    marine: false,
    flood: false,
    air: true,
  }));

  // Merged against the manifest AS IT IS NOW — see marine for why. This exact
  // source was the one that erased marine in production: it read the manifest
  // before marine wrote, and wrote after.
  const merged = await mergeManifest((current) => {
    const b = current ?? base;
    return {
      ...b,
      airGeometry: geometry,
      airIssuedAt: new Date().toISOString(),
      // Its own axis, for the same reason marine keeps one: the two APIs are
      // asked for the same window, and a mismatch must not quietly have the
      // client read a land timestep off an air grid.
      airTimesteps: timesteps,
      vars: [...b.vars.filter((v) => !v.air), ...airVars],
    };
  });

  log.info(
    { cells: total, timesteps: timesteps.length, requests: batches.length },
    'air grid: refreshed',
  );
  return merged;
}

export function registerAirGridSource(): void {
  registerSource<WeatherManifest | null>({
    name: 'weather_air',
    family: 'misc',
    // Short, NOT the daily interval. This source needs the land manifest to
    // exist, and on a cold boot it does not for the first quarter of an hour —
    // at a daily cadence, missing that window means missing the whole day. The
    // coverage + freshness guard makes every call after the first a cheap
    // no-op, so polling often costs nothing.
    intervalMs: config.WEATHER_DEPENDENT_INTERVAL_MS,
    fetch: () => refreshAirGrid(false),
  });
}
