/**
 * Fetches the gridded weather field from Open-Meteo and stores it.
 *
 * This is the only code in the project that spends the Open-Meteo budget at
 * scale, so the arithmetic in weatherGrid.ts governs everything here:
 *
 *   - One request per cell carries the WHOLE series (past_days + forecast_days),
 *     so the timeline is free and only cell count x refresh rate costs anything.
 *   - Cells are batched into Open-Meteo's comma-separated coordinate form, so a
 *     full refresh is ~24 HTTP requests rather than 5,865.
 *   - A stored dataset younger than the refresh interval short-circuits the
 *     whole thing, because the poller fires sources on startup and a restart
 *     must not re-spend a day's quota.
 *
 * TIMEZONE
 * Requests are made in UTC and timesteps are stored as UTC instants. Australia
 * spans three time zones (four in summer), so there is no single local clock
 * that makes sense for a continental grid — "09:00" would mean two different
 * moments at opposite ends of the same field. The client formats into the
 * viewer's own zone.
 */
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import {
  LAND_VARS, VAR_SCALE, type GridVar,
  allCells, batchCells, cellCount, gridGeometry, quantiseOne, reportSpend,
  reserveLocations,
} from './weatherGrid.js';
import {
  pruneGrids, readManifest, writeGrid, writeManifest,
  manifestIsFresh, manifestCovers, type ManifestVar, type WeatherManifest,
} from '../services/weatherStore.js';

const FORECAST_URL = 'https://api.open-meteo.com/v1/forecast';

/** Hours between stored timesteps. */
export const STEP_HOURS = 3;

const UNITS: Readonly<Record<string, string>> = {
  temperature_2m: '°C',
  apparent_temperature: '°C',
  relative_humidity_2m: '%',
  precipitation: 'mm',
  pressure_msl: 'hPa',
  uv_index: '',
  cape: 'J/kg',
  wind_speed_10m: 'km/h',
  wind_direction_10m: '°',
  wind_gusts_10m: 'km/h',
};

interface HourlyBlock {
  time?: unknown;
  [key: string]: unknown;
}
interface OpenMeteoPoint {
  hourly?: HourlyBlock;
}

const sleep = (ms: number) => new Promise<void>((r) => { setTimeout(r, ms); });

/**
 * Fetch one batch, pacing first and surviving a rate limit.
 *
 * Shared by the land, marine and flood sources so there is one place that
 * knows how to talk to Open-Meteo at scale.
 *
 * A 429 is retried rather than thrown, because throwing discards every
 * location already spent on this run — the most expensive possible response to
 * being told to slow down. The pacer should prevent it; this is what happens
 * when the pacer is wrong, which it has been once already.
 */
export async function pacedFetch<T>(url: string, locations: number): Promise<T> {
  const MAX_ATTEMPTS = 4;
  for (let attempt = 1; ; attempt += 1) {
    await reserveLocations(locations);
    try {
      return await fetchJson<T>(url, {
        headers: { 'User-Agent': 'AusAware/1.0 (+https://nswpsn.forcequit.xyz)' },
      });
    } catch (err) {
      const status = (err as { status?: number | null }).status ?? null;
      if (status !== 429 || attempt >= MAX_ATTEMPTS) throw err;
      // Wait out a whole window before trying again: a 429 means the last
      // minute is already spent, so anything shorter just earns another one.
      const waitMs = 60_000 * attempt;
      log.warn(
        { attempt, waitMs, locations },
        'weather: rate limited by Open-Meteo, backing off',
      );
      await sleep(waitMs);
    }
  }
}

function asNumber(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

/**
 * Indices of the hourly series to keep, and the instants they fall on.
 *
 * Open-Meteo returns hourly; the map shows 3-hourly. A continental gradient
 * does not read any differently at one-hour resolution, and keeping every hour
 * would triple both the stored bytes and the number of files for no visible
 * gain. The click-readout endpoint serves the full hourly series for a single
 * point, which is where that detail is actually wanted.
 */
export function pickTimesteps(times: readonly string[], stepHours = STEP_HOURS): {
  indices: number[];
  timesteps: string[];
} {
  const indices: number[] = [];
  const timesteps: string[] = [];
  for (let i = 0; i < times.length; i += stepHours) {
    const t = times[i];
    if (typeof t !== 'string') continue;
    const iso = toUtcIso(t);
    if (iso === null) continue;
    indices.push(i);
    timesteps.push(iso);
  }
  return { indices, timesteps };
}

/**
 * Open-Meteo's time strings to real UTC instants.
 *
 * With `timezone=UTC` it returns naive stamps like `2026-10-06T00:00` — no
 * zone designator at all. `new Date()` reads a bare stamp as LOCAL time, so
 * parsing it directly would silently shift the whole time axis by the server's
 * offset, which in Sydney is 10 or 11 hours. The field would be labelled with
 * the wrong times and the "now" marker would land in the wrong place.
 */
export function toUtcIso(t: string): string | null {
  let s = t.trim();
  if (!/[Zz]$/.test(s) && !/[+-]\d\d:?\d\d$/.test(s)) {
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(s)) s = `${s}:00Z`;
    else if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(s)) s = `${s}Z`;
    else return null;
  }
  const ms = Date.parse(s);
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
}

function buildUrl(cells: ReadonlyArray<{ lat: number; lon: number }>): string {
  const lats = cells.map((c) => c.lat.toFixed(4)).join(',');
  const lons = cells.map((c) => c.lon.toFixed(4)).join(',');
  const params = new URLSearchParams({
    latitude: lats,
    longitude: lons,
    hourly: LAND_VARS.join(','),
    timezone: 'UTC',
    past_days: String(config.WEATHER_PAST_DAYS),
    forecast_days: String(config.WEATHER_FORECAST_DAYS),
  });
  return `${FORECAST_URL}?${params.toString()}`;
}

/**
 * Fetch, quantise and store one full grid.
 *
 * Returns the manifest, which becomes the LiveStore snapshot — small enough to
 * persist as JSON, unlike the ~5 MB of grids it describes.
 */
export async function refreshWeatherGrid(force = false): Promise<WeatherManifest> {
  const existing = await readManifest();
  // Covers the variable list as well as the age — a deploy that adds a
  // variable has to invalidate the cache, or the new layer has no grids
  // behind it until tomorrow.
  const covered = manifestCovers(existing, LAND_VARS, (v) => !v.marine && !v.flood && !v.air);
  if (!force && covered && manifestIsFresh(existing, config.WEATHER_GRID_INTERVAL_MS)) {
    log.info({ issuedAt: existing!.issuedAt }, 'weather grid: stored dataset still current, not refetching');
    return existing!;
  }

  const geometry = gridGeometry();
  const cells = allCells(geometry);
  const total = cellCount(geometry);
  const batches = batchCells(cells, config.WEATHER_GRID_BATCH);
  const spend = reportSpend();

  // Allocated up front, one Int16Array per (variable, timestep), filled as
  // batches land. The alternative — holding every batch's JSON until the end —
  // peaks far higher for no benefit, and this process runs with a heap ceiling.
  let timesteps: string[] = [];
  let pick: number[] = [];
  const planes = new Map<GridVar, Int16Array[]>();
  let cellBase = 0;
  let filled = 0;

  for (let b = 0; b < batches.length; b += 1) {
    const batch = batches[b]!;
    const data = await pacedFetch<OpenMeteoPoint | OpenMeteoPoint[]>(
      buildUrl(batch), batch.length,
    );
    const points = Array.isArray(data) ? data : [data];

    if (points.length !== batch.length) {
      // Silently accepting a short response would shift every subsequent cell
      // by one and skew the entire field — a wrong map rather than no map.
      throw new Error(
        `weather grid: batch ${b} asked for ${batch.length} locations, got ${points.length}`,
      );
    }

    for (let i = 0; i < points.length; i += 1) {
      // Position in the overall sequence, never a count of successes. A point
      // that comes back without an hourly block must still consume its index
      // and stay NODATA — advancing only on success would slide every cell
      // after the gap one place west and skew the whole field.
      const cellIndex = cellBase + i;
      const hourly = points[i]?.hourly;
      if (!hourly) continue;

      if (timesteps.length === 0) {
        const times = Array.isArray(hourly.time) ? (hourly.time as string[]) : [];
        const picked = pickTimesteps(times);
        pick = picked.indices;
        timesteps = picked.timesteps;
        for (const v of LAND_VARS) {
          planes.set(v, timesteps.map(() => new Int16Array(total).fill(-32768)));
        }
      }

      for (const v of LAND_VARS) {
        const series = hourly[v];
        if (!Array.isArray(series)) continue;
        const arrays = planes.get(v);
        if (!arrays) continue;
        for (let t = 0; t < pick.length; t += 1) {
          arrays[t]![cellIndex] = quantiseOne(asNumber(series[pick[t]!]), v as GridVar);
        }
      }
      filled += 1;
    }

    cellBase += batch.length;
  }

  if (timesteps.length === 0) throw new Error('weather grid: upstream returned no time axis');

  for (const [v, arrays] of planes) {
    for (let t = 0; t < arrays.length; t += 1) {
      await writeGrid(v, timesteps[t]!, arrays[t]!);
    }
  }

  const vars: ManifestVar[] = LAND_VARS.map((name) => ({
    name,
    scale: VAR_SCALE[name],
    unit: UNITS[name] ?? '',
    marine: false,
  }));

  const manifest: WeatherManifest = {
    issuedAt: new Date().toISOString(),
    geometry,
    timesteps,
    // Marine is a separate source on its own grid and time axis, and it folds
    // itself into this manifest. Rebuilding `vars` from LAND_VARS alone would
    // drop it — and because prune below keeps only what the manifest lists,
    // that would delete every marine grid on disk and force a full refetch the
    // same day. Carry whatever marine already knows about straight through.
    vars: [...vars, ...(existing?.vars.filter((v) => v.marine || v.flood || v.air) ?? [])],
    nodata: -32768,
    ...(existing?.marineGeometry ? { marineGeometry: existing.marineGeometry } : {}),
    ...(existing?.marineTimesteps ? { marineTimesteps: existing.marineTimesteps } : {}),
    ...(existing?.floodGeometry ? { floodGeometry: existing.floodGeometry } : {}),
    ...(existing?.floodTimesteps ? { floodTimesteps: existing.floodTimesteps } : {}),
    ...(existing?.airGeometry ? { airGeometry: existing.airGeometry } : {}),
    ...(existing?.airTimesteps ? { airTimesteps: existing.airTimesteps } : {}),
  };
  await writeManifest(manifest);
  await pruneGrids(manifest);

  log.info(
    {
      cells: total, filled, timesteps: timesteps.length,
      requests: batches.length, locationsPerDay: spend.locationsPerDay,
    },
    'weather grid: refreshed',
  );
  return manifest;
}

export function registerWeatherGridSource(): void {
  registerSource<WeatherManifest>({
    name: 'weather_grid',
    family: 'misc',
    intervalMs: config.WEATHER_GRID_INTERVAL_MS,
    fetch: () => refreshWeatherGrid(false),
  });
}
