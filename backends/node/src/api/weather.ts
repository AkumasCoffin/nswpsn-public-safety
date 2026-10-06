/**
 * Weather endpoints.
 *
 *   GET /api/weather/current       — Open-Meteo current conditions for ~100
 *                                    NSW locations as a FeatureCollection
 *   GET /api/weather/radar         — RainViewer tile metadata pass-through
 *   GET /api/weather/grid/manifest — what the gridded field contains
 *   GET /api/weather/grid          — one variable at one timestep, as Int16
 *   GET /api/weather/point         — hourly series for a single coordinate
 *
 * The first two predate the gridded field and are untouched by it: the map
 * reads both today, and the station pins and the radar answer questions the
 * field does not (observed rain, named towns).
 */
import { Hono } from 'hono';
import {
  weatherCurrentSnapshot,
  weatherRadarSnapshot,
} from '../sources/weather.js';
import { LAND_VARS, MARINE_VARS, FLOOD_VARS, AIR_VARS, VAR_SCALE, type GridVar } from '../sources/weatherGrid.js';
import { readGridBytes, readManifest } from '../services/weatherStore.js';
import { SwrCache } from '../services/swrCache.js';
import { fetchJson } from '../sources/shared/http.js';
import { log } from '../lib/log.js';

export const weatherRouter = new Hono();

weatherRouter.get('/api/weather/current', (c) =>
  c.json(weatherCurrentSnapshot()),
);

weatherRouter.get('/api/weather/radar', (c) => c.json(weatherRadarSnapshot()));

// ---------------------------------------------------------------------------
// The gridded field
// ---------------------------------------------------------------------------

/**
 * Everything the client needs to interpret the binary grids: geometry, the
 * time axis, and each variable's scale. Fetched once per page load, then the
 * client pulls individual grids against it.
 */
weatherRouter.get('/api/weather/grid/manifest', async (c) => {
  const m = await readManifest();
  if (!m) {
    // Distinguishable from a server fault: the field simply has not been
    // fetched yet (first boot, or the cache was cleared). The client shows the
    // layer as unavailable rather than erroring.
    return c.json({ error: 'weather grid not built yet', ready: false }, 503);
  }
  // Short cache: the manifest only changes when a refresh lands, but a client
  // that holds a stale one asks for timesteps that no longer exist.
  c.header('Cache-Control', 'public, max-age=300');
  return c.json(m);
});

/**
 * One variable at one timestep: `cols * rows` little-endian Int16 values,
 * row-major from the south-west corner.
 *
 * Binary rather than JSON because this is the request the client makes
 * repeatedly while scrubbing the timeline. The same data as JSON numbers is
 * roughly six times the bytes and needs parsing; as Int16 it is ~11.7 KB that
 * goes straight into a typed array.
 */
weatherRouter.get('/api/weather/grid', async (c) => {
  const v = (c.req.query('var') ?? '').trim() as GridVar;
  const t = (c.req.query('t') ?? '').trim();

  const isLand = LAND_VARS.includes(v as (typeof LAND_VARS)[number]);
  const isMarine = MARINE_VARS.includes(v as (typeof MARINE_VARS)[number]);
  const isFlood = FLOOD_VARS.includes(v as (typeof FLOOD_VARS)[number]);
  const isAir = AIR_VARS.includes(v as (typeof AIR_VARS)[number]);
  if (!isLand && !isMarine && !isFlood && !isAir) {
    return c.json({ error: 'unknown variable' }, 400);
  }
  if (!t) return c.json({ error: 'missing timestep' }, 400);

  const m = await readManifest();
  if (!m) return c.json({ error: 'weather grid not built yet', ready: false }, 503);

  // Marine lives on its own coarser grid and its own time axis. Validating a
  // marine request against the land axis would reject perfectly good timesteps,
  // and returning land geometry for it would stretch the waves across the
  // continent at the wrong scale.
  const geometry = isFlood ? m.floodGeometry
    : isMarine ? m.marineGeometry
    : isAir ? m.airGeometry
    : m.geometry;
  const axis = isFlood ? m.floodTimesteps
    : isMarine ? m.marineTimesteps
    : isAir ? m.airTimesteps
    : m.timesteps;
  if (!geometry || !axis) {
    const which = isFlood ? 'flood' : isMarine ? 'marine' : 'air';
    return c.json({ error: `${which} field not built yet`, ready: false }, 503);
  }
  // Only timesteps the manifest advertises. Without this the timestep is a
  // caller-controlled string reaching a filename.
  if (!axis.includes(t)) return c.json({ error: 'unknown timestep' }, 404);

  const bytes = await readGridBytes(v, t);
  if (!bytes) return c.json({ error: 'grid not available' }, 404);

  c.header('Content-Type', 'application/octet-stream');
  // A given (variable, timestep) never changes once written — a later refresh
  // writes new timesteps rather than rewriting old ones — so this is safe to
  // cache hard, which is what makes scrubbing back and forth feel instant.
  c.header('Cache-Control', 'public, max-age=86400, immutable');
  c.header('X-Grid-Cols', String(geometry.cols));
  c.header('X-Grid-Rows', String(geometry.rows));
  c.header('X-Grid-Scale', String(VAR_SCALE[v]));
  return c.body(new Uint8Array(bytes));
});

/**
 * Full hourly series for one coordinate, for the click readout.
 *
 * Served live rather than from the grid: the grid is 3-hourly and sampled at
 * cell centres, and someone who clicks a specific place wants that place's
 * hourly numbers, not the nearest 55 km cell's. Cached because a popular spot
 * (a capital city) gets clicked repeatedly.
 */
// Bounded: a click readout is cheap to re-fetch, and an unbounded map keyed
// by coordinate is a slow memory leak on a long-running process.
const pointCache = new SwrCache<unknown>(500, 60 * 60_000);
const POINT_SWR = {
  fresh: 15 * 60_000,
  stale: 60 * 60_000,
  onError: (err: unknown) =>
    log.warn({ err: (err as Error).message }, 'weather point lookup failed'),
};

weatherRouter.get('/api/weather/point', async (c) => {
  const lat = Number(c.req.query('lat'));
  const lon = Number(c.req.query('lon'));
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return c.json({ error: 'lat and lon are required' }, 400);
  }
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    return c.json({ error: 'lat or lon out of range' }, 400);
  }

  // Rounded to the grid's own precision before it becomes a cache key, so
  // dragging a cursor a few metres does not miss the cache every time and
  // spend upstream calls on what is visually the same point.
  const key = `${lat.toFixed(2)},${lon.toFixed(2)}`;
  try {
    const { value: data } = await pointCache.get(key, async () => {
      const params = new URLSearchParams({
        latitude: lat.toFixed(2),
        longitude: lon.toFixed(2),
        hourly: LAND_VARS.join(','),
        timezone: 'UTC',
        forecast_days: '7',
      });
      return fetchJson<unknown>(`https://api.open-meteo.com/v1/forecast?${params.toString()}`, {
        headers: { 'User-Agent': 'AusAware/1.0 (+https://nswpsn.forcequit.xyz)' },
      });
    }, POINT_SWR);
    c.header('Cache-Control', 'public, max-age=900');
    return c.json(data as Record<string, unknown>);
  } catch {
    return c.json({ error: 'point lookup failed' }, 502);
  }
});
