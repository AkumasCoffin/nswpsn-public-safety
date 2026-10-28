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
 *
 * All five are also served at /api/v1/ (server.ts mirrors them). JSON is
 * wrapped in `data` there and errors take the shared shape (api/v1/
 * respond.ts); the current conditions are GeoJSON, bare on both paths, and a
 * grid is binary, the same bytes on both.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute, resolver } from 'hono-openapi';
import { config } from '../config.js';
import {
  weatherCurrentSnapshot,
  weatherRadarSnapshot,
  type WeatherSnapshot,
} from '../sources/weather.js';
import { LAND_VARS, MARINE_VARS, FLOOD_VARS, AIR_VARS, VAR_SCALE, type GridVar } from '../sources/weatherGrid.js';
import { readGridBytes, readManifest, type WeatherManifest } from '../services/weatherStore.js';
import { SwrCache } from '../services/swrCache.js';
import { fetchJson } from '../sources/shared/http.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, geo, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  ErrorBody,
  feature,
  GATED_ERRORS,
  okJson,
  PointGeometry,
  queryValidator,
  VALIDATION_ERROR,
  type SameShape,
} from './v1/schemas.js';

export const weatherRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_WEATHER = scoped({ source: 'weather', type: 'weather', window: 'live' });

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const errorJson = (description: string) => ({
  description,
  content: { 'application/json': { schema: resolver(ErrorBody) } },
});

/**
 * A parameter the handler needs, documented as required but never refused by
 * the query schema: the handler refuses it itself, in its own order and words,
 * which is what the old path has always answered. Nothing reads the caught
 * value; the handlers read c.req.query() as they always have.
 */
const neededByHandler = <S extends z.ZodType>(schema: S) => schema.catch(undefined as never);

const WeatherCurrent = z
  .object({
    type: z.literal('FeatureCollection'),
    features: z.array(
      feature(
        PointGeometry,
        z.object({
          name: z.string().describe('The town or place'),
          temperature: z.number().nullable().describe('Air temperature 2 m above the ground, °C'),
          feelsLike: z.number().nullable().describe('Apparent temperature, °C'),
          humidity: z.number().nullable().describe('Relative humidity 2 m above the ground, %'),
          windSpeed: z.number().nullable().describe('Wind speed 10 m above the ground, km/h'),
          windGusts: z.number().nullable().describe('Wind gusts 10 m above the ground, km/h'),
          windDirection: z.number().nullable().describe('Wind direction 10 m above the ground, degrees'),
          precipitation: z.number().describe('Precipitation, mm; 0 when Open-Meteo gives none'),
          weatherCode: z.number().describe('WMO weather code, as Open-Meteo gives it; 0 when it gives none'),
          weatherDescription: z
            .string()
            .describe('The code in words, e.g. "Partly Cloudy" or "Light Rain"; "Unknown" for a code without words'),
          weatherIcon: z.string().describe('An emoji for the code'),
        }),
      ),
    ),
  })
  .meta({ id: 'WeatherCurrent' });
assertShape<SameShape<z.infer<typeof WeatherCurrent>, WeatherSnapshot>>();

// RainViewer's list, passed through as it was read; only what the map relies
// on is named.
const RadarFrame = z
  .object({
    time: z.number().describe('The time the frame shows, unix seconds'),
    path: z.string().describe("The frame's path on RainViewer's tile server, to build its tile URLs from"),
  })
  .catchall(z.unknown());
const WeatherRadar = z
  .object({
    generated: z.number().optional(),
    host: z.string().optional(),
    radar: z
      .object({
        past: z.array(RadarFrame).describe('Radar frames already observed, oldest first'),
        nowcast: z.array(RadarFrame).describe('Frames forecast ahead, where RainViewer gives any'),
      })
      .catchall(z.unknown()),
  })
  .catchall(z.unknown())
  .meta({ id: 'WeatherRadar' });

const GridGeometrySchema = z.object({
  west: z.number().describe("Longitude of the grid's south-west point, degrees"),
  south: z.number().describe("Latitude of the grid's south-west point, degrees"),
  stepDeg: z.number().describe('Spacing between points, degrees, east-west and north-south alike'),
  cols: z.number().int().describe('Points across, west to east'),
  rows: z.number().int().describe('Points down, south to north'),
});

const GRID_VARS = [...LAND_VARS, ...MARINE_VARS, ...FLOOD_VARS, ...AIR_VARS] as const;

const AXIS = 'UTC instants, ISO 8601, oldest first';

const WeatherGridManifest = z
  .object({
    issuedAt: z.string().describe('When the land field was last fetched, ISO 8601'),
    geometry: GridGeometrySchema.describe('The land grid'),
    timesteps: z
      .array(z.string())
      .describe(`The land variables' time axis: ${AXIS}, three hours apart. The values \`t\` takes for a land variable`),
    vars: z
      .array(
        z.object({
          name: z.enum(GRID_VARS),
          scale: z.number().describe('A stored value divided by this is the real value'),
          unit: z.string().describe("The real value's unit; empty where it has none (UV index)"),
          marine: z
            .boolean()
            .describe('On the marine grid and axis (marineGeometry, marineTimesteps), with values over the sea only'),
          flood: z
            .boolean()
            .optional()
            .describe('River discharge, on the flood grid and its daily axis (floodGeometry, floodTimesteps), with values over land only'),
          air: z.boolean().optional().describe('Air quality or UV, on the air grid and axis (airGeometry, airTimesteps)'),
        }),
      )
      .describe('Every variable stored; a variable with none of marine, flood or air set is a land variable'),
    nodata: z.number().int().describe('The stored value that means "no value here"'),
    marineGeometry: GridGeometrySchema.optional(),
    marineTimesteps: z.array(z.string()).optional().describe(`${AXIS}, three hours apart`),
    floodGeometry: GridGeometrySchema.optional().describe('Built at the same spacing as marineGeometry'),
    marineIssuedAt: z.string().optional().describe('When the marine field was last fetched, ISO 8601'),
    floodIssuedAt: z.string().optional().describe('When the river discharge was last fetched, ISO 8601'),
    airIssuedAt: z.string().optional().describe('When the air field was last fetched, ISO 8601'),
    floodTimesteps: z.array(z.string()).optional().describe(`${AXIS}, one per day at 00:00 UTC`),
    airGeometry: GridGeometrySchema.optional(),
    airTimesteps: z.array(z.string()).optional().describe(`${AXIS}, three hours apart`),
  })
  .meta({ id: 'WeatherGridManifest' });
assertShape<SameShape<z.infer<typeof WeatherGridManifest>, WeatherManifest>>();

const WeatherGridQuery = z.object({
  var: neededByHandler(z.enum(GRID_VARS)).describe('The variable, a name from the manifest'),
  t: neededByHandler(z.string()).describe("A timestep from the manifest, exactly as written there, on the variable's own axis"),
});

// Open-Meteo's answer, passed through as it was read.
const WeatherPoint = z
  .object({
    hourly_units: z.record(z.string(), z.string()).describe('The unit of each series'),
    hourly: z
      .object({
        time: z.array(z.string()).describe('The hours, UTC, written without a zone, e.g. "2026-10-28T03:00"'),
      })
      .catchall(z.array(z.number().nullable()))
      .describe('One series per variable, a value for each hour in `time`; null where there is none'),
  })
  .catchall(z.unknown())
  .meta({ id: 'WeatherPoint' });

const WeatherPointQuery = z.object({
  lat: neededByHandler(z.coerce.number().min(-90).max(90)).describe('Latitude, degrees'),
  lon: neededByHandler(z.coerce.number().min(-180).max(180)).describe('Longitude, degrees'),
});

const GRID_UNBUILT =
  'The gridded field has not been built yet, as after a first start, or (with a key) the key could not be checked just now; try again later (service_unavailable, auth_unavailable)';

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

weatherRouter.get(
  '/api/weather/current',
  SCOPE_WEATHER,
  describeRoute({
    operationId: 'getWeatherCurrent',
    tags: ['Weather'],
    summary: 'Current conditions in NSW towns',
    description:
      'Current conditions at about 100 towns across NSW, and Canberra, from Open-Meteo forecast data. Refreshed every 30 minutes. A town Open-Meteo gives no current conditions for is left out, and the collection is empty until the first refresh after a restart.',
    responses: { ...okJson(WeatherCurrent, 'A GeoJSON FeatureCollection, one point per town'), ...GATED_ERRORS },
  }),
  (c) => geo(c, weatherCurrentSnapshot()),
);

weatherRouter.get(
  '/api/weather/radar',
  SCOPE_WEATHER,
  describeRoute({
    operationId: 'getWeatherRadar',
    tags: ['Weather'],
    summary: 'Rain radar frames',
    description:
      "RainViewer's list of rain radar frames, as RainViewer publishes it: the time of each frame and the path its map tiles are served under. The tiles themselves come from RainViewer. Refreshed every five minutes; until the first refresh after a restart both lists are empty.",
    responses: { ...okJson(envelope(WeatherRadar), "RainViewer's frame list"), ...GATED_ERRORS },
  }),
  (c) => ok(c, weatherRadarSnapshot()),
);

// ---------------------------------------------------------------------------
// The gridded field
// ---------------------------------------------------------------------------

/**
 * Everything the client needs to interpret the binary grids: geometry, the
 * time axis, and each variable's scale. Fetched once per page load, then the
 * client pulls individual grids against it.
 */
weatherRouter.get(
  '/api/weather/grid/manifest',
  SCOPE_WEATHER,
  describeRoute({
    operationId: 'getWeatherGridManifest',
    tags: ['Weather'],
    summary: 'What the gridded weather field holds',
    description:
      'The gridded weather field: Open-Meteo forecast data sampled on regular latitude/longitude grids — one for land weather, one shared by sea state and river discharge, one for air quality and UV — over the recent past and the days ahead. This lists each grid, its time axis and the variables on it, with how to read their stored values; /weather/grid serves the values. Refreshed about once a day.',
    responses: {
      ...okJson(envelope(WeatherGridManifest), 'The grids, time axes and variables'),
      ...GATED_ERRORS,
      503: errorJson(GRID_UNBUILT),
    },
  }),
  async (c) => {
    const m = await readManifest();
    if (!m) {
      // Distinguishable from a server fault: the field simply has not been
      // fetched yet (first boot, or the cache was cleared). The client shows the
      // layer as unavailable rather than erroring.
      return fail(c, 503, 'service_unavailable', 'The gridded weather field has not been built yet.', {
        error: 'weather grid not built yet',
        ready: false,
      });
    }
    // Short cache: the manifest only changes when a refresh lands, but a client
    // that holds a stale one asks for timesteps that no longer exist.
    c.header('Cache-Control', 'public, max-age=300');
    return ok(c, m);
  },
);

/**
 * One variable at one timestep: `cols * rows` little-endian Int16 values,
 * row-major from the south-west corner.
 *
 * Binary rather than JSON because this is the request the client makes
 * repeatedly while scrubbing the timeline. The same data as JSON numbers is
 * roughly six times the bytes and needs parsing; as Int16 it is ~11.7 KB that
 * goes straight into a typed array.
 *
 * The query is read by hand below, as it always was; the validator only
 * describes it and never refuses.
 */
weatherRouter.get(
  '/api/weather/grid',
  SCOPE_WEATHER,
  describeRoute({
    operationId: 'getWeatherGrid',
    tags: ['Weather'],
    summary: 'One variable of the gridded weather field at one time',
    description:
      "The values of one variable at one timestep, as raw binary: the grid's cols × rows points as little-endian signed 16-bit integers, row by row from the south-west corner — the first row is the southernmost, each row west to east. A stored value divided by X-Grid-Scale is the real value, in the unit the manifest gives; the manifest's nodata (-32768) means no value there. The grid's size comes from the manifest (the variable's grid) and from X-Grid-Cols and X-Grid-Rows. A stored grid never changes, so it may be cached for a day.",
    responses: {
      200: {
        description: 'The values, cols × rows little-endian Int16',
        headers: {
          'X-Grid-Cols': { description: 'Points across, west to east', schema: { type: 'integer' } },
          'X-Grid-Rows': { description: 'Points down, south to north', schema: { type: 'integer' } },
          'X-Grid-Scale': { description: 'A stored value divided by this is the real value', schema: { type: 'number' } },
        },
        content: { 'application/octet-stream': { schema: { type: 'string', format: 'binary' } } },
      },
      ...GATED_ERRORS,
      ...VALIDATION_ERROR,
      404: errorJson("The timestep is not on the variable's axis, or its grid is not stored (not_found)"),
      503: errorJson(GRID_UNBUILT),
    },
  }),
  queryValidator(WeatherGridQuery),
  async (c) => {
    const v = (c.req.query('var') ?? '').trim() as GridVar;
    const t = (c.req.query('t') ?? '').trim();

    const isLand = LAND_VARS.includes(v as (typeof LAND_VARS)[number]);
    const isMarine = MARINE_VARS.includes(v as (typeof MARINE_VARS)[number]);
    const isFlood = FLOOD_VARS.includes(v as (typeof FLOOD_VARS)[number]);
    const isAir = AIR_VARS.includes(v as (typeof AIR_VARS)[number]);
    if (!isLand && !isMarine && !isFlood && !isAir) {
      return fail(
        c,
        400,
        'validation_failed',
        'var: not a variable of the field',
        { error: 'unknown variable' },
        { issues: [{ path: 'var', message: 'not a variable of the field' }] },
      );
    }
    if (!t) {
      return fail(
        c,
        400,
        'validation_failed',
        't: required',
        { error: 'missing timestep' },
        { issues: [{ path: 't', message: 'required' }] },
      );
    }

    const m = await readManifest();
    if (!m) {
      return fail(c, 503, 'service_unavailable', 'The gridded weather field has not been built yet.', {
        error: 'weather grid not built yet',
        ready: false,
      });
    }

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
      return fail(c, 503, 'service_unavailable', `The ${which} part of the gridded weather field has not been built yet.`, {
        error: `${which} field not built yet`,
        ready: false,
      });
    }
    // Only timesteps the manifest advertises. Without this the timestep is a
    // caller-controlled string reaching a filename.
    if (!axis.includes(t)) {
      return fail(c, 404, 'not_found', "No such timestep on this variable's axis; the manifest lists them.", {
        error: 'unknown timestep',
      });
    }

    const bytes = await readGridBytes(v, t);
    if (!bytes) return fail(c, 404, 'not_found', 'That grid is not stored.', { error: 'grid not available' });

    c.header('Content-Type', 'application/octet-stream');
    // A given (variable, timestep) never changes once written — a later refresh
    // writes new timesteps rather than rewriting old ones — so this is safe to
    // cache hard, which is what makes scrubbing back and forth feel instant.
    c.header('Cache-Control', 'public, max-age=86400, immutable');
    c.header('X-Grid-Cols', String(geometry.cols));
    c.header('X-Grid-Rows', String(geometry.rows));
    c.header('X-Grid-Scale', String(VAR_SCALE[v]));
    return c.body(new Uint8Array(bytes));
  },
);

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

// The query is read by hand below, as it always was; the validator only
// describes it and never refuses.
weatherRouter.get(
  '/api/weather/point',
  SCOPE_WEATHER,
  describeRoute({
    operationId: 'getWeatherPoint',
    tags: ['Weather'],
    summary: 'Hourly forecast for one place',
    description:
      "Seven days of hourly weather for one place — temperature, apparent temperature, humidity, precipitation, sea-level pressure, CAPE and wind — from Open-Meteo forecast data, passed through as Open-Meteo answers it. The coordinates are rounded to two decimal places first, so nearby points share an answer. Read from Open-Meteo when asked; an answer is kept for 15 minutes and may be served up to an hour old while a fresh one is fetched.",
    responses: {
      ...okJson(envelope(WeatherPoint), "Open-Meteo's hourly forecast for the point"),
      ...GATED_ERRORS,
      ...VALIDATION_ERROR,
      502: errorJson('Open-Meteo could not be read just now (service_unavailable)'),
    },
  }),
  queryValidator(WeatherPointQuery),
  async (c) => {
    const lat = Number(c.req.query('lat'));
    const lon = Number(c.req.query('lon'));
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      const issues = [
        ...(Number.isFinite(lat) ? [] : [{ path: 'lat', message: 'required, as a number' }]),
        ...(Number.isFinite(lon) ? [] : [{ path: 'lon', message: 'required, as a number' }]),
      ];
      return fail(c, 400, 'validation_failed', `${issues[0]!.path}: ${issues[0]!.message}`, { error: 'lat and lon are required' }, { issues });
    }
    if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
      const issues = [
        ...(lat < -90 || lat > 90 ? [{ path: 'lat', message: 'out of range (-90 to 90)' }] : []),
        ...(lon < -180 || lon > 180 ? [{ path: 'lon', message: 'out of range (-180 to 180)' }] : []),
      ];
      return fail(c, 400, 'validation_failed', `${issues[0]!.path}: ${issues[0]!.message}`, { error: 'lat or lon out of range' }, { issues });
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
        if (config.OPEN_METEO_FORECAST_MODELS) {
          params.set('models', config.OPEN_METEO_FORECAST_MODELS);
        }
        return fetchJson<unknown>(`${config.OPEN_METEO_FORECAST_URL}?${params.toString()}`, {
          headers: { 'User-Agent': 'AusAware/1.0 (+https://nswpsn.forcequit.xyz)' },
        });
      }, POINT_SWR);
      c.header('Cache-Control', 'public, max-age=900');
      return ok(c, data as Record<string, unknown>);
    } catch {
      // Already an error status on the old path, so v1 keeps it too.
      return fail(c, 502, 'service_unavailable', 'The forecast for that point could not be read just now.', {
        error: 'point lookup failed',
      });
    }
  },
);
