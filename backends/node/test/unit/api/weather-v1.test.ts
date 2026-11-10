/**
 * The weather routes on /api/v1/ and on their old /api/ paths.
 *
 * One handler chain serves both (api/v1/mirror.ts), so the thing to prove is
 * that the old path answers byte for byte as it did — status, body and the
 * headers the routes set — while v1 wraps JSON in `data`, leaves GeoJSON and
 * the binary grid alone, and answers errors in the shared shape. The old
 * bodies are written out literally here, as the handlers built them before
 * they answered through api/v1/respond.ts.
 *
 * Every weather route is mirrored; nothing in this router is left off v1.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';

const m = vi.hoisted(() => ({
  fetchJson: vi.fn(),
  manifest: null as unknown,
  grids: new Map<string, Buffer>(),
}));

vi.mock('../../../src/sources/shared/http.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/sources/shared/http.js')>()),
  fetchJson: m.fetchJson,
}));

vi.mock('../../../src/services/weatherStore.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/services/weatherStore.js')>()),
  readManifest: async () => m.manifest,
  readGridBytes: async (v: string, t: string) => m.grids.get(`${v}|${t}`) ?? null,
}));

const IN_SCOPE = [
  '/api/weather/current',
  '/api/weather/radar',
  '/api/weather/grid/manifest',
  '/api/weather/grid',
  '/api/weather/point',
];

async function app() {
  const { weatherRouter } = await import('../../../src/api/weather.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', weatherRouter);
  mirror(a, weatherRouter, IN_SCOPE);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function get(a: Hono, path: string) {
  const res = await a.request(path);
  return { status: res.status, headers: [...res.headers], body: Buffer.from(await res.arrayBuffer()) };
}

const JSON_TYPE = 'application/json';
const v1 = (path: string) => path.replace(/^\/api\//, '/api/v1/');

interface V1Error {
  error: { code: string; message: string; details?: { issues: Array<{ path: string; message: string }> } };
}

const geometry = (stepDeg: number, cols: number, rows: number) => ({ west: 112, south: -44, stepDeg, cols, rows });
const LAND_T = ['2026-10-28T00:00:00.000Z', '2026-10-28T03:00:00.000Z'];
const MARINE_T = ['2026-10-28T00:00:00.000Z'];
const LAND_ONLY = {
  issuedAt: '2026-10-28T01:00:00.000Z',
  geometry: geometry(0.5, 3, 2),
  timesteps: LAND_T,
  vars: [{ name: 'temperature_2m', scale: 10, unit: '°C', marine: false }],
  nodata: -32768,
};
const WITH_MARINE = {
  ...LAND_ONLY,
  vars: [...LAND_ONLY.vars, { name: 'wave_height', scale: 100, unit: 'm', marine: true, flood: false, air: false }],
  marineGeometry: geometry(1, 2, 2),
  marineTimesteps: MARINE_T,
  marineIssuedAt: '2026-10-28T01:10:00.000Z',
};
const GRID = Buffer.from(new Int16Array([215, -32768, 198, 201, -32768, 187]).buffer);

beforeEach(() => {
  m.fetchJson.mockReset();
  m.manifest = null;
  m.grids.clear();
  liveStore.delete('weather_current');
  liveStore.delete('weather_radar');
});

describe('/weather/current — GeoJSON, bare on both paths', () => {
  it('answers the snapshot, byte for byte', async () => {
    const snapshot = {
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [151.2093, -33.8688] },
          properties: {
            name: 'Sydney CBD', temperature: 21.4, feelsLike: 20.1, humidity: 60, windSpeed: 12.3, windGusts: 30.1,
            windDirection: 140, precipitation: 0, weatherCode: 2, weatherDescription: 'Partly Cloudy', weatherIcon: '⛅',
          },
        },
      ],
    };
    liveStore.set('weather_current', snapshot);
    const a = await app();
    for (const p of ['/api/weather/current', '/api/v1/weather/current']) {
      const res = await get(a, p);
      expect(res.status, p).toBe(200);
      expect(res.headers, p).toEqual([['content-type', JSON_TYPE]]);
      expect(res.body.toString(), p).toBe(JSON.stringify(snapshot));
    }
  });

  it('an empty collection before the first poll', async () => {
    const a = await app();
    for (const p of ['/api/weather/current', '/api/v1/weather/current']) {
      expect((await get(a, p)).body.toString(), p).toBe('{"type":"FeatureCollection","features":[]}');
    }
  });
});

describe('/weather/radar — wrapped in data on v1', () => {
  it("RainViewer's list as read, unchanged on the old path", async () => {
    const radar = {
      version: '2.0',
      generated: 1793145600,
      host: 'https://tilecache.rainviewer.com',
      radar: { past: [{ time: 1793145000, path: '/v2/radar/abc' }], nowcast: [] },
      satellite: { infrared: [] },
    };
    liveStore.set('weather_radar', radar);
    const a = await app();
    const old = await get(a, '/api/weather/radar');
    expect(old.status).toBe(200);
    expect(old.headers).toEqual([['content-type', JSON_TYPE]]);
    expect(old.body.toString()).toBe(JSON.stringify(radar));
    const now = await get(a, '/api/v1/weather/radar');
    expect(now.status).toBe(200);
    expect(now.body.toString()).toBe(JSON.stringify({ data: radar }));
  });

  it('empty lists before the first poll', async () => {
    const a = await app();
    expect((await get(a, '/api/weather/radar')).body.toString()).toBe('{"radar":{"past":[],"nowcast":[]}}');
    expect((await get(a, '/api/v1/weather/radar')).body.toString()).toBe('{"data":{"radar":{"past":[],"nowcast":[]}}}');
  });
});

describe('/weather/grid/manifest', () => {
  it('the manifest with its short cache — bare on the old path, in data on v1', async () => {
    m.manifest = WITH_MARINE;
    const a = await app();
    const old = await get(a, '/api/weather/grid/manifest');
    expect(old.status).toBe(200);
    expect(old.headers).toEqual([
      ['cache-control', 'public, max-age=300'],
      ['content-type', JSON_TYPE],
    ]);
    expect(old.body.toString()).toBe(JSON.stringify(WITH_MARINE));
    const now = await get(a, '/api/v1/weather/grid/manifest');
    expect(now.status).toBe(200);
    expect(now.headers).toEqual(old.headers);
    expect(now.body.toString()).toBe(JSON.stringify({ data: WITH_MARINE }));
  });

  it('not built yet: 503 with the old body on the old path, service_unavailable on v1', async () => {
    const a = await app();
    const old = await get(a, '/api/weather/grid/manifest');
    expect(old.status).toBe(503);
    expect(old.headers).toEqual([['content-type', JSON_TYPE]]);
    expect(old.body.toString()).toBe('{"error":"weather grid not built yet","ready":false}');
    const now = await get(a, '/api/v1/weather/grid/manifest');
    expect(now.status).toBe(503);
    expect((JSON.parse(now.body.toString()) as V1Error).error.code).toBe('service_unavailable');
  });
});

describe('/weather/grid — binary, the same bytes on both paths', () => {
  it('a stored grid with its headers, never wrapped', async () => {
    m.manifest = WITH_MARINE;
    m.grids.set(`temperature_2m|${LAND_T[0]}`, GRID);
    m.grids.set(`wave_height|${MARINE_T[0]}`, GRID);
    const a = await app();
    const cases: Array<[string, string, string, string]> = [
      // query, cols, rows, scale
      [`var=temperature_2m&t=${LAND_T[0]}`, '3', '2', '10'],
      [`var=%20temperature_2m%20&t=%20${LAND_T[0]}%20`, '3', '2', '10'], // trimmed, as always
      [`var=wave_height&t=${MARINE_T[0]}`, '2', '2', '100'], // the marine grid's own geometry
    ];
    for (const [qs, cols, rows, scale] of cases) {
      for (const p of ['/api/weather/grid', '/api/v1/weather/grid']) {
        const res = await get(a, `${p}?${qs}`);
        expect(res.status, `${p}?${qs}`).toBe(200);
        expect(res.headers, `${p}?${qs}`).toEqual([
          ['cache-control', 'public, max-age=86400, immutable'],
          ['content-type', 'application/octet-stream'],
          ['x-grid-cols', cols],
          ['x-grid-rows', rows],
          ['x-grid-scale', scale],
        ]);
        expect(res.body.equals(GRID), `${p}?${qs}`).toBe(true);
      }
    }
  });

  it('refusals and misses: the old bodies on the old path, the shared shape on v1', async () => {
    m.manifest = LAND_ONLY;
    const a = await app();
    const cases: Array<[string, number, string, string, string?]> = [
      // query, status, old body, v1 code, v1 issue path
      ['', 400, '{"error":"unknown variable"}', 'validation_failed', 'var'],
      ['var=nope&t=x', 400, '{"error":"unknown variable"}', 'validation_failed', 'var'],
      ['var=nope', 400, '{"error":"unknown variable"}', 'validation_failed', 'var'], // both bad: the variable, as always
      ['var=temperature_2m', 400, '{"error":"missing timestep"}', 'validation_failed', 't'],
      ['var=temperature_2m&t=%20', 400, '{"error":"missing timestep"}', 'validation_failed', 't'],
      ['var=temperature_2m&t=2026-10-28T01:00:00.000Z', 404, '{"error":"unknown timestep"}', 'not_found'],
      [`var=temperature_2m&t=${LAND_T[1]}`, 404, '{"error":"grid not available"}', 'not_found'],
      [`var=wave_height&t=${MARINE_T[0]}`, 503, '{"error":"marine field not built yet","ready":false}', 'service_unavailable'],
      [`var=river_discharge&t=${LAND_T[0]}`, 503, '{"error":"flood field not built yet","ready":false}', 'service_unavailable'],
      [`var=pm2_5&t=${LAND_T[0]}`, 503, '{"error":"air field not built yet","ready":false}', 'service_unavailable'],
    ];
    for (const [qs, status, oldBody, code, issue] of cases) {
      const old = await get(a, `/api/weather/grid?${qs}`);
      expect(old.status, qs).toBe(status);
      expect(old.headers, qs).toEqual([['content-type', JSON_TYPE]]);
      expect(old.body.toString(), qs).toBe(oldBody);
      const now = await get(a, `/api/v1/weather/grid?${qs}`);
      expect(now.status, qs).toBe(status);
      const body = JSON.parse(now.body.toString()) as V1Error;
      expect(body.error.code, qs).toBe(code);
      if (issue) expect(body.error.details?.issues[0]?.path, qs).toBe(issue);
    }
  });

  it('no field yet: 503 — but a bad variable is still refused first', async () => {
    const a = await app();
    const old = await get(a, `/api/weather/grid?var=temperature_2m&t=${LAND_T[0]}`);
    expect(old.status).toBe(503);
    expect(old.body.toString()).toBe('{"error":"weather grid not built yet","ready":false}');
    const now = await get(a, `/api/v1/weather/grid?var=temperature_2m&t=${LAND_T[0]}`);
    expect(now.status).toBe(503);
    expect((JSON.parse(now.body.toString()) as V1Error).error.code).toBe('service_unavailable');
    expect((await get(a, '/api/weather/grid?var=nope&t=x')).status).toBe(400);
  });
});

describe('/weather/point — read from Open-Meteo when asked', () => {
  const FORECAST = {
    latitude: -33.875,
    longitude: 151.25,
    hourly_units: { time: 'iso8601', temperature_2m: '°C' },
    hourly: { time: ['2026-10-28T00:00', '2026-10-28T01:00'], temperature_2m: [21.5, null] },
  };

  it('the forecast with its cache header — bare on the old path, in data on v1', async () => {
    m.fetchJson.mockResolvedValue(FORECAST);
    const a = await app();
    const old = await get(a, '/api/weather/point?lat=-33.8688&lon=151.2093');
    expect(old.status).toBe(200);
    expect(old.headers).toEqual([
      ['cache-control', 'public, max-age=900'],
      ['content-type', JSON_TYPE],
    ]);
    expect(old.body.toString()).toBe(JSON.stringify(FORECAST));
    const now = await get(a, '/api/v1/weather/point?lat=-33.87&lon=151.21');
    expect(now.status).toBe(200);
    expect(now.headers).toEqual(old.headers);
    expect(now.body.toString()).toBe(JSON.stringify({ data: FORECAST }));
    // Rounded to two decimals, so both share one upstream read.
    expect(m.fetchJson).toHaveBeenCalledTimes(1);
    const url = new URL(m.fetchJson.mock.calls[0]![0] as string);
    expect(url.searchParams.get('latitude')).toBe('-33.87');
    expect(url.searchParams.get('longitude')).toBe('151.21');
    expect(url.searchParams.get('timezone')).toBe('UTC');
    expect(url.searchParams.get('forecast_days')).toBe('7');
  });

  it('bad coordinates: the old bodies on the old path, validation_failed on v1', async () => {
    const a = await app();
    const cases: Array<[string, string, string]> = [
      // query, old body, first v1 issue path
      ['', '{"error":"lat and lon are required"}', 'lat'],
      ['lat=1', '{"error":"lat and lon are required"}', 'lon'],
      ['lat=abc&lon=1', '{"error":"lat and lon are required"}', 'lat'],
      ['lat=Infinity&lon=1', '{"error":"lat and lon are required"}', 'lat'],
      ['lat=95&lon=abc', '{"error":"lat and lon are required"}', 'lon'], // not a number beats out of range, as always
      ['lat=91&lon=1', '{"error":"lat or lon out of range"}', 'lat'],
      ['lat=1&lon=-181', '{"error":"lat or lon out of range"}', 'lon'],
      ['lat=91&lat=1&lon=1', '{"error":"lat or lon out of range"}', 'lat'], // a repeat: the first value
    ];
    for (const [qs, oldBody, issue] of cases) {
      const old = await get(a, `/api/weather/point?${qs}`);
      expect(old.status, qs).toBe(400);
      expect(old.headers, qs).toEqual([['content-type', JSON_TYPE]]);
      expect(old.body.toString(), qs).toBe(oldBody);
      const now = await get(a, `/api/v1/weather/point?${qs}`);
      expect(now.status, qs).toBe(400);
      const body = JSON.parse(now.body.toString()) as V1Error;
      expect(body.error.code, qs).toBe('validation_failed');
      expect(body.error.details?.issues[0]?.path, qs).toBe(issue);
    }
    expect(m.fetchJson).not.toHaveBeenCalled();
  });

  it('Open-Meteo failing: 502 with the old body on the old path, the shared shape on v1', async () => {
    m.fetchJson.mockRejectedValue(new Error('HTTP 503'));
    const a = await app();
    const old = await get(a, '/api/weather/point?lat=-27.47&lon=153.03');
    expect(old.status).toBe(502);
    expect(old.headers).toEqual([['content-type', JSON_TYPE]]);
    expect(old.body.toString()).toBe('{"error":"point lookup failed"}');
    const now = await get(a, '/api/v1/weather/point?lat=-27.47&lon=153.03');
    expect(now.status).toBe(502);
    expect(JSON.parse(now.body.toString())).toEqual({
      error: { code: 'service_unavailable', message: expect.any(String) },
    });
  });
});

describe('the mirror', () => {
  it('serves every weather route on /api/v1/; an unknown v1 path is a v1 404', async () => {
    const a = await app();
    const miss = await get(a, '/api/v1/weather/nope');
    expect(miss.status).toBe(404);
    expect(JSON.parse(miss.body.toString())).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
    // The old path's own 404 is untouched.
    const oldMiss = await get(a, '/api/weather/nope');
    expect(oldMiss.status).toBe(404);
    expect(oldMiss.body.toString()).toBe('404 Not Found');
  });

  it('mirroring the in-scope paths registers exactly those', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    await app();
    const weather = mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/weather/'));
    expect(weather.map((r) => `${r.method} ${r.legacyPath}`).sort()).toEqual(IN_SCOPE.map((p) => `GET ${p}`).sort());
    expect(weather.map((r) => r.path).sort()).toEqual(IN_SCOPE.map(v1).sort());
  });
});
