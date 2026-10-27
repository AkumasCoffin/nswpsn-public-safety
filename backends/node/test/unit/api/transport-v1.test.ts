/**
 * The transport routes on /api/v1/ and on their old /api/ paths.
 *
 * mirror() serves one handler chain on both, so each route is asked the same
 * question on each: v1 wraps a JSON answer in `data` (bare GeoJSON for
 * /lines) and answers errors in the shared shape, while the old path answers
 * exactly what it always did — the same status and the same body, errors and
 * upstream failures included. The query schemas added for the description
 * must never refuse anything the handlers did not refuse themselves.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';

const { fetchJsonMock, configMock, tfnsw } = vi.hoisted(() => ({
  fetchJsonMock: vi.fn(),
  configMock: {
    LOG_LEVEL: 'warn',
    NODE_ENV: 'test',
    STATE_DIR: './test/.tmp-state',
    NSWPSN_API_KEY: 'test-key',
    PORT: 3001,
    TRANSPORT_DISABLED: false,
  } as Record<string, unknown>,
  tfnsw: { configured: false, alerts: vi.fn() },
}));

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  fetchBuffer: vi.fn(),
  HttpError: class extends Error {},
}));

vi.mock('../../../src/config.js', () => ({
  config: configMock,
  modeLabel: () => 'dev',
}));

// TfNSW is its own source with its own tests; here it only has to be
// configured or not, and to answer or fail.
vi.mock('../../../src/sources/tfnsw.js', () => ({
  fetchTfnswPositions: vi.fn(async () => []),
  applyTfnswPositions: vi.fn(),
  fetchTfnswAlerts: tfnsw.alerts,
  tfnswConfigured: () => tfnsw.configured,
  tfnswPositionsEnabled: () => false,
}));

const PATHS = [
  '/api/transport/vehicles',
  '/api/transport/shape/:id',
  '/api/transport/trip/:date/:tripId/:instance',
  '/api/transport/departures/:stopId',
  '/api/transport/lines',
  '/api/transport/stops',
  '/api/transport/alerts',
];

const NOW = new Date('2026-10-28T01:00:00.000Z');
const NOW_ISO = NOW.toISOString();
const NOW_SEC = Math.floor(NOW.getTime() / 1000);
const CBD = 'minLat=-33.92&maxLat=-33.83&minLon=151.15&maxLon=151.25';
const UPSTREAM_DOWN = { error: 'transport upstream unavailable' };

let built: Hono | null = null;
async function app(): Promise<Hono> {
  if (built) return built;
  const { transportRouter } = await import('../../../src/api/transport.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', transportRouter);
  mirror(a, transportRouter, PATHS);
  a.notFound(notFound);
  a.onError(onError);
  built = a;
  return a;
}

interface Answer {
  status: number;
  type: string | null;
  cacheControl: string | null;
  body: unknown;
}
async function get(path: string): Promise<Answer> {
  const res = await (await app()).request(path);
  return {
    status: res.status,
    type: res.headers.get('Content-Type'),
    cacheControl: res.headers.get('Cache-Control'),
    body: await res.json(),
  };
}

/** Ask the old path and its v1 twin the same thing. */
async function both(path: string): Promise<{ old: Answer; v1: Answer }> {
  const old = await get(`/api${path}`);
  const v1 = await get(`/api/v1${path}`);
  return { old, v1 };
}

/** A 200: the old body as it always was, the same body under `data` on v1. */
function expectWrapped(r: { old: Answer; v1: Answer }, oldBody: unknown): void {
  expect(r.old.status).toBe(200);
  expect(r.old.body).toEqual(oldBody);
  expect(r.v1.status).toBe(200);
  expect(r.v1.body).toEqual({ data: oldBody });
  for (const a of [r.old, r.v1]) {
    expect(a.type).toMatch(/^application\/json/);
    // Transport sets no caching of its own (server.ts keeps it off the CDN list).
    expect(a.cacheControl).toBeNull();
  }
}

/** An error: the old body byte-for-byte on /api/, the shared shape on v1. */
function expectError(
  r: { old: Answer; v1: Answer },
  status: number,
  legacy: Record<string, unknown>,
  code: string,
): void {
  expect(r.old.status).toBe(status);
  expect(r.old.body).toEqual(legacy);
  expect(r.v1.status).toBe(status);
  const err = (r.v1.body as { error: { code: string; message: string } }).error;
  expect(err.code).toBe(code);
  expect(typeof err.message).toBe('string');
  expect(Object.keys(r.v1.body as object)).toEqual(['error']);
}

function rawVehicle() {
  return {
    tripInstance: {
      shapeId: 'au2:bs:222299',
      startDate: '20261028',
      instanceNumber: 0,
      trip: {
        id: 'au2:bs:123',
        headsign: { headline: 'Sydenham', subtitle: 'via Marrickville' },
        route: {
          id: 'au2:bs:7083_SW1',
          name: 'SW1',
          longName: 'Bankstown to Sydenham',
          color: 'ed2891',
          textColor: 'FFFFFF',
          mode: 'au2:buses',
          agency: { name: 'Transit Systems NSW' },
        },
      },
    },
    vehicleInstance: {
      id: '908964',
      lastPosition: {
        time: NOW_SEC - 12,
        bearing: 270,
        speed: 15,
        vdap: 1234.5,
        occupancy: [1],
        coordinates: { lat: -33.9, lon: 151.2 },
      },
      wheelchair: 1,
      aircon: true,
      vehicleModel: 'MO8964',
    },
  };
}

const VEHICLE = {
  id: '908964',
  lat: -33.9,
  lon: 151.2,
  bearing: 270,
  speedKmh: 54,
  mode: 'buses',
  route: {
    id: 'au2:bs:7083_SW1',
    name: 'SW1',
    longName: 'Bankstown to Sydenham',
    color: '#ED2891',
    textColor: '#FFFFFF',
  },
  headsign: 'Sydenham',
  headsignSub: 'via Marrickville',
  agency: 'Transit Systems NSW',
  occupancy: 1,
  wheelchair: true,
  aircon: true,
  model: 'MO8964',
  ageSec: 12,
  posTime: NOW_SEC - 12,
  tripId: 'au2:bs:123',
  shapeId: 'au2:bs:222299',
  vdap: 1234.5,
  startDate: '20261028',
  instanceNumber: 0,
};

beforeEach(async () => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
  fetchJsonMock.mockReset();
  tfnsw.alerts.mockReset();
  tfnsw.configured = false;
  configMock['TRANSPORT_DISABLED'] = false;
  const { _resetTransportCacheForTests } = await import('../../../src/api/transport.js');
  _resetTransportCacheForTests();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('/transport/vehicles', () => {
  it('answers the snapshot — wrapped in data on v1', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [rawVehicle()] } });
    const r = await both(`/transport/vehicles?${CBD}&feeds=bs`);
    expectWrapped(r, { vehicles: [VEHICLE], count: 1, fetched_at: NOW_ISO });
    expect(fetchJsonMock).toHaveBeenCalledTimes(1); // the v1 ask was a cache hit
  });

  it('refuses what it always refused, with the old bodies on /api/', async () => {
    expectError(await both('/transport/vehicles?region=zz'), 400, { error: 'unknown region: zz' }, 'validation_failed');
    expectError(await both('/transport/vehicles?minLat=-33.9'), 400, { error: 'invalid bbox' }, 'validation_failed');
    expectError(
      await both('/transport/vehicles?minLat=-33.8&maxLat=-33.9&minLon=151.1&maxLon=151.2'),
      400,
      { error: 'bbox outside coverage' },
      'validation_failed',
    );
    expectError(
      await both('/transport/vehicles?minLat=-37&maxLat=-29&minLon=145&maxLon=150'),
      400,
      { error: 'bbox too large' },
      'validation_failed',
    );
    expectError(await both(`/transport/vehicles?${CBD}&feeds=zz`), 400, { error: 'unknown feed: zz' }, 'validation_failed');
    expectError(
      await both('/transport/vehicles?minLat=-27.6&maxLat=-27.3&minLon=152.9&maxLon=153.2&region=au4&modes=metro'),
      400,
      { error: 'unknown mode: metro' },
      'validation_failed',
    );
    expect(fetchJsonMock).not.toHaveBeenCalled();
  });

  it('the query schema refuses nothing itself: the handler decides, in its own order', async () => {
    // Both the region and the box are wrong — the handler names the region first.
    expectError(await both('/transport/vehicles?region=zz&minLat=abc'), 400, { error: 'unknown region: zz' }, 'validation_failed');
    // An upper-case, padded region is the handler's to read, and it reads it.
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    const ok = await both(`/transport/vehicles?${CBD}&region=%20AU2%20&feeds=bs`);
    expect(ok.old.status).toBe(200);
    expect(ok.v1.status).toBe(200);
    // A repeated parameter is read by its first value, on both paths.
    const repeated = await both(`/transport/vehicles?${CBD}&feeds=st&feeds=zz`);
    expect(repeated.old.status).toBe(200);
    expect(repeated.v1.status).toBe(200);
  });

  it('a cold upstream failure: 502 on both, in each path’s shape', async () => {
    fetchJsonMock.mockRejectedValue(new Error('fetch failed: ETIMEDOUT'));
    expectError(await both(`/transport/vehicles?${CBD}&feeds=bs`), 502, UPSTREAM_DOWN, 'service_unavailable');
  });

  it('switched off: the empty snapshot, flagged', async () => {
    configMock['TRANSPORT_DISABLED'] = true;
    const r = await both(`/transport/vehicles?${CBD}`);
    expectWrapped(r, { vehicles: [], count: 0, fetched_at: NOW_ISO, disabled: true });
    expect(fetchJsonMock).not.toHaveBeenCalled();
  });
});

describe('/transport/shape/:id', () => {
  it('answers the encoded track — wrapped in data on v1', async () => {
    fetchJsonMock.mockResolvedValue({ response: { shape: { id: 'au2:bs:222299', enc: 'rr_nEmdzx[PC' } } });
    expectWrapped(await both('/transport/shape/au2:bs:222299'), { id: 'au2:bs:222299', enc: 'rr_nEmdzx[PC' });
  });

  it('refuses a malformed id or an unknown region with the old bodies', async () => {
    expectError(await both('/transport/shape/DROP%20TABLE'), 400, { error: 'invalid shape id' }, 'validation_failed');
    expectError(await both('/transport/shape/au42:xx:1'), 400, { error: 'unknown region' }, 'validation_failed');
  });

  it('502 when AnyTrip fails cold', async () => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 500'));
    expectError(await both('/transport/shape/au2:bs:1'), 502, UPSTREAM_DOWN, 'service_unavailable');
  });

  it('switched off', async () => {
    configMock['TRANSPORT_DISABLED'] = true;
    expectWrapped(await both('/transport/shape/au2:bs:1'), { id: '', enc: null, disabled: true });
  });
});

describe('/transport/trip/:date/:tripId/:instance', () => {
  const TRIP = '/transport/trip/20261028/au2:bs:123/0';

  it('answers the trip — wrapped in data on v1', async () => {
    fetchJsonMock.mockResolvedValue({
      response: {
        tripInstance: {
          shapeId: 'au2:bs:222299',
          trip: { headsign: { headline: 'Sydenham' }, route: { name: 'SW1', color: 'ED2891', mode: 'au2:buses' } },
        },
        realtimePattern: [
          {
            stop: { fullName: 'Bankstown Station', coordinates: { lat: -33.917, lon: 151.034 } },
            stopSequence: 1,
            departure: { time: NOW_SEC + 60, delay: 30, occupancy: [2, 9] },
          },
        ],
        alerts: [{ header: 'Detour' }, {}],
        vehicle: { lastPosition: { time: NOW_SEC - 5, statusString: 'at Bankstown Station' } },
        rel: { next: { tripInstance: { startDate: '20261028', instanceNumber: 0, trip: { id: 'au2:bs:124' } } } },
      },
    });
    expectWrapped(await both(TRIP), {
      tripId: 'au2:bs:123',
      startDate: '20261028',
      instanceNumber: 0,
      headsign: 'Sydenham',
      route: { name: 'SW1', longName: null, color: '#ED2891', textColor: null, mode: 'buses' },
      shapeId: 'au2:bs:222299',
      stops: [
        {
          name: 'Bankstown Station',
          lat: -33.917,
          lon: 151.034,
          seq: 1,
          arr: null,
          arrDelay: null,
          dep: NOW_SEC + 60,
          depDelay: 30,
          platform: null,
          locality: null,
          code: null,
          occupancy: [2, null],
          pickUp: null,
          dropOff: null,
        },
      ],
      alerts: ['Detour'],
      vehicle: { time: NOW_SEC - 5, statusString: 'at Bankstown Station', lat: null, lon: null },
      prev: null,
      next: {
        tripId: 'au2:bs:124',
        startDate: '20261028',
        instanceNumber: 0,
        routeName: null,
        routeColor: null,
        routeTextColor: null,
        headsign: null,
      },
      fetched_at: NOW_ISO,
    });
  });

  it('refuses a bad date, trip id, instance or region with the old bodies', async () => {
    expectError(await both('/transport/trip/2026102/au2:bs:123/0'), 400, { error: 'invalid date' }, 'validation_failed');
    expectError(await both('/transport/trip/20261028/nope/0'), 400, { error: 'invalid trip id' }, 'validation_failed');
    expectError(await both('/transport/trip/20261028/au2:bs:123/1000'), 400, { error: 'invalid instance' }, 'validation_failed');
    expectError(await both('/transport/trip/20261028/au42:bs:1/0'), 400, { error: 'unknown region' }, 'validation_failed');
  });

  it('switched off: 404 on both, the old body on /api/', async () => {
    configMock['TRANSPORT_DISABLED'] = true;
    expectError(await both(TRIP), 404, { error: 'disabled' }, 'not_found');
  });

  it('502 when AnyTrip fails cold', async () => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 404'));
    expectError(await both(TRIP), 502, UPSTREAM_DOWN, 'service_unavailable');
  });
});

describe('/transport/departures/:stopId', () => {
  const departures = {
    response: {
      stop: { fullName: 'Central Station' },
      alerts: [{ header: 'Lifts out of service' }],
      departures: [
        {
          tripInstance: {
            startDate: '20261028',
            instanceNumber: 0,
            trip: { rtTripId: '123.T1', route: { name: 'T1', color: 'F99D1C', mode: 'au2:sydneytrains' } },
          },
          stopTimeInstance: {
            stopHeadsign: { headline: 'Emu Plains' },
            departure: { time: NOW_SEC + 120, delay: -30 },
            stop: { disassembled: { platformCombinedName: 'Platform 16' } },
          },
        },
      ],
    },
  };

  it('answers the departures — wrapped in data on v1', async () => {
    fetchJsonMock.mockResolvedValue(departures);
    expectWrapped(await both('/transport/departures/au2:200060?limit=5'), {
      stopId: 'au2:200060',
      stopName: 'Central Station',
      departures: [
        {
          route: { name: 'T1', color: '#F99D1C', textColor: null, mode: 'sydneytrains' },
          headsign: 'Emu Plains',
          headsignSub: null,
          dep: NOW_SEC + 120,
          delay: -30,
          platform: 'Platform 16',
          tripId: '123.T1',
          startDate: '20261028',
          instanceNumber: 0,
        },
      ],
      alerts: ['Lifts out of service'],
      fetched_at: NOW_ISO,
    });
    expect(fetchJsonMock.mock.calls[0]?.[0]).toContain('/departures/au2:200060?limit=5');
  });

  it('limit is as lenient as it always was: junk is 10, too many is 20 — never a 400', async () => {
    fetchJsonMock.mockResolvedValue(departures);
    for (const [limit, sent] of [['abc', 10], ['0', 10], ['-5', 1], ['50', 20], ['2.5', 2.5]] as const) {
      const r = await both(`/transport/departures/au2:200060?limit=${limit}`);
      expect(r.old.status, limit).toBe(200);
      expect(r.v1.status, limit).toBe(200);
      expect(fetchJsonMock.mock.calls.at(-1)?.[0], limit).toContain(`?limit=${sent}`);
    }
  });

  it('refuses a bad stop id or region with the old bodies', async () => {
    expectError(await both('/transport/departures/200060'), 400, { error: 'invalid stop id' }, 'validation_failed');
    expectError(await both('/transport/departures/au42:1'), 400, { error: 'unknown region' }, 'validation_failed');
  });

  it('switched off: 404 on both', async () => {
    configMock['TRANSPORT_DISABLED'] = true;
    expectError(await both('/transport/departures/au2:200060'), 404, { error: 'disabled' }, 'not_found');
  });

  it('502 when AnyTrip fails cold', async () => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 503'));
    expectError(await both('/transport/departures/au2:200060'), 502, UPSTREAM_DOWN, 'service_unavailable');
  });
});

describe('/transport/lines', () => {
  const line = (n: string) => ({
    type: 'Feature',
    geometry: { type: 'LineString', coordinates: [[151.2, -33.8], [151.3, -33.9]] },
    properties: { stroke: '#F99D1C', line: n },
  });

  it('is GeoJSON: the same bare FeatureCollection on both paths', async () => {
    fetchJsonMock
      .mockResolvedValueOnce({ type: 'FeatureCollection', features: [line('T1')] })
      .mockRejectedValueOnce(new Error('HTTP 500')) // one file failing alone is tolerated
      .mockResolvedValueOnce({ type: 'FeatureCollection', features: [line('QR')] });
    const r = await both('/transport/lines');
    const fc = { type: 'FeatureCollection', features: [line('T1'), line('QR')] };
    expect(r.old.status).toBe(200);
    expect(r.old.body).toEqual(fc);
    expect(r.v1.status).toBe(200);
    expect(r.v1.body).toEqual(fc);
  });

  it('502 when every file fails', async () => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 500'));
    expectError(await both('/transport/lines'), 502, UPSTREAM_DOWN, 'service_unavailable');
  });

  it('switched off: an empty collection, bare on both', async () => {
    configMock['TRANSPORT_DISABLED'] = true;
    const r = await both('/transport/lines');
    const fc = { type: 'FeatureCollection', features: [], disabled: true };
    expect(r.old.body).toEqual(fc);
    expect(r.v1.body).toEqual(fc);
  });
});

describe('/transport/stops', () => {
  it('answers the stops — wrapped in data on v1', async () => {
    fetchJsonMock.mockResolvedValue({
      response: {
        stops: [
          {
            stop: {
              id: 'au2:200060',
              fullName: 'Central Station',
              coordinates: { lat: -33.88388, lon: 151.20583 },
              modes: ['au2:metro', 'au2:sydneytrains'],
              locality: 'Sydney',
              wheelchair: true,
              facilities: { accessibility: ['Lift'] },
            },
          },
        ],
      },
    });
    expectWrapped(await both(`/transport/stops?${CBD}&modes=metro,sydneytrains`), {
      stops: [
        {
          id: 'au2:200060',
          name: 'Central Station',
          lat: -33.88388,
          lon: 151.20583,
          modes: ['metro', 'sydneytrains'],
          locality: 'Sydney',
          wheelchair: true,
          accessibility: ['Lift'],
        },
      ],
      count: 1,
      fetched_at: NOW_ISO,
    });
  });

  it('refuses what it always refused, with the old bodies on /api/', async () => {
    expectError(await both(`/transport/stops?${CBD}&modes=buses`), 400, { error: 'unknown mode: buses' }, 'validation_failed');
    expectError(await both('/transport/stops?region=au7'), 400, { error: 'unknown region: au7' }, 'validation_failed');
    expectError(await both('/transport/stops'), 400, { error: 'invalid bbox' }, 'validation_failed');
  });

  it('502 when AnyTrip fails cold', async () => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 500'));
    expectError(await both(`/transport/stops?${CBD}`), 502, UPSTREAM_DOWN, 'service_unavailable');
  });

  it('switched off', async () => {
    configMock['TRANSPORT_DISABLED'] = true;
    expectWrapped(await both(`/transport/stops?${CBD}`), { stops: [], count: 0, fetched_at: NOW_ISO, disabled: true });
  });
});

describe('/transport/alerts', () => {
  const ALERT = {
    id: 'alert-1',
    header: 'Trackwork on T1',
    description: 'Buses replace trains.',
    url: 'https://transportnsw.info/alerts',
    cause: 'MAINTENANCE',
    effect: 'DETOUR',
    severity: 'WARNING',
    start: 1750000000,
    end: 1750100000,
    routes: ['CTY_T1'],
    stops: ['2000336'],
    agencies: ['SydneyTrains'],
  };

  it('without TfNSW set up: empty and configured:false', async () => {
    expectWrapped(await both('/transport/alerts'), { alerts: [], count: 0, configured: false });
    expect(tfnsw.alerts).not.toHaveBeenCalled();
  });

  it('answers the alerts — wrapped in data on v1', async () => {
    tfnsw.configured = true;
    tfnsw.alerts.mockResolvedValue([ALERT]);
    expectWrapped(await both('/transport/alerts'), {
      alerts: [ALERT],
      count: 1,
      configured: true,
      fetched_at: NOW_ISO,
    });
  });

  it('502 when TfNSW cannot be read, with its own old body', async () => {
    tfnsw.configured = true;
    tfnsw.alerts.mockRejectedValue(new Error('down'));
    expectError(await both('/transport/alerts'), 502, { error: 'alerts upstream unavailable' }, 'service_unavailable');
  });

  it('switched off', async () => {
    configMock['TRANSPORT_DISABLED'] = true;
    expectWrapped(await both('/transport/alerts'), { alerts: [], count: 0, configured: false, disabled: true });
  });
});
