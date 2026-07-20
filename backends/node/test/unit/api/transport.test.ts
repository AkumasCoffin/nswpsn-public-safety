/**
 * Unit tests for the AnyTrip transport proxy: bbox validation/clamping/
 * snapping, feed/mode whitelists, normalization edge cases, SWR cache
 * reuse, kill switch and cold-failure behaviour.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchJsonMock = vi.fn();
const configMock: Record<string, unknown> = {
  LOG_LEVEL: 'warn',
  NODE_ENV: 'test',
  STATE_DIR: './test/.tmp-state',
  NSWPSN_API_KEY: 'test-key',
  PORT: 3001,
  TRANSPORT_DISABLED: false,
};

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

vi.mock('../../../src/config.js', () => ({
  config: configMock,
  modeLabel: () => 'dev',
}));

const CBD = 'minLat=-33.92&maxLat=-33.83&minLon=151.15&maxLon=151.25';

function rawVehicle(overrides: Record<string, unknown> = {}) {
  return {
    tripInstance: {
      shapeId: 'au2:bs:222299',
      trip: {
        id: 'au2:bs:123',
        headsign: { headline: 'Sydenham', subtitle: 'via Marrickville' },
        route: {
          id: 'au2:bs:7083_SW1',
          name: 'SW1',
          longName: 'Bankstown to Sydenham',
          color: 'ED2891',
          textColor: 'FFFFFF',
          mode: 'au2:buses',
          agency: { name: 'Transit Systems NSW' },
        },
      },
    },
    vehicleInstance: {
      id: '908964',
      lastPosition: {
        time: Math.floor(Date.now() / 1000) - 12,
        bearing: 270,
        speed: 15,
        occupancy: [1],
        vehicleOccupancy: 1,
        coordinates: { lat: -33.918, lon: 151.036 },
      },
      wheelchair: 1,
      aircon: true,
      vehicleModel: 'MO8964',
    },
    ...overrides,
  };
}

async function getVehicles(query: string) {
  const { transportRouter } = await import('../../../src/api/transport.js');
  return transportRouter.request(`/api/transport/vehicles?${query}`);
}
async function getStops(query: string) {
  const { transportRouter } = await import('../../../src/api/transport.js');
  return transportRouter.request(`/api/transport/stops?${query}`);
}

beforeEach(async () => {
  fetchJsonMock.mockReset();
  configMock['TRANSPORT_DISABLED'] = false;
  const { _resetTransportCacheForTests } = await import('../../../src/api/transport.js');
  _resetTransportCacheForTests();
});

describe('transport bbox validation', () => {
  it('400s on missing or non-numeric bbox', async () => {
    expect((await getVehicles('minLat=-33.9&maxLat=-33.8&minLon=151.1')).status).toBe(400);
    expect((await getVehicles('minLat=abc&maxLat=-33.8&minLon=151.1&maxLon=151.2')).status).toBe(400);
  });

  it('400s on inverted bbox and bbox fully outside NSW', async () => {
    expect((await getVehicles('minLat=-33.8&maxLat=-33.9&minLon=151.1&maxLon=151.2')).status).toBe(400);
    // Melbourne-ish: clamps to empty span.
    expect((await getVehicles('minLat=-38.5&maxLat=-38.1&minLon=144.5&maxLon=145.2')).status).toBe(400);
  });

  it('400s on spans over the cap', async () => {
    expect((await getVehicles('minLat=-37&maxLat=-29&minLon=145&maxLon=150')).status).toBe(400);
  });

  it('clamps coords into NSW instead of rejecting padded coastal views', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    const res = await getVehicles('minLat=-33.9&maxLat=-33.8&minLon=153.6&maxLon=154.6&feeds=bs');
    expect(res.status).toBe(200);
    expect(fetchJsonMock.mock.calls[0]?.[0]).toContain('maxLon=154');
  });

  it('snaps the bbox outward to the 0.01° grid', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    await getVehicles('minLat=-33.8674&maxLat=-33.8321&minLon=151.1539&maxLon=151.2101&feeds=bs');
    const url = fetchJsonMock.mock.calls[0]?.[0] as string;
    expect(url).toContain('minLat=-33.87');
    expect(url).toContain('maxLat=-33.83');
    expect(url).toContain('minLon=151.15');
    expect(url).toContain('maxLon=151.22');
  });
});

describe('transport feeds/modes params', () => {
  it('maps short feed codes to au2 ids and keeps the app params', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    await getVehicles(`${CBD}&feeds=st,bs`);
    const url = fetchJsonMock.mock.calls[0]?.[0] as string;
    expect(url).toContain(encodeURIComponent('au2:bs,au2:st'));
    expect(url).toContain('otrFilter=300');
    expect(url).toContain('speedFilter=15');
  });

  it('rejects unknown feeds and stop modes (including buses stops)', async () => {
    expect((await getVehicles(`${CBD}&feeds=zz`)).status).toBe(400);
    expect((await getStops(`${CBD}&modes=buses`)).status).toBe(400);
  });

  it('defaults to all feeds when the param is absent', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    await getVehicles(CBD);
    const url = fetchJsonMock.mock.calls[0]?.[0] as string;
    for (const f of ['au2:bs', 'au2:st', 'au2:mt', 'au2:nt', 'au2:fr', 'au2:lr', 'au2:sp']) {
      expect(decodeURIComponent(url)).toContain(f);
    }
  });
});

describe('transport vehicle normalization', () => {
  it('normalizes a full record', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [rawVehicle()] } });
    const res = await getVehicles(`${CBD}&feeds=bs`);
    const body = await res.json();
    expect(body.count).toBe(1);
    expect(body.vehicles[0]).toMatchObject({
      id: '908964',
      lat: -33.918,
      lon: 151.036,
      bearing: 270,
      speedKmh: 54,
      mode: 'buses',
      route: {
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
      tripId: 'au2:bs:123',
      shapeId: 'au2:bs:222299',
    });
    expect(body.vehicles[0].ageSec).toBeGreaterThanOrEqual(11);
    expect(body.vehicles[0].ageSec).toBeLessThan(20);
  });

  it('handles missing/garbage fields as nulls and drops coord-less vehicles', async () => {
    const noCoords = rawVehicle();
    (noCoords.vehicleInstance as { lastPosition: { coordinates: unknown } }).lastPosition.coordinates = {};
    const sparse = {
      tripInstance: { trip: { route: { mode: 'au2:zztrains', color: 'red' } } },
      vehicleInstance: {
        id: 'v2',
        lastPosition: { coordinates: { lat: -33.9, lon: 151.2 } },
        wheelchair: 2,
      },
    };
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [noCoords, sparse] } });
    const body = await (await getVehicles(`${CBD}&feeds=bs`)).json();
    expect(body.count).toBe(1);
    expect(body.vehicles[0]).toMatchObject({
      id: 'v2',
      mode: 'other',
      bearing: null,
      speedKmh: null,
      occupancy: null,
      wheelchair: false, // GTFS 2 = not accessible
      aircon: null,
      headsign: null,
    });
    expect(body.vehicles[0].route.color).toBe(null); // 'red' fails hex check
  });
});

describe('transport caching', () => {
  it('reuses the cache for bboxes snapping to the same cell', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [rawVehicle()] } });
    await getVehicles('minLat=-33.918&maxLat=-33.832&minLon=151.151&maxLon=151.209&feeds=bs');
    await getVehicles('minLat=-33.913&maxLat=-33.839&minLon=151.158&maxLon=151.202&feeds=bs');
    expect(fetchJsonMock).toHaveBeenCalledTimes(1);
  });

  it('different feeds miss the cache', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    await getVehicles(`${CBD}&feeds=bs`);
    await getVehicles(`${CBD}&feeds=st`);
    expect(fetchJsonMock).toHaveBeenCalledTimes(2);
  });
});

describe('transport kill switch + failures', () => {
  it('returns empty without touching upstream when disabled', async () => {
    configMock['TRANSPORT_DISABLED'] = true;
    const body = await (await getVehicles(`${CBD}&feeds=bs`)).json();
    expect(body).toMatchObject({ vehicles: [], count: 0, disabled: true });
    expect(fetchJsonMock).not.toHaveBeenCalled();
  });

  it('502s on cold-path upstream failure', async () => {
    fetchJsonMock.mockRejectedValue(new Error('fetch failed: ETIMEDOUT'));
    expect((await getVehicles(`${CBD}&feeds=bs`)).status).toBe(502);
  });
});

describe('transport shapes', () => {
  async function getShape(id: string) {
    const { transportRouter } = await import('../../../src/api/transport.js');
    return transportRouter.request(`/api/transport/shape/${encodeURIComponent(id)}`);
  }

  it('passes the encoded polyline through and caches it', async () => {
    fetchJsonMock.mockResolvedValue({ response: { shape: { id: 'au2:bs:222299', enc: 'rr_nEmdzx[PC' } } });
    const body = await (await getShape('au2:bs:222299')).json();
    expect(body).toEqual({ id: 'au2:bs:222299', enc: 'rr_nEmdzx[PC' });
    await getShape('au2:bs:222299');
    expect(fetchJsonMock).toHaveBeenCalledTimes(1);
    // Raw id in the upstream path — AnyTrip 404s on %3A-encoded colons.
    expect(fetchJsonMock.mock.calls[0]?.[0]).toContain('/shape/au2:bs:222299');
  });

  it('400s on malformed shape ids', async () => {
    expect((await getShape('DROP TABLE')).status).toBe(400);
    expect((await getShape('au2:buses:../../etc')).status).toBe(400);
  });

  it('returns enc null when upstream has no geometry', async () => {
    fetchJsonMock.mockResolvedValue({ response: {} });
    const body = await (await getShape('au2:st:x1')).json();
    expect(body).toEqual({ id: 'au2:st:x1', enc: null });
  });
});

describe('transport stops', () => {
  it('normalizes stops and strips mode prefixes', async () => {
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
              facilities: { accessibility: ['Lift', 'Escalator'] },
            },
          },
          { stop: { id: 'au2:nowhere' } }, // no coords — dropped
        ],
      },
    });
    const body = await (await getStops(`${CBD}&modes=sydneytrains,metro`)).json();
    expect(body.count).toBe(1);
    expect(body.stops[0]).toMatchObject({
      id: 'au2:200060',
      name: 'Central Station',
      lat: -33.88388,
      modes: ['metro', 'sydneytrains'],
      locality: 'Sydney',
      wheelchair: true,
      accessibility: ['Lift', 'Escalator'],
    });
    const url = fetchJsonMock.mock.calls[0]?.[0] as string;
    expect(url).toContain('limit=500');
    expect(decodeURIComponent(url)).toContain('au2:metro,au2:sydneytrains');
  });
});
