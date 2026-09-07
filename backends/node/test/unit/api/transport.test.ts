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
  // TfNSW disabled in these tests — the join must be a passthrough.
  TFNSW_API_KEY: undefined,
  TFNSW_DISABLED: false,
};

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
  it('drops vehicles with stale positions (parked sets / OCCP ghosts)', async () => {
    const stale = rawVehicle();
    (stale.vehicleInstance as { lastPosition: { time: number } }).lastPosition.time =
      Math.floor(Date.now() / 1000) - 3600;
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [stale, rawVehicle()] } });
    const res = await getVehicles(`${CBD}&feeds=bs`);
    const body = await res.json();
    expect(body.count).toBe(1); // only the fresh one survives
  });

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

describe('transport au4 (Queensland)', () => {
  const BNE = 'minLat=-27.6&maxLat=-27.3&minLon=152.9&maxLon=153.2';

  function rawAu4Vehicle(mode: string, id: string) {
    return rawVehicle({
      tripInstance: {
        shapeId: 'au4:se:7600294',
        trip: {
          id: `au4:se:${id}`,
          route: { id: `au4:se:${id}`, name: 'T4', color: '4E84C4', mode },
        },
      },
      vehicleInstance: {
        id,
        lastPosition: {
          time: Math.floor(Date.now() / 1000) - 12,
          coordinates: { lat: -27.47, lon: 153.02 },
        },
      },
    });
  }

  it('rejects a Brisbane bbox without region=au4, serves it with', async () => {
    expect((await getVehicles(`${BNE}&feeds=bs`)).status).toBe(400);
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    const res = await getVehicles(`${BNE}&region=au4`);
    expect(res.status).toBe(200);
    const url = fetchJsonMock.mock.calls[0]?.[0] as string;
    expect(url).toContain('/region/au4/vehicles');
    // The single SEQ feed carries every mode; modes= is the filter.
    expect(decodeURIComponent(url)).toContain('feeds=au4:se');
    expect(decodeURIComponent(url)).toContain(
      'modes=au4:buses,au4:ferries,au4:lightrail,au4:trains',
    );
    expect(url).toContain('otrFilter=300');
  });

  it('filters au4 vehicles by modes= and rejects unknown values', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    await getVehicles(`${BNE}&region=au4&modes=trains`);
    const url = decodeURIComponent(fetchJsonMock.mock.calls[0]?.[0] as string);
    expect(url).toContain('modes=au4:trains');
    expect(url).not.toContain('au4:buses');
    expect((await getVehicles(`${BNE}&region=au4&modes=metro`)).status).toBe(400);
    expect((await getVehicles(`${BNE}&region=au42`)).status).toBe(400);
  });

  it('collapses au4 mode names onto the shared vocabulary', async () => {
    fetchJsonMock.mockResolvedValue({
      response: {
        vehicles: [rawAu4Vehicle('au4:trains', 'v1'), rawAu4Vehicle('au4:buses', 'v2')],
      },
    });
    const body = await (await getVehicles(`${BNE}&region=au4`)).json();
    const modes = body.vehicles.map((v: { mode: string }) => v.mode).sort();
    expect(modes).toEqual(['buses', 'sydneytrains']);
  });

  it('keeps au2 and au4 cache entries apart in the border overlap band', async () => {
    // Tweed-ish bbox valid in BOTH regions — same snapped coords, so
    // only the region in the key separates them.
    const tweed = 'minLat=-28.4&maxLat=-28.1&minLon=153.3&maxLon=153.6';
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    await getVehicles(`${tweed}&feeds=bs`);
    await getVehicles(`${tweed}&region=au4&modes=buses`);
    expect(fetchJsonMock).toHaveBeenCalledTimes(2);
    expect(fetchJsonMock.mock.calls[0]?.[0]).toContain('/region/au2/');
    expect(fetchJsonMock.mock.calls[1]?.[0]).toContain('/region/au4/');
  });

  it('serves au4 stops with au4 mode ids and canonical output modes', async () => {
    fetchJsonMock.mockResolvedValue({
      response: {
        stops: [
          {
            stop: {
              id: 'au4:600029',
              fullName: 'Roma Street station',
              coordinates: { lat: -27.4655, lon: 153.0185 },
              modes: ['au4:trains'],
            },
          },
        ],
      },
    });
    const body = await (await getStops(`${BNE}&region=au4&modes=trains,ferries,lightrail`)).json();
    expect(body.stops[0].modes).toEqual(['sydneytrains']);
    const url = decodeURIComponent(fetchJsonMock.mock.calls[0]?.[0] as string);
    expect(url).toContain('/region/au4/stops');
    expect(url).toContain('au4:ferries,au4:lightrail,au4:trains');
    // au4 has no metro; buses stay excluded in every region.
    expect((await getStops(`${BNE}&region=au4&modes=metro`)).status).toBe(400);
    expect((await getStops(`${BNE}&region=au4&modes=buses`)).status).toBe(400);
  });

  it('routes id-addressed endpoints by the id prefix', async () => {
    const { transportRouter } = await import('../../../src/api/transport.js');
    fetchJsonMock.mockResolvedValue({ response: { shape: { id: 'au4:se:x1', enc: 'abc' } } });
    const res = await transportRouter.request('/api/transport/shape/au4:se:x1');
    expect(res.status).toBe(200);
    expect(fetchJsonMock.mock.calls[0]?.[0]).toContain('/region/au4/shape/au4:se:x1');
    // Passes the shape regex but not the region map.
    expect((await transportRouter.request('/api/transport/shape/au42:xx:1')).status).toBe(400);
    fetchJsonMock.mockResolvedValue({ response: { departures: [] } });
    await transportRouter.request('/api/transport/departures/au4:600029');
    const depUrl = fetchJsonMock.mock.calls[1]?.[0] as string;
    expect(depUrl).toContain('/region/au4/departures/au4:600029');
    expect((await transportRouter.request('/api/transport/departures/au42:1')).status).toBe(400);
  });
});

describe('transport trip ids with spaces (QLD timetables)', () => {
  it('accepts them and %20-encodes only the space upstream', async () => {
    const { transportRouter } = await import('../../../src/api/transport.js');
    fetchJsonMock.mockResolvedValue({ response: { tripInstance: { trip: {} } } });
    const id = 'au4:se:39018846-QR 26_27-44157-DA49';
    const res = await transportRouter.request(
      `/api/transport/trip/20260907/${encodeURIComponent(id)}/0`);
    expect(res.status).toBe(200);
    const url = fetchJsonMock.mock.calls[0]?.[0] as string;
    expect(url).toContain('/region/au4/tripInstance/20260907/au4:se:39018846-QR%2026_27-44157-DA49/0');
  });
});

describe('transport au3/au5/au9 (VIC, SA, ACT)', () => {
  const MEL = 'minLat=-37.9&maxLat=-37.7&minLon=144.8&maxLon=145.1';
  const ADL = 'minLat=-35.0&maxLat=-34.8&minLon=138.5&maxLon=138.7';
  const CBR = 'minLat=-35.35&maxLat=-35.1&minLon=149.0&maxLon=149.2';

  function rawModal(regionFeed: string, mode: string, id: string) {
    return {
      tripInstance: {
        trip: {
          id: `${regionFeed}:${id}`,
          route: { id: `${regionFeed}:${id}`, name: 'X', color: '4E84C4', mode },
        },
      },
      vehicleInstance: {
        id,
        lastPosition: {
          time: Math.floor(Date.now() / 1000) - 12,
          coordinates: { lat: -37.8, lon: 144.9 },
        },
      },
    };
  }

  it('au3 filters by modes= alone (no feeds param)', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    const res = await getVehicles(`${MEL}&region=au3`);
    expect(res.status).toBe(200);
    const url = decodeURIComponent(fetchJsonMock.mock.calls[0]?.[0] as string);
    expect(url).toContain('/region/au3/vehicles');
    expect(url).not.toContain('feeds=');
    expect(url).toContain('modes=au3:buses,au3:metrotrains,au3:tram,au3:vlinetrains');
  });

  it('an Adelaide bbox needs region=au5 (outside every other envelope)', async () => {
    expect((await getVehicles(`${ADL}&feeds=bs`)).status).toBe(400);
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    expect((await getVehicles(`${ADL}&region=au5`)).status).toBe(200);
    const url = decodeURIComponent(fetchJsonMock.mock.calls[0]?.[0] as string);
    expect(url).toContain('/region/au5/vehicles');
    expect(url).toContain('modes=au5:buses,au5:lightrail,au5:schoolbuses,au5:trains');
  });

  it('the ACT sits inside au2 — same bbox serves both regions, keyed apart', async () => {
    fetchJsonMock.mockResolvedValue({ response: { vehicles: [] } });
    await getVehicles(`${CBR}&feeds=bs`);
    await getVehicles(`${CBR}&region=au9&modes=lightrail,buses`);
    expect(fetchJsonMock).toHaveBeenCalledTimes(2);
    expect(fetchJsonMock.mock.calls[0]?.[0]).toContain('/region/au2/');
    const au9Url = decodeURIComponent(fetchJsonMock.mock.calls[1]?.[0] as string);
    expect(au9Url).toContain('/region/au9/vehicles');
    expect(au9Url).toContain('modes=au9:buses,au9:lightrail');
    // au9 runs no ferries.
    expect((await getVehicles(`${CBR}&region=au9&modes=ferries`)).status).toBe(400);
  });

  it('collapses the new mode names onto the shared vocabulary', async () => {
    fetchJsonMock.mockResolvedValue({
      response: {
        vehicles: [
          rawModal('au3:ad', 'au3:metrotrains', 'v1'),
          rawModal('au3:ad', 'au3:tram', 'v2'),
          rawModal('au3:ad', 'au3:vlinetrains', 'v3'),
          rawModal('au3:ad', 'au3:buses', 'v4'),
        ],
      },
    });
    const body = await (await getVehicles(`${MEL}&region=au3`)).json();
    const modes = body.vehicles.map((v: { mode: string }) => v.mode).sort();
    expect(modes).toEqual(['buses', 'lightrail', 'nswtrains', 'sydneytrains']);
  });

  it('au5/au9 school buses ride the buses vocabulary', async () => {
    fetchJsonMock.mockResolvedValue({
      response: { vehicles: [rawModal('au9:nw', 'au9:schoolbuses', 'v9')] },
    });
    const body = await (await getVehicles(`${CBR}&region=au9`)).json();
    expect(body.vehicles[0].mode).toBe('buses');
  });

  it('serves au3 stops and accepts au5 feed-segment stop ids', async () => {
    fetchJsonMock.mockResolvedValue({
      response: {
        stops: [
          {
            stop: {
              id: 'au3:G1058',
              fullName: 'East Malvern Railway Station',
              coordinates: { lat: -37.877, lon: 145.041 },
              modes: ['au3:metrotrains'],
            },
          },
        ],
      },
    });
    const body = await (await getStops(`${MEL}&region=au3&modes=metrotrains,tram`)).json();
    expect(body.stops[0].modes).toEqual(['sydneytrains']);
    const url = decodeURIComponent(fetchJsonMock.mock.calls[0]?.[0] as string);
    expect(url).toContain('/region/au3/stops');
    expect(url).toContain('au3:metrotrains,au3:tram');
    // Adelaide stop ids keep a feed segment (au5:ad:50009) — the
    // departures endpoint must accept the extra colon.
    fetchJsonMock.mockResolvedValue({ response: { departures: [] } });
    const dep = await (await import('../../../src/api/transport.js')).transportRouter
      .request('/api/transport/departures/au5:ad:50009');
    expect(dep.status).toBe(200);
    expect(fetchJsonMock.mock.calls[1]?.[0]).toContain('/region/au5/departures/au5:ad:50009');
  });
});
