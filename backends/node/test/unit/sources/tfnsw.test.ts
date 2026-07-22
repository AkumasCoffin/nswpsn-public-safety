/**
 * TfNSW GTFS-realtime integration: protobuf decoding (positions +
 * alerts), route-name parsing, the AnyTrip position join, config
 * gating and the /api/transport/alerts endpoint.
 *
 * Fixtures are built with the real gtfs-realtime-bindings encoder so
 * the decode path is exercised end-to-end.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import GtfsRealtimeBindings from 'gtfs-realtime-bindings';

const rt = GtfsRealtimeBindings.transit_realtime;

const { fetchBufferMock, configMock } = vi.hoisted(() => ({
  fetchBufferMock: vi.fn(),
  configMock: {
    LOG_LEVEL: 'warn',
    NODE_ENV: 'test',
    STATE_DIR: './test/.tmp-state',
    NSWPSN_API_KEY: 'test-key',
    PORT: 3001,
    TRANSPORT_DISABLED: false,
    TFNSW_API_KEY: 'test-tfnsw-key',
    TFNSW_DISABLED: false,
  } as Record<string, unknown>,
}));

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: vi.fn(),
  fetchRaw: vi.fn(),
  fetchBuffer: fetchBufferMock,
  HttpError: class extends Error {
    status: number | null;
    constructor(message: string, status: number | null = null) {
      super(message);
      this.status = status;
    }
  },
}));

vi.mock('../../../src/config.js', () => ({
  config: configMock,
  modeLabel: () => 'dev',
}));

import {
  fetchTfnswPositions,
  applyTfnswPositions,
  fetchTfnswAlerts,
  routeNameFromId,
  tfnswConfigured,
  _resetTfnswForTests,
  _testables,
  type TfnswPosition,
} from '../../../src/sources/tfnsw.js';
import type { TransportVehicle } from '../../../src/api/transport.js';

const NOW_SEC = () => Math.floor(Date.now() / 1000);

function encodeVehicleFeed(
  vehicles: Array<Record<string, unknown>>,
): Uint8Array {
  const msg = rt.FeedMessage.create({
    header: { gtfsRealtimeVersion: '2.0', timestamp: NOW_SEC() },
    entity: vehicles.map((v, i) => ({ id: String(i + 1), vehicle: v })),
  });
  return rt.FeedMessage.encode(msg).finish();
}

function fixtureVehicle(overrides: Record<string, unknown> = {}) {
  return {
    trip: { tripId: 'W123', routeId: 'CTY_T1', startDate: '20260722' },
    position: { latitude: -33.87, longitude: 151.2, bearing: 90, speed: 16.6 },
    timestamp: NOW_SEC() - 5,
    occupancyStatus: 2,
    vehicle: { id: 'A21', label: 'Waratah A21' },
    ...overrides,
  };
}

function anytripVehicle(overrides: Partial<TransportVehicle> = {}): TransportVehicle {
  return {
    id: 'v1',
    lat: -33.9,
    lon: 151.1,
    bearing: 45,
    speedKmh: 40,
    mode: 'sydneytrains',
    route: { id: 'au2:st:T1', name: 'T1', longName: 'North Shore', color: '#F18500', textColor: '#FFFFFF' },
    headsign: 'City', headsignSub: null, agency: 'Sydney Trains',
    occupancy: null, wheelchair: null, aircon: null, model: 'Waratah',
    ageSec: 30, tripId: 'W123', shapeId: 'au2:st:shape1',
    startDate: '20260722', instanceNumber: 0,
    ...overrides,
  };
}

const BBOX = { minLat: -34.2, maxLat: -33.4, minLon: 150.5, maxLon: 151.5 };

beforeEach(() => {
  fetchBufferMock.mockReset();
  configMock.TFNSW_API_KEY = 'test-tfnsw-key';
  configMock.TFNSW_DISABLED = false;
  _resetTfnswForTests();
});

describe('decodePositions', () => {
  it('decodes a GTFS-R vehicle entity', () => {
    const msg = rt.FeedMessage.decode(encodeVehicleFeed([fixtureVehicle()]));
    const out = _testables.decodePositions(msg, 'sydneytrains');
    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject({
      tripId: 'W123',
      routeId: 'CTY_T1',
      startDate: '20260722',
      lat: expect.closeTo(-33.87, 4),
      lon: expect.closeTo(151.2, 4),
      bearing: 90,
      speedKmh: 59.8, // 16.6 m/s
      occupancy: 2,
      vehicleId: 'A21',
      mode: 'sydneytrains',
    });
  });

  it('drops entities without a position and stale timestamps', () => {
    const stale = fixtureVehicle({ timestamp: NOW_SEC() - 3600 });
    const noPos = { trip: { tripId: 'X' } };
    const msg = rt.FeedMessage.decode(encodeVehicleFeed([stale, noPos]));
    expect(_testables.decodePositions(msg, 'sydneytrains')).toHaveLength(0);
  });

  it('treats ABSENT bearing/speed/occupancy as null, not protobuf default 0', () => {
    // A feed entity with position only — protobufjs serves 0 for the
    // missing fields via the prototype; they must decode as null so
    // the join keeps AnyTrip's real bearing/speed/occupancy.
    const bare = {
      trip: { tripId: 'W123' },
      position: { latitude: -33.87, longitude: 151.2 },
      timestamp: NOW_SEC() - 5,
    };
    const msg = rt.FeedMessage.decode(encodeVehicleFeed([bare]));
    const out = _testables.decodePositions(msg, 'sydneytrains');
    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject({ bearing: null, speedKmh: null, occupancy: null });
    // ...but a REAL 0 bearing (due north) survives.
    const north = { ...bare, position: { latitude: -33.87, longitude: 151.2, bearing: 0 } };
    const out2 = _testables.decodePositions(rt.FeedMessage.decode(encodeVehicleFeed([north])), 'sydneytrains');
    expect(out2[0]!.bearing).toBe(0);
  });
});

describe('routeNameFromId', () => {
  it('extracts line codes from common TfNSW route id shapes', () => {
    expect(routeNameFromId('CTY_T1')).toBe('T1');
    expect(routeNameFromId('SMNW_M1')).toBe('M1');
    expect(routeNameFromId('F1_SYD')).toBe('F1');
    expect(routeNameFromId('CCN_W1')).toBe('CCN');
    expect(routeNameFromId('2441_374')).toBe('374');
    expect(routeNameFromId(null)).toBeNull();
    expect(routeNameFromId('averylongroutename_withnomatch')).toBeNull();
  });
});

describe('applyTfnswPositions', () => {
  const tfPos = (overrides: Partial<TfnswPosition> = {}): TfnswPosition => ({
    tripId: 'W123', routeId: 'CTY_T1', startDate: '20260722',
    lat: -33.85, lon: 151.21, bearing: 100, speedKmh: 55,
    timestamp: NOW_SEC() - 3, occupancy: 3, vehicleId: 'A21',
    label: 'Waratah A21', mode: 'sydneytrains',
    ...overrides,
  });

  it('TfNSW coordinates are primary for matched vehicles, metadata stays AnyTrip', () => {
    const { vehicles, matched, added } = applyTfnswPositions(
      [anytripVehicle({ ageSec: 30 })], [tfPos()], BBOX, 1500,
    );
    expect(matched).toBe(1);
    expect(added).toBe(0);
    expect(vehicles[0]).toMatchObject({
      lat: -33.85, lon: 151.21, bearing: 100, speedKmh: 55, occupancy: 3,
      route: { name: 'T1', color: '#F18500' }, // AnyTrip metadata retained
      shapeId: 'au2:st:shape1',
    });
    expect(vehicles[0]!.ageSec).toBeLessThan(10); // TfNSW timestamp won
  });

  it('appends TfNSW-only trips inside the bbox as synthetic vehicles', () => {
    const { vehicles, added } = applyTfnswPositions(
      [anytripVehicle()],
      [tfPos(), tfPos({ tripId: 'W999', vehicleId: 'B7', routeId: 'SMNW_M1', mode: 'metro' })],
      BBOX, 1500,
    );
    expect(added).toBe(1);
    const synth = vehicles.find((v) => v.tripId === 'W999');
    expect(synth).toMatchObject({
      id: 'B7', mode: 'metro', shapeId: null, instanceNumber: 0,
      route: { name: 'M1', color: null },
    });
  });

  it('drops TfNSW-only trips outside the bbox and respects the cap', () => {
    const outside = tfPos({ tripId: 'FAR1', lat: -30, lon: 152 });
    const r1 = applyTfnswPositions([], [outside], BBOX, 1500);
    expect(r1.vehicles).toHaveLength(0);
    const r2 = applyTfnswPositions(
      [anytripVehicle()], [tfPos({ tripId: 'W555' })], BBOX, 1,
    );
    expect(r2.vehicles).toHaveLength(1); // cap blocks the append
  });

  it('keeps serving a blinked-out entity via per-trip stickiness', () => {
    const first = applyTfnswPositions([anytripVehicle()], [tfPos()], BBOX, 1500);
    expect(first.vehicles[0]!.lat).toBe(-33.85);
    // The entity misses the next batch (GTFS-R blink) — the vehicle
    // must STAY in the TfNSW frame via the remembered position, not
    // flip to AnyTrip's ~30s-ahead interpolation and back.
    const other = tfPos({ tripId: 'OTHER', lat: -33.86, lon: 151.0 });
    const second = applyTfnswPositions([anytripVehicle()], [other], BBOX, 1500);
    expect(second.vehicles[0]!.lat).toBe(-33.85);
    expect(second.vehicles[0]!.lon).toBe(151.21);
  });

  it('drops a vehicle once its TfNSW position goes stale (no AnyTrip fallback)', () => {
    vi.useFakeTimers();
    try {
      applyTfnswPositions([anytripVehicle()], [tfPos()], BBOX, 1500);
      vi.advanceTimersByTime(70_000); // past the 60s sticky window
      const other = tfPos({ tripId: 'OTHER', lat: -33.86, lon: 151.0 });
      const r = applyTfnswPositions([anytripVehicle()], [other], BBOX, 1500);
      // TfNSW is the ONLY position source now: with no fresh/sticky TfNSW
      // position for W123 it's dropped, never shown at AnyTrip's frame.
      expect(r.vehicles.find((v) => v.tripId === 'W123')).toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it('returns no vehicles when TfNSW has no positions', () => {
    // TfNSW is the sole position source, so "no positions" ⇒ nothing to
    // place. (The route only calls this when TfNSW returned data; a total
    // TfNSW outage is handled upstream by keeping AnyTrip as-is.)
    const { vehicles } = applyTfnswPositions([anytripVehicle()], [], BBOX, 1500);
    expect(vehicles).toHaveLength(0);
  });

  it('keeps AnyTrip bearing/speed/occupancy when TfNSW decoded them as null', () => {
    const { vehicles } = applyTfnswPositions(
      [anytripVehicle({ bearing: 123, speedKmh: 77, occupancy: 4 })],
      [tfPos({ bearing: null, speedKmh: null, occupancy: null })],
      BBOX, 1500,
    );
    expect(vehicles[0]).toMatchObject({ bearing: 123, speedKmh: 77, occupancy: 4 });
  });

  it('on a trip-id mismatch, positions come purely from TfNSW (AnyTrip dropped)', () => {
    // If AnyTrip's rtTripId space differs from GTFS-R's tripId, nothing
    // matches: the AnyTrip vehicle (interpolated frame) is dropped and the
    // TfNSW entity becomes the vehicle — one marker, TfNSW-positioned, no
    // duplicate. There's no AnyTrip-frame fallback to alternate with.
    const { vehicles, matched, added } = applyTfnswPositions(
      [anytripVehicle({ tripId: 'anytrip-style-id' })],
      [tfPos({ tripId: 'gtfs-style-id' })],
      BBOX, 1500,
    );
    expect(matched).toBe(0);
    expect(added).toBe(1);
    expect(vehicles).toHaveLength(1);
    expect(vehicles[0]!.tripId).toBe('gtfs-style-id'); // TfNSW, not AnyTrip
    expect(vehicles[0]!.lat).toBe(-33.85);             // TfNSW coordinates
  });
});

describe('fetchTfnswPositions', () => {
  it('returns [] when unconfigured, without touching the network', async () => {
    configMock.TFNSW_API_KEY = undefined;
    expect(tfnswConfigured()).toBe(false);
    expect(await fetchTfnswPositions(['st'])).toEqual([]);
    expect(fetchBufferMock).not.toHaveBeenCalled();
  });

  it('returns [] when the kill switch is on', async () => {
    configMock.TFNSW_DISABLED = true;
    expect(await fetchTfnswPositions(['st'])).toEqual([]);
    expect(fetchBufferMock).not.toHaveBeenCalled();
  });

  it('fetches the mapped feed with the apikey header and decodes', async () => {
    fetchBufferMock.mockResolvedValue(encodeVehicleFeed([fixtureVehicle()]));
    const out = await fetchTfnswPositions(['st']);
    expect(out).toHaveLength(1);
    expect(out[0]!.tripId).toBe('W123');
    const [url, opts] = fetchBufferMock.mock.calls[0]!;
    expect(url).toBe('https://api.transport.nsw.gov.au/v2/gtfs/vehiclepos/sydneytrains');
    expect(opts.headers.Authorization).toBe('apikey test-tfnsw-key');
  });

  it('tolerates a failing feed (contributes nothing)', async () => {
    fetchBufferMock.mockRejectedValue(new Error('boom'));
    expect(await fetchTfnswPositions(['st'])).toEqual([]);
  });

  it('serves LAST-GOOD positions through a failing refresh — never an empty flip', async () => {
    vi.useFakeTimers();
    try {
      fetchBufferMock.mockResolvedValueOnce(encodeVehicleFeed([fixtureVehicle()]));
      const first = await fetchTfnswPositions(['st']);
      expect(first).toHaveLength(1);
      vi.advanceTimersByTime(25_000); // past fresh (15s + ≤5s jitter)
      fetchBufferMock.mockRejectedValue(Object.assign(new Error('HTTP 429'), { status: 429 }));
      const second = await fetchTfnswPositions(['st']);
      // A failed refresh must serve the previous batch (stale window),
      // not cache [] as a success — an empty batch flips every matched
      // vehicle back to the AnyTrip frame and teleports pins.
      expect(second).toHaveLength(1);
      expect(second[0]!.tripId).toBe('W123');
    } finally {
      vi.useRealTimers();
    }
  });

  it('parks a 429-rate-limited feed instead of re-hitting it', async () => {
    fetchBufferMock.mockRejectedValue(Object.assign(new Error('HTTP 429'), { status: 429 }));
    expect(await _testables.fetchFeed('/v2/gtfs/vehiclepos/sydneytrains')).toBeNull();
    expect(fetchBufferMock).toHaveBeenCalledTimes(1);
    // Parked — the next attempt short-circuits without a fetch.
    expect(await _testables.fetchFeed('/v2/gtfs/vehiclepos/sydneytrains')).toBeNull();
    expect(fetchBufferMock).toHaveBeenCalledTimes(1);
  });

  it('parks a 404 feed for the long window', async () => {
    fetchBufferMock.mockRejectedValue(Object.assign(new Error('HTTP 404'), { status: 404 }));
    await _testables.fetchFeed('/v1/gtfs/vehiclepos/lightrail/innerwest');
    await _testables.fetchFeed('/v1/gtfs/vehiclepos/lightrail/innerwest');
    expect(fetchBufferMock).toHaveBeenCalledTimes(1);
  });
});

describe('alerts', () => {
  function encodeAlertFeed(): Uint8Array {
    const msg = rt.FeedMessage.create({
      header: { gtfsRealtimeVersion: '2.0', timestamp: NOW_SEC() },
      entity: [
        {
          id: 'alert-1',
          alert: {
            activePeriod: [{ start: 1750000000, end: 1750100000 }],
            informedEntity: [
              { routeId: 'CTY_T1' },
              { stopId: '2000336' },
              { agencyId: 'SydneyTrains' },
            ],
            cause: rt.Alert.Cause.MAINTENANCE,
            effect: rt.Alert.Effect.DETOUR,
            severityLevel: rt.Alert.SeverityLevel.WARNING,
            headerText: { translation: [{ text: 'Trackwork on T1', language: 'en' }] },
            descriptionText: { translation: [{ text: 'Buses replace trains.', language: 'en' }] },
            url: { translation: [{ text: 'https://transportnsw.info/alerts' }] },
          },
        },
        { id: 'no-alert' }, // non-alert entity skipped
      ],
    });
    return rt.FeedMessage.encode(msg).finish();
  }

  it('decodes and normalizes alert entities', () => {
    const msg = rt.FeedMessage.decode(encodeAlertFeed());
    const out = _testables.decodeAlerts(msg);
    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject({
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
    });
  });

  it('fetchTfnswAlerts uses the consolidated feed and caches', async () => {
    fetchBufferMock.mockResolvedValue(encodeAlertFeed());
    const first = await fetchTfnswAlerts();
    expect(first).toHaveLength(1);
    expect(fetchBufferMock.mock.calls[0]![0]).toBe(
      'https://api.transport.nsw.gov.au/v2/gtfs/alerts/all',
    );
    await fetchTfnswAlerts();
    expect(fetchBufferMock).toHaveBeenCalledTimes(1); // SWR fresh window
  });

  it('returns [] when unconfigured', async () => {
    configMock.TFNSW_API_KEY = undefined;
    expect(await fetchTfnswAlerts()).toEqual([]);
  });
});

describe('/api/transport/alerts endpoint', () => {
  it('reports configured:false without a key', async () => {
    configMock.TFNSW_API_KEY = undefined;
    const { transportRouter } = await import('../../../src/api/transport.js');
    const res = await transportRouter.request('/api/transport/alerts');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { configured: boolean; alerts: unknown[] };
    expect(body.configured).toBe(false);
    expect(body.alerts).toEqual([]);
  });
});
