/**
 * Endeavour Energy outages on /api/v1/ and on their old /api/ paths.
 *
 * One handler serves both (api/v1/mirror.ts): the lists are wrapped in `data`
 * on v1 and bare on the old paths. The routes that read Endeavour's outage
 * service on the request (/current/all, /future/all, /postcodes) keep the
 * empty 200 the old path always sent when it cannot be read, and answer 503
 * service_unavailable on v1. /current/raw and /future/raw stay off v1. The
 * old paths are checked byte for byte.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';

const fetchJsonMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson: fetchJsonMock,
  fetchText: vi.fn(),
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

const V1_PATHS = [
  '/api/endeavour/current',
  '/api/endeavour/maintenance',
  '/api/endeavour/future',
  '/api/endeavour/planned',
  '/api/endeavour/current/all',
  '/api/endeavour/future/all',
  '/api/endeavour/postcodes',
];

async function app() {
  const { endeavourRouter } = await import('../../../src/api/endeavour.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', endeavourRouter);
  mirror(a, endeavourRouter, V1_PATHS);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function get(a: Hono, path: string) {
  const res = await a.request(path);
  return { status: res.status, type: res.headers.get('content-type'), text: await res.text() };
}

const v1 = (p: string) => p.replace(/^\/api\//, '/api/v1/');

function outage(id: string, outageType: string, extra: Record<string, unknown> = {}) {
  return {
    id,
    suburb: 'Penrith',
    streets: 'High St',
    customersAffected: 12,
    status: 'Active',
    cause: 'Tree on lines',
    outageType,
    startTime: '2026-10-28T08:00:00+11:00',
    estimatedRestoration: null,
    lastUpdated: '2026-10-28T08:05:00+11:00',
    latitude: -33.75,
    longitude: 150.69,
    postcode: '2750',
    hasGPS: true,
    ...extra,
  };
}

const CURRENT = [outage('U1', 'Unplanned')];
const MAINTENANCE = [outage('M1', 'Current Maintenance', { endTime: null, duration: '' })];
const FUTURE = [outage('F1', 'Future Maintenance', { endTime: '2026-11-01T15:00:00+11:00', duration: '' })];

// What Endeavour's get_outage_areas_fast answers (trimmed).
const AREAS = [
  { incident_id: 'U1', outage_type: 'UNPLANNED', incident_status: 'NEW', customers_affected: 12, center_lat: -33.75, center_lng: 150.69 },
  { incident_id: 'M1', outage_type: 'PLANNED', incident_status: 'DESPATCHED', customers_affected: 4 },
  { incident_id: 'F1', outage_type: 'planned', incident_status: 'SCHEDULED', customers_affected: 9, extra: 'kept' },
  { incident_id: 'X1' },
];
const NOT_PLANNED = [AREAS[0], AREAS[3]];
const PLANNED = [AREAS[1], AREAS[2]];

beforeEach(async () => {
  fetchJsonMock.mockReset();
  for (const k of ['endeavour_current', 'endeavour_maintenance', 'endeavour_planned']) liveStore.delete(k);
  (await import('../../../src/api/endeavour.js'))._resetEndeavourPostcodesCacheForTests();
});

describe('the outage lists from the poll', () => {
  it('answers each list — wrapped in data on v1, bare on the old path', async () => {
    liveStore.set('endeavour_current', CURRENT);
    liveStore.set('endeavour_maintenance', MAINTENANCE);
    liveStore.set('endeavour_planned', FUTURE);
    const a = await app();
    const want: Record<string, unknown[]> = {
      '/api/endeavour/current': CURRENT,
      '/api/endeavour/maintenance': MAINTENANCE,
      '/api/endeavour/future': FUTURE,
      // Work under way first, then work to come.
      '/api/endeavour/planned': [...MAINTENANCE, ...FUTURE],
    };
    for (const [p, list] of Object.entries(want)) {
      expect(await get(a, p), p).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(list) });
      expect(await get(a, v1(p)), p).toEqual({ status: 200, type: 'application/json', text: JSON.stringify({ data: list }) });
    }
    expect(fetchJsonMock).not.toHaveBeenCalled();
  });

  it('an empty list before the first poll, or when the snapshot is not a list', async () => {
    liveStore.set('endeavour_maintenance', { not: 'a list' });
    const a = await app();
    for (const p of V1_PATHS.slice(0, 4)) {
      expect((await get(a, p)).text, p).toBe('[]');
      expect((await get(a, v1(p))).text, p).toBe('{"data":[]}');
    }
  });
});

describe('the outage areas read on the request', () => {
  it("answers Endeavour's records as published, split on PLANNED", async () => {
    fetchJsonMock.mockResolvedValue(AREAS);
    const a = await app();
    expect(await get(a, '/api/endeavour/current/all')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(NOT_PLANNED) });
    expect(await get(a, '/api/v1/endeavour/current/all')).toEqual({
      status: 200,
      type: 'application/json',
      text: JSON.stringify({ data: NOT_PLANNED }),
    });
    expect(await get(a, '/api/endeavour/future/all')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(PLANNED) });
    expect(await get(a, '/api/v1/endeavour/future/all')).toEqual({
      status: 200,
      type: 'application/json',
      text: JSON.stringify({ data: PLANNED }),
    });
    expect(String(fetchJsonMock.mock.calls[0]?.[0])).toContain('/rpc/get_outage_areas_fast');
    expect(fetchJsonMock.mock.calls[0]?.[1]).toMatchObject({ method: 'POST' });
  });

  it('when Endeavour cannot be read: the old path its empty 200, v1 a 503', async () => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 502'));
    const a = await app();
    for (const p of ['/api/endeavour/current/all', '/api/endeavour/future/all']) {
      expect(await get(a, p), p).toEqual({ status: 200, type: 'application/json', text: '[]' });
      const res = await get(a, v1(p));
      expect(res.status, p).toBe(503);
      expect(JSON.parse(res.text), p).toEqual({ error: { code: 'service_unavailable', message: expect.any(String) } });
    }
  });

  it('an answer that is not a list is a failure on v1, an empty list on the old path', async () => {
    fetchJsonMock.mockResolvedValue({ message: 'JWT expired' });
    const a = await app();
    for (const p of ['/api/endeavour/current/all', '/api/endeavour/future/all']) {
      expect((await get(a, p)).text, p).toBe('[]');
      const res = await get(a, v1(p));
      expect(res.status, p).toBe(503);
      expect(JSON.parse(res.text).error.code, p).toBe('service_unavailable');
    }
  });

  it('a real empty list is an empty list on v1 too', async () => {
    fetchJsonMock.mockResolvedValue([]);
    const a = await app();
    expect(await get(a, '/api/v1/endeavour/current/all')).toEqual({ status: 200, type: 'application/json', text: '{"data":[]}' });
  });
});

describe('/endeavour/postcodes', () => {
  const ROWS = [{ postcode: '2750' }, { postcode: '2000' }, { postcode: '2750' }, { postcode: null }, { postcode: '' }];
  const WANT = { postcodes: ['2000', '2750'], count: 2 };

  it('answers the distinct postcodes, sorted — wrapped on v1 — and keeps them for an hour', async () => {
    fetchJsonMock.mockResolvedValue(ROWS);
    const a = await app();
    expect(await get(a, '/api/endeavour/postcodes')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(WANT) });
    expect(await get(a, '/api/v1/endeavour/postcodes')).toEqual({
      status: 200,
      type: 'application/json',
      text: JSON.stringify({ data: WANT }),
    });
    expect(fetchJsonMock).toHaveBeenCalledTimes(1);
    const url = String(fetchJsonMock.mock.calls[0]?.[0]);
    expect(url).toContain('/outage-points');
    expect(url).toContain('limit=5000');
  });

  it('a failure is never kept: the old path its empty 200, v1 a 503, and the next request asks again', async () => {
    fetchJsonMock.mockRejectedValue(new Error('timeout'));
    const a = await app();
    expect(await get(a, '/api/endeavour/postcodes')).toEqual({
      status: 200,
      type: 'application/json',
      text: '{"postcodes":[],"count":0}',
    });
    const res = await get(a, '/api/v1/endeavour/postcodes');
    expect(res.status).toBe(503);
    expect(JSON.parse(res.text)).toEqual({ error: { code: 'service_unavailable', message: expect.any(String) } });

    fetchJsonMock.mockReset();
    fetchJsonMock.mockResolvedValue('not a list');
    expect((await get(a, '/api/endeavour/postcodes')).text).toBe('{"postcodes":[],"count":0}');
    expect((await get(a, '/api/v1/endeavour/postcodes')).status).toBe(503);
    expect(fetchJsonMock).toHaveBeenCalledTimes(2);

    fetchJsonMock.mockReset();
    fetchJsonMock.mockResolvedValue(ROWS);
    expect((await get(a, '/api/v1/endeavour/postcodes')).text).toBe(JSON.stringify({ data: WANT }));
    expect(fetchJsonMock).toHaveBeenCalledTimes(1);
  });

  it('a kept answer outlives a later failure', async () => {
    fetchJsonMock.mockResolvedValue(ROWS);
    const a = await app();
    await get(a, '/api/endeavour/postcodes');
    fetchJsonMock.mockReset();
    fetchJsonMock.mockRejectedValue(new Error('down'));
    expect((await get(a, '/api/endeavour/postcodes')).text).toBe(JSON.stringify(WANT));
    expect((await get(a, '/api/v1/endeavour/postcodes')).text).toBe(JSON.stringify({ data: WANT }));
    expect(fetchJsonMock).not.toHaveBeenCalled();
  });
});

describe('the mirror', () => {
  it('/current/raw and /future/raw stay off v1, and are unchanged on the old path', async () => {
    fetchJsonMock.mockResolvedValue(AREAS);
    const a = await app();
    expect(await get(a, '/api/endeavour/current/raw')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(NOT_PLANNED) });
    expect(await get(a, '/api/endeavour/future/raw')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(PLANNED) });
    for (const p of ['/api/v1/endeavour/current/raw', '/api/v1/endeavour/future/raw']) {
      const res = await get(a, p);
      expect(res.status, p).toBe(404);
      expect(JSON.parse(res.text).error.code, p).toBe('not_found');
    }
    fetchJsonMock.mockReset();
    fetchJsonMock.mockRejectedValue(new Error('down'));
    expect(await get(a, '/api/endeavour/current/raw')).toEqual({ status: 200, type: 'application/json', text: '[]' });
  });

  it('serves every in-scope route on /api/v1/', async () => {
    fetchJsonMock.mockResolvedValue([]);
    const a = await app();
    for (const p of V1_PATHS) expect((await a.request(v1(p))).status, p).toBe(200);
  });
});
