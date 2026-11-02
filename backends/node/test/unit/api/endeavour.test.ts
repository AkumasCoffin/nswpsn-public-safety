/**
 * Endeavour Energy outages on their /api/ paths — this site's and the bot's,
 * not on /api/v1/ (an API key reads /api/v1/outages).
 *
 * The routes that read Endeavour's outage service on the request
 * (/current/all, /future/all, /current/raw, /future/raw, /postcodes) answer
 * an empty 200 when it cannot be read, as they always have. Checked byte for
 * byte.
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

const LISTS = ['/api/endeavour/current', '/api/endeavour/maintenance', '/api/endeavour/future', '/api/endeavour/planned'];

async function app() {
  const { endeavourRouter } = await import('../../../src/api/endeavour.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', endeavourRouter);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function get(a: Hono, path: string) {
  const res = await a.request(path);
  return { status: res.status, type: res.headers.get('content-type'), text: await res.text() };
}

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
  it('answers each list as the poll left it', async () => {
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
    }
    expect(fetchJsonMock).not.toHaveBeenCalled();
  });

  it('an empty list before the first poll, or when the snapshot is not a list', async () => {
    liveStore.set('endeavour_maintenance', { not: 'a list' });
    const a = await app();
    for (const p of LISTS) expect((await get(a, p)).text, p).toBe('[]');
  });
});

describe('the outage areas read on the request', () => {
  it("answers Endeavour's records as published, split on PLANNED", async () => {
    fetchJsonMock.mockResolvedValue(AREAS);
    const a = await app();
    expect(await get(a, '/api/endeavour/current/all')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(NOT_PLANNED) });
    expect(await get(a, '/api/endeavour/future/all')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(PLANNED) });
    expect(String(fetchJsonMock.mock.calls[0]?.[0])).toContain('/rpc/get_outage_areas_fast');
    expect(fetchJsonMock.mock.calls[0]?.[1]).toMatchObject({ method: 'POST' });
  });

  it('when Endeavour cannot be read, or answers with something that is not a list: an empty 200', async () => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 502'));
    const a = await app();
    for (const p of ['/api/endeavour/current/all', '/api/endeavour/future/all']) {
      expect(await get(a, p), p).toEqual({ status: 200, type: 'application/json', text: '[]' });
    }
    fetchJsonMock.mockReset();
    fetchJsonMock.mockResolvedValue({ message: 'JWT expired' });
    for (const p of ['/api/endeavour/current/all', '/api/endeavour/future/all']) {
      expect(await get(a, p), p).toEqual({ status: 200, type: 'application/json', text: '[]' });
    }
  });

  it('/current/raw and /future/raw answer what /current/all and /future/all do', async () => {
    fetchJsonMock.mockResolvedValue(AREAS);
    const a = await app();
    expect(await get(a, '/api/endeavour/current/raw')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(NOT_PLANNED) });
    expect(await get(a, '/api/endeavour/future/raw')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(PLANNED) });
    fetchJsonMock.mockReset();
    fetchJsonMock.mockRejectedValue(new Error('down'));
    expect(await get(a, '/api/endeavour/current/raw')).toEqual({ status: 200, type: 'application/json', text: '[]' });
  });
});

describe('/endeavour/postcodes', () => {
  const ROWS = [{ postcode: '2750' }, { postcode: '2000' }, { postcode: '2750' }, { postcode: null }, { postcode: '' }];
  const WANT = { postcodes: ['2000', '2750'], count: 2 };

  it('answers the distinct postcodes, sorted, and keeps them for an hour', async () => {
    fetchJsonMock.mockResolvedValue(ROWS);
    const a = await app();
    expect(await get(a, '/api/endeavour/postcodes')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(WANT) });
    expect(await get(a, '/api/endeavour/postcodes')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(WANT) });
    expect(fetchJsonMock).toHaveBeenCalledTimes(1);
    const url = String(fetchJsonMock.mock.calls[0]?.[0]);
    expect(url).toContain('/outage-points');
    expect(url).toContain('limit=5000');
  });

  it('a failure is never kept: an empty 200, and the next request asks again', async () => {
    fetchJsonMock.mockRejectedValue(new Error('timeout'));
    const a = await app();
    expect(await get(a, '/api/endeavour/postcodes')).toEqual({
      status: 200,
      type: 'application/json',
      text: '{"postcodes":[],"count":0}',
    });

    fetchJsonMock.mockReset();
    fetchJsonMock.mockResolvedValue('not a list');
    expect((await get(a, '/api/endeavour/postcodes')).text).toBe('{"postcodes":[],"count":0}');
    expect((await get(a, '/api/endeavour/postcodes')).text).toBe('{"postcodes":[],"count":0}');
    expect(fetchJsonMock).toHaveBeenCalledTimes(2);

    fetchJsonMock.mockReset();
    fetchJsonMock.mockResolvedValue(ROWS);
    expect((await get(a, '/api/endeavour/postcodes')).text).toBe(JSON.stringify(WANT));
    expect(fetchJsonMock).toHaveBeenCalledTimes(1);
  });

  it('a kept answer outlives a later failure', async () => {
    fetchJsonMock.mockResolvedValue(ROWS);
    const a = await app();
    await get(a, '/api/endeavour/postcodes');
    fetchJsonMock.mockReset();
    fetchJsonMock.mockRejectedValue(new Error('down'));
    expect((await get(a, '/api/endeavour/postcodes')).text).toBe(JSON.stringify(WANT));
    expect(fetchJsonMock).not.toHaveBeenCalled();
  });
});
