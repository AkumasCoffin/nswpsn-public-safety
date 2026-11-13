/**
 * CitiPower and Powercor outage source tests.
 *
 * The rows below are real ones from the two distributors' shared file
 * (13 November 2026, 19:41 in Melbourne), each outline cut down to five of
 * its 73 points. The fetch tests mock the shared HTTP module so the upstream
 * URL never gets hit.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// vi.hoisted: see ../endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import register, { parseCppc, fetchCppc, _resetCppcShare } from '../../../../src/sources/outages/cppc.js';

const HAWTHORN_RING = [
  [145.0209330579919, -37.8159642201395],
  [145.0215022561673, -37.8155145584611],
  [145.020933057992, -37.8150648995216],
  [145.0203638598166, -37.8155145584611],
  [145.0209330579919, -37.8159642201395],
];

const CITIPOWER = {
  ORDER_ID: '2413608-1',
  BUSINESS: 'CitiPower',
  AREA: 'BOROONDARA',
  TOWN: 'HAWTHORN',
  POSTCODE: '3122',
  CUSTOMERS: '1',
  ETR: '21:00 13-11-2026',
  CREW_STATUS: 'Outage Reported',
  CAUSE: 'Under investigation',
  START_TIME: '15:53 13-11-2026',
  PRIVATISED: 'CHURCH STREET, HAWTHORN',
  geometry: { type: 'Polygon', coordinates: [HAWTHORN_RING] },
  centre: [145.020933057992, -37.8155145598478],
};

const CITIPOWER_2 = {
  ...CITIPOWER,
  ORDER_ID: '2413353-1',
  AREA: 'PORT PHILLIP',
  TOWN: 'MIDDLE PARK',
  POSTCODE: '3206',
  START_TIME: '10:29 13-11-2026',
  PRIVATISED: 'G/L,  BEACONSFIELD PARADE, MIDDLE PARK',
  centre: [144.962767771347, -37.8544276723046],
};

/** Since yesterday, its estimate long past, some customers back on. */
const POWERCOR = {
  ORDER_ID: 'F-15712-i',
  BUSINESS: 'Powercor',
  AREA: 'COHUNA',
  TOWN: 'LAKE CHARM',
  POSTCODE: '3581',
  CUSTOMERS: '2',
  ETR: '14:00 12-11-2026',
  CREW_STATUS: 'Partially Restored',
  CAUSE: 'Under investigation',
  START_TIME: '12:34 12-11-2026',
  PRIVATISED: 'MURRAY VALLEY HIGHWAY, LAKE CHARM',
  geometry: {
    type: 'Polygon',
    coordinates: [
      [
        [143.8474938326649, -35.6573478239383],
        [143.848047245564, -35.6568981623635],
        [143.847493832665, -35.6564485033204],
        [143.8469404197659, -35.6568981623635],
        [143.8474938326649, -35.6573478239383],
      ],
    ],
  },
  centre: [143.847493832665, -35.6568981636462],
};

const POWERCOR_2 = {
  ...POWERCOR,
  ORDER_ID: '2413535-1',
  AREA: 'BALLAN',
  TOWN: 'BALLAN',
  POSTCODE: '3342',
  CUSTOMERS: '1',
  ETR: '21:00 13-11-2026',
  CREW_STATUS: 'Crews Attending',
  START_TIME: '13:49 13-11-2026',
  PRIVATISED: 'HADDON DRIVE, BALLAN',
  centre: [144.237655495865, -37.6145158316678],
};

const file = (ROW?: unknown) => ({ ROWSET: { createDateTime: '2026-11-13T19:41:23', ...(ROW === undefined ? {} : { ROW }) } });
const FILE_URL = 'https://s3-ap-southeast-2.amazonaws.com/cppc-outage/outages.json';
const NOW = Date.parse('2026-11-13T08:45:00Z');

describe('parseCppc', () => {
  it('a CitiPower outage: every field', () => {
    expect(parseCppc(file([CITIPOWER]))).toEqual([
      {
        id: '2413608-1',
        source: 'citipower',
        distributor: 'citipower',
        state: 'VIC',
        kind: 'unplanned',
        area: 'Hawthorn',
        customers_affected: 1,
        cause: 'Under investigation',
        // 15:53 in Melbourne, in daylight saving (UTC+11).
        start_at: '2026-11-13T04:53:00.000Z',
        restore_at: '2026-11-13T10:00:00.000Z',
        // The file says when it was written, not when an outage changed.
        updated_at: null,
        lat: -37.8155145598478,
        lon: 145.020933057992,
        // The feed's 50 m circle around the centre is not passed on as an outline.
        polygons: [],
        details: { crew_status: 'Outage reported', streets: 'Church Street, Hawthorn', postcode: '3122', region: 'Boroondara' },
      },
    ]);
  });

  it("a Powercor outage is Powercor's, and a partly restored one is still an outage", () => {
    const [o] = parseCppc(file([POWERCOR]));
    expect(o).toMatchObject({
      id: 'F-15712-i',
      source: 'powercor',
      distributor: 'powercor',
      state: 'VIC',
      kind: 'unplanned',
      area: 'Lake Charm',
      customers_affected: 2,
      start_at: '2026-11-12T01:34:00.000Z',
      restore_at: '2026-11-12T03:00:00.000Z',
      lat: -35.6568981636462,
      lon: 143.847493832665,
      details: { crew_status: 'Partially restored', streets: 'Murray Valley Highway, Lake Charm', postcode: '3581', region: 'Cohuna' },
    });
    expect(o!.polygons).toEqual([]);
    expect(o).not.toHaveProperty('status');
  });

  it('tidies the street text', () => {
    expect(parseCppc(file([CITIPOWER_2]))[0]!.details['streets']).toBe('G/L, Beaconsfield Parade, Middle Park');
  });

  it('nothing about the read goes into a record', () => {
    const a = parseCppc(file([CITIPOWER, POWERCOR]));
    const later = { ROWSET: { createDateTime: '2026-11-13T19:44:23', ROW: [CITIPOWER, POWERCOR] } };
    expect(parseCppc(later)).toEqual(a);
  });

  it('takes ROW as a list, as one row, or missing', () => {
    expect(parseCppc(file([CITIPOWER, POWERCOR])).map((o) => o.id)).toEqual(['2413608-1', 'F-15712-i']);
    expect(parseCppc(file(POWERCOR)).map((o) => o.id)).toEqual(['F-15712-i']);
    expect(parseCppc(file())).toEqual([]);
    expect(parseCppc(file(null))).toEqual([]);
    expect(parseCppc(file(''))).toEqual([]);
    expect(parseCppc(file([]))).toEqual([]);
  });

  it('drops a row that is over, has no id, or is on neither network', () => {
    expect(parseCppc(file([{ ...CITIPOWER, CREW_STATUS: 'Restored' }, { ...POWERCOR, CREW_STATUS: 'Completed' }, { ...POWERCOR, CREW_STATUS: 'Cancelled' }]))).toEqual([]);
    expect(parseCppc(file([{ ...CITIPOWER, ORDER_ID: '' }, { ...CITIPOWER, BUSINESS: 'United Energy' }, null, 'x']))).toEqual([]);
  });

  it('leaves out what a row does not give', () => {
    const [o] = parseCppc(file([{ ORDER_ID: 7, BUSINESS: 'POWERCOR' }]));
    expect(o).toMatchObject({
      id: '7', distributor: 'powercor', area: '', customers_affected: 0, cause: '', start_at: null, restore_at: null, lat: null, lon: null, polygons: [], details: {},
    });
  });

  it('throws on an answer of the wrong shape, so the last good snapshot stays', () => {
    for (const bad of [null, 42, '<html>', [], [CITIPOWER], {}, { ROWSET: null }, { ROWSET: [CITIPOWER] }, { ROW: [CITIPOWER] }, file('nope'), file(3)]) {
      expect(() => parseCppc(bad), JSON.stringify(bad)).toThrow('cppc');
    }
  });
});

describe('fetchCppc', () => {
  beforeEach(() => {
    fetchJson.mockReset();
    _resetCppcShare();
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  it('each distributor gets only its own rows, from one download', async () => {
    fetchJson.mockResolvedValue(file([CITIPOWER, POWERCOR, CITIPOWER_2, POWERCOR_2]));
    const citipower = await fetchCppc('citipower');
    vi.setSystemTime(NOW + 2_000);
    const powercor = await fetchCppc('powercor');
    expect(citipower.map((o) => [o.source, o.id])).toEqual([['citipower', '2413608-1'], ['citipower', '2413353-1']]);
    expect(powercor.map((o) => [o.source, o.id])).toEqual([['powercor', 'F-15712-i'], ['powercor', '2413535-1']]);
    expect(fetchJson.mock.calls).toEqual([[FILE_URL, { timeoutMs: 20_000 }]]);
  });

  it('two sources asking at once wait for the same request', async () => {
    fetchJson.mockResolvedValue(file([CITIPOWER, POWERCOR]));
    const [a, b] = await Promise.all([fetchCppc('citipower'), fetchCppc('powercor')]);
    expect([a.length, b.length]).toEqual([1, 1]);
    expect(fetchJson).toHaveBeenCalledTimes(1);
  });

  it('downloads again once the minute is up', async () => {
    fetchJson.mockResolvedValue(file([CITIPOWER]));
    await fetchCppc('citipower');
    vi.setSystemTime(NOW + 59_000);
    await fetchCppc('powercor');
    expect(fetchJson).toHaveBeenCalledTimes(1);

    fetchJson.mockResolvedValue(file([CITIPOWER, CITIPOWER_2]));
    vi.setSystemTime(NOW + 60_000);
    expect(await fetchCppc('citipower')).toHaveLength(2);
    expect(fetchJson).toHaveBeenCalledTimes(2);
  });

  it('a failed download fails both sources once, and the next poll asks again', async () => {
    fetchJson.mockRejectedValueOnce(new Error('HTTP 503'));
    await expect(fetchCppc('citipower')).rejects.toThrow('HTTP 503');
    await expect(fetchCppc('powercor')).rejects.toThrow('HTTP 503');
    expect(fetchJson).toHaveBeenCalledTimes(1);

    vi.setSystemTime(NOW + 180_000);
    fetchJson.mockResolvedValueOnce(file([POWERCOR]));
    expect(await fetchCppc('powercor')).toHaveLength(1);
    expect(fetchJson).toHaveBeenCalledTimes(2);
  });

  it('throws for both when the file is the wrong shape', async () => {
    fetchJson.mockResolvedValue({ Error: 'AccessDenied' });
    await expect(fetchCppc('citipower')).rejects.toThrow('cppc');
    await expect(fetchCppc('powercor')).rejects.toThrow('cppc');
  });

  it('no outages on a network is an empty list, not a failure', async () => {
    fetchJson.mockResolvedValue(file([POWERCOR]));
    expect(await fetchCppc('citipower')).toEqual([]);
  });
});

describe('register()', () => {
  it('registers the two distributors, each every three minutes, each fetching its own', async () => {
    const registry = await import('../../../../src/services/sourceRegistry.js');
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    register();
    expect(spy.mock.calls.map((c) => [c[0].name, c[0].family, c[0].intervalMs])).toEqual([
      ['citipower', 'power', 180_000],
      ['powercor', 'power', 180_000],
    ]);
    fetchJson.mockReset();
    _resetCppcShare();
    fetchJson.mockResolvedValue(file([CITIPOWER, POWERCOR]));
    const stored = (await Promise.all(spy.mock.calls.map((c) => c[0].fetch()))) as Array<Array<{ id: string; status: string }>>;
    expect(stored.map((s) => s.map((o) => [o.id, o.status]))).toEqual([[['2413608-1', 'active']], [['F-15712-i', 'active']]]);
    expect(fetchJson).toHaveBeenCalledTimes(1);
    spy.mockRestore();
  });
});
