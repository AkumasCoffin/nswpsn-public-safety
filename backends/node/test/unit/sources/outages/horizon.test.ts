/**
 * Horizon Power outages (sources/outages/horizon.ts).
 *
 * The entries below are real ones from Horizon's list (13 November 2026,
 * imported 16:40 Perth time). The fetch tests mock the shared HTTP module so
 * the upstream URL never gets hit.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// vi.hoisted: see ../endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import { fetchHorizon, parseHorizon } from '../../../../src/sources/outages/horizon.js';
import { withStatus } from '../../../../src/sources/outages/common.js';

const IMPORTED = '2026-11-13T16:40:00.443';
const blank = { depotName: '', depotContactNumber: '', importedDateTime: IMPORTED, affected: false, endDate: null };

const PLANNED_STARTED = {
  ...blank,
  entityId: 'accfdb4c-015f-469f-bf75-75bd11d9d0c5',
  outageSK: 2696,
  outageType: 'Planned',
  status: 'Started',
  externalId: 'INCD-229775-a',
  startDateTime: '2026-11-13T11:00:00',
  estimatedFixDateTime: '2026-11-13T12:30:00',
  region: 'Mid West',
  serviceArea: 'Sandstone',
  createdDate: '2026-10-28T19:51:37',
  updatedDateTime: '2026-11-13T00:56:18',
  customersAffected: '1',
  centerLatitude: -27.98,
  centerLongitude: 119.29,
  radius: 100,
};

const PLANNED_SCHEDULED = {
  ...blank,
  entityId: '3bc72b48-5630-4eb7-8f01-1e4a6bc7805a',
  outageSK: 2732,
  outageType: 'Planned',
  status: 'Scheduled',
  externalId: 'INCD-230245-a',
  startDateTime: '2026-11-20T09:00:00',
  estimatedFixDateTime: '2026-11-20T15:00:00',
  region: 'Esperance',
  serviceArea: 'Hopetoun',
  createdDate: '2026-11-11T09:56:34',
  updatedDateTime: '2026-11-12T10:36:16',
  customersAffected: '1',
  centerLatitude: -33.77,
  centerLongitude: 120.38,
  radius: 100,
};

const UNPLANNED_STARTED = {
  ...blank,
  entityId: '163b12ac-10a0-4da7-928e-cc7f00ba36e2',
  outageSK: 2735,
  outageType: 'Unplanned',
  status: 'Started',
  externalId: 'INCD-275250-U',
  startDateTime: '2026-11-13T09:32:44',
  estimatedFixDateTime: '2026-11-13T17:30:00',
  region: 'East Pilbara',
  serviceArea: 'Port Hedland',
  createdDate: '2026-11-13T13:20:58',
  updatedDateTime: '2026-11-13T13:25:53',
  customersAffected: '17',
  centerLatitude: -20.3,
  centerLongitude: 118.63,
  radius: 100,
};

/**
 * "Unplanned" and "Scheduled": work entered nine minutes before it began, for
 * a two-hour window on the hour. One incident, two patches around Dampier.
 */
const DAMPIER = {
  outageType: 'Unplanned',
  status: 'Scheduled',
  externalId: 'INCD-222961-W',
  startDateTime: '2026-11-11T14:00:00',
  estimatedFixDateTime: '2026-11-11T16:00:00',
  region: 'West Pilbara',
  serviceArea: 'Dampier',
  createdDate: '2026-11-11T13:51:40',
  updatedDateTime: '2026-11-11T13:56:28',
};
const DAMPIER_A = { ...blank, ...DAMPIER, entityId: '8a3c058e-2af9-41cc-8448-781e25f6f7c4', outageSK: 2723, customersAffected: '22', centerLatitude: -20.74, centerLongitude: 116.76, radius: 30087 };
const DAMPIER_B = { ...blank, ...DAMPIER, entityId: '136d99fc-5ea3-414c-8fa0-7cd0a2a85826', outageSK: 2722, customersAffected: '29', centerLatitude: -20.62, centerLongitude: 116.76, radius: 5235 };

/** One planned job in three patches of Carnarvon; two of them have six customers each. */
const CARNARVON = {
  outageType: 'Planned',
  status: 'Scheduled',
  externalId: 'INCD-229895-a',
  startDateTime: '2026-11-17T08:00:00',
  estimatedFixDateTime: '2026-11-17T12:00:00',
  region: 'Gascoyne',
  serviceArea: 'Carnarvon',
  createdDate: '2026-11-03T13:56:29',
  updatedDateTime: '2026-11-10T13:28:54',
};
const CARNARVON_A = { ...blank, ...CARNARVON, entityId: '903b5844-ba0f-43ff-a00a-537e3261f2c9', outageSK: 2708, customersAffected: '6', centerLatitude: -24.82, centerLongitude: 113.76, radius: 318 };
const CARNARVON_B = { ...blank, ...CARNARVON, entityId: 'f92e54d9-2971-42f3-a0ef-98a75e254275', outageSK: 2709, customersAffected: '4', centerLatitude: -24.82, centerLongitude: 113.75, radius: 221 };
const CARNARVON_C = { ...blank, ...CARNARVON, entityId: '73e3e832-2b7d-4f14-b902-d7f00912f37d', outageSK: 2707, customersAffected: '6', centerLatitude: -24.83, centerLongitude: 113.75, radius: 4420 };

const answer = (outages: unknown, extra: Record<string, unknown> = {}) => ({
  version: null,
  statusCode: 200,
  message: null,
  isError: null,
  responseException: null,
  result: { outages },
  importedDateTime: IMPORTED,
  ...extra,
});

/** 16:40 in Perth. */
const NOW = Date.parse('2026-11-13T08:40:00Z');
const parse = (...outages: unknown[]) => parseHorizon(answer(outages), NOW);

describe('parseHorizon', () => {
  it('planned work under way: every field, Perth times as UTC', () => {
    expect(parse(PLANNED_STARTED)).toEqual([
      {
        id: 'INCD-229775-a',
        source: 'horizon',
        distributor: 'horizon',
        state: 'WA',
        kind: 'planned',
        area: 'Sandstone',
        customers_affected: 1,
        cause: '',
        // Perth is UTC+8 all year.
        start_at: '2026-11-13T03:00:00.000Z',
        restore_at: '2026-11-13T04:30:00.000Z',
        // 00:56 in Perth is the day before in UTC.
        updated_at: '2026-11-12T16:56:18.000Z',
        lat: -27.98,
        lon: 119.29,
        polygons: [],
        details: { region: 'Mid West', status: 'Started', radius_m: 100, created_at: '2026-10-28T11:51:37.000Z' },
      },
    ]);
  });

  it('an unplanned outage: customers as a number, times to the second', () => {
    expect(parse(UNPLANNED_STARTED)[0]).toMatchObject({
      id: 'INCD-275250-U',
      kind: 'unplanned',
      area: 'Port Hedland',
      customers_affected: 17,
      start_at: '2026-11-13T01:32:44.000Z',
      restore_at: '2026-11-13T09:30:00.000Z',
      updated_at: '2026-11-13T05:25:53.000Z',
      lat: -20.3,
      lon: 118.63,
      details: { region: 'East Pilbara', status: 'Started', radius_m: 100, created_at: '2026-11-13T05:20:58.000Z' },
    });
  });

  it('reads Perth time the same in winter: no daylight saving', () => {
    const [o] = parse({ ...UNPLANNED_STARTED, startDateTime: '2026-08-27T18:49:33', estimatedFixDateTime: '2026-08-28T02:00:00' });
    expect(o).toMatchObject({ start_at: '2026-08-27T10:49:33.000Z', restore_at: '2026-08-27T18:00:00.000Z' });
  });

  it('takes the kind from outageType and gives no status of its own', () => {
    const out = parse(PLANNED_STARTED, PLANNED_SCHEDULED, UNPLANNED_STARTED, DAMPIER_A);
    expect(out.map((o) => [o.id, o.kind, o.details['status']])).toEqual([
      ['INCD-229775-a', 'planned', 'Started'],
      ['INCD-230245-a', 'planned', 'Scheduled'],
      ['INCD-275250-U', 'unplanned', 'Started'],
      ['INCD-222961-W', 'unplanned', 'Scheduled'],
    ]);
    expect(out.every((o) => !('status' in o))).toBe(true);
    expect(out.map((o) => withStatus(o, NOW).status)).toEqual(['active', 'scheduled', 'active', 'active']);
  });

  it("passes on Horizon's Scheduled only for planned work with no readable start", () => {
    const [noStart, started, unplanned] = parse(
      { ...PLANNED_SCHEDULED, startDateTime: null },
      { ...PLANNED_STARTED, startDateTime: 'soon' },
      { ...DAMPIER_A, startDateTime: null },
    );
    expect(noStart).toMatchObject({ start_at: null, status: 'scheduled' });
    expect('status' in started!).toBe(false);
    expect('status' in unplanned!).toBe(false);
  });

  it('joins the patches of one incident: customers added up, placed at the largest patch', () => {
    const out = parse(DAMPIER_A, PLANNED_STARTED, DAMPIER_B);
    expect(out.map((o) => o.id)).toEqual(['INCD-222961-W', 'INCD-229775-a']);
    expect(out[0]).toMatchObject({
      kind: 'unplanned',
      area: 'Dampier',
      customers_affected: 51,
      start_at: '2026-11-11T06:00:00.000Z',
      restore_at: '2026-11-11T08:00:00.000Z',
      updated_at: '2026-11-11T05:56:28.000Z',
      lat: -20.62,
      lon: 116.76,
      details: { region: 'West Pilbara', status: 'Scheduled', radius_m: 5235, created_at: '2026-11-11T05:51:40.000Z', patches: 2 },
    });
    // One patch on its own is not counted.
    expect('patches' in out[1]!.details).toBe(false);
  });

  it('is the same record whatever order the patches come in', () => {
    const [a] = parse(CARNARVON_A, CARNARVON_B, CARNARVON_C);
    const [b] = parse(CARNARVON_C, CARNARVON_B, CARNARVON_A);
    expect(a).toEqual(b);
    // Two patches have six customers; the one Horizon numbered first places it.
    expect(a).toMatchObject({ customers_affected: 16, lat: -24.83, lon: 113.75, area: 'Carnarvon', details: { radius_m: 4420, patches: 3 } });
  });

  it('spans the patches when their times or areas differ', () => {
    const [o] = parse(
      { ...DAMPIER_A, serviceArea: 'KARRATHA', startDateTime: '2026-11-11T13:00:00', updatedDateTime: '2026-11-11T15:00:00' },
      { ...DAMPIER_B, estimatedFixDateTime: '2026-11-11T18:00:00' },
    );
    expect(o).toMatchObject({
      area: 'Karratha, Dampier',
      start_at: '2026-11-11T05:00:00.000Z',
      restore_at: '2026-11-11T10:00:00.000Z',
      updated_at: '2026-11-11T07:00:00.000Z',
    });
  });

  it('falls back to entityId when there is no externalId', () => {
    const out = parse({ ...PLANNED_STARTED, externalId: '' }, { ...UNPLANNED_STARTED, externalId: null });
    expect(out.map((o) => o.id)).toEqual(['accfdb4c-015f-469f-bf75-75bd11d9d0c5', '163b12ac-10a0-4da7-928e-cc7f00ba36e2']);
  });

  it('drops an entry whose endDate has passed, and keeps one whose end is still to come', () => {
    expect(parse({ ...UNPLANNED_STARTED, endDate: '2026-11-13T16:39:00' })).toEqual([]);
    const [o] = parse({ ...UNPLANNED_STARTED, endDate: '2026-11-13T16:41:00' });
    expect(o!.id).toBe('INCD-275250-U');
    // The estimate still speaks for the restore time; the end only stands in for a missing one.
    expect(o!.restore_at).toBe('2026-11-13T09:30:00.000Z');
    expect(parse({ ...UNPLANNED_STARTED, endDate: '2026-11-13T18:00:00', estimatedFixDateTime: null })[0]!.restore_at).toBe('2026-11-13T10:00:00.000Z');
  });

  it('drops an entry whose status says it is over, in any case', () => {
    for (const status of ['Completed', 'CANCELLED', 'Canceled', 'Restored', 'finished', 'Resolved', 'Closed']) {
      expect(parse({ ...PLANNED_STARTED, status }), status).toEqual([]);
    }
    // One finished patch leaves the rest of its incident standing.
    expect(parse(DAMPIER_A, { ...DAMPIER_B, status: 'Completed' })[0]).toMatchObject({ customers_affected: 22, lat: -20.74 });
  });

  it('keeps nothing that moves with every import', () => {
    const first = parseHorizon(answer([PLANNED_STARTED, DAMPIER_A, DAMPIER_B]), NOW);
    const later = '2026-11-13T16:45:00.512';
    const second = parseHorizon(
      answer(
        [PLANNED_STARTED, DAMPIER_A, DAMPIER_B].map((o) => ({ ...o, importedDateTime: later })),
        { importedDateTime: later },
      ),
      NOW + 5 * 60_000,
    );
    expect(second).toEqual(first);
    // 16:40:00.443 Perth, in either spelling.
    expect(JSON.stringify(first)).not.toMatch(/16:40:00|08:40:00/);
  });

  it('names a depot when Horizon gives one, and manages without what is missing', () => {
    expect(parse({ ...PLANNED_STARTED, depotName: 'Carnarvon Depot', depotContactNumber: '08 9941 0000' })[0]!.details).toMatchObject({
      depot: 'Carnarvon Depot',
      depot_phone: '08 9941 0000',
    });
    expect(parse({ entityId: 'e-1', outageType: 'Unplanned', customersAffected: 'some' })).toEqual([
      expect.objectContaining({
        id: 'e-1',
        kind: 'unplanned',
        area: '',
        customers_affected: 0,
        start_at: null,
        restore_at: null,
        updated_at: null,
        lat: null,
        lon: null,
        details: {},
      }),
    ]);
  });

  it('an empty list is no outages', () => {
    expect(parseHorizon(answer([]), NOW)).toEqual([]);
  });

  it('throws when Horizon reports an error', () => {
    expect(() => parseHorizon(answer([PLANNED_STARTED], { isError: true, message: 'Database unavailable' }), NOW)).toThrow('Database unavailable');
    expect(() => parseHorizon({ statusCode: 500, isError: true, result: null }, NOW)).toThrow('reported an error');
    // false and null both mean no error.
    expect(parseHorizon(answer([PLANNED_STARTED], { isError: false }), NOW)).toHaveLength(1);
  });

  it('throws on a wrong-shaped answer', () => {
    for (const bad of [null, undefined, 42, '<html>', [PLANNED_STARTED]]) {
      expect(() => parseHorizon(bad, NOW), JSON.stringify(bad)).toThrow('not an object');
    }
    for (const bad of [{}, { result: null }, { result: {} }, { result: { outages: null } }, { result: { outages: { a: 1 } } }, { result: [PLANNED_STARTED] }]) {
      expect(() => parseHorizon(bad, NOW), JSON.stringify(bad)).toThrow('not an array');
    }
    expect(() => parseHorizon(answer([{ id: 1 }, null]), NOW)).toThrow('could be read');
  });

  it('a list of outages that are all over is no outages, not a wrong shape', () => {
    expect(parse({ ...PLANNED_STARTED, status: 'Completed' }, { ...UNPLANNED_STARTED, endDate: '2026-11-13T12:00:00' })).toEqual([]);
  });
});

describe('fetchHorizon', () => {
  beforeEach(() => {
    fetchJson.mockReset();
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  it('reads the one endpoint, with twenty seconds to answer, and drops by the clock', async () => {
    fetchJson.mockResolvedValueOnce(answer([PLANNED_STARTED, { ...UNPLANNED_STARTED, endDate: '2026-11-13T16:00:00' }, PLANNED_SCHEDULED]));
    const out = await fetchHorizon();
    expect(out.map((o) => o.id)).toEqual(['INCD-229775-a', 'INCD-230245-a']);
    expect(fetchJson.mock.calls).toEqual([['https://www.horizonpower.com.au/api/outage/getdetailedoutages', { timeoutMs: 20_000 }]]);
  });

  it('throws on an error answer and on a failed request, so the last good snapshot stays', async () => {
    fetchJson.mockResolvedValueOnce(answer(null, { isError: true }));
    await expect(fetchHorizon()).rejects.toThrow('reported an error');
    fetchJson.mockResolvedValueOnce({ statusCode: 200, isError: null, result: null });
    await expect(fetchHorizon()).rejects.toThrow('not an array');
    fetchJson.mockRejectedValueOnce(new Error('HTTP 503'));
    await expect(fetchHorizon()).rejects.toThrow('HTTP 503');
  });
});

describe('register()', () => {
  afterEach(() => vi.resetModules());

  it('polls every three minutes under the name horizon, once switched on (it is off by default)', async () => {
    process.env['HORIZON_DISABLED'] = 'false';
    vi.resetModules();
    const registry = await import('../../../../src/services/sourceRegistry.js');
    const { default: register } = await import('../../../../src/sources/outages/horizon.js');
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    register();
    expect(spy.mock.calls.map((c) => [c[0].name, c[0].family, c[0].intervalMs])).toEqual([['horizon', 'power', 180_000]]);
    spy.mockRestore();
    delete process.env['HORIZON_DISABLED'];
  });
});
