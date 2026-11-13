/**
 * SA Power Networks outage tests (sources/outages/sapn.ts).
 *
 * The outages are real records from SAPN's two endpoints (13 November 2026),
 * with outlines and feeder lists cut short. The fetch tests mock the shared
 * HTTP module, so SAPN is never asked.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// vi.hoisted: see ../endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import register, {
  SAPN_CURRENT_URL,
  SAPN_PLANNED_URL,
  fetchSapn,
  parseSapnCurrent,
  parseSapnPlanned,
} from '../../../../src/sources/outages/sapn.js';
import { _resetKeptParts, withStatus } from '../../../../src/sources/outages/common.js';
import * as registry from '../../../../src/services/sourceRegistry.js';

/** 12:30 in Adelaide on 13 November: the morning's planned work is under way. */
const NOW = Date.parse('2026-11-13T02:00:00Z');

const CURRENT = {
  jobID: '160110617',
  isPaw: false,
  startDateTime: '2026-11-13T17:04:54',
  estimatedRestoration: '2026-11-13T19:15:00',
  reason: 'To be investigated',
  status: 'Our crews are working to restore power',
  affectedCustomers: 46,
  affectedSuburbs: [
    { name: 'Good Hope Landing', postcode: '5330' },
    { name: 'Wigley Flat', postcode: '5330' },
    { name: 'Woolpunda', postcode: '5330' },
  ],
  geometry: [
    { lat: -34.2, lng: 140.2 },
    { lat: -34.18, lng: 140.14 },
    { lat: -34.16, lng: 140.18 },
    { lat: -34.18, lng: 140.22 },
  ],
};

const CURRENT_2 = {
  jobID: '160110636',
  isPaw: false,
  startDateTime: '2026-11-13T17:49:44',
  estimatedRestoration: '2026-11-14T00:00:00',
  reason: 'To be investigated',
  status: 'Our crews are travelling to this area',
  affectedCustomers: 352,
  affectedSuburbs: [
    { name: 'Blackfellows Caves', postcode: '5291' },
    { name: 'Carpenter Rocks', postcode: '5291' },
    { name: 'German Creek', postcode: '5280' },
  ],
  geometry: [
    { lat: -37.9298739967958, lng: 140.421512382575 },
    { lat: -37.9163319586064, lng: 140.399696233738 },
    { lat: -37.8385027294778, lng: 140.474413210916 },
  ],
};

/** Started at 07:00 today, ends at 17:00. */
const PLANNED_TODAY = {
  startDateTime: '2026-11-13T07:00:00',
  endDateTime: '2026-11-13T17:00:00',
  reason: 'Electricity retailer requested work',
  affectedCustomers: 14,
  affectedSuburbs: [{ name: 'Craigmore', postcode: '5114' }],
  geometry: [
    { lat: -34.7091562480462, lng: 138.705791151717 },
    { lat: -34.7090663488958, lng: 138.705776479378 },
    { lat: -34.7086974217781, lng: 138.705728339257 },
    { lat: -34.7087319575293, lng: 138.705784457547 },
  ],
  jobID: '186408',
  status: 'Scheduled',
  feeders: ['EL10 20023'],
};

/** Two localities here have no postcode. */
const PLANNED_AHEAD = {
  startDateTime: '2026-11-19T08:30:00',
  endDateTime: '2026-11-19T15:30:00',
  reason: 'Maintaining and upgrading the electricity network',
  affectedCustomers: 86,
  affectedSuburbs: [
    { name: 'Cradock', postcode: '5432' },
    { name: 'Hawker', postcode: '5434' },
    { name: 'Shaggy Ridge', postcode: null },
    { name: 'Willippa', postcode: '5432' },
    { name: 'Worumba', postcode: null },
  ],
  geometry: [
    { lat: -32.099641605493, lng: 138.533178484719 },
    { lat: -32.09382116242844, lng: 138.51987248458212 },
    { lat: -32.0858723597831, lng: 138.50373931649565 },
  ],
  jobID: '182102',
  status: 'Scheduled',
  feeders: ['HK05 3', 'HK04 11', 'HK04 10'],
};

/** One job, two interruptions on the same day. */
const PLANNED_TWICE_AM = {
  startDateTime: '2026-11-20T08:30:00',
  endDateTime: '2026-11-20T10:00:00',
  reason: 'Accessing electrical equipment (under 15 mins)',
  affectedCustomers: 232,
  affectedSuburbs: [
    { name: 'Basket Range', postcode: '5138' },
    { name: 'Uraidla', postcode: '5142' },
  ],
  geometry: [
    { lat: -34.9595754101556, lng: 138.753155538031 },
    { lat: -34.9592614729498, lng: 138.75245132572 },
    { lat: -34.9583062161532, lng: 138.752071015712 },
  ],
  jobID: '188412',
  status: 'Scheduled',
  feeders: ['SG04 18149', 'SG04 10'],
};
const PLANNED_TWICE_PM = { ...PLANNED_TWICE_AM, startDateTime: '2026-11-20T13:30:00', endDateTime: '2026-11-20T15:00:00' };

const current = (...currentOutages: unknown[]) => ({ currentOutages, searchCriteria: { isEmpty: true }, lastUpdated: '2026-11-13T19:12:01' });
const planned = (...plannedOutages: unknown[]) => ({ plannedOutages, lastUpdated: '2026-11-13T19:12:01', searchCriteria: { isEmpty: true } });

describe('parseSapnCurrent', () => {
  it('an unplanned outage: the outline as [longitude, latitude], and its middle as the position', () => {
    expect(parseSapnCurrent(current(CURRENT))).toEqual([
      {
        id: '160110617',
        source: 'sapn',
        distributor: 'sapn',
        state: 'SA',
        kind: 'unplanned',
        area: 'Good Hope Landing, Wigley Flat, Woolpunda',
        customers_affected: 46,
        cause: 'To be investigated',
        // Adelaide in November is UTC+10:30.
        start_at: '2026-11-13T06:34:54.000Z',
        restore_at: '2026-11-13T08:45:00.000Z',
        updated_at: null,
        lat: expect.closeTo(-34.18, 9),
        lon: expect.closeTo(140.185, 9),
        polygons: [[[140.2, -34.2], [140.14, -34.18], [140.18, -34.16], [140.22, -34.18]]],
        details: { status: 'Our crews are working to restore power', postcodes: ['5330'], is_paw: false },
      },
    ]);
  });

  it('Adelaide time in winter is UTC+9:30', () => {
    const [o] = parseSapnCurrent(current({ ...CURRENT, startDateTime: '2026-07-15T17:04:54', estimatedRestoration: '2026-07-15T19:15:00' }));
    expect([o!.start_at, o!.restore_at]).toEqual(['2026-07-15T07:34:54.000Z', '2026-07-15T09:45:00.000Z']);
  });

  it("keeps SAPN's sentence and its flag in details, as given, and each postcode once", () => {
    const [, second] = parseSapnCurrent(current(CURRENT, { ...CURRENT_2, isPaw: true }));
    expect(second).toMatchObject({
      id: '160110636',
      area: 'Blackfellows Caves, Carpenter Rocks, German Creek',
      customers_affected: 352,
      restore_at: '2026-11-13T13:30:00.000Z',
      details: { status: 'Our crews are travelling to this area', postcodes: ['5291', '5280'], is_paw: true },
    });
    expect(second).not.toHaveProperty('status');
  });

  it('leaves the answer time off the records', () => {
    const first = parseSapnCurrent(current(CURRENT, CURRENT_2));
    expect(parseSapnCurrent({ ...current(CURRENT, CURRENT_2), lastUpdated: '2026-11-13T19:15:01' })).toEqual(first);
    expect(JSON.stringify(first)).not.toContain('19:12');
    expect(first.every((o) => o.updated_at === null)).toBe(true);
  });

  it('drops an outage that is restored, cancelled or completed', () => {
    const over = ['Power has been restored', 'Cancelled', 'Completed'].map((status, i) => ({ ...CURRENT, jobID: `over-${i}`, status }));
    expect(parseSapnCurrent(current(...over, CURRENT_2)).map((o) => o.id)).toEqual(['160110636']);
  });

  it('does without what is missing: no outline is no position', () => {
    const [o] = parseSapnCurrent(current({ jobID: 'X1', geometry: [{ lat: -34.9, lng: 138.6 }] }, null, 'x'));
    expect(o).toMatchObject({ id: 'X1', area: '', customers_affected: 0, cause: '', start_at: null, restore_at: null, lat: null, lon: null, polygons: [], details: {} });
    expect(parseSapnCurrent(current())).toEqual([]);
  });

  it('several areas, should SAPN ever give them, are several outlines', () => {
    const a = [{ lat: -34, lng: 138 }, { lat: -34, lng: 138.2 }, { lat: -34.2, lng: 138.2 }];
    const b = [{ lat: -35, lng: 139 }, { lat: -35, lng: 139.2 }, { lat: -35.2, lng: 139.2 }];
    const [o] = parseSapnCurrent(current({ ...CURRENT, geometry: [a, b] }));
    expect(o!.polygons).toEqual([
      [[138, -34], [138.2, -34], [138.2, -34.2]],
      [[139, -35], [139.2, -35], [139.2, -35.2]],
    ]);
    expect(o!.lat).toBeCloseTo(-34.5666667, 6);
    expect(o!.lon).toBeCloseTo(138.6333333, 6);
  });

  it('throws when the answer is not the current list', () => {
    for (const bad of [null, 42, '<html>', [], {}, { currentOutages: null }, planned(PLANNED_AHEAD)]) {
      expect(() => parseSapnCurrent(bad), JSON.stringify(bad)).toThrow('sapn: currentOutages is not a list');
    }
  });
});

describe('parseSapnPlanned', () => {
  it('planned work: its end as the restore time, no status of its own, feeders left out', () => {
    const out = parseSapnPlanned(planned(PLANNED_TODAY, PLANNED_AHEAD), NOW);
    expect(out[0]).toEqual({
      id: '186408',
      source: 'sapn',
      distributor: 'sapn',
      state: 'SA',
      kind: 'planned',
      area: 'Craigmore',
      customers_affected: 14,
      cause: 'Electricity retailer requested work',
      start_at: '2026-11-12T20:30:00.000Z',
      restore_at: '2026-11-13T06:30:00.000Z',
      updated_at: null,
      lat: expect.closeTo(-34.70891299406235, 9),
      lon: expect.closeTo(138.70577010697474, 9),
      polygons: [
        [
          [138.705791151717, -34.7091562480462],
          [138.705776479378, -34.7090663488958],
          [138.705728339257, -34.7086974217781],
          [138.705784457547, -34.7087319575293],
        ],
      ],
      details: { status: 'Scheduled', postcodes: ['5114'] },
    });
    expect(out[1]).toMatchObject({
      id: '182102',
      area: 'Cradock, Hawker, Shaggy Ridge, Willippa, Worumba',
      start_at: '2026-11-18T22:00:00.000Z',
      restore_at: '2026-11-19T05:00:00.000Z',
      details: { status: 'Scheduled', postcodes: ['5432', '5434'] },
    });
    // SAPN says "Scheduled" of both; the clock says one has started.
    expect(out.map((o) => withStatus(o, NOW).status)).toEqual(['active', 'scheduled']);
  });

  it('drops work whose end has passed, and anything cancelled or completed', () => {
    const list = planned(PLANNED_TODAY, PLANNED_AHEAD, { ...PLANNED_AHEAD, jobID: 'C1', status: 'Cancelled' }, { ...PLANNED_AHEAD, jobID: 'C2', status: 'Completed' });
    const ids = (nowMs: number) => parseSapnPlanned(list, nowMs).map((o) => o.id);
    expect(ids(NOW)).toEqual(['186408', '182102']);
    // 17:00 in Adelaide is 06:30 UTC: gone the moment it ends.
    expect(ids(Date.parse('2026-11-13T06:29:59Z'))).toEqual(['186408', '182102']);
    expect(ids(Date.parse('2026-11-13T06:30:00Z'))).toEqual(['182102']);
    // Work with no readable end is not taken as over.
    expect(parseSapnPlanned(planned({ ...PLANNED_TODAY, endDateTime: null }), NOW + 86_400_000 * 30)).toHaveLength(1);
  });

  it('a job listed more than once: each interruption has its own id, and the job number in details', () => {
    const lateNight = { ...PLANNED_TWICE_AM, endDateTime: '2026-11-20T09:00:00' };
    const out = parseSapnPlanned(planned(PLANNED_TWICE_AM, PLANNED_AHEAD, PLANNED_TWICE_PM, lateNight), NOW);
    expect(out.map((o) => [o.id, o.details['job_id']])).toEqual([
      ['188412@2026-11-20T08:30~2026-11-20T10:00', '188412'],
      ['182102', undefined],
      ['188412@2026-11-20T13:30~2026-11-20T15:00', '188412'],
      // Starting together and ending apart is still two.
      ['188412@2026-11-20T08:30~2026-11-20T09:00', '188412'],
    ]);
    expect(out.map((o) => o.start_at)).toEqual([
      '2026-11-19T22:00:00.000Z',
      '2026-11-18T22:00:00.000Z',
      '2026-11-20T03:00:00.000Z',
      '2026-11-19T22:00:00.000Z',
    ]);
  });

  it('throws when the answer is not the planned list', () => {
    for (const bad of [null, '<html>', [], {}, { plannedOutages: {} }, current(CURRENT)]) {
      expect(() => parseSapnPlanned(bad, NOW), JSON.stringify(bad)).toThrow('sapn: plannedOutages is not a list');
    }
  });
});

describe('fetchSapn', () => {
  beforeEach(() => {
    fetchJson.mockReset();
    _resetKeptParts();
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  /** Answers the two endpoints; a function is called per request. */
  function upstream(cur: unknown, plan: unknown) {
    fetchJson.mockImplementation(async (url: string) => {
      const a = url === SAPN_CURRENT_URL ? cur : url === SAPN_PLANNED_URL ? plan : new Error(`unexpected url ${url}`);
      if (a instanceof Error) throw a;
      return typeof a === 'function' ? (a as () => unknown)() : a;
    });
  }
  const urls = () => fetchJson.mock.calls.map((c) => String(c[0]));
  const ids = (rs: Array<{ id: string }>) => rs.map((r) => r.id);

  it('reads both endpoints: the unplanned outages, then the planned work', async () => {
    upstream(current(CURRENT), planned(PLANNED_TODAY, PLANNED_AHEAD));
    const out = await fetchSapn();
    expect(out.map((o) => [o.id, o.kind])).toEqual([
      ['160110617', 'unplanned'],
      ['186408', 'planned'],
      ['182102', 'planned'],
    ]);
    expect(urls().sort()).toEqual([
      'https://outage.apps.sapowernetworks.com.au/Outages/GetPublicisedCurrentOutages/',
      'https://outage.apps.sapowernetworks.com.au/Outages/GetPublicisedPlannedOutages/',
    ]);
    for (const call of fetchJson.mock.calls) expect(call[1]).toEqual({ timeoutMs: call[0] === SAPN_PLANNED_URL ? 30_000 : 20_000 });
  });

  it('reads the planned list again only after 15 minutes, the current one every time', async () => {
    upstream(current(CURRENT), planned(PLANNED_AHEAD));
    await fetchSapn();

    vi.setSystemTime(NOW + 14 * 60_000);
    upstream(current(CURRENT, CURRENT_2), planned(PLANNED_AHEAD, PLANNED_TWICE_AM));
    fetchJson.mockClear();
    expect(ids(await fetchSapn())).toEqual(['160110617', '160110636', '182102']);
    expect(urls()).toEqual([SAPN_CURRENT_URL]);

    vi.setSystemTime(NOW + 15 * 60_000);
    fetchJson.mockClear();
    expect(ids(await fetchSapn())).toEqual(['160110617', '160110636', '182102', '188412']);
    expect(urls().sort()).toEqual([SAPN_CURRENT_URL, SAPN_PLANNED_URL]);
  });

  it('work that ends between two reads of the planned list goes when it ends', async () => {
    // 16:55 in Adelaide; the work ends at 17:00.
    vi.setSystemTime(Date.parse('2026-11-13T06:25:00Z'));
    upstream(current(), planned(PLANNED_TODAY, PLANNED_AHEAD));
    expect(ids(await fetchSapn())).toEqual(['186408', '182102']);
    vi.setSystemTime(Date.parse('2026-11-13T06:31:00Z'));
    fetchJson.mockClear();
    expect(ids(await fetchSapn())).toEqual(['182102']);
    expect(urls()).toEqual([SAPN_CURRENT_URL]);
  });

  it('one endpoint failing keeps the other, and its own last good answer', async () => {
    upstream(current(CURRENT), planned(PLANNED_AHEAD));
    await fetchSapn();

    vi.setSystemTime(NOW + 3 * 60_000);
    upstream(new Error('HTTP 503'), planned(PLANNED_AHEAD));
    expect(ids(await fetchSapn())).toEqual(['160110617', '182102']);

    // The planned list is due and fails: the copy in hand, and the fresh current list.
    vi.setSystemTime(NOW + 16 * 60_000);
    upstream(current(CURRENT_2), { error: 'nope' });
    expect(ids(await fetchSapn())).toEqual(['160110636', '182102']);
  });

  it('both failing with nothing held fails the poll, so the last good snapshot stays', async () => {
    upstream(new Error('HTTP 503'), new Error('timeout'));
    await expect(fetchSapn()).rejects.toThrow('HTTP 503');
    upstream({ message: 'down for maintenance' }, []);
    await expect(fetchSapn()).rejects.toThrow('sapn: currentOutages is not a list');
  });

  it('no outages at all is an answer, not a failure', async () => {
    upstream(current(), planned());
    expect(await fetchSapn()).toEqual([]);
  });
});

describe('register()', () => {
  afterEach(() => vi.restoreAllMocks());

  it('registers sapn as a power source, every three minutes', () => {
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    register();
    expect(spy.mock.calls.map((c) => [c[0].name, c[0].family, c[0].intervalMs])).toEqual([['sapn', 'power', 180_000]]);
  });
});
