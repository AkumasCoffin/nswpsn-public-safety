/**
 * AusNet outage source tests.
 *
 * The records below are real ones from AusNet's combined outage list
 * (13 November 2026, 19:45 in Melbourne). The fetch tests mock the shared
 * HTTP module so the upstream URL never gets hit.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// vi.hoisted: see ../endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import register, { parseAusnet, fetchAusnet } from '../../../../src/sources/outages/ausnet.js';

const UNPLANNED = {
  id: 'INCD-224356-U',
  incident: 'INCD-224356-U',
  type: 'Unplanned',
  cause: '',
  delayReason: '',
  etrUpdateReason: '',
  emergencyFlag: 'Y',
  status: 'CONSTRUCTION RESPONDING',
  incidentStatus: 'In Progress',
  statusLastUpdated: '2026-11-13 06:55PM',
  unplannedStartTime: '2026-11-13 08:22AM',
  plannedStartTime: '',
  plannedDate: '',
  plannedEndTime: '',
  plannedDelayed: 'false',
  estimatedTimeToAssessment: '2026-11-13 09:52AM',
  initialEstimatedTimeToRestoration: '2026-11-13 01:30PM',
  latestEstimatedTimeToRestoration: '2026-11-13 11:59PM',
  actualTimeOfRestoration: '',
  mergedIncidentId: '',
  priority: 3,
  longitude: 145.01705752480441,
  latitude: -37.6415651370858,
  incidentType: 'LV',
  categoryId: 'LVT',
  incidentCreated: '2026-11-13 08:24:02.204937 Australia/Melbourne',
  incidentLastUpdated: '2026-11-13 16:58:03.163723 Australia/Melbourne',
  impactedNMI: null,
  nmiCount: 3,
  details: [{ townName: 'EPPING', postCode: 3076, customerCount: 3 }],
};

/** No estimate yet. */
const UNPLANNED_NO_ESTIMATE = {
  ...UNPLANNED,
  id: 'INCD-224422-U',
  incident: 'INCD-224422-U',
  emergencyFlag: '',
  status: 'RESPONDING',
  statusLastUpdated: '2026-11-13 05:55PM',
  unplannedStartTime: '2026-11-13 01:22PM',
  initialEstimatedTimeToRestoration: '',
  latestEstimatedTimeToRestoration: '',
  longitude: 145.33766624970667,
  latitude: -38.03479962626255,
  incidentLastUpdated: '2026-11-13 18:14:02.84546 Australia/Melbourne',
  nmiCount: 94,
  details: [{ townName: 'BERWICK', postCode: 3806, customerCount: 94 }],
};

const RESTORED = {
  ...UNPLANNED,
  id: 'INCD-224412-U',
  incident: 'INCD-224412-U',
  status: 'RESTORED',
  incidentStatus: 'Completed',
  statusLastUpdated: '2026-11-13 06:22PM',
  unplannedStartTime: '2026-11-13 12:00PM',
  latestEstimatedTimeToRestoration: '2026-11-13 07:30PM',
  actualTimeOfRestoration: '2026-11-13 06:20PM',
  nmiCount: 0,
  details: [
    { townName: 'CHIRNSIDE PARK', postCode: 3116, customerCount: 206 },
    { townName: 'CHRISTMAS HILLS', postCode: 3775, customerCount: 3 },
  ],
};

/** Planned work four days ahead. unplannedStartTime is when the job was entered. */
const PLANNED = {
  id: 'INCD-221225-U',
  incident: 'INCD-221225-U',
  type: 'Planned',
  cause: '',
  delayReason: '',
  etrUpdateReason: '',
  emergencyFlag: '',
  status: 'SCHEDULED',
  incidentStatus: 'Pre-Arranged',
  statusLastUpdated: '2026-10-17 08:47AM',
  unplannedStartTime: '2026-10-13 01:37PM',
  plannedStartTime: '2026-11-17 08:45AM',
  plannedDate: '2026-11-17 08:45AM' as string | null,
  plannedEndTime: '2026-11-17 04:00PM',
  plannedDelayed: 'false',
  estimatedTimeToAssessment: '',
  initialEstimatedTimeToRestoration: '',
  latestEstimatedTimeToRestoration: '',
  actualTimeOfRestoration: '',
  mergedIncidentId: '',
  priority: 0,
  longitude: 146.4128957458807,
  latitude: -38.337059136237556,
  incidentType: 'HV_N',
  categoryId: 'HVP',
  incidentCreated: '2026-10-17 08:47:02.402654 Australia/Melbourne',
  incidentLastUpdated: '',
  impactedNMI: null,
  nmiCount: 168,
  details: [
    { townName: 'JEERALANG JUNCTION', postCode: 3840, customerCount: 149 },
    { townName: 'JUMBUK', postCode: 3869, customerCount: 18 },
    { townName: 'YINNAR SOUTH', postCode: 3869, customerCount: 1 },
  ],
};

/** Planned work that ran over today; its update time is written with an offset. */
const PLANNED_DELAYED = {
  ...PLANNED,
  id: 'INCD-139187-W',
  incident: 'INCD-139187-W',
  status: 'DELAYED',
  incidentStatus: 'In Progress',
  statusLastUpdated: '2026-11-13 04:00PM',
  unplannedStartTime: '2026-10-28 09:05AM',
  plannedStartTime: '2026-11-13 08:00AM',
  plannedDate: '2026-11-13 08:00AM',
  plannedEndTime: '2026-11-13 04:00PM',
  plannedDelayed: 'true',
  longitude: 144.97729809032927,
  latitude: -37.61116387039585,
  incidentLastUpdated: '2026-11-13 16:00:05.209339 +11:00',
  nmiCount: 1,
  details: [{ townName: 'WOLLERT', postCode: 3750, customerCount: 1 }],
};

/** Only plannedDate says when; nobody has worked out yet who is affected. */
const PLANNED_DATE_ONLY = {
  ...PLANNED,
  id: 'INCD-139352-W',
  incident: 'INCD-139352-W',
  statusLastUpdated: '2026-11-06 02:32PM',
  unplannedStartTime: '2026-11-06 02:29PM',
  plannedStartTime: '',
  plannedDate: '2026-11-23 09:00AM',
  plannedEndTime: '',
  incidentLastUpdated: '2026-11-06 14:35:02.826598 Australia/Melbourne',
  nmiCount: 0,
  details: [],
};

const CANCELLED = { ...PLANNED, id: 'INCD-220001-U', status: 'CANCELLED', incidentStatus: 'Cancelled' };

/** Never closed: "delayed" since February 2024, at 0,0. */
const STALE_2024 = {
  ...PLANNED,
  id: 'INCD-102493-W',
  incident: 'INCD-102493-W',
  status: 'DELAYED',
  statusLastUpdated: '2026-01-22 02:36PM',
  unplannedStartTime: '2024-01-30 03:50PM',
  plannedStartTime: '2024-02-13 04:15PM',
  plannedDate: null,
  plannedEndTime: '2024-02-13 04:45PM',
  plannedDelayed: 'true',
  longitude: 0,
  latitude: 0,
  nmiCount: 2053,
};

/** Never closed, and with no date at all. */
const STALE_NO_DATE = {
  ...PLANNED,
  id: 'INCD-178535-U',
  incident: 'INCD-178535-U',
  incidentStatus: 'In Progress',
  statusLastUpdated: '2025-10-31 01:53PM',
  unplannedStartTime: '2025-10-31 01:51PM',
  plannedStartTime: '',
  plannedDate: null,
  plannedEndTime: '',
  incidentLastUpdated: '2025-11-10 09:59:02.493681 Australia/Melbourne',
  nmiCount: 0,
  details: [],
};

const NOW = Date.parse('2026-11-13T08:45:00Z');
const answer = (...data: unknown[]) => ({ data, errors: [], success: true });
const parse = (...data: unknown[]) => parseAusnet(answer(...data), NOW);

describe('parseAusnet', () => {
  it('an unplanned outage: every field', () => {
    expect(parse(UNPLANNED)).toEqual([
      {
        id: 'INCD-224356-U',
        source: 'ausnet',
        distributor: 'ausnet',
        state: 'VIC',
        kind: 'unplanned',
        area: 'Epping',
        customers_affected: 3,
        cause: '',
        // 08:22 in Melbourne, in daylight saving (UTC+11).
        start_at: '2026-11-12T21:22:00.000Z',
        // 11:59 pm: the latest estimate, not the first one.
        restore_at: '2026-11-13T12:59:00.000Z',
        // The status changed at 6:55 pm, after the incident's own 4:58 pm.
        updated_at: '2026-11-13T07:55:00.000Z',
        lat: -37.6415651370858,
        lon: 145.01705752480441,
        polygons: [],
        details: { status: 'Construction responding', incident_status: 'In Progress', postcode: '3076', emergency: true },
      },
    ]);
  });

  it('an unplanned outage with no estimate has no restore time', () => {
    const [o] = parse(UNPLANNED_NO_ESTIMATE);
    expect(o).toMatchObject({ kind: 'unplanned', area: 'Berwick', customers_affected: 94, start_at: '2026-11-13T02:22:00.000Z', restore_at: null });
    // 18:14:02.84546 in Melbourne.
    expect(o!.updated_at).toBe('2026-11-13T07:14:02.845Z');
    expect(o!.details).toEqual({ status: 'Responding', incident_status: 'In Progress', postcode: '3806' });
    // The first estimate stands in when there is no later one.
    expect(parse({ ...UNPLANNED, latestEstimatedTimeToRestoration: '' })[0]!.restore_at).toBe('2026-11-13T02:30:00.000Z');
  });

  it('planned work: its planned times, towns joined, no status of its own', () => {
    const [o] = parse(PLANNED);
    expect(o).toEqual({
      id: 'INCD-221225-U',
      source: 'ausnet',
      distributor: 'ausnet',
      state: 'VIC',
      kind: 'planned',
      area: 'Jeeralang Junction, Jumbuk, Yinnar South',
      customers_affected: 168,
      cause: '',
      // Not unplannedStartTime, which is the day the job was entered.
      start_at: '2026-11-16T21:45:00.000Z',
      restore_at: '2026-11-17T05:00:00.000Z',
      updated_at: '2026-10-16T21:47:00.000Z',
      lat: -38.337059136237556,
      lon: 146.4128957458807,
      polygons: [],
      details: { status: 'Scheduled', incident_status: 'Pre-Arranged', postcode: '3840, 3869' },
    });
    expect(o).not.toHaveProperty('status');
  });

  it('planned work running late is kept, marked delayed, and reads an update time written with an offset', () => {
    const [o] = parse(PLANNED_DELAYED);
    expect(o).toMatchObject({
      id: 'INCD-139187-W',
      kind: 'planned',
      area: 'Wollert',
      start_at: '2026-11-12T21:00:00.000Z',
      restore_at: '2026-11-13T05:00:00.000Z',
      updated_at: '2026-11-13T05:00:05.209Z',
      details: { status: 'Delayed', incident_status: 'In Progress', postcode: '3750', delayed: true },
    });
  });

  it('takes the day from plannedDate when there is no planned start', () => {
    const [o] = parse(PLANNED_DATE_ONLY);
    expect(o).toMatchObject({ id: 'INCD-139352-W', start_at: '2026-11-22T22:00:00.000Z', restore_at: null, area: '', customers_affected: 0 });
    expect(o).not.toHaveProperty('status');
  });

  it("passes on AusNet's word only when the start cannot be read", () => {
    expect(parse({ ...PLANNED, plannedStartTime: 'Monday morning', plannedDate: '' })[0]).toMatchObject({ start_at: null, status: 'scheduled' });
    expect(parse({ ...PLANNED_DELAYED, plannedStartTime: 'Monday morning', plannedDate: '' })[0]).not.toHaveProperty('status');
  });

  it('adds up the towns when nmiCount is 0, and reads reasons when they are given', () => {
    const [o] = parse({ ...UNPLANNED, nmiCount: 0, details: RESTORED.details, delayReason: ' Access to site ', etrUpdateReason: 'Fault found' });
    expect(o).toMatchObject({ customers_affected: 209, area: 'Chirnside Park, Christmas Hills' });
    expect(o!.details).toMatchObject({ postcode: '3116, 3775', delay_reason: 'Access to site', estimate_reason: 'Fault found' });
  });

  it('drops what is over: cancelled, completed, restored, de-logged, or with a restoration time', () => {
    expect(parse(RESTORED, CANCELLED)).toEqual([]);
    expect(parse({ ...PLANNED, incidentStatus: 'Cancelled' })).toEqual([]);
    expect(parse({ ...PLANNED, incidentStatus: 'Completed' })).toEqual([]);
    expect(parse({ ...PLANNED, incidentStatus: 'Delog' })).toEqual([]);
    expect(parse({ ...PLANNED, status: 'cancelled' })).toEqual([]);
    expect(parse({ ...UNPLANNED, status: 'RESTORED' })).toEqual([]);
    expect(parse({ ...UNPLANNED, actualTimeOfRestoration: '2026-11-13 06:20PM' })).toEqual([]);
  });

  it('drops planned work nobody closed: ended more than a day ago, or with no date at all', () => {
    expect(parse(STALE_2024, STALE_NO_DATE)).toEqual([]);
    // Ended 26 hours ago: gone. Ended 3 hours 45 ago: still listed.
    expect(parse({ ...PLANNED_DELAYED, plannedStartTime: '2026-11-12 08:00AM', plannedEndTime: '2026-11-12 05:00PM' })).toEqual([]);
    expect(parse(PLANNED_DELAYED)).toHaveLength(1);
    // The rule is for planned work: an unplanned outage is never too old.
    expect(parse({ ...UNPLANNED, unplannedStartTime: '2026-11-01 08:22AM', latestEstimatedTimeToRestoration: '2026-11-02 08:00AM' })).toHaveLength(1);
  });

  it('0,0 is no position', () => {
    expect(parse({ ...PLANNED, latitude: 0, longitude: 0 })[0]).toMatchObject({ lat: null, lon: null });
  });

  it('keeps the survivors in order and skips what is not a record', () => {
    const out = parse(UNPLANNED, null, RESTORED, 'x', PLANNED, CANCELLED, STALE_2024, PLANNED_DELAYED, { type: 'Planned' });
    expect(out.map((o) => o.id)).toEqual(['INCD-224356-U', 'INCD-221225-U', 'INCD-139187-W']);
  });

  it('throws on an answer of the wrong shape, so the last good snapshot stays', () => {
    for (const bad of [null, 42, '<html>', [], [UNPLANNED], { data: [UNPLANNED] }, { success: false, data: [UNPLANNED] }, { success: true, data: null }, { success: true, data: {} }]) {
      expect(() => parseAusnet(bad, NOW), JSON.stringify(bad)).toThrow('ausnet');
    }
    expect(parseAusnet({ success: true, data: [], errors: [] }, NOW)).toEqual([]);
  });
});

describe('fetchAusnet', () => {
  beforeEach(() => {
    fetchJson.mockReset();
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  it('reads the one URL with a 30 s timeout and parses it as of now', async () => {
    fetchJson.mockResolvedValueOnce(answer(UNPLANNED, CANCELLED, STALE_2024, PLANNED));
    expect((await fetchAusnet()).map((o) => o.id)).toEqual(['INCD-224356-U', 'INCD-221225-U']);
    expect(fetchJson.mock.calls).toEqual([
      ['https://outagetrackerservice.ausnetservices.com.au/api/v1/outages/combinedoutage', { timeoutMs: 30_000 }],
    ]);
    // A day and a bit after it ends, the planned work is gone as well.
    vi.setSystemTime(Date.parse('2026-11-18T06:00:00Z'));
    fetchJson.mockResolvedValueOnce(answer(UNPLANNED, PLANNED));
    expect((await fetchAusnet()).map((o) => o.id)).toEqual(['INCD-224356-U']);
  });

  it('throws when AusNet answers an error, and when it cannot be reached', async () => {
    fetchJson.mockResolvedValueOnce({ data: null, errors: ['boom'], success: false });
    await expect(fetchAusnet()).rejects.toThrow('ausnet');
    fetchJson.mockRejectedValueOnce(new Error('HTTP 503'));
    await expect(fetchAusnet()).rejects.toThrow('HTTP 503');
  });
});

describe('register()', () => {
  it('registers ausnet with the poller every five minutes', async () => {
    const registry = await import('../../../../src/services/sourceRegistry.js');
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    register();
    expect(spy.mock.calls.map((c) => [c[0].name, c[0].family, c[0].intervalMs])).toEqual([['ausnet', 'power', 300_000]]);
    spy.mockRestore();
  });
});
