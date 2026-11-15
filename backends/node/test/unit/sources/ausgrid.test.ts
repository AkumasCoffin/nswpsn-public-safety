/**
 * Ausgrid fetcher tests.
 *
 * The events below are real records from Ausgrid's outage list (November
 * 2026), trimmed to a few. The fetch tests mock the shared HTTP module so the
 * upstream URL never gets hit.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// vi.hoisted: see ../endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import {
  normaliseAusgridOutage,
  fetchAusgridOutages,
  fetchAusgridStats,
  ausgridScheduled,
  __resetAusgridFutureCache,
  type RawAusgridEvent,
} from '../../../src/sources/ausgrid.js';

const UNPLANNED: RawAusgridEvent = {
  EventId: 737005555,
  OutageDisplayType: 'U',
  PowerStatus: 'InProgress',
  Cause: 'customer equipment fault',
  Reason: null,
  Area: ['Beacon Hill'],
  WhenUpdated: '2026-11-13T02:44:09Z',
  StartDateTime: '2026-11-10T10:00:00Z',
  EstRestTime: null,
  DelayInHours: 0,
  ActualRestoreTime: null,
  MarkerLocation: { lat: -33.75219, lng: 151.2532 },
  Bounds: { bottomleftlat: -33.75224, bottomleftlng: 151.25315, toprightlat: -33.75214, toprightlng: 151.25325 },
  StreetsAffected: [],
  AffectedCustomers: 17,
};

const RESTORED: RawAusgridEvent = {
  EventId: 737005321,
  OutageDisplayType: 'U',
  PowerStatus: 'Restored',
  Cause: 'animal on power lines',
  Reason: null,
  Area: ['Charlestown'],
  WhenUpdated: '2026-11-13T02:33:34Z',
  StartDateTime: '2026-11-09T16:00:00Z',
  EstRestTime: '2026-11-09T21:30:00Z',
  DelayInHours: 0,
  ActualRestoreTime: '2026-11-13T02:33:28Z',
  MarkerLocation: null,
  Bounds: null,
  StreetsAffected: [],
  AffectedCustomers: 82,
};

/** Planned work under way today. */
const PLANNED_NOW: RawAusgridEvent = {
  EventId: 141462,
  OutageDisplayType: 'P',
  PowerStatus: 'RestorationHasCommenced',
  Cause: 'working on overhead powerlines in your neighbourhood to prevent future outages and improve service',
  Reason: null,
  Area: ['Lovedale'],
  WhenUpdated: '2026-11-13T01:10:16Z',
  StartDateTime: '2026-11-12T21:00:00Z',
  EstRestTime: '2026-11-13T12:30:00Z',
  DelayInHours: 0,
  ActualRestoreTime: null,
  MarkerLocation: { lat: -32.72156, lng: 151.34336 },
  Bounds: { bottomleftlat: -32.72161, bottomleftlng: 151.34331, toprightlat: -32.72151, toprightlng: 151.34341 },
  StreetsAffected: ['Talga Rd'],
  AffectedCustomers: 1,
};

/** Planned work two weeks ahead: only the 30-day list has it. */
const PLANNED_AHEAD: RawAusgridEvent = {
  EventId: 139733,
  OutageDisplayType: 'P',
  PowerStatus: 'Planned',
  Cause: 'working on overhead powerlines in your neighbourhood to prevent future outages and improve service',
  Reason: null,
  Area: ['Branxton', 'Greta'],
  WhenUpdated: '2026-10-20T00:13:21Z',
  StartDateTime: '2026-11-28T21:00:00Z',
  EstRestTime: '2026-11-29T05:00:00Z',
  DelayInHours: 0,
  ActualRestoreTime: null,
  MarkerLocation: { lat: -32.67517, lng: 151.37018 },
  Bounds: { bottomleftlat: -32.69124, bottomleftlng: 151.3442, toprightlat: -32.65909, toprightlng: 151.39617 },
  StreetsAffected: ['Bowen St', 'Bridge St', 'Centre St'],
  AffectedCustomers: 372,
};

const PLANNED_SCHEDULED: RawAusgridEvent = {
  EventId: 125317,
  OutageDisplayType: 'P',
  PowerStatus: 'ProceedingAsScheduled',
  Cause: 'replacing or fixing powerlines above ground in your neighbourhood',
  Reason: null,
  Area: ['Killarney Vale'],
  WhenUpdated: '2026-11-01T20:40:15Z',
  StartDateTime: '2026-11-20T21:00:00Z',
  EstRestTime: '2026-11-21T05:00:00Z',
  DelayInHours: 0,
  ActualRestoreTime: null,
  MarkerLocation: { lat: -33.37303, lng: 151.46121 },
  Bounds: { bottomleftlat: -33.37717, bottomleftlng: 151.45761, toprightlat: -33.36888, toprightlng: 151.46481 },
  StreetsAffected: ['George Evans Rd', 'Mort St', 'Robert Bourke St', 'Robertson Rd'],
  AffectedCustomers: 152,
};

const CANCELLED: RawAusgridEvent = {
  EventId: 140725,
  OutageDisplayType: 'P',
  PowerStatus: 'Cancelled',
  Cause: 'working on underground cables or equipment in your area to make your power supply more resilient',
  Reason: 'an unforeseen issue that has prevented the work from being carried out',
  Area: ['Macquarie Park'],
  WhenUpdated: '2026-09-21T00:29:31Z',
  StartDateTime: '2026-11-02T00:00:00Z',
  EstRestTime: '2026-11-27T08:00:00Z',
  DelayInHours: 0,
  ActualRestoreTime: null,
  MarkerLocation: { lat: -33.78118, lng: 151.1274 },
  Bounds: { bottomleftlat: -33.78123, bottomleftlng: 151.12735, toprightlat: -33.78113, toprightlng: 151.12745 },
  StreetsAffected: ['Talavera Rd'],
  AffectedCustomers: 1,
};

const NOW = Date.parse('2026-11-13T07:40:00Z');
const ids = (out: { Markers: Array<{ OutageId: unknown }> }) => out.Markers.map((m) => m.OutageId);
const urls = () => fetchJson.mock.calls.map((c) => String(c[0]));
const TODAY_URL = 'https://www.ausgrid.com.au/api/outages-map/outages/list?futureDayLimit=0';
const FUTURE_URL = 'https://www.ausgrid.com.au/api/outages-map/outages/list?futureDayLimit=30';

describe('normaliseAusgridOutage', () => {
  it('an unplanned outage: every field, PascalCase and camelCase', () => {
    expect(normaliseAusgridOutage(UNPLANNED)).toEqual({
      OutageId: 737005555,
      outageId: 737005555,
      JobId: '',
      Suburb: 'Beacon Hill',
      suburb: 'Beacon Hill',
      StreetName: '',
      streetName: '',
      Streets: '',
      streets: '',
      Postcode: '',
      postcode: '',
      CustomersAffected: 17,
      customersAffected: 17,
      OutageType: 'Unplanned',
      outageType: 'Unplanned',
      Cause: 'customer equipment fault',
      cause: 'customer equipment fault',
      Reason: '',
      Detail: '',
      StatusText: 'In progress',
      StartTime: '2026-11-10T10:00:00Z',
      startTime: '2026-11-10T10:00:00Z',
      // No estimate given.
      EstRestoration: '',
      estRestoration: '',
      EndDateTime: '',
      endDateTime: '',
      WhenUpdated: '2026-11-13T02:44:09Z',
      Latitude: -33.75219,
      latitude: -33.75219,
      Longitude: 151.2532,
      longitude: 151.2532,
      Status: 'InProgress',
      Classification: null,
      // Bounds is a box, not an outline: no polygon is made of it.
      Polygons: [],
    });
  });

  it('planned work: areas and streets joined, the first street, the status in words', () => {
    const out = normaliseAusgridOutage(PLANNED_AHEAD)!;
    expect(out).toMatchObject({
      OutageId: 139733,
      OutageType: 'Planned',
      outageType: 'Planned',
      Suburb: 'Branxton, Greta',
      suburb: 'Branxton, Greta',
      Streets: 'Bowen St, Bridge St, Centre St',
      streets: 'Bowen St, Bridge St, Centre St',
      StreetName: 'Bowen St',
      streetName: 'Bowen St',
      CustomersAffected: 372,
      StatusText: 'Planned',
      Status: 'Planned',
      StartTime: '2026-11-28T21:00:00Z',
      EstRestoration: '2026-11-29T05:00:00Z',
      EndDateTime: '2026-11-29T05:00:00Z',
      endDateTime: '2026-11-29T05:00:00Z',
      WhenUpdated: '2026-10-20T00:13:21Z',
      Polygons: [],
    });
    expect(normaliseAusgridOutage(PLANNED_SCHEDULED)?.StatusText).toBe('Proceeding as scheduled');
    expect(normaliseAusgridOutage(PLANNED_NOW)?.StatusText).toBe('Restoration has commenced');
  });

  it('takes an area or a street given as one string, and a reason when there is one', () => {
    const out = normaliseAusgridOutage({ ...PLANNED_NOW, Area: 'Lovedale', StreetsAffected: 'Talga Rd', Reason: 'an unexpected complication on site' });
    expect(out).toMatchObject({ Suburb: 'Lovedale', Streets: 'Talga Rd', StreetName: 'Talga Rd', Reason: 'an unexpected complication on site' });
  });

  it('leaves out what Ausgrid does not give', () => {
    const out = normaliseAusgridOutage({ EventId: 1, OutageDisplayType: 'U', MarkerLocation: null, AffectedCustomers: 'lots' });
    expect(out).toMatchObject({
      Suburb: '', Streets: '', Cause: '', StatusText: '', Status: null, StartTime: '', EstRestoration: '', WhenUpdated: '',
      Latitude: null, Longitude: null, CustomersAffected: 0,
    });
  });

  it('drops events that are over: restored, completed, cancelled, in any case', () => {
    expect(normaliseAusgridOutage(RESTORED)).toBeNull();
    expect(normaliseAusgridOutage(CANCELLED)).toBeNull();
    expect(normaliseAusgridOutage({ ...PLANNED_NOW, PowerStatus: 'Completed' })).toBeNull();
    expect(normaliseAusgridOutage({ ...PLANNED_NOW, PowerStatus: 'COMPLETED' })).toBeNull();
    expect(normaliseAusgridOutage({ ...UNPLANNED, PowerStatus: 'restored' })).toBeNull();
    // Each word ends only its own kind of outage.
    expect(normaliseAusgridOutage({ ...UNPLANNED, PowerStatus: 'Completed' })).not.toBeNull();
    expect(normaliseAusgridOutage({ ...PLANNED_NOW, PowerStatus: 'Restored' })).not.toBeNull();
  });

  it('returns null for anything that is not an event', () => {
    expect(normaliseAusgridOutage(null as never)).toBeNull();
    expect(normaliseAusgridOutage('nope' as never)).toBeNull();
    expect(normaliseAusgridOutage([] as never)).toBeNull();
  });
});

describe('ausgridScheduled', () => {
  it('is true only for planned work that has not started', () => {
    expect(ausgridScheduled(normaliseAusgridOutage(PLANNED_AHEAD)!, NOW)).toBe(true);
    expect(ausgridScheduled(normaliseAusgridOutage(PLANNED_SCHEDULED)!, NOW)).toBe(true);
    // Started yesterday.
    expect(ausgridScheduled(normaliseAusgridOutage(PLANNED_NOW)!, NOW)).toBe(false);
    expect(ausgridScheduled(normaliseAusgridOutage(UNPLANNED)!, NOW)).toBe(false);
    // The moment it starts it is under way.
    expect(ausgridScheduled(normaliseAusgridOutage(PLANNED_AHEAD)!, Date.parse('2026-11-28T21:00:00Z'))).toBe(false);
  });

  it('is false for an unplanned outage whatever its start, and for a start that is not a time', () => {
    expect(ausgridScheduled(normaliseAusgridOutage({ ...UNPLANNED, StartDateTime: '2026-12-01T00:00:00Z' })!, NOW)).toBe(false);
    expect(ausgridScheduled(normaliseAusgridOutage({ ...PLANNED_AHEAD, StartDateTime: null })!, NOW)).toBe(false);
    expect(ausgridScheduled(normaliseAusgridOutage({ ...PLANNED_AHEAD, StartDateTime: 'soon' })!, NOW)).toBe(false);
  });
});

describe('fetchAusgridOutages', () => {
  beforeEach(() => {
    fetchJson.mockReset();
    __resetAusgridFutureCache();
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  /** Answers the two list URLs; a function is called per request. */
  function upstream(today: unknown, future: unknown | (() => unknown)) {
    fetchJson.mockImplementation(async (url: string) => {
      if (url === TODAY_URL) return today;
      if (url === FUTURE_URL) return typeof future === 'function' ? future() : future;
      throw new Error(`unexpected url ${url}`);
    });
  }

  it('reads today and the next 30 days, never the unbounded list, and merges them without the over events', async () => {
    upstream([UNPLANNED, RESTORED, PLANNED_NOW], [UNPLANNED, RESTORED, PLANNED_NOW, PLANNED_SCHEDULED, PLANNED_AHEAD, CANCELLED]);
    const out = await fetchAusgridOutages();
    expect(urls()).toEqual([TODAY_URL, FUTURE_URL]);
    expect(ids(out)).toEqual([737005555, 141462, 125317, 139733]);
    expect(out.Polygons).toEqual([]);
  });

  it("today's record wins over the cached copy of the same event", async () => {
    // The 30-day list was read while the work was still to come.
    upstream([], [{ ...PLANNED_NOW, PowerStatus: 'ProceedingAsScheduled' }, PLANNED_AHEAD]);
    await fetchAusgridOutages();
    upstream([PLANNED_NOW], []);
    vi.setSystemTime(NOW + 2 * 60_000);
    const out = await fetchAusgridOutages();
    expect(out.Markers.find((m) => m.OutageId === 141462)?.Status).toBe('RestorationHasCommenced');
    expect(ids(out)).toEqual([141462, 139733]);

    // And once today's list says it is finished, the cached planned copy goes too.
    upstream([{ ...PLANNED_NOW, PowerStatus: 'Completed' }], []);
    vi.setSystemTime(NOW + 4 * 60_000);
    expect(ids(await fetchAusgridOutages())).toEqual([139733]);
  });

  it('reads the 30-day list again only after 30 minutes', async () => {
    upstream([UNPLANNED], [PLANNED_AHEAD]);
    await fetchAusgridOutages();
    vi.setSystemTime(NOW + 29 * 60_000);
    upstream([UNPLANNED], [PLANNED_AHEAD, PLANNED_SCHEDULED]);
    fetchJson.mockClear();
    expect(ids(await fetchAusgridOutages())).toEqual([737005555, 139733]);
    expect(urls()).toEqual([TODAY_URL]);

    vi.setSystemTime(NOW + 30 * 60_000);
    fetchJson.mockClear();
    expect(ids(await fetchAusgridOutages())).toEqual([737005555, 139733, 125317]);
    expect(urls()).toEqual([TODAY_URL, FUTURE_URL]);
  });

  it('keeps the previous 30-day list when reading it fails, and does not ask again for 30 minutes', async () => {
    upstream([UNPLANNED], [PLANNED_AHEAD]);
    await fetchAusgridOutages();

    vi.setSystemTime(NOW + 30 * 60_000);
    upstream([UNPLANNED, PLANNED_NOW], () => {
      throw new Error('HTTP 503');
    });
    expect(ids(await fetchAusgridOutages())).toEqual([737005555, 141462, 139733]);

    // Something that is not a list is a failed read as well.
    vi.setSystemTime(NOW + 60 * 60_000);
    upstream([UNPLANNED], { error: 'nope' });
    expect(ids(await fetchAusgridOutages())).toEqual([737005555, 139733]);

    vi.setSystemTime(NOW + 62 * 60_000);
    fetchJson.mockClear();
    await fetchAusgridOutages();
    expect(urls()).toEqual([TODAY_URL]);
  });

  it("carries on with today's outages when the first 30-day read fails", async () => {
    upstream([UNPLANNED], () => {
      throw new Error('timeout');
    });
    expect(ids(await fetchAusgridOutages())).toEqual([737005555]);
  });

  it("throws when today's answer is not a list, so the last good snapshot stays", async () => {
    for (const bad of [42, null, { Markers: [{ OutageId: 1 }], Polygons: [] }, '<html>']) {
      upstream(bad, [PLANNED_AHEAD]);
      await expect(fetchAusgridOutages(), JSON.stringify(bad)).rejects.toThrow('not an array');
    }
    // The 30-day list was not asked for on the strength of a bad answer.
    expect(urls().every((u) => u === TODAY_URL)).toBe(true);
  });

  it("throws when today's list cannot be fetched", async () => {
    fetchJson.mockRejectedValue(new Error('HTTP 404'));
    await expect(fetchAusgridOutages()).rejects.toThrow('HTTP 404');
  });

  it('an empty today list is no outages, not a failure', async () => {
    upstream([], []);
    expect(await fetchAusgridOutages()).toEqual({ Markers: [], Polygons: [] });
  });
});

describe('fetchAusgridStats', () => {
  it("reads Ausgrid's counters as they are", async () => {
    fetchJson.mockReset();
    const stats = { OutagePlannedTodayCount: 3, OutagePlannedTodayCustomers: 266, OutageUnplannedPdoCount: 11, OutageUnplannedPdoCustomers: 6118 };
    fetchJson.mockResolvedValueOnce(stats);
    expect(await fetchAusgridStats()).toEqual(stats);
    expect(urls()).toEqual(['https://www.ausgrid.com.au/api/outages-map/outages/stats']);
  });
});

describe('register()', () => {
  const saved = process.env['AUSGRID_DISABLED'];
  afterEach(() => {
    if (saved === undefined) delete process.env['AUSGRID_DISABLED'];
    else process.env['AUSGRID_DISABLED'] = saved;
    vi.resetModules();
  });

  /** The module reads the flag once, when it loads. */
  async function load(flag: string | undefined) {
    if (flag === undefined) delete process.env['AUSGRID_DISABLED'];
    else process.env['AUSGRID_DISABLED'] = flag;
    vi.resetModules();
    const { liveStore } = await import('../../../src/store/live.js');
    const registry = await import('../../../src/services/sourceRegistry.js');
    const { register } = await import('../../../src/sources/ausgrid.js');
    return { liveStore, registry, register };
  }

  it('polls by default: both sources registered, saved snapshots kept', async () => {
    const { liveStore, registry, register } = await load(undefined);
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    liveStore.set('ausgrid', { Markers: [{ OutageId: 1 }], Polygons: [] });
    register();
    expect(spy.mock.calls.map((c) => [c[0].name, c[0].family, c[0].intervalMs])).toEqual([
      ['ausgrid', 'power', 120_000],
      ['ausgrid_stats', 'power', 120_000],
    ]);
    expect(liveStore.get('ausgrid')).not.toBeNull();
    liveStore.delete('ausgrid');
  });

  it('archives every outage it holds, one row each under its EventId', async () => {
    const { registry, register } = await load('false');
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    register();
    const archiveItems = spy.mock.calls[0]![0].archiveItems!;
    const Markers = [UNPLANNED, PLANNED_AHEAD].map((e) => normaliseAusgridOutage(e)!);
    const rows = archiveItems({ Markers, Polygons: [] }, 1_700_000_000, 'ausgrid');
    expect(rows.map((r) => [r.source, r.source_id, r.category, r.lat, r.lng])).toEqual([
      ['ausgrid', '737005555', 'Unplanned', -33.75219, 151.2532],
      ['ausgrid', '139733', 'Planned', -32.67517, 151.37018],
    ]);
    // Nothing in a row's data changes between polls unless Ausgrid changes
    // the outage, so the archive writer can skip it (store/archive.ts).
    expect(archiveItems({ Markers, Polygons: [] }, 1_700_000_120, 'ausgrid').map((r) => r.data)).toEqual(rows.map((r) => r.data));
    expect(rows[1]!.data).toMatchObject({ title: 'Branxton, Greta', WhenUpdated: '2026-10-20T00:13:21Z' });
  });

  it('with AUSGRID_DISABLED=true: registers nothing and drops the snapshots loaded from disk, so stale outages are never served as current', async () => {
    const { liveStore, registry, register } = await load('TRUE');
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    liveStore.set('ausgrid', { Markers: [{ OutageId: 1 }], Polygons: [] });
    liveStore.set('ausgrid_stats', { total: 3 });
    register();
    expect(spy).not.toHaveBeenCalled();
    expect(liveStore.get('ausgrid')).toBeNull();
    expect(liveStore.get('ausgrid_stats')).toBeNull();
  });
});
