/**
 * Western Power outages (sources/outages/westernpower.ts).
 *
 * The records below are real ones from Western Power's list (November 2026),
 * one or two of each type. The fetch tests mock the shared HTTP module so the
 * upstream URL never gets hit.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// vi.hoisted: see ../endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import { fetchWesternPower, parseWesternPower } from '../../../../src/sources/outages/westernpower.js';
import { withStatus } from '../../../../src/sources/outages/common.js';

/** Planned work six days ahead. */
const FUTURE = {
  outageId: 'INCD-203314-X',
  outageType: 'F',
  startTime: '20261119T073000+08:00',
  restorationTime: '20261119T153000+08:00',
  onMap: true,
  outageUpdatedTime: '20261109T160006+00:00',
  affectedCustomers: 44,
  areas: ['GREENMOUNT', 'KOONGAMIA'],
  latitudeCentroid: -31.90525,
  longitudeCentroid: 116.04871,
};

/** Planned work under way: started to the second that morning. */
const PLANNED_NOW = {
  outageId: 'INCD-511792-V',
  outageType: 'P',
  startTime: '20261113T093955+08:00',
  restorationTime: '20261113T160000+08:00',
  onMap: true,
  outageUpdatedTime: '20261108T000455+00:00',
  affectedCustomers: 36,
  areas: ['HAY'],
  latitudeCentroid: -34.95937,
  longitudeCentroid: 117.39033,
};

const UNPLANNED = {
  outageId: 'INCD-2186581-U',
  outageType: 'U',
  startTime: '20261113T162650+08:00',
  restorationTime: '20261113T233000+08:00',
  onMap: true,
  outageUpdatedTime: '20261113T082723+00:00',
  affectedCustomers: 10,
  areas: ['BOORALAMING', 'BADGERIN ROCK', 'MANMANNING'],
  latitudeCentroid: -30.90239,
  longitudeCentroid: 117.25707,
};

/** Two customers: listed, but not drawn on Western Power's map. */
const UNPLANNED_OFF_MAP = {
  outageId: 'INCD-2186101-U',
  outageType: 'U',
  startTime: '20261113T075326+08:00',
  restorationTime: '20261119T183000+08:00',
  onMap: false,
  outageUpdatedTime: '20261112T235357+00:00',
  affectedCustomers: 2,
  areas: ['WESTONIA'],
  latitudeCentroid: -31.28575,
  longitudeCentroid: 118.69815,
};

/** Planned work to come whose id ends in -U: the id's letter is not the type. */
const FUTURE_U_ID = {
  outageId: 'INCD-2185571-U',
  outageType: 'F',
  startTime: '20261117T080000+08:00',
  restorationTime: '20261117T160000+08:00',
  onMap: false,
  outageUpdatedTime: '20261112T030249+00:00',
  affectedCustomers: 1,
  areas: ['BOYUP BROOK'],
  latitudeCentroid: -33.82947,
  longitudeCentroid: 116.38411,
};

const NOW = Date.parse('2026-11-13T08:40:00Z');

describe('parseWesternPower', () => {
  it('planned work to come: every field, Perth times as UTC', () => {
    expect(parseWesternPower([FUTURE])).toEqual([
      {
        id: 'INCD-203314-X',
        source: 'westernpower',
        distributor: 'westernpower',
        state: 'WA',
        kind: 'planned',
        area: 'Greenmount, Koongamia',
        customers_affected: 44,
        cause: '',
        // 07:30 in Perth is 23:30 the day before in UTC.
        start_at: '2026-11-18T23:30:00.000Z',
        restore_at: '2026-11-19T07:30:00.000Z',
        // Written with +00:00, so it is not moved.
        updated_at: '2026-11-09T16:00:06.000Z',
        lat: -31.90525,
        lon: 116.04871,
        polygons: [],
        details: { type: 'F', on_map: true },
      },
    ]);
  });

  it('F and P are planned, U is unplanned, whatever letter the id ends in', () => {
    const out = parseWesternPower([FUTURE, PLANNED_NOW, UNPLANNED, UNPLANNED_OFF_MAP, FUTURE_U_ID]);
    expect(out.map((o) => [o.id, o.kind, o.details['type']])).toEqual([
      ['INCD-203314-X', 'planned', 'F'],
      ['INCD-511792-V', 'planned', 'P'],
      ['INCD-2186581-U', 'unplanned', 'U'],
      ['INCD-2186101-U', 'unplanned', 'U'],
      ['INCD-2185571-U', 'planned', 'F'],
    ]);
  });

  it('reads the compact times to the second', () => {
    const [planned, unplanned] = parseWesternPower([PLANNED_NOW, UNPLANNED]);
    expect(planned).toMatchObject({
      start_at: '2026-11-13T01:39:55.000Z',
      restore_at: '2026-11-13T08:00:00.000Z',
      updated_at: '2026-11-08T00:04:55.000Z',
      area: 'Hay',
      customers_affected: 36,
    });
    expect(unplanned).toMatchObject({
      start_at: '2026-11-13T08:26:50.000Z',
      restore_at: '2026-11-13T15:30:00.000Z',
      updated_at: '2026-11-13T08:27:23.000Z',
      area: 'Booralaming, Badgerin Rock, Manmanning',
      customers_affected: 10,
      lat: -30.90239,
      lon: 117.25707,
    });
  });

  it('gives no status of its own: the clock decides between scheduled and active', () => {
    const [future, now, unplanned] = parseWesternPower([FUTURE, PLANNED_NOW, UNPLANNED]);
    expect('status' in future!).toBe(false);
    expect('status' in now!).toBe(false);
    expect(withStatus(future!, NOW).status).toBe('scheduled');
    expect(withStatus(now!, NOW).status).toBe('active');
    expect(withStatus(unplanned!, NOW).status).toBe('active');
  });

  it('keeps an outage that is not on the map, and says so', () => {
    const [o] = parseWesternPower([UNPLANNED_OFF_MAP]);
    expect(o).toMatchObject({ id: 'INCD-2186101-U', customers_affected: 2, area: 'Westonia', details: { type: 'U', on_map: false } });
  });

  it('passes on "to come" only when the start time cannot be read', () => {
    const [future, now] = parseWesternPower([
      { ...FUTURE, startTime: 'soon' },
      { ...PLANNED_NOW, startTime: null },
    ]);
    expect(future).toMatchObject({ start_at: null, status: 'scheduled' });
    expect(withStatus(future!, NOW).status).toBe('scheduled');
    expect('status' in now!).toBe(false);
    expect(withStatus(now!, NOW).status).toBe('active');
  });

  it('reads customers and coordinates written as text, and manages without what is missing', () => {
    const [o] = parseWesternPower([
      { ...UNPLANNED, affectedCustomers: '156', latitudeCentroid: '-31.899', longitudeCentroid: '115.87157', areas: 'DIANELLA' },
    ]);
    expect(o).toMatchObject({ customers_affected: 156, lat: -31.899, lon: 115.87157, area: 'Dianella' });
    expect(parseWesternPower([{ outageId: 'INCD-1-U', outageType: 'u' }])).toEqual([
      expect.objectContaining({
        id: 'INCD-1-U',
        kind: 'unplanned',
        area: '',
        customers_affected: 0,
        start_at: null,
        restore_at: null,
        updated_at: null,
        lat: null,
        lon: null,
        details: { type: 'U' },
      }),
    ]);
  });

  it('names a locality once', () => {
    expect(parseWesternPower([{ ...FUTURE, areas: ['GREENMOUNT', 'KOONGAMIA', 'GREENMOUNT', '', null] }])[0]!.area).toBe('Greenmount, Koongamia');
  });

  it('leaves out a type it does not know, a record with no id, and anything that is not a record', () => {
    const out = parseWesternPower([FUTURE, { ...UNPLANNED, outageType: 'C' }, { ...UNPLANNED, outageId: '' }, null, 'x', []]);
    expect(out.map((o) => o.id)).toEqual(['INCD-203314-X']);
  });

  it('an empty list is no outages', () => {
    expect(parseWesternPower([])).toEqual([]);
  });

  it('throws on anything but a list, and on a list with nothing readable in it', () => {
    for (const bad of [null, undefined, 42, '<html>', { outages: [FUTURE] }]) {
      expect(() => parseWesternPower(bad), JSON.stringify(bad)).toThrow('not an array');
    }
    expect(() => parseWesternPower([{ id: 1, type: 'Planned' }, { error: 'nope' }])).toThrow('could be read');
  });
});

describe('fetchWesternPower', () => {
  beforeEach(() => fetchJson.mockReset());

  it('reads the one endpoint, with twenty seconds to answer', async () => {
    fetchJson.mockResolvedValueOnce([FUTURE, UNPLANNED]);
    const out = await fetchWesternPower();
    expect(out.map((o) => o.id)).toEqual(['INCD-203314-X', 'INCD-2186581-U']);
    expect(fetchJson.mock.calls).toEqual([['https://www.westernpower.com.au/api/corp/outage/all-outages', { timeoutMs: 20_000 }]]);
  });

  it('throws on a wrong-shaped answer and on a failed request, so the last good snapshot stays', async () => {
    fetchJson.mockResolvedValueOnce({ message: 'Service unavailable' });
    await expect(fetchWesternPower()).rejects.toThrow('not an array');
    fetchJson.mockRejectedValueOnce(new Error('HTTP 503'));
    await expect(fetchWesternPower()).rejects.toThrow('HTTP 503');
  });
});

describe('register()', () => {
  afterEach(() => vi.resetModules());

  it('polls every three minutes under the name westernpower', async () => {
    vi.resetModules();
    const registry = await import('../../../../src/services/sourceRegistry.js');
    const { default: register } = await import('../../../../src/sources/outages/westernpower.js');
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    register();
    expect(spy.mock.calls.map((c) => [c[0].name, c[0].family, c[0].intervalMs])).toEqual([['westernpower', 'power', 180_000]]);
    spy.mockRestore();
  });
});
