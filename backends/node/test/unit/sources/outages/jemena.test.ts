/**
 * Jemena outage source tests.
 *
 * The events below are real ones from Jemena's outage file (13 November
 * 2026, 19:41 in Melbourne), with the outlines cut down to a few points and
 * the restored parts' outlines redrawn small. The fetch tests mock the
 * shared HTTP module so the upstream URL never gets hit.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

// vi.hoisted: see ../endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import register, { parseJemena, fetchJemena } from '../../../../src/sources/outages/jemena.js';

const RING = [
  [144.84493311706538, -37.813776078874334],
  [144.84325475417288, -37.81144519954643],
  [144.84771567602053, -37.81096090050184],
  [144.8473072240275, -37.813833320382905],
  [144.84493311706538, -37.813776078874334],
];

const PROPS = {
  eventId: 'J.26.0152146',
  customerReference: '',
  customersOff: '8',
  status: 'In Progress (ZM10)',
  type: 'Unplanned',
  resolvedFlag: 'false',
  showOutageFlag: 'true',
  symptoms: [],
  cause: 'Vehicle Damage-',
  startTime: '2026-11-13T06:35:50',
  restoreTime: '',
  dispatchedTime: '2026-11-12T20:19:35',
  etr: '2026-11-13T20:36:00',
  plannedStartTime: '',
  plannedEndTime: '',
  lastUpdateTime: '2026-11-13T16:24:34',
};

/** One restored part of an outage, as History lists it. */
const part = (restored: string, customers: number, lat: number, lon: number, streets: string[], suburb = 'BROOKLYN', postcode = 3012) => ({
  ImpactedSuburbs: [{ SuburbName: suburb, PostCode: postcode, Streets: streets }],
  ImpactedAreaGeoJson: {
    type: 'Feature',
    properties: { type: 'Historical', customersOff: customers, restoreTime: restored },
    geometry: { type: 'Polygon', coordinates: [[[lon, lat], [lon + 0.001, lat], [lon + 0.001, lat + 0.001], [lon, lat]]] },
  },
  ImpactedArea: [[{ Latitude: lat, Longitude: lon }, { Latitude: lat, Longitude: lon + 0.001 }, { Latitude: lat + 0.001, Longitude: lon + 0.001 }]],
  RestorationTime: restored,
  ImpactedCustomers: customers,
  LocationGeoJson: { type: 'Feature', properties: { type: 'Historical', customersOff: customers, restoreTime: restored }, geometry: { type: 'Point', coordinates: [lon, lat] } },
  Location: { Latitude: lat, Longitude: lon },
});

/** Four parts already back on, eight customers still off. The parts are not in time order. */
const UNPLANNED = {
  EstimatedRestorationTime: '2026-11-13T20:36:00',
  DispatchedTime: '2026-11-12T20:19:35',
  EventId: 'J.26.0152146',
  Type: 'Unplanned',
  Status: 'In Progress (ZM10)',
  Cause: 'Third party asset damage',
  StartTime: '2026-11-13T06:35:50',
  RestorationTime: null,
  ImpactedCustomers: 8,
  LocationGeoJson: { type: 'Feature', properties: PROPS, geometry: { type: 'Point', coordinates: [144.8454134201956, -37.81227256729028] } },
  Location: { Latitude: -37.81227256729028, Longitude: 144.8454134201956 },
  ImpactedSuburbs: [{ SuburbName: 'BROOKLYN', PostCode: 3012, Streets: ['BUNTING RD', 'MCDONALD RD'] }],
  ImpactedAreaGeoJson: { type: 'Feature', properties: PROPS, geometry: { type: 'Polygon', coordinates: [RING] } },
  ImpactedArea: [RING.map(([lon, lat]) => ({ Latitude: lat, Longitude: lon }))],
  LastUpdatedTime: '2026-11-13T08:40:54.335Z',
  FeedReceivedTime: '2026-11-13T05:27:03.637Z',
  History: [
    part('2026-11-13T06:39:19', 13, -37.8094987287178, 144.84309714677593, ['MARKET ROAD', 'MCDONALD RD', 'SOMERVILLE RD']),
    part('2026-11-13T06:49:03', 197, -37.81597777034965, 144.84060913892392, ['GEELONG RD', 'OLD GEELONG RD', 'TRADE PL']),
    part('2026-11-13T07:32:34', 6, -37.81560823416525, 144.84452672440563, ['CAHIR CT', 'MCDONALD RD']),
    part('2026-11-13T07:17:54', 45, -37.81153127988307, 144.83885842291704, ['BUNTING RD', 'SOMERVILLE RD']),
  ],
};

/** Finished planned work: the top level is emptied and the restored part holds the place. */
const PLANNED_COMPLETE = {
  PlannedStartTime: '2026-11-13T08:00:00',
  PlannedEndTime: '2026-11-13T16:00:00',
  EventId: 'J.26.0149897',
  Type: 'Planned',
  Status: 'Complete',
  Cause: 'Essential Maintenance',
  StartTime: '2026-11-13T08:01:23',
  RestorationTime: '2026-11-13T12:11:40' as string | null,
  ImpactedCustomers: 16 as number | null,
  LocationGeoJson: null,
  Location: null,
  ImpactedSuburbs: [],
  ImpactedAreaGeoJson: null,
  ImpactedArea: null,
  LastUpdatedTime: '2026-11-13T08:40:54.335Z',
  FeedReceivedTime: '2026-11-13T02:20:50.604Z',
  History: [part('2026-11-13T12:11:40', 14, -37.630659041506085, 144.80724813149715, ['GREEN ST'], 'BULLA', 3428)],
};

const UNPLANNED_COMPLETED = {
  EstimatedRestorationTime: '2026-11-12T22:49:43',
  DispatchedTime: '2026-11-11T21:42:28',
  EventId: 'J.26.0152140',
  Type: 'Unplanned',
  Status: 'Completed',
  Cause: 'Network equipment fault',
  StartTime: '2026-11-12T18:49:31',
  RestorationTime: '2026-11-12T20:59:35',
  ImpactedCustomers: 0,
  LocationGeoJson: null,
  Location: null,
  ImpactedSuburbs: [],
  ImpactedAreaGeoJson: null,
  ImpactedArea: null,
  LastUpdatedTime: '2026-11-13T08:40:54.335Z',
  FeedReceivedTime: '2026-11-12T20:02:23.858Z',
  History: [part('2026-11-12T20:59:35', 30, -37.7219598517911, 145.10505698737637, ['ADINA CT', 'ALLIMA AV'], 'YALLAMBIE', 3085)],
};

/**
 * The planned work above made to read as still under way. The file had no
 * planned work in progress when it was saved.
 */
const PLANNED_UNDERWAY = { ...PLANNED_COMPLETE, Status: 'In Progress (ZM10)', RestorationTime: null, ImpactedCustomers: null };

describe('parseJemena', () => {
  it('an unplanned outage: every field', () => {
    expect(parseJemena([UNPLANNED])).toEqual([
      {
        id: 'J.26.0152146',
        source: 'jemena',
        distributor: 'jemena',
        state: 'VIC',
        kind: 'unplanned',
        area: 'Brooklyn',
        // What is still off, not the 261 already back on.
        customers_affected: 8,
        cause: 'Third party asset damage',
        // 06:35:50 in Melbourne, in daylight saving (UTC+11).
        start_at: '2026-11-12T19:35:50.000Z',
        restore_at: '2026-11-13T09:36:00.000Z',
        // The event's own 16:24:34, not the file's LastUpdatedTime.
        updated_at: '2026-11-13T05:24:34.000Z',
        lat: -37.81227256729028,
        lon: 144.8454134201956,
        polygons: [RING],
        details: { status: 'In progress', streets: 'Bunting Rd, Mcdonald Rd', postcode: '3012', reason: 'Vehicle Damage' },
      },
    ]);
  });

  it('drops events that are over: Completed, Complete, or with a restoration time', () => {
    expect(parseJemena([UNPLANNED_COMPLETED, PLANNED_COMPLETE])).toEqual([]);
    expect(parseJemena([{ ...UNPLANNED, Status: 'Complete' }])).toEqual([]);
    expect(parseJemena([{ ...UNPLANNED, Status: 'COMPLETED (ZM99)' }])).toEqual([]);
    expect(parseJemena([{ ...UNPLANNED, RestorationTime: '2026-11-13T19:00:00' }])).toEqual([]);
    expect(parseJemena([UNPLANNED_COMPLETED, UNPLANNED, PLANNED_COMPLETE]).map((o) => o.id)).toEqual(['J.26.0152146']);
  });

  it('planned work: its planned start, its planned end when there is no estimate, no status of its own', () => {
    const [o] = parseJemena([PLANNED_UNDERWAY]);
    expect(o).toMatchObject({
      id: 'J.26.0149897',
      kind: 'planned',
      cause: 'Essential Maintenance',
      // 08:00, the planned start, not the 08:01:23 the power went off.
      start_at: '2026-11-12T21:00:00.000Z',
      restore_at: '2026-11-13T05:00:00.000Z',
      // No GeoJSON on the event, so the time with the Z is all there is.
      updated_at: '2026-11-13T08:40:54.335Z',
    });
    expect(o).not.toHaveProperty('status');
    // Planned times only in the GeoJSON properties are read too.
    const props = { ...PROPS, plannedStartTime: '2026-11-20T09:00:00', plannedEndTime: '2026-11-20T15:00:00', etr: '', cause: 'Essential Maintenance' };
    const [later] = parseJemena([{ ...UNPLANNED, Type: 'Planned', StartTime: null, EstimatedRestorationTime: null, LocationGeoJson: { ...UNPLANNED.LocationGeoJson, properties: props } }]);
    expect(later).toMatchObject({ kind: 'planned', start_at: '2026-11-19T22:00:00.000Z', restore_at: '2026-11-20T04:00:00.000Z' });
  });

  it('falls back to the part restored last for the place, the suburbs, the outline and a missing count', () => {
    const [o] = parseJemena([PLANNED_UNDERWAY]);
    expect(o).toMatchObject({
      area: 'Bulla',
      customers_affected: 14,
      lat: -37.630659041506085,
      lon: 144.80724813149715,
      details: { status: 'In progress', streets: 'Green St', postcode: '3428' },
    });
    expect(o!.polygons).toHaveLength(1);

    // Restored last is 07:32:34, the third of the four, not the one at either end.
    const [bare] = parseJemena([{ ...UNPLANNED, Location: null, LocationGeoJson: null, ImpactedSuburbs: [], ImpactedAreaGeoJson: null, ImpactedArea: null }]);
    expect(bare).toMatchObject({
      lat: -37.81560823416525,
      lon: 144.84452672440563,
      area: 'Brooklyn',
      // The top level still says 8, and that stands.
      customers_affected: 8,
      details: { status: 'In progress', streets: 'Cahir Ct, Mcdonald Rd', postcode: '3012' },
    });
    expect(bare!.polygons[0]![0]).toEqual([144.84452672440563, -37.81560823416525]);
  });

  it('a top-level count of 0 stands: the parts in History are back on', () => {
    const [o] = parseJemena([{ ...UNPLANNED, ImpactedCustomers: 0 }]);
    expect(o!.customers_affected).toBe(0);
  });

  it('reads the place from the GeoJSON point, and the outline from ImpactedArea, when the plain fields are missing', () => {
    const [o] = parseJemena([{ ...UNPLANNED, Location: null, ImpactedAreaGeoJson: null, History: [] }]);
    expect(o).toMatchObject({ lat: -37.81227256729028, lon: 144.8454134201956, polygons: [RING] });
  });

  it('joins several suburbs and leaves out what an event does not give', () => {
    const two = [
      { SuburbName: 'BROOKLYN', PostCode: 3012, Streets: ['BUNTING RD'] },
      { SuburbName: 'ALTONA NORTH', PostCode: 3025, Streets: ['BUNTING RD', 'KYLE RD'] },
    ];
    expect(parseJemena([{ ...UNPLANNED, ImpactedSuburbs: two }])[0]).toMatchObject({
      area: 'Brooklyn, Altona North',
      details: { streets: 'Bunting Rd, Kyle Rd', postcode: '3012, 3025' },
    });
    expect(parseJemena([{ EventId: 'J.1', Type: 'Unplanned' }, { Type: 'Unplanned' }, null])).toEqual([
      {
        id: 'J.1', source: 'jemena', distributor: 'jemena', state: 'VIC', kind: 'unplanned', area: '', customers_affected: 0, cause: '',
        start_at: null, restore_at: null, updated_at: null, lat: null, lon: null, polygons: [], details: {},
      },
    ]);
  });

  it('throws when the answer is not a list, so the last good snapshot stays', () => {
    for (const bad of [null, 42, '<html>', {}, { outages: [UNPLANNED] }]) {
      expect(() => parseJemena(bad), JSON.stringify(bad)).toThrow('jemena');
    }
    expect(parseJemena([])).toEqual([]);
  });
});

describe('fetchJemena', () => {
  beforeEach(() => fetchJson.mockReset());

  it('reads the one file and leaves out what is over', async () => {
    fetchJson.mockResolvedValueOnce([UNPLANNED_COMPLETED, PLANNED_COMPLETE, UNPLANNED]);
    expect((await fetchJemena()).map((o) => o.id)).toEqual(['J.26.0152146']);
    expect(fetchJson.mock.calls).toEqual([['https://poweroutages.jemena.com.au/data/all-outages.json', { timeoutMs: 20_000 }]]);
  });

  it('throws on a wrong-shaped answer, and when the file cannot be reached', async () => {
    fetchJson.mockResolvedValueOnce({ message: 'Forbidden' });
    await expect(fetchJemena()).rejects.toThrow('jemena');
    fetchJson.mockRejectedValueOnce(new Error('HTTP 404'));
    await expect(fetchJemena()).rejects.toThrow('HTTP 404');
  });
});

describe('register()', () => {
  it('registers jemena with the poller every three minutes', async () => {
    const registry = await import('../../../../src/services/sourceRegistry.js');
    const spy = vi.spyOn(registry, 'registerSource').mockImplementation(() => undefined as never);
    register();
    expect(spy.mock.calls.map((c) => [c[0].name, c[0].family, c[0].intervalMs])).toEqual([['jemena', 'power', 180_000]]);
    spy.mockRestore();
  });
});
