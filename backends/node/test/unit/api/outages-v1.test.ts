/**
 * GET /api/v1/outages — every electricity distributor's outages in one feed
 * (api/outages.ts).
 *
 * The NSW distributors are read from the same LiveStore snapshots the
 * per-distributor routes serve, each in its source's own shape; the other
 * states' from stores that hold the shared shape already. Pinned here: the
 * shared snake_case shape and what goes in `details`; kind and status from
 * each distributor's own terms; the stored-history source each is filed under;
 * times in UTC — a zone as written, none as Sydney time, and one that cannot be
 * read kept in details; every outage's state; the state/distributor/kind/status
 * filters and their refusals; `sources` per distributor; the bare
 * FeatureCollection on both paths; and what a key needs.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';
import { createHash } from 'node:crypto';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  getWriterPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

import { liveStore } from '../../../src/store/live.js';
import { outagesRouter, outageUtc, fromEssential } from '../../../src/api/outages.js';
import { mirror } from '../../../src/api/v1/mirror.js';
import { notFound, onError } from '../../../src/api/v1/respond.js';
import { requireApiKey } from '../../../src/services/auth/apiKey.js';
import { _clearApiKeyCache, lookupPrefix } from '../../../src/services/auth/apiKeys.js';
import { parseEssentialKml } from '../../../src/sources/essential.js';
import { normaliseAusgridOutage } from '../../../src/sources/ausgrid.js';
import { DISTRIBUTOR_INFO, DISTRIBUTORS, outageOf, withStatus } from '../../../src/sources/outages/common.js';

const NOW = Date.UTC(2026, 10, 2, 1, 0, 0); // 2026-11-02T01:00:00Z
const READ_AT = new Date(NOW).toISOString();
const STORES = [
  'endeavour_current', 'endeavour_maintenance', 'endeavour_planned', 'essential_current', 'essential_future', 'ausgrid',
  'ausnet', 'citipower', 'powercor', 'jemena', 'sapn', 'westernpower', 'horizon',
];

// --- Each distributor in its source's own shape ------------------------------

function endeavour(id: string, outageType: string, extra: Record<string, unknown> = {}) {
  return {
    id,
    suburb: 'Penrith',
    streets: 'High St',
    customersAffected: 12,
    status: 'Active',
    cause: 'Tree on lines',
    outageType,
    startTime: '2026-10-28T08:00:00+11:00',
    estimatedRestoration: '2026-10-28T12:30:00+00:00',
    lastUpdated: '2026-10-28T08:05:00.123456+00:00',
    latitude: -33.75,
    longitude: 150.69,
    postcode: '2750',
    hasGPS: true,
    ...extra,
  };
}
const EN_CURRENT = endeavour('MMJ 1', 'Unplanned');
const EN_MAINT = endeavour('SP 2', 'Current Maintenance', { status: 'Scheduled', cause: 'Maintenance Work', endTime: '2026-10-28T12:30:00+00:00', duration: '' });
// No suburb from Endeavour (the poller wrote "Unknown"), no position, no times.
const EN_FUTURE = endeavour('SP 3', 'Future Maintenance', {
  suburb: 'Unknown', streets: '', status: 'Scheduled', startTime: null, estimatedRestoration: null, lastUpdated: '',
  latitude: null, longitude: null, hasGPS: false, endTime: null, duration: '',
});

const KML = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://earth.google.com/kml/2.1">
  <Document>
    <Placemark id="INCD-1-r">
      <name>WAGGA WAGGA</name>
      <styleUrl>#unplanned</styleUrl>
      <description><![CDATA[
        <div><span>Time Off:</span>28/10/2026 14:30:00</div>
        <div><span>Est. Time On:</span>soon</div>
        <div><span>Customers affected:</span> 42</div>
        <div><span>Last Updated:</span>28/10/2026 14:35:00</div>
      ]]></description>
      <Point><coordinates>147.367,-35.108</coordinates></Point>
    </Placemark>
    <Placemark id="INCD-2-p">
      <name>DUBBO</name>
      <styleUrl>#planned</styleUrl>
      <description><![CDATA[
        <div><span>Time Off:</span>15/07/2027 08:00:00</div>
        <div><span>Est. Time On:</span>15/07/2027 15:00:00</div>
        <div><span>Customers affected:</span> 5</div>
        <div><span>Reason:</span>General network maintenance</div>
      ]]></description>
      <Point><coordinates>148.601,-32.243</coordinates></Point>
      <Polygon><outerBoundaryIs><LinearRing>
        <coordinates>148.6,-32.2 148.61,-32.2 148.61,-32.25 148.6,-32.2</coordinates>
      </LinearRing></outerBoundaryIs></Polygon>
    </Placemark>
  </Document>
</kml>`;
const [ES_UNPLANNED, ES_PLANNED_NOW] = parseEssentialKml(KML, 'current');
const ES_FUTURE = parseEssentialKml(KML, 'future').filter((o) => o.outageType === 'planned');

// Ausgrid's own events (sources/ausgrid.ts), times in UTC with a Z.
const AUSGRID = normaliseAusgridOutage({
  EventId: 833006316,
  OutageDisplayType: 'U',
  PowerStatus: 'InProgress',
  Cause: 'overhead line damage',
  Reason: null,
  Area: ['Ryde', 'West Ryde'],
  WhenUpdated: '2026-11-02T00:50:09Z',
  StartDateTime: '2026-11-01T22:45:00Z',
  EstRestTime: '2026-11-02T03:00:00Z',
  MarkerLocation: { lat: -33.81, lng: 151.1 },
  Bounds: { bottomleftlat: -33.82, bottomleftlng: 151.09, toprightlat: -33.8, toprightlng: 151.11 },
  StreetsAffected: ['Blaxland Rd', 'Victoria Rd'],
  AffectedCustomers: 57,
})!;
const planned = (EventId: number, PowerStatus: string, StartDateTime: string, EstRestTime: string | null) =>
  normaliseAusgridOutage({
    EventId,
    OutageDisplayType: 'P',
    PowerStatus,
    Cause: 'upgrading power poles in your area to make your electricity supply more resilient',
    Reason: null,
    Area: ['Greta'],
    WhenUpdated: '2026-10-20T00:13:21Z',
    StartDateTime,
    EstRestTime,
    MarkerLocation: { lat: -32.67, lng: 151.37 },
    StreetsAffected: ['Bowen St'],
    AffectedCustomers: 12,
  })!;
// Started an hour before NOW; and booked for the end of the month.
const AUSGRID_PLANNED_NOW = planned(141462, 'InProgress', '2026-11-02T00:00:00Z', '2026-11-02T05:00:00Z');
const AUSGRID_PLANNED_AHEAD = planned(139733, 'ProceedingAsScheduled', '2026-11-28T21:00:00Z', '2026-11-29T05:00:00Z');

type Props = Record<string, any>;
type Answer = {
  type: string;
  features: Array<{ type: string; geometry: { type: string; coordinates: number[] } | null; properties: Props }>;
  count: number;
  sources: Array<{ distributor: string; name: string; state: string; count: number; updated_at: string | null }>;
};

// --- The other states' distributors, in the shape their sources store ------------

// Planned work an hour from NOW, stored by a poll that ran when it was still to come.
const PLANNED_START = new Date(NOW + 3_600_000).toISOString();
const AUSNET_UNPLANNED = withStatus(
  outageOf('ausnet', {
    id: 'INC-501',
    kind: 'unplanned',
    area: 'Wodonga',
    customers_affected: 310,
    cause: 'Tree on powerline',
    start_at: '2026-11-01T23:10:00.000Z',
    restore_at: '2026-11-02T03:30:00.000Z',
    updated_at: '2026-11-02T00:40:00.000Z',
    lat: -36.12,
    lon: 146.88,
    details: { towns: ['Wodonga', 'Bandiana'] },
  }),
  NOW,
);
const AUSNET_PLANNED = withStatus(outageOf('ausnet', { id: 'PLN-9', kind: 'planned', area: 'Benalla', customers_affected: 40, start_at: PLANNED_START }), NOW);
const HORIZON_UNPLANNED = withStatus(
  outageOf('horizon', {
    id: 'HP-77',
    kind: 'unplanned',
    area: 'Broome',
    customers_affected: 25,
    lat: -17.96,
    lon: 122.24,
    polygons: [[[122.2, -17.9], [122.3, -17.9], [122.3, -18]]],
  }),
  NOW,
);

const PROPERTY_NAMES = [
  'area', 'cause', 'customers_affected', 'details', 'distributor', 'id', 'kind', 'polygons', 'restore_at', 'source',
  'start_at', 'state', 'status', 'updated_at',
].sort();

function app(opts: { gate?: boolean } = {}) {
  const a = new Hono();
  if (opts.gate) a.use('*', requireApiKey);
  a.route('/', outagesRouter);
  mirror(a, outagesRouter, ['/api/outages']);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function read(path: string): Promise<Answer> {
  const res = await app().request(path);
  expect(res.status, path).toBe(200);
  return (await res.json()) as Answer;
}
const byId = (body: Answer, id: string) => body.features.find((f) => f.properties['id'] === id)!;
const ids = (body: Answer) => body.features.map((f) => f.properties['id']);

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
  liveStore.set('endeavour_current', [EN_CURRENT]);
  liveStore.set('endeavour_maintenance', [EN_MAINT]);
  liveStore.set('endeavour_planned', [EN_FUTURE]);
  liveStore.set('essential_current', [ES_UNPLANNED, ES_PLANNED_NOW]);
  liveStore.set('essential_future', ES_FUTURE);
  // Ausgrid has not been read yet: its store is empty.
  liveStore.delete('ausgrid');
});
afterEach(() => {
  vi.useRealTimers();
  for (const s of STORES) liveStore.delete(s);
});

describe('GET /api/v1/outages', () => {
  it('one bare FeatureCollection, the same on both paths, every distributor in order', async () => {
    const v1 = await app().request('/api/v1/outages');
    expect(v1.status).toBe(200);
    expect(v1.headers.get('content-type')).toBe('application/json');
    const body = (await v1.json()) as Answer;
    expect(await (await app().request('/api/outages')).json()).toEqual(body);
    expect(Object.keys(body).sort()).toEqual(['count', 'features', 'sources', 'type']);
    expect(ids(body)).toEqual(['MMJ 1', 'SP 2', 'SP 3', 'INCD-1-r', 'INCD-2-p', 'INCD-2-p']);
    expect(body.count).toBe(6);
    for (const f of body.features) expect(Object.keys(f.properties).sort(), f.properties['id']).toEqual(PROPERTY_NAMES);
  });

  it('an Endeavour Energy outage: zoned times in UTC, its own fields in details', async () => {
    const body = await read('/api/v1/outages?distributor=endeavour&kind=unplanned');
    expect(body.features).toEqual([
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [150.69, -33.75] },
        properties: {
          id: 'MMJ 1',
          source: 'endeavour_current',
          distributor: 'endeavour',
          state: 'NSW',
          kind: 'unplanned',
          status: 'active',
          area: 'Penrith',
          customers_affected: 12,
          cause: 'Tree on lines',
          start_at: '2026-10-27T21:00:00.000Z',
          restore_at: '2026-10-28T12:30:00.000Z',
          updated_at: '2026-10-28T08:05:00.123Z',
          polygons: [],
          details: { streets: 'High St', status: 'Active', postcode: '2750' },
        },
      },
    ]);
  });

  it("Endeavour's planned work: under way is active, to come is scheduled; both filed under endeavour_planned", async () => {
    const body = await read('/api/v1/outages?distributor=endeavour');
    expect(byId(body, 'SP 2').properties).toMatchObject({ kind: 'planned', status: 'active', source: 'endeavour_planned' });
    // No suburb (the poller's "Unknown"), no position and no times: empty, null and null.
    const future = byId(body, 'SP 3');
    expect(future.geometry).toBeNull();
    expect(future.properties).toMatchObject({
      kind: 'planned', status: 'scheduled', source: 'endeavour_planned', area: '', start_at: null, restore_at: null, updated_at: null,
    });
    expect(future.properties['details']).toEqual({ streets: '', status: 'Scheduled', postcode: '2750', duration: '' });
  });

  it('an Essential Energy outage: its times read as Sydney time; one that cannot be read kept in details', async () => {
    const body = await read('/api/v1/outages?distributor=essential&kind=unplanned');
    expect(body.features).toEqual([
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [147.367, -35.108] },
        properties: {
          id: 'INCD-1-r',
          source: 'essential_current',
          distributor: 'essential',
          state: 'NSW',
          kind: 'unplanned',
          status: 'active',
          area: 'WAGGA WAGGA',
          customers_affected: 42,
          // No reason given: the poller's "Unknown" is empty here.
          cause: '',
          // 14:30 in Sydney in daylight saving (+11:00).
          start_at: '2026-10-28T03:30:00.000Z',
          restore_at: null,
          updated_at: '2026-10-28T03:35:00.000Z',
          polygons: [],
          // The restoration time as written, since it is not a time; the read ones are not repeated.
          details: { title: 'WAGGA WAGGA', estTimeOn: 'soon' },
        },
      },
    ]);
  });

  it("Essential's planned outages: now and to come, filed as the poller files them, with the area's outline", async () => {
    const body = await read('/api/v1/outages?distributor=essential&kind=planned');
    expect(body.features.map((f) => [f.properties['source'], f.properties['status']])).toEqual([
      ['essential_planned', 'active'],
      ['essential_future', 'scheduled'],
    ]);
    const [now] = body.features;
    expect(now!.properties).toMatchObject({
      cause: 'General network maintenance',
      // 08:00 in Sydney in winter (+10:00).
      start_at: '2027-07-14T22:00:00.000Z',
      restore_at: '2027-07-15T05:00:00.000Z',
      polygons: [[[148.6, -32.2], [148.61, -32.2], [148.61, -32.25], [148.6, -32.2]]],
    });
  });

  it("an Essential Energy outage across the border is Queensland's, and a request for QLD finds it", async () => {
    // Goondiwindi, on the Queensland side of the Macintyre.
    const over = { ...ES_UNPLANNED, incidentId: 'ES-QLD-1', latitude: -28.546, longitude: 150.307 };
    expect(fromEssential(over).state).toBe('QLD');
    expect(fromEssential(ES_UNPLANNED).state).toBe('NSW');
    // No location: it stays with the distributor's own state.
    expect(fromEssential({ ...ES_UNPLANNED, latitude: null, longitude: null } as typeof ES_UNPLANNED).state).toBe('NSW');

    liveStore.set('essential_current', [ES_UNPLANNED, over]);
    const qld = await read('/api/v1/outages?state=QLD');
    expect(ids(qld)).toEqual(['ES-QLD-1']);
    expect(qld.features[0]!.properties).toMatchObject({ distributor: 'essential', state: 'QLD' });
    expect(ids(await read('/api/v1/outages?state=NSW&distributor=essential'))).not.toContain('ES-QLD-1');
  });

  it('an Ausgrid outage: the common fields, and streets, status and reason in details under four keys', async () => {
    liveStore.set('ausgrid', { Markers: [AUSGRID], Polygons: [] });
    const body = await read('/api/v1/outages?distributor=ausgrid');
    expect(body.features).toEqual([
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [151.1, -33.81] },
        properties: {
          id: '833006316',
          source: 'ausgrid',
          distributor: 'ausgrid',
          state: 'NSW',
          kind: 'unplanned',
          status: 'active',
          area: 'Ryde, West Ryde',
          customers_affected: 57,
          cause: 'overhead line damage',
          start_at: '2026-11-01T22:45:00.000Z',
          restore_at: '2026-11-02T03:00:00.000Z',
          updated_at: '2026-11-02T00:50:09.000Z',
          // Ausgrid gives a box around the outage, not its outline.
          polygons: [],
          details: { streets: 'Blaxland Rd, Victoria Rd', status: 'In progress', power_status: 'InProgress', reason: '' },
        },
      },
    ]);
  });

  it("Ausgrid's planned work: under way is active, not yet started is scheduled, and it becomes active when it starts", async () => {
    liveStore.set('ausgrid', { Markers: [AUSGRID, AUSGRID_PLANNED_NOW, AUSGRID_PLANNED_AHEAD], Polygons: [] });
    const body = await read('/api/v1/outages?distributor=ausgrid');
    expect(body.features.map((f) => [f.properties['id'], f.properties['kind'], f.properties['status']])).toEqual([
      ['833006316', 'unplanned', 'active'],
      ['141462', 'planned', 'active'],
      ['139733', 'planned', 'scheduled'],
    ]);
    expect(byId(body, '139733').properties).toMatchObject({
      source: 'ausgrid',
      start_at: '2026-11-28T21:00:00.000Z',
      restore_at: '2026-11-29T05:00:00.000Z',
      updated_at: '2026-10-20T00:13:21.000Z',
      details: { streets: 'Bowen St', status: 'Proceeding as scheduled', power_status: 'ProceedingAsScheduled', reason: '' },
    });
    expect(ids(await read('/api/v1/outages?distributor=ausgrid&status=scheduled'))).toEqual(['139733']);
    expect(ids(await read('/api/v1/outages?distributor=ausgrid&status=active'))).toEqual(['833006316', '141462']);

    vi.setSystemTime(Date.parse('2026-11-28T21:00:00Z'));
    expect(ids(await read('/api/v1/outages?distributor=ausgrid&status=scheduled'))).toEqual([]);
  });

  it('an Ausgrid time that cannot be read stays in details as written', async () => {
    liveStore.set('ausgrid', { Markers: [{ ...AUSGRID, EstRestoration: 'soon', WhenUpdated: '' }], Polygons: [] });
    const [f] = (await read('/api/v1/outages?distributor=ausgrid')).features;
    expect(f!.properties).toMatchObject({ restore_at: null, updated_at: null });
    expect(f!.properties['details']).toEqual({
      streets: 'Blaxland Rd, Victoria Rd', status: 'In progress', power_status: 'InProgress', reason: '', EstRestoration: 'soon',
    });
  });

  it("another state's distributor: its store is the shared shape, read as it is", async () => {
    liveStore.set('ausnet', [AUSNET_UNPLANNED]);
    const body = await read('/api/v1/outages?distributor=ausnet');
    expect(body.features).toEqual([
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [146.88, -36.12] },
        properties: {
          id: 'INC-501',
          source: 'ausnet',
          distributor: 'ausnet',
          state: 'VIC',
          kind: 'unplanned',
          status: 'active',
          area: 'Wodonga',
          customers_affected: 310,
          cause: 'Tree on powerline',
          start_at: '2026-11-01T23:10:00.000Z',
          restore_at: '2026-11-02T03:30:00.000Z',
          updated_at: '2026-11-02T00:40:00.000Z',
          polygons: [],
          details: { towns: ['Wodonga', 'Bandiana'] },
        },
      },
    ]);
    expect(body.sources).toEqual([{ distributor: 'ausnet', name: 'AusNet', state: 'VIC', count: 1, updated_at: READ_AT }]);
    for (const f of body.features) expect(Object.keys(f.properties).sort()).toEqual(PROPERTY_NAMES);
  });

  it("its planned work is scheduled until it starts, whatever the stored record says", async () => {
    liveStore.set('ausnet', [AUSNET_UNPLANNED, AUSNET_PLANNED]);
    expect(AUSNET_PLANNED.status).toBe('scheduled');
    expect(ids(await read('/api/v1/outages?distributor=ausnet&status=scheduled'))).toEqual(['PLN-9']);
    expect(ids(await read('/api/v1/outages?distributor=ausnet&status=active'))).toEqual(['INC-501']);

    // The poller has not run again; the work has begun.
    vi.setSystemTime(NOW + 3_600_000);
    expect(ids(await read('/api/v1/outages?distributor=ausnet&status=scheduled'))).toEqual([]);
    const now = byId(await read('/api/v1/outages?distributor=ausnet&status=active'), 'PLN-9');
    expect(now.properties).toMatchObject({ kind: 'planned', status: 'active', state: 'VIC', start_at: PLANNED_START });
  });

  it('every outage says its state, and the merged list runs NSW first, then state by state', async () => {
    liveStore.set('ausnet', [AUSNET_UNPLANNED]);
    liveStore.set('horizon', [HORIZON_UNPLANNED]);
    const body = await read('/api/v1/outages');
    expect(body.features.map((f) => [f.properties['distributor'], f.properties['state']])).toEqual([
      ['endeavour', 'NSW'], ['endeavour', 'NSW'], ['endeavour', 'NSW'],
      ['essential', 'NSW'], ['essential', 'NSW'], ['essential', 'NSW'],
      ['ausnet', 'VIC'],
      ['horizon', 'WA'],
    ]);
    expect(byId(body, 'HP-77')).toMatchObject({
      geometry: { type: 'Point', coordinates: [122.24, -17.96] },
      properties: { polygons: [[[122.2, -17.9], [122.3, -17.9], [122.3, -18]]], details: {} },
    });
  });

  it('?state= keeps the distributors of those states (any case), and the sources list follows', async () => {
    liveStore.set('ausnet', [AUSNET_UNPLANNED, AUSNET_PLANNED]);
    liveStore.set('horizon', [HORIZON_UNPLANNED]);
    const vic = await read('/api/v1/outages?state=vic');
    expect(ids(vic)).toEqual(['INC-501', 'PLN-9']);
    expect(vic.sources.map((s) => [s.distributor, s.state, s.count])).toEqual([
      ['ausnet', 'VIC', 2], ['citipower', 'VIC', 0], ['powercor', 'VIC', 0], ['jemena', 'VIC', 0],
    ]);
    expect(ids(await read('/api/v1/outages?state=WA,%20Vic&kind=unplanned'))).toEqual(['INC-501', 'HP-77']);
    expect((await read('/api/v1/outages?state=NSW')).count).toBe(6);
    // A state and a distributor of another state have nothing in common.
    const none = await read('/api/v1/outages?state=QLD&distributor=ausnet');
    expect(none).toMatchObject({ count: 0, features: [], sources: [] });
    // Queensland has no distributor of its own in the feed. Essential
    // Energy, a NSW distributor whose network reaches into southern
    // Queensland, is the one asked: it is listed, with none of its outages
    // there just now.
    const qld = await read('/api/v1/outages?state=qld');
    expect(qld.count).toBe(0);
    expect(qld.sources.map((s) => [s.distributor, s.state, s.count, s.updated_at === null])).toEqual([
      ['essential', 'NSW', 0, false],
    ]);
  });

  it('a state the feed has no distributor in is refused, like any unknown one', async () => {
    for (const state of ['ACT', 'NT', 'nsw,xx']) {
      const res = await app().request(`/api/v1/outages?state=${state}`);
      expect(res.status, state).toBe(400);
      const { error } = (await res.json()) as { error: { code: string; message: string } };
      expect(error.code).toBe('validation_failed');
      expect(error.message, state).toBe(`state: unknown state: ${state === 'nsw,xx' ? 'XX' : state} (one of NSW, VIC, QLD, SA, WA)`);
    }
  });

  it('filters by distributor, kind and status (any case), and the sources list follows', async () => {
    expect(ids(await read('/api/v1/outages?status=scheduled'))).toEqual(['SP 3', 'INCD-2-p']);
    expect(ids(await read('/api/v1/outages?kind=PLANNED&status=active'))).toEqual(['SP 2', 'INCD-2-p']);
    const one = await read('/api/v1/outages?distributor=Essential');
    expect(one.sources.map((s) => s.distributor)).toEqual(['essential']);
    expect(ids(await read('/api/v1/outages?distributor=endeavour,ausgrid&kind=unplanned'))).toEqual(['MMJ 1']);
  });

  it('`sources`: one entry per distributor, counted after the filters, null when not every feed has been read', async () => {
    // Read and empty: no outages. Not the same as Ausgrid's, which is not read.
    liveStore.set('sapn', []);
    const body = await read('/api/v1/outages?kind=planned');
    expect(body.sources.slice(0, 3)).toEqual([
      { distributor: 'endeavour', name: 'Endeavour Energy', state: 'NSW', count: 2, updated_at: READ_AT },
      { distributor: 'essential', name: 'Essential Energy', state: 'NSW', count: 2, updated_at: READ_AT },
      { distributor: 'ausgrid', name: 'Ausgrid', state: 'NSW', count: 0, updated_at: null },
    ]);
    // Every distributor, in order, each under its name and state; the ones with no source yet are not read.
    expect(body.sources.map((s) => s.distributor)).toEqual([...DISTRIBUTORS]);
    for (const s of body.sources) {
      const info = DISTRIBUTOR_INFO[s.distributor as keyof typeof DISTRIBUTOR_INFO];
      expect(s, s.distributor).toMatchObject({ name: info.name, state: info.state });
    }
    expect(body.sources.slice(3).map((s) => [s.distributor, s.count, s.updated_at])).toEqual([
      ['ausnet', 0, null], ['citipower', 0, null], ['powercor', 0, null], ['jemena', 0, null],
      ['sapn', 0, READ_AT], ['westernpower', 0, null], ['horizon', 0, null],
    ]);
    liveStore.delete('essential_future');
    const partial = await read('/api/v1/outages');
    expect(partial.sources.find((s) => s.distributor === 'essential')).toEqual({
      distributor: 'essential', name: 'Essential Energy', state: 'NSW', count: 2, updated_at: null,
    });
  });

  it('an unknown value is refused: 400 validation_failed, naming each bad field', async () => {
    const res = await app().request('/api/v1/outages?state=mars&distributor=agl&kind=maybe&status=done');
    expect(res.status).toBe(400);
    const { error } = (await res.json()) as { error: { code: string; message: string; details: { issues: Array<{ path: string; message: string }> } } };
    expect(error.code).toBe('validation_failed');
    expect(error.details.issues.map((i) => i.path)).toEqual(['state', 'distributor', 'kind', 'status']);
    expect(error.message).toBe('state: unknown state: MARS (one of NSW, VIC, QLD, SA, WA)');
    expect(error.details.issues[1]!.message).toBe(
      'unknown distributor: agl (one of endeavour, essential, ausgrid, ausnet, citipower, powercor, jemena, sapn, westernpower, horizon)',
    );
  });
});

describe('outageUtc', () => {
  it('a time with a zone as written; without one as Sydney time; anything else null', () => {
    expect(outageUtc('2026-11-04T20:30:00+00:00')).toBe('2026-11-04T20:30:00.000Z');
    expect(outageUtc('2026-11-02T01:10:13.919746+00:00')).toBe('2026-11-02T01:10:13.919Z');
    expect(outageUtc('2026-11-02T12:00:00Z')).toBe('2026-11-02T12:00:00.000Z');
    expect(outageUtc('02/11/2026 12:00:00')).toBe('2026-11-02T01:00:00.000Z');
    expect(outageUtc('15/07/2026 12:00')).toBe('2026-07-15T02:00:00.000Z');
    expect(outageUtc('2026-07-15T12:00:00')).toBe('2026-07-15T02:00:00.000Z');
    for (const bad of ['', '  ', 'soon', '31/13/2026 10:00:00', '2026-11-02', null, undefined, 12]) expect(outageUtc(bad), String(bad)).toBeNull();
  });
});

describe('what a key needs to read it', () => {
  const KEY = 'ak_abcdefghij_' + 'O'.repeat(32);
  function withScopes(scopes: string[]) {
    _clearApiKeyCache();
    queryMock.mockReset();
    queryMock.mockResolvedValue({
      rows: [{
        id: 'k1', user_id: 'u1', name: 'cli', prefix: lookupPrefix(KEY),
        key_hash: createHash('sha256').update(KEY).digest('hex'),
        scopes, rate_limit_per_min: 100, created_at: 'x', last_used_at: null, expires_at: null, revoked_at: null,
      }],
      rowCount: 1,
    });
  }
  const asKey = { headers: { 'X-API-Key': KEY } };

  it('a key holding outages:live reads it', async () => {
    withScopes(['outages:live']);
    const res = await app({ gate: true }).request('/api/v1/outages', asKey);
    expect(res.status).toBe(200);
    expect(((await res.json()) as Answer).count).toBe(6);
  });

  it('a key with another feed, or outages history, is refused, told what it needs', async () => {
    for (const scopes of [['fires:live'], ['outages:history']]) {
      withScopes(scopes);
      const res = await app({ gate: true }).request('/api/v1/outages', asKey);
      expect(res.status, scopes[0]).toBe(403);
      expect(await res.json()).toEqual({
        error: { code: 'api_key_scope_forbidden', message: "This key's scopes do not cover this endpoint. It needs outages:live." },
      });
    }
  });
});
