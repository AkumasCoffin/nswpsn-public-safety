/**
 * The other states' road sources (sources/interstateRoads.ts): each
 * publisher's records in the Live Traffic shape, on the feed for their kind,
 * with their state and credit; what is left out (ended, re-shared, simply
 * open); a part that fails keeping its last good records; history filed
 * under the NSW feed of each record's type; and the polyline encoding.
 * Fixtures follow the publishers' own field names, trimmed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const { fetchJsonMock } = vi.hoisted(() => ({ fetchJsonMock: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

import {
  actRecords,
  archiveByType,
  collectParts,
  interstateRecords,
  qldRecords,
  roadRecord,
  saClosureRecords,
  saOutbackRecords,
  saWorksRecords,
  vicRecords,
  _resetInterstateMemory,
} from '../../../src/sources/interstateRoads.js';
import { decodePolyline, encodePolyline } from '../../../src/sources/shared/polyline.js';
import { liveStore } from '../../../src/store/live.js';

const NOW = Date.UTC(2026, 10, 2, 0, 0, 0); // 2026-11-02T00:00Z
const DAY = 86_400_000;

describe('encodePolyline', () => {
  it("matches Google's own worked example, and decodes back", () => {
    // developers.google.com/maps/documentation/utilities/polylinealgorithm
    const line = [[-120.2, 38.5], [-120.95, 40.7], [-126.453, 43.252]];
    expect(encodePolyline(line)).toBe('_p~iF~ps|U_ulLnnqC_mqNvxq`@');
    expect(decodePolyline('_p~iF~ps|U_ulLnnqC_mqNvxq`@')).toEqual(line);
  });

  it('is empty for fewer than two usable points', () => {
    expect(encodePolyline([[151, -33]])).toBe('');
    expect(encodePolyline([[151, -33], [Number.NaN, 1]])).toBe('');
  });
});

// --- QLD ---------------------------------------------------------------------

function qldEvent(over: Record<string, unknown> = {}, geometry: unknown = { type: 'MultiPoint', coordinates: [[152.5, -27.5]] }) {
  return {
    type: 'Feature',
    geometry,
    properties: {
      id: 838672,
      status: 'Published',
      published: null,
      source: { source_name: 'EPS', provided_by: 'Department of Transport and Main Roads', provided_by_url: 'https://qldtraffic.qld.gov.au' },
      event_type: 'Crash',
      event_subtype: 'Single vehicle',
      event_due_to: null,
      impact: { direction: 'Both directions', impact_type: 'Lanes affected', delay: null },
      duration: { start: '2026-11-01T00:34:00+10:00', end: null },
      event_priority: 'Medium',
      description: 'In the vicinity of Spring Hill Access Rd',
      advice: 'Proceed with caution',
      information: null,
      road_summary: { road_name: 'Flinders Highway', locality: 'Torrens Creek', district: 'North West' },
      last_updated: '2026-11-02T07:25:46.150425+10:00',
      web_link: null,
      ...over,
    },
  };
}

describe('qldRecords', () => {
  it('a crash: an Incident in the Live Traffic shape, with its state and credit', () => {
    const [r] = qldRecords({ features: [qldEvent()] }, NOW);
    expect(r!.geometry).toEqual({ type: 'Point', coordinates: [152.5, -27.5] });
    expect(r!.properties).toMatchObject({
      id: 'qld:838672',
      type: 'Incident',
      mainCategory: 'Crash',
      subCategory: 'Single vehicle',
      incidentType: 'CRASH',
      subcategory: 'CRASH',
      incidentKind: 'Unplanned',
      title: 'Crash: Single vehicle',
      roads: 'Flinders Highway Torrens Creek',
      otherAdvice: 'In the vicinity of Spring Hill Access Rd',
      adviceB: 'Proceed with caution',
      affectedDirection: 'Both directions',
      start: '2026-10-31T14:34:00.000Z',
      lastUpdated: '2026-11-01T21:25:46.150Z',
      region: 'North West',
      orgName: '',
      state: 'QLD',
      source: 'qldtraffic',
      credit: 'QLDTraffic (Queensland Department of Transport and Main Roads)',
    });
  });

  it('each kind goes on its feed; roadworks and events are planned', () => {
    const types = qldRecords(
      {
        features: [
          qldEvent({ id: 1, event_type: 'Roadworks', event_subtype: 'Planned roadworks' }),
          qldEvent({ id: 2, event_type: 'Flooding', event_subtype: 'Flash flooding' }),
          qldEvent({ id: 3, event_type: 'Special event', event_subtype: 'N/A' }),
          qldEvent({ id: 4, event_type: 'Hazard', event_subtype: 'Road damage' }),
          qldEvent({ id: 5, event_type: 'Congestion', event_subtype: 'Recurring' }),
        ],
      },
      NOW,
    ).map((r) => [r.properties.type, r.properties.incidentKind, r.properties.title]);
    expect(types).toEqual([
      ['Roadwork', 'Planned', 'Roadworks: Planned roadworks'],
      ['Flood', 'Unplanned', 'Flooding: Flash flooding'],
      ['Major Event', 'Planned', 'Special event'],
      ['Incident', 'Unplanned', 'Hazard: Road damage'],
      ['Incident', 'Unplanned', 'Congestion: Recurring'],
    ]);
  });

  it('a road line: encoded, with the pin at the middle of the longest line', () => {
    const line = [[153.0, -28.0], [153.01, -28.01], [153.02, -28.02]];
    const [r] = qldRecords({ features: [qldEvent({}, { type: 'MultiLineString', coordinates: [line, [[153.5, -28.5], [153.5001, -28.5001]]] })] }, NOW);
    expect(r!.geometry.coordinates).toEqual([153.01, -28.01]);
    expect(decodePolyline(r!.properties.encodedPolyline)).toEqual(line);
    expect(r!.properties.encodedPolylines).toHaveLength(2);
  });

  it("a council's event is credited to the council", () => {
    const [r] = qldRecords({
      features: [qldEvent({ source: { provided_by: 'Isaac Regional Council', provided_by_url: 'https://www.isaac.qld.gov.au/' } })],
    }, NOW);
    expect(r!.properties).toMatchObject({
      orgName: 'Isaac Regional Council',
      orgWebsite: 'https://www.isaac.qld.gov.au/',
      credit: 'Isaac Regional Council, via QLDTraffic (Queensland Department of Transport and Main Roads)',
    });
  });

  it("leaves out Transport for NSW's events, unpublished ones, ended ones and ones with nowhere to pin", () => {
    const out = qldRecords(
      {
        features: [
          qldEvent({ id: 1, source: { provided_by: 'Transport for NSW' } }),
          qldEvent({ id: 2, status: 'Draft' }),
          qldEvent({ id: 3, duration: { start: '2026-10-01T00:00:00+10:00', end: '2026-10-31T00:00:00+10:00' } }),
          qldEvent({ id: 4 }, { type: 'MultiPoint', coordinates: [] }),
          qldEvent({ id: 5, duration: { start: '2026-10-01T00:00:00+10:00', end: '2026-12-01T00:00:00+10:00' } }),
        ],
      },
      NOW,
    );
    expect(out.map((r) => r.properties.id)).toEqual(['qld:5']);
  });
});

// --- SA ----------------------------------------------------------------------

describe('SA', () => {
  const works = (over: Record<string, unknown> = {}) => ({
    attributes: {
      ROADWORKS_AND_INCIDENTS_ID: 'RW1', REC_TYPE: '24HR ROADWORKS', DESCRIPTION: 'Line Marking &amp; Painting',
      LOCAL_ROAD: 'Lonsdale Road', SUBURB: 'HALLETT COVE', SIDE_STREET: 'Patpa Dr', LATITUDE: -35.07, LONGITUDE: 138.5,
      START_DATE: NOW - DAY, END_DATE: NOW + DAY, TRAFFIC_DIR: 'Both Directions', NO_LANES_CLOSED: 1, SPEED_LIMIT: 40, ...over,
    },
  });

  it('roadworks and incidents, in either field case', () => {
    const out = saWorksRecords(
      [
        works(),
        { attributes: { roadworks_and_incidents_id: 'IN1', rec_type: 'INCIDENT', description: 'Ferry Closed', local_road: 'Cooper Creek Ferry Crossing', suburb: 'ETADUNNA', latitude: -28.7, longitude: 138.6, start_date: NOW - DAY, end_date: null, traffic_dir: '[]' } },
      ],
      NOW,
    );
    expect(out.map((r) => r.properties)).toMatchObject([
      {
        id: 'sa:RW1', type: 'Roadwork', mainCategory: '24hr Roadworks', incidentType: 'ROADWORK', incidentKind: 'Planned',
        title: 'Line Marking & Painting', roads: 'Lonsdale Road Hallett Cove', crossStreet: 'Patpa Dr',
        otherAdvice: '1 lane closed', affectedDirection: 'Both Directions', speedLimit: '40', state: 'SA', source: 'traffic_sa',
      },
      { id: 'sa:IN1', type: 'Incident', mainCategory: 'Incident', incidentKind: 'Unplanned', title: 'Ferry Closed', roads: 'Cooper Creek Ferry Crossing Etadunna', affectedDirection: '', end: '' },
    ]);
    expect(out[0]!.properties.credit).toContain('Government of South Australia');
  });

  it('an ended roadwork is left out', () => {
    expect(saWorksRecords([works({ END_DATE: NOW - 1000 })], NOW)).toEqual([]);
  });

  it("closures and detours: their line, the closure's own words, and only while in force", () => {
    const geometry = { paths: [[[138.49, -34.84], [138.495, -34.845], [138.5, -34.85]]] };
    const out = saClosureRecords(
      [
        { attributes: { location_id: '{A2129F14}', plot_type: 'RD_CLOSURE', plot_details: '<p>CAUSEWAY Rd Closed due to Fire</p>', local_road_name: 'CAUSEWAY ROAD', start_suburb: 'Glanville', start_side_street_name1: 'SEMAPHORE RD', end_side_street_name1: 'MILITARY RD', start_date: NOW - DAY, end_date: NOW + DAY }, geometry },
        { attributes: { location_id: '{OLD}', plot_type: 'RD_CLOSURE', plot_details: 'Old', start_date: NOW - 60 * DAY, end_date: NOW - 50 * DAY }, geometry },
        { attributes: { location_id: '{DET}', plot_type: 'RD_DETOUR', local_road_name: 'PORT ROAD', end_date: null }, geometry },
      ],
      NOW,
    );
    expect(out.map((r) => r.properties)).toMatchObject([
      { id: 'sa:closure:A2129F14', type: 'Incident', mainCategory: 'Road closure', incidentType: 'ROAD CLOSURE', title: 'CAUSEWAY Rd Closed due to Fire', roads: 'Causeway Road Glanville', crossStreet: 'Semaphore Rd', secondLocation: 'Military Rd', locationQualifier: 'between' },
      { id: 'sa:closure:DET', mainCategory: 'Detour', incidentType: '', title: 'Port Road detour' },
    ]);
    expect(out[0]!.geometry.coordinates).toEqual([138.495, -34.845]);
    expect(decodePolyline(out[0]!.properties.encodedPolyline)).toHaveLength(3);
  });

  it('the outback report: every section not simply open, closures marked so', () => {
    const geometry = { paths: [[[138.0, -29.0], [138.2, -29.2]]] };
    const out = saOutbackRecords([
      { attributes: { ROAD_SECTION: 'Birdsville Track', AREA_NAME: 'Strzelecki Track, Birdsville Track and nearby areas', COMMENTS: 'Closed due to flooding.', CODE: 5, DESCRIPTION: 'Closed' }, geometry },
      { attributes: { ROAD_SECTION: 'Oodnadatta Track', AREA_NAME: 'Oodnadatta', COMMENTS: '', CODE: 4, DESCRIPTION: '4WD' }, geometry },
      { attributes: { ROAD_SECTION: 'Open road', CODE: 1, DESCRIPTION: 'Open' }, geometry },
    ]);
    expect(out.map((r) => [r.properties.title, r.properties.incidentType, r.properties.type, r.properties.mainCategory])).toEqual([
      ['Birdsville Track: Closed', 'ROAD CLOSURE', 'Incident', 'Outback road conditions'],
      ['Oodnadatta Track: 4WD', '', 'Incident', 'Outback road conditions'],
    ]);
    expect(out[0]!.properties.id).toMatch(/^sa:outback:[0-9a-f]{16}$/);
    expect(out[0]!.properties.region).toBe('Strzelecki Track, Birdsville Track and nearby areas');
  });
});

// --- ACT ---------------------------------------------------------------------

describe('actRecords', () => {
  const plan = (type: string, over: Record<string, unknown> = {}) => ({
    attributes: {
      globalid: `{${type}-id}`, projectTitle: 'TTE22-ACT-2184 - ACT Government - Suburban Land Agency - Whitlam Display Village', type,
      roadsClosed: 'Klaus Moje Street (both directions)', reasonRoadClosure: 'Whitlam display village.', suburb1: 'WHITLAM',
      startTimeClosure: NOW - DAY, endTimeClosure: NOW + DAY, ...over,
    },
    geometry: { x: 149.04, y: -35.27 },
  });

  it('plans: works on the roadwork feed, events on major events, the rest incidents; permit numbers off the title', () => {
    const out = actRecords(
      [plan('roadWorks'), plan('lightRail'), plan('specialEvent'), plan('other'), plan('utilities', { endTimeClosure: NOW - 1 })],
      false,
      NOW,
    );
    expect(out.map((r) => [r.properties.type, r.properties.mainCategory, r.properties.incidentType])).toEqual([
      ['Roadwork', 'Roadworks', 'ROADWORK'],
      ['Roadwork', 'Light rail', 'ROADWORK'],
      ['Major Event', 'Special event', ''],
      ['Incident', 'Road closure', ''],
    ]);
    expect(out[0]!.properties).toMatchObject({
      id: 'act:roadWorks-id', title: 'ACT Government - Suburban Land Agency - Whitlam Display Village',
      roads: 'Klaus Moje Street (both directions)', otherAdvice: 'Whitlam display village.', region: 'Whitlam',
      incidentKind: 'Planned', state: 'ACT', source: 'roads_act', credit: 'ACT Government (Roads ACT)',
    });
  });

  it('unplanned closures are incidents, open-ended ones included', () => {
    const [r] = actRecords([plan('buildingConstruction', { endTimeClosure: null, statusEndTimeClosure: 'untilFurtherNotice', projectTitle: 'Car park closed' })], true, NOW);
    expect(r!.properties).toMatchObject({
      id: 'act:closure:buildingConstruction-id', type: 'Incident', mainCategory: 'Unplanned road closure', subCategory: 'Building construction',
      incidentType: 'ROAD CLOSURE', incidentKind: 'Unplanned', title: 'Car park closed', end: '',
    });
  });
});

// --- VIC ---------------------------------------------------------------------

describe('vicRecords', () => {
  it('unplanned: the v3 shape; inactive ones left out', () => {
    const out = vicRecords(
      [
        {
          type: 'Feature',
          geometry: { type: 'LineString', coordinates: [[145.0, -37.8], [145.1, -37.81], [145.2, -37.82]] },
          properties: {
            id: 'U1', eventId: 'E1', eventLocationStatus: 'Active', status: 'Active', eventType: 'Crash', eventSubType: 'Multi-vehicle',
            closedRoadName: 'Monash Freeway', reference: { startIntersectionLocality: 'Glen Iris', localGovernmentArea: 'Stonnington' },
            impact: { direction: 'Inbound', impactType: 'Lanes closed' }, description: '<p>Two lanes closed</p>',
            created: '2026-11-01T22:00:00Z', lastUpdated: '2026-11-01T23:00:00Z', weblinkURL: 'https://example.vic.gov.au/e1',
          },
        },
        { type: 'Feature', geometry: { type: 'Point', coordinates: [145, -37] }, properties: { id: 'U2', eventLocationStatus: 'Inactive', eventType: 'Hazard' } },
      ],
      false,
      NOW,
    );
    expect(out).toHaveLength(1);
    expect(out[0]!.geometry.coordinates).toEqual([145.1, -37.81]);
    expect(out[0]!.properties).toMatchObject({
      id: 'vic:unplanned:U1', type: 'Incident', mainCategory: 'Crash', incidentType: 'CRASH', incidentKind: 'Unplanned',
      title: 'Crash: Multi-vehicle', roads: 'Monash Freeway Glen Iris', otherAdvice: 'Two lanes closed', affectedDirection: 'Inbound',
      region: 'Stonnington', webLinks: [{ url: 'https://example.vic.gov.au/e1' }], state: 'VIC', source: 'transport_vic',
    });
  });

  it('planned: a geometry collection; roadworks on the roadwork feed, events on major events; ended ones left out', () => {
    const geometry = { type: 'GeometryCollection', geometries: [{ type: 'Point', coordinates: [144.9, -37.7] }, { type: 'LineString', coordinates: [[144.9, -37.7], [144.91, -37.71]] }] };
    const out = vicRecords(
      [
        { type: 'Feature', geometry, properties: { id: 'P1', status: 'Active', eventType: 'Roadworks', eventSubtype: 'Resurfacing', closedRoadName: 'Princes Hwy', startIntersectionLocality: 'Werribee', duration: { start: '2026-11-01T10:00:00Z', end: '2026-11-20T10:00:00Z' }, impact: { delay: 'Minor delays', speedLimitOnSite: '40' } } },
        { type: 'Feature', geometry, properties: { id: 'P2', status: 'Active', eventType: 'Special event', duration: { end: '2026-11-03T10:00:00Z' } } },
        { type: 'Feature', geometry, properties: { id: 'P3', status: 'Active', eventType: 'Roadworks', duration: { end: '2026-10-01T10:00:00Z' } } },
      ],
      true,
      NOW,
    );
    expect(out.map((r) => [r.properties.id, r.properties.type])).toEqual([
      ['vic:planned:P1', 'Roadwork'],
      ['vic:planned:P2', 'Major Event'],
    ]);
    expect(out[0]!.geometry.coordinates).toEqual([144.9, -37.7]);
    expect(out[0]!.properties).toMatchObject({ roads: 'Princes Hwy Werribee', expectedDelay: 'Minor delays', speedLimit: '40', incidentKind: 'Planned', end: '2026-11-20T10:00:00.000Z' });
    expect(out[0]!.properties.encodedPolyline).not.toBe('');
  });
});

// --- Polling, merging, history ------------------------------------------------

const rec = (id: string, type: 'Incident' | 'Roadwork' | 'Flood' | 'Major Event', state: 'QLD' | 'SA' | 'VIC' | 'ACT' = 'QLD') =>
  roadRecord({ id, lon: 153, lat: -27, type, mainCategory: type, title: id, state, source: 'qldtraffic', credit: 'c' });

describe('collectParts', () => {
  beforeEach(() => _resetInterstateMemory());

  it('joins the parts, one record per id', async () => {
    const snap = await collectParts('SA', [
      ['a', async () => [rec('x', 'Roadwork'), rec('y', 'Incident')]],
      ['b', async () => [rec('y', 'Incident'), rec('z', 'Flood')]],
    ]);
    expect(snap.features.map((f) => f.properties.id)).toEqual(['x', 'y', 'z']);
    expect(snap.count).toBe(3);
  });

  it("a failed part answers with its last good records; the others are fresh", async () => {
    await collectParts('SA', [['a', async () => [rec('old-a', 'Roadwork')]], ['b', async () => [rec('old-b', 'Incident')]]]);
    const snap = await collectParts('SA', [
      ['a', async () => { throw new Error('HTTP 503'); }],
      ['b', async () => [rec('new-b', 'Incident')]],
    ]);
    expect(snap.features.map((f) => f.properties.id)).toEqual(['old-a', 'new-b']);
  });

  it('every part failing with nothing to keep fails the poll', async () => {
    await expect(collectParts('ACT', [['a', async () => { throw new Error('HTTP 500'); }]])).rejects.toThrow('HTTP 500');
  });
});

describe('interstateRecords and archiveByType', () => {
  it("every state's records of one type", () => {
    liveStore.set('roads_qld', { type: 'FeatureCollection', features: [rec('q1', 'Roadwork'), rec('q2', 'Flood')], count: 2 });
    liveStore.set('roads_act', { type: 'FeatureCollection', features: [rec('a1', 'Roadwork', 'ACT')], count: 1 });
    try {
      expect(interstateRecords('Roadwork').map((f) => f.properties.id)).toEqual(['q1', 'a1']);
      expect(interstateRecords('Flood').map((f) => f.properties.id)).toEqual(['q2']);
      expect(interstateRecords('Major Event')).toEqual([]);
    } finally {
      liveStore.delete('roads_qld');
      liveStore.delete('roads_act');
    }
  });

  it('history files each record under the NSW feed of its type', () => {
    const rows = archiveByType(
      { type: 'FeatureCollection', features: [rec('r', 'Roadwork'), rec('f', 'Flood'), rec('i', 'Incident'), rec('e', 'Major Event')], count: 4 },
      1_700_000_000,
      'roads_qld',
    );
    expect(rows.map((r) => [r.source_id, r.source])).toEqual([
      ['r', 'traffic_roadwork'],
      ['f', 'traffic_flood'],
      ['i', 'traffic_incident'],
      ['e', 'traffic_majorevent'],
    ]);
  });
});

describe('fetching', () => {
  beforeEach(() => {
    _resetInterstateMemory();
    fetchJsonMock.mockReset();
  });

  it('QLD reads its one file', async () => {
    const { fetchQldRoads, QLD_EVENTS_URL } = await import('../../../src/sources/interstateRoads.js');
    fetchJsonMock.mockResolvedValueOnce({ features: [qldEvent()] });
    const snap = await fetchQldRoads();
    expect(fetchJsonMock.mock.calls[0]![0]).toBe(QLD_EVENTS_URL);
    expect(snap.count).toBe(1);
  });

  it('an ArcGIS layer is read page by page while the server says there is more', async () => {
    const { fetchActRoads } = await import('../../../src/sources/interstateRoads.js');
    const feat = (id: string) => ({ attributes: { globalid: id, type: 'roadWorks', projectTitle: id, endTimeClosure: null }, geometry: { x: 149, y: -35 } });
    fetchJsonMock.mockImplementation(async (url: string) => {
      if (url.includes('Public_Temporary_Road_Closures')) return { features: [] };
      return url.includes('resultOffset=0') ? { features: [feat('a')], exceededTransferLimit: true } : { features: [feat('b')] };
    });
    const snap = await fetchActRoads();
    expect(snap.features.map((f) => f.properties.id).sort()).toEqual(['act:a', 'act:b']);
    const planUrls = fetchJsonMock.mock.calls.map((c) => String(c[0])).filter((u) => u.includes('Road_Closures_public_view'));
    expect(planUrls).toHaveLength(2);
    expect(planUrls[0]).toContain('outSR=4326');
  });

  it('VIC falls back to the Live Traffic aggregate while Transport Victoria refuses the key', async () => {
    const { fetchVicRoads } = await import('../../../src/sources/interstateRoads.js');
    const { config } = await import('../../../src/config.js');
    const held = config.VIC_TRANSPORT_API_KEY;
    (config as { VIC_TRANSPORT_API_KEY?: string }).VIC_TRANSPORT_API_KEY = 'not-the-subscription-key';
    try {
      fetchJsonMock.mockImplementation(async (url: string) => {
        if (url.includes('opendata.transport.vic.gov.au')) throw new Error(`HTTP 401 for ${url}`);
        return [];
      });
      const snap = await fetchVicRoads();
      expect(snap.count).toBe(0);
      const urls = fetchJsonMock.mock.calls.map((c) => String(c[0]));
      expect(urls.some((u) => u.includes('opendata.transport.vic.gov.au'))).toBe(true);
      expect(urls.some((u) => u.includes('livetraffic.com'))).toBe(true);
    } finally {
      (config as { VIC_TRANSPORT_API_KEY?: string }).VIC_TRANSPORT_API_KEY = held;
    }
  });
});
