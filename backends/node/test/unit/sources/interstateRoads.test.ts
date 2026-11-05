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
  // DIT's road drawing for SA's roadworks: not to be had here, so jobs come without lines.
  fetchBuffer: vi.fn(async () => {
    throw new Error('no network in tests');
  }),
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
  saJobRecords,
  saOutbackRecords,
  saWorksRecords,
  vicRecords,
  vicPlannedRecords,
  vicWorkingNow,
  vicTime,
  _resetInterstateMemory,
} from '../../../src/sources/interstateRoads.js';
import { decodePolyline, encodePolyline } from '../../../src/sources/shared/polyline.js';
import { parseSaRoads } from '../../../src/sources/saRoads.js';
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

  it('the incidents layer: its direction list and "No Restriction" read as words', () => {
    const [r] = saWorksRecords(
      [works({ REC_TYPE: 'INCIDENT', TRAFFIC_DIR: '[{"direction": "SOUTHBOUND", "lanes_closed": 0}, {"direction": "NORTHBOUND", "lanes_closed": 0}]', NO_LANES_CLOSED: null, SPEED_LIMIT: 'No Restriction' })],
      NOW,
    );
    expect(r!.properties).toMatchObject({ affectedDirection: 'Southbound, Northbound', speedLimit: '', otherAdvice: '' });
  });

  // The roadworks layer lists a job site by site, as SA sends it: lanes and speed as words.
  const site = (over: Record<string, unknown> = {}) =>
    works({ ROADWORKS_AND_INCIDENTS_ID: '3441713417', GIS_LINK_ID: 99993441713417, ESRI_OID: 1, NO_LANES_CLOSED: 'Shuttle Flow', SPEED_LIMIT: '40', ...over });
  const hallett = site({ SIDE_STREET: 'Patpa Dr', LATITUDE: -35.07, LONGITUDE: 138.5 });
  const middle = site({ SIDE_STREET: 'Zwerner Dr', LATITUDE: -35.08, LONGITUDE: 138.51, END_DATE: NOW + 2 * DAY });
  const sheidow = site({ SIDE_STREET: 'Majors Rd', SUBURB: 'SHEIDOW PARK', LATITUDE: -35.09, LONGITUDE: 138.52, NO_LANES_CLOSED: 'One Lane', SPEED_LIMIT: '60' });

  it('the roadworks layer: one record per job, pinned at its middle site, its ends named', () => {
    const out = saJobRecords([hallett, middle, sheidow], NOW);
    expect(out).toHaveLength(1);
    expect(out[0]!.geometry.coordinates).toEqual([138.51, -35.08]);
    expect(out[0]!.properties).toMatchObject({
      type: 'Roadwork', mainCategory: '24hr Roadworks', incidentType: 'ROADWORK', incidentKind: 'Planned', title: 'Line Marking & Painting',
      roads: 'Lonsdale Road, Sheidow Park to Hallett Cove', crossStreet: 'Majors Rd', secondLocation: 'Patpa Dr', locationQualifier: 'between',
      otherAdvice: '3 sites over 3 km. Shuttle flow at 2, one lane closed at 1.', affectedDirection: 'Both Directions', speedLimit: '40',
      start: new Date(NOW - DAY).toISOString(), end: new Date(NOW + 2 * DAY).toISOString(), state: 'SA', source: 'traffic_sa',
    });
    expect(out[0]!.properties.id).toMatch(/^sa:job:[0-9a-f]{16}$/);
    expect(out[0]!.properties.sites).toEqual([
      { lat: -35.09, lon: 138.52, crossStreet: 'Majors Rd', suburb: 'Sheidow Park', direction: 'Both Directions', lanes: 'one lane closed', speedLimit: '60' },
      { lat: -35.08, lon: 138.51, crossStreet: 'Zwerner Dr', suburb: 'Hallett Cove', direction: 'Both Directions', lanes: 'shuttle flow', speedLimit: '40' },
      { lat: -35.07, lon: 138.5, crossStreet: 'Patpa Dr', suburb: 'Hallett Cove', direction: 'Both Directions', lanes: 'shuttle flow', speedLimit: '40' },
    ]);
  });

  it("a job keeps its id across SA's reloads, a moved end, changed lanes and an added site", () => {
    const [first] = saJobRecords([hallett, middle, sheidow], NOW);
    // Reloaded: every number reissued, another order; the end moved out, a site's lanes changed, a site added.
    const reload = (s: ReturnType<typeof site>, n: number, over: Record<string, unknown> = {}) => ({
      attributes: { ...s.attributes, ROADWORKS_AND_INCIDENTS_ID: `34417165${n}`, GIS_LINK_ID: 9999344171650 + n, ESRI_OID: 70 + n, ...over },
    });
    const [again] = saJobRecords(
      [reload(sheidow, 1), reload(hallett, 2, { END_DATE: NOW + 9 * DAY }), reload(middle, 3, { NO_LANES_CLOSED: 'Contraflow' }), site({ SIDE_STREET: 'Lonsdale Hwy', LATITUDE: -35.1 })],
      NOW,
    );
    expect(again!.properties.id).toBe(first!.properties.id);
    expect(again!.properties.sites).toHaveLength(4);
    expect(again!.properties.end).toBe(new Date(NOW + 9 * DAY).toISOString());
    // Another start is another job.
    expect(saJobRecords([site({ START_DATE: NOW - 2 * DAY })], NOW)[0]!.properties.id).not.toBe(first!.properties.id);
  });

  it('sites that restrict nothing, or have ended, are left out, and a job with none left goes', () => {
    const free = { NO_LANES_CLOSED: 'None', SPEED_LIMIT: 'No Restriction' };
    const out = saJobRecords(
      [
        hallett,
        site({ ...free, SIDE_STREET: 'Free St', LATITUDE: -35.2 }),
        site({ SIDE_STREET: 'Done St', LATITUDE: -35.3, END_DATE: NOW - 1000 }),
        site({ ...free, DESCRIPTION: 'Mowing', SIDE_STREET: 'Grass Rd' }),
        site({ NO_LANES_CLOSED: 'None', DESCRIPTION: 'Vms Trailer', SIDE_STREET: 'Sign Rd' }),
      ],
      NOW,
    );
    expect(out.map((r) => [r.properties.title, r.properties.sites!.map((s) => s.crossStreet), r.properties.otherAdvice])).toEqual([
      ['Line Marking & Painting', ['Patpa Dr'], 'Shuttle flow'],
      ['Vms Trailer', ['Sign Rd'], '40 km/h limit'],
    ]);
  });

  // Lonsdale Road as DIT might draw it: 4 km on a straight line through the three sites (1, 2 and 3 km along).
  const lonsdale = parseSaRoads([
    {
      features: [
        { properties: { gis_road_no: '03400', side: 'U/L', start_rrd: 0, end_rrd: 4 }, geometry: { type: 'MultiLineString', coordinates: [[[138.49, -35.06], [138.53, -35.1]]] } },
      ],
    },
  ]);
  const onRoad = (s: ReturnType<typeof site>) => ({ attributes: { ...s.attributes, ROAD_NO: 3400 } });

  it('on a road DIT draws: sites in order along it, and the road between the ends as its line', () => {
    const [job] = saJobRecords([onRoad(sheidow), onRoad(hallett), onRoad(middle)], NOW, lonsdale);
    const p = job!.properties;
    expect(p.sites!.map((s) => s.crossStreet)).toEqual(['Patpa Dr', 'Zwerner Dr', 'Majors Rd']);
    expect(p).toMatchObject({ roads: 'Lonsdale Road, Hallett Cove to Sheidow Park', crossStreet: 'Patpa Dr', secondLocation: 'Majors Rd', otherAdvice: '3 sites over 2 km. Shuttle flow at 2, one lane closed at 1.' });
    const line = decodePolyline(p.encodedPolyline);
    expect(line[0]![0]).toBeCloseTo(138.5, 4);
    expect(line[line.length - 1]![0]).toBeCloseTo(138.52, 4);
    expect(p.encodedPolylines).toHaveLength(1);
  });

  it('no line for sites all at one spot, or off every road DIT draws', () => {
    const [here] = saJobRecords([onRoad(hallett), onRoad(site({ NO_LANES_CLOSED: 'One Lane' }))], NOW, lonsdale);
    expect(here!.properties.sites).toHaveLength(2);
    expect(here!.properties.encodedPolylines).toEqual([]);
    const [elsewhere] = saJobRecords([hallett, middle], NOW, lonsdale);
    expect(elsewhere!.properties.encodedPolylines).toEqual([]);
  });

  it('a site off the road: the line covers the sites on it, the order is by distance', () => {
    const far = onRoad(site({ SIDE_STREET: 'Far Rd', LATITUDE: -35.2, LONGITUDE: 138.52 }));
    const [job] = saJobRecords([onRoad(hallett), onRoad(middle), far], NOW, lonsdale);
    expect(job!.properties.sites!.map((s) => s.crossStreet)).toEqual(['Far Rd', 'Zwerner Dr', 'Patpa Dr']);
    const line = decodePolyline(job!.properties.encodedPolyline);
    expect([line[0]![0], line[line.length - 1]![0]].map((x) => Number(x.toFixed(4)))).toEqual([138.5, 138.51]);
  });

  it("the layer's incidents stay records of their own, by incident number", () => {
    const out = saJobRecords(
      [hallett, site({ REC_TYPE: 'INCIDENT', INCIDENT_ID: 87254, DESCRIPTION: 'Ferry Closed', LOCAL_ROAD: 'Birdsville Track', SUBURB: null, NO_LANES_CLOSED: null, SPEED_LIMIT: null, END_DATE: null })],
      NOW,
    );
    expect(out.map((r) => [r.properties.id.replace(/[0-9a-f]{16}$/, '…'), r.properties.type, r.properties.title])).toEqual([
      ['sa:job:…', 'Roadwork', 'Line Marking & Painting'],
      ['sa:incident:87254', 'Incident', 'Ferry Closed'],
    ]);
    expect(out[1]!.properties.sites).toBeUndefined();
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

describe('VIC planned: working hours', () => {
  // Melbourne is UTC+11 in November (AEDT).
  const mel = (iso: string): number => Date.parse(`${iso}+11:00`);
  const weekdays = (startTime: string, duration: string) =>
    ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'].map((startDay) => ({ startDay, daysDuration: 1, startTime, duration, allDay: false }));

  it('reads a time without an offset as Melbourne local', () => {
    expect(vicTime('2026-11-03T09:30:00')).toBe(mel('2026-11-03T09:30:00'));
    expect(vicTime('2026-11-03T09:30:00Z')).toBe(Date.parse('2026-11-03T09:30:00Z'));
    // Winter: AEST, UTC+10.
    expect(vicTime('2026-07-01T12:00:00')).toBe(Date.parse('2026-07-01T12:00:00+10:00'));
  });

  it('a day window: inside 9:30 AM + 6 h on a weekday, not after or at the weekend', () => {
    const d = { start: '2026-01-01T00:00:00', end: '2027-01-01T00:00:00', recurrences: weekdays('9:30 AM', 'PT6H0M') };
    expect(vicWorkingNow(d, mel('2026-11-03T10:00:00'))).toBe(true); // Tuesday
    expect(vicWorkingNow(d, mel('2026-11-03T15:31:00'))).toBe(false);
    expect(vicWorkingNow(d, mel('2026-11-07T10:00:00'))).toBe(false); // Saturday
  });

  it('a night window written negative runs into the next morning', () => {
    // 9 PM with PT-15H0M is 9 PM to 6 AM.
    const d = { start: '2026-01-01T00:00:00', end: '2027-01-01T00:00:00', recurrences: weekdays('9:00 PM', 'PT-15H0M') };
    expect(vicWorkingNow(d, mel('2026-11-03T23:00:00'))).toBe(true);
    expect(vicWorkingNow(d, mel('2026-11-04T05:59:00'))).toBe(true); // Tuesday night's shift
    expect(vicWorkingNow(d, mel('2026-11-04T06:01:00'))).toBe(false);
    expect(vicWorkingNow(d, mel('2026-11-04T12:00:00'))).toBe(false);
    // Friday night's shift reaches Saturday morning; there is no Saturday night one.
    expect(vicWorkingNow(d, mel('2026-11-07T02:00:00'))).toBe(true);
    expect(vicWorkingNow(d, mel('2026-11-07T23:00:00'))).toBe(false);
  });

  it('all day, PT0H0M, and no windows at all count the whole day', () => {
    const at = mel('2026-11-03T03:00:00');
    const range = { start: '2026-01-01T00:00:00', end: '2027-01-01T00:00:00' };
    expect(vicWorkingNow({ ...range, recurrences: [{ startDay: 'Tuesday', daysDuration: 1, allDay: true }] }, at)).toBe(true);
    expect(vicWorkingNow({ ...range, recurrences: [{ startDay: 'Tuesday', daysDuration: 1, startTime: '12:00 AM', duration: 'PT0H0M', allDay: false }] }, at)).toBe(true);
    expect(vicWorkingNow(range, at)).toBe(true);
  });

  it('outside its dates it is not working, whatever the hour', () => {
    const recurrences = [{ startDay: 'Tuesday', daysDuration: 1, allDay: true }];
    expect(vicWorkingNow({ start: '2026-12-01T00:00:00', end: '2027-01-01T00:00:00', recurrences }, mel('2026-11-03T10:00:00'))).toBe(false);
    expect(vicWorkingNow({ start: '2025-01-01T00:00:00', end: '2026-11-01T00:00:00', recurrences }, mel('2026-11-03T10:00:00'))).toBe(false);
  });
});

describe('vicPlannedRecords', () => {
  const at = Date.parse('2026-11-03T23:00:00+11:00'); // Tuesday 11 PM in Melbourne
  const night = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'].map((startDay) => ({ startDay, daysDuration: 1, startTime: '9:00 PM', duration: 'PT-15H0M', allDay: false }));
  const day = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'].map((startDay) => ({ startDay, daysDuration: 1, startTime: '9:30 AM', duration: 'PT6H0M', allDay: false }));
  const dur = (recurrences: unknown[]) => ({ start: '2026-01-01T00:00:00', end: '2027-01-01T00:00:00', recurrences });
  const stretch = (id: string, lon: number, props: Record<string, unknown> = {}) => ({
    type: 'Feature',
    geometry: { type: 'GeometryCollection', geometries: [{ type: 'LineString', coordinates: [[lon, -37.8], [lon + 0.01, -37.81]] }] },
    properties: {
      id, status: 'Pending', eventType: 'Roadworks', eventSubtype: 'Planned roadworks', eventDueTo: 'Maintenance Works',
      closedRoadName: 'MONASH FREEWAY ', startIntersectionLocality: 'GLEN IRIS', endIntersectionLocality: 'CHADSTONE',
      description: 'Resurfacing. Lanes closed overnight.', impact: { impactType: 'Closures', speedLimitOnSite: '40' },
      duration: dur(night),
      ...props,
    },
  });

  it('keeps closures inside their hours, one record per job with every stretch drawn', () => {
    const out = vicPlannedRecords(
      [
        stretch('A', 145.0),
        stretch('B', 145.1, { startIntersectionLocality: 'CHADSTONE', endIntersectionLocality: 'MULGRAVE' }),
        stretch('C', 145.2, { impact: { impactType: 'Lanes blocked' } }), // not a closure
        stretch('D', 145.3, { closedRoadName: 'PUNT ROAD', duration: dur(day) }), // outside its hours
      ],
      at,
    );
    expect(out).toHaveLength(1);
    const p = out[0]!.properties;
    expect(p.id).toMatch(/^vic:planned:job:[0-9a-f]{16}$/);
    expect(p.encodedPolylines).toHaveLength(2);
    expect(p.roads).toBe('Monash Freeway, Glen Iris to Mulgrave');
    expect(p.otherAdvice).toBe('2 stretches of road. Resurfacing. Lanes closed overnight.');
    expect(p).toMatchObject({ type: 'Roadwork', state: 'VIC', incidentKind: 'Planned' });
  });

  it("a job keeps its id when one of its stretches drops out", () => {
    const both = vicPlannedRecords([stretch('A', 145.0), stretch('B', 145.1)], at);
    const one = vicPlannedRecords([stretch('B', 145.1)], at);
    expect(one[0]!.properties.id).toBe(both[0]!.properties.id);
  });

  it('the same layer by day leaves the night works out', () => {
    expect(vicPlannedRecords([stretch('A', 145.0)], Date.parse('2026-11-04T12:00:00+11:00'))).toHaveLength(0);
  });

  it('special events stay one record each while they are on, closure or not', () => {
    const out = vicPlannedRecords(
      [stretch('E', 145.0, { eventType: 'Special event', impact: { impactType: 'Lanes affected' } })],
      at,
    );
    expect(out.map((r) => [r.properties.id, r.properties.type])).toEqual([['vic:planned:E', 'Major Event']]);
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

  it("SA reads its roadworks layer in an order of what the records say, one record per job", async () => {
    const { fetchSaRoads, SA_ROADWORKS_URL } = await import('../../../src/sources/interstateRoads.js');
    const site = (side: string, lat: number) => ({
      attributes: { REC_TYPE: '24HR ROADWORKS', DESCRIPTION: 'Asphalt Works', LOCAL_ROAD: 'Barrier Highway', SUBURB: 'BURRA', SIDE_STREET: side, LATITUDE: lat, LONGITUDE: 138.9, START_DATE: Date.now() - DAY, END_DATE: Date.now() + DAY, NO_LANES_CLOSED: 'Shuttle Flow', SPEED_LIMIT: '40' },
    });
    fetchJsonMock.mockImplementation(async (url: string) => (url.startsWith(SA_ROADWORKS_URL) ? { features: [site('A St', -33.6), site('B St', -33.7)] } : { features: [] }));
    const snap = await fetchSaRoads();
    expect(snap.features.map((f) => [f.properties.id.replace(/[0-9a-f]{16}$/, '…'), f.properties.sites?.length])).toEqual([['sa:job:…', 2]]);
    const url = String(fetchJsonMock.mock.calls.find((c) => String(c[0]).startsWith(SA_ROADWORKS_URL))![0]);
    expect(new URL(url).searchParams.get('orderByFields')).toMatch(/^START_DATE,/);
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
