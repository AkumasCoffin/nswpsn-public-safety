/**
 * Unit tests for the Live Traffic NSW source.
 *
 * We don't test fetchHazard end-to-end because that lives behind the
 * `kind` config inside register(). Instead we exercise parseTrafficItem
 * (the core parser) and the camera fetcher with mocked fetchJson.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchJsonMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

describe('traffic.parseTrafficItem', () => {
  it('returns a feature with all parsed fields', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    const item = {
      id: 'inc-1',
      geometry: { type: 'Point', coordinates: [151.0, -33.5] },
      properties: {
        headline: 'CRASH Single vehicle',
        mainCategory: 'Incident',
        subCategory: 'Crash',
        roads: [{ mainStreet: 'Pacific Hwy', suburb: 'Hornsby', affectedDirection: 'North' }],
        impactedLanes: ['Left'],
        speedLimit: '60',
        ended: false,
      },
    };
    const f = parseTrafficItem(item, 'Incident');
    expect(f).not.toBeNull();
    if (!f) throw new Error('no feature');
    expect(f.geometry.coordinates).toEqual([151.0, -33.5]);
    expect(f.properties.id).toBe('inc-1');
    expect(f.properties.type).toBe('Incident');
    expect(f.properties.incidentType).toBe('CRASH');
    expect(f.properties.title).toBe('Single vehicle');
    expect(f.properties.roads).toBe('Pacific Hwy Hornsby');
    expect(f.properties.affectedDirection).toBe('North');
    expect(f.properties.impactedLanes).toEqual(['Left']);
    expect(f.properties.speedLimit).toBe('60');
    expect(f.properties.isEnded).toBe(false);
    expect(f.properties.source).toBe('livetraffic');
  });

  it('returns null when the item has no coordinates', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    expect(parseTrafficItem({ id: 'x' }, 'Incident')).toBeNull();
  });

  it('accepts lat/lng fallback fields', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    const f = parseTrafficItem({ id: 'y', lat: -33, lng: 151 }, 'Roadwork');
    expect(f).not.toBeNull();
    expect(f?.geometry.coordinates).toEqual([151, -33]);
  });
});

describe('traffic.extractIncidentType', () => {
  it('matches longer prefixes before shorter ones', async () => {
    const { extractIncidentType } = await import('../../../src/sources/traffic.js');
    expect(extractIncidentType('CHANGED TRAFFIC CONDITIONS Foo Rd').incidentType).toBe(
      'CHANGED TRAFFIC CONDITIONS',
    );
    expect(extractIncidentType('FLOODING on highway').incidentType).toBe('FLOODING');
    expect(extractIncidentType('CRASH at intersection').incidentType).toBe('CRASH');
  });

  it('returns empty type when no prefix matches', async () => {
    const { extractIncidentType } = await import('../../../src/sources/traffic.js');
    const r = extractIncidentType('A random title');
    expect(r.incidentType).toBe('');
    expect(r.cleanTitle).toBe('A random title');
  });
});

describe('traffic.fetchTrafficCameras', () => {
  beforeEach(() => fetchJsonMock.mockReset());

  it('filters down to camera-shaped entries', async () => {
    fetchJsonMock.mockResolvedValueOnce([
      {
        id: 'cam-1',
        eventType: 'liveCams',
        geometry: { type: 'Point', coordinates: [150.5, -33.0] },
        properties: {
          title: 'M1 northbound',
          href: 'https://example.com/cam.jpg',
          direction: 'North',
        },
      },
      {
        id: 'not-a-cam',
        eventType: 'incident',
        geometry: { type: 'Point', coordinates: [150, -33] },
        properties: {},
      },
    ]);
    const { fetchTrafficCameras } = await import('../../../src/sources/traffic.js');
    const out = await fetchTrafficCameras();
    expect(out.count).toBe(1);
    const f = out.features[0];
    expect(f).toBeDefined();
    if (!f) throw new Error('no f');
    expect(f.properties.id).toBe('cam-1');
    expect(f.properties.title).toBe('M1 northbound');
    expect(f.properties.imageUrl).toBe('https://example.com/cam.jpg');
  });

  it('throws on upstream error', async () => {
    fetchJsonMock.mockRejectedValueOnce(new Error('500'));
    const { fetchTrafficCameras } = await import('../../../src/sources/traffic.js');
    await expect(fetchTrafficCameras()).rejects.toThrow('500');
  });
});

describe('traffic.parseTrafficItem — real upstream field names', () => {
  // Upstream does not use the key names an earlier version of the parser
  // looked for. These lock in the shapes the live feed actually sends.
  const realShape = {
    id: 225630,
    geometry: { type: 'Point', coordinates: [151.1448757, -33.9613516] },
    properties: {
      headline: '',
      displayName: 'Crash on West Botany Street',
      mainCategory: 'Incident',
      // never `subCategory`
      subCategoryA: '2 cars',
      subCategoryB: '',
      // an ARRAY of objects, never a bare string
      encodedPolylines: [{ levels: '', direction: 'BOTH_DIRECTIONS', coords: 'fpsxEae{h[PC' }],
      arrangementElements: ['detour'],
      roads: [{
        mainStreet: 'West Botany Street', suburb: 'Rockdale', region: 'Sydney',
        crossStreet: 'French Street', secondLocation: 'Bermill Street',
        locationQualifier: 'between', queueLength: 250,
        impactedLanes: ['Left lane'], conditionTendency: 'Easing',
      }],
      isLocalRoad: 'State road',
    },
  };

  it('reads subCategory from subCategoryA', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    expect(parseTrafficItem(realShape, 'Incident')?.properties.subCategory).toBe('2 cars');
  });

  it('decodes the encodedPolylines array instead of dropping it', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    const f = parseTrafficItem(realShape, 'Incident')!;
    // asString() on an array used to yield '' and lose the road geometry.
    expect(f.properties.encodedPolyline).toBe('fpsxEae{h[PC');
    expect(f.properties.encodedPolylines).toEqual([
      { coords: 'fpsxEae{h[PC', direction: 'BOTH_DIRECTIONS' },
    ]);
  });

  it('lifts impactedLanes and location detail out of roads[]', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    const p = parseTrafficItem(realShape, 'Incident')!.properties;
    expect(p.impactedLanes).toEqual(['Left lane']);
    expect(p.region).toBe('Sydney');
    expect(p.crossStreet).toBe('French Street');
    expect(p.secondLocation).toBe('Bermill Street');
    expect(p.locationQualifier).toBe('between');
    expect(p.queueLength).toBe(250);
    expect(p.isLocalRoad).toBe('State road');
  });

  it('joins every road rather than only the first', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    const multi = {
      ...realShape,
      properties: {
        ...realShape.properties,
        roads: [
          { mainStreet: 'A St', suburb: 'Alpha' },
          { mainStreet: 'B St', suburb: 'Beta' },
        ],
      },
    };
    expect(parseTrafficItem(multi, 'Incident')?.properties.roads).toBe('A St Alpha; B St Beta');
  });

  it('carries council attribution from the LGA feed', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    const lga = {
      ...realShape,
      properties: {
        ...realShape.properties,
        OrgName: 'MidCoast Council',
        OrgContact: '02 7955 7777',
        OrgEmail: 'council@midcoast.nsw.gov.au',
        OrgWebsite: 'https://www.midcoast.nsw.gov.au',
      },
    };
    const p = parseTrafficItem(lga, 'Council')!.properties;
    expect(p.orgName).toBe('MidCoast Council');
    expect(p.orgEmail).toBe('council@midcoast.nsw.gov.au');
  });
});

describe('traffic.fetchTrafficWorks', () => {
  beforeEach(() => fetchJsonMock.mockReset());

  it('keeps works, drops cameras, rest areas and council registry rows', async () => {
    const { fetchTrafficWorks } = await import('../../../src/sources/traffic.js');
    fetchJsonMock.mockResolvedValueOnce([
      // ACT light-rail closure — the kind of record only this feed has.
      { id: '1', eventCategory: 'lightRail', eventType: 'Roadwork',
        geometry: { type: 'POINT', coordinates: [149.13, -35.28] },
        properties: { title: 'Gordon St Closure', displayName: 'Light Rail Stage 2A' } },
      // Cameras have their own source.
      { id: '2', eventCategory: 'liveCams', eventType: 'liveCams',
        geometry: { type: 'Point', coordinates: [151, -33] }, properties: {} },
      // Static amenities, not road events.
      { id: '3', eventCategory: 'restAreas', eventType: 'restAreas',
        geometry: { type: 'Point', coordinates: [151, -33] }, properties: {} },
      // Council registry metadata sitting at Null Island.
      { id: '4', eventCategory: 'LGACouncilParticipation', type: 'participatingLGA',
        geometry: { type: '', coordinates: [0, 0] }, properties: { name: 'Albury City Council' } },
    ]);
    const snap = await fetchTrafficWorks();
    expect(snap.count).toBe(1);
    expect(snap.features[0]?.properties.upstreamId).toBe('1');
    expect(snap.features[0]?.properties.mainCategory).toBe('lightRail');
  });

  it('accepts POINT as well as Point geometry casing', async () => {
    const { fetchTrafficWorks } = await import('../../../src/sources/traffic.js');
    fetchJsonMock.mockResolvedValueOnce([
      { id: 'a', eventCategory: 'Roadwork', geometry: { type: 'POINT', coordinates: [151, -33] }, properties: { title: 'x' } },
      { id: 'b', eventCategory: 'Roadwork', geometry: { type: 'Point', coordinates: [152, -34] }, properties: { title: 'y' } },
    ]);
    expect((await fetchTrafficWorks()).count).toBe(2);
  });

  it("treats the string 'True' in `ended` as ended", async () => {
    const { fetchTrafficWorks } = await import('../../../src/sources/traffic.js');
    // This feed is Python-serialised: booleans arrive as 'False'/'True'.
    fetchJsonMock.mockResolvedValueOnce([
      { id: 'a', eventCategory: 'Roadwork', geometry: { type: 'Point', coordinates: [151, -33] }, properties: { title: 'x', ended: 'True' } },
      { id: 'b', eventCategory: 'Roadwork', geometry: { type: 'Point', coordinates: [152, -34] }, properties: { title: 'y', ended: 'False' } },
    ]);
    const snap = await fetchTrafficWorks();
    expect(snap.features.map((f) => f.properties.upstreamId)).toEqual(['b']);
  });
});

describe('traffic advice text', () => {
  it('strips the HTML upstream embeds in advice fields', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    const f = parseTrafficItem({
      id: 'x',
      geometry: { type: 'Point', coordinates: [151, -33] },
      properties: {
        displayName: 'Roadwork',
        // Councils submit advice as HTML; it used to render as literal tags.
        otherAdvice: '<p>A section of Miners&nbsp;Road is closed.<br/>Use Bungendore Rd &amp; detour.</p>',
      },
    }, 'Council');
    expect(f?.properties.otherAdvice).toBe(
      'A section of Miners Road is closed. Use Bungendore Rd & detour.',
    );
  });
});

describe('traffic works ids are stable across upstream rebuilds', () => {
  beforeEach(() => fetchJsonMock.mockReset());

  const work = (upstreamId) => ({
    id: upstreamId,
    eventCategory: 'Roadwork',
    geometry: { type: 'Point', coordinates: [151.12345, -33.54321] },
    properties: { title: 'Shoulder Work' },
  });

  it('derives the same id when upstream regenerates its surrogate key', async () => {
    const { fetchTrafficWorks } = await import('../../../src/sources/traffic.js');
    // Measured upstream behaviour: an unchanged work reappears under a new
    // id on each rebuild, so archiving on it would fabricate a new incident
    // every poll.
    fetchJsonMock.mockResolvedValueOnce([work('3322871441')]);
    const first = await fetchTrafficWorks();
    fetchJsonMock.mockResolvedValueOnce([work('3322908764')]);
    const second = await fetchTrafficWorks();

    expect(first.features[0]?.properties.id).toBe(second.features[0]?.properties.id);
    expect(first.features[0]?.properties.id).toMatch(/^ltw:[0-9a-f]{20}$/);
    // The volatile key is kept, just not used as identity.
    expect(first.features[0]?.properties.upstreamId).toBe('3322871441');
    expect(second.features[0]?.properties.upstreamId).toBe('3322908764');
  });

  it('gives genuinely different works different ids', async () => {
    const { fetchTrafficWorks } = await import('../../../src/sources/traffic.js');
    fetchJsonMock.mockResolvedValueOnce([
      work('1'),
      { ...work('2'), geometry: { type: 'Point', coordinates: [152.0, -33.0] } },
    ]);
    const snap = await fetchTrafficWorks();
    expect(new Set(snap.features.map((f) => f.properties.id)).size).toBe(2);
  });
});

describe('traffic archive facet (lowercase subcategory)', () => {
  it('falls back to mainCategory when no incident-type prefix matches', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    // Council rows title themselves 'SCHEDULED ROADWORK' with no known
    // prefix, which used to leave 190 of 482 rows with no facet at all.
    const f = parseTrafficItem({
      id: 'lga-1',
      geometry: { type: 'Point', coordinates: [151, -33] },
      properties: { displayName: 'Miners Road, Captains Flat', mainCategory: 'SCHEDULED ROADWORK' },
    }, 'Council');
    expect(f?.properties.subcategory).toBe('SCHEDULED ROADWORK');
  });

  it('keeps the incident-type token when one is present', async () => {
    const { parseTrafficItem } = await import('../../../src/sources/traffic.js');
    const f = parseTrafficItem({
      id: 'inc-1',
      geometry: { type: 'Point', coordinates: [151, -33] },
      properties: { displayName: 'CRASH Pacific Hwy', mainCategory: 'CRASH' },
    }, 'Incident');
    // Dashboard/bot subtype filters key on these tokens — must not change.
    expect(f?.properties.subcategory).toBe('CRASH');
  });
});

describe('traffic works drops RFS re-publications', () => {
  beforeEach(() => fetchJsonMock.mockReset());

  it('skips static infrastructure — checking stations and rest areas', async () => {
    const { fetchTrafficWorks } = await import('../../../src/sources/traffic.js');
    fetchJsonMock.mockResolvedValueOnce([
      { id: 'h1', eventCategory: 'hvcs', eventType: 'hvcs',
        geometry: { type: 'Point', coordinates: [150.3, -33.5] },
        properties: { heading: 'Heavy vehicle checking station' } },
      { id: 'r1', eventCategory: 'restAreas', eventType: 'restAreas',
        geometry: { type: 'Point', coordinates: [151, -33] }, properties: {} },
      { id: 'w1', eventCategory: 'Roadwork', eventType: 'Roadwork',
        geometry: { type: 'Point', coordinates: [151, -33] }, properties: { title: 'Sewer works' } },
    ]);
    const snap = await fetchTrafficWorks();
    expect(snap.count).toBe(1);
    expect(snap.features[0]?.properties.upstreamId).toBe('w1');
  });

  it("skips eventType 'Fire' items — the rfs source already has them", async () => {
    const { fetchTrafficWorks } = await import('../../../src/sources/traffic.js');
    fetchJsonMock.mockResolvedValueOnce([
      { id: '1', eventCategory: 'Not Applicable', eventType: 'Fire',
        geometry: { type: 'Point', coordinates: [151, -33] }, properties: { title: 'Grass fire' } },
      { id: '2', eventCategory: 'Roadwork', eventType: 'Roadwork',
        geometry: { type: 'Point', coordinates: [151, -33] }, properties: { title: 'Sewer works' } },
    ]);
    const snap = await fetchTrafficWorks();
    expect(snap.count).toBe(1);
    expect(snap.features[0]?.properties.upstreamId).toBe('2');
    // and works' facet key is its category
    expect(snap.features[0]?.properties.subcategory).toBe('Roadwork');
  });
});

describe('traffic works survives an upstream apiSource dropout', () => {
  beforeEach(async () => {
    fetchJsonMock.mockReset();
    const { _resetWorksCarryForward } = await import('../../../src/sources/traffic.js');
    _resetWorksCarryForward();
  });

  const rec = (id, apiSource, lon) => ({
    id, apiSource, eventCategory: 'Roadwork',
    geometry: { type: 'POINT', coordinates: [lon, -35.28] },
    properties: { title: 'Works ' + id },
  });

  it('carries a group forward when upstream drops it wholesale', async () => {
    const { fetchTrafficWorks } = await import('../../../src/sources/traffic.js');
    // Poll 1: both jurisdictions present.
    fetchJsonMock.mockResolvedValueOnce([
      rec('act-1', 'actRoadInfo', 149.12),
      rec('nsw-1', 'nswRoadInfo', 151.2),
    ]);
    expect((await fetchTrafficWorks()).count).toBe(2);

    // Poll 2: the ACT group has vanished entirely — the real upstream
    // failure mode, which used to empty Canberra off the map.
    fetchJsonMock.mockResolvedValueOnce([rec('nsw-1', 'nswRoadInfo', 151.2)]);
    const second = await fetchTrafficWorks();
    expect(second.count).toBe(2);
    expect(second.features.some((f) => f.properties.upstreamId === 'act-1')).toBe(true);
  });

  it('lets a returning group replace what was carried', async () => {
    const { fetchTrafficWorks } = await import('../../../src/sources/traffic.js');
    fetchJsonMock.mockResolvedValueOnce([rec('act-1', 'actRoadInfo', 149.12)]);
    await fetchTrafficWorks();
    fetchJsonMock.mockResolvedValueOnce([]);              // dropout
    await fetchTrafficWorks();
    fetchJsonMock.mockResolvedValueOnce([rec('act-2', 'actRoadInfo', 149.13)]);
    const third = await fetchTrafficWorks();
    // The fresh batch wins; the stale record is not also kept.
    expect(third.count).toBe(1);
    expect(third.features[0]?.properties.upstreamId).toBe('act-2');
  });

  it('does not carry a group forward when it merely shrinks', async () => {
    const { fetchTrafficWorks } = await import('../../../src/sources/traffic.js');
    fetchJsonMock.mockResolvedValueOnce([
      rec('a', 'actRoadInfo', 149.12), rec('b', 'actRoadInfo', 149.13),
    ]);
    await fetchTrafficWorks();
    // One record is a legitimate update, not an outage — trust it.
    fetchJsonMock.mockResolvedValueOnce([rec('a', 'actRoadInfo', 149.12)]);
    expect((await fetchTrafficWorks()).count).toBe(1);
  });
});

/**
 * The works aggregate is filed by category rather than under a "Works"
 * type of its own — the logs page was offering a type whose contents
 * were really Incidents, Roadwork, Flooding and Major Events under one
 * label. These buckets must stay in step with map.html's _worksTarget().
 */
describe('traffic.worksArchiveSource', () => {
  it('files the construction family as roadwork', async () => {
    const { worksArchiveSource } = await import('../../../src/sources/traffic.js');
    for (const c of [
      'Roadwork',
      'roadWorks',
      'buildingConstruction',
      'lightRail',
      'utilities',
      'telecommunication',
    ]) {
      expect(worksArchiveSource(c)).toBe('traffic_roadwork');
    }
  });

  it('files floods and events under their own types', async () => {
    const { worksArchiveSource } = await import('../../../src/sources/traffic.js');
    expect(worksArchiveSource('Flood')).toBe('traffic_flood');
    expect(worksArchiveSource('MajorEvent')).toBe('traffic_majorevent');
    expect(worksArchiveSource('specialEvent')).toBe('traffic_majorevent');
  });

  it('treats everything else as an incident', async () => {
    const { worksArchiveSource } = await import('../../../src/sources/traffic.js');
    for (const c of ['generalHazards', 'Breakdown', 'Crash', 'Advice', 'other', 'Not Applicable', '']) {
      expect(worksArchiveSource(c)).toBe('traffic_incident');
    }
  });

  it('is case- and punctuation-insensitive, since the feed is neither', async () => {
    const { worksArchiveSource } = await import('../../../src/sources/traffic.js');
    expect(worksArchiveSource('ROAD WORKS')).toBe('traffic_roadwork');
    expect(worksArchiveSource('light rail')).toBe('traffic_roadwork');
    expect(worksArchiveSource('major event')).toBe('traffic_majorevent');
  });
});

describe('traffic.worksArchiveItems', () => {
  it('re-points each row without disturbing anything else about it', async () => {
    const { worksArchiveItems } = await import('../../../src/sources/traffic.js');
    const snapshot = {
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [151.2, -33.8] },
          properties: { id: 'ltw:aaa', title: 'Pit works', mainCategory: 'Roadwork' },
        },
        {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [151.3, -33.9] },
          properties: { id: 'ltw:bbb', title: 'Water over road', mainCategory: 'Flood' },
        },
        {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [151.4, -34.0] },
          properties: { id: 'ltw:ccc', title: 'Debris', mainCategory: 'generalHazards' },
        },
      ],
      count: 3,
    };
    const rows = worksArchiveItems(snapshot, 1_700_000_000, 'traffic_works');
    expect(rows.map((r) => r.source)).toEqual([
      'traffic_roadwork',
      'traffic_flood',
      'traffic_incident',
    ]);
    // ids stay ltw:-prefixed so they cannot collide with the state
    // feeds' own upstream ids now sharing these sources
    expect(rows.every((r) => String(r.source_id).startsWith('ltw:'))).toBe(true);
    expect(rows[0]!.fetched_at).toBe(1_700_000_000);
  });
});
