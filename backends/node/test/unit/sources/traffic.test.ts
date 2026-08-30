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
    expect(snap.features[0]?.properties.id).toBe('1');
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
    expect(snap.features.map((f) => f.properties.id)).toEqual(['b']);
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
