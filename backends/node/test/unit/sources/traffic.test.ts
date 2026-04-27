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
