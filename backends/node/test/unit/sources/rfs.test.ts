/**
 * Unit tests for the RFS source fetcher + helpers.
 *
 * No real network: we mock src/sources/shared/http.ts so fetchJson
 * resolves with a fixture shaped like the real majorIncidents.json — a
 * GeoJSON FeatureCollection whose geometry is a Point, or a
 * GeometryCollection of the Point and the fire-ground Polygons.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchJsonMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

const DESC =
  'ALERT LEVEL: Advice <br />LOCATION: Foo Rd, Bar <br />COUNCIL AREA: Bar Shire <br />STATUS: Being controlled <br />TYPE: Bush Fire <br />SIZE: 5 ha <br />RESPONSIBLE AGENCY: NSW RFS <br />UPDATED: 7 Jan 2026 13:35';

function fixture() {
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        geometry: {
          type: 'GeometryCollection',
          geometries: [
            { type: 'Point', coordinates: [151.2, -33.8] },
            {
              type: 'GeometryCollection',
              geometries: [
                // Outer ring, then a hole the map has no way to draw.
                { type: 'Polygon', coordinates: [[[151.2, -33.8], [151.21, -33.81], [151.19, -33.82], [151.2, -33.8]], [[151.2, -33.81], [151.201, -33.811], [151.199, -33.812], [151.2, -33.81]]] },
                { type: 'Polygon', coordinates: [[[150.1, -32.1], [150.2, -32.2], [150.0, -32.3], [150.1, -32.1]]] },
              ],
            },
          ],
        },
        properties: {
          title: 'Bushfire near Foo', link: 'https://example.com/foo', category: 'Advice',
          guid: 'https://incidents.rfs.nsw.gov.au/api/v1/incidents/1', guid_isPermaLink: 'true',
          pubDate: '7/01/2026 2:35:00 AM', description: DESC,
        },
      },
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [150.00000000000003, -32.5] },
        properties: {
          title: 'Watch and Act fire', link: 'https://example.com/bar', category: 'Watch and Act',
          guid: 'https://incidents.rfs.nsw.gov.au/api/v1/incidents/2', guid_isPermaLink: 'true',
          pubDate: '7/01/2026 2:00:00 AM', description: 'Watch and Act: stay informed',
        },
      },
      {
        type: 'Feature',
        geometry: { type: 'Polygon', coordinates: [[[151, -33], [151.1, -33.1], [151.2, -33], [151, -33]]] },
        properties: { title: 'No pin', guid: 'g3', description: 'incident without a point' },
      },
    ],
  };
}

describe('rfs.fetchRfs', () => {
  beforeEach(() => {
    fetchJsonMock.mockReset();
  });

  it('parses features with a pin and skips ones without', async () => {
    fetchJsonMock.mockResolvedValueOnce(fixture());
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const out = await fetchRfs();
    expect(out.type).toBe('FeatureCollection');
    expect(out.count).toBe(2);
    expect(out.features.length).toBe(2);

    const f0 = out.features[0];
    expect(f0).toBeDefined();
    if (!f0) throw new Error('no f0');
    expect(f0.geometry).toEqual({ type: 'Point', coordinates: [151.2, -33.8] });
    expect(f0.properties.title).toBe('Bushfire near Foo');
    expect(f0.properties.link).toBe('https://example.com/foo');
    expect(f0.properties.guid).toBe('https://incidents.rfs.nsw.gov.au/api/v1/incidents/1');
    expect(f0.properties.alertLevel).toBe('Advice');
    expect(f0.properties.location).toBe('Foo Rd, Bar');
    expect(f0.properties.councilArea).toBe('Bar Shire');
    expect(f0.properties.status).toBe('Being controlled');
    expect(f0.properties.fireType).toBe('Bush Fire');
    expect(f0.properties.size).toBe('5 ha');
    expect(f0.properties.responsibleAgency).toBe('NSW RFS');
    expect(f0.properties.updated).toBe('7 Jan 2026 13:35');
    expect(f0.properties.updatedISO).toMatch(/^2026-01-07T13:35:00\+11:00$/);
    expect(f0.properties.source).toBe('rfs');
  });

  it('turns each fire ground into the GeoRSS string the map reads, outer rings only', async () => {
    fetchJsonMock.mockResolvedValueOnce(fixture());
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const out = await fetchRfs();
    expect(out.features[0]?.properties.polygons).toEqual([
      '-33.8 151.2 -33.81 151.21 -33.82 151.19 -33.8 151.2',
      '-32.1 150.1 -32.2 150.2 -32.3 150 -32.1 150.1',
    ]);
    expect(out.features[1]?.properties.polygons).toEqual([]);
  });

  it('writes numbers as the XML feed did — fifteen significant digits', async () => {
    fetchJsonMock.mockResolvedValueOnce(fixture());
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const out = await fetchRfs();
    expect(out.features[1]?.geometry.coordinates).toEqual([150, -32.5]);
  });

  it('falls back to category when description has no ALERT LEVEL', async () => {
    fetchJsonMock.mockResolvedValueOnce(fixture());
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const out = await fetchRfs();
    const f1 = out.features[1];
    expect(f1).toBeDefined();
    if (!f1) throw new Error('no f1');
    expect(f1.properties.alertLevel).toBe('Watch and Act');
  });

  it('keeps the feed as published for the v1 raw route', async () => {
    const feed = fixture();
    fetchJsonMock.mockResolvedValueOnce(feed);
    const { fetchRfs, rfsFeedSnapshot } = await import('../../../src/sources/rfs.js');
    await fetchRfs();
    expect(rfsFeedSnapshot()).toEqual(feed);
  });

  it('throws when the body is not the feed, so the last good snapshot stays', async () => {
    fetchJsonMock.mockResolvedValueOnce({ error: 'maintenance' });
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    await expect(fetchRfs()).rejects.toThrow(/not a FeatureCollection/);
  });

  it('throws when upstream throws', async () => {
    fetchJsonMock.mockRejectedValueOnce(new Error('boom'));
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    await expect(fetchRfs()).rejects.toThrow('boom');
  });
});

describe('rfs.parseRfsDescription', () => {
  it('extracts every field from a typical description', async () => {
    const { parseRfsDescription } = await import('../../../src/sources/rfs.js');
    const r = parseRfsDescription(
      'ALERT LEVEL: Emergency Warning <br />LOCATION: Foo <br />STATUS: Out <br />UPDATED: 1 Feb 2026 09:00',
    );
    expect(r.alertLevel).toBe('Emergency Warning');
    expect(r.location).toBe('Foo');
    expect(r.status).toBe('Out');
    expect(r.updated).toBe('1 Feb 2026 09:00');
    // February is AEDT (DST) per the simple month-based approximation.
    expect(r.updatedISO).toBe('2026-02-01T09:00:00+11:00');
  });
});
