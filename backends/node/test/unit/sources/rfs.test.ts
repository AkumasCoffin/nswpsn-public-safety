/**
 * Unit tests for the RFS source fetcher + helpers.
 *
 * No real network: we mock src/sources/shared/http.ts so fetchJson
 * resolves with a fixture shaped like the real majorIncidents.json — a
 * GeoJSON FeatureCollection whose geometry is a Point, or a
 * GeometryCollection of the Point and the fire-ground Polygons.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';

const fetchJsonMock = vi.fn();
const fetchTextMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: fetchTextMock,
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

const FDR_XML = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<FireDangerMap>
  <District>
    <Name>Far North Coast</Name>
    <RegionNumber>1</RegionNumber>
    <Councils>Ballina; Byron; Clarence Valley</Councils>
    <DangerLevelToday>NONE</DangerLevelToday>
    <DangerLevelTomorrow>MODERATE</DangerLevelTomorrow>
    <FireBanToday>No</FireBanToday>
    <FireBanTomorrow>No</FireBanTomorrow>
  </District>
  <District>
    <Name>Greater Hunter</Name>
    <RegionNumber>7</RegionNumber>
    <DangerLevelToday>Extreme</DangerLevelToday>
    <DangerLevelTomorrow>HIGH</DangerLevelTomorrow>
    <FireBanToday>Yes</FireBanToday>
    <FireBanTomorrow>No</FireBanTomorrow>
  </District>
  <District><RegionNumber>99</RegionNumber></District>
</FireDangerMap>`;

describe('rfs.parseFireDangerMap', () => {
  it('reads each district into fields, keeping the old title and description', async () => {
    const { parseFireDangerMap } = await import('../../../src/sources/rfs.js');
    const r = parseFireDangerMap(FDR_XML);
    expect(r).toHaveLength(2); // the record with no name is dropped
    expect(r[0]).toEqual({
      title: 'Far North Coast',
      description: 'Today: no rating, no total fire ban. Tomorrow: MODERATE, no total fire ban.',
      source: 'rfs_fdr',
      district: 'Far North Coast',
      region_number: 1,
      councils: ['Ballina', 'Byron', 'Clarence Valley'],
      danger_today: 'NONE',
      danger_tomorrow: 'MODERATE',
      fire_ban_today: false,
      fire_ban_tomorrow: false,
    });
    // Upper-cased as the feed mostly writes it; a missing Councils is an empty list.
    expect(r[1]).toMatchObject({ danger_today: 'EXTREME', fire_ban_today: true, councils: [] });
    expect(r[1]?.description).toBe('Today: EXTREME, total fire ban. Tomorrow: HIGH, no total fire ban.');
  });
});

describe('rfs.rfsFireDanger — read at most every ten minutes', () => {
  beforeEach(async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-10-28T00:00:00Z'));
    fetchTextMock.mockReset();
    const { _resetRfsFireDanger } = await import('../../../src/sources/rfs.js');
    _resetRfsFireDanger();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('serves the last read for ten minutes, then reads again', async () => {
    fetchTextMock.mockResolvedValue(FDR_XML);
    const { rfsFireDanger } = await import('../../../src/sources/rfs.js');
    const first = await rfsFireDanger();
    expect(first.ratings).toHaveLength(2);
    expect(first.fetched_at).toBe('2026-10-28T00:00:00.000Z');
    vi.setSystemTime(new Date('2026-10-28T00:09:00Z'));
    await rfsFireDanger();
    expect(fetchTextMock).toHaveBeenCalledTimes(1);
    vi.setSystemTime(new Date('2026-10-28T00:10:01Z'));
    await rfsFireDanger();
    expect(fetchTextMock).toHaveBeenCalledTimes(2);
  });

  it('keeps serving the last good read through an outage, retrying once a minute', async () => {
    const { rfsFireDanger } = await import('../../../src/sources/rfs.js');
    fetchTextMock.mockResolvedValueOnce(FDR_XML);
    await rfsFireDanger();
    vi.setSystemTime(new Date('2026-10-28T00:11:00Z'));
    fetchTextMock.mockRejectedValue(new Error('down'));
    expect((await rfsFireDanger()).fetched_at).toBe('2026-10-28T00:00:00.000Z');
    vi.setSystemTime(new Date('2026-10-28T00:11:30Z'));
    await rfsFireDanger();
    expect(fetchTextMock).toHaveBeenCalledTimes(2); // not retried inside the minute
    vi.setSystemTime(new Date('2026-10-28T00:12:01Z'));
    await rfsFireDanger();
    expect(fetchTextMock).toHaveBeenCalledTimes(3);
  });

  it('throws only when there has never been a good read; an empty feed counts as a failure', async () => {
    const { rfsFireDanger } = await import('../../../src/sources/rfs.js');
    fetchTextMock.mockResolvedValueOnce('<FireDangerMap></FireDangerMap>');
    await expect(rfsFireDanger()).rejects.toThrow(/no districts/);
  });
});

describe('GET /api/rfs/fdr and /api/v1/rfs/fdr', () => {
  async function app() {
    const { rfsRouter } = await import('../../../src/api/rfs.js');
    const { mirror } = await import('../../../src/api/v1/mirror.js');
    const { notFound, onError } = await import('../../../src/api/v1/respond.js');
    const a = new Hono();
    a.route('/', rfsRouter);
    mirror(a, rfsRouter, ['/api/rfs/fdr']);
    a.notFound(notFound);
    a.onError(onError);
    return a;
  }
  beforeEach(async () => {
    fetchTextMock.mockReset();
    const { _resetRfsFireDanger } = await import('../../../src/sources/rfs.js');
    _resetRfsFireDanger();
  });

  it('answers the districts — wrapped in data on v1', async () => {
    fetchTextMock.mockResolvedValue(FDR_XML);
    const a = await app();
    const old = (await (await a.request('/api/rfs/fdr')).json()) as { ratings: unknown[] };
    expect(old.ratings).toHaveLength(2);
    const v1 = (await (await a.request('/api/v1/rfs/fdr')).json()) as { data: { ratings: unknown[] } };
    expect(v1.data.ratings).toHaveLength(2);
  });

  it('when the RFS cannot be read: the old path its empty 200, v1 a 503', async () => {
    fetchTextMock.mockRejectedValue(new Error('down'));
    const a = await app();
    const old = await a.request('/api/rfs/fdr');
    expect(old.status).toBe(200);
    expect(await old.json()).toEqual({ ratings: [] });
    const v1 = await a.request('/api/v1/rfs/fdr');
    expect(v1.status).toBe(503);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('service_unavailable');
  });
});

