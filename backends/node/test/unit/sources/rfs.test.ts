/**
 * Unit tests for the RFS source fetcher + helpers.
 *
 * No real network: we mock src/sources/shared/http.ts so fetchText
 * resolves with a captured XML fixture. The fixture is a stripped-down
 * but representative slice of the real RFS majorIncidents.xml feed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchTextMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: fetchTextMock,
  fetchJson: vi.fn(),
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

const RFS_FIXTURE = `<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:georss="http://www.georss.org/georss">
  <channel>
    <title>RFS Major Incidents</title>
    <description>Major fire incidents</description>
    <pubDate>Tue, 7 Jan 2026 13:00:00 +1100</pubDate>
    <item>
      <title>Bushfire near Foo</title>
      <link>https://example.com/foo</link>
      <description>ALERT LEVEL: Advice &lt;br /&gt;LOCATION: Foo Rd, Bar &lt;br /&gt;COUNCIL AREA: Bar Shire &lt;br /&gt;STATUS: Being controlled &lt;br /&gt;TYPE: Bush Fire &lt;br /&gt;SIZE: 5 ha &lt;br /&gt;RESPONSIBLE AGENCY: NSW RFS &lt;br /&gt;UPDATED: 7 Jan 2026 13:35</description>
      <guid>https://example.com/foo#1</guid>
      <category>Advice</category>
      <georss:point>-33.8 151.2</georss:point>
      <georss:polygon>-33.8 151.2 -33.81 151.21 -33.82 151.19 -33.8 151.2</georss:polygon>
    </item>
    <item>
      <title>Watch and Act fire</title>
      <link>https://example.com/bar</link>
      <description>Watch and Act: stay informed</description>
      <guid>https://example.com/bar#2</guid>
      <category>Watch and Act</category>
      <georss:point>-32.5 150.0</georss:point>
    </item>
    <item>
      <title>No coords</title>
      <description>incident without geo</description>
      <guid>g3</guid>
    </item>
  </channel>
</rss>`;

describe('rfs.fetchRfs', () => {
  beforeEach(() => {
    fetchTextMock.mockReset();
  });

  it('parses items with coords and skips ones without', async () => {
    fetchTextMock.mockResolvedValueOnce(RFS_FIXTURE);
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const out = await fetchRfs();
    expect(out.type).toBe('FeatureCollection');
    expect(out.count).toBe(2);
    expect(out.features.length).toBe(2);

    const f0 = out.features[0];
    expect(f0).toBeDefined();
    if (!f0) throw new Error('no f0');
    expect(f0.geometry.coordinates).toEqual([151.2, -33.8]);
    expect(f0.properties.title).toBe('Bushfire near Foo');
    expect(f0.properties.alertLevel).toBe('Advice');
    expect(f0.properties.location).toBe('Foo Rd, Bar');
    expect(f0.properties.councilArea).toBe('Bar Shire');
    expect(f0.properties.status).toBe('Being controlled');
    expect(f0.properties.fireType).toBe('Bush Fire');
    expect(f0.properties.size).toBe('5 ha');
    expect(f0.properties.responsibleAgency).toBe('NSW RFS');
    expect(f0.properties.updated).toBe('7 Jan 2026 13:35');
    expect(f0.properties.updatedISO).toMatch(/^2026-01-07T13:35:00\+11:00$/);
    expect(f0.properties.polygons).toEqual([
      '-33.8 151.2 -33.81 151.21 -33.82 151.19 -33.8 151.2',
    ]);
    expect(f0.properties.source).toBe('rfs');
  });

  it('falls back to category when description has no ALERT LEVEL', async () => {
    fetchTextMock.mockResolvedValueOnce(RFS_FIXTURE);
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const out = await fetchRfs();
    const f1 = out.features[1];
    expect(f1).toBeDefined();
    if (!f1) throw new Error('no f1');
    expect(f1.properties.alertLevel).toBe('Watch and Act');
  });

  it('throws when upstream throws', async () => {
    fetchTextMock.mockRejectedValueOnce(new Error('boom'));
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
