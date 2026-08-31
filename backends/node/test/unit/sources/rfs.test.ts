/**
 * Unit tests for the RFS source fetcher + helpers.
 *
 * No real network: we mock src/sources/shared/http.ts so fetchText
 * resolves with a captured XML fixture. The fixture is a stripped-down
 * but representative slice of the real RFS majorIncidents.xml feed.
 *
 * fetchRfs reads TWO feeds - the RSS incidents and the CAP-AU alerts -
 * so every test here queues two responses, in that order. The CAP one is
 * deliberately allowed to fail in its own tests: it is enrichment, and
 * losing it must not cost us the incidents.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchTextMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: fetchTextMock,
  fetchJson: vi.fn(),
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

/**
 * The CAP envelope, trimmed to the shape that matters: EDXL-DE wrapping
 * one CAP-AU alert per incident, joined to the RSS by <incidents>.
 * Note the default xmlns on <alert> - fast-xml-parser strips it, which
 * is why the parser looks up bare local names.
 */
const CAP_FIXTURE = `<?xml version="1.0" encoding="utf-8"?>
<EDXLDistribution xmlns="urn:oasis:names:tc:emergency:EDXL:DE:1.0">
  <distributionID>RFSUniqueID:2026-01-07T13:00:00Z</distributionID>
  <senderID>webmaster@rfs.nsw.gov.au</senderID>
  <contentObject>
    <contentDescription>Information on Bushfire near Foo</contentDescription>
    <xmlContent>
      <embeddedXMLContent>
        <alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
          <identifier>2026-01-07T13:35:00.0000000:1</identifier>
          <incidents>1</incidents>
          <info>
            <category>Fire</category>
            <event>Bush Fire</event>
            <responseType>Prepare</responseType>
            <urgency>Expected</urgency>
            <severity>Severe</severity>
            <certainty>Observed</certainty>
            <senderName>NSW Rural Fire Service</senderName>
            <headline>Bushfire near Foo</headline>
            <instruction>Monitor conditions. Know what you will do.</instruction>
            <web>https://example.com/foo</web>
            <parameter><valueName>FuelType</valueName><value>Forest</value></parameter>
            <parameter><valueName>FireDangerClass</valueName><value>3</value></parameter>
            <parameter><valueName>ControlAuthority</valueName><value>Rural Fire Service</value></parameter>
            <parameter><valueName>AllocatedResources</valueName><value>4</value></parameter>
          </info>
        </alert>
      </embeddedXMLContent>
    </xmlContent>
  </contentObject>
</EDXLDistribution>`;

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
      <guid>https://incidents.rfs.nsw.gov.au/api/v1/incidents/1</guid>
      <category>Advice</category>
      <georss:point>-33.8 151.2</georss:point>
      <georss:polygon>-33.8 151.2 -33.81 151.21 -33.82 151.19 -33.8 151.2</georss:polygon>
    </item>
    <item>
      <title>Watch and Act fire</title>
      <link>https://example.com/bar</link>
      <description>Watch and Act: stay informed</description>
      <guid>https://incidents.rfs.nsw.gov.au/api/v1/incidents/2</guid>
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
    fetchTextMock.mockResolvedValueOnce(RFS_FIXTURE).mockResolvedValueOnce(CAP_FIXTURE);
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
    fetchTextMock.mockResolvedValueOnce(RFS_FIXTURE).mockResolvedValueOnce(CAP_FIXTURE);
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const out = await fetchRfs();
    const f1 = out.features[1];
    expect(f1).toBeDefined();
    if (!f1) throw new Error('no f1');
    expect(f1.properties.alertLevel).toBe('Watch and Act');
  });

  it('throws when the incidents feed throws — that one IS the record', async () => {
    fetchTextMock
      .mockRejectedValueOnce(new Error('boom'))
      .mockResolvedValueOnce(CAP_FIXTURE);
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    await expect(fetchRfs()).rejects.toThrow('boom');
  });

  it('joins the CAP alert onto its incident', async () => {
    fetchTextMock.mockResolvedValueOnce(RFS_FIXTURE).mockResolvedValueOnce(CAP_FIXTURE);
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const f0 = (await fetchRfs()).features[0]!;
    expect(f0.properties).toMatchObject({
      capEvent: 'Bush Fire',
      capCategory: 'Fire',
      // The three axes CAP adds that the alert level alone cannot carry:
      // how bad, how soon, how sure.
      capSeverity: 'Severe',
      capUrgency: 'Expected',
      capCertainty: 'Observed',
      capResponseType: 'Prepare',
      capSender: 'NSW Rural Fire Service',
      capInstruction: 'Monitor conditions. Know what you will do.',
    });
  });

  it('reads the agency-specific CAP parameters', async () => {
    fetchTextMock.mockResolvedValueOnce(RFS_FIXTURE).mockResolvedValueOnce(CAP_FIXTURE);
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const f0 = (await fetchRfs()).features[0]!;
    expect(f0.properties).toMatchObject({
      capFuelType: 'Forest',
      capFireDangerClass: '3',
      capControlAuthority: 'Rural Fire Service',
      capAllocatedResources: '4',
    });
  });

  it('leaves the CAP fields blank on an incident the CAP feed omits', async () => {
    fetchTextMock.mockResolvedValueOnce(RFS_FIXTURE).mockResolvedValueOnce(CAP_FIXTURE);
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const f1 = (await fetchRfs()).features[1]!;
    // Blank, not absent — the frontend renders these without a guard.
    expect(f1.properties.capSeverity).toBe('');
    expect(f1.properties.capEvent).toBe('');
    expect(f1.properties.alertLevel).toBe('Watch and Act');
  });

  it('still publishes the incidents when the CAP feed fails', async () => {
    // CAP is enrichment. A failure there must cost the extra fields, not
    // the layer.
    fetchTextMock
      .mockResolvedValueOnce(RFS_FIXTURE)
      .mockRejectedValueOnce(new Error('cap 503'));
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const out = await fetchRfs();
    expect(out.count).toBe(2);
    expect(out.features[0]!.properties.capSeverity).toBe('');
  });

  it('still publishes the incidents when the CAP envelope is reshaped', async () => {
    fetchTextMock
      .mockResolvedValueOnce(RFS_FIXTURE)
      .mockResolvedValueOnce('<html>not xml we understand</html>');
    const { fetchRfs } = await import('../../../src/sources/rfs.js');
    const out = await fetchRfs();
    expect(out.count).toBe(2);
    expect(out.features[0]!.properties.capEvent).toBe('');
  });
});

describe('rfs.parseRfsCap', () => {
  it('keys alerts by the incident id the RSS guid ends with', async () => {
    const { parseRfsCap, rfsIncidentId } = await import('../../../src/sources/rfs.js');
    const byId = parseRfsCap(CAP_FIXTURE);
    expect([...byId.keys()]).toEqual(['1']);
    expect(rfsIncidentId('https://incidents.rfs.nsw.gov.au/api/v1/incidents/673561')).toBe('673561');
  });

  it('falls back to the identifier tail when <incidents> is missing', async () => {
    const { parseRfsCap } = await import('../../../src/sources/rfs.js');
    const byId = parseRfsCap(CAP_FIXTURE.replace('<incidents>1</incidents>', ''));
    expect([...byId.keys()]).toEqual(['1']);
  });

  it('returns nothing for an envelope with no alerts', async () => {
    const { parseRfsCap } = await import('../../../src/sources/rfs.js');
    expect(parseRfsCap('<EDXLDistribution></EDXLDistribution>').size).toBe(0);
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
