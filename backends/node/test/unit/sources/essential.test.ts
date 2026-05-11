/**
 * Essential Energy KML parser tests.
 *
 * Pure-function coverage of `parseEssentialKml` against a hand-rolled
 * KML fixture that mirrors the real `current.kml` shape. The fetch
 * itself doesn't get tested here — that's just `fetchText` plus this
 * function — but exercising the parser is what matters: the upstream
 * format is the source of every parser bug.
 */
import { describe, it, expect } from 'vitest';
import { parseEssentialKml } from '../../../src/sources/essential.js';

// Real Essential KML structure: kml/Document/Placemark[*]. Description
// is HTML wrapped in CDATA. Two placemarks below — one planned, one
// unplanned.
const SAMPLE_KML = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://earth.google.com/kml/2.1">
  <Document>
    <Placemark id="INCD-1-r">
      <name>WAGGA WAGGA</name>
      <styleUrl>#unplanned-style</styleUrl>
      <description><![CDATA[
        <div><span>Time Off:</span>15/06/2024 14:30:00</div>
        <div><span>Est. Time On:</span>15/06/2024 17:00:00</div>
        <div><span>Customers affected:</span> 42</div>
        <div><span>Reason:</span>Storm damage</div>
        <div><span>Last Updated:</span>15/06/2024 14:35:00</div>
      ]]></description>
      <Point>
        <coordinates>147.367,-35.108</coordinates>
      </Point>
    </Placemark>
    <Placemark id="INCD-2-p">
      <name>DUBBO</name>
      <styleUrl>#planned-style</styleUrl>
      <description><![CDATA[
        <div><span>Time Off:</span>20/06/2024 08:00:00</div>
        <div><span>Est. Time On:</span>20/06/2024 12:00:00</div>
        <div><span>Customers affected:</span> 5</div>
        <div><span>Reason:</span>Maintenance</div>
        <div><span>Last Updated:</span>14/06/2024 09:00:00</div>
      ]]></description>
      <Point>
        <coordinates>148.601,-32.243</coordinates>
      </Point>
      <Polygon>
        <outerBoundaryIs>
          <LinearRing>
            <coordinates>148.6,-32.2 148.61,-32.2 148.61,-32.25 148.6,-32.25 148.6,-32.2</coordinates>
          </LinearRing>
        </outerBoundaryIs>
      </Polygon>
    </Placemark>
  </Document>
</kml>`;

describe('parseEssentialKml', () => {
  it('parses placemarks from the current feed', () => {
    const out = parseEssentialKml(SAMPLE_KML, 'current');
    expect(out).toHaveLength(2);

    const wagga = out[0]!;
    expect(wagga.incidentId).toBe('INCD-1-r');
    expect(wagga.suburb).toBe('WAGGA WAGGA');
    expect(wagga.outageType).toBe('unplanned');
    expect(wagga.customersAffected).toBe(42);
    expect(wagga.cause).toBe('Storm damage');
    expect(wagga.timeOff).toBe('15/06/2024 14:30:00');
    expect(wagga.estTimeOn).toBe('15/06/2024 17:00:00');
    expect(wagga.lastUpdated).toBe('15/06/2024 14:35:00');
    expect(wagga.latitude).toBeCloseTo(-35.108, 3);
    expect(wagga.longitude).toBeCloseTo(147.367, 3);
    expect(wagga.source).toBe('essential_current');
    expect(wagga.status).toBe('active');
    expect(wagga.feedType).toBe('current');
    // ISO-converted "DD/MM/YYYY HH:mm:ss" → "YYYY-MM-DDTHH:mm:ss".
    expect(wagga.sourceTimestamp).toBe('2024-06-15T14:30:00');
    expect(wagga.polygon).toBeNull();
  });

  it('classifies planned styleUrl as planned outage', () => {
    const out = parseEssentialKml(SAMPLE_KML, 'current');
    const dubbo = out[1]!;
    expect(dubbo.outageType).toBe('planned');
    // Planned in the *current* feed maps to essential_planned source.
    expect(dubbo.source).toBe('essential_planned');
    expect(dubbo.polygon).not.toBeNull();
    expect(dubbo.polygon).toHaveLength(5);
    expect(dubbo.polygon?.[0]?.[0]).toBeCloseTo(148.6, 2);
  });

  it('marks future-feed entries as scheduled regardless of styleUrl', () => {
    const out = parseEssentialKml(SAMPLE_KML, 'future');
    expect(out).toHaveLength(2);
    for (const o of out) {
      expect(o.feedType).toBe('future');
      expect(o.status).toBe('scheduled');
      expect(o.source).toBe('essential_future');
    }
  });

  it('returns empty array for empty/garbage XML', () => {
    expect(parseEssentialKml('<kml/>', 'current')).toEqual([]);
    expect(
      parseEssentialKml('<?xml version="1.0"?><kml><Document/></kml>', 'current'),
    ).toEqual([]);
  });

  it('falls back gracefully when description is missing', () => {
    const xml = `<?xml version="1.0"?>
<kml xmlns="http://earth.google.com/kml/2.1">
  <Document>
    <Placemark id="X">
      <name>NOWHERE</name>
      <styleUrl>#unplanned</styleUrl>
      <Point><coordinates>150,-33</coordinates></Point>
    </Placemark>
  </Document>
</kml>`;
    const out = parseEssentialKml(xml, 'current');
    expect(out).toHaveLength(1);
    expect(out[0]?.cause).toBe('Unknown');
    expect(out[0]?.customersAffected).toBe(0);
    expect(out[0]?.title).toBe('NOWHERE');
  });
});
