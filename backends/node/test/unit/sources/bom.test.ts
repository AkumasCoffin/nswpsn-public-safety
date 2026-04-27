/**
 * Unit tests for the BOM source fetcher.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchTextMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: fetchTextMock,
  fetchJson: vi.fn(),
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

const BOM_WARNING_FIXTURE = `<?xml version="1.0" encoding="utf-8"?>
<product>
  <warnings>
    <warning type="land">
      <title>Severe Thunderstorm Warning for Sydney</title>
      <description>Damaging winds expected.</description>
      <area>Greater Sydney</area>
      <issued>2026-04-26T13:00:00+10:00</issued>
      <expiry>2026-04-26T18:00:00+10:00</expiry>
    </warning>
    <warning type="marine">
      <title>Strong Wind Warning - Coastal Waters</title>
      <description>Gale force winds offshore.</description>
      <area>NSW Coast</area>
      <issued>2026-04-26T12:00:00+10:00</issued>
      <expiry>2026-04-27T00:00:00+10:00</expiry>
    </warning>
    <warning type="land">
      <title>Severe Thunderstorm Warning for Sydney</title>
      <description>Duplicate that should dedupe.</description>
    </warning>
  </warnings>
</product>`;

const BOM_RSS_FIXTURE = `<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>BOM</title>
    <item>
      <title>Flood Watch</title>
      <description>Heavy rain over north coast.</description>
      <link>https://bom.example/flood</link>
      <pubDate>2026-04-26T08:00:00+10:00</pubDate>
    </item>
    <item>
      <title>Marine Wind Warning</title>
      <description>Strong winds at sea.</description>
      <link>https://bom.example/marine</link>
      <pubDate>2026-04-26T09:00:00+10:00</pubDate>
    </item>
  </channel>
</rss>`;

describe('bom.fetchBom', () => {
  beforeEach(() => {
    fetchTextMock.mockReset();
  });

  it('parses <warning> elements, dedupes by title, categorises, severity', async () => {
    fetchTextMock.mockResolvedValueOnce(BOM_WARNING_FIXTURE);
    const { fetchBom } = await import('../../../src/sources/bom.js');
    const out = await fetchBom();
    expect(out.count).toBe(2);
    expect(out.warnings.length).toBe(2);
    const land = out.warnings.find((w) => w.category === 'land');
    const marine = out.warnings.find((w) => w.category === 'marine');
    expect(land?.severity).toBe('severe');
    expect(land?.area).toBe('Greater Sydney');
    expect(marine?.severity).toBe('warning');
    expect(out.counts).toEqual({ land: 1, marine: 1 });
  });

  it('falls back to <item> elements when no <warning> exist', async () => {
    fetchTextMock.mockResolvedValueOnce(BOM_RSS_FIXTURE);
    const { fetchBom } = await import('../../../src/sources/bom.js');
    const out = await fetchBom();
    expect(out.count).toBe(2);
    const flood = out.warnings.find((w) => w.title === 'Flood Watch');
    const marine = out.warnings.find((w) => w.title === 'Marine Wind Warning');
    expect(flood?.category).toBe('land');
    expect(flood?.link).toBe('https://bom.example/flood');
    expect(marine?.category).toBe('marine');
  });

  it('throws on upstream failure', async () => {
    fetchTextMock.mockRejectedValueOnce(new Error('upstream down'));
    const { fetchBom } = await import('../../../src/sources/bom.js');
    await expect(fetchBom()).rejects.toThrow('upstream down');
  });
});

describe('bom.categorizeBomWarning', () => {
  it('flags marine keywords', async () => {
    const { categorizeBomWarning } = await import('../../../src/sources/bom.js');
    expect(categorizeBomWarning('Coastal waters wind')).toBe('marine');
    expect(categorizeBomWarning('Surf advice')).toBe('marine');
    expect(categorizeBomWarning('Flood Watch')).toBe('land');
  });
});

describe('bom.getBomSeverity', () => {
  it('classifies by keyword priority', async () => {
    const { getBomSeverity } = await import('../../../src/sources/bom.js');
    expect(getBomSeverity('Severe Thunderstorm')).toBe('severe');
    expect(getBomSeverity('Flood Warning')).toBe('warning');
    expect(getBomSeverity('Heatwave Watch')).toBe('watch');
    expect(getBomSeverity('Advice only')).toBe('advice');
    expect(getBomSeverity('General notice')).toBe('info');
  });
});
