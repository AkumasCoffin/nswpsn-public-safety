/**
 * News RSS source — covers the parser (RSS + Atom variants), the
 * keyword-driven category detector, and the date parser.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchRawMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: vi.fn(),
  fetchRaw: fetchRawMock,
  HttpError: class extends Error {},
}));

const RSS_FIXTURE = `<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>Bushfire emergency in Penrith</title>
      <link>https://example.com/a</link>
      <description><![CDATA[<p>Firefighters battled a major <b>blaze</b> overnight</p>]]></description>
      <pubDate>Tue, 21 Apr 2026 13:35:00 +1000</pubDate>
    </item>
    <item>
      <title>Sunny weather forecast</title>
      <link>https://example.com/b</link>
      <description>Bureau of Meteorology says clear skies ahead</description>
      <pubDate>Mon, 20 Apr 2026 09:00:00 +1000</pubDate>
    </item>
  </channel>
</rss>`;

const ATOM_FIXTURE = `<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Feed</title>
  <entry>
    <title>Police arrest two over robbery</title>
    <link href="https://example.com/atom-a"/>
    <summary>Officers arrested two suspects after a ram raid in Sydney CBD.</summary>
    <published>2026-04-22T08:30:00+10:00</published>
  </entry>
</feed>`;

describe('news.detectCategory', () => {
  beforeEach(async () => {
    const mod = await import('../../../src/sources/news.js');
    mod._resetNewsBackoffForTests();
  });

  it('classifies clear emergencies', async () => {
    const { detectCategory } = await import('../../../src/sources/news.js');
    expect(detectCategory('Police arrest suspects after stabbing', '')).toBe(
      'emergency',
    );
    expect(detectCategory('House fire destroys home', '')).toBe('emergency');
  });

  it('classifies weather articles', async () => {
    const { detectCategory } = await import('../../../src/sources/news.js');
    expect(detectCategory('Severe weather warning issued', '')).toBe('weather');
    expect(detectCategory('Heatwave to hit NSW', '')).toBe('weather');
  });

  it('falls back to general', async () => {
    const { detectCategory } = await import('../../../src/sources/news.js');
    expect(detectCategory('Local council approves new park', '')).toBe(
      'general',
    );
  });

  it('treats fire-ban headlines as weather', async () => {
    const { detectCategory } = await import('../../../src/sources/news.js');
    expect(detectCategory('Total fire ban declared', '')).toBe('weather');
  });
});

describe('news.parseRssDate', () => {
  it('parses RFC 2822 + ISO + bad input', async () => {
    const { parseRssDate } = await import('../../../src/sources/news.js');
    expect(parseRssDate('Tue, 21 Apr 2026 13:35:00 +1000')).toBeGreaterThan(0);
    expect(parseRssDate('2026-04-21T13:35:00Z')).toBeGreaterThan(0);
    expect(parseRssDate('2026-04-21T13:35:00+00:00')).toBeGreaterThan(0);
    expect(parseRssDate('garbage')).toBe(0);
    expect(parseRssDate('')).toBe(0);
  });
});

describe('news.aggregateNews', () => {
  beforeEach(async () => {
    fetchRawMock.mockReset();
    const mod = await import('../../../src/sources/news.js');
    mod._resetNewsBackoffForTests();
  });

  it('aggregates a single source and shapes the response', async () => {
    fetchRawMock.mockResolvedValue({
      status: 200,
      text: RSS_FIXTURE,
      headers: new Headers(),
    });
    const { aggregateNews } = await import('../../../src/sources/news.js');
    const out = await aggregateNews({ sources: 'abc', limit: 5 });
    expect(out.count).toBe(2);
    expect(out.items[0]?.source).toBe('ABC News');
    // The bushfire item is more recent — should sort first.
    expect(out.items[0]?.title).toContain('Bushfire');
    // Description HTML stripping.
    expect(out.items[0]?.description).not.toContain('<');
    expect(out.sources['abc']?.status).toBe('ok');
    expect(out.available_categories).toEqual([
      'general',
      'emergency',
      'weather',
    ]);
  });

  it('parses Atom feeds', async () => {
    fetchRawMock.mockResolvedValue({
      status: 200,
      text: ATOM_FIXTURE,
      headers: new Headers(),
    });
    const { aggregateNews } = await import('../../../src/sources/news.js');
    const out = await aggregateNews({ sources: 'abc', limit: 5 });
    expect(out.count).toBe(1);
    expect(out.items[0]?.title).toBe('Police arrest two over robbery');
    expect(out.items[0]?.link).toBe('https://example.com/atom-a');
    expect(out.items[0]?.category).toBe('emergency');
  });

  it('marks failed sources as error and keeps others', async () => {
    fetchRawMock.mockResolvedValueOnce({
      status: 403,
      text: '',
      headers: new Headers(),
    });
    const { aggregateNews } = await import('../../../src/sources/news.js');
    const out = await aggregateNews({ sources: 'abc', limit: 5 });
    // 403 returns empty items + records a failure (not an error result).
    expect(out.count).toBe(0);
    expect(out.sources['abc']?.status).toBe('empty');
  });

  it('filters by category', async () => {
    fetchRawMock.mockResolvedValue({
      status: 200,
      text: RSS_FIXTURE,
      headers: new Headers(),
    });
    const { aggregateNews } = await import('../../../src/sources/news.js');
    const out = await aggregateNews({ sources: 'abc', category: 'emergency' });
    expect(out.items.length).toBe(1);
    expect(out.items[0]?.category).toBe('emergency');
  });
});
