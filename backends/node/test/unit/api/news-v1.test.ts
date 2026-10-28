/**
 * The news routes on /api/v1/ and on their old /api/ paths.
 *
 * One handler serves both (api/v1/mirror.ts), answering through
 * api/v1/respond.ts: JSON wrapped in `data` on v1, the old bodies byte for
 * byte on /api/. The aggregator turns a feed it cannot read into an empty
 * one, so when no feed asked for gives anything the old path still answers
 * its empty 200 and v1 answers 503 service_unavailable.
 *
 * Both news routes are mirrored; nothing in this router is left off v1.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const fetchRawMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: vi.fn(),
  fetchRaw: fetchRawMock,
  HttpError: class extends Error {},
}));

const IN_SCOPE = ['/api/news/rss', '/api/news/sources'];

async function app() {
  const { newsRouter } = await import('../../../src/api/news.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', newsRouter);
  mirror(a, newsRouter, IN_SCOPE);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function get(a: Hono, path: string) {
  const res = await a.request(path);
  return { status: res.status, headers: [...res.headers], text: await res.text() };
}

const JSON_TYPE = 'application/json';
const KEYS = ['abc', 'news_com_au', 'nine', 'smh', 'sydney_sun', 'sbs', 'nsw_police', 'frnsw'];

function rss(key: string): string {
  return `<?xml version="1.0"?>
<rss version="2.0"><channel><title>${key}</title>
  <item>
    <title>Bushfire emergency near ${key}</title>
    <link>https://example.com/${key}/1</link>
    <description><![CDATA[<p>Firefighters battled a <b>blaze</b></p>]]></description>
    <pubDate>Tue, 27 Oct 2026 13:35:00 +1000</pubDate>
  </item>
  <item>
    <title>Council opens a library in ${key}</title>
    <link>https://example.com/${key}/2</link>
    <description>Plain text</description>
    <pubDate>Mon, 26 Oct 2026 09:00:00 +1000</pubDate>
  </item>
</channel></rss>`;
}

/** Answer each feed by its URL's key: `ok`, a status, or a dropped connection. */
function upstream(by: (key: string) => 'ok' | number | 'throw') {
  fetchRawMock.mockImplementation(async (url: string) => {
    const { RSS_FEEDS } = await import('../../../src/sources/news.js');
    const key = Object.entries(RSS_FEEDS).find(([, f]) => f.url === url)?.[0] ?? '';
    const how = by(key);
    if (how === 'throw') throw new Error('fetch failed');
    if (how === 'ok') return { status: 200, text: rss(key), headers: new Headers() };
    return { status: how, text: 'nope', headers: new Headers() };
  });
}

/** What aggregateNews answers for this query and upstream, as the route has always passed it on. */
async function aggregate(q: { sources?: string; category?: string; limit?: number }) {
  const { aggregateNews, _resetNewsBackoffForTests } = await import('../../../src/sources/news.js');
  _resetNewsBackoffForTests();
  const res = await aggregateNews(q);
  _resetNewsBackoffForTests();
  return JSON.stringify(res);
}

beforeEach(async () => {
  fetchRawMock.mockReset();
  const { _resetNewsCacheForTests } = await import('../../../src/api/news.js');
  const { _resetNewsBackoffForTests } = await import('../../../src/sources/news.js');
  _resetNewsCacheForTests();
  _resetNewsBackoffForTests();
});

describe('/news/rss', () => {
  it('the aggregate — bare on the old path, in data on v1', async () => {
    upstream(() => 'ok');
    const want = await aggregate({ sources: '', category: '', limit: 8 });
    expect(JSON.parse(want).count).toBe(16);
    const a = await app();
    const old = await get(a, '/api/news/rss');
    expect(old).toEqual({ status: 200, headers: [['content-type', JSON_TYPE]], text: want });
    const now = await get(a, '/api/v1/news/rss');
    expect(now).toEqual({ status: 200, headers: [['content-type', JSON_TYPE]], text: `{"data":${want}}` });
  });

  it('parameters are read as they always were — junk is never a 400', async () => {
    upstream(() => 'ok');
    const a = await app();
    const cases: Array<[string, { sources?: string; category?: string; limit?: number }]> = [
      ['sources=abc,sbs&category=emergency&limit=1', { sources: 'abc,sbs', category: 'emergency', limit: 1 }],
      ['sources=nope', { sources: 'nope', category: '', limit: 8 }], // asked for nothing: an empty 200 on both
      ['limit=abc', { sources: '', category: '', limit: 8 }],
      ['limit=0', { sources: '', category: '', limit: 8 }],
      ['limit=-5', { sources: '', category: '', limit: 1 }],
      ['limit=50&limit=1', { sources: '', category: '', limit: 20 }], // a repeat: the first value
      ['category=nope', { sources: '', category: 'nope', limit: 8 }],
    ];
    for (const [qs, q] of cases) {
      const want = await aggregate(q);
      const old = await get(a, `/api/news/rss?${qs}`);
      expect(old.status, qs).toBe(200);
      expect(old.text, qs).toBe(want);
      const now = await get(a, `/api/v1/news/rss?${qs}`);
      expect(now.status, qs).toBe(200);
      expect(now.text, qs).toBe(`{"data":${want}}`);
    }
  });

  it('some feeds failing: a 200 on both paths, with how each fared', async () => {
    upstream((key) => (key === 'abc' ? 'ok' : key === 'nine' ? 'throw' : 503));
    const a = await app();
    const res = await get(a, '/api/v1/news/rss?sources=abc,nine,sbs');
    expect(res.status).toBe(200);
    const body = JSON.parse(res.text) as { data: { count: number; sources: Record<string, { status: string }> } };
    expect(body.data.count).toBe(2);
    expect(Object.fromEntries(Object.entries(body.data.sources).map(([k, s]) => [k, s.status]))).toEqual({
      abc: 'ok',
      nine: 'empty',
      sbs: 'empty',
    });
  });

  it('every feed failing: the old path its empty 200 (cached as always), v1 a 503', async () => {
    upstream(() => 'throw');
    const a = await app();
    const empty = {
      items: [],
      count: 0,
      sources: Object.fromEntries(KEYS.map((k) => [k, { name: expect.any(String), count: 0, status: 'empty' }])),
      category_counts: { general: 0, emergency: 0, weather: 0 },
      available_sources: KEYS,
      available_categories: ['general', 'emergency', 'weather'],
    };
    const old = await get(a, '/api/news/rss');
    expect(old.status).toBe(200);
    expect(old.headers).toEqual([['content-type', JSON_TYPE]]);
    expect(JSON.parse(old.text)).toEqual(empty);
    expect(fetchRawMock).toHaveBeenCalledTimes(KEYS.length);

    // Same query on v1: answered from the cache, as a 503.
    const now = await get(a, '/api/v1/news/rss');
    expect(now.status).toBe(503);
    expect(JSON.parse(now.text)).toEqual({ error: { code: 'service_unavailable', message: expect.any(String) } });
    expect(fetchRawMock).toHaveBeenCalledTimes(KEYS.length);

    // Cold on v1 as well: a 503.
    expect((await get(a, '/api/v1/news/rss?limit=3')).status).toBe(503);
  });

  it('the aggregator itself failing: the old path its fallback 200, v1 a 503', async () => {
    const { RSS_FEEDS } = await import('../../../src/sources/news.js');
    const feeds = RSS_FEEDS as Record<string, unknown>;
    feeds['broken'] = {
      name: 'Broken',
      icon: '',
      get url(): string {
        throw new Error('no url');
      },
    };
    try {
      const a = await app();
      const old = await get(a, '/api/news/rss?sources=broken');
      expect(old.status).toBe(200);
      expect(old.text).toBe(
        JSON.stringify({
          items: [],
          count: 0,
          sources: {},
          category_counts: { general: 0, emergency: 0, weather: 0 },
          available_sources: [...KEYS, 'broken'],
          available_categories: ['general', 'emergency', 'weather'],
        }),
      );
      const now = await get(a, '/api/v1/news/rss?sources=broken');
      expect(now.status).toBe(503);
      expect((JSON.parse(now.text) as { error: { code: string } }).error.code).toBe('service_unavailable');
    } finally {
      delete feeds['broken'];
    }
  });
});

describe('/news/sources', () => {
  it('the feeds and categories — bare on the old path, in data on v1', async () => {
    const { RSS_FEEDS } = await import('../../../src/sources/news.js');
    const want = JSON.stringify({ sources: RSS_FEEDS, categories: ['general', 'emergency', 'weather'] });
    const a = await app();
    expect(await get(a, '/api/news/sources')).toEqual({ status: 200, headers: [['content-type', JSON_TYPE]], text: want });
    expect(await get(a, '/api/v1/news/sources')).toEqual({
      status: 200,
      headers: [['content-type', JSON_TYPE]],
      text: `{"data":${want}}`,
    });
  });
});

describe('the mirror', () => {
  it('an unknown v1 path is a v1 404; the old 404 is untouched', async () => {
    const a = await app();
    const miss = await get(a, '/api/v1/news/nope');
    expect(miss.status).toBe(404);
    expect(JSON.parse(miss.text)).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
    expect(await get(a, '/api/news/nope')).toEqual({
      status: 404,
      headers: [['content-type', 'text/plain; charset=UTF-8']],
      text: '404 Not Found',
    });
  });

  it('mirroring the in-scope paths registers exactly those', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    await app();
    const news = mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/news/'));
    expect(news.map((r) => `${r.method} ${r.legacyPath}`).sort()).toEqual(IN_SCOPE.map((p) => `GET ${p}`).sort());
  });
});
