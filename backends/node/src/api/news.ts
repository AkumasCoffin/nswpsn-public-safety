/**
 * GET /api/news/rss     — aggregated feed
 * GET /api/news/sources — available sources / categories
 *
 * Mirrors python external_api_proxy.py:6100 + 6241. The aggregator is
 * cached for CACHE_TTL_RSS (5 min) — the cache is keyed by the
 * (sources, category, limit) triple so different queries don't trample
 * each other.
 */
import { Hono } from 'hono';
import {
  aggregateNews,
  RSS_FEEDS,
  type AggregateResponse,
} from '../sources/news.js';
import { log } from '../lib/log.js';

const CACHE_TTL_MS = 5 * 60_000;
const cache = new Map<string, { res: AggregateResponse; expiresAt: number }>();

function cacheKey(sources: string, category: string, limit: number): string {
  return `${sources}|${category}|${limit}`;
}

export const newsRouter = new Hono();

newsRouter.get('/api/news/rss', async (c) => {
  const url = new URL(c.req.url);
  const sources = url.searchParams.get('sources') ?? '';
  const category = url.searchParams.get('category') ?? '';
  const limitRaw = url.searchParams.get('limit') ?? '8';
  const limit = Math.max(1, Math.min(20, Number(limitRaw) || 8));

  const key = cacheKey(sources, category, limit);
  const now = Date.now();
  const hit = cache.get(key);
  if (hit && hit.expiresAt > now) return c.json(hit.res);

  try {
    const res = await aggregateNews({ sources, category, limit });
    cache.set(key, { res, expiresAt: now + CACHE_TTL_MS });
    return c.json(res);
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'news aggregation failed');
    return c.json({
      items: [],
      count: 0,
      sources: {},
      category_counts: { general: 0, emergency: 0, weather: 0 },
      available_sources: Object.keys(RSS_FEEDS),
      available_categories: ['general', 'emergency', 'weather'],
    });
  }
});

newsRouter.get('/api/news/sources', (c) =>
  c.json({
    sources: RSS_FEEDS,
    categories: ['general', 'emergency', 'weather'],
  }),
);

/** Drop the RSS aggregate cache. Wired into /api/cache/clear. Also
 *  used by tests to prevent bleed between runs. */
export function _resetNewsCacheForTests(): void {
  cache.clear();
}
