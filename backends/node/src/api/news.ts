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
/**
 * Cap on distinct cached queries.
 *
 * This cache had NO bound of any kind: expiry was checked on read but nothing
 * ever deleted an entry, and the key is built from `sources` and `category`
 * straight off the query string with no validation. /api/news/rss is public and
 * unauthenticated, so any crawler varying ?sources= minted a permanent entry
 * holding a full aggregate response — an unbounded, remotely-driven allocation.
 * The real key space is a handful of source/category combinations.
 */
const CACHE_MAX_ENTRIES = 64;
const cache = new Map<string, { res: AggregateResponse; expiresAt: number }>();

function cacheKey(sources: string, category: string, limit: number): string {
  return `${sources}|${category}|${limit}`;
}

/** Drop expired entries, then the oldest-written while still over the cap.
 *  Map preserves insertion order, so the first key is the oldest. */
function pruneCache(now: number): void {
  for (const [k, v] of cache) {
    if (v.expiresAt <= now) cache.delete(k);
  }
  while (cache.size > CACHE_MAX_ENTRIES) {
    const oldest = cache.keys().next().value;
    if (oldest === undefined) break;
    cache.delete(oldest);
  }
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
    pruneCache(now);
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
