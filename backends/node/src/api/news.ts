/**
 * GET /api/news/rss     — aggregated feed
 * GET /api/news/sources — available sources / categories
 *
 * Mirrors python external_api_proxy.py:6100 + 6241. The aggregator is
 * cached for CACHE_TTL_RSS (5 min) — the cache is keyed by the
 * (sources, category, limit) triple so different queries don't trample
 * each other.
 *
 * Both are also served at /api/v1/ (server.ts mirrors them), wrapped in
 * `data` there (api/v1/respond.ts). When none of the feeds asked for gives
 * anything, the old path answers the empty aggregate (200), as it always
 * has; /api/v1/ answers 503 service_unavailable, so an outage does not read
 * as "no news".
 */
import { Hono, type Context } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import {
  aggregateNews,
  RSS_FEEDS,
  type AggregateResponse,
} from '../sources/news.js';
import { log } from '../lib/log.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, isV1, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  GATED_ERRORS,
  okJson,
  queryValidator,
  UNAVAILABLE_ERROR,
  type SameShape,
} from './v1/schemas.js';

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

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_NEWS = scoped({ source: 'news', type: 'news', window: 'live' });

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const CATEGORIES = 'general, emergency or weather';

const NewsHeadlines = z
  .object({
    items: z
      .array(
        z.object({
          title: z.string(),
          link: z.string().describe("The story on the publisher's own site"),
          description: z
            .string()
            .describe("The feed's summary as plain text (its HTML removed), cut to 300 characters; empty when it has none"),
          published: z.string().describe('When it was published, as the feed writes it'),
          source: z.string().describe("The feed's name, as in /news/sources"),
          icon: z.string().describe('An emoji for the feed'),
          category: z
            .string()
            .describe(`${CATEGORIES}: AusAware's own reading of keywords in the title and summary, not the publisher's`),
          timestamp: z.number().optional().describe('`published` as unix seconds; 0 when it cannot be read'),
        }),
      )
      .describe('Newest first'),
    count: z.number().int().describe('Number of items'),
    sources: z
      .record(
        z.string(),
        z.object({
          name: z.string(),
          count: z.number().int().describe('Items taken from its feed'),
          status: z
            .string()
            .describe(
              'ok when its feed gave items; empty when it gave none, including when it could not be read or is being rested after failing twice in a row; error when reading it failed in some other way',
            ),
          error: z.string().optional().describe('With status error, what went wrong'),
        }),
      )
      .describe('How each feed asked for fared, by feed key'),
    category_counts: z
      .object({ general: z.number().int(), emergency: z.number().int(), weather: z.number().int() })
      .describe('Items in each category, before the category filter'),
    available_sources: z.array(z.string()).describe('Every feed key'),
    available_categories: z.array(z.string()),
  })
  .meta({ id: 'NewsHeadlines' });
assertShape<SameShape<z.infer<typeof NewsHeadlines>, AggregateResponse>>();

// The query is read by hand in the handler, as it always was; this only
// describes it and never refuses.
const NewsHeadlinesQuery = z.object({
  sources: z
    .string()
    .optional()
    .describe('Comma-separated feed keys from /news/sources, e.g. abc,sbs; unknown keys are ignored. Every feed when absent or empty'),
  category: z
    .string()
    .optional()
    .describe(`Only items in this category: ${CATEGORIES}; any other value matches nothing. Every category when absent or empty`),
  limit: z.coerce
    .number()
    .min(1)
    .max(20)
    .optional()
    .catch(undefined)
    .describe(
      "Most items to take from each feed, 1–20; default 8, as is 0 or anything not a number. Less than 1 is read as 1 and more than 20 as 20. A feed gives at most its first 10",
    ),
});

const NewsSources = z
  .object({
    sources: z
      .record(
        z.string(),
        z.object({
          name: z.string(),
          url: z.string().describe("The publisher's public RSS feed"),
          icon: z.string().describe('An emoji for the feed'),
        }),
      )
      .describe('By feed key, the keys /news/rss takes in `sources`'),
    categories: z.array(z.string()),
  })
  .meta({ id: 'NewsSources' });

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/**
 * Did no feed asked for give anything? The aggregator turns a feed it cannot
 * read into an empty one, so this is the only sign of an outage it leaves. A
 * request naming no known feed asked for nothing, and is not one.
 */
function nothingRead(res: AggregateResponse): boolean {
  const feeds = Object.values(res.sources);
  return feeds.length > 0 && feeds.every((f) => f.status !== 'ok');
}

const UNREADABLE = 'None of the news feeds could be read just now.';

function answer(c: Context, res: AggregateResponse): Response {
  if (isV1(c) && nothingRead(res)) return fail(c, 503, 'service_unavailable', UNREADABLE);
  return ok(c, res);
}

newsRouter.get(
  '/api/news/rss',
  SCOPE_NEWS,
  describeRoute({
    operationId: 'getNewsHeadlines',
    tags: ['News'],
    summary: 'News headlines',
    description:
      "Headlines, summaries and links from the public RSS feeds of the Australian news outlets and NSW agencies listed in /news/sources, newest first, each sorted into a category by keyword. AusAware is not affiliated with any of them; `link` leads to the story on their own site. The feeds are read when asked, and each combination of parameters is answered from a five-minute cache. A feed that keeps failing is rested for a while before it is tried again.",
    responses: {
      ...okJson(envelope(NewsHeadlines), 'The headlines, with how each feed fared'),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
    },
  }),
  queryValidator(NewsHeadlinesQuery),
  async (c) => {
    const url = new URL(c.req.url);
    const sources = url.searchParams.get('sources') ?? '';
    const category = url.searchParams.get('category') ?? '';
    const limitRaw = url.searchParams.get('limit') ?? '8';
    const limit = Math.max(1, Math.min(20, Number(limitRaw) || 8));

    const key = cacheKey(sources, category, limit);
    const now = Date.now();
    const hit = cache.get(key);
    if (hit && hit.expiresAt > now) return answer(c, hit.res);

    try {
      const res = await aggregateNews({ sources, category, limit });
      cache.set(key, { res, expiresAt: now + CACHE_TTL_MS });
      pruneCache(now);
      return answer(c, res);
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'news aggregation failed');
      // The old path has always answered an empty aggregate (200) rather than an error.
      if (!isV1(c)) {
        return ok(c, {
          items: [],
          count: 0,
          sources: {},
          category_counts: { general: 0, emergency: 0, weather: 0 },
          available_sources: Object.keys(RSS_FEEDS),
          available_categories: ['general', 'emergency', 'weather'],
        } satisfies z.infer<typeof NewsHeadlines>);
      }
      return fail(c, 503, 'service_unavailable', UNREADABLE);
    }
  },
);

newsRouter.get(
  '/api/news/sources',
  SCOPE_NEWS,
  describeRoute({
    operationId: 'getNewsSources',
    tags: ['News'],
    summary: 'News feeds and categories',
    description:
      'The news outlets and agencies /news/rss reads, each with its public RSS feed, and the categories it sorts headlines into. AusAware is not affiliated with any of them.',
    responses: { ...okJson(envelope(NewsSources), 'The feeds and categories'), ...GATED_ERRORS },
  }),
  (c) =>
    ok(c, {
      sources: RSS_FEEDS,
      categories: ['general', 'emergency', 'weather'],
    } satisfies z.infer<typeof NewsSources>),
);

/** Drop the RSS aggregate cache. Wired into /api/cache/clear. Also
 *  used by tests to prevent bleed between runs. */
export function _resetNewsCacheForTests(): void {
  cache.clear();
}
