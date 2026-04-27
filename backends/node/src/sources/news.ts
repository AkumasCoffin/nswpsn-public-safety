/**
 * News RSS aggregation.
 *
 * Mirrors python external_api_proxy.py:5826-6248. Same RSS_FEEDS list,
 * same auto-categorisation (general / emergency / weather), same
 * per-feed fail backoff so a permanently-broken feed (NSW Police 403,
 * F&RNSW 404) doesn't fire on every /api/news/rss call.
 *
 * Two routes consume this:
 *   - GET /api/news/rss      — aggregated, sortable, filterable
 *   - GET /api/news/sources  — list of configured sources
 *
 * Aggregation runs on demand with a tiny TTL cache (CACHE_TTL_RSS = 5
 * min in python). The handler invokes `aggregateNews(query)` directly
 * — no LiveStore involvement (the per-feed backoff state is module-
 * scoped here, just like python's threading.Lock-guarded globals).
 */
import { fetchRaw } from './shared/http.js';
import { parseXml, asArray, textOf } from './shared/xml.js';

// ---------------------------------------------------------------------------
// Sources / category-keyword tables (verbatim port of python globals)
// ---------------------------------------------------------------------------

export interface RssFeed {
  name: string;
  url: string;
  icon: string;
}

export const RSS_FEEDS: Record<string, RssFeed> = {
  abc: {
    name: 'ABC News',
    url: 'https://www.abc.net.au/news/feed/2942460/rss.xml',
    icon: '\u{1F4F0}',
  },
  news_com_au: {
    name: 'news.com.au',
    url: 'https://www.news.com.au/content-feeds/latest-news-national/',
    icon: '\u{1F4F0}',
  },
  nine: {
    name: '9News',
    url: 'https://www.9news.com.au/rss',
    icon: '\u{1F4FA}',
  },
  smh: {
    name: 'Sydney Morning Herald',
    url: 'https://www.smh.com.au/rss/feed.xml',
    icon: '\u{1F4C4}',
  },
  sydney_sun: {
    name: 'Sydney Sun',
    url: 'http://feeds.sydneysun.com/rss/ae0def0d9b645403',
    icon: '\u{2600}\u{FE0F}',
  },
  sbs: {
    name: 'SBS News',
    url: 'https://www.sbs.com.au/news/feed',
    icon: '\u{1F4E1}',
  },
  nsw_police: {
    name: 'NSW Police',
    url: 'https://www.police.nsw.gov.au/news/nsw_police_news.rss',
    icon: '\u{1F46E}',
  },
  frnsw: {
    name: 'Fire & Rescue NSW',
    url: 'https://www.fire.nsw.gov.au/feeds/feed.rss',
    icon: '\u{1F692}',
  },
};

// Category keywords — direct port of CATEGORY_KEYWORDS in python.
const CATEGORY_KEYWORDS = {
  emergency_strong: [
    'emergency', 'evacuate', 'evacuation', 'rescue', 'rescued',
    'ambulance', 'paramedic', 'triple zero', '000',
    'rfs', 'ses', 'frnsw', 'firefighter', 'firefighters',
    'crash', 'collision', 'accident', 'fatal', 'fatality',
    'death', 'dead', 'killed', 'dies', 'died', 'body found',
    'injured', 'injury', 'injuries', 'critical condition', 'hospital',
    'missing person', 'search and rescue', 'found dead',
    'disaster', 'catastrophe', 'crisis',
    'police', 'arrest', 'arrested', 'charged', 'custody', 'detained',
    'shooting', 'shot', 'gunman', 'gunmen', 'armed',
    'stabbing', 'stabbed', 'knife attack',
    'attack', 'attacked', 'assault', 'assaulted', 'bashing', 'bashed',
    'murder', 'murdered', 'homicide', 'manslaughter',
    'terror', 'terrorism', 'terrorist',
    'hostage', 'siege',
    'mauled', 'mauling', 'dog attack', 'bitten',
    'robbery', 'robbed', 'ram raid', 'raid', 'theft', 'stolen',
    'break-in', 'burglary', 'home invasion',
    'carjacking', 'carjacked',
    'drug bust', 'drug raid',
    'blaze', 'bushfire', 'wildfire', 'flames', 'inferno',
    'house fire', 'building fire', 'factory fire', 'car fire',
    'power outage', 'blackout',
    'explosion', 'exploded', 'bomb',
    'derailed', 'derailment',
    'capsized', 'sinking', 'aground', 'mayday',
    'plane crash', 'helicopter crash',
  ],
  weather: [
    'weather forecast', 'weather warning', 'weather conditions', 'weather event',
    'severe weather', 'wet weather', 'wild weather', 'extreme weather',
    'heatwave', 'heat wave', 'cold snap', 'cold front', 'warm front',
    'rainfall', 'downpour', 'heavy rain', 'rain warning',
    'thunderstorm', 'lightning', 'stormy weather',
    'cyclone', 'tropical cyclone', 'tornado', 'hurricane',
    'flood warning', 'flood watch',
    'bom', 'bureau of meteorology',
    'drought', 'dry conditions',
    'snowfall', 'frost warning', 'fog warning', 'hailstorm',
    'temperature', 'celsius', 'humidity',
  ],
  fire: ['fire danger', 'total fire ban', 'fire ban', 'fire risk', 'fire', 'burning'],
  flood: ['flood', 'flooding', 'floodwater', 'floods'],
} as const;

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function wordMatch(keyword: string, text: string): boolean {
  return new RegExp(`\\b${escapeRegex(keyword)}\\b`).test(text);
}

export function detectCategory(title: string, description: string): string {
  const text = `${title} ${description}`.toLowerCase();
  for (const kw of CATEGORY_KEYWORDS.emergency_strong) {
    if (wordMatch(kw, text)) return 'emergency';
  }
  const hasWeather = CATEGORY_KEYWORDS.weather.some((kw) => wordMatch(kw, text));
  const hasFire = CATEGORY_KEYWORDS.fire.some((kw) => wordMatch(kw, text));
  if (hasFire) {
    for (const term of ['fire danger', 'fire ban', 'fire risk', 'fire weather']) {
      if (wordMatch(term, text)) return 'weather';
    }
    return 'emergency';
  }
  const hasFlood = CATEGORY_KEYWORDS.flood.some((kw) => wordMatch(kw, text));
  if (hasFlood) {
    if (wordMatch('warning', text) || wordMatch('watch', text) || hasWeather) {
      return 'weather';
    }
    return 'emergency';
  }
  if (hasWeather) return 'weather';
  return 'general';
}

// ---------------------------------------------------------------------------
// Per-feed exponential backoff (module-scoped — equivalent to python's
// _rss_feed_backoff/_rss_feed_fail_counts dict + lock).
// ---------------------------------------------------------------------------

const BACKOFF_THRESHOLD = 3;
const BACKOFF_STEPS_MS = [
  5 * 60_000,
  15 * 60_000,
  60 * 60_000,
  4 * 60 * 60_000,
] as const;

interface BackoffState {
  parkedUntil: number;
  failCount: number;
}
const feedBackoff = new Map<string, BackoffState>();

export function _resetNewsBackoffForTests(): void {
  feedBackoff.clear();
}

// ---------------------------------------------------------------------------
// Date parsing — RFC 2822 / ISO / common alternate formats (mirrors python's
// parse_rss_date). Returns Unix seconds (matching python's mktime_tz output).
// ---------------------------------------------------------------------------

const RFC2822_MONTHS: Record<string, number> = {
  jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
  jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
};

function parseRfc2822(s: string): number {
  // e.g. "Tue, 21 Apr 2026 13:35:00 +1000"
  const m = s.trim().match(
    /^(?:[A-Za-z]{3},\s*)?(\d{1,2})\s+([A-Za-z]{3})\s+(\d{2,4})\s+(\d{2}):(\d{2})(?::(\d{2}))?\s*([+-]\d{4}|GMT|UT|UTC|[A-Z]{1,3})?/,
  );
  if (!m) return NaN;
  const [, dStr, monStr, yStr, hStr, miStr, sStr, tzStr] = m;
  if (!dStr || !monStr || !yStr || !hStr || !miStr) return NaN;
  const day = Number(dStr);
  const month = RFC2822_MONTHS[monStr.toLowerCase()];
  if (month === undefined) return NaN;
  let year = Number(yStr);
  if (year < 100) year += year < 70 ? 2000 : 1900;
  const hour = Number(hStr);
  const minute = Number(miStr);
  const second = sStr ? Number(sStr) : 0;
  let offsetMin = 0;
  if (tzStr) {
    const off = tzStr.match(/^([+-])(\d{2})(\d{2})$/);
    if (off) {
      const sign = off[1] === '-' ? -1 : 1;
      offsetMin = sign * (Number(off[2]) * 60 + Number(off[3]));
    }
    // Named zones (GMT/UT/UTC) → 0 offset (python mktime_tz behaviour).
  }
  const utcMs = Date.UTC(year, month, day, hour, minute, second) - offsetMin * 60_000;
  return Math.floor(utcMs / 1000);
}

function parseIso(s: string): number {
  const cleaned = s.trim().replace(/Z$/, '+00:00');
  const ms = Date.parse(cleaned);
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : NaN;
}

export function parseRssDate(dateStr: string): number {
  if (!dateStr) return 0;
  const rfc = parseRfc2822(dateStr);
  if (Number.isFinite(rfc)) return rfc;
  const iso = parseIso(dateStr);
  if (Number.isFinite(iso)) return iso;
  return 0;
}

// ---------------------------------------------------------------------------
// Per-feed parser
// ---------------------------------------------------------------------------

export interface NewsItem {
  title: string;
  link: string;
  description: string;
  published: string;
  source: string;
  icon: string;
  category: string;
  timestamp?: number;
}

function stripHtml(s: string): string {
  return s.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
}

function applyBackoffSuccess(url: string): void {
  feedBackoff.delete(url);
}

function applyBackoffFailure(url: string): number {
  const now = Date.now();
  const prev = feedBackoff.get(url) ?? { parkedUntil: 0, failCount: 0 };
  const failCount = prev.failCount + 1;
  let parkedUntil = 0;
  if (failCount >= BACKOFF_THRESHOLD) {
    const stepIdx = Math.min(
      failCount - BACKOFF_THRESHOLD,
      BACKOFF_STEPS_MS.length - 1,
    );
    parkedUntil = now + (BACKOFF_STEPS_MS[stepIdx] ?? 0);
  }
  feedBackoff.set(url, { parkedUntil, failCount });
  return parkedUntil;
}

async function parseRssFeed(
  url: string,
  sourceName: string,
  sourceIcon: string,
): Promise<NewsItem[]> {
  const now = Date.now();
  const back = feedBackoff.get(url);
  if (back && back.parkedUntil > now) return [];

  let raw;
  try {
    raw = await fetchRaw(url, {
      timeoutMs: 10_000,
      headers: {
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        Accept: 'application/rss+xml, application/xml, text/xml, */*',
      },
      allow_non_2xx: true,
    });
  } catch {
    applyBackoffFailure(url);
    return [];
  }
  if (raw.status !== 200) {
    applyBackoffFailure(url);
    return [];
  }
  applyBackoffSuccess(url);

  let parsed: Record<string, unknown>;
  try {
    parsed = parseXml(raw.text);
  } catch {
    return [];
  }

  // Try RSS 2.0 first.
  const rss = (parsed['rss'] as Record<string, unknown> | undefined) ?? parsed;
  const channel =
    rss && typeof rss === 'object'
      ? ((rss as Record<string, unknown>)['channel'] as
          | Record<string, unknown>
          | undefined)
      : undefined;

  const items: NewsItem[] = [];

  if (channel) {
    const rawItems = asArray(channel['item']) as Array<Record<string, unknown>>;
    for (const item of rawItems.slice(0, 10)) {
      const title = textOf(item, 'title').trim();
      const link = textOf(item, 'link').trim();
      const rawDesc = textOf(item, 'description');
      const description = rawDesc ? stripHtml(rawDesc).slice(0, 300) : '';
      const published = textOf(item, 'pubDate').trim();
      items.push({
        title,
        link,
        description,
        published,
        source: sourceName,
        icon: sourceIcon,
        category: detectCategory(title, description),
      });
    }
    return items;
  }

  // Atom fallback.
  const feed =
    (parsed['feed'] as Record<string, unknown> | undefined) ??
    (parsed as Record<string, unknown>);
  const entries = asArray(feed['entry']) as Array<Record<string, unknown>>;
  for (const entry of entries.slice(0, 10)) {
    const title = textOf(entry, 'title').trim();
    let link = '';
    const linkRaw = entry['link'];
    if (linkRaw && typeof linkRaw === 'object' && '@_href' in linkRaw) {
      link = String((linkRaw as Record<string, unknown>)['@_href'] ?? '');
    } else {
      link = textOf(entry, 'link').trim();
    }
    const summary = textOf(entry, 'summary');
    const description = summary ? stripHtml(summary).slice(0, 300) : '';
    const published =
      textOf(entry, 'published').trim() || textOf(entry, 'updated').trim();
    items.push({
      title,
      link,
      description,
      published,
      source: sourceName,
      icon: sourceIcon,
      category: detectCategory(title, description),
    });
  }
  return items;
}

// ---------------------------------------------------------------------------
// Aggregation entrypoint used by the route handler
// ---------------------------------------------------------------------------

export interface AggregateQuery {
  sources?: string;
  category?: string;
  limit?: number;
}

export interface AggregateResponse {
  items: NewsItem[];
  count: number;
  sources: Record<
    string,
    { name: string; count: number; status: string; error?: string }
  >;
  category_counts: { general: number; emergency: number; weather: number };
  available_sources: string[];
  available_categories: string[];
}

export async function aggregateNews(
  q: AggregateQuery = {},
): Promise<AggregateResponse> {
  const limitRaw = q.limit ?? 8;
  const limit = Math.max(1, Math.min(20, Number(limitRaw) || 8));
  const requested = (q.sources ?? '').trim();
  const feedsToFetch: Record<string, RssFeed> = {};
  if (requested) {
    for (const key of requested.split(',').map((s) => s.trim())) {
      const f = RSS_FEEDS[key];
      if (f) feedsToFetch[key] = f;
    }
  } else {
    Object.assign(feedsToFetch, RSS_FEEDS);
  }

  const entries = Object.entries(feedsToFetch);
  const settled = await Promise.allSettled(
    entries.map(([, feed]) => parseRssFeed(feed.url, feed.name, feed.icon)),
  );

  const sourcesStatus: AggregateResponse['sources'] = {};
  const allItems: NewsItem[] = [];
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    const result = settled[i];
    if (!entry || !result) continue;
    const [key, feed] = entry;
    if (result.status === 'fulfilled') {
      const arr = result.value;
      sourcesStatus[key] = {
        name: feed.name,
        count: arr.length,
        status: arr.length > 0 ? 'ok' : 'empty',
      };
      for (const item of arr.slice(0, limit)) {
        allItems.push(item);
      }
    } else {
      sourcesStatus[key] = {
        name: feed.name,
        count: 0,
        status: 'error',
        error: String(result.reason ?? 'unknown'),
      };
    }
  }

  for (const item of allItems) {
    item.timestamp = parseRssDate(item.published);
  }
  allItems.sort((a, b) => (b.timestamp ?? 0) - (a.timestamp ?? 0));

  let filtered = allItems;
  if (q.category) {
    filtered = allItems.filter((it) => it.category === q.category);
  }

  const category_counts = { general: 0, emergency: 0, weather: 0 };
  for (const it of filtered) {
    if (it.category in category_counts) {
      category_counts[it.category as keyof typeof category_counts] += 1;
    }
  }

  return {
    items: filtered,
    count: filtered.length,
    sources: sourcesStatus,
    category_counts,
    available_sources: Object.keys(RSS_FEEDS),
    available_categories: ['general', 'emergency', 'weather'],
  };
}

// No source registration — /api/news/rss is on-demand. The python
// equivalent uses a 5 min response cache; route handler wraps a tiny
// TTL cache around aggregateNews().
export default function register(): void {
  // intentionally empty — kept for API-symmetry with other source modules.
}
