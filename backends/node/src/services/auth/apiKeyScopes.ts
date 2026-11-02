/**
 * What a named API key may read — its scopes.
 *
 * A scope is one feed and one window, `feed:window` like the role names
 * (services/auth/roles.ts):
 *
 *   fires:live      the fire services' incidents as the map shows them now
 *   fires:history   the same incidents as stored over time
 *
 * FEEDS lists the feeds and which windows each has: `live` is what the map
 * shows now, polled each minute and cached; `history` is stored records over
 * time. Staff grant a key a set of feed/window cells (the table on the staff
 * page), and a key reads an endpoint when it holds the cell the endpoint
 * declares (`scoped()` below, on the route itself). A key always reads, never
 * writes. There are no wildcards: a feed added later is granted to nobody
 * until staff tick it.
 *
 * The literal scope `read` is the old rule, kept for every key made before
 * scopes were enforced: GET or HEAD on the fixed list of upstream feeds in
 * LEGACY_READ_PREFIXES, nothing else. A key whose scopes hold `read` is judged
 * by that list and nothing else — so turning scopes on changed nothing for
 * existing keys. A scope this table does not know grants nothing.
 *
 * What is deliberately NOT a feed: anything about people or nodes, this
 * site's own reference data, and other people's pictures and words. Live data
 * is what the public map shows; node status (receivers, monitored sites),
 * profiles, accounts, user-submitted incidents, site statistics, boundaries,
 * the radio site list, reception transcripts, camera images (traffic, flood,
 * fire-tower, airport) and news outlets' RSS feeds are never declared, so no
 * scope reaches them.
 *
 * The feed list lives here, not in shared/alert-catalog.json: that file is
 * what the Discord bot and its dashboard alert on, and not every source is an
 * alert on purpose. A test keeps the two aligned where they overlap.
 */
import type { Context, MiddlewareHandler } from 'hono';
import { matchedRoutes } from 'hono/route';
import { unversionedPath } from '../../lib/apiVersion.js';

export type ScopeWindow = 'live' | 'history';

export interface Feed {
  /** What staff and users see in the table. */
  label: string;
  /** What the feed takes in, in a line, for the table's tooltip and the docs. */
  covers: string;
  /** The windows an endpoint of this feed serves; the table offers only these. */
  windows: readonly ScopeWindow[];
}

/** The feeds a key can be granted, in the order the table lists them. */
export const FEEDS = {
  fires: {
    label: 'Fires',
    covers:
      'Every fire service in one feed (NSW RFS, QLD Fire Department, VIC Emergency, WA DFES, SA CFS and MFS, NT Fire and Rescue) with the QLD Fire Department and WA DFES public warnings, satellite hotspots and fire danger ratings',
    windows: ['live', 'history'],
  },
  warnings: {
    label: 'Weather warnings',
    covers: 'Bureau of Meteorology land and marine warnings',
    windows: ['live', 'history'],
  },
  ambulance: { label: 'ACT Ambulance', covers: 'ACT Ambulance Service incidents', windows: ['live', 'history'] },
  outages: {
    label: 'Power outages',
    covers: 'Every electricity distributor in one feed (Endeavour Energy, Essential Energy, Ausgrid), unplanned and planned',
    windows: ['live', 'history'],
  },
  roads: {
    label: 'Road hazards',
    covers:
      'Road incidents, roadworks, flooded roads and major events in NSW (Live Traffic NSW), QLD, VIC, SA and the ACT (each from its own feed), and NSW road fires, alpine conditions and council works',
    windows: ['live', 'history'],
  },
  transport: { label: 'Public transport', covers: 'Vehicles, stops, lines, trips, departures and alerts', windows: ['live'] },
  aircraft: { label: 'Aircraft', covers: 'ADS-B positions, trails and photos; history replays past positions', windows: ['live', 'history'] },
  ships: { label: 'Ships', covers: 'AIS vessel positions and details', windows: ['live'] },
  weather: { label: 'Weather', covers: 'Current conditions, radar, the forecast grid and point forecasts', windows: ['live'] },
  beaches: {
    label: 'Beaches & sharks',
    covers: 'Beachwatch water quality, BeachSafe conditions and SharkSmart reports',
    windows: ['live', 'history'],
  },
  pager: { label: 'Pager messages', covers: 'Pager hits from the Pagermon relay', windows: ['live', 'history'] },
  summaries: { label: 'Radio summaries', covers: 'The hourly and ad-hoc radio summaries', windows: ['history'] },
} as const satisfies Readonly<Record<string, Feed>>;

export type FeedKey = keyof typeof FEEDS;

const isFeed = (key: string): key is FeedKey => Object.prototype.hasOwnProperty.call(FEEDS, key);

/** The pre-scope rule, held by every key made before scopes were enforced. */
export const LEGACY_READ = 'read';

/** Every cell of the table is about thirty; this is just a sanity bound. */
const MAX_SCOPES = 64;

/**
 * What one endpoint serves: a feed and a window. A feed of '*' is a route
 * spanning feeds (stored history): any scope in that window gets in, and the
 * route serves only what the key's scopes cover (historySourcesFor below).
 */
export interface ScopeNeed {
  feed: FeedKey | '*';
  window: ScopeWindow;
}

/** The one need every valid key satisfies (its own API description). */
export const ANY_KEY = Object.freeze({ anyKey: true as const });
export type RouteNeed = ScopeNeed | typeof ANY_KEY;

const SCOPE_RE = /^([a-z_]+):(live|history)$/;

/** A scope in the feed:window form, if it is one the table offers. */
export function parseScope(scope: string): { feed: FeedKey; window: ScopeWindow } | null {
  const m = SCOPE_RE.exec(scope);
  if (!m || !isFeed(m[1]!)) return null;
  const feed = m[1];
  const window = m[2] as ScopeWindow;
  return (FEEDS[feed].windows as readonly ScopeWindow[]).includes(window) ? { feed, window } : null;
}

/** Trimmed, lower-case, de-duplicated and sorted, so equal sets compare equal. */
export function canonicalScopes(list: readonly string[]): string[] {
  return [...new Set(list.map((s) => s.trim().toLowerCase()).filter(Boolean))].sort();
}

/**
 * Judged by the old feed list: it holds `read`, which every key made before
 * scopes has, and keeps exactly the access it always had. Nothing else is:
 * a scope this table does not know — the retired source:/type:/window: form,
 * or a typo issued by hand — grants nothing, rather than falling back to a
 * list wider than whatever it was meant to be.
 */
export function isLegacyScopes(scopes: readonly string[]): boolean {
  return scopes.includes(LEGACY_READ);
}

export type ScopesCheck = { ok: true; scopes: string[] } | { ok: false; error: string };

/**
 * A scope list from a request body. Every entry must be a cell the table
 * offers, and `read` stands alone. `allowLegacy` is for staff: a user cannot
 * ask for the old blanket rule, staff may grant it. `allowEmpty` is for the
 * user's request, where asking for scopes is optional.
 */
export function validateScopes(
  raw: unknown,
  opts: { allowLegacy: boolean; allowEmpty: boolean },
): ScopesCheck {
  if (raw == null) {
    return opts.allowEmpty ? { ok: true, scopes: [] } : { ok: false, error: 'scopes are required' };
  }
  if (!Array.isArray(raw) || raw.some((s) => typeof s !== 'string')) {
    return { ok: false, error: 'scopes must be a list of strings' };
  }
  const scopes = canonicalScopes(raw as string[]);
  if (!scopes.length) {
    return opts.allowEmpty ? { ok: true, scopes } : { ok: false, error: 'scopes are required' };
  }
  if (scopes.length > MAX_SCOPES) return { ok: false, error: `at most ${MAX_SCOPES} scopes` };
  if (scopes.includes(LEGACY_READ)) {
    if (!opts.allowLegacy) return { ok: false, error: `"${LEGACY_READ}" can only be granted by staff` };
    if (scopes.length > 1) return { ok: false, error: `"${LEGACY_READ}" cannot be combined with other scopes` };
    return { ok: true, scopes };
  }
  const bad = scopes.filter((s) => parseScope(s) === null);
  if (bad.length) return { ok: false, error: `unknown scope${bad.length === 1 ? '' : 's'}: ${bad.join(', ')}` };
  return { ok: true, scopes };
}

/**
 * The scopes that satisfy a need, as people write them — for messages and the
 * API description. A route spanning feeds is met by any scope in its window,
 * written `*:history`.
 */
export function needScopes(need: RouteNeed): string[] {
  if ('anyKey' in need) return [];
  return [`${need.feed}:${need.window}`];
}

/** The need an `x-scopes` entry of the API description stands for (needScopes, read back). */
export function needFromScope(scope: string): ScopeNeed | null {
  const m = /^(\*|[a-z_]+):(live|history)$/.exec(scope);
  if (!m) return null;
  if (m[1] === '*') return { feed: '*', window: m[2] as ScopeWindow };
  const parsed = parseScope(scope);
  return parsed && { feed: parsed.feed, window: parsed.window };
}

// ---------------------------------------------------------------------------
// Stored history: which archived sources a key may read.
// ---------------------------------------------------------------------------

/**
 * Every `source` value the archive stores (archive_* tables, the column
 * /api/data/history filters on), and the feed it is the history of. A key
 * reads an archived source's records when it holds that feed's history cell.
 *
 * Left out on purpose, so no key ever reads them through history:
 * `user_incident` (people's own reports, carrying user ids),
 * `radio_summary` (served by /api/summaries under its own feed), and the
 * retired `essential_energy_cancelled`.
 */
export const HISTORY_SOURCES: Readonly<Record<string, FeedKey>> = {
  rfs: 'fires',
  nt_fire: 'fires',
  qld_fire: 'fires',
  vic_emergency: 'fires',
  wa_incident: 'fires',
  sa_cfs: 'fires',
  sa_mfs: 'fires',
  qld_warning: 'fires',
  wa_warning: 'fires',
  bom_land: 'warnings',
  bom_marine: 'warnings',
  act_ambulance: 'ambulance',
  traffic_incident: 'roads',
  traffic_roadwork: 'roads',
  traffic_flood: 'roads',
  traffic_fire: 'roads',
  traffic_majorevent: 'roads',
  traffic_alpine: 'roads',
  traffic_lga: 'roads',
  traffic_works: 'roads',
  endeavour_current: 'outages',
  endeavour_planned: 'outages',
  endeavour_maintenance: 'outages',
  ausgrid: 'outages',
  essential_current: 'outages',
  essential_planned: 'outages',
  essential_future: 'outages',
  dorsal: 'beaches',
  pager: 'pager',
};

/**
 * The archived sources a key in the new form may read through stored history:
 * those of every feed whose history cell it holds. Empty for a `read` key —
 * the old feed list never included history.
 */
export function historySourcesFor(scopes: readonly string[]): string[] {
  if (isLegacyScopes(scopes)) return [];
  const held = new Set(scopes);
  return Object.entries(HISTORY_SOURCES)
    .filter(([, feed]) => held.has(`${feed}:history`))
    .map(([archived]) => archived);
}

/** Does a key in the new form satisfy this need? */
export function scopesSatisfy(scopes: readonly string[], need: RouteNeed): boolean {
  if ('anyKey' in need) return true;
  // A route spanning feeds (stored history) admits a key holding that window
  // of a feed it can actually answer for — aircraft:history or
  // summaries:history alone would get in only to be refused every record.
  if (need.feed === '*') {
    return scopes.some((s) => {
      const p = parseScope(s);
      return p !== null && p.window === need.window && SPANNED_FEEDS.has(p.feed);
    });
  }
  return scopes.includes(`${need.feed}:${need.window}`);
}

/** The feeds a route spanning feeds answers for: those the archive stores. */
const SPANNED_FEEDS: ReadonlySet<FeedKey> = new Set(Object.values(HISTORY_SOURCES));

// ---------------------------------------------------------------------------
// The old rule, for `read` keys.
// ---------------------------------------------------------------------------

/**
 * What a `read` key may reach: the upstream data feeds, read-only.
 *
 * A key is a polling credential for somebody's script. It is NOT a login, and
 * passing the credential gate was never meant to mean "everything a page can
 * see" — but that is what it meant, because the gate is all most routes have.
 * An audit of the registered routes found a key could read the complete role
 * matrix of any account (/api/check-admin/:userId), read anyone's profile,
 * enumerate every live visitor session, walk the whole rdio corpus with its
 * transcripts and audio links, spend the server's paid what3words quota, post
 * an unbounded takedown notice into the private staff Discord channel, inflate
 * any contributor's view counts, and push 25 MB of audio at the GPU
 * transcription backends — none of which is polling a data feed.
 *
 * So the class was confined at the one chokepoint: a key may only make a GET or
 * HEAD, and only to a path on this list. The list is upstream sources — what
 * the feeds publish — plus two the owner asked for by name. It deliberately
 * does NOT include this site's own content or telemetry. Scoped keys replaced
 * it for new keys; this list is frozen as what `read` means, but for one
 * narrowing (owner, 2026-11-02): no key reads camera images or news outlets'
 * feeds, so the camera and news routes left it (/api/qld/ held only the QLD
 * cameras, /api/aviation/ only the airport ones).
 */
const LEGACY_READ_PREFIXES: readonly string[] = [
  // Emergency services, state by state.
  '/api/rfs/',
  '/api/act-ambulance/',
  '/api/qld-fire/',
  '/api/nt-fire/',
  '/api/sa-fire/',
  '/api/vic-emergency/',
  '/api/wa-emergency/',
  '/api/firms/',
  // Roads (not /api/traffic/cameras: LEGACY_READ_EXCLUDED), transport,
  // aircraft, marine (AIS).
  '/api/traffic/',
  '/api/transport/',
  '/api/adsb/',
  '/api/marinetraffic/',
  // Weather, fire danger, water.
  '/api/weather/',
  '/api/bom/',
  '/api/beachsafe',
  '/api/beachwatch',
  '/api/dorsal/',
  // Power outages.
  '/api/ausgrid/',
  '/api/endeavour/',
  '/api/essential/',
  // Reference geometry.
  '/api/boundaries/',
  // Pager traffic from the owner's own Pagermon relay, and the public radio
  // site reference — both included at the owner's request.
  '/api/pager/',
  '/api/radio/',
];

/** Exact paths, for feeds whose route has no trailing segment. */
const LEGACY_READ_PATHS: ReadonlySet<string> = new Set([
  '/api/beachsafe',
  '/api/beachwatch',
  // The fire services merged (api/fires.ts), and the electricity distributors
  // (api/outages.ts): nothing a `read` key could not already read one agency
  // at a time, and where /api/v1/ serves them now.
  '/api/fires',
  '/api/outages',
]);

/** Under a prefix above, but camera images: no key reads them. */
const LEGACY_READ_EXCLUDED: readonly string[] = ['/api/traffic/cameras'];

function legacyReadAllows(method: string, path: string): boolean {
  if (method !== 'GET' && method !== 'HEAD') return false;
  // The versioned path of a feed is the same feed: `read` reaches both.
  const p = unversionedPath(path);
  if (LEGACY_READ_EXCLUDED.some((x) => p === x || p.startsWith(`${x}/`))) return false;
  if (LEGACY_READ_PATHS.has(p)) return true;
  return LEGACY_READ_PREFIXES.some((prefix) => p.startsWith(prefix));
}

// ---------------------------------------------------------------------------
// Declaring a route's need, and finding it at the gate.
// ---------------------------------------------------------------------------

const SCOPE = Symbol.for('ausaware.apiKeyScope');
/** Hono wraps a sub-app's handlers in this when the sub-app has its own onError. */
const COMPOSED_HANDLER = '__COMPOSED_HANDLER';

type ScopedHandler = MiddlewareHandler & { [SCOPE]?: RouteNeed };

const declared = new Map<string, ScopeNeed>();
/** The windows a route spanning feeds (stored history) has been declared for. */
const spanning = new Set<ScopeWindow>();

/**
 * Declare what a route serves, as its first middleware:
 *
 *   firesRouter.get('/api/v1/fires', scoped({ feed: 'fires', window: 'live' }), (c) => …)
 *
 * It only annotates — the request goes straight on. Enforcing it here would
 * be too late: the credential gate (apiKey.ts) has to refuse before any
 * handler runs, and after the key's rate limit, so a refused path still
 * spends the key's budget. The gate finds the declaration on the matched
 * route chain (routeNeed below). Unlike requireRole, which decides in the
 * middleware itself, this is a label the gate reads.
 *
 * A route without a declaration is out of reach of every scoped key. When a
 * route carries more than one, the last on its chain counts.
 */
export function scoped(need: RouteNeed): MiddlewareHandler {
  const mw: ScopedHandler = async (_c, next) => {
    await next();
  };
  mw[SCOPE] = need;
  if ('anyKey' in need) return mw;
  if (need.feed === '*') spanning.add(need.window);
  else declared.set(`${need.feed}:${need.window}`, need);
  return mw;
}

/**
 * Every feed/window cell a route serves so far — the cells a key can actually
 * use. A route spanning feeds (stored history) stands for the history of each
 * feed the archive holds, so it is listed as those.
 */
export function declaredNeeds(): Array<{ feed: FeedKey; window: ScopeWindow }> {
  const out = new Map<string, { feed: FeedKey; window: ScopeWindow }>();
  for (const [k, n] of declared) if (n.feed !== '*') out.set(k, { feed: n.feed, window: n.window });
  for (const window of spanning) {
    for (const feed of new Set(Object.values(HISTORY_SOURCES))) out.set(`${feed}:${window}`, { feed, window });
  }
  return [...out.values()];
}

/** The need a handler declares, if it is a scoped() label (through Hono's wrapper). */
function needOf(handler: unknown): RouteNeed | null {
  let h = handler as ScopedHandler & Record<string, unknown>;
  while (typeof h[COMPOSED_HANDLER] === 'function') h = h[COMPOSED_HANDLER] as typeof h;
  return h[SCOPE] ?? null;
}

/** The need a chain of handlers declares: the last declaration on it. */
export function declaredNeedOf(handlers: readonly unknown[]): RouteNeed | null {
  let need: RouteNeed | null = null;
  for (const h of handlers) need = needOf(h) ?? need;
  return need;
}

/**
 * The matched route's declared need, and whether any route (not just
 * middleware) matched at all. Hono resolves the whole handler chain before the
 * first middleware runs, so the gate can read it without an index of its own.
 */
export function routeNeed(c: Context): { matched: boolean; need: RouteNeed | null } {
  const routes = matchedRoutes(c);
  return {
    matched: routes.some((r) => r.method !== 'ALL'),
    need: declaredNeedOf(routes.map((r) => r.handler)),
  };
}

export type ScopeDecision =
  | { ok: true }
  | { ok: false; error: 'api_key_path_forbidden' | 'api_key_scope_forbidden'; message: string };

const LEGACY_REFUSAL = {
  ok: false as const,
  error: 'api_key_path_forbidden' as const,
  message: 'API keys may only read the upstream data feeds (GET). This path is not one of them.',
};

/**
 * May a key with these scopes make this request? `route` is routeNeed(c).
 *
 * A key in the new form that asks for a path no route serves is let through,
 * so the not-found answer says so — a typo is a 404, not a refusal. A route
 * declared ANY_KEY (the API description) is readable by every key, `read`
 * included.
 */
export function keyMayRequest(
  scopes: readonly string[],
  method: string,
  path: string,
  route: { matched: boolean; need: RouteNeed | null },
): ScopeDecision {
  const reading = method === 'GET' || method === 'HEAD';
  if (reading && route.need && 'anyKey' in route.need) return { ok: true };
  if (isLegacyScopes(scopes)) return legacyReadAllows(method, path) ? { ok: true } : LEGACY_REFUSAL;

  if (!reading) {
    return { ok: false, error: 'api_key_scope_forbidden', message: 'API keys only read (GET).' };
  }
  if (!route.matched) return { ok: true };
  if (!route.need) {
    return {
      ok: false,
      error: 'api_key_scope_forbidden',
      message: 'This path is not part of the data API keys can read.',
    };
  }
  const need = route.need;
  if (scopesSatisfy(scopes, need)) return { ok: true };
  if ('anyKey' in need) return { ok: true };
  const message =
    need.feed === '*'
      ? `This key's scopes do not cover this endpoint. It needs the ${need.window} of at least one of: ${[...SPANNED_FEEDS].map((f) => `${f}:${need.window}`).join(', ')}.`
      : `This key's scopes do not cover this endpoint. It needs ${needScopes(need).join(', ')}.`;
  return { ok: false, error: 'api_key_scope_forbidden', message };
}

/**
 * Would a key with these scopes be let through to GET this route? The same
 * answer the gate gives, for a route rather than a request — what the API
 * description shows a key (api/v1/openapi.ts). `path` may be a template
 * ('/api/v1/agency/extended/{slug}'); the old feed list matches by prefix, so a
 * template reads the same as any path it stands for.
 */
export function keyCanRead(scopes: readonly string[], path: string, need: RouteNeed | null): boolean {
  return keyMayRequest(scopes, 'GET', path, { matched: true, need }).ok;
}
