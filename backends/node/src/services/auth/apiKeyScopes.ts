/**
 * What a named API key may read — its scopes.
 *
 * A scope is one of three dimensions, `main:sub` like the role names
 * (services/auth/roles.ts):
 *
 *   source:<key>    whose data — rfs, livetraffic, adsb, pager, … (SCOPE_SOURCES)
 *   type:<family>   what kind — incidents, warnings, outages, … (SCOPE_TYPES)
 *   window:live     what the map shows now, polled each minute and cached
 *   window:history  stored records over time
 *
 * and any dimension may be `*`. An endpoint declares the source, type and
 * window it serves (`scoped()` below, on the route itself), and a key is let
 * through when it matches all three. A key always reads, never writes.
 *
 * The literal scope `read` is the old rule, kept for every key made before
 * scopes were enforced: GET or HEAD on the fixed list of upstream feeds in
 * LEGACY_READ_PREFIXES, nothing else. A key whose scopes hold `read`, or hold
 * no source:/type:/window: scope at all, is judged by that list and nothing
 * else — so turning scopes on changed nothing for existing keys.
 *
 * What is deliberately NOT a scope: anything about people or nodes. Live data
 * is what the public map shows; node status (receivers, monitored sites),
 * profiles, accounts, user-submitted incidents and site statistics are never
 * declared, so no scope reaches them.
 *
 * The source list lives here, not in shared/alert-catalog.json: that file is
 * what the Discord bot and its dashboard alert on, and not every source is an
 * alert on purpose. A test keeps the two aligned where they overlap.
 */
import type { Context, MiddlewareHandler } from 'hono';
import { matchedRoutes } from 'hono/route';
import { unversionedPath } from '../../lib/apiVersion.js';

/** Sources a key can be scoped to. Labels are what staff and users see. */
export const SCOPE_SOURCES: Readonly<Record<string, string>> = {
  rfs: 'NSW Rural Fire Service',
  firms: 'NASA FIRMS hotspots',
  vic: 'VIC Emergency',
  qfd: 'QLD Fire Department',
  dfes: 'WA DFES',
  safire: 'SA CFS / MFS',
  ntfrs: 'NT Fire and Rescue',
  actas: 'ACT Ambulance',
  bom: 'Bureau of Meteorology',
  livetraffic: 'Live Traffic NSW',
  transport: 'Public transport',
  adsb: 'Aircraft (ADS-B)',
  aviation: 'Airport cameras',
  marinetraffic: 'Ships (AIS)',
  weather: 'Weather',
  beach: 'Beachwatch and BeachSafe',
  dorsal: 'Shark reports',
  endeavour: 'Endeavour Energy',
  ausgrid: 'Ausgrid',
  essential: 'Essential Energy',
  centralwatch: 'Fire-tower cameras',
  qldtraffic: 'QLD Traffic cameras',
  news: 'News',
  boundaries: 'Boundaries',
  pager: 'Pager messages',
  radio: 'Radio site reference',
  rdio: 'Radio summaries and transcripts',
};

/** Kinds of data. */
export const SCOPE_TYPES: Readonly<Record<string, string>> = {
  incidents: 'Incidents',
  warnings: 'Warnings',
  outages: 'Power outages',
  hazards: 'Hazards',
  cameras: 'Cameras',
  weather: 'Weather',
  transit: 'Public transport',
  aircraft: 'Aircraft',
  vessels: 'Ships',
  pager: 'Pager messages',
  radio: 'Radio sites',
  summaries: 'Radio summaries',
  transcripts: 'Reception transcripts',
  news: 'News',
  boundaries: 'Boundaries',
};

export const SCOPE_WINDOWS: Readonly<Record<'live' | 'history', string>> = {
  live: 'Live — what the map shows now',
  history: 'History — stored records over time',
};

export type ScopeDim = 'source' | 'type' | 'window';
const DIMS: readonly ScopeDim[] = ['source', 'type', 'window'];
const VALUES: Record<ScopeDim, Readonly<Record<string, string>>> = {
  source: SCOPE_SOURCES,
  type: SCOPE_TYPES,
  window: SCOPE_WINDOWS,
};

/** The pre-scope rule, held by every key made before scopes were enforced. */
export const LEGACY_READ = 'read';

/** At most this many scopes on one key — the whole taxonomy is about fifty. */
const MAX_SCOPES = 64;

/**
 * What one endpoint serves. `source`/`type` are a key of their table; a
 * window of 'any' is reference data that fits either window (a key with any
 * window scope passes).
 */
export interface ScopeNeed {
  source: string;
  type: string;
  window: 'live' | 'history' | 'any';
}

/** The one need every valid key satisfies (its own API description). */
export const ANY_KEY = Object.freeze({ anyKey: true as const });
export type RouteNeed = ScopeNeed | typeof ANY_KEY;

const SCOPE_RE = /^(source|type|window):([a-z0-9_]+|\*)$/;

function parse(scope: string): { dim: ScopeDim; value: string } | null {
  const m = SCOPE_RE.exec(scope);
  if (!m) return null;
  const dim = m[1] as ScopeDim;
  const value = m[2]!;
  if (value !== '*' && !Object.prototype.hasOwnProperty.call(VALUES[dim], value)) return null;
  return { dim, value };
}

/** Trimmed, lower-case, de-duplicated and sorted, so equal sets compare equal. */
export function canonicalScopes(list: readonly string[]): string[] {
  return [...new Set(list.map((s) => s.trim().toLowerCase()).filter(Boolean))].sort();
}

/**
 * Judged by the old feed list: it holds `read`, or nothing in the new form —
 * a key from before scopes, or one issued by hand with a scope that never
 * meant anything. Such a key keeps exactly the access it always had.
 */
export function isLegacyScopes(scopes: readonly string[]): boolean {
  if (scopes.includes(LEGACY_READ)) return true;
  return !scopes.some((s) => parse(s) !== null);
}

export type ScopesCheck = { ok: true; scopes: string[] } | { ok: false; error: string };

/**
 * A scope list from a request body. Every entry must be a known scope; a set
 * in the new form needs at least one scope in each dimension (a key with no
 * window could never read anything), and `read` stands alone. `allowLegacy`
 * is for staff: a user cannot ask for the old blanket rule, staff may grant it.
 * `allowEmpty` is for the user's request, where asking for scopes is optional.
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
  const bad = scopes.filter((s) => parse(s) === null);
  if (bad.length) return { ok: false, error: `unknown scope${bad.length === 1 ? '' : 's'}: ${bad.join(', ')}` };
  const missing = DIMS.filter((d) => !scopes.some((s) => s.startsWith(`${d}:`)));
  if (missing.length) {
    return { ok: false, error: `scopes need at least one ${missing.map((d) => `${d}:`).join(', ')} scope` };
  }
  return { ok: true, scopes };
}

/** The scopes that satisfy a need, as people write them — for messages and the API description. */
export function needScopes(need: RouteNeed): string[] {
  if ('anyKey' in need) return [];
  return [
    `source:${need.source}`,
    `type:${need.type}`,
    need.window === 'any' ? 'window:live|history' : `window:${need.window}`,
  ];
}

function dimAllows(held: ReadonlySet<string>, dim: ScopeDim, value: string): boolean {
  if (held.has(`${dim}:*`)) return true;
  if (dim === 'window' && value === 'any') return [...held].some((s) => s.startsWith('window:'));
  // A route that spans that dimension (stored history spans sources and
  // types): any scope in it gets in, and the route serves only what the
  // key's scopes cover (historySourcesFor below).
  if (value === '*') return [...held].some((s) => s.startsWith(`${dim}:`));
  return held.has(`${dim}:${value}`);
}

// ---------------------------------------------------------------------------
// Stored history: which archived sources a key may read.
// ---------------------------------------------------------------------------

/**
 * Every `source` value the archive stores (archive_* tables, the column
 * /api/data/history filters on), and the scope source and type it is the
 * history of. A key reads an archived source's records when its scopes cover
 * that source and type, and window:history — the same rule as a live route.
 *
 * Left out on purpose, so no key ever reads them through history:
 * `user_incident` (people's own reports, carrying user ids),
 * `radio_summary` (served by /api/summaries under its own scope), and the
 * retired `essential_energy_cancelled`.
 */
export const HISTORY_SOURCES: Readonly<Record<string, { source: string; type: string }>> = {
  rfs: { source: 'rfs', type: 'incidents' },
  nt_fire: { source: 'ntfrs', type: 'incidents' },
  qld_fire: { source: 'qfd', type: 'incidents' },
  qld_warning: { source: 'qfd', type: 'warnings' },
  vic_emergency: { source: 'vic', type: 'incidents' },
  wa_incident: { source: 'dfes', type: 'incidents' },
  wa_warning: { source: 'dfes', type: 'warnings' },
  sa_cfs: { source: 'safire', type: 'incidents' },
  sa_mfs: { source: 'safire', type: 'incidents' },
  act_ambulance: { source: 'actas', type: 'incidents' },
  bom_land: { source: 'bom', type: 'warnings' },
  bom_marine: { source: 'bom', type: 'warnings' },
  traffic_incident: { source: 'livetraffic', type: 'hazards' },
  traffic_roadwork: { source: 'livetraffic', type: 'hazards' },
  traffic_flood: { source: 'livetraffic', type: 'hazards' },
  traffic_fire: { source: 'livetraffic', type: 'hazards' },
  traffic_majorevent: { source: 'livetraffic', type: 'hazards' },
  traffic_alpine: { source: 'livetraffic', type: 'hazards' },
  traffic_lga: { source: 'livetraffic', type: 'hazards' },
  traffic_works: { source: 'livetraffic', type: 'hazards' },
  endeavour_current: { source: 'endeavour', type: 'outages' },
  endeavour_planned: { source: 'endeavour', type: 'outages' },
  endeavour_maintenance: { source: 'endeavour', type: 'outages' },
  ausgrid: { source: 'ausgrid', type: 'outages' },
  essential_current: { source: 'essential', type: 'outages' },
  essential_planned: { source: 'essential', type: 'outages' },
  essential_future: { source: 'essential', type: 'outages' },
  dorsal: { source: 'dorsal', type: 'hazards' },
  pager: { source: 'pager', type: 'pager' },
};

/**
 * The archived sources a key in the new form may read through stored history:
 * those whose scope source and type it covers, when it holds window:history.
 * Empty for a `read` key — the old feed list never included history.
 */
export function historySourcesFor(scopes: readonly string[]): string[] {
  if (isLegacyScopes(scopes)) return [];
  const held = new Set(scopes);
  if (!dimAllows(held, 'window', 'history')) return [];
  return Object.entries(HISTORY_SOURCES)
    .filter(([, s]) => dimAllows(held, 'source', s.source) && dimAllows(held, 'type', s.type))
    .map(([archived]) => archived);
}

/** Does a key in the new form satisfy this need? */
export function scopesSatisfy(scopes: readonly string[], need: RouteNeed): boolean {
  if ('anyKey' in need) return true;
  const held = new Set(scopes);
  return dimAllows(held, 'source', need.source)
    && dimAllows(held, 'type', need.type)
    && dimAllows(held, 'window', need.window);
}

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
 * it for new keys; this list is frozen as what `read` means.
 */
const LEGACY_READ_PREFIXES: readonly string[] = [
  // Emergency services, state by state.
  '/api/rfs/',
  '/api/act-ambulance/',
  '/api/qld-fire/',
  '/api/qld/',
  '/api/nt-fire/',
  '/api/sa-fire/',
  '/api/vic-emergency/',
  '/api/wa-emergency/',
  '/api/firms/',
  // Roads, transport, aviation, marine (AIS).
  '/api/traffic/',
  '/api/transport/',
  '/api/adsb/',
  '/api/aviation/',
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
  // Fire-tower cameras, news, reference geometry.
  '/api/centralwatch/',
  '/api/news/',
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
]);

function legacyReadAllows(method: string, path: string): boolean {
  if (method !== 'GET' && method !== 'HEAD') return false;
  // The versioned path of a feed is the same feed: `read` reaches both.
  const p = unversionedPath(path);
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
/** Whether a route spanning sources (stored history) has been declared. */
let spanning = false;

/**
 * Declare what a route serves, as its first middleware:
 *
 *   rfsRouter.get('/api/rfs/incidents', scoped(RFS_INCIDENTS), (c) => …)
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
  if (need.source === '*' || need.type === '*') spanning = true;
  else declared.set(`${need.source}|${need.type}|${need.window}`, need);
  return mw;
}

/**
 * Every need declared on a route so far — the combinations a key can actually
 * use. A route spanning sources (stored history) stands for the history of
 * each archived source, so it is listed as those, each a plain combination a
 * scope picker can match.
 */
export function declaredNeeds(): ScopeNeed[] {
  const out = new Map(declared);
  if (spanning) {
    for (const s of Object.values(HISTORY_SOURCES)) {
      out.set(`${s.source}|${s.type}|history`, { source: s.source, type: s.type, window: 'history' });
    }
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
  if (scopesSatisfy(scopes, route.need)) return { ok: true };
  // A spanning dimension (`source:*` on stored history) is met by any scope
  // in it, so it is never what is missing — name only what is.
  const missing = needScopes(route.need).filter((s) => !s.endsWith(':*'));
  return {
    ok: false,
    error: 'api_key_scope_forbidden',
    message: `This key's scopes do not cover this endpoint. It needs ${missing.join(', ')}.`,
  };
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
