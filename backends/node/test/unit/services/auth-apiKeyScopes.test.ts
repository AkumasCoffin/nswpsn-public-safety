/**
 * Scopes on a named key: the feed:window grammar, what a set must hold, how a
 * key is matched against what a route declares, how the gate finds that
 * declaration on the matched route, stored history (one route spanning feeds,
 * narrowed per key), and the old `read` rule kept exactly as it was.
 */
import { describe, it, expect } from 'vitest';
import { Hono } from 'hono';
import {
  ANY_KEY,
  FEEDS,
  HISTORY_SOURCES,
  canonicalScopes,
  declaredNeeds,
  historySourcesFor,
  isLegacyScopes,
  keyCanRead,
  keyMayRequest,
  needFromScope,
  needScopes,
  parseScope,
  routeNeed,
  scoped,
  scopesSatisfy,
  validateScopes,
  type RouteNeed,
  type ScopeNeed,
} from '../../../src/services/auth/apiKeyScopes.js';
import { catalog } from '../../../src/services/alertCatalog.js';
import { DISTRIBUTOR_INFO, OUTAGE_SOURCES, OUTAGE_STORES } from '../../../src/sources/outages/common.js';

const FIRES_LIVE = ['fires:live'];
const FIRES: ScopeNeed = { feed: 'fires', window: 'live' };
const HISTORY: ScopeNeed = { feed: '*', window: 'history' };
/** A key granted under the retired source/type/window grammar. */
const RETIRED = ['source:rfs', 'type:incidents', 'window:live'];

/** The feeds stored history answers for, in the order HISTORY_SOURCES first names them. */
const SPANNED = [...new Set(Object.values(HISTORY_SOURCES))];
const HISTORY_REFUSAL =
  "This key's scopes do not cover this endpoint. It needs the history of at least one of: " +
  'fires:history, warnings:history, ambulance:history, roads:history, outages:history, beaches:history, pager:history.';

/** Every cell of the staff table: each feed with each window it offers. */
const CELLS = Object.entries(FEEDS).flatMap(([feed, f]) => f.windows.map((w) => `${feed}:${w}`));
const HISTORY_CELLS = CELLS.filter((s) => s.endsWith(':history'));

describe('the feed table', () => {
  it('a cell is a feed and a window that feed offers; nothing else parses', () => {
    for (const [feed, f] of Object.entries(FEEDS)) {
      for (const window of ['live', 'history'] as const) {
        const offered = (f.windows as readonly string[]).includes(window);
        expect(parseScope(`${feed}:${window}`), `${feed}:${window}`).toEqual(offered ? { feed, window } : null);
      }
    }
    for (const s of ['fires', 'fires:*', '*:live', '*:history', 'nope:live', 'fires:sometimes', 'source:rfs', 'read']) {
      expect(parseScope(s), s).toBeNull();
    }
  });

  it('every feed has a label, says what it covers, and offers at least one window', () => {
    for (const [feed, f] of Object.entries(FEEDS)) {
      expect(f.label, feed).toBeTruthy();
      expect(f.covers, feed).toBeTruthy();
      expect(f.windows.length, feed).toBeGreaterThan(0);
    }
  });
});

describe('validateScopes', () => {
  const staff = { allowLegacy: true, allowEmpty: false };
  const user = { allowLegacy: false, allowEmpty: true };

  it('canonicalises: trimmed, lower-case, de-duplicated, sorted', () => {
    expect(canonicalScopes([' Weather:LIVE', 'fires:live', 'FIRES:live '])).toEqual(['fires:live', 'weather:live']);
    expect(validateScopes(['Weather:Live', 'fires:live ', 'FIRES:LIVE'], staff)).toEqual({
      ok: true,
      scopes: ['fires:live', 'weather:live'],
    });
  });

  it('every cell the table offers is accepted, all at once', () => {
    expect(validateScopes(CELLS, staff)).toEqual({ ok: true, scopes: [...CELLS].sort() });
  });

  it('a window the feed does not offer is refused by name', () => {
    expect(validateScopes(['cameras:history'], staff)).toEqual({ ok: false, error: 'unknown scope: cameras:history' });
    expect(validateScopes(['summaries:live'], staff)).toEqual({ ok: false, error: 'unknown scope: summaries:live' });
    // One bad cell spoils the set.
    expect(validateScopes(['fires:live', 'ships:history'], staff)).toEqual({ ok: false, error: 'unknown scope: ships:history' });
  });

  it('unknown feeds and shapes are refused, by name; there are no wildcards', () => {
    expect(validateScopes(['nope:live'], staff)).toEqual({ ok: false, error: 'unknown scope: nope:live' });
    expect(validateScopes(['fires:sometimes', 'fires'], staff)).toEqual({ ok: false, error: 'unknown scopes: fires, fires:sometimes' });
    // `*:history` is what a route spanning feeds declares, never something a key holds.
    expect(validateScopes(['*:history'], staff).ok).toBe(false);
    expect(validateScopes(['fires:*'], staff).ok).toBe(false);
    // The retired source/type/window grammar.
    expect(validateScopes(['source:rfs', 'type:incidents', 'window:live'], staff).ok).toBe(false);
    expect(validateScopes('fires:live', staff)).toEqual({ ok: false, error: 'scopes must be a list of strings' });
    expect(validateScopes([1, 2], staff).ok).toBe(false);
  });

  it('`read` is for staff only, and alone', () => {
    expect(validateScopes(['read'], staff)).toEqual({ ok: true, scopes: ['read'] });
    expect(validateScopes(['read', 'fires:live'], staff)).toEqual({ ok: false, error: '"read" cannot be combined with other scopes' });
    expect(validateScopes(['read'], user)).toEqual({ ok: false, error: '"read" can only be granted by staff' });
  });

  it('empty is fine for a user\'s ask, never for a grant', () => {
    expect(validateScopes(undefined, user)).toEqual({ ok: true, scopes: [] });
    expect(validateScopes([], user)).toEqual({ ok: true, scopes: [] });
    expect(validateScopes(undefined, staff).ok).toBe(false);
    expect(validateScopes([], staff).ok).toBe(false);
    // A user may ask for cells, just not for `read`.
    expect(validateScopes(['fires:history'], user)).toEqual({ ok: true, scopes: ['fires:history'] });
  });

  it('a list longer than the whole table is refused', () => {
    const many = CELLS.concat(Array.from({ length: 64 }, (_, i) => `x${i}:live`));
    expect(validateScopes(many, staff)).toEqual({ ok: false, error: 'at most 64 scopes' });
  });
});

describe('isLegacyScopes', () => {
  it('only `read` is the old rule', () => {
    expect(isLegacyScopes(['read'])).toBe(true);
    expect(isLegacyScopes(['read', 'fires:live'])).toBe(true);
    expect(isLegacyScopes(FIRES_LIVE)).toBe(false);
    expect(isLegacyScopes(['summaries:history'])).toBe(false);
  });

  it('a scope the table does not know is not `read`: never widened to the old list', () => {
    // The retired source/type/window form, a typo issued by hand, or nothing at all.
    expect(isLegacyScopes(RETIRED)).toBe(false);
    expect(isLegacyScopes(['admin'])).toBe(false);
    expect(isLegacyScopes(['fires:liv'])).toBe(false);
    expect(isLegacyScopes([])).toBe(false);
  });
});

describe('scopesSatisfy', () => {
  it('a key gets exactly the cells it holds', () => {
    expect(scopesSatisfy(FIRES_LIVE, FIRES)).toBe(true);
    expect(scopesSatisfy(FIRES_LIVE, { feed: 'fires', window: 'history' })).toBe(false);
    expect(scopesSatisfy(FIRES_LIVE, { feed: 'warnings', window: 'live' })).toBe(false);
    // The other window of the same feed is not enough.
    expect(scopesSatisfy(['fires:history'], FIRES)).toBe(false);
    expect(scopesSatisfy(['fires:history', 'fires:live'], FIRES)).toBe(true);
  });

  it('ANY_KEY is satisfied by any key', () => {
    expect(scopesSatisfy(FIRES_LIVE, ANY_KEY)).toBe(true);
    expect(needScopes(ANY_KEY)).toEqual([]);
  });

  it('names what a need asks for, and reads it back', () => {
    expect(needScopes(FIRES)).toEqual(['fires:live']);
    expect(needScopes(HISTORY)).toEqual(['*:history']);
    for (const cell of CELLS) {
      const need = needFromScope(cell);
      expect(need, cell).not.toBeNull();
      expect(needScopes(need!), cell).toEqual([cell]);
    }
    expect(needFromScope('*:history')).toEqual(HISTORY);
    // Only cells the table offers, and no grammar of the past.
    for (const s of ['cameras:history', 'nope:live', 'fires', 'source:rfs', 'window:live']) {
      expect(needFromScope(s), s).toBeNull();
    }
  });
});

describe('keyMayRequest', () => {
  const matched = (need: RouteNeed | null) => ({ matched: true, need });

  describe('a `read` key keeps exactly the old rule', () => {
    it('GET/HEAD on the feed list, nothing else, whatever the routes declare', () => {
      // The per-agency fire routes declare nothing now; a `read` key keeps them.
      expect(keyMayRequest(['read'], 'GET', '/api/rfs/incidents', matched(null)).ok).toBe(true);
      expect(keyMayRequest(['read'], 'GET', '/api/nt-fire/incidents', matched(null)).ok).toBe(true);
      expect(keyMayRequest(['read'], 'GET', '/api/boundaries/lga-names', matched(null)).ok).toBe(true);
      expect(keyMayRequest(['read'], 'GET', '/api/radio/grn-sites', matched(null)).ok).toBe(true);
      expect(keyMayRequest(['read'], 'HEAD', '/api/beachwatch', matched(null)).ok).toBe(true);
      // Declared or not, an off-list path is refused with the old code.
      expect(keyMayRequest(['read'], 'GET', '/api/summaries', matched({ feed: 'summaries', window: 'history' })))
        .toMatchObject({ ok: false, error: 'api_key_path_forbidden' });
      expect(keyMayRequest(['read'], 'GET', '/api/rdio/calls/7', matched(null)))
        .toMatchObject({ ok: false, error: 'api_key_path_forbidden' });
      expect(keyMayRequest(['read'], 'POST', '/api/rfs/fdr', matched(FIRES)))
        .toMatchObject({ ok: false, error: 'api_key_path_forbidden' });
      // Unmatched paths behave as they always did: on the list → through (a 404 follows), off it → refused.
      expect(keyMayRequest(['read'], 'GET', '/api/rfs/nope', { matched: false, need: null }).ok).toBe(true);
      expect(keyMayRequest(['read'], 'GET', '/api/nope', { matched: false, need: null }).ok).toBe(false);
    });

    it('reaches the versioned twin of a feed it can read, and the merged fire feed', () => {
      expect(keyMayRequest(['read'], 'GET', '/api/v1/rfs/fdr', matched(FIRES)).ok).toBe(true);
      expect(keyMayRequest(['read'], 'GET', '/api/v1/summaries', matched(null)).ok).toBe(false);
      // Every fire service in one feed: nothing it could not already read one agency at a time.
      expect(keyMayRequest(['read'], 'GET', '/api/v1/fires', matched(FIRES))).toEqual({ ok: true });
      expect(keyMayRequest(['read'], 'GET', '/api/fires', matched(FIRES))).toEqual({ ok: true });
      // An exact path, not a prefix.
      expect(keyMayRequest(['read'], 'GET', '/api/firesale', { matched: false, need: null }).ok).toBe(false);
    });
  });

  describe('a key holding only scopes the table does not know', () => {
    // The retired source/type/window grammar, or a typo: a scoped key holding nothing.
    for (const scopes of [RETIRED, ['fires:liv']]) {
      it(`${scopes.join(',')}: refused every declared route, as a scoped key, never by the old list`, () => {
        for (const [path, need] of [['/api/v1/fires', FIRES], ['/api/v1/history', HISTORY]] as const) {
          expect(keyMayRequest(scopes, 'GET', path, matched(need)), path).toMatchObject({ ok: false, error: 'api_key_scope_forbidden' });
        }
        // A path the old list has, and that `read` reaches — not this key.
        expect(keyMayRequest(scopes, 'GET', '/api/rfs/incidents', matched(null))).toEqual({
          ok: false,
          error: 'api_key_scope_forbidden',
          message: 'This path is not part of the data API keys can read.',
        });
        expect(keyCanRead(scopes, '/api/v1/rfs/fdr', FIRES)).toBe(false);
        expect(historySourcesFor(scopes)).toEqual([]);
      });
    }
  });

  describe('a key in the new form', () => {
    it('reads what it is scoped for', () => {
      expect(keyMayRequest(FIRES_LIVE, 'GET', '/api/v1/fires', matched(FIRES))).toEqual({ ok: true });
      expect(keyMayRequest(FIRES_LIVE, 'HEAD', '/api/v1/fires', matched(FIRES))).toEqual({ ok: true });
      // The unversioned path of a declared route is the same route.
      expect(keyMayRequest(FIRES_LIVE, 'GET', '/api/fires', matched(FIRES))).toEqual({ ok: true });
    });

    it('is refused elsewhere, saying which cell the endpoint needs', () => {
      expect(keyMayRequest(FIRES_LIVE, 'GET', '/api/v1/bom/warnings', matched({ feed: 'warnings', window: 'live' }))).toEqual({
        ok: false,
        error: 'api_key_scope_forbidden',
        message: "This key's scopes do not cover this endpoint. It needs warnings:live.",
      });
      expect(keyMayRequest(['fires:history'], 'GET', '/api/v1/fires', matched(FIRES))).toEqual({
        ok: false,
        error: 'api_key_scope_forbidden',
        message: "This key's scopes do not cover this endpoint. It needs fires:live.",
      });
    });

    it('never writes, however many cells it holds', () => {
      expect(keyMayRequest(CELLS, 'POST', '/api/v1/fires', matched(FIRES))).toEqual({
        ok: false,
        error: 'api_key_scope_forbidden',
        message: 'API keys only read (GET).',
      });
    });

    it('is refused on a route nothing declared — however many cells it holds', () => {
      // Node status, this site's own reference data, transcripts, the per-agency fire routes.
      for (const path of ['/api/adsb/receivers', '/api/boundaries/lga-names', '/api/radio/grn-sites', '/api/rdio/calls/7', '/api/rfs/incidents']) {
        expect(keyMayRequest(CELLS, 'GET', path, matched(null)), path).toEqual({
          ok: false,
          error: 'api_key_scope_forbidden',
          message: 'This path is not part of the data API keys can read.',
        });
      }
    });

    it('goes through to the 404 when no route matches at all', () => {
      expect(keyMayRequest(FIRES_LIVE, 'GET', '/api/v1/nope', { matched: false, need: null })).toEqual({ ok: true });
    });
  });

  describe('an ANY_KEY route (the API description)', () => {
    it('is readable by every key, `read` included, though it is on no list', () => {
      for (const scopes of [['read'], FIRES_LIVE, ['pager:history']]) {
        expect(keyMayRequest(scopes, 'GET', '/api/v1/openapi.json', matched(ANY_KEY))).toEqual({ ok: true });
      }
    });

    it('is still read-only', () => {
      expect(keyMayRequest(['read'], 'POST', '/api/v1/openapi.json', matched(ANY_KEY)).ok).toBe(false);
      expect(keyMayRequest(FIRES_LIVE, 'POST', '/api/v1/openapi.json', matched(ANY_KEY)).ok).toBe(false);
    });
  });
});

describe('keyCanRead — the gate’s answer for a route, as the API description asks it', () => {
  it('matches keyMayRequest on a GET', () => {
    expect(keyCanRead(FIRES_LIVE, '/api/v1/fires', FIRES)).toBe(true);
    expect(keyCanRead(FIRES_LIVE, '/api/v1/bom/warnings', { feed: 'warnings', window: 'live' })).toBe(false);
    expect(keyCanRead(FIRES_LIVE, '/api/v1/fires', null)).toBe(false);
    expect(keyCanRead(['pager:history'], '/api/v1/history', HISTORY)).toBe(true);
  });

  it('judges a `read` key by the path, templates included', () => {
    expect(keyCanRead(['read'], '/api/v1/rfs/fdr', null)).toBe(true);
    expect(keyCanRead(['read'], '/api/v1/fires', FIRES)).toBe(true);
    expect(keyCanRead(['read'], '/api/v1/agency/extended/{slug}', null)).toBe(false);
    // The old list never included history, declared or not.
    expect(keyCanRead(['read'], '/api/v1/history', HISTORY)).toBe(false);
  });
});

describe('routeNeed — the declaration on the matched route', () => {
  function probe() {
    const seen: Array<ReturnType<typeof routeNeed>> = [];
    const app = new Hono();
    app.use('*', async (c, next) => {
      seen.push(routeNeed(c));
      await next();
    });
    const fires = scoped({ feed: 'fires', window: 'live' });
    app.get('/api/fires', fires, (c) => c.text('ok'));
    // Two declarations on one chain: the last counts.
    app.get('/api/bom/warnings', fires, scoped({ feed: 'warnings', window: 'live' }), (c) => c.text('ok'));
    app.get('/api/adsb/receivers', (c) => c.text('ok'));
    // A sub-app with its own error handler: Hono wraps its handlers on mount.
    const sub = new Hono();
    sub.onError((_e, c) => c.text('err', 500));
    sub.get('/api/adsb/history', scoped({ feed: 'aircraft', window: 'history' }), (c) => c.text('ok'));
    app.route('/', sub);
    return { app, seen };
  }

  it('finds the declaration, the last one winning', async () => {
    const { app, seen } = probe();
    await app.request('/api/fires');
    await app.request('/api/bom/warnings');
    expect(seen[0]).toEqual({ matched: true, need: { feed: 'fires', window: 'live' } });
    expect(seen[1]).toEqual({ matched: true, need: { feed: 'warnings', window: 'live' } });
  });

  it('a matched route with no declaration, and no route at all', async () => {
    const { app, seen } = probe();
    await app.request('/api/adsb/receivers');
    await app.request('/api/nowhere');
    expect(seen[0]).toEqual({ matched: true, need: null });
    expect(seen[1]).toEqual({ matched: false, need: null });
  });

  it('sees through a mounted sub-app\'s wrapped handlers', async () => {
    const { app, seen } = probe();
    await app.request('/api/adsb/history');
    expect(seen[0]).toEqual({ matched: true, need: { feed: 'aircraft', window: 'history' } });
  });

  it('records what has been declared, for the staff table', () => {
    scoped({ feed: 'news', window: 'live' });
    expect(declaredNeeds()).toContainEqual({ feed: 'news', window: 'live' });
  });
});

describe('the feeds stay aligned with the alert catalog', () => {
  // Providers the bot alerts on that are deliberately NOT key feeds: Wire
  // articles are this site's own content, and user incidents carry user ids.
  const NOT_KEY_SOURCES = new Set(['wire', 'user']);

  // The feed each catalog provider's data is read under. A provider added to
  // the catalog fails the first test until someone decides where it belongs.
  const PROVIDER_FEEDS: Record<string, string[]> = {
    rfs: ['fires'],
    firms: ['fires'],
    vic: ['fires'],
    qfd: ['fires', 'warnings'],
    dfes: ['fires', 'warnings'],
    safire: ['fires'],
    ntfrs: ['fires'],
    actas: ['ambulance'],
    bom: ['warnings'],
    livetraffic: ['roads'],
    power: ['outages'],
    rdio: ['summaries'],
    dorsal: ['beaches'],
  };

  it('every other catalog provider is read under a feed the table has', () => {
    const providers = catalog.providers.map((p) => p.key).filter((k) => !NOT_KEY_SOURCES.has(k));
    expect(providers.filter((k) => !PROVIDER_FEEDS[k])).toEqual([]);
    for (const feeds of Object.values(PROVIDER_FEEDS)) {
      for (const f of feeds) expect(Object.keys(FEEDS), f).toContain(f);
    }
  });

  it('every archived source the catalog names is readable through history, under its provider\'s feed', () => {
    // Never key-readable through history: people's own reports, and summaries
    // (their own route, under their own feed).
    const NEVER = new Set(['user_incident', 'radio_summary']);
    for (const t of catalog.types) {
      if (!t.archiveSource || NEVER.has(t.archiveSource) || NOT_KEY_SOURCES.has(t.provider)) continue;
      const feed = HISTORY_SOURCES[t.archiveSource];
      expect(feed, t.archiveSource).toBeDefined();
      expect(PROVIDER_FEEDS[t.provider], t.archiveSource).toContain(feed);
    }
    for (const never of [...NEVER, 'essential_energy_cancelled']) expect(HISTORY_SOURCES[never], never).toBeUndefined();
  });

  it('every source the merged outage feed files a record under is readable through outages history', () => {
    // The catalog names only the distributors the bot alerts on (Horizon
    // Power is not read), so the test above does not reach every one of the
    // other states' distributors.
    for (const source of OUTAGE_SOURCES) expect(HISTORY_SOURCES[source], source).toBe('outages');
    for (const store of OUTAGE_STORES) expect(FEEDS.outages.covers, store).toContain(DISTRIBUTOR_INFO[store].name);
  });

  it('every archived source names a feed that offers history', () => {
    for (const [archived, feed] of Object.entries(HISTORY_SOURCES)) {
      expect(Object.keys(FEEDS), archived).toContain(feed);
      expect(FEEDS[feed].windows as readonly string[], archived).toContain('history');
    }
  });
});

describe('stored history: a route spanning feeds, narrowed per key', () => {
  it('the history cell of a feed the archive holds gets in; nothing else does', () => {
    for (const cell of HISTORY_CELLS) {
      const archived = SPANNED.includes(cell.split(':')[0] as (typeof SPANNED)[number]);
      expect(scopesSatisfy([cell], HISTORY), cell).toBe(archived);
    }
    expect(scopesSatisfy(['fires:history'], HISTORY)).toBe(true);
    // History of its own, elsewhere: stored history could only refuse every record.
    expect(scopesSatisfy(['aircraft:history'], HISTORY)).toBe(false);
    expect(scopesSatisfy(['summaries:history'], HISTORY)).toBe(false);
    expect(scopesSatisfy(['aircraft:history', 'summaries:history'], HISTORY)).toBe(false);
    expect(scopesSatisfy(['aircraft:history', 'pager:history'], HISTORY)).toBe(true);
    // Live cells never.
    expect(scopesSatisfy(FIRES_LIVE, HISTORY)).toBe(false);
    expect(scopesSatisfy(CELLS.filter((s) => s.endsWith(':live')), HISTORY)).toBe(false);
    // Only a cell the table offers counts.
    expect(scopesSatisfy(['cameras:history'], HISTORY)).toBe(false);
    expect(scopesSatisfy(RETIRED, HISTORY)).toBe(false);
  });

  it('a refusal names every cell that would get in', () => {
    expect(SPANNED).toEqual(['fires', 'warnings', 'ambulance', 'roads', 'outages', 'beaches', 'pager']);
    for (const scopes of [FIRES_LIVE, ['aircraft:history'], ['summaries:history']]) {
      expect(keyMayRequest(scopes, 'GET', '/api/v1/history', { matched: true, need: HISTORY }), scopes[0]).toEqual({
        ok: false,
        error: 'api_key_scope_forbidden',
        message: HISTORY_REFUSAL,
      });
    }
  });

  it('historySourcesFor: the archived sources of every feed whose history the key holds', () => {
    // The QFD and DFES warnings are part of the fires feed; warnings is the BOM's.
    expect(historySourcesFor(['fires:history']).sort()).toEqual(
      ['nt_fire', 'qld_fire', 'qld_warning', 'rfs', 'sa_cfs', 'sa_mfs', 'vic_emergency', 'wa_incident', 'wa_warning'],
    );
    expect(historySourcesFor(['warnings:history', 'pager:history']).sort()).toEqual(
      ['bom_land', 'bom_marine', 'pager'],
    );
    // The NSW distributors under their feeds' names, every other one under its own.
    expect(historySourcesFor(['outages:history']).sort()).toEqual(
      [
        'ausgrid', 'ausnet', 'citipower', 'endeavour_current', 'endeavour_maintenance', 'endeavour_planned',
        'essential_current', 'essential_future', 'essential_planned', 'horizon', 'jemena', 'powercor', 'sapn', 'westernpower',
      ],
    );
    // A live cell reads no history, and a history cell with nothing archived reads none either.
    expect(historySourcesFor(['fires:live'])).toEqual([]);
    expect(historySourcesFor(['aircraft:history', 'summaries:history'])).toEqual([]);
    // The old rule never included history.
    expect(historySourcesFor(['read'])).toEqual([]);
    // Every cell there is: every archived source, never people's own reports.
    const all = historySourcesFor(CELLS);
    expect(all.sort()).toEqual(Object.keys(HISTORY_SOURCES).sort());
    expect(all).not.toContain('user_incident');
    expect(all).not.toContain('radio_summary');
  });

  it('declared, it stands for each feed the archive holds — never a `*` — for the staff table', () => {
    scoped(HISTORY);
    const needs = declaredNeeds();
    expect(needs.some((n) => (n.feed as string) === '*')).toBe(false);
    for (const feed of new Set(Object.values(HISTORY_SOURCES))) {
      expect(needs).toContainEqual({ feed, window: 'history' });
    }
    // A feed with no archived source is not made up for it (nothing in this file declares summaries).
    expect(needs).not.toContainEqual({ feed: 'summaries', window: 'history' });
  });
});
