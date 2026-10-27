/**
 * Scopes on a named key: the grammar, what a set must hold, how a key is
 * matched against what a route declares, how the gate finds that declaration
 * on the matched route, and the old `read` rule kept exactly as it was.
 */
import { describe, it, expect } from 'vitest';
import { Hono } from 'hono';
import {
  ANY_KEY,
  SCOPE_SOURCES,
  canonicalScopes,
  declaredNeeds,
  isLegacyScopes,
  keyMayRequest,
  needScopes,
  routeNeed,
  scoped,
  scopesSatisfy,
  validateScopes,
  type RouteNeed,
} from '../../../src/services/auth/apiKeyScopes.js';
import { catalog } from '../../../src/services/alertCatalog.js';

const RFS_LIVE = ['source:rfs', 'type:incidents', 'window:live'];
const RFS_INCIDENTS: RouteNeed = { source: 'rfs', type: 'incidents', window: 'live' };

describe('validateScopes', () => {
  const staff = { allowLegacy: true, allowEmpty: false };
  const user = { allowLegacy: false, allowEmpty: true };

  it('canonicalises: trimmed, lower-case, de-duplicated, sorted', () => {
    expect(canonicalScopes([' Window:LIVE', 'source:rfs', 'type:incidents', 'source:rfs'])).toEqual(
      ['source:rfs', 'type:incidents', 'window:live'],
    );
    expect(validateScopes(['window:live', 'TYPE:incidents', 'source:rfs'], staff)).toEqual({ ok: true, scopes: RFS_LIVE });
  });

  it('wildcards per dimension', () => {
    expect(validateScopes(['source:*', 'type:*', 'window:*'], staff).ok).toBe(true);
  });

  it('a set in the new form needs every dimension', () => {
    const r = validateScopes(['source:rfs', 'type:incidents'], staff);
    expect(r).toEqual({ ok: false, error: 'scopes need at least one window: scope' });
  });

  it('unknown values and shapes are refused, by name', () => {
    expect(validateScopes(['source:nope', 'type:incidents', 'window:live'], staff)).toEqual({ ok: false, error: 'unknown scope: source:nope' });
    expect(validateScopes(['place:rfs', 'type:incidents', 'window:live'], staff).ok).toBe(false);
    expect(validateScopes(['window:sometimes', 'source:rfs', 'type:incidents'], staff).ok).toBe(false);
    expect(validateScopes('source:rfs', staff).ok).toBe(false);
    expect(validateScopes([1, 2], staff).ok).toBe(false);
  });

  it('`read` is for staff only, and alone', () => {
    expect(validateScopes(['read'], staff)).toEqual({ ok: true, scopes: ['read'] });
    expect(validateScopes(['read', ...RFS_LIVE], staff).ok).toBe(false);
    expect(validateScopes(['read'], user).ok).toBe(false);
  });

  it('empty is fine for a user\'s ask, never for a grant', () => {
    expect(validateScopes(undefined, user)).toEqual({ ok: true, scopes: [] });
    expect(validateScopes([], user)).toEqual({ ok: true, scopes: [] });
    expect(validateScopes(undefined, staff).ok).toBe(false);
    expect(validateScopes([], staff).ok).toBe(false);
  });

  it('a list longer than the whole taxonomy is refused', () => {
    const many = Object.keys(SCOPE_SOURCES).map((s) => `source:${s}`).concat(Array.from({ length: 60 }, (_, i) => `source:x${i}`));
    expect(validateScopes(many, staff).ok).toBe(false);
  });
});

describe('isLegacyScopes', () => {
  it('`read`, or nothing in the new form, is the old rule', () => {
    expect(isLegacyScopes(['read'])).toBe(true);
    expect(isLegacyScopes(['read', 'source:rfs'])).toBe(true);
    // A key issued by hand with a scope that never meant anything keeps what it had.
    expect(isLegacyScopes(['admin'])).toBe(true);
    expect(isLegacyScopes([])).toBe(true);
    expect(isLegacyScopes(RFS_LIVE)).toBe(false);
  });
});

describe('scopesSatisfy', () => {
  it('needs a match in every dimension', () => {
    expect(scopesSatisfy(RFS_LIVE, RFS_INCIDENTS)).toBe(true);
    expect(scopesSatisfy(RFS_LIVE, { source: 'rfs', type: 'warnings', window: 'live' })).toBe(false);
    expect(scopesSatisfy(RFS_LIVE, { source: 'bom', type: 'incidents', window: 'live' })).toBe(false);
    expect(scopesSatisfy(RFS_LIVE, { source: 'rfs', type: 'incidents', window: 'history' })).toBe(false);
  });

  it('a wildcard covers its dimension only', () => {
    expect(scopesSatisfy(['source:*', 'type:incidents', 'window:live'], { source: 'vic', type: 'incidents', window: 'live' })).toBe(true);
    expect(scopesSatisfy(['source:*', 'type:incidents', 'window:live'], { source: 'vic', type: 'warnings', window: 'live' })).toBe(false);
    expect(scopesSatisfy(['source:*', 'type:*', 'window:*'], { source: 'adsb', type: 'aircraft', window: 'history' })).toBe(true);
  });

  it('window "any" (reference data) takes either window', () => {
    const need: RouteNeed = { source: 'pager', type: 'pager', window: 'any' };
    expect(scopesSatisfy(['source:pager', 'type:pager', 'window:history'], need)).toBe(true);
    expect(scopesSatisfy(['source:pager', 'type:pager', 'window:live'], need)).toBe(true);
    expect(scopesSatisfy(['source:pager', 'type:pager'], need)).toBe(false);
  });

  it('ANY_KEY is satisfied by any key', () => {
    expect(scopesSatisfy(RFS_LIVE, ANY_KEY)).toBe(true);
    expect(needScopes(ANY_KEY)).toEqual([]);
  });

  it('names what a need asks for', () => {
    expect(needScopes(RFS_INCIDENTS)).toEqual(['source:rfs', 'type:incidents', 'window:live']);
    expect(needScopes({ source: 'pager', type: 'pager', window: 'any' })).toEqual(['source:pager', 'type:pager', 'window:live|history']);
  });
});

describe('keyMayRequest', () => {
  const matched = (need: RouteNeed | null) => ({ matched: true, need });

  describe('a `read` key keeps exactly the old rule', () => {
    it('GET/HEAD on the feed list, nothing else, whatever the routes declare', () => {
      expect(keyMayRequest(['read'], 'GET', '/api/rfs/incidents', matched(null)).ok).toBe(true);
      expect(keyMayRequest(['read'], 'HEAD', '/api/beachwatch', matched(null)).ok).toBe(true);
      // Declared or not, an off-list path is refused with the old code.
      expect(keyMayRequest(['read'], 'GET', '/api/summaries', matched({ source: 'rdio', type: 'summaries', window: 'history' })))
        .toMatchObject({ ok: false, error: 'api_key_path_forbidden' });
      expect(keyMayRequest(['read'], 'POST', '/api/rfs/incidents', matched(RFS_INCIDENTS)))
        .toMatchObject({ ok: false, error: 'api_key_path_forbidden' });
      // Unmatched paths behave as they always did: on the list → through (a 404 follows), off it → refused.
      expect(keyMayRequest(['read'], 'GET', '/api/rfs/nope', { matched: false, need: null }).ok).toBe(true);
      expect(keyMayRequest(['read'], 'GET', '/api/nope', { matched: false, need: null }).ok).toBe(false);
    });

    it('reaches the versioned twin of a feed it can read', () => {
      expect(keyMayRequest(['read'], 'GET', '/api/v1/rfs/incidents', matched(RFS_INCIDENTS)).ok).toBe(true);
      expect(keyMayRequest(['read'], 'GET', '/api/v1/summaries', matched(null)).ok).toBe(false);
    });
  });

  describe('a key in the new form', () => {
    it('reads what it is scoped for', () => {
      expect(keyMayRequest(RFS_LIVE, 'GET', '/api/rfs/incidents', matched(RFS_INCIDENTS))).toEqual({ ok: true });
      expect(keyMayRequest(RFS_LIVE, 'HEAD', '/api/rfs/incidents', matched(RFS_INCIDENTS))).toEqual({ ok: true });
    });

    it('is refused elsewhere, saying what the endpoint needs', () => {
      const r = keyMayRequest(RFS_LIVE, 'GET', '/api/rfs/fdr', matched({ source: 'rfs', type: 'warnings', window: 'live' }));
      expect(r).toMatchObject({ ok: false, error: 'api_key_scope_forbidden' });
      expect(!r.ok && r.message).toContain('type:warnings');
    });

    it('never writes', () => {
      expect(keyMayRequest(['source:*', 'type:*', 'window:*'], 'POST', '/api/rfs/incidents', matched(RFS_INCIDENTS)))
        .toMatchObject({ ok: false, error: 'api_key_scope_forbidden' });
    });

    it('is refused on a route nothing declared — however wide its scopes', () => {
      expect(keyMayRequest(['source:*', 'type:*', 'window:*'], 'GET', '/api/adsb/receivers', matched(null)))
        .toMatchObject({ ok: false, error: 'api_key_scope_forbidden' });
    });

    it('goes through to the 404 when no route matches at all', () => {
      expect(keyMayRequest(RFS_LIVE, 'GET', '/api/rfs/nope', { matched: false, need: null })).toEqual({ ok: true });
    });
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
    const area = scoped({ source: 'rfs', type: 'incidents', window: 'live' });
    app.get('/api/rfs/incidents', area, (c) => c.text('ok'));
    // Two declarations on one chain: the last counts.
    app.get('/api/rfs/fdr', area, scoped({ source: 'rfs', type: 'warnings', window: 'live' }), (c) => c.text('ok'));
    app.get('/api/adsb/receivers', (c) => c.text('ok'));
    // A sub-app with its own error handler: Hono wraps its handlers on mount.
    const sub = new Hono();
    sub.onError((_e, c) => c.text('err', 500));
    sub.get('/api/bom/warnings', scoped({ source: 'bom', type: 'warnings', window: 'live' }), (c) => c.text('ok'));
    app.route('/', sub);
    return { app, seen };
  }

  it('finds the declaration, the last one winning', async () => {
    const { app, seen } = probe();
    await app.request('/api/rfs/incidents');
    await app.request('/api/rfs/fdr');
    expect(seen[0]).toEqual({ matched: true, need: { source: 'rfs', type: 'incidents', window: 'live' } });
    expect(seen[1]).toEqual({ matched: true, need: { source: 'rfs', type: 'warnings', window: 'live' } });
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
    await app.request('/api/bom/warnings');
    expect(seen[0]).toEqual({ matched: true, need: { source: 'bom', type: 'warnings', window: 'live' } });
  });

  it('records what has been declared, for the scope pickers', () => {
    scoped({ source: 'news', type: 'news', window: 'live' });
    expect(declaredNeeds()).toContainEqual({ source: 'news', type: 'news', window: 'live' });
  });
});

describe('the sources stay aligned with the alert catalog', () => {
  // Providers the bot alerts on that are deliberately NOT key sources: Wire
  // articles are this site's own content, and user incidents carry user ids.
  const NOT_KEY_SOURCES = new Set(['wire', 'user']);

  it('every other catalog provider is a scope source under the same key', () => {
    const missing = catalog.providers
      .map((p) => p.key)
      .filter((k) => !NOT_KEY_SOURCES.has(k) && !Object.prototype.hasOwnProperty.call(SCOPE_SOURCES, k));
    expect(missing).toEqual([]);
  });
});
