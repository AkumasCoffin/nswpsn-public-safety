/**
 * The credential gate, class by class. The two rules the whole change
 * exists for are pinned here: a browser token works only with a first-party
 * Origin, and a named key is refused the moment a browser Origin appears.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { createHash } from 'node:crypto';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  getWriterPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

import { requireApiKey } from '../../../src/services/auth/apiKey.js';
import { mintBrowserToken } from '../../../src/services/auth/browserToken.js';
import { _clearApiKeyCache, lookupPrefix } from '../../../src/services/auth/apiKeys.js';
import { scoped } from '../../../src/services/auth/apiKeyScopes.js';

const SITE = 'https://nswpsn.forcequit.xyz';
const UA = 'Mozilla/5.0 gate-test';
const IP = '203.0.113.9';

const body = (c: { get: (k: never) => unknown }) => ({
  ok: true,
  auth: (c.get as (k: string) => unknown)('authClass') ?? null,
  key: (c.get as (k: string) => unknown)('apiKeyPrefix') ?? null,
});

function makeApp() {
  const app = new Hono();
  app.use('*', requireApiKey);
  app.get('/api/thing', (c) => c.json(body(c)));
  app.get('/api/health', (c) => c.json({ ok: true }));
  // A data feed (on the API-key allowlist) and two things a key must never
  // reach: a write to a feed path, and someone's role matrix.
  app.get(FEED, (c) => c.json(body(c)));
  app.post(FEED, (c) => c.json(body(c)));
  app.get('/api/check-admin/:userId', (c) => c.json(body(c)));
  return app;
}

/** An upstream data feed — what a named key is for. */
const FEED = '/api/rfs/incidents';

function browserHeaders(extra: Record<string, string> = {}) {
  return { 'User-Agent': UA, 'CF-Connecting-IP': IP, Origin: SITE, ...extra };
}

const KEY = 'ak_abcdefghij_' + 'K'.repeat(32);
function keyRow() {
  return {
    id: 'k1', user_id: 'u1', name: 'cli', prefix: lookupPrefix(KEY),
    key_hash: createHash('sha256').update(KEY).digest('hex'),
    scopes: ['read'], rate_limit_per_min: 2, created_at: 'x', last_used_at: null, expires_at: null, revoked_at: null,
  };
}

beforeEach(() => {
  queryMock.mockReset();
  queryMock.mockResolvedValue({ rows: [keyRow()], rowCount: 1 });
  _clearApiKeyCache();
});

describe('requireApiKey', () => {
  it('public paths need nothing', async () => {
    const res = await makeApp().request('/api/health');
    expect(res.status).toBe(200);
  });

  it('nothing presented is the same 401 as always', async () => {
    const res = await makeApp().request('/api/thing');
    expect(res.status).toBe(401);
    expect(await res.json()).toMatchObject({ error: 'API key required' });
  });

  describe('browser session token', () => {
    it('passes with a first-party Origin from the client it was minted for', async () => {
      const { token } = mintBrowserToken({ ip: IP, ua: UA });
      const res = await makeApp().request('/api/thing', {
        headers: browserHeaders({ Authorization: `Bearer ${token}` }),
      });
      expect(res.status).toBe(200);
      expect(await res.json()).toMatchObject({ auth: 'browser' });
    });

    it('is refused with no Origin — a curl holding a copied token', async () => {
      const { token } = mintBrowserToken({ ip: IP, ua: UA });
      const res = await makeApp().request('/api/thing', {
        headers: { 'User-Agent': UA, 'CF-Connecting-IP': IP, Authorization: `Bearer ${token}` },
      });
      expect(res.status).toBe(401);
      expect(await res.json()).toMatchObject({ error: 'token_origin' });
    });

    it('is refused from another site, and from another address', async () => {
      const { token } = mintBrowserToken({ ip: IP, ua: UA });
      let res = await makeApp().request('/api/thing', {
        headers: browserHeaders({ Authorization: `Bearer ${token}`, Origin: 'https://evil.example' }),
      });
      expect(res.status).toBe(401);
      res = await makeApp().request('/api/thing', {
        headers: browserHeaders({ Authorization: `Bearer ${token}`, 'CF-Connecting-IP': '198.51.100.1' }),
      });
      expect(res.status).toBe(401);
      expect(await res.json()).toMatchObject({ error: 'token_invalid' });
    });

    it('says expired so the page knows to re-mint', async () => {
      const { token } = mintBrowserToken({ ip: IP, ua: UA }, Date.now() - 2_000_000);
      const res = await makeApp().request('/api/thing', {
        headers: browserHeaders({ Authorization: `Bearer ${token}` }),
      });
      expect(res.status).toBe(401);
      expect(await res.json()).toMatchObject({ error: 'token_expired' });
    });
  });

  describe('named API key', () => {
    it('works from the command line (no Origin), via Bearer or X-API-Key', async () => {
      let res = await makeApp().request(FEED, { headers: { Authorization: `Bearer ${KEY}` } });
      expect(res.status).toBe(200);
      expect(await res.json()).toMatchObject({ auth: 'apikey', key: 'abcdefghij' });
      res = await makeApp().request(FEED, { headers: { 'X-API-Key': KEY } });
      expect(res.status).toBe(200);
    });

    it('is refused from any web page — an Origin header is present', async () => {
      const res = await makeApp().request(FEED, {
        headers: { Authorization: `Bearer ${KEY}`, Origin: 'https://someone-elses.site' },
      });
      expect(res.status).toBe(403);
      expect(await res.json()).toMatchObject({ error: 'api_key_browser_use' });
      // Not even our own pages: keys are for scripts.
      const own = await makeApp().request(FEED, { headers: browserHeaders({ Authorization: `Bearer ${KEY}` }) });
      expect(own.status).toBe(403);
    });

    it('wrong key is the same 403 as always; revoked too', async () => {
      const res = await makeApp().request(FEED, {
        headers: { Authorization: `Bearer ak_abcdefghij_${'W'.repeat(32)}` },
      });
      expect(res.status).toBe(403);
      expect(await res.json()).toMatchObject({ error: 'Invalid API key' });
    });

    it('enforces the key\'s own per-minute limit', async () => {
      const app = makeApp();
      const h = { headers: { Authorization: `Bearer ${KEY}` } };
      expect((await app.request(FEED, h)).status).toBe(200);
      expect((await app.request(FEED, h)).status).toBe(200);
      const res = await app.request(FEED, h);
      expect(res.status).toBe(429);
      expect(await res.json()).toMatchObject({ error: 'rate_limited' });
    });

    it('answers 503, not 403, when the database cannot be reached', async () => {
      const { getPool } = await import('../../../src/db/pool.js');
      vi.mocked(getPool).mockResolvedValueOnce(null as never);
      const res = await makeApp().request(FEED, { headers: { Authorization: `Bearer ${KEY}` } });
      expect(res.status).toBe(503);
    });

    // A key is a polling credential for the upstream feeds, and passing the
    // credential gate must not mean "everything a signed-in page can see".
    // Before this, a key could read any account's role matrix, walk the rdio
    // call corpus, spend the paid what3words quota and POST a takedown notice
    // into the staff Discord — none of which is polling a feed.
    describe('reaches the data feeds and nothing else', () => {
      const h = { headers: { Authorization: `Bearer ${KEY}` } };

      it('refuses a path that is not a data feed, however valid the key', async () => {
        const res = await makeApp().request('/api/thing', h);
        expect(res.status).toBe(403);
        expect(await res.json()).toMatchObject({ error: 'api_key_path_forbidden' });
      });

      it('refuses the role matrix of an account outright', async () => {
        const res = await makeApp().request('/api/check-admin/0199e8f0-dead-4beef-8000-000000000001', h);
        expect(res.status).toBe(403);
        expect(await res.json()).toMatchObject({ error: 'api_key_path_forbidden' });
      });

      it('refuses a WRITE even to a path it may read', async () => {
        // A `read` key: the method and the old feed list hold the line.
        const res = await makeApp().request(FEED, { method: 'POST', ...h });
        expect(res.status).toBe(403);
        expect(await res.json()).toMatchObject({ error: 'api_key_path_forbidden' });
      });

      it('still spends the key budget on refused paths, so scanning is not free', async () => {
        const app = makeApp();
        // rate_limit_per_min is 2 on the fixture key.
        expect((await app.request('/api/thing', h)).status).toBe(403);
        expect((await app.request('/api/thing', h)).status).toBe(403);
        expect((await app.request(FEED, h)).status).toBe(429);
      });

      it('leaves the browser token and the static key alone', async () => {
        // Only the named-key class is confined: a page and the Discord bot
        // both still reach everything they did.
        const { token } = mintBrowserToken({ ip: IP, ua: UA });
        const page = await makeApp().request('/api/thing', {
          headers: browserHeaders({ Authorization: `Bearer ${token}` }),
        });
        expect(page.status).toBe(200);
        const bot = await makeApp().request('/api/thing', {
          headers: { Authorization: 'Bearer test-api-key' },
        });
        expect(bot.status).toBe(200);
      });

      it('reaches the versioned twin of a feed it can read', async () => {
        const app = new Hono();
        app.use('*', requireApiKey);
        app.get('/api/v1/rfs/incidents', (c) => c.json(body(c)));
        app.get('/api/v1/summaries', (c) => c.json(body(c)));
        expect((await app.request('/api/v1/rfs/incidents', h)).status).toBe(200);
        expect((await app.request('/api/v1/summaries', h)).status).toBe(403);
      });
    });

    // A key with scopes in the new form reads what its scopes cover, and the
    // routes say what they serve (scoped(), apiKeyScopes.ts).
    describe('scoped keys', () => {
      const h = { headers: { Authorization: `Bearer ${KEY}` } };
      function scopedApp() {
        const app = new Hono();
        app.use('*', requireApiKey);
        app.get('/api/rfs/incidents', scoped({ source: 'rfs', type: 'incidents', window: 'live' }), (c) => c.json(body(c)));
        app.get('/api/rfs/fdr', scoped({ source: 'rfs', type: 'warnings', window: 'live' }), (c) => c.json(body(c)));
        app.get('/api/adsb/history', scoped({ source: 'adsb', type: 'aircraft', window: 'history' }), (c) => c.json(body(c)));
        app.get('/api/pager/hits', scoped({ source: 'pager', type: 'pager', window: 'any' }), (c) => c.json(body(c)));
        // Node status: declared by nothing, so no scope reaches it.
        app.get('/api/adsb/receivers', (c) => c.json(body(c)));
        return app;
      }
      function withScopes(scopes: string[], over: Record<string, unknown> = {}) {
        queryMock.mockResolvedValue({ rows: [{ ...keyRow(), scopes, rate_limit_per_min: 100, ...over }], rowCount: 1 });
      }

      it('reads a route its scopes cover', async () => {
        withScopes(['source:rfs', 'type:incidents', 'window:live']);
        const res = await scopedApp().request('/api/rfs/incidents', h);
        expect(res.status).toBe(200);
        expect(await res.json()).toMatchObject({ auth: 'apikey', key: 'abcdefghij' });
      });

      it('is refused on each dimension it lacks, with the scopes the route needs', async () => {
        withScopes(['source:rfs', 'type:incidents', 'window:live']);
        const app = scopedApp();
        for (const path of ['/api/rfs/fdr', '/api/adsb/history', '/api/pager/hits']) {
          const res = await app.request(path, h);
          expect(res.status).toBe(403);
          expect(await res.json()).toMatchObject({ error: 'api_key_scope_forbidden' });
        }
        const res = await app.request('/api/rfs/fdr', h);
        expect(((await res.json()) as { message: string }).message).toContain('type:warnings');
      });

      it('wildcards, and window "any" for reference data', async () => {
        withScopes(['source:*', 'type:*', 'window:history']);
        const app = scopedApp();
        expect((await app.request('/api/adsb/history', h)).status).toBe(200);
        expect((await app.request('/api/pager/hits', h)).status).toBe(200);
        expect((await app.request('/api/rfs/incidents', h)).status).toBe(403);
      });

      it('never reaches an undeclared route, however wide its scopes', async () => {
        withScopes(['source:*', 'type:*', 'window:*']);
        const res = await scopedApp().request('/api/adsb/receivers', h);
        expect(res.status).toBe(403);
        expect(await res.json()).toMatchObject({ error: 'api_key_scope_forbidden' });
      });

      it('never writes', async () => {
        withScopes(['source:*', 'type:*', 'window:*']);
        const app = scopedApp();
        app.post('/api/rfs/incidents', (c) => c.json(body(c)));
        const res = await app.request('/api/rfs/incidents', { method: 'POST', ...h });
        expect(res.status).toBe(403);
        expect(await res.json()).toMatchObject({ error: 'api_key_scope_forbidden' });
      });

      it('a path no route serves is a 404, not a refusal', async () => {
        withScopes(['source:rfs', 'type:incidents', 'window:live']);
        expect((await scopedApp().request('/api/rfs/nope', h)).status).toBe(404);
      });

      it('still spends the budget on refusals', async () => {
        withScopes(['source:rfs', 'type:incidents', 'window:live'], { rate_limit_per_min: 2 });
        const app = scopedApp();
        expect((await app.request('/api/rfs/fdr', h)).status).toBe(403);
        expect((await app.request('/api/rfs/fdr', h)).status).toBe(403);
        expect((await app.request('/api/rfs/incidents', h)).status).toBe(429);
      });

      it('is still refused from a web page, but not from a same-origin read with no Origin', async () => {
        withScopes(['source:rfs', 'type:incidents', 'window:live']);
        const app = scopedApp();
        const page = await app.request('/api/rfs/incidents', { headers: { ...h.headers, Origin: 'https://someone-elses.site' } });
        expect(page.status).toBe(403);
        expect(await page.json()).toMatchObject({ error: 'api_key_browser_use' });
        // A same-origin GET carries no Origin header (Fetch spec) — what the
        // API's own docs page relies on to load the spec with a key.
        const same = await app.request('/api/rfs/incidents', { headers: { ...h.headers, 'Sec-Fetch-Site': 'same-origin' } });
        expect(same.status).toBe(200);
      });

      it('lets the API’s own page through when the browser says so — Safari sends Origin even same-origin', async () => {
        withScopes(['source:rfs', 'type:incidents', 'window:live']);
        const app = scopedApp();
        const safari = await app.request('/api/rfs/incidents', {
          headers: { ...h.headers, Origin: 'https://api.forcequit.xyz', 'Sec-Fetch-Site': 'same-origin' },
        });
        expect(safari.status).toBe(200);
        // Our own site is same-SITE, not same-origin: still a web page using a key.
        for (const site of ['same-site', 'cross-site', 'none']) {
          const res = await app.request('/api/rfs/incidents', {
            headers: { ...h.headers, Origin: SITE, 'Sec-Fetch-Site': site },
          });
          expect(res.status, site).toBe(403);
        }
      });
    });
  });

  // Which requests count against a key (apiKeyUsage.ts reads apiKeyUsageId
  // after the response exists). The refusals are the point: staff need to see
  // a key used from a page, after revocation, past its limit or off its paths.
  describe('usage attribution', () => {
    function attributing(seen: Array<string | null>) {
      const app = new Hono();
      app.use('*', async (c, next) => {
        await next();
        seen.push(((c.get as (k: string) => unknown)('apiKeyUsageId') as string | undefined) ?? null);
      });
      app.use('*', requireApiKey);
      app.get(FEED, (c) => c.json(body(c)));
      app.get('/api/thing', (c) => c.json(body(c)));
      return app;
    }
    const h = { headers: { Authorization: `Bearer ${KEY}` } };

    it('marks an accepted request, a refused path, a web page and a request over the limit', async () => {
      const seen: Array<string | null> = [];
      const app = attributing(seen);
      expect((await app.request(FEED, h)).status).toBe(200);
      expect((await app.request('/api/thing', h)).status).toBe(403);
      expect((await app.request(FEED, h)).status).toBe(429);
      expect((await app.request(FEED, { headers: { ...h.headers, Origin: 'https://someone-elses.site' } })).status).toBe(403);
      expect(seen).toEqual(['k1', 'k1', 'k1', 'k1']);
    });

    it('marks a revoked key that is still being used', async () => {
      queryMock.mockResolvedValue({ rows: [{ ...keyRow(), revoked_at: 'r' }], rowCount: 1 });
      const seen: Array<string | null> = [];
      const res = await attributing(seen).request(FEED, h);
      expect(res.status).toBe(403);
      expect(await res.json()).toMatchObject({ error: 'Invalid API key' });
      expect(seen).toEqual(['k1']);
    });

    it('does not mark a wrong secret against a real prefix, nor the static key', async () => {
      const seen: Array<string | null> = [];
      const app = attributing(seen);
      expect((await app.request(FEED, { headers: { Authorization: `Bearer ak_abcdefghij_${'W'.repeat(32)}` } })).status).toBe(403);
      expect((await app.request('/api/thing', { headers: { Authorization: 'Bearer test-api-key' } })).status).toBe(200);
      expect(seen).toEqual([null, null]);
    });

    it('a web page with a key still gets the browser-use 403 when the database is down', async () => {
      const { getPool } = await import('../../../src/db/pool.js');
      vi.mocked(getPool).mockResolvedValueOnce(null as never);
      const res = await makeApp().request(FEED, { headers: { Authorization: `Bearer ${KEY}`, Origin: 'https://x.example' } });
      expect(res.status).toBe(403);
      expect(await res.json()).toMatchObject({ error: 'api_key_browser_use' });
    });
  });

  describe('static site key (server callers)', () => {
    it('still works via Bearer, X-API-Key and ?api_key=', async () => {
      const app = makeApp();
      expect((await app.request('/api/thing', { headers: { Authorization: 'Bearer test-api-key' } })).status).toBe(200);
      expect((await app.request('/api/thing', { headers: { 'X-API-Key': 'test-api-key' } })).status).toBe(200);
      const res = await app.request('/api/thing?api_key=test-api-key');
      expect(res.status).toBe(200);
      expect(await res.json()).toMatchObject({ auth: 'static' });
    });

    it('wrong static key is 403 with the original body', async () => {
      const res = await makeApp().request('/api/thing', { headers: { 'X-API-Key': 'nope' } });
      expect(res.status).toBe(403);
      expect(await res.json()).toMatchObject({ error: 'Invalid API key' });
    });
  });
});
