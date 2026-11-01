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
import { FEEDS, scoped } from '../../../src/services/auth/apiKeyScopes.js';

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

      it('reaches the versioned twin of a feed it can read, and the merged fire feed', async () => {
        queryMock.mockResolvedValue({ rows: [{ ...keyRow(), rate_limit_per_min: 100 }], rowCount: 1 });
        const app = new Hono();
        app.use('*', requireApiKey);
        app.get('/api/v1/rfs/fdr', (c) => c.json(body(c)));
        app.get('/api/v1/summaries', (c) => c.json(body(c)));
        app.get('/api/v1/fires', scoped({ feed: 'fires', window: 'live' }), (c) => c.json(body(c)));
        expect((await app.request('/api/v1/rfs/fdr', h)).status).toBe(200);
        const summaries = await app.request('/api/v1/summaries', h);
        expect(summaries.status).toBe(403);
        expect(await summaries.json()).toMatchObject({ error: { code: 'api_key_path_forbidden' } });
        // Every fire service in one feed: what it could already read one agency at a time.
        const fires = await app.request('/api/v1/fires', h);
        expect(fires.status).toBe(200);
        expect(await fires.json()).toMatchObject({ auth: 'apikey' });
      });
    });

    // A key with scopes in the new form reads the feed/window cells it holds,
    // and the routes say which cell they serve (scoped(), apiKeyScopes.ts).
    describe('scoped keys', () => {
      const h = { headers: { Authorization: `Bearer ${KEY}` } };
      /** Every cell of the staff table. */
      const EVERY_CELL = Object.entries(FEEDS).flatMap(([feed, f]) => f.windows.map((w) => `${feed}:${w}`));
      function scopedApp() {
        const app = new Hono();
        app.use('*', requireApiKey);
        const fires = scoped({ feed: 'fires', window: 'live' });
        app.get('/api/fires', fires, (c) => c.json(body(c)));
        app.get('/api/rfs/fdr', fires, (c) => c.json(body(c)));
        app.get('/api/bom/warnings', scoped({ feed: 'warnings', window: 'live' }), (c) => c.json(body(c)));
        app.get('/api/adsb/history', scoped({ feed: 'aircraft', window: 'history' }), (c) => c.json(body(c)));
        app.get('/api/data/history', scoped({ feed: '*', window: 'history' }), (c) => c.json(body(c)));
        // Declared by nothing, so no scope reaches them: node status, the
        // per-agency fire routes (this site's and the bot's), this site's own
        // reference data, and reception transcripts.
        app.get('/api/adsb/receivers', (c) => c.json(body(c)));
        app.get('/api/rfs/incidents', (c) => c.json(body(c)));
        // (Not lga-names: that one is public, for the signup form.)
        app.get('/api/boundaries/:kind', (c) => c.json(body(c)));
        app.get('/api/radio/grn-sites', (c) => c.json(body(c)));
        app.get('/api/rdio/calls/:callId', (c) => c.json(body(c)));
        return app;
      }
      function withScopes(scopes: string[], over: Record<string, unknown> = {}) {
        queryMock.mockResolvedValue({ rows: [{ ...keyRow(), scopes, rate_limit_per_min: 100, ...over }], rowCount: 1 });
      }
      const refusal = async (res: Response) => ((await res.json()) as { error: string; message: string });

      it('reads every route that serves a cell it holds', async () => {
        withScopes(['fires:live']);
        const app = scopedApp();
        for (const path of ['/api/fires', '/api/rfs/fdr']) {
          const res = await app.request(path, h);
          expect(res.status, path).toBe(200);
          expect(await res.json(), path).toMatchObject({ auth: 'apikey', key: 'abcdefghij' });
        }
      });

      it('is refused on every cell it lacks, told which cell the route needs', async () => {
        withScopes(['fires:live']);
        const app = scopedApp();
        const needs: Record<string, string> = {
          '/api/bom/warnings': 'It needs warnings:live.',
          '/api/adsb/history': 'It needs aircraft:history.',
          '/api/data/history':
            'It needs the history of at least one of: fires:history, warnings:history, ambulance:history, ' +
            'roads:history, outages:history, beaches:history, pager:history.',
        };
        for (const [path, need] of Object.entries(needs)) {
          const res = await app.request(path, h);
          expect(res.status, path).toBe(403);
          const body = await refusal(res);
          expect(body.error, path).toBe('api_key_scope_forbidden');
          expect(body.message, path).toContain(need);
        }
      });

      it('the other window of a feed it holds is not enough', async () => {
        withScopes(['aircraft:live', 'fires:history']);
        const app = scopedApp();
        expect((await app.request('/api/adsb/history', h)).status).toBe(403);
        expect((await app.request('/api/fires', h)).status).toBe(403);
      });

      it('stored history admits the history cell of a feed the archive holds (the route narrows it)', async () => {
        for (const scopes of [['fires:history'], ['pager:history'], ['aircraft:history', 'warnings:history']]) {
          withScopes(scopes);
          _clearApiKeyCache();
          const app = scopedApp();
          expect((await app.request('/api/data/history', h)).status, scopes.join()).toBe(200);
          // Live routes still need their own cell.
          expect((await app.request('/api/fires', h)).status, scopes.join()).toBe(403);
        }
      });

      it('…and refuses history held only elsewhere: aircraft and summaries are not in the archive', async () => {
        for (const scopes of [['aircraft:history'], ['summaries:history'], ['aircraft:history', 'summaries:history']]) {
          withScopes(scopes);
          _clearApiKeyCache();
          const app = scopedApp();
          const res = await app.request('/api/data/history', h);
          expect(res.status, scopes.join()).toBe(403);
          expect((await refusal(res)).error, scopes.join()).toBe('api_key_scope_forbidden');
          // Its own history route is still its own.
          if (scopes.includes('aircraft:history')) expect((await app.request('/api/adsb/history', h)).status).toBe(200);
        }
      });

      it('a key holding only retired or unknown scopes grants nothing, refused as a scoped key', async () => {
        for (const scopes of [['source:rfs', 'type:incidents', 'window:live'], ['fires:liv']]) {
          withScopes(scopes);
          _clearApiKeyCache();
          const app = scopedApp();
          // Declared routes, and the undeclared ones a `read` key would reach: all refused
          // with the scoped code, never judged by the old list.
          for (const path of ['/api/fires', '/api/rfs/fdr', '/api/data/history', '/api/rfs/incidents', '/api/boundaries/lga']) {
            const res = await app.request(path, h);
            expect(res.status, `${scopes.join()} ${path}`).toBe(403);
            expect((await refusal(res)).error, `${scopes.join()} ${path}`).toBe('api_key_scope_forbidden');
          }
        }
      });

      it('never reaches an undeclared route, however many cells it holds', async () => {
        withScopes(EVERY_CELL);
        const app = scopedApp();
        for (const path of ['/api/adsb/receivers', '/api/rfs/incidents', '/api/boundaries/lga', '/api/radio/grn-sites', '/api/rdio/calls/7']) {
          const res = await app.request(path, h);
          expect(res.status, path).toBe(403);
          expect(await refusal(res), path).toEqual({
            error: 'api_key_scope_forbidden',
            message: 'This path is not part of the data API keys can read.',
          });
        }
      });

      it('never writes', async () => {
        withScopes(EVERY_CELL);
        const app = scopedApp();
        app.post('/api/fires', (c) => c.json(body(c)));
        const res = await app.request('/api/fires', { method: 'POST', ...h });
        expect(res.status).toBe(403);
        expect(await res.json()).toMatchObject({ error: 'api_key_scope_forbidden' });
      });

      it('a path no route serves is a 404, not a refusal', async () => {
        withScopes(['fires:live']);
        expect((await scopedApp().request('/api/fires/nope', h)).status).toBe(404);
      });

      it('still spends the budget on refusals', async () => {
        withScopes(['fires:live'], { rate_limit_per_min: 2 });
        const app = scopedApp();
        expect((await app.request('/api/bom/warnings', h)).status).toBe(403);
        expect((await app.request('/api/bom/warnings', h)).status).toBe(403);
        expect((await app.request('/api/fires', h)).status).toBe(429);
      });

      it("is refused from any web page: an Origin from anywhere, or a browser's Sec-Fetch-Site", async () => {
        withScopes(['fires:live']);
        const app = scopedApp();
        expect((await app.request('/api/fires', h)).status).toBe(200);
        const browser: Array<Record<string, string>> = [
          { Origin: 'https://someone-elses.site' },
          // Our own site is same-SITE: still a web page using a key.
          { Origin: SITE, 'Sec-Fetch-Site': 'same-site' },
          // This API's own origin, as Safari sends it, and a same-origin GET,
          // which carries no Origin at all: no page here uses a key any more.
          { Origin: 'https://api.forcequit.xyz', 'Sec-Fetch-Site': 'same-origin' },
          { 'Sec-Fetch-Site': 'same-origin' },
          { 'Sec-Fetch-Site': 'cross-site' },
          { 'Sec-Fetch-Site': 'none' },
        ];
        for (const extra of browser) {
          const res = await app.request('/api/fires', { headers: { ...h.headers, ...extra } });
          expect(res.status, JSON.stringify(extra)).toBe(403);
          expect(await res.json(), JSON.stringify(extra)).toMatchObject({ error: 'api_key_browser_use' });
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
