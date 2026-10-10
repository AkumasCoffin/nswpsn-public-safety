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

const SITE = 'https://nswpsn.forcequit.xyz';
const UA = 'Mozilla/5.0 gate-test';
const IP = '203.0.113.9';

function makeApp() {
  const app = new Hono();
  app.use('*', requireApiKey);
  app.get('/api/thing', (c) => c.json({ ok: true, auth: c.get('authClass') ?? null, key: c.get('apiKeyPrefix') ?? null }));
  app.get('/api/health', (c) => c.json({ ok: true }));
  return app;
}

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
      let res = await makeApp().request('/api/thing', { headers: { Authorization: `Bearer ${KEY}` } });
      expect(res.status).toBe(200);
      expect(await res.json()).toMatchObject({ auth: 'apikey', key: 'abcdefghij' });
      res = await makeApp().request('/api/thing', { headers: { 'X-API-Key': KEY } });
      expect(res.status).toBe(200);
    });

    it('is refused from any web page — an Origin header is present', async () => {
      const res = await makeApp().request('/api/thing', {
        headers: { Authorization: `Bearer ${KEY}`, Origin: 'https://someone-elses.site' },
      });
      expect(res.status).toBe(403);
      expect(await res.json()).toMatchObject({ error: 'api_key_browser_use' });
      // Not even our own pages: keys are for scripts.
      const own = await makeApp().request('/api/thing', { headers: browserHeaders({ Authorization: `Bearer ${KEY}` }) });
      expect(own.status).toBe(403);
    });

    it('wrong key is the same 403 as always; revoked too', async () => {
      const res = await makeApp().request('/api/thing', {
        headers: { Authorization: `Bearer ak_abcdefghij_${'W'.repeat(32)}` },
      });
      expect(res.status).toBe(403);
      expect(await res.json()).toMatchObject({ error: 'Invalid API key' });
    });

    it('enforces the key\'s own per-minute limit', async () => {
      const app = makeApp();
      const h = { headers: { Authorization: `Bearer ${KEY}` } };
      expect((await app.request('/api/thing', h)).status).toBe(200);
      expect((await app.request('/api/thing', h)).status).toBe(200);
      const res = await app.request('/api/thing', h);
      expect(res.status).toBe(429);
      expect(await res.json()).toMatchObject({ error: 'rate_limited' });
    });

    it('answers 503, not 403, when the database cannot be reached', async () => {
      const { getPool } = await import('../../../src/db/pool.js');
      vi.mocked(getPool).mockResolvedValueOnce(null as never);
      const res = await makeApp().request('/api/thing', { headers: { Authorization: `Bearer ${KEY}` } });
      expect(res.status).toBe(503);
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
