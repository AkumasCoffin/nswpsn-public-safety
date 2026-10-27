/**
 * The versioned API through the real app (createApp): what /api/v1/ answers,
 * that the unversioned twin still answers exactly as before, and that the
 * credential gate judges both paths alike — with the v1 error shape on v1.
 */
import { describe, it, expect, vi, beforeEach, beforeAll } from 'vitest';
import { createHash } from 'node:crypto';
import type { Hono } from 'hono';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  getWriterPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));
vi.mock('../../../src/services/agencyData.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/services/agencyData.js')>()),
  getAllAgencyExtended: vi.fn(async () => ({ rfs: { title: 'RFS', tag: 't', badges: [], overview: [], sections: [] } })),
  getAgencyExtended: vi.fn(async (slug: string) =>
    slug === 'rfs' ? { title: 'RFS', tag: 't', badges: [], overview: [], sections: [] } : null),
}));

import { liveStore } from '../../../src/store/live.js';
import { _clearApiKeyCache, lookupPrefix } from '../../../src/services/auth/apiKeys.js';

const STATIC = { 'X-API-Key': 'test-api-key' };
const KEY = 'ak_abcdefghij_' + 'K'.repeat(32);
let keyScopes: string[] = ['read'];

function keyRow() {
  return {
    id: 'k1', user_id: 'u1', name: 'cli', prefix: lookupPrefix(KEY),
    key_hash: createHash('sha256').update(KEY).digest('hex'),
    scopes: keyScopes, rate_limit_per_min: 100, created_at: 'x', last_used_at: null, expires_at: null, revoked_at: null,
  };
}

const RFS = {
  type: 'FeatureCollection',
  features: [{ type: 'Feature', geometry: { type: 'Point', coordinates: [151, -33] }, properties: { title: 'Fire' } }],
  count: 1,
};
const BOM = { warnings: [{ title: 'Flood Watch' }], count: 1, counts: { land: 1, marine: 0 } };

let app: Hono;
beforeAll(async () => {
  const { createApp } = await import('../../../src/server.js');
  app = createApp();
});

beforeEach(() => {
  keyScopes = ['read'];
  _clearApiKeyCache();
  queryMock.mockReset();
  queryMock.mockImplementation(async (sql: string) =>
    /FROM api_keys/.test(sql) ? { rows: [keyRow()], rowCount: 1 } : { rows: [], rowCount: 0 });
  liveStore.set('rfs_incidents', RFS);
  liveStore.set('bom_warnings', BOM);
});

const get = (path: string, headers: Record<string, string> = STATIC) => app.request(path, { headers });

describe('answers on /api/v1/', () => {
  it('GeoJSON is the bare FeatureCollection on both paths', async () => {
    const v1 = await get('/api/v1/rfs/incidents');
    const old = await get('/api/rfs/incidents');
    expect(v1.status).toBe(200);
    expect(await v1.json()).toEqual(RFS);
    expect(await old.json()).toEqual(RFS);
  });

  it('everything else is wrapped in data on v1, and unchanged on the old path', async () => {
    expect(await (await get('/api/v1/bom/warnings')).json()).toEqual({ data: BOM });
    expect(await (await get('/api/bom/warnings')).json()).toEqual(BOM);
  });

  it('a public route stays public on v1', async () => {
    const res = await get('/api/v1/health', {});
    expect(res.status).toBe(200);
    expect((await res.json()).data).toMatchObject({ status: 'ok' });
    expect(await (await get('/api/health', {})).json()).toMatchObject({ status: 'ok' });
  });

  it('a path parameter works on the mirror, and its 404 is the v1 error', async () => {
    expect(await (await get('/api/v1/agency/extended/rfs', {})).json()).toMatchObject({ data: { slug: 'rfs', title: 'RFS' } });
    const v1 = await get('/api/v1/agency/extended/nope', {});
    expect(v1.status).toBe(404);
    expect(await v1.json()).toEqual({ error: { code: 'not_found', message: 'No agency by that slug.' } });
    const old = await get('/api/agency/extended/nope', {});
    expect(old.status).toBe(404);
    expect(await old.json()).toEqual({ error: 'unknown agency' });
  });

  it('bad input: validation_failed with the issues on v1, the old body on the old path', async () => {
    const v1 = await get('/api/v1/boundaries/lga-names?state=XX', {});
    expect(v1.status).toBe(400);
    const body = await v1.json();
    expect(body.error.code).toBe('validation_failed');
    expect(body.error.details.issues[0].path).toBe('state');
    const old = await get('/api/boundaries/lga-names?state=XX', {});
    expect(old.status).toBe(400);
    expect(await old.json()).toEqual({ error: 'state must be one of NSW, VIC, QLD, SA, WA, TAS, NT, ACT' });
  });

  it('a tolerant query keeps its tolerance on both paths', async () => {
    for (const path of ['/api/v1/sa-fire/cfs?all=yes', '/api/sa-fire/cfs?all=yes', '/api/v1/nt-fire/incidents?all=yes']) {
      expect((await get(path)).status, path).toBe(200);
    }
  });

  it('RFS raw on v1 is the feed as last polled, from memory — no fetch per request', async () => {
    const res = await get('/api/v1/rfs/incidents/raw');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ type: 'FeatureCollection', features: [] });
  });

  it('a repeated parameter is read by its first value, as it always was', async () => {
    for (const path of ['/api/v1/boundaries/lga-names?state=nsw&state=xx', '/api/boundaries/lga-names?state=nsw&state=xx']) {
      const res = await get(path, {});
      expect(res.status, path).toBe(200);
    }
    expect((await (await get('/api/v1/boundaries/lga-names?state=vic&state=nsw', {})).json()).data.state).toBe('VIC');
  });

  it('HEAD works on a mirrored GET', async () => {
    const res = await app.request('/api/v1/rfs/incidents', { method: 'HEAD', headers: STATIC });
    expect(res.status).toBe(200);
  });

  it('the public 30 s cache applies to the v1 twin of a cacheable path', async () => {
    expect((await get('/api/v1/rfs/incidents')).headers.get('Cache-Control')).toBe('public, max-age=30, stale-while-revalidate=300');
  });
});

describe('not found and errors', () => {
  it('an unknown v1 path is a JSON 404 once past the gate', async () => {
    const res = await get('/api/v1/nope');
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
  });

  it('a route that is not versioned is not on v1', async () => {
    expect((await get('/api/v1/adsb/aircraft')).status).toBe(404);
    expect((await get('/api/v1/heartbeat')).status).toBe(404);
  });

  it('an unknown old path keeps the plain-text 404', async () => {
    const res = await get('/api/nope');
    expect(res.status).toBe(404);
    expect(await res.text()).toBe('404 Not Found');
  });
});

describe('the gate on /api/v1/', () => {
  it('no credential: 401, in the v1 shape', async () => {
    const res = await get('/api/v1/rfs/incidents', {});
    expect(res.status).toBe(401);
    expect(await res.json()).toEqual({
      error: { code: 'unauthorized', message: 'Provide API key via Authorization: Bearer <key> header or X-API-Key header' },
    });
  });

  it('no credential on the old path: the old body', async () => {
    const res = await get('/api/rfs/incidents', {});
    expect(await res.json()).toMatchObject({ error: 'API key required' });
  });

  it('a wrong static key: 403 invalid_api_key', async () => {
    const res = await get('/api/v1/rfs/incidents', { 'X-API-Key': 'nope' });
    expect(res.status).toBe(403);
    expect((await res.json()).error.code).toBe('invalid_api_key');
  });

  it('a scoped key reaches the v1 twin of what it may read', async () => {
    keyScopes = ['source:rfs', 'type:incidents', 'window:live'];
    expect((await get('/api/v1/rfs/incidents', { 'X-API-Key': KEY })).status).toBe(200);
    const res = await get('/api/v1/bom/warnings', { 'X-API-Key': KEY });
    expect(res.status).toBe(403);
    const body = await res.json();
    expect(body.error.code).toBe('api_key_scope_forbidden');
    expect(body.error.message).toContain('source:bom');
  });

  it('a `read` key reaches the v1 twins of the old feed list', async () => {
    expect((await get('/api/v1/bom/warnings', { 'X-API-Key': KEY })).status).toBe(200);
  });

  it('a key from a web page is refused in the v1 shape', async () => {
    const res = await get('/api/v1/rfs/incidents', { 'X-API-Key': KEY, Origin: 'https://example.com' });
    expect(res.status).toBe(403);
    expect((await res.json()).error.code).toBe('api_key_browser_use');
  });
});
