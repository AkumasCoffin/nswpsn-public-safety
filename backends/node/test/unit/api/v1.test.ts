/**
 * The versioned API through the real app (createApp): what /api/v1/ answers,
 * that the unversioned twin still answers exactly as before, that the
 * credential gate judges both paths alike — with the v1 error shape on v1 —
 * and that the feed table and the routes agree once every router is mounted.
 */
import { describe, it, expect, vi, beforeEach, beforeAll } from 'vitest';
import { createHash } from 'node:crypto';
import type { Hono } from 'hono';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock, connect: async () => ({ query: queryMock, release: () => {} }) })),
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
import { declaredNeeds, FEEDS } from '../../../src/services/auth/apiKeyScopes.js';
import { ALERT_TYPE_DEFS } from '../../../src/services/alertCatalog.js';

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

/** Every cell of the staff table. */
const EVERY_CELL = Object.entries(FEEDS).flatMap(([feed, f]) => f.windows.map((w) => `${feed}:${w}`));

const RFS = {
  type: 'FeatureCollection',
  features: [{ type: 'Feature', geometry: { type: 'Point', coordinates: [151, -33] }, properties: { title: 'Fire', guid: 'rfs-1' } }],
  count: 1,
};
const ACT = {
  type: 'FeatureCollection',
  features: [{
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [149.1, -35.3] },
    properties: {
      id: 'act-1', title: 'Medical', location_text: 'Civic', status: 'Ongoing', type: 'Ambulance', agency: 'ACTAS',
      category: 'Medical', subcategory: '', timestamp: 1793000000, time_of_call: '01/11/2026 10:00', updated: '01/11/2026 10:05',
      is_active: true,
    },
  }],
  count: 1,
};
const BOM = { warnings: [{ title: 'Flood Watch' }], count: 1, counts: { land: 1, marine: 0 } };

/** Per-agency incident routes, boundaries, the radio site list and transcripts: this site's routes, not the API's. */
const NOT_ON_V1 = [
  '/api/v1/rfs/incidents',
  '/api/v1/rfs/incidents/raw',
  '/api/v1/nt-fire/incidents',
  '/api/v1/qld-fire/incidents',
  '/api/v1/vic-emergency/events',
  '/api/v1/wa-emergency/incidents',
  '/api/v1/sa-fire/cfs',
  '/api/v1/sa-fire/mfs',
  '/api/v1/boundaries/lga-names',
  '/api/v1/boundaries/lga',
  '/api/v1/radio/grn-sites',
  '/api/v1/rdio/calls/7',
];

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
  liveStore.set('act_ambulance', ACT);
  liveStore.set('bom_warnings', BOM);
});

const get = (path: string, headers: Record<string, string> = STATIC) => app.request(path, { headers });
const asKey = { 'X-API-Key': KEY };

describe('answers on /api/v1/', () => {
  it('GeoJSON is the bare FeatureCollection on both paths', async () => {
    const v1 = await get('/api/v1/act-ambulance/incidents');
    const old = await get('/api/act-ambulance/incidents');
    expect(v1.status).toBe(200);
    expect(await v1.json()).toEqual(ACT);
    expect(await old.json()).toEqual(ACT);
    // The merged fire feed too.
    const fires = await (await get('/api/v1/fires?source=rfs')).json();
    expect(fires).toMatchObject({ type: 'FeatureCollection', count: 1 });
    expect(fires).not.toHaveProperty('data');
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
    const v1 = await get('/api/v1/weather/point?lat=xx&lon=151');
    expect(v1.status).toBe(400);
    const body = await v1.json();
    expect(body.error.code).toBe('validation_failed');
    expect(body.error.details.issues[0].path).toBe('lat');
    const old = await get('/api/weather/point?lat=xx&lon=151');
    expect(old.status).toBe(400);
    expect(await old.json()).toEqual({ error: 'lat and lon are required' });
  });

  it('a tolerant query keeps its tolerance on both paths', async () => {
    for (const path of ['/api/v1/fires?all=yes', '/api/fires?all=yes', '/api/sa-fire/cfs?all=yes', '/api/nt-fire/incidents?all=yes']) {
      expect((await get(path)).status, path).toBe(200);
    }
  });

  it('a repeated parameter is read by its first value, as it always was', async () => {
    for (const path of ['/api/v1/fires?state=nsw&state=xx', '/api/fires?state=nsw&state=xx']) {
      expect((await get(path)).status, path).toBe(200);
    }
    const body = (await (await get('/api/v1/fires?state=vic&state=nsw')).json()) as { sources: Array<{ state: string }> };
    expect(body.sources.map((s) => s.state)).toEqual(['VIC']);
  });

  it('HEAD works on a mirrored GET', async () => {
    const res = await app.request('/api/v1/fires', { method: 'HEAD', headers: STATIC });
    expect(res.status).toBe(200);
  });

  it('the public 30 s cache applies to the v1 twin of a cacheable path', async () => {
    expect((await get('/api/v1/fires')).headers.get('Cache-Control')).toBe('public, max-age=30, stale-while-revalidate=300');
  });

  it('a refusal or a failure on a cacheable path is not cached, on either path', async () => {
    for (const path of ['/api/v1/fires', '/api/fires', '/api/v1/bom/warnings', '/api/bom/warnings']) {
      const res = await get(path, {});
      expect(res.status, path).toBe(401);
      expect(res.headers.get('Cache-Control'), path).toBeNull();
    }
    // /api/rfs/incidents is cacheable; its v1 path does not exist.
    const missing = await get('/api/v1/rfs/incidents');
    expect(missing.status).toBe(404);
    expect(missing.headers.get('Cache-Control')).toBeNull();
    queryMock.mockImplementation(async () => {
      throw new Error('db down');
    });
    for (const path of ['/api/v1/data/history', '/api/data/history']) {
      const res = await get(path);
      expect(res.status, path).toBeGreaterThanOrEqual(500);
      expect(res.headers.get('Cache-Control'), path).toBeNull();
    }
  });
});

describe('not found and errors', () => {
  it('an unknown v1 path is a JSON 404 once past the gate', async () => {
    const res = await get('/api/v1/nope');
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
  });

  it('a route that is not versioned is not on v1', async () => {
    expect((await get('/api/v1/adsb/receivers')).status).toBe(404); // node status: never versioned
    expect((await get('/api/v1/heartbeat')).status).toBe(404);
  });

  it('the per-agency fire routes, boundaries, the radio site list and transcripts are not on v1', async () => {
    for (const path of NOT_ON_V1) {
      const res = await get(path);
      expect(res.status, path).toBe(404);
      expect(await res.json(), path).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
    }
  });

  it('…but the site\'s own fire routes still answer on /api/, as before', async () => {
    const rfs = await get('/api/rfs/incidents');
    expect(rfs.status).toBe(200);
    expect(await rfs.json()).toEqual(RFS);
    for (const path of ['/api/nt-fire/incidents', '/api/qld-fire/incidents', '/api/vic-emergency/events', '/api/wa-emergency/incidents', '/api/sa-fire/cfs', '/api/sa-fire/mfs']) {
      expect((await get(path)).status, path).toBe(200);
    }
  });

  it('an unknown old path keeps the plain-text 404', async () => {
    const res = await get('/api/nope');
    expect(res.status).toBe(404);
    expect(await res.text()).toBe('404 Not Found');
  });
});

describe('the gate on /api/v1/', () => {
  it('no credential: 401, in the v1 shape', async () => {
    const res = await get('/api/v1/fires', {});
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
    const res = await get('/api/v1/fires', { 'X-API-Key': 'nope' });
    expect(res.status).toBe(403);
    expect((await res.json()).error.code).toBe('invalid_api_key');
  });

  it('a scoped key reads the routes of the cells it holds, on both paths', async () => {
    keyScopes = ['fires:live'];
    expect((await get('/api/v1/fires', asKey)).status).toBe(200);
    expect((await get('/api/fires', asKey)).status).toBe(200);
    const res = await get('/api/v1/bom/warnings', asKey);
    expect(res.status).toBe(403);
    expect((await res.json()).error).toEqual({
      code: 'api_key_scope_forbidden',
      message: "This key's scopes do not cover this endpoint. It needs warnings:live.",
    });
  });

  it('a scoped key never reaches this site\'s own routes, however many cells it holds', async () => {
    keyScopes = EVERY_CELL;
    const own = [
      '/api/rfs/incidents',
      '/api/nt-fire/incidents',
      '/api/qld-fire/incidents',
      '/api/vic-emergency/events',
      '/api/wa-emergency/incidents',
      '/api/sa-fire/cfs',
      '/api/boundaries/lga',
      '/api/radio/grn-sites',
      '/api/rdio/calls/7',
      '/api/adsb/receivers',
    ];
    for (const path of own) {
      const res = await get(path, asKey);
      expect(res.status, path).toBe(403);
      expect(await res.json(), path).toMatchObject({ error: 'api_key_scope_forbidden' });
    }
    // Their v1 paths do not exist: a 404, not a refusal.
    for (const path of NOT_ON_V1.filter((p) => !p.includes('lga-names'))) {
      expect((await get(path, asKey)).status, path).toBe(404);
    }
  });

  it('a `read` key reaches the v1 twins of the old feed list', async () => {
    expect((await get('/api/v1/bom/warnings', asKey)).status).toBe(200);
  });

  it('a `read` key keeps the old per-agency fire paths, and reads the merged feed on both paths', async () => {
    for (const path of ['/api/rfs/incidents', '/api/nt-fire/incidents', '/api/sa-fire/cfs', '/api/v1/fires', '/api/fires']) {
      expect((await get(path, asKey)).status, path).toBe(200);
    }
    // Still not stored history: the old list never had it.
    const history = await get('/api/v1/data/history', asKey);
    expect(history.status).toBe(403);
    expect((await history.json()).error.code).toBe('api_key_path_forbidden');
  });

  it('a key holding only retired scopes reads nothing: refused as a scoped key, not by the old list', async () => {
    keyScopes = ['source:rfs', 'type:incidents', 'window:live'];
    for (const path of ['/api/v1/fires', '/api/v1/bom/warnings', '/api/rfs/incidents', '/api/v1/data/history']) {
      const res = await get(path, asKey);
      expect(res.status, path).toBe(403);
      const body = await res.json();
      expect(body.error.code ?? body.error, path).toBe('api_key_scope_forbidden');
    }
  });

  it('stored history: fires:history gets in, aircraft:history alone does not', async () => {
    keyScopes = ['fires:history'];
    expect((await get('/api/v1/data/history', asKey)).status).toBe(200);
    _clearApiKeyCache();
    keyScopes = ['aircraft:history'];
    const res = await get('/api/v1/data/history', asKey);
    expect(res.status).toBe(403);
    expect((await res.json()).error.code).toBe('api_key_scope_forbidden');
  });

  it('a key from a web page is refused in the v1 shape', async () => {
    const res = await get('/api/v1/fires', { 'X-API-Key': KEY, Origin: 'https://example.com' });
    expect(res.status).toBe(403);
    expect((await res.json()).error.code).toBe('api_key_browser_use');
  });
});

describe('the feed table and the routes agree, with every router mounted', () => {
  const cell = (n: { feed: string; window: string }) => `${n.feed}:${n.window}`;

  it('every window a feed offers is served by at least one route, and no route serves another', () => {
    const served = [...new Set(declaredNeeds().map(cell))].sort();
    expect(served).toEqual([...EVERY_CELL].sort());
  });

  it('no route names a feed the table does not have', () => {
    for (const n of declaredNeeds()) expect(Object.keys(FEEDS), cell(n)).toContain(n.feed);
  });
});

describe('the Discord bot polls each catalog feed where it is', () => {
  // shared/alert-catalog.json's endpoints are what the bot polls. A typo there
  // is a 404 the bot logs and an alert type that silently never fires.
  const served = () => new Set(app.routes.filter((r) => r.method === 'GET').map((r) => r.path));
  const polled = ALERT_TYPE_DEFS.flatMap((t) => (t.endpoint ? [{ key: t.key, path: t.endpoint.split('?')[0]! }] : []));

  it('every endpoint is a route the app serves', () => {
    const paths = served();
    for (const p of polled) expect(paths.has(p.path), `${p.key}: ${p.path}`).toBe(true);
  });

  it('a feed that is on /api/v1/ is polled there', () => {
    const paths = served();
    for (const p of polled) {
      if (p.path.startsWith('/api/v1/')) continue;
      expect(paths.has(p.path.replace('/api/', '/api/v1/')), `${p.key}: ${p.path}`).toBe(false);
    }
  });
});
