/**
 * GET /api/v1/outages/areas — each electricity distributor's supply area
 * (api/outages.ts, sources/outages/boundaries.ts).
 *
 * Pinned here: one bare FeatureCollection, the same on both paths; a Polygon
 * or MultiPolygon per distributor with its name and state; `sources` for every
 * distributor that publishes an area; the day-long cache header, and its
 * absence while an area is missing; and that a credential reads this route
 * exactly when it reads /api/v1/outages. The distributors' files are the cut
 * down real ones the source's tests use, and no site is ever asked.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';
import { createHash } from 'node:crypto';

// vi.hoisted: see ../sources/endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  getWriterPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

import { outagesRouter } from '../../../src/api/outages.js';
import { mirror } from '../../../src/api/v1/mirror.js';
import { notFound, onError } from '../../../src/api/v1/respond.js';
import { requireApiKey } from '../../../src/services/auth/apiKey.js';
import { _clearApiKeyCache, lookupPrefix } from '../../../src/services/auth/apiKeys.js';
import { routeNeed } from '../../../src/services/auth/apiKeyScopes.js';
import { _resetNetworkAreas } from '../../../src/sources/outages/boundaries.js';
import { AREA_FILES } from '../sources/outages/boundaries.fixtures.js';

const NOW = Date.parse('2026-11-13T08:40:00Z');
const READ_AT = new Date(NOW).toISOString();

interface Answer {
  type: string;
  count: number;
  features: Array<{ type: string; geometry: { type: string; coordinates: unknown[] }; properties: Record<string, unknown> }>;
  sources: Array<{ distributor: string; name: string; state: string; updated_at: string | null }>;
}

const answer = (url: string): Promise<unknown> =>
  url in AREA_FILES ? Promise.resolve(AREA_FILES[url]) : Promise.reject(new Error(`unexpected URL ${url}`));

function app(opts: { gate?: boolean } = {}) {
  const a = new Hono();
  if (opts.gate) a.use('*', requireApiKey);
  a.route('/', outagesRouter);
  mirror(a, outagesRouter, ['/api/outages', '/api/outages/areas']);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
  _resetNetworkAreas();
  fetchJson.mockReset();
  fetchJson.mockImplementation(answer);
});
afterEach(() => {
  vi.useRealTimers();
  _resetNetworkAreas();
});

describe('GET /api/v1/outages/areas', () => {
  it('one bare FeatureCollection, the same on both paths: a feature per distributor with an area', async () => {
    const v1 = await app().request('/api/v1/outages/areas');
    expect(v1.status).toBe(200);
    expect(v1.headers.get('content-type')).toBe('application/json');
    const body = (await v1.json()) as Answer;
    expect(await (await app().request('/api/outages/areas')).json()).toEqual(body);
    expect(Object.keys(body).sort()).toEqual(['count', 'features', 'sources', 'type']);
    expect(body.type).toBe('FeatureCollection');
    expect(body.count).toBe(4);
    expect(body.features.map((f) => [f.type, f.geometry.type, f.properties])).toEqual([
      ['Feature', 'Polygon', { distributor: 'ausgrid', name: 'Ausgrid', state: 'NSW' }],
      ['Feature', 'Polygon', { distributor: 'citipower', name: 'CitiPower', state: 'VIC' }],
      ['Feature', 'Polygon', { distributor: 'powercor', name: 'Powercor', state: 'VIC' }],
      ['Feature', 'Polygon', { distributor: 'jemena', name: 'Jemena', state: 'VIC' }],
    ]);
    // Rings of [longitude, latitude], closed: what L.geoJSON draws.
    for (const f of body.features) {
      for (const ring of f.geometry.coordinates as number[][][]) {
        expect(ring.length).toBeGreaterThanOrEqual(4);
        expect(ring[ring.length - 1]).toEqual(ring[0]);
        for (const [lon, lat] of ring) {
          expect(lon).toBeGreaterThan(140);
          expect(lat).toBeLessThan(-25);
        }
      }
    }
    // Both paths were answered from one read of each file.
    expect(fetchJson).toHaveBeenCalledTimes(4);
  });

  it('`sources` lists every distributor that publishes an area, read or not', async () => {
    const body = (await (await app().request('/api/v1/outages/areas')).json()) as Answer;
    expect(body.sources).toEqual([
      { distributor: 'ausgrid', name: 'Ausgrid', state: 'NSW', updated_at: READ_AT },
      { distributor: 'citipower', name: 'CitiPower', state: 'VIC', updated_at: READ_AT },
      { distributor: 'powercor', name: 'Powercor', state: 'VIC', updated_at: READ_AT },
      { distributor: 'jemena', name: 'Jemena', state: 'VIC', updated_at: READ_AT },
    ]);
  });

  it('may be kept for a day once every area is read', async () => {
    for (const path of ['/api/v1/outages/areas', '/api/outages/areas']) {
      const res = await app().request(path);
      expect(res.headers.get('cache-control'), path).toBe('private, max-age=86400');
    }
  });

  it('an answer with an area missing is not kept for a day, and says which is missing', async () => {
    fetchJson.mockImplementation((url: string) => (url.includes('jemena') ? Promise.resolve({ error: 'gone' }) : answer(url)));
    const res = await app().request('/api/v1/outages/areas');
    expect(res.status).toBe(200);
    expect(res.headers.get('cache-control')).toBeNull();
    const body = (await res.json()) as Answer;
    expect(body.features.map((f) => f.properties['distributor'])).toEqual(['ausgrid', 'citipower', 'powercor']);
    expect(body.count).toBe(3);
    expect(body.sources.find((s) => s.distributor === 'jemena')!.updated_at).toBeNull();
  });

  it('every distributor failing is still an answer: no features, every source unread', async () => {
    fetchJson.mockRejectedValue(new Error('HTTP 503'));
    const res = await app().request('/api/v1/outages/areas');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Answer;
    expect(body).toMatchObject({ type: 'FeatureCollection', features: [], count: 0 });
    expect(body.sources.map((s) => s.updated_at)).toEqual([null, null, null, null]);
  });
});

describe('gated exactly like /api/v1/outages', () => {
  const KEY = 'ak_abcdefghij_' + 'A'.repeat(32);
  function withScopes(scopes: string[]) {
    _clearApiKeyCache();
    queryMock.mockReset();
    queryMock.mockResolvedValue({
      rows: [{
        id: 'k1', user_id: 'u1', name: 'cli', prefix: lookupPrefix(KEY),
        key_hash: createHash('sha256').update(KEY).digest('hex'),
        scopes, rate_limit_per_min: 100, created_at: 'x', last_used_at: null, expires_at: null, revoked_at: null,
      }],
      rowCount: 1,
    });
  }
  const asKey = { headers: { 'X-API-Key': KEY } };
  const PAIRS: Array<[areas: string, outages: string]> = [
    ['/api/v1/outages/areas', '/api/v1/outages'],
    ['/api/outages/areas', '/api/outages'],
  ];

  it('the route declares the same need: outages, live', async () => {
    const seen: Array<ReturnType<typeof routeNeed>> = [];
    const a = new Hono();
    a.use('*', async (c, next) => {
      seen.push(routeNeed(c));
      await next();
    });
    a.route('/', outagesRouter);
    mirror(a, outagesRouter, ['/api/outages', '/api/outages/areas']);
    for (const [areas, outages] of PAIRS) {
      seen.length = 0;
      await a.request(areas);
      await a.request(outages);
      expect(seen[0], areas).toEqual({ matched: true, need: { feed: 'outages', window: 'live' } });
      expect(seen[0], areas).toEqual(seen[1]);
    }
  });

  it('a key holding outages:live reads it', async () => {
    for (const [areas] of PAIRS) {
      withScopes(['outages:live']);
      const res = await app({ gate: true }).request(areas, asKey);
      expect(res.status, areas).toBe(200);
      expect(((await res.json()) as Answer).count).toBe(4);
    }
  });

  it('a key with another feed, or outages history, is refused, told what it needs', async () => {
    for (const scopes of [['fires:live'], ['outages:history']]) {
      withScopes(scopes);
      const res = await app({ gate: true }).request('/api/v1/outages/areas', asKey);
      expect(res.status, scopes[0]).toBe(403);
      expect(await res.json()).toEqual({
        error: { code: 'api_key_scope_forbidden', message: "This key's scopes do not cover this endpoint. It needs outages:live." },
      });
    }
    // Refused before anything was read.
    expect(fetchJson).not.toHaveBeenCalled();
  });

  it('a key with the old read scope reads it, as it reads the outages feed', async () => {
    for (const [areas] of PAIRS) {
      withScopes(['read']);
      expect((await app({ gate: true }).request(areas, asKey)).status, areas).toBe(200);
    }
  });

  it('every kind of credential gets the same answer here as on the outages feed', async () => {
    const credentials: Array<[string, string[] | null, RequestInit]> = [
      ['no credential', null, {}],
      ['an unknown key', [], { headers: { 'X-API-Key': 'ak_zzzzzzzzzz_' + 'Z'.repeat(32) } }],
      ['outages:live', ['outages:live'], asKey],
      ['outages:history', ['outages:history'], asKey],
      ['fires:live', ['fires:live'], asKey],
      ['every feed, live', ['*:live'], asKey],
      ['the old read scope', ['read'], asKey],
      ['a key with a browser Origin', ['outages:live'], { headers: { 'X-API-Key': KEY, Origin: 'https://example.com' } }],
    ];
    for (const [what, scopes, init] of credentials) {
      for (const [areas, outages] of PAIRS) {
        const statuses: number[] = [];
        const errors: unknown[] = [];
        for (const path of [areas, outages]) {
          if (scopes) withScopes(scopes);
          else {
            _clearApiKeyCache();
            queryMock.mockReset();
            queryMock.mockResolvedValue({ rows: [], rowCount: 0 });
          }
          const res = await app({ gate: true }).request(path, init);
          statuses.push(res.status);
          errors.push(res.status === 200 ? null : await res.json());
        }
        expect(statuses[0], `${what} ${areas}`).toBe(statuses[1]);
        expect(errors[0], `${what} ${areas}`).toEqual(errors[1]);
      }
    }
  });
});
