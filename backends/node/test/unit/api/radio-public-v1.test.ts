/**
 * GET /api/radio/grn-sites on /api/v1/ and on its old /api/ path.
 *
 * One handler chain serves both (api/v1/mirror.ts): the old path answers byte
 * for byte as it did, v1 wraps the body in `data` and answers errors in the
 * shared shape. The old bodies are written out literally.
 *
 * Not mirrored: /api/radio/monitored-sites (node status, for the site's own
 * map) and the owner's PATCH /api/radio/grn-sites/:id.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';

const m = vi.hoisted(() => ({
  pool: 'ok' as 'none' | 'ok' | 'fail',
  rows: [] as unknown[],
  queries: 0,
}));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => {
    if (m.pool === 'none') return null;
    return {
      query: async () => {
        m.queries++;
        if (m.pool === 'fail') throw new Error('connection reset');
        return { rows: m.rows, rowCount: m.rows.length };
      },
    };
  }),
  getWriterPool: vi.fn(async () => null),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

vi.mock('../../../src/services/nodes/hub.js', () => ({
  hub: { liveStatus: () => ({ status: null }), isOnline: () => false },
}));

const NOW = Date.UTC(2026, 9, 28, 4, 0, 0);
const JSON_TYPE = 'application/json';

const ROWS = [
  {
    id: 1,
    data: {
      NAME: 'Governor Phillip Tower 1 Farrer Place SYDNEY',
      Latitude: -33.864292,
      Longitude: 151.211542,
      'GRN Site ID #': '001-031',
      'NAC Code': '2D0',
      Frequencies: ['413.125000', '415.262500 ALT CC'],
      'Last Update': '25/04/2024',
    },
  },
  { id: 2, data: { NAME: ['M4 East tunnel', 'Location\tHOMEBUSH NSW'], Latitude: null, Longitude: null, Frequencies: '421.8750' } },
];
const SITES = { sites: ROWS.map((r) => ({ id: r.id, ...r.data })) };

async function app() {
  const { radioPublicRouter } = await import('../../../src/api/radio-public.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', radioPublicRouter);
  mirror(a, radioPublicRouter, ['/api/radio/grn-sites']);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

beforeEach(() => {
  // The list is cached in module state: start each test cold.
  vi.resetModules();
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
  m.pool = 'ok';
  m.rows = ROWS;
  m.queries = 0;
});
afterEach(() => vi.useRealTimers());

describe('GET /api/radio/grn-sites and /api/v1/radio/grn-sites', () => {
  it('every site: bare on the old path, in data on v1', async () => {
    const a = await app();
    const old = await a.request('/api/radio/grn-sites');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify(SITES));

    const v1 = await a.request('/api/v1/radio/grn-sites');
    expect(v1.status).toBe(200);
    expect([...v1.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await v1.text()).toBe(JSON.stringify({ data: SITES }));
    expect(m.queries).toBe(1); // the second read came from the 30-second cache
  });

  it('read again once the cache is 30 seconds old', async () => {
    const a = await app();
    await a.request('/api/v1/radio/grn-sites');
    m.rows = [ROWS[0]];
    vi.setSystemTime(NOW + 29_000);
    expect(await (await a.request('/api/v1/radio/grn-sites')).json()).toEqual({ data: SITES });
    vi.setSystemTime(NOW + 31_000);
    expect(await (await a.request('/api/v1/radio/grn-sites')).json()).toEqual({
      data: { sites: [{ id: 1, ...ROWS[0]!.data }] },
    });
  });

  it('no database: 503 with the old body on the old path', async () => {
    m.pool = 'none';
    const a = await app();
    const old = await a.request('/api/radio/grn-sites');
    expect(old.status).toBe(503);
    expect(await old.text()).toBe('{"error":"database unavailable"}');
    const v1 = await a.request('/api/v1/radio/grn-sites');
    expect(v1.status).toBe(503);
    expect(await v1.json()).toEqual({
      error: { code: 'service_unavailable', message: 'The radio site list is not available just now.' },
    });
  });

  it('a failing read with nothing cached: 500 with the old body on the old path', async () => {
    m.pool = 'fail';
    const a = await app();
    const old = await a.request('/api/radio/grn-sites');
    expect(old.status).toBe(500);
    expect(await old.text()).toBe('{"error":"query failed"}');
    const v1 = await a.request('/api/v1/radio/grn-sites');
    expect(v1.status).toBe(500);
    expect(await v1.json()).toEqual({
      error: { code: 'internal_error', message: 'The radio site list could not be read.' },
    });
  });

  it('a failing read with an earlier copy: that copy, on both paths', async () => {
    const a = await app();
    await a.request('/api/radio/grn-sites');
    m.pool = 'fail';
    vi.setSystemTime(NOW + 60_000);
    const old = await a.request('/api/radio/grn-sites');
    expect(old.status).toBe(200);
    expect(await old.text()).toBe(JSON.stringify(SITES));
    const v1 = await a.request('/api/v1/radio/grn-sites');
    expect(v1.status).toBe(200);
    expect(await v1.text()).toBe(JSON.stringify({ data: SITES }));
  });
});

describe('what is not mirrored', () => {
  it('/api/v1/radio/monitored-sites is a v1 404; the old path still answers as before', async () => {
    m.pool = 'none';
    const a = await app();
    const v1 = await a.request('/api/v1/radio/monitored-sites');
    expect(v1.status).toBe(404);
    expect(await v1.json()).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
    const old = await a.request('/api/radio/monitored-sites');
    expect(old.status).toBe(503);
    expect(await old.text()).toBe('{"error":"database unavailable"}');
  });

  it('PATCH /api/v1/radio/grn-sites/:id is a v1 404; the old PATCH still asks for the owner', async () => {
    const a = await app();
    const init = { method: 'PATCH', body: '{"NAME":"x"}', headers: { 'content-type': 'application/json' } };
    const v1 = await a.request('/api/v1/radio/grn-sites/1', init);
    expect(v1.status).toBe(404);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('not_found');
    const old = await a.request('/api/radio/grn-sites/1', init);
    expect(old.status).toBe(401);
    expect(await old.text()).toBe('{"error":"authentication required"}');
    expect(m.queries).toBe(0);
  });

  it('mirroring registers exactly the GET', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    await app();
    expect(mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/radio/')).map((r) => `${r.method} ${r.legacyPath}`)).toEqual([
      'GET /api/radio/grn-sites',
    ]);
  });
});
