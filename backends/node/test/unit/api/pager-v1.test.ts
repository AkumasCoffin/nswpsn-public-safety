/**
 * GET /api/pager/hits on /api/v1/ and on its old /api/ path.
 *
 * One handler chain serves both (api/v1/mirror.ts). The body is GeoJSON, so
 * it is bare on both paths, byte for byte. They part only when the database
 * gives no answer — down, failing, slow, or in the backoff after a failure:
 * the old path falls back to the poller's snapshot as it always has, and v1
 * answers 503 rather than pass that partial list off as the window.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';

const m = vi.hoisted(() => ({
  pool: 'ok' as 'none' | 'ok' | 'fail' | 'slow',
  rows: [] as unknown[],
  snapshot: { messages: [] as unknown[], count: 0 },
  params: [] as unknown[][],
}));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => {
    if (m.pool === 'none') return null;
    return {
      connect: async () => ({
        query: async (sql: string, params?: unknown[]) => {
          if (/^(BEGIN|COMMIT|ROLLBACK|SET LOCAL)/.test(sql.trim())) return { rows: [] };
          m.params.push(params ?? []);
          if (m.pool === 'fail') throw new Error('statement timeout');
          if (m.pool === 'slow') return new Promise((r) => setTimeout(() => r({ rows: m.rows }), 10_000));
          return { rows: m.rows };
        },
        release: () => {},
      }),
    };
  }),
  getWriterPool: vi.fn(async () => null),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

vi.mock('../../../src/sources/pager.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/sources/pager.js')>()),
  pagerSnapshot: () => m.snapshot,
}));

const NOW = Date.UTC(2026, 9, 28, 4, 0, 0);
const NOW_S = NOW / 1000;
const JSON_TYPE = 'application/json';

const ROW = {
  source_id: '101',
  lat: -33.8,
  lng: 151.2,
  category: 'NSWRFS',
  subcategory: '1234567',
  fetched_at: new Date(NOW - 60_000),
  data: {
    id: 101,
    incident_id: '26-121910',
    capcode: '1234567',
    alias: 'Kenthurst',
    agency: 'NSWRFS',
    message: '08 September 2026 14:31:37 CVDO - 26-121910 - MVA - INCIDENT CALL - X ST - [151.2,-33.8]',
    type: 'MVA',
    call_class: 'INCIDENT CALL',
    is_stop: false,
    incident_time: '2026-10-28T14:59:00',
    timestamp: NOW_S - 90,
  },
};
// A row the route never maps: no position.
const COORDLESS = { ...ROW, source_id: '102', lat: null, data: { ...ROW.data, id: 102 } };

/** What the route makes of ROW, written out as it always has. */
const ROW_BODY = {
  type: 'FeatureCollection',
  features: [
    {
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [151.2, -33.8] },
      properties: {
        id: '101',
        pager_msg_id: 101,
        incident_id: '26-121910',
        capcode: '1234567',
        alias: 'Kenthurst',
        agency: 'NSWRFS',
        message: 'CVDO - 26-121910 - MVA - INCIDENT CALL - X ST - [151.2,-33.8]',
        type: 'MVA',
        call_class: 'INCIDENT CALL',
        is_stop: false,
        incident_time: '2026-10-28T14:59:00',
        fetched_at: NOW_S - 60,
        timestamp: NOW_S - 90,
        is_live: true,
        lat: -33.8,
        lon: 151.2,
      },
    },
  ],
  count: 1,
  hours: 24,
};

const MESSAGE = {
  id: 5,
  incident_id: '26-000005',
  capcode: 'C1',
  alias: 'Galston',
  agency: 'NSWRFS',
  source: 's',
  message: 'm',
  type: 'Bush Fire',
  call_class: 'FIRECALL',
  address_text: '',
  is_stop: false,
  lat: -33.6,
  lon: 151.0,
  incident_time: '2026-10-28T14:58:20',
  timestamp: NOW_S - 100,
};

/** The old path's fallback: the poller's snapshot, shaped the same way. */
const SNAPSHOT_BODY = {
  type: 'FeatureCollection',
  features: [
    {
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [151.0, -33.6] },
      properties: {
        id: '5',
        pager_msg_id: 5,
        incident_id: '26-000005',
        capcode: 'C1',
        alias: 'Galston',
        agency: 'NSWRFS',
        message: 'm',
        type: 'Bush Fire',
        call_class: 'FIRECALL',
        is_stop: false,
        incident_time: '2026-10-28T14:58:20',
        fetched_at: NOW_S - 100,
        timestamp: NOW_S - 100,
        is_live: true,
        lat: -33.6,
        lon: 151.0,
      },
    },
  ],
  count: 1,
  hours: 24,
};

const UNAVAILABLE = {
  error: {
    code: 'service_unavailable',
    message: 'The pager message history could not be read just now; try again shortly.',
  },
};

async function app() {
  const { pagerRouter } = await import('../../../src/api/pager.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', pagerRouter);
  mirror(a, pagerRouter, ['/api/pager/hits']);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

beforeEach(() => {
  // The route keeps its cache and DB backoff in module state: start clean.
  vi.resetModules();
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] });
  vi.setSystemTime(NOW);
  m.pool = 'ok';
  m.rows = [ROW, COORDLESS];
  m.snapshot = { messages: [MESSAGE], count: 1 };
  m.params = [];
});
afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
});

describe('GET /api/pager/hits and /api/v1/pager/hits', () => {
  it('from the database: the same bare FeatureCollection on both paths', async () => {
    const a = await app();
    const old = await a.request('/api/pager/hits');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify(ROW_BODY));

    const v1 = await a.request('/api/v1/pager/hits');
    expect(v1.status).toBe(200);
    expect([...v1.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await v1.text()).toBe(JSON.stringify(ROW_BODY));
  });

  it('lenient parameters stay lenient on both paths: junk is the default, never a 400', async () => {
    // [query, hours served, limit sent to the database]. Each case names its
    // own capcode so it is a key of its own, and the database is asked.
    const cases: Array<[string, number, number]> = [
      ['hours=abc', 24, 500],
      ['hours=0&limit=0', 24, 500],
      ['hours=999&limit=99999', 168, 2000],
      ['hours=12.9&limit=2.5', 12, 2],
      ['hours=-3&limit=-1', 24, 500],
      ['hours=6&hours=48&limit=7&limit=9', 6, 7], // a repeat: the first value
    ];
    const a = await app();
    for (const [i, [qs, hours, limit]] of cases.entries()) {
      for (const [j, p] of ['/api/pager/hits', '/api/v1/pager/hits'].entries()) {
        m.params = [];
        const url = `${p}?${qs}&capcode=c${i}-${j}`;
        const res = await a.request(url);
        expect(res.status, url).toBe(200);
        expect(((await res.json()) as { hours: number }).hours, url).toBe(hours);
        expect(m.params[0]!.at(-1), url).toBe(limit);
      }
    }
  });

  it('capcode and incident_id filter in the query; empty means no filter', async () => {
    const a = await app();
    await a.request('/api/v1/pager/hits?capcode=1234567&incident_id=26-121910');
    expect(m.params[0]!.slice(2)).toEqual(['1234567', '26-121910', 500]);
    m.params = [];
    await a.request('/api/v1/pager/hits?capcode=&incident_id=&hours=5');
    expect(m.params[0]!.slice(2)).toEqual([500]);
  });

  it('a warm cache answers both paths without the database', async () => {
    const a = await app();
    await a.request('/api/pager/hits');
    m.params = [];
    const v1 = await a.request('/api/v1/pager/hits');
    expect(await v1.text()).toBe(JSON.stringify(ROW_BODY));
    expect(m.params).toEqual([]);
  });

  it('no database: the snapshot on the old path, 503 on v1', async () => {
    m.pool = 'none';
    const a = await app();
    const old = await a.request('/api/pager/hits');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify(SNAPSHOT_BODY));

    const v1 = await a.request('/api/v1/pager/hits');
    expect(v1.status).toBe(503);
    expect(await v1.json()).toEqual(UNAVAILABLE);
  });

  it('a failing database, and the backoff after it: the snapshot on the old path, 503 on v1', async () => {
    m.pool = 'fail';
    const a = await app();
    const v1 = await a.request('/api/v1/pager/hits');
    expect(v1.status).toBe(503);
    expect(await v1.json()).toEqual(UNAVAILABLE);
    expect(m.params).toHaveLength(1);

    // In the backoff the database is not asked at all.
    m.pool = 'ok';
    m.params = [];
    const v1Again = await a.request('/api/v1/pager/hits?hours=6');
    expect(v1Again.status).toBe(503);
    const old = await a.request('/api/pager/hits');
    expect(old.status).toBe(200);
    expect(await old.text()).toBe(JSON.stringify(SNAPSHOT_BODY));
    expect(m.params).toEqual([]);
  });

  it('a slow database: after four seconds the snapshot on the old path, 503 on v1', async () => {
    m.pool = 'slow';
    const a = await app();
    const pending = a.request('/api/v1/pager/hits');
    await vi.advanceTimersByTimeAsync(4_001);
    const v1 = await pending;
    expect(v1.status).toBe(503);
    expect(await v1.json()).toEqual(UNAVAILABLE);

    const pendingOld = a.request('/api/pager/hits?limit=3');
    await vi.advanceTimersByTimeAsync(4_001);
    const old = await pendingOld;
    expect(old.status).toBe(200);
    expect(await old.text()).toBe(JSON.stringify(SNAPSHOT_BODY));

    // The refresh kept running and warmed the key: v1 now answers from it.
    await vi.advanceTimersByTimeAsync(10_000);
    const warm = await a.request('/api/v1/pager/hits');
    expect(warm.status).toBe(200);
    expect(await warm.text()).toBe(JSON.stringify(ROW_BODY));
  });

  it('past the stale window with the database failing: the snapshot on the old path, 503 on v1', async () => {
    const a = await app();
    await a.request('/api/pager/hits'); // warm
    m.pool = 'fail';
    vi.setSystemTime(NOW + 11 * 60_000); // past stale, still held: the refresh blocks
    const v1 = await a.request('/api/v1/pager/hits');
    expect(v1.status).toBe(503);
    expect(await v1.json()).toEqual(UNAVAILABLE);

    // The same on the old path, from a fresh start.
    vi.resetModules();
    const b = await app();
    m.pool = 'ok';
    vi.setSystemTime(NOW);
    await b.request('/api/pager/hits');
    m.pool = 'fail';
    vi.setSystemTime(NOW + 11 * 60_000);
    const old = await b.request('/api/pager/hits');
    expect(old.status).toBe(200);
    expect(await old.text()).toBe(JSON.stringify(SNAPSHOT_BODY));
  });

  it('during the backoff a warm key is still served from the cache on both paths', async () => {
    const a = await app();
    await a.request('/api/pager/hits'); // warm
    m.pool = 'fail';
    vi.setSystemTime(NOW + 2 * 60_000); // stale: served, refresh fails behind → backoff
    await a.request('/api/pager/hits');
    await vi.advanceTimersByTimeAsync(0);
    m.params = [];
    for (const p of ['/api/pager/hits', '/api/v1/pager/hits']) {
      const res = await a.request(p);
      expect(res.status, p).toBe(200);
      expect(await res.text(), p).toBe(JSON.stringify(ROW_BODY));
    }
    expect(m.params).toEqual([]);
  });

  it('mirroring registers exactly this route', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    await app();
    expect(mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/pager/')).map((r) => `${r.method} ${r.legacyPath}`)).toEqual([
      'GET /api/pager/hits',
    ]);
  });
});
