/**
 * The MarineTraffic routes on /api/v1/ and on their old /api/ paths.
 *
 * One handler chain serves both (api/v1/mirror.ts), so the old path must
 * answer byte for byte as it did — every status, body and error — while v1
 * wraps the body in `data` and answers errors in the shared shape. The old
 * bodies are written out literally here.
 *
 * The router keeps its caches and refresh state in module scope, so every
 * test loads it fresh. /api/marinetraffic/vessel-image/:id is a dead route
 * left as it is: not mirrored.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const m = vi.hoisted(() => ({
  ready: true,
  fetchJson: vi.fn(),
  fetchVesselDetail: vi.fn(),
  fetchBinary: vi.fn(),
  query: vi.fn(),
}));

vi.mock('../../../src/services/marinetrafficBrowser.js', () => ({
  marinetrafficBrowser: {
    isReady: () => m.ready,
    fetchJson: m.fetchJson,
    fetchVesselDetail: m.fetchVesselDetail,
    fetchBinary: m.fetchBinary,
  },
}));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: m.query })),
  getWriterPool: vi.fn(() => Promise.resolve({ query: m.query })),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

const IN_SCOPE = ['/api/marinetraffic/vessels', '/api/marinetraffic/vessel/:id'];

async function app() {
  vi.resetModules();
  const { marinetrafficRouter } = await import('../../../src/api/marinetraffic.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', marinetrafficRouter);
  mirror(a, marinetrafficRouter, IN_SCOPE);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

const ROW_1 = { SHIP_ID: '371234', SHIPNAME: 'SPIRIT OF SYDNEY', LAT: '-33.85', LON: '151.21', SPEED: '0' };
const ROW_2 = { SHIP_ID: '371235', SHIPNAME: 'PACIFIC TUG', LAT: -33.9, LON: 151.25 };
const TILE = { type: 1, data: { rows: [ROW_1, ROW_2] } };
const DETAIL = { shipId: '371234', name: 'SPIRIT OF SYDNEY', imo: '9123456', mmsi: '503000123', length: 95 };

const errorCode = async (res: Response) => ((await res.json()) as { error: { code: string } }).error.code;

beforeEach(() => {
  m.ready = true;
  m.fetchJson.mockReset();
  m.fetchVesselDetail.mockReset();
  m.fetchBinary.mockReset();
  m.query.mockReset();
  m.query.mockResolvedValue({ rows: [] });
});

describe('GET /api/marinetraffic/vessels and /api/v1/marinetraffic/vessels', () => {
  it('cold, with no browser: the old 503 body on the old path, service_unavailable on v1', async () => {
    m.ready = false;
    const a = await app();
    const old = await a.request('/api/marinetraffic/vessels');
    expect(old.status).toBe(503);
    expect(await old.text()).toBe(
      JSON.stringify({
        error: 'browser worker not ready',
        message:
          'MarineTraffic proxy requires the headless browser worker. Check that playwright is installed and MARINETRAFFIC_DISABLED is not set.',
        vessels: [],
      }),
    );
    const v1 = await a.request('/api/v1/marinetraffic/vessels');
    expect(v1.status).toBe(503);
    expect(await errorCode(v1)).toBe('service_unavailable');
    expect(m.fetchJson).not.toHaveBeenCalled();
  });

  it('cold: warming while the first batch runs, then the merged tiles — in data on v1', async () => {
    m.fetchJson.mockResolvedValue(TILE);
    const a = await app();

    const old = await a.request('/api/marinetraffic/vessels');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', 'application/json']]);
    expect(await old.text()).toBe('{"type":1,"data":{"rows":[],"areaShips":0},"warming":true}');
    const v1 = await a.request('/api/v1/marinetraffic/vessels');
    expect(v1.status).toBe(200);
    expect(await v1.text()).toBe('{"data":{"type":1,"data":{"rows":[],"areaShips":0},"warming":true}}');

    // Every tile gives the same two vessels and no count of its own: one row
    // each after the merge, and the row count stands in for areaShips.
    const merged = JSON.stringify({ type: 1, data: { rows: [ROW_1, ROW_2], areaShips: 2 } });
    await vi.waitFor(async () => {
      expect(await (await a.request('/api/marinetraffic/vessels')).text()).toBe(merged);
    });
    const v1Warm = await a.request('/api/v1/marinetraffic/vessels');
    expect(v1Warm.status).toBe(200);
    expect(await v1Warm.text()).toBe(`{"data":${merged}}`);
  });

  it("sums the tiles' own counts when they give them", async () => {
    m.fetchJson.mockResolvedValue({ type: 1, data: { rows: [ROW_1], areaShips: 3 } });
    const a = await app();
    await a.request('/api/marinetraffic/vessels');
    await vi.waitFor(async () => {
      const body = (await (await a.request('/api/marinetraffic/vessels')).json()) as {
        warming?: boolean;
        data: { rows: unknown[]; areaShips: number };
      };
      expect(body.warming).toBeUndefined();
      expect(body.data.rows).toEqual([ROW_1]);
      const tiles = new Set(m.fetchJson.mock.calls.map(([url]) => url)).size;
      expect(tiles).toBeGreaterThan(1);
      expect(body.data.areaShips).toBe(3 * tiles);
    });
    const old = await (await a.request('/api/marinetraffic/vessels')).text();
    expect(await (await a.request('/api/v1/marinetraffic/vessels')).text()).toBe(`{"data":${old}}`);
  });
});

describe('GET /api/marinetraffic/vessel/:id and /api/v1/marinetraffic/vessel/:id', () => {
  it('not a number: the old 400 body, validation_failed on v1', async () => {
    const a = await app();
    const old = await a.request('/api/marinetraffic/vessel/abc');
    expect(old.status).toBe(400);
    expect(await old.text()).toBe('{"error":"invalid ship id"}');
    const v1 = await a.request('/api/v1/marinetraffic/vessel/abc');
    expect(v1.status).toBe(400);
    const body = (await v1.json()) as { error: { code: string; details: { issues: Array<{ path: string }> } } };
    expect(body.error.code).toBe('validation_failed');
    expect(body.error.details.issues[0]!.path).toBe('id');
    expect(m.query).not.toHaveBeenCalled();
  });

  it('read from MarineTraffic, then from memory — the record as it came, in data on v1', async () => {
    m.fetchVesselDetail.mockResolvedValue(DETAIL);
    const a = await app();
    const old = await a.request('/api/marinetraffic/vessel/371234');
    expect(old.status).toBe(200);
    expect(await old.text()).toBe(JSON.stringify(DETAIL));
    // Written back to the database for next time.
    expect(m.query.mock.calls.some(([sql]) => /INSERT INTO marinetraffic_vessel_cache/.test(String(sql)))).toBe(true);

    const v1 = await a.request('/api/v1/marinetraffic/vessel/371234');
    expect(v1.status).toBe(200);
    expect(await v1.text()).toBe(JSON.stringify({ data: DETAIL }));
    expect(m.fetchVesselDetail).toHaveBeenCalledTimes(1);
  });

  it('a fresh database row is served without MarineTraffic', async () => {
    m.query.mockResolvedValue({ rows: [{ data: DETAIL, fetched_at: new Date(Date.now() - 3_600_000) }] });
    // A fresh router each time, so neither answer comes from the in-memory layer.
    const a = await app();
    expect(await (await a.request('/api/marinetraffic/vessel/371234')).text()).toBe(JSON.stringify(DETAIL));
    const b = await app();
    expect(await (await b.request('/api/v1/marinetraffic/vessel/371234')).text()).toBe(JSON.stringify({ data: DETAIL }));
    expect(m.fetchVesselDetail).not.toHaveBeenCalled();
  });

  it('a stale database row stands in when MarineTraffic cannot be read', async () => {
    m.ready = false;
    m.query.mockResolvedValue({ rows: [{ data: DETAIL, fetched_at: new Date(Date.now() - 3 * 86_400_000) }] });
    const a = await app();
    const old = await a.request('/api/marinetraffic/vessel/371234');
    expect(old.status).toBe(200);
    expect(await old.text()).toBe(JSON.stringify(DETAIL));
    const v1 = await a.request('/api/v1/marinetraffic/vessel/371234');
    expect(await v1.text()).toBe(JSON.stringify({ data: DETAIL }));
  });

  it('no browser and nothing cached: the old 503 body, service_unavailable on v1', async () => {
    m.ready = false;
    const a = await app();
    const old = await a.request('/api/marinetraffic/vessel/371234');
    expect(old.status).toBe(503);
    expect(await old.text()).toBe('{"error":"browser worker not ready"}');
    const v1 = await a.request('/api/v1/marinetraffic/vessel/371234');
    expect(v1.status).toBe(503);
    expect(await errorCode(v1)).toBe('service_unavailable');
  });

  it('MarineTraffic gives nothing and nothing is cached: the old 502 body; 502 service_unavailable on v1', async () => {
    m.fetchVesselDetail.mockResolvedValue(null);
    const a = await app();
    const old = await a.request('/api/marinetraffic/vessel/371234');
    expect(old.status).toBe(502);
    expect([...old.headers]).toEqual([['content-type', 'application/json']]);
    expect(await old.text()).toBe('{"error":"vessel detail not found"}');
    const v1 = await a.request('/api/v1/marinetraffic/vessel/371234');
    expect(v1.status).toBe(502);
    expect(await v1.json()).toEqual({
      error: { code: 'service_unavailable', message: 'MarineTraffic gave no record for this vessel.' },
    });
  });
});

describe('what is not mirrored', () => {
  it('/api/v1/marinetraffic/vessel-image/x is a v1 404; the old route is untouched', async () => {
    const a = await app();
    const v1 = await a.request('/api/v1/marinetraffic/vessel-image/x');
    expect(v1.status).toBe(404);
    expect(await v1.json()).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });

    const bad = await a.request('/api/marinetraffic/vessel-image/x');
    expect(bad.status).toBe(400);
    expect(bad.headers.get('Content-Type')).toMatch(/^text\/plain/);
    expect(await bad.text()).toBe('invalid id');

    m.fetchBinary.mockResolvedValue({ status: 200, bytes: new Uint8Array([1, 2, 3]), contentType: 'image/webp' });
    const img = await a.request('/api/marinetraffic/vessel-image/371234');
    expect(img.status).toBe(200);
    expect(img.headers.get('Content-Type')).toBe('image/webp');
    expect(img.headers.get('Cache-Control')).toBe('public, max-age=21600');
    expect(new Uint8Array(await img.arrayBuffer())).toEqual(new Uint8Array([1, 2, 3]));
  });

  it('mirroring only the in-scope paths registers exactly those', async () => {
    await app();
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    const mt = mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/marinetraffic/'));
    expect(mt.map((r) => `${r.method} ${r.legacyPath}`).sort()).toEqual(IN_SCOPE.map((p) => `GET ${p}`).sort());
  });
});
