/**
 * GET /api/boundaries/:kind on /api/v1/ and on its old /api/ path, mirrored
 * alongside /api/boundaries/lga-names.
 *
 * One handler chain serves both (api/v1/mirror.ts). The answer is GeoJSON
 * from a pre-serialised byte cache, so the same bytes and headers go out on
 * both paths. The errors keep their old bodies on the old path and take the
 * v1 shape on /api/v1/. lga-names is registered first and must still be the
 * route that answers /api/v1/boundaries/lga-names.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const m = vi.hoisted(() => ({
  query: vi.fn(),
  pool: true,
}));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve(m.pool ? { query: m.query } : null)),
  getWriterPool: vi.fn(() => Promise.resolve(m.pool ? { query: m.query } : null)),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

const IN_SCOPE = ['/api/boundaries/lga-names', '/api/boundaries/:kind'];

async function app() {
  const { boundariesRouter, clearBoundaryCache } = await import('../../../src/api/boundaries.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  clearBoundaryCache();
  const a = new Hono();
  a.route('/', boundariesRouter);
  mirror(a, boundariesRouter, IN_SCOPE);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

const square = (x: number, y: number, s: number) => [[[x, y], [x + s, y], [x + s, y + s], [x, y + s], [x, y]]];

const ROWS = [
  { ext_id: 'lga1', name: 'Blacktown', short_name: 'Blacktown', state: 'NSW', class: null, geom: { type: 'Polygon', coordinates: square(150.8, -33.8, 0.1) } },
  { ext_id: 'lga2#0', name: 'Sutherland', short_name: 'Sutherland', state: 'NSW', class: null, geom: { type: 'MultiPolygon', coordinates: [square(151, -34.1, 0.05)] } },
];

/** The body as the route has always written it for a tight view: geometry as stored, nothing truncated. */
const TIGHT_BODY = JSON.stringify({
  type: 'FeatureCollection',
  features: ROWS.map((r) => ({
    type: 'Feature',
    geometry: r.geom,
    properties: { id: r.ext_id, kind: 'lga', name: r.name, shortName: r.short_name, state: r.state, class: r.class },
  })),
  count: 2,
  truncated: false,
});

const GEO_HEADERS = (cache: 'hit' | 'miss') => [
  ['cache-control', 'private, max-age=3600'],
  ['content-type', 'application/json; charset=UTF-8'],
  ['x-cache', cache],
];

beforeEach(() => {
  m.query.mockReset();
  m.query.mockResolvedValue({ rows: ROWS });
  m.pool = true;
});

describe('GET /api/boundaries/:kind and /api/v1/boundaries/:kind', () => {
  it('the same GeoJSON bytes and headers on both paths, cold then cached', async () => {
    const a = await app();
    const old = await a.request('/api/boundaries/lga?bbox=150.82,-34.08,151.08,-33.72');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual(GEO_HEADERS('miss'));
    expect(await old.text()).toBe(TIGHT_BODY);

    // Same snapped view: served from the byte cache, on either path.
    const v1 = await a.request('/api/v1/boundaries/lga?bbox=150.81,-34.09,151.09,-33.71');
    expect(v1.status).toBe(200);
    expect([...v1.headers]).toEqual(GEO_HEADERS('hit'));
    expect(await v1.text()).toBe(TIGHT_BODY);
    expect(m.query).toHaveBeenCalledTimes(1);

    const v1Cold = await a.request('/api/v1/boundaries/lga?bbox=150,-35,151,-34&limit=1');
    expect(v1Cold.status).toBe(200);
    expect(v1Cold.headers.get('X-Cache')).toBe('miss');
    const parsed = (await v1Cold.json()) as { type: string; count: number; truncated: boolean };
    expect(parsed.type).toBe('FeatureCollection'); // bare, not in `data`
    expect(parsed.count).toBe(1);
    expect(parsed.truncated).toBe(true);
  });

  it('state needs no bbox; lenient limits never become a 400', async () => {
    const a = await app();
    for (const qs of ['', '?limit=', '?limit=abc', '?limit=-5', '?limit=99999', '?limit=2&limit=x', '?bbox=junk']) {
      for (const p of ['/api/boundaries/state', '/api/v1/boundaries/state']) {
        const res = await a.request(`${p}${qs}`);
        expect(res.status, `${p}${qs}`).toBe(200);
        expect(((await res.json()) as { type: string }).type, `${p}${qs}`).toBe('FeatureCollection');
      }
    }
  });

  it('an unknown kind: the old 400 body; validation_failed on v1', async () => {
    const a = await app();
    for (const kind of ['foo', 'LGA']) {
      const old = await a.request(`/api/boundaries/${kind}?bbox=1,2,3,4`);
      expect(old.status, kind).toBe(400);
      expect([...old.headers], kind).toEqual([['content-type', 'application/json']]);
      expect(await old.text(), kind).toBe('{"error":"unknown_kind","kinds":["locality","lga","state"]}');
      const v1 = await a.request(`/api/v1/boundaries/${kind}?bbox=1,2,3,4`);
      expect(v1.status, kind).toBe(400);
      expect(await v1.json(), kind).toEqual({
        error: {
          code: 'validation_failed',
          message: 'kind: must be one of locality, lga, state',
          details: { issues: [{ path: 'kind', message: 'must be one of locality, lga, state' }] },
        },
      });
    }
    expect(m.query).not.toHaveBeenCalled();
  });

  it('locality and lga without a usable bbox: the old 400 body; validation_failed on v1', async () => {
    const a = await app();
    for (const [kind, qs] of [['locality', ''], ['lga', '?bbox='], ['lga', '?bbox=1,2,3'], ['locality', '?bbox=1,2,three,4']] as const) {
      const old = await a.request(`/api/boundaries/${kind}${qs}`);
      expect(old.status, kind + qs).toBe(400);
      expect(await old.text(), kind + qs).toBe(JSON.stringify({ error: 'bbox_required', kind }));
      const v1 = await a.request(`/api/v1/boundaries/${kind}${qs}`);
      expect(v1.status, kind + qs).toBe(400);
      const body = (await v1.json()) as { error: { code: string; details: { issues: Array<{ path: string }> } } };
      expect(body.error.code, kind + qs).toBe('validation_failed');
      expect(body.error.details.issues[0]!.path, kind + qs).toBe('bbox');
    }
    expect(m.query).not.toHaveBeenCalled();
  });

  it('no database: the old 503 body; service_unavailable on v1', async () => {
    m.pool = false;
    const a = await app();
    const old = await a.request('/api/boundaries/state');
    expect(old.status).toBe(503);
    expect([...old.headers]).toEqual([['content-type', 'application/json']]);
    expect(await old.text()).toBe('{"error":"database unavailable"}');
    const v1 = await a.request('/api/v1/boundaries/state');
    expect(v1.status).toBe(503);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('service_unavailable');
  });

  it('a failing query: the old 500 body; internal_error on v1', async () => {
    m.query.mockRejectedValue(new Error('statement timeout'));
    const a = await app();
    const old = await a.request('/api/boundaries/state');
    expect(old.status).toBe(500);
    expect(await old.text()).toBe('{"error":"query_failed"}');
    const v1 = await a.request('/api/v1/boundaries/state');
    expect(v1.status).toBe(500);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('internal_error');
  });
});

describe('lga-names alongside :kind', () => {
  it('/api/v1/boundaries/lga-names still reaches lga-names, not :kind', async () => {
    m.query.mockResolvedValue({ rows: [{ name: 'Blacktown' }, { name: 'Penrith' }] });
    const a = await app();
    const v1 = await a.request('/api/v1/boundaries/lga-names?state=nsw');
    expect(v1.status).toBe(200);
    expect(await v1.json()).toEqual({ data: { state: 'NSW', names: ['Blacktown', 'Penrith'] } });
    const old = await a.request('/api/boundaries/lga-names?state=NSW');
    expect(await old.json()).toEqual({ state: 'NSW', names: ['Blacktown', 'Penrith'] });
    // lga-names' own refusal, not :kind's unknown_kind.
    const bad = await a.request('/api/v1/boundaries/lga-names?state=XX');
    expect(bad.status).toBe(400);
    expect(((await bad.json()) as { error: { details: { issues: Array<{ path: string }> } } }).error.details.issues[0]!.path).toBe('state');
  });

  it('exactly the two routes are mirrored', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    const a = await app();
    const b = mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/boundaries/'));
    expect(b.map((r) => `${r.method} ${r.legacyPath}`).sort()).toEqual(IN_SCOPE.map((p) => `GET ${p}`).sort());
    const v1 = await a.request('/api/v1/boundaries/lga/extra');
    expect(v1.status).toBe(404);
    expect(await v1.json()).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
  });

  it('mirroring :kind without lga-names is refused, so lga-names cannot be shadowed', async () => {
    const { boundariesRouter } = await import('../../../src/api/boundaries.js');
    const { mirror } = await import('../../../src/api/v1/mirror.js');
    expect(() => mirror(new Hono(), boundariesRouter, ['/api/boundaries/:kind'])).toThrow(/runs before/);
  });
});
