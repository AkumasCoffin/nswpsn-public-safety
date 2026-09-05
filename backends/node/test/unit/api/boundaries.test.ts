/**
 * Boundary helper tests.
 *
 * The parts worth pinning are the ones a wrong answer would be silent
 * about: a bbox parsed with the corners the wrong way round returning
 * nothing instead of everything, and a point-in-polygon test that
 * ignores holes — which for these datasets means the ACT reading as
 * NSW, since the ACT is a hole in it.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import {
  parseBbox,
  geometryContains,
  simplifyRing,
  simplifyGeometry,
  toleranceFor,
  viewKey,
  clearBoundaryCache,
  __cache,
} from '../../../src/api/boundaries.js';

/** A closed square ring with `per` evenly spaced points along each side. */
function denseSquare(size: number, per: number): number[][] {
  const pts: number[][] = [];
  const step = size / per;
  for (let i = 0; i < per; i++) pts.push([i * step, 0]);
  for (let i = 0; i < per; i++) pts.push([size, i * step]);
  for (let i = 0; i < per; i++) pts.push([size - i * step, size]);
  for (let i = 0; i < per; i++) pts.push([0, size - i * step]);
  pts.push([0, 0]);
  return pts;
}

describe('parseBbox', () => {
  it('reads west,south,east,north', () => {
    expect(parseBbox('150.5,-34.2,151.5,-33.5')).toEqual([150.5, -34.2, 151.5, -33.5]);
  });

  it('normalises corners given the other way round', () => {
    // A caller that sends north,east first should still get its viewport
    // rather than an empty result.
    expect(parseBbox('151.5,-33.5,150.5,-34.2')).toEqual([150.5, -34.2, 151.5, -33.5]);
  });

  it('rejects anything that is not four finite numbers', () => {
    expect(parseBbox(undefined)).toBeNull();
    expect(parseBbox('')).toBeNull();
    expect(parseBbox('1,2,3')).toBeNull();
    expect(parseBbox('1,2,3,4,5')).toBeNull();
    expect(parseBbox('1,2,three,4')).toBeNull();
    expect(parseBbox('NaN,2,3,4')).toBeNull();
  });

  it('tolerates whitespace, which URLs pick up', () => {
    expect(parseBbox(' 150.5 , -34.2 , 151.5 , -33.5 ')).toEqual([150.5, -34.2, 151.5, -33.5]);
  });
});

describe('geometryContains', () => {
  const square = {
    type: 'Polygon',
    coordinates: [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
  };

  it('places a point inside and outside a simple polygon', () => {
    expect(geometryContains(square, 5, 5)).toBe(true);
    expect(geometryContains(square, 15, 5)).toBe(false);
    expect(geometryContains(square, 5, -1)).toBe(false);
  });

  it('respects a hole — the ACT-in-NSW case', () => {
    const withHole = {
      type: 'Polygon',
      coordinates: [
        [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
        [[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]],
      ],
    };
    expect(geometryContains(withHole, 1, 1)).toBe(true);   // in the ring
    expect(geometryContains(withHole, 5, 5)).toBe(false);  // in the hole
  });

  it('handles a MultiPolygon, which most of these rows are', () => {
    const multi = {
      type: 'MultiPolygon',
      coordinates: [
        [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        [[[5, 5], [6, 5], [6, 6], [5, 6], [5, 5]]],
      ],
    };
    expect(geometryContains(multi, 0.5, 0.5)).toBe(true);
    expect(geometryContains(multi, 5.5, 5.5)).toBe(true);
    expect(geometryContains(multi, 3, 3)).toBe(false);
  });

  it('returns false rather than throwing on junk', () => {
    expect(geometryContains(null, 1, 1)).toBe(false);
    expect(geometryContains({ type: 'Point', coordinates: [1, 1] }, 1, 1)).toBe(false);
    expect(geometryContains({ type: 'Polygon' }, 1, 1)).toBe(false);
    expect(geometryContains({ type: 'Polygon', coordinates: [] }, 1, 1)).toBe(false);
  });
});

describe('toleranceFor', () => {
  it('scales with the width of the view', () => {
    // A ten-degree view can show far less detail than a one-degree one,
    // and asking for the same precision in both is what made a
    // state-wide request 2 MB.
    const wide = toleranceFor([140, -39, 150, -34]);
    const narrow = toleranceFor([150, -34, 151, -33]);
    expect(wide).toBeGreaterThan(narrow);
    expect(wide).toBeCloseTo(10 / 3000, 8);
  });

  it('returns 0 for a view finer than the stored geometry', () => {
    // The import fetched at 0.0005 degrees; asking for finer than that
    // would cost CPU to change nothing.
    expect(toleranceFor([151.0, -33.9, 151.1, -33.8])).toBe(0);
    expect(toleranceFor(null)).toBe(0);
    expect(toleranceFor([151, -34, 151, -33])).toBe(0);
  });
});

describe('simplifyRing', () => {
  it('drops points that sit on the line between their neighbours', () => {
    const straight = [[0, 0], [1, 0], [2, 0], [3, 0], [4, 0]];
    expect(simplifyRing(straight, 0.1)).toEqual([[0, 0], [4, 0]]);
  });

  it('keeps the points that carry the shape', () => {
    const corner = [[0, 0], [1, 0], [2, 0], [2, 2], [0, 2], [0, 0]];
    const out = simplifyRing(corner, 0.1);
    expect(out).toContainEqual([2, 0]);
    expect(out).toContainEqual([2, 2]);
    // The collinear midpoint goes; the corners stay.
    expect(out).not.toContainEqual([1, 0]);
  });

  it('thins a dense ring hard without moving its corners', () => {
    const ring = denseSquare(1, 50);
    const out = simplifyRing(ring, 0.05);
    expect(out.length).toBeLessThan(10);
    expect(out[0]).toEqual([0, 0]);
    expect(out[out.length - 1]).toEqual([0, 0]);
  });

  it('leaves a ring alone when there is no tolerance to spend', () => {
    const ring = denseSquare(1, 10);
    expect(simplifyRing(ring, 0)).toBe(ring);
  });

  it('survives a ring whose endpoints coincide', () => {
    // A degenerate segment would divide by zero in the distance test.
    const ring = [[0, 0], [1, 1], [2, 0], [0, 0]];
    expect(() => simplifyRing(ring, 0.5)).not.toThrow();
  });

  it('handles a ring far longer than the call stack would take', () => {
    // Tasmania's coastline is tens of thousands of points; a recursive
    // implementation blows up on it.
    const big: number[][] = [];
    for (let i = 0; i < 60000; i++) big.push([i / 60000, Math.sin(i / 900) / 1000]);
    expect(() => simplifyRing(big, 0.01)).not.toThrow();
    expect(simplifyRing(big, 0.01).length).toBeLessThan(big.length);
  });
});

describe('simplifyGeometry', () => {
  const square = (x: number, y: number, size: number) => ({
    type: 'Polygon' as const,
    coordinates: [[[x, y], [x + size, y], [x + size, y + size], [x, y + size], [x, y]]],
  });

  it('passes geometry through untouched at zero tolerance', () => {
    const g = square(0, 0, 1);
    expect(simplifyGeometry(g, 0)).toBe(g);
  });

  it('keeps a shape that still fills more than a pixel', () => {
    const out = simplifyGeometry(square(0, 0, 1), 0.01) as { type: string };
    expect(out).not.toBeNull();
    expect(out.type).toBe('Polygon');
  });

  it('drops a part too small to render as more than a dot', () => {
    // This is what makes the state layer affordable: Tasmania is 5,887
    // polygons and nearly all of them are specks at national zoom.
    expect(simplifyGeometry(square(0, 0, 0.001), 0.05)).toBeNull();
  });

  it('drops only the specks from a MultiPolygon, not the mainland', () => {
    const out = simplifyGeometry(
      { type: 'MultiPolygon', coordinates: [square(0, 0, 1).coordinates, square(9, 9, 0.0001).coordinates] },
      0.01,
    ) as { type: string; coordinates: unknown[] };
    // One survivor doesn't need the MultiPolygon wrapper.
    expect(out.type).toBe('Polygon');
  });

  it('keeps both parts when both are big enough', () => {
    const out = simplifyGeometry(
      { type: 'MultiPolygon', coordinates: [square(0, 0, 1).coordinates, square(9, 9, 1).coordinates] },
      0.01,
    ) as { type: string; coordinates: unknown[] };
    expect(out.type).toBe('MultiPolygon');
    expect(out.coordinates).toHaveLength(2);
  });

  it('keeps a hole that survives, and loses one that does not', () => {
    const withHoles = {
      type: 'Polygon' as const,
      coordinates: [
        square(0, 0, 10).coordinates[0]!,
        square(2, 2, 4).coordinates[0]!,      // big hole — kept
        square(8, 8, 0.0005).coordinates[0]!, // pinprick — collapses
      ],
    };
    const out = simplifyGeometry(withHoles, 0.05) as { coordinates: unknown[] };
    expect(out.coordinates).toHaveLength(2);
  });

  it('still contains its own interior after simplifying', () => {
    // The whole point of a boundary is which side of it you are on.
    const g = { type: 'Polygon' as const, coordinates: [denseSquare(1, 40)] };
    const out = simplifyGeometry(g, 0.02);
    expect(geometryContains(out, 0.5, 0.5)).toBe(true);
    expect(geometryContains(out, 1.5, 0.5)).toBe(false);
  });
});

describe('viewKey', () => {
  it('rounds the viewport, so nudging the map by a street reuses the answer', () => {
    // The map rounds its own refetch key to the same twentieth of a
    // degree; matching that is what makes the cache hit at all.
    expect(viewKey('locality', [150.501, -34.201, 151.499, -33.499], 800))
      .toBe(viewKey('locality', [150.502, -34.202, 151.498, -33.498], 800));
  });

  it('separates viewports that really are different', () => {
    expect(viewKey('locality', [150.5, -34.2, 151.5, -33.5], 800))
      .not.toBe(viewKey('locality', [150.5, -34.2, 152.5, -33.5], 800));
  });

  it('separates kinds and limits — same box, different response', () => {
    const box: [number, number, number, number] = [150.5, -34.2, 151.5, -33.5];
    expect(viewKey('locality', box, 800)).not.toBe(viewKey('lga', box, 800));
    expect(viewKey('locality', box, 800)).not.toBe(viewKey('locality', box, 200));
  });
});

describe('boundary response cache', () => {
  beforeEach(() => clearBoundaryCache());

  it('returns what it was given', () => {
    __cache.put('k', '{"a":1}');
    expect(__cache.get('k')).toBe('{"a":1}');
    expect(__cache.get('other')).toBeNull();
  });

  it('tracks bytes and replaces rather than double-counting', () => {
    __cache.put('k', 'x'.repeat(100));
    expect(__cache.bytes()).toBe(100);
    __cache.put('k', 'y'.repeat(10));
    expect(__cache.bytes()).toBe(10);
    expect(__cache.size()).toBe(1);
  });

  it('evicts the least recently used once the budget is spent', () => {
    const big = 'x'.repeat(Math.floor(__cache.maxBytes / 3) + 1);
    __cache.put('a', big);
    __cache.put('b', big);
    // Touching 'a' makes 'b' the oldest.
    __cache.get('a');
    __cache.put('c', big);
    expect(__cache.get('b')).toBeNull();
    expect(__cache.get('a')).not.toBeNull();
    expect(__cache.get('c')).not.toBeNull();
    expect(__cache.bytes()).toBeLessThanOrEqual(__cache.maxBytes);
  });

  it('declines a response bigger than the whole budget', () => {
    // Storing it would evict everything and then still not fit.
    __cache.put('huge', 'x'.repeat(__cache.maxBytes + 1));
    expect(__cache.get('huge')).toBeNull();
    expect(__cache.bytes()).toBe(0);
  });
});

describe('GET /api/boundaries/lga-names', () => {
  // The signup form's State -> LGA flow. Public on purpose (signup runs
  // before auth exists) and cached per state for the process lifetime,
  // because LGA boundaries change on a census cadence.
  async function appWithPool(query: (sql: string, params?: unknown[]) => Promise<unknown>) {
    const { vi } = await import('vitest');
    vi.resetModules();
    vi.doMock('../../../src/db/pool.js', () => ({
      getPool: vi.fn(() => Promise.resolve({ query })),
      getWriterPool: vi.fn(() => Promise.resolve(null)),
      closePool: vi.fn(),
    }));
    const { boundariesRouter } = await import('../../../src/api/boundaries.js');
    const { Hono } = await import('hono');
    const app = new Hono();
    app.route('/', boundariesRouter);
    return app;
  }

  it('lists a state’s LGA names, sorted by the query itself', async () => {
    const { vi } = await import('vitest');
    const q = vi.fn(async () => ({ rows: [{ name: 'Blacktown' }, { name: 'Penrith' }] }));
    const app = await appWithPool(q as never);
    const res = await app.request('/api/boundaries/lga-names?state=nsw');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({ state: 'NSW', names: ['Blacktown', 'Penrith'] });
    expect(String(q.mock.calls[0]![0])).toContain("kind = 'lga'");
  });

  it('caches per state, so a second request costs no query', async () => {
    const { vi } = await import('vitest');
    const q = vi.fn(async () => ({ rows: [] }));
    const app = await appWithPool(q as never);
    await app.request('/api/boundaries/lga-names?state=ACT');
    // ACT genuinely has no incorporated LGAs — an empty list is an answer,
    // not a miss, and must be cached like any other.
    const res = await app.request('/api/boundaries/lga-names?state=ACT');
    expect((await res.json()).names).toEqual([]);
    expect(q).toHaveBeenCalledTimes(1);
  });

  it('rejects anything that is not an Australian state code', async () => {
    const app = await appWithPool((async () => ({ rows: [] })) as never);
    expect((await app.request('/api/boundaries/lga-names?state=XX')).status).toBe(400);
    expect((await app.request('/api/boundaries/lga-names')).status).toBe(400);
  });
});
