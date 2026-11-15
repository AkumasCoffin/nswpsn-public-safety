/**
 * Network area tests (sources/outages/boundaries.ts).
 *
 * The files are the distributors' own (boundaries.fixtures.ts), cut short.
 * The read tests mock the shared HTTP module, so no site is ever asked.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// vi.hoisted: see ../endeavour.test.ts for the rationale.
const { fetchJson } = vi.hoisted(() => ({ fetchJson: vi.fn() }));
vi.mock('../../../../src/sources/shared/http.js', () => ({
  fetchJson,
  HttpError: class HttpError extends Error {},
}));

import {
  AREA_MIN_RING_DEG2,
  AREA_REFRESH_MS,
  AREA_RETRY_MS,
  AREA_SOURCES,
  AREA_TOLERANCE_DEG,
  _networkAreaReads,
  _resetNetworkAreas,
  areaPolygons,
  networkAreas,
  parseArea,
  ringArea,
  simplifyArea,
  simplifyRing,
  unmask,
  type AreaGeometry,
} from '../../../../src/sources/outages/boundaries.js';
import {
  AREA_FILES,
  AUSGRID_AREA,
  CITIPOWER_AREA,
  JEMENA_AREA,
  JEMENA_POCKET,
  JEMENA_RING,
  JEMENA_WORLD,
} from './boundaries.fixtures.js';

const NOW = Date.parse('2026-11-13T08:40:00Z');
const ENV = ['JEMENA_DISABLED'];

const ringsOf = (g: AreaGeometry): number[][][] => (g.type === 'Polygon' ? g.coordinates : g.coordinates.flat(1));
const round4 = (p: number[]) => [Math.round(p[0]! * 1e4) / 1e4, Math.round(p[1]! * 1e4) / 1e4];

/** How far a point is from the nearest edge of a closed ring, in degrees. */
function distanceToRing(p: number[], ring: number[][]): number {
  let best = Infinity;
  for (let i = 1; i < ring.length; i++) {
    const a = ring[i - 1]!;
    const b = ring[i]!;
    const dx = b[0]! - a[0]!;
    const dy = b[1]! - a[1]!;
    const len2 = dx * dx + dy * dy;
    const t = len2 === 0 ? 0 : Math.max(0, Math.min(1, ((p[0]! - a[0]!) * dx + (p[1]! - a[1]!) * dy) / len2));
    best = Math.min(best, Math.hypot(a[0]! + t * dx - p[0]!, a[1]! + t * dy - p[1]!));
  }
  return best;
}

/** What every ring served must be. */
function expectRings(g: AreaGeometry) {
  for (const ring of ringsOf(g)) {
    expect(ring.length).toBeGreaterThanOrEqual(4);
    expect(ring[ring.length - 1]).toEqual(ring[0]);
    for (const p of ring) {
      expect(p).toHaveLength(2);
      expect(p).toEqual(round4(p));
    }
  }
}

describe("each distributor's file", () => {
  it('Ausgrid: one open ring of {lat, lng} becomes a closed Polygon in [lon, lat]', () => {
    const g = parseArea('ausgrid', AUSGRID_AREA);
    expect(g.type).toBe('Polygon');
    expectRings(g);
    expect(ringsOf(g)).toHaveLength(1);
    const [ring] = ringsOf(g);
    expect(ring![0]).toEqual([150.8515, -33.089]);
    // These two dozen points are kilometres apart: every one is a corner.
    expect(ring).toHaveLength(AUSGRID_AREA.length + 1);
  });

  it('CitiPower (and Powercor): a FeatureCollection of one Polygon, the points that add nothing gone', () => {
    const g = parseArea('citipower', CITIPOWER_AREA);
    expect(g.type).toBe('Polygon');
    expectRings(g);
    expect(ringsOf(g)).toHaveLength(1);
    const ring = ringsOf(g)[0]!;
    expect(ring[0]).toEqual([144.9758, -37.8694]);
    const before = CITIPOWER_AREA.features[0]!.geometry.coordinates[0]!;
    expect(ring.length).toBeLessThan(before.length);
    expect(ring.length).toBeGreaterThan(before.length / 2);
    for (const p of before) expect(distanceToRing(p, ring)).toBeLessThanOrEqual(AREA_TOLERANCE_DEG + 0.00008);
  });

  it('Jemena: the hole in the world is the area; a pocket too small to draw is left out', () => {
    const g = parseArea('jemena', JEMENA_AREA);
    expect(g.type).toBe('Polygon');
    expectRings(g);
    expect(ringsOf(g)).toHaveLength(1);
    const xs = ringsOf(g)[0]!.map((p) => p[0]!);
    // Melbourne's north-west, not the world.
    expect(Math.min(...xs)).toBeCloseTo(144.6778, 4);
    expect(Math.max(...xs)).toBeCloseTo(145.1111, 4);
    expect(ringArea(JEMENA_POCKET)).toBeLessThan(AREA_MIN_RING_DEG2);
  });

  it('Jemena: a pocket is a hole in the area it lies in, and survives when it is large enough', () => {
    const polygons = unmask(areaPolygons(JEMENA_AREA));
    expect(polygons).toEqual([[JEMENA_RING, JEMENA_POCKET]]);
    const g = simplifyArea(polygons, 0.0002, 0)!;
    expect(g.type).toBe('Polygon');
    expect(ringsOf(g)).toHaveLength(2);
    expectRings(g);
  });

  it('Jemena: a file with no world-wide mask in it is not read as an area', () => {
    const noMask = { type: 'FeatureCollection', features: [JEMENA_AREA.features[1]] };
    expect(() => parseArea('jemena', noMask)).toThrow(/jemena: network area file: no mask polygon/);
    // The mask alone has no hole: no area.
    expect(unmask([[JEMENA_WORLD]])).toEqual([]);
    expect(() => parseArea('jemena', { type: 'Polygon', coordinates: [JEMENA_WORLD] })).toThrow(/no area large enough/);
  });

  it('anything that is not an area throws, so the read fails and the copy held stays', () => {
    const page = '<!DOCTYPE html><title>Just a moment...</title>';
    const bad: unknown[] = [
      null, undefined, page, 42, [], {}, { error: 'nope' },
      { type: 'FeatureCollection', features: [] },
      { type: 'FeatureCollection' },
      { type: 'Point', coordinates: [151, -33] },
      // Two points are no ring.
      [{ lat: -33, lng: 151 }, { lat: -34, lng: 151 }],
      // A ring a metre across is no area.
      { type: 'Polygon', coordinates: [[[151, -33], [151.00001, -33], [151, -33.00001], [151, -33]]] },
    ];
    for (const payload of bad) expect(() => parseArea('ausgrid', payload), JSON.stringify(payload)).toThrow(/ausgrid: network area file/);
  });
});

describe('areaPolygons', () => {
  const SQUARE = [[150, -33], [151, -33], [151, -34], [150, -34], [150, -33]];
  const HOLE = [[150.4, -33.4], [150.6, -33.4], [150.6, -33.6], [150.4, -33.4]];

  it('reads a ring, a polygon, a list of polygons and every GeoJSON wrapping of them', () => {
    expect(areaPolygons(SQUARE)).toEqual([[SQUARE]]);
    expect(areaPolygons(SQUARE.map(([lng, lat]) => ({ lat, lng })))).toEqual([[SQUARE]]);
    expect(areaPolygons([SQUARE, HOLE])).toEqual([[SQUARE, HOLE]]);
    expect(areaPolygons([[SQUARE, HOLE], [SQUARE]])).toEqual([[SQUARE, HOLE], [SQUARE]]);
    expect(areaPolygons({ type: 'Polygon', coordinates: [SQUARE, HOLE] })).toEqual([[SQUARE, HOLE]]);
    expect(areaPolygons({ type: 'MultiPolygon', coordinates: [[SQUARE, HOLE], [SQUARE]] })).toEqual([[SQUARE, HOLE], [SQUARE]]);
    expect(areaPolygons({ type: 'Feature', geometry: { type: 'Polygon', coordinates: [SQUARE] } })).toEqual([[SQUARE]]);
    expect(
      areaPolygons({
        type: 'FeatureCollection',
        features: [
          { type: 'Feature', geometry: { type: 'GeometryCollection', geometries: [{ type: 'Point', coordinates: [150, -33] }, { type: 'Polygon', coordinates: [SQUARE] }] } },
          { type: 'Feature', geometry: null },
          { type: 'Feature', geometry: { type: 'MultiPolygon', coordinates: [[SQUARE, HOLE]] } },
        ],
      }),
    ).toEqual([[SQUARE], [SQUARE, HOLE]]);
  });

  it('a polygon without an outer ring is none, and a broken hole is dropped', () => {
    expect(areaPolygons({ type: 'Polygon', coordinates: [[[150, -33]], HOLE] })).toEqual([]);
    expect(areaPolygons({ type: 'Polygon', coordinates: [SQUARE, 'x', [[1, 2]]] })).toEqual([[SQUARE]]);
  });
});

describe('simplifying', () => {
  /** A circle of `n` points, not closed. */
  const circle = (n: number, radius: number, cx = 150, cy = -33) =>
    Array.from({ length: n }, (_, i) => [cx + radius * Math.cos((2 * Math.PI * i) / n), cy + radius * Math.sin((2 * Math.PI * i) / n)]);

  it('a known shape: far fewer points, closed, and every original point within the tolerance of the outline', () => {
    const original = circle(3600, 0.5);
    const ring = simplifyRing(original)!;
    expect(ring.length).toBeGreaterThanOrEqual(4);
    expect(ring.length).toBeLessThan(original.length / 20);
    expect(ring[ring.length - 1]).toEqual(ring[0]);
    for (const p of ring) expect(p).toEqual(round4(p));
    // The tolerance, and the most that rounding to four decimals can move a corner.
    const allowed = AREA_TOLERANCE_DEG + 0.00008;
    let worst = 0;
    for (const p of original) worst = Math.max(worst, distanceToRing(p, ring));
    expect(worst).toBeLessThanOrEqual(allowed);
    // Not simplified to nothing: the area is the circle's, to within a part in a hundred.
    expect(ringArea(ring)).toBeGreaterThan(Math.PI * 0.25 * 0.99);
  });

  it('a tighter tolerance keeps more points', () => {
    const original = circle(3600, 0.5);
    expect(simplifyRing(original, 0.0005)!.length).toBeGreaterThan(simplifyRing(original, 0.005)!.length);
  });

  it('points on a straight edge go; the corners stay, in order', () => {
    const edge = (a: number[], b: number[]) => Array.from({ length: 50 }, (_, i) => [a[0]! + ((b[0]! - a[0]!) * i) / 50, a[1]! + ((b[1]! - a[1]!) * i) / 50]);
    const corners = [[150, -33], [151, -33], [151, -34], [150, -34]];
    const square = corners.flatMap((c, i) => edge(c, corners[(i + 1) % 4]!));
    expect(simplifyRing(square)).toEqual([[150, -33], [151, -33], [151, -34], [150, -34], [150, -33]]);
    // The same ring given closed is the same answer.
    expect(simplifyRing([...square, square[0]!])).toEqual([[150, -33], [151, -33], [151, -34], [150, -34], [150, -33]]);
  });

  it('a ring of a hundred thousand jagged points does not exhaust the stack', () => {
    const jagged = circle(100_000, 1).map(([x, y], i) => [x! + (i % 2 ? 0.01 : 0), y!]);
    const ring = simplifyRing(jagged)!;
    expect(ring.length).toBeGreaterThan(1000);
    expect(ring[ring.length - 1]).toEqual(ring[0]);
  });

  it('a ring that collapses, or encloses next to nothing, is no ring', () => {
    expect(simplifyRing([])).toBeNull();
    expect(simplifyRing([[150, -33], [151, -33]])).toBeNull();
    // A sliver: its points are all within the tolerance of one line.
    expect(simplifyRing([[150, -33], [150.5, -33.0001], [151, -33], [150.5, -33.0002]])).toBeNull();
    // A real shape, but an islet: a circle 500 m in radius.
    expect(simplifyRing(circle(90, 0.005))).toBeNull();
    expect(simplifyRing(circle(90, 0.005), 0.0005, 0)).not.toBeNull();
    // Points that are not numbers are passed over.
    expect(simplifyRing([[150, -33], [Number.NaN, 1], [151, -33], [151, -34], [150, -34]])).toEqual([[150, -33], [151, -33], [151, -34], [150, -34], [150, -33]]);
  });

  it('holes are kept only if they survive, and a polygon goes with its outer ring', () => {
    const outer = circle(720, 0.5);
    const lake = circle(360, 0.1);
    const pond = circle(90, 0.004);
    const islet = circle(90, 0.004, 152, -33);
    const one = simplifyArea([[outer, lake, pond], [islet, lake]])!;
    expect(one.type).toBe('Polygon');
    expect(ringsOf(one)).toHaveLength(2);
    expectRings(one);

    const two = simplifyArea([[outer, pond], [circle(720, 0.3, 152, -33)]])!;
    expect(two.type).toBe('MultiPolygon');
    expect((two.coordinates as number[][][][]).map((p) => p.length)).toEqual([1, 1]);
    expectRings(two);

    expect(simplifyArea([[islet]])).toBeNull();
    expect(simplifyArea([])).toBeNull();
  });
});

describe('networkAreas', () => {
  const answer = (url: string): Promise<unknown> => {
    if (!(url in AREA_FILES)) return Promise.reject(new Error(`unexpected URL ${url}`));
    return Promise.resolve(AREA_FILES[url]);
  };
  const asked = () => fetchJson.mock.calls.map((c) => (c[0] as string).replace('https://', ''));
  const ON = ['ausgrid', 'citipower', 'powercor', 'jemena'];
  const READ_AT = new Date(NOW).toISOString();
  const AUSGRID_URL = 'www.ausgrid.com.au/api/outages-map/network-area';
  const POWERCOR_URL = 'www.powercor.com.au/assets/geo/powercor-network.json';

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
    for (const k of ENV) delete process.env[k];
    _resetNetworkAreas();
    fetchJson.mockReset();
    fetchJson.mockImplementation(answer);
  });
  afterEach(() => {
    vi.useRealTimers();
    for (const k of ENV) delete process.env[k];
    _resetNetworkAreas();
  });

  it('the first request reads each distributor that is on, once, and answers with their areas', async () => {
    const { areas, sources, complete } = await networkAreas();
    expect(asked()).toEqual([
      AUSGRID_URL,
      'www.powercor.com.au/assets/geo/citipower-network.json',
      POWERCOR_URL,
      'poweroutages.jemena.com.au/boundary.json',
    ]);
    expect(areas.map((a) => [a.distributor, a.name, a.state, a.geometry.type])).toEqual([
      ['ausgrid', 'Ausgrid', 'NSW', 'Polygon'],
      ['citipower', 'CitiPower', 'VIC', 'Polygon'],
      ['powercor', 'Powercor', 'VIC', 'Polygon'],
      ['jemena', 'Jemena', 'VIC', 'Polygon'],
    ]);
    for (const a of areas) expectRings(a.geometry);
    expect(sources).toEqual([
      { distributor: 'ausgrid', name: 'Ausgrid', state: 'NSW', updated_at: READ_AT },
      { distributor: 'citipower', name: 'CitiPower', state: 'VIC', updated_at: READ_AT },
      { distributor: 'powercor', name: 'Powercor', state: 'VIC', updated_at: READ_AT },
      { distributor: 'jemena', name: 'Jemena', state: 'VIC', updated_at: READ_AT },
    ]);
    expect(complete).toBe(true);
    // Ausgrid is asked the way its outages are; the others plainly.
    expect(fetchJson.mock.calls[0]![1].headers).toMatchObject({ Origin: 'https://www.ausgrid.com.au' });
    expect(fetchJson.mock.calls[1]![1].headers).toBeUndefined();
  });

  it('a distributor switched off with <NAME>_DISABLED=true is not asked, and is when switched on', async () => {
    process.env['JEMENA_DISABLED'] = 'true';
    const { areas, sources, complete } = await networkAreas();
    expect(asked().join(' ')).not.toMatch(/jemena/);
    expect(areas.map((a) => a.distributor)).toEqual(['ausgrid', 'citipower', 'powercor']);
    expect(sources.find((s) => s.distributor === 'jemena')!.updated_at).toBeNull();
    expect(complete).toBe(true);

    delete process.env['JEMENA_DISABLED'];
    fetchJson.mockClear();
    expect((await networkAreas()).areas.map((a) => a.distributor)).toEqual(ON);
    expect(asked()).toEqual(['poweroutages.jemena.com.au/boundary.json']);

    // Switched off again: not served from the copy read while it was on.
    process.env['JEMENA_DISABLED'] = 'true';
    const off = await networkAreas();
    expect(off.areas.map((a) => a.distributor)).toEqual(['ausgrid', 'citipower', 'powercor']);
    expect(off.sources.find((s) => s.distributor === 'jemena')!.updated_at).toBeNull();
    expect(off.complete).toBe(true);
  });

  it('nothing is read again for 24 hours; then each is, while the copy held is served', async () => {
    const first = await networkAreas();
    fetchJson.mockClear();
    vi.setSystemTime(NOW + AREA_REFRESH_MS - 1);
    expect((await networkAreas()).areas).toEqual(first.areas);
    expect(fetchJson).not.toHaveBeenCalled();

    vi.setSystemTime(NOW + AREA_REFRESH_MS);
    // The new read has not answered when this request is: nobody waits for it.
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    fetchJson.mockImplementation((url: string) => gate.then(() => answer(url)));
    const stale = await networkAreas();
    expect(fetchJson).toHaveBeenCalledTimes(4);
    expect(stale.sources.slice(0, 4).map((s) => s.updated_at)).toEqual([READ_AT, READ_AT, READ_AT, READ_AT]);
    expect(stale.areas).toEqual(first.areas);

    release();
    await _networkAreaReads();
    const fresh = await networkAreas();
    expect(fetchJson).toHaveBeenCalledTimes(4);
    expect(fresh.sources[0]!.updated_at).toBe(new Date(NOW + AREA_REFRESH_MS).toISOString());
  });

  it('a failed read keeps the copy held, and is tried again after an hour, not on every request', async () => {
    const first = await networkAreas();
    const later = NOW + AREA_REFRESH_MS;
    vi.setSystemTime(later);
    fetchJson.mockClear();
    fetchJson.mockImplementation((url: string) => (url.includes('ausgrid') ? Promise.reject(new Error('HTTP 503')) : answer(url)));
    await networkAreas();
    await _networkAreaReads();
    const kept = await networkAreas();
    expect(kept.areas).toEqual(first.areas);
    expect(kept.sources.map((s) => s.updated_at).slice(0, 2)).toEqual([READ_AT, new Date(later).toISOString()]);
    expect(kept.complete).toBe(true);
    expect(asked().filter((u) => u === AUSGRID_URL)).toHaveLength(1);

    vi.setSystemTime(later + AREA_RETRY_MS - 1);
    await networkAreas();
    expect(asked().filter((u) => u === AUSGRID_URL)).toHaveLength(1);

    // An hour on: asked again, and only Ausgrid.
    vi.setSystemTime(later + AREA_RETRY_MS);
    fetchJson.mockClear();
    fetchJson.mockImplementation(answer);
    await networkAreas();
    await _networkAreaReads();
    expect(asked()).toEqual([AUSGRID_URL]);
    expect((await networkAreas()).sources[0]!.updated_at).toBe(new Date(later + AREA_RETRY_MS).toISOString());
  });

  it('a wrong-shaped answer is a failed read: the copy held stays', async () => {
    const first = await networkAreas();
    const later = new Date(NOW + AREA_REFRESH_MS).toISOString();
    vi.setSystemTime(NOW + AREA_REFRESH_MS);
    fetchJson.mockImplementation((url: string) => {
      if (url.includes('jemena')) return Promise.resolve({ type: 'FeatureCollection', features: [] });
      if (url.includes('citipower')) return Promise.resolve({ message: 'moved' });
      return answer(url);
    });
    await networkAreas();
    await _networkAreaReads();
    const kept = await networkAreas();
    expect(kept.areas).toEqual(first.areas);
    expect(kept.sources.map((s) => s.updated_at).slice(0, 4)).toEqual([later, READ_AT, later, READ_AT]);
  });

  it('a distributor never read is absent, with a null updated_at, and is tried again after an hour', async () => {
    fetchJson.mockImplementation((url: string) => (url.includes('powercor-network') ? Promise.reject(new Error('timeout')) : answer(url)));
    const { areas, sources, complete } = await networkAreas();
    expect(areas.map((a) => a.distributor)).toEqual(['ausgrid', 'citipower', 'jemena']);
    expect(sources.find((s) => s.distributor === 'powercor')).toEqual({ distributor: 'powercor', name: 'Powercor', state: 'VIC', updated_at: null });
    expect(complete).toBe(false);

    fetchJson.mockClear();
    fetchJson.mockImplementation(answer);
    vi.setSystemTime(NOW + AREA_RETRY_MS - 1);
    expect((await networkAreas()).complete).toBe(false);
    expect(fetchJson).not.toHaveBeenCalled();

    // With nothing held the retry is waited for.
    vi.setSystemTime(NOW + AREA_RETRY_MS);
    const again = await networkAreas();
    expect(asked()).toEqual([POWERCOR_URL]);
    expect(again.areas.map((a) => a.distributor)).toEqual(ON);
    expect(again.complete).toBe(true);
  });

  it('requests that arrive while a read is under way share it', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    fetchJson.mockImplementation((url: string) => gate.then(() => answer(url)));
    const requests = [networkAreas(), networkAreas(), networkAreas()];
    await Promise.resolve();
    expect(fetchJson).toHaveBeenCalledTimes(4);
    release();
    const answers = await Promise.all(requests);
    expect(fetchJson).toHaveBeenCalledTimes(4);
    for (const a of answers) expect(a.areas.map((x) => x.distributor)).toEqual(ON);
    expect(answers[1]).toEqual(answers[0]);
  });
});
