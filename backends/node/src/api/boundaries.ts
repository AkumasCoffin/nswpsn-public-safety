/**
 * GET /api/boundaries/:kind — administrative boundaries for a viewport.
 *
 *   kind = locality | lga | state
 *   ?bbox=west,south,east,north   required for locality and lga
 *   ?limit=                       cap, default 20,000
 *
 * The map used to load whole GeoJSON files; the national datasets are
 * far too big for that (15,785 localities alone), so it asks for what is
 * on screen instead. Rows carry a precomputed bounding box and the query
 * is a plain overlap test — this database has no PostGIS and doesn't
 * need it for a viewport.
 *
 * EVERYTHING IN VIEW, AT THE RESOLUTION THE VIEW CAN SHOW. Counting rows
 * is the wrong way to keep the response small: a state-wide view of
 * Victoria holds about 4,100 localities and capping it at 800 drew a
 * fifth of the suburbs and left the rest as holes in the mesh. What
 * actually costs bytes is vertices, and at that zoom the stored geometry
 * carries far more of them than the screen has pixels — the import
 * fetches at 55 m, and one pixel there is closer to 800 m.
 *
 * So the bbox decides WHICH shapes and the bbox's width decides HOW
 * FINELY they are drawn: coordinates are simplified to about half a
 * pixel of the requested view, and parts smaller than that are dropped
 * because they cannot render as more than a dot. Zooming in tightens the
 * tolerance and the detail comes back. The database keeps the precise
 * geometry — boundaryForPoint needs it, and a border that decides which
 * state an incident is in must not be the simplified one.
 */
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';

export const boundariesRouter = new Hono();

const KINDS = new Set(['locality', 'lga', 'state']);
/** Kinds where asking for the whole country would be absurd. */
const NEEDS_BBOX = new Set(['locality', 'lga']);
// Bytes are governed by the simplification below, not by the row count,
// so the cap is now only a runaway guard rather than a display decision.
const DEFAULT_LIMIT = 20000;
const MAX_LIMIT = 40000;

/**
 * How fine to draw, in degrees, for a view this wide.
 *
 * The divisor is a pixel budget: at 3,000 the tolerance is one pixel on
 * a 3,000px-wide map and about half a pixel on the ~1,600px one most
 * people have, which is below what any straight edge can show. Measured
 * over a Victoria-wide view of 4,102 suburbs it takes the response from
 * 1.9 MB gzipped to under 1 MB and the vertex count from 297k to 140k —
 * and the browser has to draw every one of those vertices, so that half
 * matters more than the bytes do.
 *
 * Returns 0 for a view tight enough that the stored geometry is already
 * coarser than the screen; then nothing is touched.
 */
export function toleranceFor(bbox: [number, number, number, number] | null): number {
  if (!bbox) return 0;
  const [w, , e] = bbox;
  const span = Math.abs(e - w);
  if (!Number.isFinite(span) || span <= 0) return 0;
  const tol = span / 3000;
  // The import fetched at maxAllowableOffset 0.0005, so asking for finer
  // than that would only cost CPU to change nothing.
  return tol <= 0.0005 ? 0 : tol;
}

/**
 * Ramer-Douglas-Peucker on one ring. Keeps the points that carry the
 * shape and drops the ones that sit within `tol` of the line between
 * their neighbours. Iterative rather than recursive: a coastline ring
 * can run to tens of thousands of points and blowing the stack on
 * Tasmania is a poor way to draw a border.
 */
export function simplifyRing(ring: number[][], tol: number): number[][] {
  const n = ring.length;
  if (n < 3 || tol <= 0) return ring;
  const keep = new Uint8Array(n);
  keep[0] = 1;
  keep[n - 1] = 1;
  const tol2 = tol * tol;
  const stack: Array<[number, number]> = [[0, n - 1]];
  while (stack.length) {
    const [first, last] = stack.pop()!;
    if (last <= first + 1) continue;
    const a = ring[first]!;
    const b = ring[last]!;
    const ax = a[0]!, ay = a[1]!, bx = b[0]!, by = b[1]!;
    const dx = bx - ax;
    const dy = by - ay;
    const len2 = dx * dx + dy * dy;
    let worst = -1;
    let worstD = tol2;
    for (let i = first + 1; i < last; i++) {
      const p = ring[i]!;
      const px = p[0]!, py = p[1]!;
      let d2: number;
      if (len2 === 0) {
        // Degenerate segment — the endpoints coincide, so measure to the
        // point itself rather than dividing by zero.
        const ex = px - ax;
        const ey = py - ay;
        d2 = ex * ex + ey * ey;
      } else {
        let t = ((px - ax) * dx + (py - ay) * dy) / len2;
        t = t < 0 ? 0 : t > 1 ? 1 : t;
        const ex = px - (ax + t * dx);
        const ey = py - (ay + t * dy);
        d2 = ex * ex + ey * ey;
      }
      if (d2 > worstD) {
        worstD = d2;
        worst = i;
      }
    }
    if (worst > 0) {
      keep[worst] = 1;
      stack.push([first, worst], [worst, last]);
    }
  }
  const out: number[][] = [];
  for (let i = 0; i < n; i++) if (keep[i]) out.push(ring[i]!);
  return out;
}

/** A ring needs three distinct corners plus the repeated closing point. */
function usableRing(ring: number[][]): boolean {
  return ring.length >= 4;
}

/** Whether a polygon is large enough to render as more than a dot. */
function polygonVisible(rings: unknown, tol: number): boolean {
  if (tol <= 0) return true;
  const outer = Array.isArray(rings) ? (rings[0] as number[][] | undefined) : undefined;
  if (!outer || !outer.length) return false;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const p of outer) {
    if (!Array.isArray(p) || p.length < 2) continue;
    const x = p[0]!, y = p[1]!;
    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }
  // One pixel across in either direction is the floor. Tighter than that
  // and real suburbs start vanishing from a wide view, which reads as a
  // hole in the mesh; looser and Tasmania's 5,887 islets — most of the
  // state layer's payload — are drawn as specks nobody can see.
  return maxX - minX >= tol || maxY - minY >= tol;
}

/**
 * Simplify a Polygon / MultiPolygon for display. Returns null when
 * nothing survives — the caller drops the feature rather than emitting
 * an empty geometry.
 */
export function simplifyGeometry(geom: unknown, tol: number): unknown {
  if (tol <= 0 || !geom || typeof geom !== 'object') return geom;
  const g = geom as { type?: string; coordinates?: unknown };
  const doPolygon = (rings: unknown): number[][][] | null => {
    if (!Array.isArray(rings) || !rings.length) return null;
    if (!polygonVisible(rings, tol)) return null;
    const out: number[][][] = [];
    for (const r of rings) {
      if (!Array.isArray(r)) continue;
      const simplified = simplifyRing(r as number[][], tol);
      // A hole that collapses is simply gone; an outer ring that
      // collapses takes the polygon with it.
      if (usableRing(simplified)) out.push(simplified);
      else if (out.length === 0) return null;
    }
    return out.length ? out : null;
  };
  if (g.type === 'Polygon') {
    const rings = doPolygon(g.coordinates);
    return rings ? { type: 'Polygon', coordinates: rings } : null;
  }
  if (g.type === 'MultiPolygon') {
    if (!Array.isArray(g.coordinates)) return null;
    const polys: number[][][][] = [];
    for (const p of g.coordinates) {
      const rings = doPolygon(p);
      if (rings) polys.push(rings);
    }
    if (!polys.length) return null;
    // One surviving part doesn't need the MultiPolygon wrapper.
    return polys.length === 1
      ? { type: 'Polygon', coordinates: polys[0] }
      : { type: 'MultiPolygon', coordinates: polys };
  }
  return geom;
}

interface BoundaryRow {
  ext_id: string;
  name: string;
  short_name: string | null;
  state: string | null;
  class: string | null;
  geom: unknown;
}

export function parseBbox(raw: string | undefined): [number, number, number, number] | null {
  if (!raw) return null;
  const parts = raw.split(',').map((v) => Number(v.trim()));
  if (parts.length !== 4 || parts.some((n) => !Number.isFinite(n))) return null;
  const [w, s, e, n] = parts as [number, number, number, number];
  // Tolerate a caller that passes the corners the other way round rather
  // than silently returning nothing.
  return [Math.min(w, e), Math.min(s, n), Math.max(w, e), Math.max(s, n)];
}

function toFeature(r: BoundaryRow, kind: string, geom: unknown) {
  return {
    type: 'Feature' as const,
    geometry: geom,
    properties: {
      id: r.ext_id,
      kind,
      name: r.name,
      shortName: r.short_name,
      state: r.state,
      class: r.class,
    },
  };
}

boundariesRouter.get('/api/boundaries/:kind', async (c) => {
  const kind = c.req.param('kind');
  if (!KINDS.has(kind)) {
    return c.json({ error: 'unknown_kind', kinds: [...KINDS] }, 400);
  }

  const bbox = parseBbox(c.req.query('bbox'));
  if (NEEDS_BBOX.has(kind) && !bbox) {
    return c.json({ error: 'bbox_required', kind }, 400);
  }

  const limitRaw = Number(c.req.query('limit'));
  const limit = Number.isFinite(limitRaw)
    ? Math.min(Math.max(1, Math.floor(limitRaw)), MAX_LIMIT)
    : DEFAULT_LIMIT;

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);

  try {
    // Ask for one more than the cap so "there were more" is knowable
    // without a second COUNT.
    const params: unknown[] = [kind, limit + 1];
    let where = 'kind = $1';
    if (bbox) {
      const [w, s, e, n] = bbox;
      // Standard box overlap: two boxes miss only if one is wholly left,
      // right, above or below the other.
      where += ' AND min_lon <= $3 AND max_lon >= $4 AND min_lat <= $5 AND max_lat >= $6';
      params.push(e, w, n, s);
    }
    const { rows } = await pool.query<BoundaryRow>(
      `SELECT ext_id, name, short_name, state, class, geom
         FROM boundaries
        WHERE ${where}
        ORDER BY (max_lat - min_lat) * (max_lon - min_lon) DESC
        LIMIT $2`,
      params,
    );

    const truncated = rows.length > limit;
    const kept = truncated ? rows.slice(0, limit) : rows;
    // Drawn at the view's own resolution — see the note at the top. A
    // shape that simplifies away entirely was too small to see.
    const tol = toleranceFor(bbox);
    const features = [];
    for (const r of kept) {
      const geom = simplifyGeometry(r.geom, tol);
      if (geom) features.push(toFeature(r, kind, geom));
    }
    return c.json({
      type: 'FeatureCollection',
      features,
      count: features.length,
      // Largest-first ordering means a truncated response still shows the
      // shapes that dominate the view, and the caller can say so.
      truncated,
    });
  } catch (err) {
    log.warn({ err: (err as Error).message, kind }, 'boundaries query failed');
    return c.json({ error: 'query_failed' }, 500);
  }
});

/**
 * Which boundary of `kind` contains this point, or null.
 *
 * This is the reason the data is in the database rather than in static
 * files: nothing upstream tells us which council or suburb an incident
 * is in, and the coordinate is the only reliable answer.
 *
 * The bbox index narrows to a handful of candidates, then the same
 * even-odd ray cast lib/stateMask.ts uses decides. Rings after the first
 * are holes, so a point inside one is outside the shape.
 */
export async function boundaryForPoint(
  kind: string,
  lon: number,
  lat: number,
): Promise<{ name: string; shortName: string | null; state: string | null } | null> {
  if (!KINDS.has(kind) || !Number.isFinite(lon) || !Number.isFinite(lat)) return null;
  const pool = await getPool();
  if (!pool) return null;

  const { rows } = await pool.query<BoundaryRow>(
    `SELECT ext_id, name, short_name, state, class, geom
       FROM boundaries
      WHERE kind = $1
        AND min_lon <= $2 AND max_lon >= $2
        AND min_lat <= $3 AND max_lat >= $3
      -- Smallest first: a suburb sits inside its LGA's box, and when
      -- boxes nest the tighter shape is the more specific answer.
      ORDER BY (max_lat - min_lat) * (max_lon - min_lon) ASC`,
    [kind, lon, lat],
  );

  for (const r of rows) {
    if (geometryContains(r.geom, lon, lat)) {
      return { name: r.name, shortName: r.short_name, state: r.state };
    }
  }
  return null;
}

function ringContains(ring: unknown, lon: number, lat: number): boolean {
  if (!Array.isArray(ring)) return false;
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const a = ring[i] as number[] | undefined;
    const b = ring[j] as number[] | undefined;
    if (!a || !b || a.length < 2 || b.length < 2) continue;
    const xi = a[0]!, yi = a[1]!, xj = b[0]!, yj = b[1]!;
    if ((yi > lat) !== (yj > lat) && lon < ((xj - xi) * (lat - yi)) / (yj - yi) + xi) {
      inside = !inside;
    }
  }
  return inside;
}

/** Polygon or MultiPolygon containment, holes respected. */
export function geometryContains(geom: unknown, lon: number, lat: number): boolean {
  if (!geom || typeof geom !== 'object') return false;
  const g = geom as { type?: string; coordinates?: unknown };
  const polygon = (rings: unknown): boolean => {
    if (!Array.isArray(rings) || rings.length === 0) return false;
    if (!ringContains(rings[0], lon, lat)) return false;
    // Any subsequent ring is a hole punched out of the outer one.
    for (let i = 1; i < rings.length; i++) {
      if (ringContains(rings[i], lon, lat)) return false;
    }
    return true;
  };
  if (g.type === 'Polygon') return polygon(g.coordinates);
  if (g.type === 'MultiPolygon') {
    if (!Array.isArray(g.coordinates)) return false;
    return g.coordinates.some((p) => polygon(p));
  }
  return false;
}
