/**
 * GET /api/boundaries/:kind — administrative boundaries for a viewport.
 *
 *   kind = locality | lga | state
 *   ?bbox=west,south,east,north   required for locality and lga
 *   ?limit=                       cap, default 800
 *
 * The map used to load whole GeoJSON files; the national datasets are
 * far too big for that (15,785 localities alone), so it asks for what is
 * on screen instead. Rows carry a precomputed bounding box and the query
 * is a plain overlap test — this database has no PostGIS and doesn't
 * need it for a viewport.
 *
 * `state` ignores the bbox: 12,844 rows sounds like a lot but almost all
 * of them are tiny islands, and the mainland outlines are wanted at
 * every zoom, so it is served whole and cached.
 */
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';

export const boundariesRouter = new Hono();

const KINDS = new Set(['locality', 'lga', 'state']);
/** Kinds where asking for the whole country would be absurd. */
const NEEDS_BBOX = new Set(['locality', 'lga']);
const DEFAULT_LIMIT = 800;
const MAX_LIMIT = 3000;

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

function toFeature(r: BoundaryRow, kind: string) {
  return {
    type: 'Feature' as const,
    geometry: r.geom,
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
    return c.json({
      type: 'FeatureCollection',
      features: kept.map((r) => toFeature(r, kind)),
      count: kept.length,
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
