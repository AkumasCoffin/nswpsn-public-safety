/**
 * Default per-source archive-row extractor.
 *
 * Most sources return a GeoJSON FeatureCollection or a flat array.
 * Python's `data_history` table stored ONE ROW per incident — title /
 * severity / status / location_text projected into top-level columns,
 * the original feature stashed in `data`. The frontend logs page reads
 * those top-level columns to build its row labels.
 *
 * The Node port collapsed that — initially the poller stored ONE ROW
 * per poll (the whole FeatureCollection as a single `data` blob), so
 * /api/data/history rendered every snapshot as a single "Unknown"
 * incident. This module fixes that by walking the snapshot at write
 * time and emitting per-incident ArchiveRows whose `data` blob is the
 * feature's `properties` (so the JSONB-based projections in
 * dataHistoryQuery's SELECT still find title/severity/etc. at the top
 * level of `data`).
 */
import type { ArchiveRow } from '../store/archive.js';

interface FeatureLike {
  type?: string;
  geometry?: {
    type?: string;
    coordinates?: unknown;
  } | null;
  properties?: Record<string, unknown> | null;
  // Some non-GeoJSON sources put fields at the top level rather than
  // under properties; handle both shapes.
  [k: string]: unknown;
}

interface FeatureCollectionLike {
  type?: string;
  features?: FeatureLike[];
}

function asFiniteNumber(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function asString(v: unknown): string | null {
  if (typeof v === 'string' && v.trim() !== '') return v;
  return null;
}

function isFeatureCollection(v: unknown): v is FeatureCollectionLike {
  return (
    !!v &&
    typeof v === 'object' &&
    Array.isArray((v as FeatureCollectionLike).features)
  );
}

function isFeature(v: unknown): v is FeatureLike {
  if (!v || typeof v !== 'object') return false;
  const f = v as FeatureLike;
  return f.type === 'Feature' || (typeof f === 'object' && 'properties' in f);
}

/**
 * Extract lat/lng from a GeoJSON-style feature. Handles `geometry.coordinates`
 * `[lng, lat]` pairs (the standard form), with a few common variants
 * upstream sources sometimes emit (lat at the top level, separate lat/lng
 * fields, etc.).
 */
function extractLatLng(item: Record<string, unknown>): {
  lat: number | null;
  lng: number | null;
} {
  // GeoJSON-standard geometry.coordinates
  const geom = item['geometry'] as Record<string, unknown> | undefined;
  if (geom && Array.isArray(geom['coordinates'])) {
    const [c0, c1] = geom['coordinates'] as unknown[];
    const lng = asFiniteNumber(c0);
    const lat = asFiniteNumber(c1);
    if (lat !== null && lng !== null) return { lat, lng };
  }

  // Top-level lat/lng (Python upstreams sometimes emit this).
  const props = (item['properties'] as Record<string, unknown> | undefined) ?? item;
  const latRaw =
    props['lat'] ??
    props['latitude'] ??
    item['lat'] ??
    item['latitude'];
  const lngRaw =
    props['lng'] ??
    props['lon'] ??
    props['longitude'] ??
    item['lng'] ??
    item['lon'] ??
    item['longitude'];
  return {
    lat: asFiniteNumber(latRaw),
    lng: asFiniteNumber(lngRaw),
  };
}

/**
 * Stable id for a feature — first one of: properties.id, properties.guid,
 * properties.uuid, top-level id. Returns null when none present (the
 * archive row's source_id is nullable; a null id just means we can't
 * dedup this row across polls).
 */
function extractSourceId(item: Record<string, unknown>): string | null {
  const props = (item['properties'] as Record<string, unknown> | undefined) ?? {};
  const candidates = [
    props['id'],
    props['guid'],
    props['uuid'],
    props['incident_id'],
    props['incidentId'],
    item['id'],
    item['uuid'],
  ];
  for (const c of candidates) {
    const s = asString(c);
    if (s) return s;
  }
  return null;
}

/**
 * Choose `category` and `subcategory` from a feature's properties, with
 * a few common upstream synonyms baked in.
 */
function extractCategoryFields(props: Record<string, unknown>): {
  category: string | null;
  subcategory: string | null;
} {
  const cat = asString(
    props['category'] ?? props['alertLevel'] ?? props['type'] ?? props['warningType'],
  );
  const sub = asString(
    props['subcategory'] ?? props['subType'] ?? props['fireType'],
  );
  return { category: cat, subcategory: sub };
}

/**
 * Build the per-feature `data` blob the JSONB projection in
 * dataHistoryQuery expects. The blob carries a flat shape so
 * `data->>'title'` etc. resolve directly — same projection python's
 * data_history columns gave us, just under a JSONB roof.
 */
function flatDataFromFeature(item: FeatureLike): Record<string, unknown> {
  const props = (item.properties ?? {}) as Record<string, unknown>;
  // Start with the properties (they already carry title/severity/etc.
  // for most upstreams), then overlay a few normalised aliases so
  // dataHistoryQuery's `data->>'title'` finds something even when an
  // upstream uses a synonym (`name`, `headline`, etc.).
  const out: Record<string, unknown> = { ...props };
  if (out['title'] === undefined) {
    const aliasTitle =
      asString(props['name']) ??
      asString(props['headline']) ??
      asString(props['displayName']);
    if (aliasTitle) out['title'] = aliasTitle;
  }
  if (out['location_text'] === undefined) {
    const aliasLocation =
      asString(props['location']) ??
      asString(props['streets']) ??
      asString(props['suburb']) ??
      asString(props['city']);
    if (aliasLocation) out['location_text'] = aliasLocation;
  }
  // Also surface lat/lng inside data so callers that only read `data`
  // (not the row's columns) can still find geometry.
  if (out['lat'] === undefined && out['lng'] === undefined) {
    const ll = extractLatLng(item as Record<string, unknown>);
    if (ll.lat !== null) out['lat'] = ll.lat;
    if (ll.lng !== null) out['lng'] = ll.lng;
  }
  return out;
}

/**
 * Default fan-out: turn a poller snapshot into per-incident ArchiveRows.
 *
 * Recognises:
 *   - GeoJSON FeatureCollection → one row per `features[i]`
 *   - Flat array of plain objects → one row per element
 *   - Anything else (object, scalar) → one row carrying the whole snapshot
 */
export function defaultArchiveItems(
  source: string,
  snapshot: unknown,
  fetched_at: number,
): ArchiveRow[] {
  if (snapshot === null || snapshot === undefined) return [];

  if (isFeatureCollection(snapshot)) {
    const out: ArchiveRow[] = [];
    for (const f of snapshot.features ?? []) {
      if (!isFeature(f)) continue;
      const props = (f.properties ?? {}) as Record<string, unknown>;
      const { lat, lng } = extractLatLng(f as Record<string, unknown>);
      const { category, subcategory } = extractCategoryFields(props);
      out.push({
        source,
        source_id: extractSourceId(f as Record<string, unknown>),
        fetched_at,
        lat,
        lng,
        category,
        subcategory,
        data: flatDataFromFeature(f),
      });
    }
    return out;
  }

  if (Array.isArray(snapshot)) {
    const out: ArchiveRow[] = [];
    for (const item of snapshot) {
      if (item === null || typeof item !== 'object') continue;
      const obj = item as Record<string, unknown>;
      const { lat, lng } = extractLatLng(obj);
      const { category, subcategory } = extractCategoryFields(obj);
      out.push({
        source,
        source_id: extractSourceId(obj),
        fetched_at,
        lat,
        lng,
        category,
        subcategory,
        data: obj,
      });
    }
    return out;
  }

  // Scalar / object snapshot: archive as one row.
  return [
    {
      source,
      fetched_at,
      data: snapshot,
    },
  ];
}
