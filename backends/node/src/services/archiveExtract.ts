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

/**
 * Extract a normalised "when did the source publish/update this row"
 * unix-seconds timestamp from a feature's properties. Walks a list of
 * known timestamp keys per source, accepts ISO strings / unix seconds /
 * unix millis. Returns null when the upstream payload has no usable
 * time field — readers fall back to fetched_at in that case.
 *
 * Why we do this at write time: the time field is per-source and
 * sometimes nested. Doing it once in the writer means SQL filters /
 * sorts can hit a top-level indexed column on the sidecar instead of
 * (data->>'k')::bigint casts.
 */
function asEpochSeconds(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) {
    // Heuristic: numbers > 10^12 are millis (e.g. Date.now()), seconds
    // since 1970 are 10-digit until ~Nov 2286. Convert millis -> sec.
    return v > 1e12 ? Math.floor(v / 1000) : Math.floor(v);
  }
  if (typeof v === 'string' && v.trim() !== '') {
    const s = v.trim();
    // Pure number string?
    const n = Number(s);
    if (Number.isFinite(n) && /^\d+(\.\d+)?$/.test(s)) {
      return n > 1e12 ? Math.floor(n / 1000) : Math.floor(n);
    }
    // Try Date.parse — handles ISO 8601 + RFC 2822 + most other forms.
    const ms = Date.parse(s);
    if (Number.isFinite(ms)) return Math.floor(ms / 1000);
  }
  return null;
}

export function extractSourceTimestampUnix(
  props: Record<string, unknown>,
  source?: string,
): number | null {
  // Per-source preferred field, then a fallback list of common keys.
  // RFS feeds carry `pubDate` (RFC 2822). Waze uses pubMillis (epoch
  // millis). BoM uses `effective`. Traffic uses `lastUpdated`. Power
  // outages use start/endDateTime — we prefer those over fetch time
  // because outage announcements list the announce time as the start.
  const candidates: string[] = [
    'source_timestamp_unix',
    'source_timestamp',
    'pubMillis',
    'pubDate',
    'pubdate',
    'lastUpdated',
    'last_updated',
    'updated',
    'updated_at',
    'updatedAt',
    'lastModified',
    'last_modified',
    'effective',
    'startTime',
    'start_time',
    'StartTime',
    'created',
    'created_at',
    'createdAt',
    'timestamp',
    'date',
    'datetime',
  ];
  // Source-specific overrides at the front of the candidate list.
  if (source) {
    if (source.startsWith('waze_')) candidates.unshift('pubMillis');
    if (source === 'rfs') candidates.unshift('pubDate');
    if (source.startsWith('bom_')) candidates.unshift('effective');
  }
  for (const k of candidates) {
    const t = asEpochSeconds(props[k]);
    if (t !== null) return t;
  }
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
    // Essential's flat-array path puts incidentId at the top level
    // (no `properties` wrapper), so per-feature dedup needs both
    // top-level lookups too.
    item['incident_id'],
    item['incidentId'],
  ];
  for (const c of candidates) {
    const s = asString(c);
    if (s) return s;
  }
  return null;
}

/**
 * Choose `category` and `subcategory` from a feature's properties or
 * a power outage object, with synonyms covering every upstream we
 * archive. Endeavour / Ausgrid / Essential outages carry an
 * `outageType` ("Unplanned"/"Planned"); RFS features carry an
 * `alertLevel`; BoM warnings carry `warningType`; etc.
 *
 * `source` is consulted so waze-specific fallbacks apply only to
 * waze rows: when a waze alert has a `type` but no `subtype`, the
 * subcategory defaults to the category. Mirrors what the legacy
 * frontend rendered when subtype was absent — pins fall back to the
 * category icon/colour rather than rendering as "Unknown".
 */
function extractCategoryFields(
  props: Record<string, unknown>,
  source?: string,
): {
  category: string | null;
  subcategory: string | null;
} {
  const cat = asString(
    props['category'] ??
      props['alertLevel'] ??
      props['outageType'] ??
      props['type'] ??
      props['warningType'] ??
      props['eventType'] ??
      props['eventCategory'],
  );
  let sub = asString(
    props['subcategory'] ??
      // Waze alerts carry `subtype` lowercase ("POLICE_VISIBLE",
      // "HAZARD_ON_ROAD_OBJECT", etc.). Live-polled rows missed it
      // because the previous code only looked at camelCase `subType`.
      props['subtype'] ??
      props['subType'] ??
      props['fireType'] ??
      props['cause'],
  );
  // For waze pins specifically: when the upstream alert has a type
  // but no subtype (e.g. a bare type='POLICE' with no further detail),
  // surface the category as the subcategory so the frontend's
  // pin-renderer doesn't show "Unknown" or fall through to a default
  // icon. User-requested behaviour.
  if (sub === null && cat !== null && source && source.startsWith('waze_')) {
    sub = cat;
  }
  return { category: cat, subcategory: sub };
}

/**
 * Apply title / location_text aliases to an object so the JSONB
 * projection at /api/data/history finds usable values regardless of
 * whether the upstream calls the field `title`, `name`, `headline`,
 * `streets`, `suburb`, etc. Used by both the GeoJSON-feature path
 * and the array-element path.
 *
 * Aliases (first match wins):
 *   title         ← name | headline | displayName | streets | suburb | description
 *   location_text ← location | streets | suburb | city | address | streetName
 */
function applyAliases(
  out: Record<string, unknown>,
  src: Record<string, unknown>,
): void {
  if (out['title'] === undefined) {
    const aliasTitle =
      asString(src['name']) ??
      asString(src['headline']) ??
      asString(src['displayName']) ??
      asString(src['streets']) ??
      asString(src['suburb']) ??
      asString(src['description']);
    if (aliasTitle) out['title'] = aliasTitle;
  }
  if (out['location_text'] === undefined) {
    const aliasLocation =
      asString(src['location']) ??
      asString(src['streets']) ??
      asString(src['suburb']) ??
      asString(src['city']) ??
      asString(src['address']) ??
      asString(src['streetName']);
    if (aliasLocation) out['location_text'] = aliasLocation;
  }
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
  // for most upstreams), then overlay normalised aliases.
  const out: Record<string, unknown> = { ...props };
  applyAliases(out, props);
  // Surface lat/lng inside data so callers that only read `data`
  // (not the row's columns) can still find geometry.
  if (out['lat'] === undefined && out['lng'] === undefined) {
    const ll = extractLatLng(item as Record<string, unknown>);
    if (ll.lat !== null) out['lat'] = ll.lat;
    if (ll.lng !== null) out['lng'] = ll.lng;
  }
  return out;
}

/**
 * Build a flat data blob from an array element (e.g. an Endeavour
 * outage object — no GeoJSON wrapping). Aliases are applied
 * directly against the object's own keys so power outages with no
 * `title` field still surface a readable label.
 */
function flatDataFromArrayItem(item: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...item };
  applyAliases(out, item);
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
      const { category, subcategory } = extractCategoryFields(props, source);
      out.push({
        source,
        source_id: extractSourceId(f as Record<string, unknown>),
        fetched_at,
        source_timestamp_unix: extractSourceTimestampUnix(props, source),
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
      const { category, subcategory } = extractCategoryFields(obj, source);
      out.push({
        source,
        source_id: extractSourceId(obj),
        fetched_at,
        source_timestamp_unix: extractSourceTimestampUnix(obj, source),
        lat,
        lng,
        category,
        subcategory,
        // Apply the same aliases the Feature path uses. Without this,
        // power outages (no `title`/`location_text` keys, just
        // `suburb`/`streets`/`outageType`) ended up with null
        // title/location/category in /api/data/history.
        data: flatDataFromArrayItem(obj),
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
