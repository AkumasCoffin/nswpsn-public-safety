/**
 * Preset filter validator. Mirrors python's `_dash_validate_filters` /
 * `_dash_validate_geofilter` (external_api_proxy.py:17336-17521).
 *
 * Returns a normalised `filters` dict (possibly empty). Throws
 * `FilterValidationError` on any structural / value issue with a short
 * message that's safe to surface in the 400 body.
 *
 * The filters blob is stored as jsonb on alert_presets. The bot reads
 * it back and applies the rules at evaluation time, so wire-format
 * compatibility with python is load-bearing — anything that the python
 * validator accepted must still round-trip through this one and come
 * out structurally identical.
 */

export class FilterValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FilterValidationError';
  }
}

// Per-alert-type severity scales. Mirrors python lines 17302-17307.
const SEVERITY_SCALES: Record<string, ReadonlySet<string>> = {
  rfs: new Set(['advice', 'watch_and_act', 'emergency']),
  bom_land: new Set(['minor', 'moderate', 'major']),
  bom_marine: new Set(['minor', 'moderate', 'major']),
  traffic_majorevent: new Set(['minor', 'moderate', 'major']),
};

// Flat union of every severity token across scales — used to validate the
// legacy single-string severity_min form (no per-type context).
const ALL_SEVERITIES: ReadonlySet<string> = new Set(
  Object.values(SEVERITY_SCALES).flatMap((s) => Array.from(s)),
);

// Canonical alert_types that carry a meaningful sub-type field. Mirrors
// python lines 17315-17322.
const SUBTYPE_AWARE_TYPES: ReadonlySet<string> = new Set([
  'rfs',
  'bom_land', 'bom_marine',
  'traffic_incident', 'traffic_roadwork', 'traffic_flood',
  'traffic_fire', 'traffic_majorevent',
  'waze_hazard', 'waze_jam', 'waze_police', 'waze_roadwork',
  'user_incident',
]);

const KNOWN_KEYS: ReadonlySet<string> = new Set([
  'keywords_include', 'keywords_exclude',
  'severity_min', 'subtype_filters',
  'geofilter', 'bbox',
]);

const GEOFILTER_TYPES: ReadonlySet<string> = new Set(['bbox', 'ring', 'polygon']);
const RING_RADIUS_MIN_M = 1;
const RING_RADIUS_MAX_M = 500_000; // 500 km
const POLYGON_MIN_POINTS = 3;
const POLYGON_MAX_POINTS = 100;

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

/**
 * Validate a geofilter dict. Discriminated union on `type`. Returns the
 * normalised dict. Mirrors python's `_dash_validate_geofilter`.
 */
export function validateGeofilter(g: unknown): Record<string, unknown> {
  if (!isPlainObject(g)) {
    throw new FilterValidationError('geofilter must be an object.');
  }
  const t = g['type'];
  if (typeof t !== 'string' || !GEOFILTER_TYPES.has(t)) {
    throw new FilterValidationError(
      `geofilter.type must be one of ${Array.from(GEOFILTER_TYPES).sort().join(', ')}.`,
    );
  }

  if (t === 'bbox') {
    const latMin = toFloat(g['lat_min']);
    const latMax = toFloat(g['lat_max']);
    const lngMin = toFloat(g['lng_min']);
    const lngMax = toFloat(g['lng_max']);
    if (latMin === null || latMax === null || lngMin === null || lngMax === null) {
      throw new FilterValidationError(
        'bbox geofilter needs numeric lat_min/lat_max/lng_min/lng_max.',
      );
    }
    if (!(latMin >= -90 && latMin <= 90 && latMax >= -90 && latMax <= 90)) {
      throw new FilterValidationError('bbox lat must be in [-90, 90].');
    }
    if (!(lngMin >= -180 && lngMin <= 180 && lngMax >= -180 && lngMax <= 180)) {
      throw new FilterValidationError('bbox lng must be in [-180, 180].');
    }
    if (latMin > latMax) {
      throw new FilterValidationError('bbox lat_min must be <= lat_max.');
    }
    if (lngMin > lngMax) {
      throw new FilterValidationError('bbox lng_min must be <= lng_max.');
    }
    return {
      type: 'bbox',
      lat_min: latMin, lat_max: latMax,
      lng_min: lngMin, lng_max: lngMax,
    };
  }

  if (t === 'ring') {
    const lat = toFloat(g['lat']);
    const lng = toFloat(g['lng']);
    const radiusM = toFloat(g['radius_m']);
    if (lat === null || lng === null || radiusM === null) {
      throw new FilterValidationError('ring geofilter needs numeric lat/lng/radius_m.');
    }
    if (!(lat >= -90 && lat <= 90)) {
      throw new FilterValidationError('ring lat must be in [-90, 90].');
    }
    if (!(lng >= -180 && lng <= 180)) {
      throw new FilterValidationError('ring lng must be in [-180, 180].');
    }
    if (!(radiusM >= RING_RADIUS_MIN_M && radiusM <= RING_RADIUS_MAX_M)) {
      throw new FilterValidationError(
        `ring radius_m must be in [${RING_RADIUS_MIN_M}, ${RING_RADIUS_MAX_M}].`,
      );
    }
    return { type: 'ring', lat, lng, radius_m: radiusM };
  }

  // polygon
  const pts = g['points'];
  if (!Array.isArray(pts)) {
    throw new FilterValidationError('polygon.points must be a list.');
  }
  const n = pts.length;
  if (n < POLYGON_MIN_POINTS) {
    throw new FilterValidationError(`polygon needs at least ${POLYGON_MIN_POINTS} points.`);
  }
  if (n > POLYGON_MAX_POINTS) {
    throw new FilterValidationError(`polygon: max ${POLYGON_MAX_POINTS} points.`);
  }
  const normPts: Array<[number, number]> = [];
  for (const p of pts) {
    if (!Array.isArray(p) || p.length !== 2) {
      throw new FilterValidationError('polygon points must be [lat, lng] pairs.');
    }
    const plat = toFloat(p[0]);
    const plng = toFloat(p[1]);
    if (plat === null || plng === null) {
      throw new FilterValidationError('polygon points must be numeric.');
    }
    if (!(plat >= -90 && plat <= 90 && plng >= -180 && plng <= 180)) {
      throw new FilterValidationError('polygon point out of lat/lng bounds.');
    }
    normPts.push([plat, plng]);
  }
  return { type: 'polygon', points: normPts };
}

/**
 * Validate + normalise a preset.filters dict. Returns a possibly-empty
 * normalised dict. Throws FilterValidationError on any issue.
 *
 * Accepts:
 *   - keywords_include / keywords_exclude: string[] (max 20, each <=100 chars)
 *   - severity_min: legacy string OR per-alert-type dict
 *   - subtype_filters: { [alert_type]: string[] } restricted to subtype-aware types
 *   - geofilter: discriminated union on `type` (bbox/ring/polygon)
 *   - bbox: legacy alias auto-converted to geofilter type=bbox
 */
export function validateFilters(v: unknown): Record<string, unknown> {
  if (v === null || v === undefined) return {};
  if (!isPlainObject(v)) {
    throw new FilterValidationError('filters must be an object.');
  }
  const unknown: string[] = [];
  for (const k of Object.keys(v)) {
    if (!KNOWN_KEYS.has(k)) unknown.push(k);
  }
  if (unknown.length > 0) {
    throw new FilterValidationError(`unknown filter keys: ${JSON.stringify(unknown.sort())}`);
  }

  const out: Record<string, unknown> = {};

  for (const key of ['keywords_include', 'keywords_exclude'] as const) {
    const raw = v[key];
    if (raw === null || raw === undefined) continue;
    if (!Array.isArray(raw)) {
      throw new FilterValidationError(`${key} must be a list.`);
    }
    if (raw.length > 20) {
      throw new FilterValidationError(`${key}: max 20 entries.`);
    }
    const kws: string[] = [];
    for (const k of raw) {
      if (typeof k !== 'string') {
        throw new FilterValidationError(`${key}: each entry must be a string.`);
      }
      const s = k.trim();
      if (!s) continue;
      if (s.length > 100) {
        throw new FilterValidationError(`${key}: each entry must be <= 100 chars.`);
      }
      kws.push(s);
    }
    if (kws.length > 0) out[key] = kws;
  }

  if ('severity_min' in v && v['severity_min'] !== null && v['severity_min'] !== undefined) {
    const sev = v['severity_min'];
    if (typeof sev === 'string') {
      // Legacy single-token form. Stored as-is; the bot fans it out per
      // alert_type at evaluation time.
      const s = sev.trim().toLowerCase();
      if (s && !ALL_SEVERITIES.has(s)) {
        throw new FilterValidationError(
          `severity_min must be one of ${Array.from(ALL_SEVERITIES).sort().join(', ')}.`,
        );
      }
      if (s) out['severity_min'] = s;
    } else if (isPlainObject(sev)) {
      const unknownSev: string[] = [];
      for (const k of Object.keys(sev)) {
        if (!(k in SEVERITY_SCALES)) unknownSev.push(k);
      }
      if (unknownSev.length > 0) {
        throw new FilterValidationError(
          `severity_min: unknown alert types ${JSON.stringify(unknownSev.sort())}.`,
        );
      }
      const normalised: Record<string, string> = {};
      for (const [at, val] of Object.entries(sev)) {
        if (val === null || val === '') continue;
        if (typeof val !== 'string') {
          throw new FilterValidationError(`severity_min[${at}] must be a string.`);
        }
        const vv = val.trim().toLowerCase();
        const scale = SEVERITY_SCALES[at];
        if (!scale || !scale.has(vv)) {
          throw new FilterValidationError(
            `severity_min[${at}] must be one of ${
              scale ? Array.from(scale).sort().join(', ') : '<unknown>'
            }.`,
          );
        }
        normalised[at] = vv;
      }
      if (Object.keys(normalised).length > 0) out['severity_min'] = normalised;
    } else {
      throw new FilterValidationError('severity_min must be a string or object.');
    }
  }

  if (
    'subtype_filters' in v &&
    v['subtype_filters'] !== null &&
    v['subtype_filters'] !== undefined
  ) {
    const sf = v['subtype_filters'];
    if (!isPlainObject(sf)) {
      throw new FilterValidationError('subtype_filters must be an object.');
    }
    const unknownSf: string[] = [];
    for (const k of Object.keys(sf)) {
      if (!SUBTYPE_AWARE_TYPES.has(k)) unknownSf.push(k);
    }
    if (unknownSf.length > 0) {
      throw new FilterValidationError(
        `subtype_filters: unsupported alert types ${JSON.stringify(unknownSf.sort())}.`,
      );
    }
    const normalised: Record<string, string[]> = {};
    for (const [at, raw] of Object.entries(sf)) {
      if (raw === null || raw === undefined) continue;
      if (!Array.isArray(raw)) {
        throw new FilterValidationError(`subtype_filters[${at}] must be a list.`);
      }
      if (raw.length > 50) {
        throw new FilterValidationError(`subtype_filters[${at}]: max 50 entries.`);
      }
      const items: string[] = [];
      for (const s of raw) {
        if (typeof s !== 'string') {
          throw new FilterValidationError(
            `subtype_filters[${at}]: each entry must be a string.`,
          );
        }
        const ss = s.trim();
        if (!ss) continue;
        if (ss.length > 100) {
          throw new FilterValidationError(
            `subtype_filters[${at}]: each entry must be <= 100 chars.`,
          );
        }
        items.push(ss);
      }
      if (items.length > 0) {
        // Dedup while preserving insertion order.
        const seen = new Set<string>();
        const deduped: string[] = [];
        for (const it of items) {
          if (!seen.has(it)) {
            seen.add(it);
            deduped.push(it);
          }
        }
        normalised[at] = deduped;
      }
    }
    if (Object.keys(normalised).length > 0) out['subtype_filters'] = normalised;
  }

  if ('geofilter' in v && v['geofilter'] !== null && v['geofilter'] !== undefined) {
    out['geofilter'] = validateGeofilter(v['geofilter']);
  }

  // Legacy alias: a top-level `bbox` is auto-converted to geofilter type=bbox.
  // Older presets persist with this shape; the bot still reads it. Both can't
  // be set simultaneously.
  if ('bbox' in v && v['bbox'] !== null && v['bbox'] !== undefined) {
    if ('geofilter' in out) {
      throw new FilterValidationError('use only one of bbox / geofilter.');
    }
    const legacy = v['bbox'];
    if (!isPlainObject(legacy)) {
      throw new FilterValidationError('bbox must be an object.');
    }
    out['geofilter'] = validateGeofilter({ type: 'bbox', ...legacy });
  }

  return out;
}

/**
 * Coerce a value to a finite number, returning null if it's not coercible.
 * Mirrors python's `float()` cast semantics — ints, floats, numeric strings.
 * Booleans are explicitly rejected so `true` doesn't become `1`.
 */
function toFloat(v: unknown): number | null {
  if (typeof v === 'boolean') return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'string') {
    const s = v.trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}
