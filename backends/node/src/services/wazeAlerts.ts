/**
 * Convert raw Waze ingest alerts/jams into GeoJSON features.
 *
 * Mirrors python parse_waze_alert() (line 9363) and parse_waze_jam()
 * (line 9421). Property names + shapes match exactly so the frontend
 * doesn't have to change.
 */
import type {
  WazeAlert,
  WazeJam,
  WazeGeoFeature,
} from '../types/waze.js';

interface ParsedAlert extends WazeGeoFeature {
  properties: {
    id: string;
    type: string;
    wazeType: string;
    wazeSubtype: string;
    title: string;
    displayType: string;
    street: string;
    city: string;
    location: string;
    thumbsUp: number;
    reliability: number;
    created: string;
    reportBy: string;
    source: 'waze';
  };
}

/** Title-case e.g. "POLICE_VISIBLE" -> "Police Visible". */
function titleCase(s: string): string {
  return s
    .replace(/_/g, ' ')
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function isoFromMillis(ms: unknown): string {
  if (typeof ms !== 'number' || !Number.isFinite(ms) || ms <= 0) return '';
  try {
    return new Date(ms).toISOString().replace(/\.\d{3}Z$/, 'Z');
  } catch {
    return '';
  }
}

/** Returns null if the alert lacks usable coordinates. */
export function parseWazeAlert(
  alert: WazeAlert,
  category: string,
): ParsedAlert | null {
  const loc = alert.location ?? {};
  const lat =
    typeof loc.y === 'number'
      ? loc.y
      : typeof loc.latitude === 'number'
        ? loc.latitude
        : typeof alert.lat === 'number'
          ? alert.lat
          : null;
  const lon =
    typeof loc.x === 'number'
      ? loc.x
      : typeof loc.longitude === 'number'
        ? loc.longitude
        : typeof alert.lon === 'number'
          ? alert.lon
          : null;
  if (lat === null || lon === null) return null;

  // Loose typing because the userscript ships whatever Waze put in the
  // georss feed. Cast to record-of-unknown for safe property access.
  const a = alert as Record<string, unknown>;
  const alertType = String(a['type'] ?? '');
  const subtype = String(a['subtype'] ?? '');
  const street = String(a['street'] ?? '');
  const city = String(a['city'] ?? '');
  const description = String(a['reportDescription'] ?? '');
  const thumbsUp = typeof a['nThumbsUp'] === 'number' ? a['nThumbsUp'] : 0;
  const reliability =
    typeof a['reliability'] === 'number' ? a['reliability'] : 0;
  const created = isoFromMillis(a['pubMillis']);

  const displayType = subtype
    ? titleCase(subtype)
    : alertType
      ? titleCase(alertType)
      : '';

  const locationStr = [street, city].filter(Boolean).join(', ');

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [lon, lat] },
    properties: {
      id: String(alert.uuid ?? alert.id ?? ''),
      type: category,
      wazeType: alertType,
      wazeSubtype: subtype,
      title: description || displayType,
      displayType,
      street,
      city,
      location: locationStr,
      thumbsUp,
      reliability,
      created,
      reportBy: String(a['reportBy'] ?? ''),
      source: 'waze',
    },
  };
}

const SEVERITY_MAP: Record<number, string> = {
  0: 'Free Flow',
  1: 'Light Traffic',
  2: 'Moderate Traffic',
  3: 'Heavy Traffic',
  4: 'Standstill',
  5: 'Blocked',
};

/** GeoJSON LineString feature for a jam. Returns null if no usable line. */
export function parseWazeJam(jam: WazeJam): {
  type: 'Feature';
  geometry: { type: 'LineString'; coordinates: [number, number][] };
  properties: Record<string, unknown>;
} | null {
  const j = jam as Record<string, unknown>;
  const line = j['line'];
  if (!Array.isArray(line) || line.length < 2) return null;
  const coords: [number, number][] = [];
  for (const pt of line) {
    if (
      pt &&
      typeof pt === 'object' &&
      typeof (pt as Record<string, unknown>)['x'] === 'number' &&
      typeof (pt as Record<string, unknown>)['y'] === 'number'
    ) {
      const p = pt as Record<string, number>;
      const x = p['x'];
      const y = p['y'];
      if (typeof x === 'number' && typeof y === 'number') {
        coords.push([x, y]);
      }
    }
  }
  if (coords.length < 2) return null;

  const street = String(j['street'] ?? '');
  const city = String(j['city'] ?? '');
  const speedKmh =
    typeof j['speedKMH'] === 'number'
      ? j['speedKMH']
      : typeof j['speed'] === 'number'
        ? j['speed']
        : 0;
  const length = typeof j['length'] === 'number' ? j['length'] : 0;
  const delay = typeof j['delay'] === 'number' ? j['delay'] : 0;
  const level = typeof j['level'] === 'number' ? j['level'] : 0;
  const created = isoFromMillis(j['pubMillis']);
  const severity = SEVERITY_MAP[level] ?? `Level ${level}`;
  const locationStr = [street, city].filter(Boolean).join(', ');
  const delayMins = delay ? Math.round(delay / 60) : 0;

  return {
    type: 'Feature',
    geometry: { type: 'LineString', coordinates: coords },
    properties: {
      id: String(jam.uuid ?? jam.id ?? ''),
      type: 'Jam',
      street,
      city,
      location: locationStr,
      speed: speedKmh,
      speedKMH: speedKmh,
      length,
      delay,
      delayMins,
      level,
      severity,
      created,
      source: 'waze',
    },
  };
}

/** Categorisation logic — same as Python's waze_police / waze_hazards. */
export function isPoliceAlert(a: WazeAlert): boolean {
  const t = String((a as Record<string, unknown>)['type'] ?? '').toUpperCase();
  const s = String(
    (a as Record<string, unknown>)['subtype'] ?? '',
  ).toUpperCase();
  return t === 'POLICE' || s.includes('POLICE');
}

export function isRoadworkAlert(a: WazeAlert): boolean {
  const t = String((a as Record<string, unknown>)['type'] ?? '').toUpperCase();
  const s = String(
    (a as Record<string, unknown>)['subtype'] ?? '',
  ).toUpperCase();
  return t === 'CONSTRUCTION' || s.includes('CONSTRUCTION');
}

export function isHazardAlert(a: WazeAlert): boolean {
  if (isPoliceAlert(a) || isRoadworkAlert(a)) return false;
  const t = String((a as Record<string, unknown>)['type'] ?? '').toUpperCase();
  // Same set Python uses: HAZARD / ACCIDENT / JAM / ROAD_CLOSED.
  return (
    t === 'HAZARD' || t === 'ACCIDENT' || t === 'JAM' || t === 'ROAD_CLOSED'
  );
}
