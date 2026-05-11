/**
 * Live Traffic NSW — six sub-feeds: incidents, roadwork, flood, fire,
 * majorevent, cameras.
 *
 * All five hazard feeds share the same structure (a JSON list of
 * geo-keyed items) so they share parsing logic. Cameras have a
 * different upstream (`all-feeds-web.json`) and a different output
 * shape; they get their own fetcher.
 *
 * Output shape mirrors the Python routes at external_api_proxy.py:7239
 * onwards — each is a GeoJSON FeatureCollection.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const HAZARD_BASE = 'https://www.livetraffic.com/traffic/hazards';
const CAMERAS_URL = 'https://www.livetraffic.com/datajson/all-feeds-web.json';

export interface TrafficFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    id: string;
    type: string;
    incidentType: string;
    mainCategory: string;
    subCategory: string;
    incidentKind: string;
    title: string;
    headline: string;
    displayName: string;
    subtitle: string;
    otherAdvice: string;
    adviceB: string;
    roads: string;
    affectedDirection: string;
    impactedLanes: unknown[];
    speedLimit: string;
    expectedDelay: string;
    diversions: string;
    encodedPolyline: string;
    created: string;
    lastUpdated: string;
    start: string;
    end: string;
    isEnded: boolean;
    isMajor: boolean;
    arrangement: string;
    periods: unknown[];
    source: 'livetraffic';
  };
}

export interface TrafficSnapshot {
  type: 'FeatureCollection';
  features: TrafficFeature[];
  count: number;
}

export interface TrafficCameraFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    id: string;
    title: string;
    view: string;
    direction: string;
    region: string;
    imageUrl: string;
    path: string;
    source: 'livetraffic_cameras';
  };
}

export interface TrafficCamerasSnapshot {
  type: 'FeatureCollection';
  features: TrafficCameraFeature[];
  count: number;
}

const INCIDENT_TYPE_PREFIXES = [
  'SPECIAL EVENT CLEARWAYS',
  'MAJOR EVENT CLEARWAYS',
  'TRAFFIC LIGHTS BLACKED OUT',
  'CHANGED TRAFFIC CONDITIONS',
  'HOLIDAY TRAFFIC EXPECTED',
  'ADVERSE WEATHER',
  'BUILDING FIRE',
  'EARLIER FIRE',
  'GRASS FIRE',
  'BUSH FIRE',
  'CLEARWAYS',
  'BREAKDOWN',
  'FLOODING',
  'CRASH',
  'HAZARD',
  'LANDSLIDE',
  'SMOKE',
  'FIRE',
  'FLOOD',
  'ROADWORK',
  'ROAD CLOSURE',
  'SPECIAL EVENT',
  'MAJOR EVENT',
] as const;

export function extractIncidentType(title: string): { incidentType: string; cleanTitle: string } {
  if (!title) return { incidentType: '', cleanTitle: title || '' };
  const trimmed = title.trim();
  const upper = trimmed.toUpperCase();
  for (const prefix of INCIDENT_TYPE_PREFIXES) {
    if (!upper.startsWith(prefix)) continue;
    if (upper.length > prefix.length) {
      const next = upper[prefix.length];
      if (next && /[A-Z]/.test(next)) continue; // word-boundary check
    }
    let remaining = trimmed.slice(prefix.length).trim();
    remaining = remaining.replace(/^[\s\-:,]+/, '').trim();
    if (remaining && (remaining.length < 5 || /^[A-Z]{2,4}$/i.test(remaining))) {
      remaining = '';
    }
    return { incidentType: prefix, cleanTitle: remaining || trimmed };
  }
  return { incidentType: '', cleanTitle: trimmed };
}

function asNumber(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function asString(v: unknown): string {
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return '';
}

function asArray(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

function asBool(v: unknown): boolean {
  return v === true;
}

export function parseTrafficItem(item: unknown, hazardType: string): TrafficFeature | null {
  if (item === null || typeof item !== 'object') return null;
  const it = item as Record<string, unknown>;

  let lat: number | null = null;
  let lon: number | null = null;
  const geometry = it['geometry'] as Record<string, unknown> | undefined;
  if (geometry) {
    const coords = geometry['coordinates'];
    if (Array.isArray(coords) && coords.length >= 2) {
      lon = asNumber(coords[0]);
      lat = asNumber(coords[1]);
    }
  } else if ('latitude' in it && 'longitude' in it) {
    lat = asNumber(it['latitude']);
    lon = asNumber(it['longitude']);
  } else if ('lat' in it && 'lng' in it) {
    lat = asNumber(it['lat']);
    lon = asNumber(it['lng']);
  }
  if (lat === null || lon === null) return null;

  const rawProps = (it['properties'] as Record<string, unknown> | undefined) ?? {};
  const props: Record<string, unknown> = { ...rawProps };
  for (const [k, v] of Object.entries(it)) {
    if (k !== 'geometry' && k !== 'properties' && k !== 'type' && v) {
      props[k] = v;
    }
  }

  const roadsInfo = props['roads'];
  let roadsStr = '';
  let affectedDirection = '';
  if (Array.isArray(roadsInfo) && roadsInfo.length > 0) {
    const first = roadsInfo[0];
    if (first && typeof first === 'object') {
      const r = first as Record<string, unknown>;
      roadsStr = `${asString(r['mainStreet'])} ${asString(r['suburb'])}`.trim();
      affectedDirection = asString(r['affectedDirection']);
    }
  } else if (roadsInfo) {
    roadsStr = asString(roadsInfo);
  }

  const rawTitle =
    asString(props['headline']) ||
    asString(props['title']) ||
    asString(props['displayName']);
  const { incidentType, cleanTitle } = extractIncidentType(rawTitle);

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [lon, lat] },
    properties: {
      id: asString(it['id']),
      type: hazardType,
      incidentType,
      mainCategory: asString(props['mainCategory']),
      subCategory: asString(props['subCategory']),
      incidentKind: asString(props['incidentKind']),
      title: cleanTitle || rawTitle,
      headline: asString(props['headline']),
      displayName: asString(props['displayName']),
      subtitle: asString(props['subtitle']),
      otherAdvice:
        asString(props['otherAdvice']) || asString(props['adviceA']),
      adviceB: asString(props['adviceB']),
      roads: roadsStr,
      affectedDirection,
      impactedLanes: asArray(props['impactedLanes']),
      speedLimit: asString(props['speedLimit']),
      expectedDelay:
        asString(props['expectedDelay']) || asString(props['delay']),
      diversions:
        asString(props['diversions']) || asString(props['diversion']),
      encodedPolyline:
        asString(props['encodedPolyline']) ||
        asString(props['encodedPolylines']),
      created: asString(props['created']) || asString(props['start']),
      lastUpdated:
        asString(props['lastUpdated']) || asString(props['end']),
      start: asString(props['start']),
      end: asString(props['end']),
      isEnded:
        asBool(props['ended']) || asBool(props['isEnded']),
      isMajor: asBool(props['isMajor']),
      arrangement: asString(props['arrangement']),
      periods: asArray(props['periods']),
      source: 'livetraffic',
    },
  };
}

interface HazardKind {
  storeKey: string;
  /** Source value written to archive_traffic rows. Mirrors python's
   *  data_history source value (singular) — keep distinct from the
   *  LiveStore key (plural) so we don't break /api/traffic/incidents. */
  archiveSource: string;
  endpoint: string; // 'incident', 'roadwork', etc.
  label: string;    // 'Incident', 'Roadwork', etc.
  active: number;
  idle: number;
}

const HAZARD_KINDS: HazardKind[] = [
  { storeKey: 'traffic_incidents', archiveSource: 'traffic_incident', endpoint: 'incident', label: 'Incident', active: 60_000, idle: 120_000 },
  { storeKey: 'traffic_roadwork', archiveSource: 'traffic_roadwork', endpoint: 'roadwork', label: 'Roadwork', active: 300_000, idle: 600_000 },
  { storeKey: 'traffic_flood', archiveSource: 'traffic_flood', endpoint: 'flood', label: 'Flood', active: 300_000, idle: 600_000 },
  { storeKey: 'traffic_fire', archiveSource: 'traffic_fire', endpoint: 'fire', label: 'Fire', active: 300_000, idle: 600_000 },
  { storeKey: 'traffic_majorevent', archiveSource: 'traffic_majorevent', endpoint: 'majorevent', label: 'Major Event', active: 300_000, idle: 600_000 },
];

async function fetchHazard(kind: HazardKind): Promise<TrafficSnapshot> {
  const url = `${HAZARD_BASE}/${kind.endpoint}.json`;
  const data = await fetchJson<unknown>(url, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
  const items: unknown[] = Array.isArray(data)
    ? data
    : (() => {
        if (data && typeof data === 'object' && 'features' in data) {
          const f = (data as Record<string, unknown>)['features'];
          return Array.isArray(f) ? f : [];
        }
        return [];
      })();

  const features: TrafficFeature[] = [];
  for (const item of items) {
    if (!item || typeof item !== 'object') continue;
    const props = ((item as Record<string, unknown>)['properties'] ??
      item) as Record<string, unknown>;
    if (props['ended'] === true) continue;
    const f = parseTrafficItem(item, kind.label);
    if (f) features.push(f);
  }
  return {
    type: 'FeatureCollection',
    features,
    count: features.length,
  };
}

/** Cached fetcher pair so /api/.../raw can return upstream verbatim
 *  without us sourcing it again from LiveStore. The raw endpoint isn't
 *  polled — it's request-scoped because Python applies a TTL via its
 *  own HTTP cache. We treat each /raw call as a live passthrough for
 *  byte-for-byte parity. */
export async function fetchHazardRaw(endpoint: string): Promise<unknown> {
  const url = `${HAZARD_BASE}/${endpoint}.json`;
  return fetchJson<unknown>(url, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
}

export async function fetchTrafficCameras(): Promise<TrafficCamerasSnapshot> {
  const data = await fetchJson<unknown>(CAMERAS_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
  const items = Array.isArray(data) ? data : [];
  const features: TrafficCameraFeature[] = [];
  for (const item of items) {
    if (!item || typeof item !== 'object') continue;
    const it = item as Record<string, unknown>;
    const eventType = asString(it['eventType']).toLowerCase();
    const eventCategory = asString(it['eventCategory']).toLowerCase();
    const props = (it['properties'] as Record<string, unknown> | undefined) ?? {};
    const href = asString(props['href']);
    const looksLikeCam =
      eventType.includes('livecam') ||
      eventCategory.includes('livecam') ||
      href.endsWith('.jpeg') ||
      href.endsWith('.jpg');
    if (!looksLikeCam) continue;

    const geom = it['geometry'] as Record<string, unknown> | undefined;
    if (!geom || geom['type'] !== 'Point') continue;
    const coords = geom['coordinates'];
    if (!Array.isArray(coords) || coords.length < 2) continue;
    const lon = asNumber(coords[0]);
    const lat = asNumber(coords[1]);
    if (lon === null || lat === null) continue;

    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [lon, lat] },
      properties: {
        id: asString(it['id']),
        title: asString(props['title']) || 'Traffic Camera',
        view: asString(props['view']),
        direction: asString(props['direction']),
        region: asString(props['region']),
        imageUrl: asString(props['href']),
        path: asString(it['path']),
        source: 'livetraffic_cameras',
      },
    });
  }
  return {
    type: 'FeatureCollection',
    features,
    count: features.length,
  };
}

export default function register(): void {
  for (const k of HAZARD_KINDS) {
    registerSource<TrafficSnapshot>({
      name: k.storeKey,
      archiveSource: k.archiveSource,
      family: 'traffic',
      intervalActiveMs: k.active,
      intervalIdleMs: k.idle,
      fetch: () => fetchHazard(k),
    });
  }
  registerSource<TrafficCamerasSnapshot>({
    name: 'traffic_cameras',
    family: 'traffic',
    intervalActiveMs: 60_000,
    intervalIdleMs: 120_000,
    fetch: fetchTrafficCameras,
  });
}

export function trafficHazardSnapshot(name: string): TrafficSnapshot {
  return (
    liveStore.getData<TrafficSnapshot>(name) ?? {
      type: 'FeatureCollection',
      features: [],
      count: 0,
    }
  );
}

export function trafficCamerasSnapshot(): TrafficCamerasSnapshot {
  return (
    liveStore.getData<TrafficCamerasSnapshot>('traffic_cameras') ?? {
      type: 'FeatureCollection',
      features: [],
      count: 0,
    }
  );
}
