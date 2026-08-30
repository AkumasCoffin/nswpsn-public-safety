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
import { createHash } from 'node:crypto';
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const HAZARD_BASE = 'https://www.livetraffic.com/traffic/hazards';
// all-feeds-web.json is the site's own aggregate layer. We read it twice:
// once for the live cameras, once for the works/hazard records that appear
// nowhere else — including the ACT works, Canberra light-rail closures and
// council roadworks that the five hazard feeds have no coverage of.
const CAMERAS_URL = 'https://www.livetraffic.com/datajson/all-feeds-web.json';
const WEB_FEED_URL = CAMERAS_URL;

/** Categories served by their own dedicated source already, or that are
 *  facilities rather than road events. Rest areas are static amenities
 *  (850 of them) and belong on their own layer, not in a works feed. */
const WEB_FEED_SKIP = new Set(['livecams', 'restareas']);

/**
 * Stable id for a works record.
 *
 * This feed's `id` is a surrogate key that upstream REGENERATES on each
 * rebuild — measured across two snapshots ~90 min apart, 689 of 1,277
 * unchanged works came back under a new id (3322871441 -> 3322908764).
 * Archiving on it would treat every rebuild as ~1,700 brand-new
 * incidents (~288k junk rows/day) and make the sidecar useless.
 *
 * Coordinates + title + category are stable across those rebuilds, so
 * hash those instead. Same approach bom.ts uses for its id-less feed.
 */
function stableWorksId(lon: number, lat: number, title: string, cat: string): string {
  return (
    'ltw:' +
    createHash('sha1')
      .update(`${lon.toFixed(5)}|${lat.toFixed(5)}|${title}|${cat}`)
      .digest('hex')
      .slice(0, 20)
  );
}

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
    /** Every polyline the item carries, not just the first. */
    encodedPolylines: { coords: string; direction: string }[];
    created: string;
    lastUpdated: string;
    start: string;
    end: string;
    isEnded: boolean;
    isMajor: boolean;
    arrangement: string;
    periods: unknown[];
    /** 'State road' | 'Local road' — upstream's road-ownership flag. */
    isLocalRoad: string;
    region: string;
    crossStreet: string;
    secondLocation: string;
    locationQualifier: string;
    queueLength: number | null;
    duration: string;
    adviceC: string;
    webLinks: unknown[];
    /** Upstream's own key. Volatile on the works feed — see
     *  stableWorksId() — so never use it as an identity. */
    upstreamId?: string;
    /** Council attribution — only the LGA feed carries these. */
    orgName: string;
    orgContact: string;
    orgEmail: string;
    orgWebsite: string;
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

/**
 * Advice fields come through with HTML markup embedded (`<p>`, `<br>`,
 * `&nbsp;`). Consumers render them as text, so the tags showed up
 * literally on the page. Strip to plain prose here rather than in each
 * of the three frontends.
 */
function asProse(v: unknown): string {
  return asString(v)
    .replace(/<br\s*\/?>/gi, ' ')
    .replace(/<[^>]*>/g, '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/\s+/g, ' ')
    .trim();
}

function asArray(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

function asBool(v: unknown): boolean {
  return v === true;
}

/**
 * Upstream sends `encodedPolylines` as an ARRAY of
 * `{ levels, direction, coords }`, never a bare string — a plain
 * asString() on it yields '' and silently drops the road geometry.
 * Roughly a fifth of incidents carry one.
 */
function parsePolylines(v: unknown): { coords: string; direction: string }[] {
  const out: { coords: string; direction: string }[] = [];
  if (typeof v === 'string' && v) {
    out.push({ coords: v, direction: '' });
    return out;
  }
  for (const el of asArray(v)) {
    if (typeof el === 'string' && el) {
      out.push({ coords: el, direction: '' });
      continue;
    }
    if (el && typeof el === 'object') {
      const o = el as Record<string, unknown>;
      const coords = asString(o['coords']) || asString(o['encodedPolyline']);
      if (coords) out.push({ coords, direction: asString(o['direction']) });
    }
  }
  return out;
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

  // roads[] holds the useful location detail. Previous versions read only
  // mainStreet/suburb off roads[0] and looked for an `affectedDirection`
  // key that upstream does not have; the real keys are below.
  const roadsInfo = props['roads'];
  let roadsStr = '';
  let affectedDirection = '';
  let impactedLanes: unknown[] = [];
  let region = '';
  let crossStreet = '';
  let secondLocation = '';
  let locationQualifier = '';
  let queueLength: number | null = null;
  if (Array.isArray(roadsInfo) && roadsInfo.length > 0) {
    // Join every road, not just the first — an incident can span several.
    roadsStr = roadsInfo
      .map((entry) => {
        if (!entry || typeof entry !== 'object') return '';
        const r = entry as Record<string, unknown>;
        return `${asString(r['mainStreet'])} ${asString(r['suburb'])}`.trim();
      })
      .filter(Boolean)
      .join('; ');
    const first = roadsInfo[0];
    if (first && typeof first === 'object') {
      const r = first as Record<string, unknown>;
      affectedDirection =
        asString(r['affectedDirection']) || asString(r['conditionTendency']);
      impactedLanes = asArray(r['impactedLanes']);
      region = asString(r['region']);
      crossStreet = asString(r['crossStreet']);
      secondLocation = asString(r['secondLocation']);
      locationQualifier = asString(r['locationQualifier']);
      queueLength = asNumber(r['queueLength']);
    }
  } else if (roadsInfo) {
    roadsStr = asString(roadsInfo);
  }

  const polylines = parsePolylines(
    props['encodedPolylines'] ?? props['encodedPolyline'],
  );

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
      // Upstream has subCategoryA/subCategoryB, never a plain
      // `subCategory` — reading that key alone left this always empty.
      subCategory:
        asString(props['subCategory']) ||
        asString(props['subCategoryA']) ||
        asString(props['subCategoryB']),
      incidentKind: asString(props['incidentKind']),
      title: cleanTitle || rawTitle,
      headline: asString(props['headline']),
      displayName: asString(props['displayName']),
      subtitle: asString(props['subtitle']),
      otherAdvice:
        asProse(props['otherAdvice']) || asProse(props['adviceA']),
      adviceB: asProse(props['adviceB']),
      roads: roadsStr,
      affectedDirection,
      // impactedLanes is nested under roads[], not top level.
      impactedLanes: impactedLanes.length
        ? impactedLanes
        : asArray(props['impactedLanes']),
      speedLimit: asString(props['speedLimit']),
      expectedDelay:
        asString(props['expectedDelay']) || asString(props['delay']),
      diversions:
        asString(props['diversions']) || asString(props['diversion']),
      encodedPolyline: polylines[0]?.coords ?? '',
      encodedPolylines: polylines,
      created: asString(props['created']) || asString(props['start']),
      lastUpdated:
        asString(props['lastUpdated']) || asString(props['end']),
      start: asString(props['start']),
      end: asString(props['end']),
      isEnded:
        asBool(props['ended']) || asBool(props['isEnded']),
      isMajor: asBool(props['isMajor']),
      arrangement:
        asString(props['arrangement']) ||
        asString(props['arrangementElements']),
      periods: asArray(props['periods']),
      isLocalRoad: asString(props['isLocalRoad']),
      region,
      crossStreet,
      secondLocation,
      locationQualifier,
      queueLength,
      duration: asString(props['duration']),
      adviceC: asProse(props['adviceC']),
      webLinks: asArray(props['webLinks']),
      // Present only on the council-submitted LGA feed.
      orgName: asString(props['OrgName']),
      orgContact: asString(props['OrgContact']),
      orgEmail: asString(props['OrgEmail']),
      orgWebsite: asString(props['OrgWebsite']),
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
  intervalMs: number;
}

const HAZARD_KINDS: HazardKind[] = [
  { storeKey: 'traffic_incidents', archiveSource: 'traffic_incident', endpoint: 'incident', label: 'Incident', intervalMs: 60_000 },
  { storeKey: 'traffic_roadwork', archiveSource: 'traffic_roadwork', endpoint: 'roadwork', label: 'Roadwork', intervalMs: 300_000 },
  { storeKey: 'traffic_flood', archiveSource: 'traffic_flood', endpoint: 'flood', label: 'Flood', intervalMs: 300_000 },
  { storeKey: 'traffic_fire', archiveSource: 'traffic_fire', endpoint: 'fire', label: 'Fire', intervalMs: 300_000 },
  { storeKey: 'traffic_majorevent', archiveSource: 'traffic_majorevent', endpoint: 'majorevent', label: 'Major Event', intervalMs: 300_000 },
  { storeKey: 'traffic_alpine', archiveSource: 'traffic_alpine', endpoint: 'alpine', label: 'Alpine', intervalMs: 300_000 },
  // Council-submitted local-road records. A completely separate reporting
  // stream from the five feeds above: verified zero overlap with them on
  // id, coordinate AND street+suburb, and every row carries Org* council
  // attribution that no other feed has.
  { storeKey: 'traffic_lga', archiveSource: 'traffic_lga', endpoint: 'regional/lga-incidents', label: 'Council', intervalMs: 300_000 },
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

/**
 * Works + hazards out of all-feeds-web.json.
 *
 * This file is Python-serialised: nested values arrive as `str()` output
 * ("[{'suburb': 'MOLONGLO'}]", "False", epoch-millis as strings), so the
 * generic parser can't read its `roads`. We take the flat fields it does
 * expose and let `otherAdvice` carry the road description, which upstream
 * writes as plain prose.
 *
 * Geometry casing is inconsistent here — 'Point', 'POINT' and '' all
 * occur — so match case-insensitively rather than on the exact string.
 */
export async function fetchTrafficWorks(): Promise<TrafficSnapshot> {
  const data = await fetchJson<unknown>(WEB_FEED_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
  const items = Array.isArray(data) ? data : [];
  const features: TrafficFeature[] = [];
  for (const item of items) {
    if (!item || typeof item !== 'object') continue;
    const it = item as Record<string, unknown>;
    const cat = asString(it['eventCategory']);
    const kind = asString(it['eventType']);
    const key = cat.toLowerCase().replace(/[^a-z]/g, '');
    const kindKey = kind.toLowerCase().replace(/[^a-z]/g, '');
    if (WEB_FEED_SKIP.has(key) || WEB_FEED_SKIP.has(kindKey)) continue;

    const geom = it['geometry'] as Record<string, unknown> | undefined;
    if (!geom) continue;
    if (asString(geom['type']).toLowerCase() !== 'point') continue;
    const coords = geom['coordinates'];
    if (!Array.isArray(coords) || coords.length < 2) continue;
    const lon = asNumber(coords[0]);
    const lat = asNumber(coords[1]);
    if (lon === null || lat === null) continue;

    const props = (it['properties'] as Record<string, unknown> | undefined) ?? {};
    // 'ended' is the string 'False'/'None' here, never a real boolean.
    if (asString(props['ended']).toLowerCase() === 'true') continue;

    const title = asString(props['title']) || asString(props['heading']);
    const displayName = asString(props['displayName']);
    const { incidentType, cleanTitle } = extractIncidentType(title || displayName);

    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [lon, lat] },
      properties: {
        id: stableWorksId(lon, lat, title || displayName, cat),
        /** Upstream's own key — kept for reference, but it is volatile. */
        upstreamId: asString(it['id']),
        type: cat || kind || 'Works',
        incidentType,
        mainCategory: cat,
        subCategory:
          asString(props['subCategoryA']) || asString(props['subCategoryB']),
        incidentKind: kind,
        title: cleanTitle || title || displayName,
        headline: '',
        displayName,
        subtitle: '',
        otherAdvice: asProse(props['otherAdvice']),
        adviceB: '',
        roads: asString(props['road']) || asString(props['suburb']),
        affectedDirection: '',
        impactedLanes: [],
        speedLimit: '',
        expectedDelay: '',
        diversions: '',
        encodedPolyline: '',
        encodedPolylines: [],
        created: asString(props['created']) || asString(props['start']),
        lastUpdated: asString(props['lastUpdated']),
        start: asString(props['start']),
        end: asString(props['end']),
        isEnded: false,
        isMajor: false,
        arrangement: '',
        periods: [],
        isLocalRoad: '',
        region: '',
        crossStreet: '',
        secondLocation: '',
        locationQualifier: '',
        queueLength: null,
        duration: '',
        adviceC: '',
        webLinks: [],
        orgName: '',
        orgContact: '',
        orgEmail: '',
        orgWebsite: '',
        source: 'livetraffic',
      },
    });
  }
  return { type: 'FeatureCollection', features, count: features.length };
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
      intervalMs: k.intervalMs,
      fetch: () => fetchHazard(k),
    });
  }
  registerSource<TrafficSnapshot>({
    name: 'traffic_works',
    family: 'traffic',
    intervalMs: 300_000,
    fetch: fetchTrafficWorks,
  });
  registerSource<TrafficCamerasSnapshot>({
    name: 'traffic_cameras',
    family: 'traffic',
    intervalMs: 60_000,
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
