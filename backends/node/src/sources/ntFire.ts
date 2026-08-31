/**
 * NT Fire & Rescue Service incidents — PFES incident map feed.
 *
 * Northern Territory Police, Fire and Emergency Services publish the
 * data behind their public incident map. Every record is NTFRS; the feed
 * mixes structure fires, grass fires, automatic alarms, permit burns,
 * hazmat, road crashes and BushfiresNT warnings.
 *
 * ATTRIBUTION: the feed asks not to be re-published. We poll it at a
 * fraction of its own refresh rate (it updates every 10-15 minutes) and
 * credit NTFRS wherever these records are shown. Keep both of those true
 * if this file is touched.
 *
 * SHAPE. Not a bare FeatureCollection — the features are nested under
 * `incidents`, and the wrapper also carries the feed's own `lastupdated`
 * and `note`. Geometry is Point for most records and Polygon for
 * BushfiresNT fire grounds.
 *
 * PROPERTY NAMES ARE DELIBERATELY THE RFS ONES. Emitting `alertLevel`
 * and `fireType` means archiveExtract's generic promotion
 * (category <- alertLevel, subcategory <- fireType) gives NT the same
 * facets as NSW RFS with no new mapping, and map.html can render both
 * through one code path.
 *
 * CLOSED RECORDS ARE KEPT, NOT DROPPED. Most of the feed is closed at
 * any moment (30 of 31 in the first sample). They are archived with
 * `is_active: false` so the logs page can show how an incident
 * progressed, and the map route filters them out instead. This is the
 * opposite of the ACT Ambulance source, which drops Finished responses
 * at ingest and therefore loses every job's final transition.
 *
 * TWO FIELDS DISAGREE ABOUT "CLOSED". `_status` is what drives their own
 * map icons; `Status` is the operational state and lags — one sampled
 * record was `_status: closed` while `Status` still read `Going`. Use
 * `_status`.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const NT_FIRE_URL = 'https://www.pfes.nt.gov.au/incidentmap/json/incidents.json';

/** Which map layer a record belongs on. */
export type NtLayer = 'fire' | 'hazard';

/**
 * `_category` -> layer. Values are from the published map key: fire,
 * roadcrash, alarm, advice, other, bushfire-emergency, bushfire-watch,
 * bushfire-advice, plannedburn (each also appearing closed).
 *
 * Everything is a fire except road crashes, which are hazards. "Other"
 * (hazmat, smoke complaints, illegal burns) sits with the fires because
 * it is still an NTFRS turnout.
 */
export function ntLayerFor(category: string): NtLayer {
  const c = category.toLowerCase().replace(/[^a-z]/g, '');
  if (c.includes('roadcrash')) return 'hazard';
  return 'fire';
}

export interface NtFireFeature {
  type: 'Feature';
  geometry:
    | { type: 'Point'; coordinates: [number, number] }
    | { type: 'Polygon'; coordinates: number[][][] };
  properties: {
    title: string;
    guid: string;
    status: string;
    location: string;
    alertLevel: string;
    fireType: string;
    responsibleAgency: string;
    updated: string;
    updatedISO: string;
    /** Which map layer this belongs on — 'fire' or 'hazard'. */
    layer: NtLayer;
    /** Upstream's own category slug, kept for debugging and filters. */
    ntCategory: string;
    /** Bushfire advice text, present only on BushfiresNT records. */
    currentSituation: string;
    risks: string;
    whatToDo: string;
    adviceToPublic: string;
    /** False once the incident is closed — the map route drops these. */
    is_active: boolean;
    agency: 'NTFRS';
    source: 'nt_fire';
  };
}

export interface NtFireSnapshot {
  type: 'FeatureCollection';
  features: NtFireFeature[];
  count: number;
}

const EMPTY: NtFireSnapshot = { type: 'FeatureCollection', features: [], count: 0 };

function asString(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return '';
}

function asNumber(v: unknown): number | null {
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/** Collapse the whitespace upstream leaves in the free-text advice fields. */
function tidy(v: unknown): string {
  return asString(v).replace(/\s+/g, ' ').trim();
}

/**
 * A representative point for a record, whatever its geometry.
 *
 * Polygons (BushfiresNT fire grounds) still need a single coordinate for
 * the id and for the map pin; the ring's centroid is close enough at the
 * scale a fire is displayed.
 */
function representativePoint(geom: Record<string, unknown>): [number, number] | null {
  const type = asString(geom['type']);
  const coords = geom['coordinates'];
  if (type === 'Point' && Array.isArray(coords) && coords.length >= 2) {
    const lon = asNumber(coords[0]);
    const lat = asNumber(coords[1]);
    return lon !== null && lat !== null ? [lon, lat] : null;
  }
  if (type === 'Polygon' && Array.isArray(coords) && Array.isArray(coords[0])) {
    const ring = coords[0] as unknown[];
    let sx = 0;
    let sy = 0;
    let n = 0;
    for (const pt of ring) {
      if (!Array.isArray(pt) || pt.length < 2) continue;
      const x = asNumber(pt[0]);
      const y = asNumber(pt[1]);
      if (x === null || y === null) continue;
      sx += x;
      sy += y;
      n += 1;
    }
    return n > 0 ? [sx / n, sy / n] : null;
  }
  return null;
}

/**
 * A stable identity for an NT incident.
 *
 * The feed publishes no id, so one is derived — and it MUST survive the
 * transition to closed, or the closing snapshot archives as a brand-new
 * incident and the status history splits in two. So it is built only
 * from things that don't change over an incident's life: where it is,
 * what it is, and when it was first notified. Status, alert level and
 * `_lastupdate` are all deliberately excluded.
 */
export function ntIncidentId(
  lon: number,
  lat: number,
  eventType: string,
  notified: string,
): string {
  const round = (n: number): string => n.toFixed(5);
  const key = `${round(lon)}|${round(lat)}|${eventType.toLowerCase()}|${notified}`;
  // Short, readable, and namespaced so it can't be mistaken for an
  // upstream identifier from another feed.
  let h = 0;
  for (let i = 0; i < key.length; i += 1) {
    h = (h * 31 + key.charCodeAt(i)) | 0;
  }
  return `ntf:${(h >>> 0).toString(16).padStart(8, '0')}`;
}

/** Map one upstream feature, or null when it can't be placed. */
export function toNtFeature(raw: unknown): NtFireFeature | null {
  if (!raw || typeof raw !== 'object') return null;
  const f = raw as Record<string, unknown>;
  const geom = (f['geometry'] as Record<string, unknown> | undefined) ?? {};
  const point = representativePoint(geom);
  if (!point) return null;

  const p = (f['properties'] as Record<string, unknown> | undefined) ?? {};
  const category = asString(p['_category']);
  const eventType = tidy(p['_eventtype']) || tidy(p['Fire Type']);
  const location = tidy(p['_location']) || tidy(p['Location']);
  const notified = asString(p['_datenotified']);

  // `_status` drives their own map icons; `Status` lags behind it.
  const closed = asString(p['_status']).toLowerCase() === 'closed';
  const lastUpdate = asString(p['_lastupdate']);

  const geometry: NtFireFeature['geometry'] =
    asString(geom['type']) === 'Polygon'
      ? { type: 'Polygon', coordinates: geom['coordinates'] as number[][][] }
      : { type: 'Point', coordinates: point };

  return {
    type: 'Feature',
    geometry,
    properties: {
      // The feed has no headline, so build one the way their own map
      // labels a pin: what happened, and where.
      title: [eventType, location].filter(Boolean).join(' - ') || 'NT Fire & Rescue incident',
      guid: ntIncidentId(point[0], point[1], eventType, notified),
      status: tidy(p['Status']),
      location,
      alertLevel: tidy(p['Alert Level']),
      fireType: eventType,
      responsibleAgency: tidy(p['Responsible Agency']) || 'NTFRS',
      updated: tidy(p['Last Update']),
      updatedISO: lastUpdate,
      layer: ntLayerFor(category),
      ntCategory: category,
      currentSituation: tidy(p['Current Situation']),
      risks: tidy(p['Risks']),
      whatToDo: tidy(p['What to do']),
      adviceToPublic: tidy(p['Advice to the Public']),
      is_active: !closed,
      agency: 'NTFRS',
      source: 'nt_fire',
    },
  };
}

/** Throws on upstream failure so the poller's backoff engages. */
export async function fetchNtFire(): Promise<NtFireSnapshot> {
  const data = await fetchJson<unknown>(NT_FIRE_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
    timeoutMs: 20_000,
  });
  const root = data as Record<string, unknown> | null;
  const wrapper = root?.['incidents'] as Record<string, unknown> | undefined;
  const items = wrapper && Array.isArray(wrapper['features']) ? (wrapper['features'] as unknown[]) : null;
  if (!items) {
    throw new Error('nt_fire: feed returned no incidents.features array');
  }
  const features: NtFireFeature[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    const f = toNtFeature(item);
    if (!f) continue;
    // Two records at the same place, of the same type, notified at the
    // same minute would collide; keep the first rather than stack them.
    if (seen.has(f.properties.guid)) continue;
    seen.add(f.properties.guid);
    features.push(f);
  }
  return { type: 'FeatureCollection', features, count: features.length };
}

export default function register(): void {
  registerSource<NtFireSnapshot>({
    name: 'nt_fire',
    // archive_rfs, beside NSW RFS — the logs page presents them as one
    // "Fires" provider.
    family: 'rfs',
    // Upstream refreshes every 10-15 minutes and asks not to be hammered.
    intervalMs: 300_000,
    fetch: fetchNtFire,
  });
}

/** Every record, closed ones included — the archive wants those. */
export function ntFireSnapshot(): NtFireSnapshot {
  return liveStore.getData<NtFireSnapshot>('nt_fire') ?? EMPTY;
}

/**
 * What the map should draw: open incidents only.
 *
 * Closed records stay in the archive (that is the whole point of keeping
 * them) but a closed job is not a live incident and must not sit on the
 * map. Filtering here rather than in the browser matches every other
 * layer, none of which pass `active_only`.
 */
export function ntFireActiveSnapshot(): NtFireSnapshot {
  const snap = ntFireSnapshot();
  const features = snap.features.filter((f) => f.properties.is_active !== false);
  return { type: 'FeatureCollection', features, count: features.length };
}
