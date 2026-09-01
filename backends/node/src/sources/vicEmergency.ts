/**
 * Victoria — emergency.vic.gov.au events feed.
 *
 * One endpoint, five publishers. `sourceOrg` says which:
 *
 *   VIC/CFA    Country Fire Authority — fires, but also road accidents,
 *              washaways and hazmat
 *   EMV        Emergency Management Victoria — the CAP warning layer,
 *              currently riverine floods (SES) and the odd grass fire
 *   VIC/DEECA  Dept of Energy, Environment and Climate Action — the
 *              planned burn program
 *   VIC/SES    State Emergency Service — storm, flood, building damage
 *   VIC/ESTA   the state's triple-zero dispatcher, publishing calls it
 *              has dispatched but no single agency owns yet
 *   NSW/RFS    a re-publication of a NSW incident, DROPPED — we ingest
 *              the RFS feed directly, and keeping this would put every
 *              border fire on the map twice
 *
 * ROUTING IS BY EVENT, NOT BY AGENCY. Only fire alerts belong on the
 * Fires layer, so a CFA hazmat call goes to Hazards and an SES riverine
 * flood warning goes to Floods, even though CFA is a fire service. See
 * vicLayerFor().
 *
 * ALERT LEVEL ONLY EXISTS ON WARNINGS. `category1` carries the
 * Australian Warning System level (Advice / Watch and Act / Emergency
 * Warning) when `feedType` is 'warning', and the incident TYPE when it
 * is 'incident'. Reading it as a level regardless would label every CFA
 * callout with an alert level of "Fire".
 *
 * Property names are the RFS ones, as with the NT and QLD sources, so
 * archiveExtract promotes category <- alertLevel and
 * subcategory <- fireType generically and the frontend renders every
 * agency through one path.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const VIC_URL = 'https://emergency.vic.gov.au/public/events-geojson.json';

/** Which map layer a record belongs on. */
export type VicLayer = 'fire' | 'flood' | 'hazard';

/**
 * `sourceOrg` to the short code used for labels and agency filters.
 * Anything unrecognised keeps its own name rather than being forced
 * into a bucket — a new publisher should show up as itself.
 */
export function vicAgencyFor(sourceOrg: string): string {
  const o = sourceOrg.toUpperCase();
  if (o.includes('CFA')) return 'CFA';
  if (o.includes('DEECA')) return 'DEECA';
  if (o.includes('SES')) return 'SES';
  if (o.includes('EMV')) return 'EMV';
  if (o.includes('ESTA')) return 'ESTA';
  return sourceOrg || 'VIC';
}

/**
 * Where a Victorian record belongs.
 *
 * Reads the event, not the publisher: CFA runs to more than fires and
 * SES warnings are mostly water. Checked flood-first because a flood
 * rescue names both, and the water is the thing to show.
 */
export function vicLayerFor(props: Record<string, unknown>): VicLayer {
  const cap = (props['cap'] as Record<string, unknown> | undefined) ?? {};
  const text = [
    props['category1'],
    props['category2'],
    cap['event'],
    cap['category'],
  ]
    .map((v) => String(v ?? '').toLowerCase())
    .join(' ');

  if (/flood|water over|riverine|storm surge|tsunami/.test(text)) return 'flood';
  if (/hazmat|hazardous material|chemical/.test(text)) return 'hazard';
  if (/fire|burn|smoke/.test(text)) return 'fire';

  // Victoria is the one feed where the publisher has to be consulted,
  // because it is the one feed that is not a single agency's. A CFA or
  // DEECA road accident is a fire service's own job and belongs on Fires
  // with the rest of its work; an SES tree-down or building-damage call
  // is not, and stays on Hazards where it has always been.
  const org = vicAgencyFor(asString(props['sourceOrg']));
  return org === 'CFA' || org === 'DEECA' ? 'fire' : 'hazard';
}

export interface VicFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
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
    layer: VicLayer;
    agency: string;
    /** Warning-area rings, when the record carries any. */
    polygons: number[][][];
    /** The CAP <info> block, kept whole for the detail panel. */
    capEvent: string;
    capSeverity: string;
    capUrgency: string;
    capCertainty: string;
    capResponseType: string;
    capSender: string;
    /** Plain-text advice; webBody is the HTML version of the same. */
    text: string;
    action: string;
    resources: string;
    sizeFmt: string;
    url: string;
    is_active: boolean;
    source: 'vic_emergency';
  };
}

export interface VicSnapshot {
  type: 'FeatureCollection';
  features: VicFeature[];
  count: number;
}

const EMPTY: VicSnapshot = { type: 'FeatureCollection', features: [], count: 0 };

function asString(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return '';
}

function tidy(v: unknown): string {
  return asString(v).replace(/\s+/g, ' ').trim();
}

/**
 * The pin position and any warning rings.
 *
 * Incidents are a plain Point. Warnings are a GeometryCollection of
 * Point + Polygon pairs — the point to pin and the polygon to shade, the
 * same shape RFS fire grounds already render in.
 */
function geometryOf(geom: unknown): { point: [number, number]; polygons: number[][][] } | null {
  const g = geom as Record<string, unknown> | undefined;
  if (!g) return null;

  const readPoint = (o: Record<string, unknown>): [number, number] | null => {
    const c = o['coordinates'];
    if (!Array.isArray(c) || c.length < 2) return null;
    const lon = Number(c[0]);
    const lat = Number(c[1]);
    return Number.isFinite(lon) && Number.isFinite(lat) ? [lon, lat] : null;
  };

  if (g['type'] === 'Point') {
    const p = readPoint(g);
    return p ? { point: p, polygons: [] } : null;
  }

  if (g['type'] === 'GeometryCollection' && Array.isArray(g['geometries'])) {
    let point: [number, number] | null = null;
    const polygons: number[][][] = [];
    for (const raw of g['geometries'] as unknown[]) {
      const m = raw as Record<string, unknown>;
      if (m['type'] === 'Point' && !point) point = readPoint(m);
      else if (m['type'] === 'Polygon' && Array.isArray(m['coordinates'])) {
        const ring = (m['coordinates'] as unknown[])[0];
        if (Array.isArray(ring)) polygons.push(ring as number[][]);
      }
    }
    // A warning with rings but no point still needs somewhere to sit;
    // the first ring's centroid is close enough at pin scale.
    if (!point && polygons.length && polygons[0]!.length) {
      const r = polygons[0]!;
      let sx = 0;
      let sy = 0;
      let n = 0;
      for (const pt of r) {
        if (!Array.isArray(pt) || pt.length < 2) continue;
        sx += Number(pt[0]);
        sy += Number(pt[1]);
        n += 1;
      }
      if (n) point = [sx / n, sy / n];
    }
    return point ? { point, polygons } : null;
  }

  return null;
}

/** Map one feed record, or null when it can't be placed or shouldn't be kept. */
export function toVicFeature(raw: unknown): VicFeature | null {
  if (!raw || typeof raw !== 'object') return null;
  const f = raw as Record<string, unknown>;
  const p = (f['properties'] as Record<string, unknown> | undefined) ?? {};

  const org = asString(p['sourceOrg']);
  // Victoria re-publishes NSW RFS incidents. We ingest that feed
  // directly, so keeping these would double every NSW fire.
  if (org.toUpperCase().startsWith('NSW/')) return null;

  const geo = geometryOf(f['geometry']);
  if (!geo) return null;

  const id = asString(p['id']) || asString(p['sourceId']);
  if (!id) return null;

  const isWarning = asString(p['feedType']).toLowerCase() === 'warning';
  const cap = (p['cap'] as Record<string, unknown> | undefined) ?? {};
  const cat1 = tidy(p['category1']);
  const cat2 = tidy(p['category2']);
  const location = tidy(p['location']);
  const iso = asString(p['updated']) || asString(p['created']);

  // `sourceTitle` is unusable on two of the five publishers: SES sends
  // the literal string "Undefined", and on a warning it holds the alert
  // level ("Advice"), which would put six identically-named pins on the
  // map. Both fall back to what the record is and where.
  const rawTitle = tidy(p['sourceTitle']);
  const title = isWarning
    ? [tidy(cap['event']) || cat2, location].filter(Boolean).join(' - ')
    : (/^undefined$/i.test(rawTitle) ? '' : rawTitle);

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: geo.point },
    properties: {
      title: title || tidy(p['webHeadline']) || cat2 || cat1 || 'Victorian incident',
      guid: `vic:${id}`,
      status: tidy(p['status']),
      location,
      // Only warnings carry a level here; on incidents category1 is the
      // incident type, so reading it as a level would give every CFA
      // callout an alert level of "Fire".
      alertLevel: isWarning ? cat1 : '',
      // The specific type, which for a warning is what the CAP event says.
      fireType: isWarning ? tidy(cap['event']) || cat2 : cat2 || cat1,
      responsibleAgency: vicAgencyFor(org),
      updated: iso ? new Date(iso).toLocaleString('en-AU', { timeZone: 'Australia/Melbourne' }) : '',
      updatedISO: iso ? new Date(iso).toISOString() : '',
      layer: vicLayerFor(p),
      agency: vicAgencyFor(org),
      polygons: geo.polygons,
      capEvent: tidy(cap['event']),
      capSeverity: tidy(cap['severity']),
      capUrgency: tidy(cap['urgency']),
      capCertainty: tidy(cap['certainty']),
      capResponseType: tidy(cap['responseType']),
      capSender: tidy(cap['senderName']),
      text: tidy(p['text']),
      action: tidy(p['action']),
      resources: asString(p['resources']),
      sizeFmt: tidy(p['sizeFmt']),
      url: asString(p['url']),
      is_active: true,
      source: 'vic_emergency',
    },
  };
}

/** Throws on upstream failure so the poller's backoff engages. */
export async function fetchVicEmergency(): Promise<VicSnapshot> {
  const data = await fetchJson<unknown>(VIC_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
    timeoutMs: 20_000,
  });
  const root = data as Record<string, unknown> | null;
  const items = root && Array.isArray(root['features']) ? (root['features'] as unknown[]) : null;
  if (!items) {
    throw new Error('vic_emergency: feed returned no features array');
  }
  const features: VicFeature[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    const f = toVicFeature(item);
    if (!f) continue;
    if (seen.has(f.properties.guid)) continue;
    seen.add(f.properties.guid);
    features.push(f);
  }
  return { type: 'FeatureCollection', features, count: features.length };
}

export default function register(): void {
  registerSource<VicSnapshot>({
    name: 'vic_emergency',
    // archive_rfs, with the other emergency-service feeds.
    family: 'rfs',
    intervalMs: 120_000,
    fetch: fetchVicEmergency,
  });
}

export function vicSnapshot(): VicSnapshot {
  return liveStore.getData<VicSnapshot>('vic_emergency') ?? EMPTY;
}
