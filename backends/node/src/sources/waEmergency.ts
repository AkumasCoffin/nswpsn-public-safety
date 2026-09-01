/**
 * Western Australia — DFES live incidents and public warnings.
 *
 * Two public JSON endpoints from Emergency WA, no key, CORS open:
 *
 *   /v1/incidents   what DFES is turned out to right now
 *   /v1/warnings    what the public is being told about it
 *
 * The same pair Queensland publishes, and both are ingested for the same
 * reason: the warnings feed lists only what a public warning has been
 * issued for (3 of 48 records when this was written), so on its own it
 * shows a fraction of the state; the incidents feed carries no alert
 * level, so on its own it could not colour anything by severity.
 *
 * PROPERTY NAMES ARE THE RFS ONES, deliberately — `alertLevel`,
 * `fireType`, `status`, `location`. archiveExtract promotes
 * category <- alertLevel and subcategory <- fireType generically, so
 * these get the same facets as NSW, NT, QLD and VIC for free, and
 * map.html renders every agency on the Fires layer through one path.
 *
 * THE JOIN QUEENSLAND NEVER GOT. qldFire.ts carries a `warningIncidentKey`
 * that always returns '' because upstream leaves `MasterIncidentNum`
 * null, so a QFD fire with a warning shows as two pins. WA populates the
 * equivalent field: both feeds carry an `event` id, and a warning's
 * `event` matches its incident's. Both are exposed here — `eventRef` on
 * incidents, `incidentRef` on warnings — so the frontend can show one
 * pin per event wearing the warning's level and the incident's status.
 *
 * WHAT THE DATA DICTATES.
 *
 * 1. `name` is the useful type, not `incident-type`. The latter reads
 *    "Other Incident" for 36 of 48 records; `name` says "Burn Off",
 *    "Road Crash", "Bushfire".
 * 2. Burn-offs are the bulk of the feed (35 of 48). WA publishes routine
 *    ones where the RFS publishes only major incidents. They are kept —
 *    "Burn Off" folds into the same Planned Burn pill as the RFS's
 *    "Hazard Reduction", so they can be switched off there.
 * 3. Nothing closes. Both feeds list only current records and drop them
 *    when they end, so the last stored state is whatever it was when the
 *    record vanished.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const BASE = 'https://api.emergency.wa.gov.au/v1';
const INCIDENTS_URL = `${BASE}/incidents`;
const WARNINGS_URL = `${BASE}/warnings`;

/** Which map layer a record belongs on. */
export type WaLayer = 'fire' | 'flood' | 'hazard';

export interface WaFeature {
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
    layer: WaLayer;
    /** Warning areas as GeoJSON outer rings, [lon, lat]. */
    polygons: number[][][];
    /** The event this record belongs to — the two feeds join on it. */
    eventRef: string;
    /** Warnings only: the incident's event id, so it can be folded in. */
    incidentRef: string;
    /** Warnings only — the public advice, kept for the detail panel. */
    headline: string;
    alertLine: string;
    whatToDo: string;
    peopleAffected: string;
    /** Warnings only — "Monitor conditions", and whether it is escalating. */
    action: string;
    actionType: string;
    capSeverity: string;
    capUrgency: string;
    capCertainty: string;
    /** Incidents only. */
    cadId: string;
    region: string;
    is_active: boolean;
    agency: 'DFES';
    source: 'wa_incident' | 'wa_warning';
  };
}

export interface WaSnapshot {
  type: 'FeatureCollection';
  features: WaFeature[];
  count: number;
}

const EMPTY: WaSnapshot = { type: 'FeatureCollection', features: [], count: 0 };

function asString(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return '';
}

function tidy(v: unknown): string {
  return asString(v).replace(/\s+/g, ' ').trim();
}

/** First entry of a string array field (`suburbs`, `lga`, `dfes-regions`). */
function firstOf(v: unknown): string {
  return Array.isArray(v) ? tidy(v[0]) : tidy(v);
}

/**
 * WA writes its longer notes as HTML fragments — `<p>`, `<ul>`, and
 * anchors with target/rel attributes. The detail panel renders text, so
 * the tags come out and the entities come back.
 */
export function stripHtml(v: unknown): string {
  return asString(v)
    .replace(/<\s*br\s*\/?\s*>/gi, ' ')
    .replace(/<\s*\/\s*(p|li|ul|ol|div)\s*>/gi, ' ')
    .replace(/<[^>]*>/g, '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, '\'')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Where a WA record belongs.
 * *
 * A FIRE SERVICE'S OWN JOBS STAY ON ITS OWN LAYER. Crashes, rescues,
 * alarms and assists are what these brigades spend most of their time
 * on, and pushing them to Hazards split one agency's work across two
 * layers — while NSW, whose feed has never been routed at all, kept its
 * MVA/Transport and Medical jobs on Fires the whole time. Only two
 * things are carved out: flooding, which belongs on Floods, and hazmat,
 * which is a different kind of emergency rather than a different kind
 * of fire.
 *
 * ORDER IS LOAD-BEARING between those two. WA words a chemical incident
 * as `hazmat-type: "HAZMAT Fire"` with a `cap-event-type` of Toxic
 * Plume, so a fire test running first would file a chemical spill as a
 * fire; and a flood rescue names both.
 */
export function waLayerFor(text: string): WaLayer {
  const t = text.toLowerCase();
  if (/flood|water over|storm surge|tsunami/.test(t)) return 'flood';
  if (/hazmat|hazardous material|chemical|cbrne|radiolog|toxic/.test(t)) {
    return 'hazard';
  }
  return 'fire';
}

/** Everything a record says about what it is, as one string for matching. */
function kindText(p: Record<string, unknown>): string {
  const capEvents = Array.isArray(p['cap-event-type'])
    ? (p['cap-event-type'] as unknown[]).map(asString).join(' ')
    : '';
  return [
    asString(p['name']),
    asString(p['incident-type']),
    asString(p['warning-type']),
    asString(p['entitySubType']),
    asString(p['hazmat-type']),
    asString(p['cap-category']),
    capEvents,
  ]
    .filter(Boolean)
    .join(' ');
}

/**
 * The Australian Warning System level, which WA has no field for.
 *
 * The level is inside the warning's name — "Bushfire Advice" — and in
 * the machine-readable `entitySubType`, "warnings_bushfire--advice". The
 * subtype is read first because it is a slug and cannot be reworded.
 *
 * Deliberately NOT derived from `cap-severity`: the live hazmat warning
 * is "Extreme - extraordinary threat" while being an ordinary general
 * warning. Severity and public alert level are different scales here.
 *
 * A warning with no level in its name returns '' and lands in the
 * existing "Not Applicable" bucket, where the pin falls back to showing
 * what kind of thing it is. That is correct, not a gap.
 */
export function waAlertLevel(warningType: unknown, entitySubType: unknown): string {
  const t = `${asString(entitySubType)} ${asString(warningType)}`.toLowerCase();
  if (/emergency[\s-]*warning/.test(t)) return 'Emergency Warning';
  if (/watch[\s-]*and[\s-]*act/.test(t)) return 'Watch and Act';
  if (/all[\s-]*clear/.test(t)) return 'All Clear';
  if (/advice/.test(t)) return 'Advice';
  return '';
}

/**
 * A warning's type with the alert level taken out of it: "Bushfire
 * Advice" is a bushfire at Advice level, and leaving the level in the
 * type would split one fire pill into three.
 */
export function waFireType(warningType: unknown): string {
  const cleaned = tidy(warningType)
    .replace(/\b(emergency\s*warning|watch\s*and\s*act|all\s*clear|advice)\b/gi, '')
    .replace(/\s+/g, ' ')
    .trim();
  return cleaned || tidy(warningType);
}

/**
 * The pin position and any warning areas.
 *
 * WA nests a whole FeatureCollection under `geo-source`: incidents carry
 * a single Point, warnings carry a Point AND one or more Polygons — the
 * point to pin and the polygon to shade, the same shape RFS fire grounds
 * already render in.
 */
function geometryOf(
  geoSource: unknown,
): { point: [number, number] | null; polygons: number[][][] } {
  const out: { point: [number, number] | null; polygons: number[][][] } = {
    point: null,
    polygons: [],
  };
  const g = geoSource as Record<string, unknown> | undefined;
  const feats = g && Array.isArray(g['features']) ? (g['features'] as unknown[]) : [];

  const readPoint = (c: unknown): [number, number] | null => {
    if (!Array.isArray(c) || c.length < 2) return null;
    const lon = Number(c[0]);
    const lat = Number(c[1]);
    return Number.isFinite(lon) && Number.isFinite(lat) ? [lon, lat] : null;
  };

  for (const raw of feats) {
    const f = raw as Record<string, unknown> | undefined;
    const geom = f?.['geometry'] as Record<string, unknown> | undefined;
    if (!geom) continue;
    const type = geom['type'];
    if (type === 'Point') {
      if (!out.point) out.point = readPoint(geom['coordinates']);
    } else if (type === 'Polygon' && Array.isArray(geom['coordinates'])) {
      const ring = (geom['coordinates'] as unknown[])[0];
      if (Array.isArray(ring)) out.polygons.push(ring as number[][]);
    } else if (type === 'MultiPolygon' && Array.isArray(geom['coordinates'])) {
      for (const poly of geom['coordinates'] as unknown[]) {
        const ring = Array.isArray(poly) ? (poly as unknown[])[0] : null;
        if (Array.isArray(ring)) out.polygons.push(ring as number[][]);
      }
    }
  }
  return out;
}

/**
 * Where to pin a record.
 *
 * `geo-source`'s Point wins over the `location` object because they are
 * not the same place: on the live hazmat warning the geo-source point is
 * the tip site and `location` is the centre of North West Cape, 13 km
 * away. `location` is the fallback, not the answer.
 */
function pointFor(
  p: Record<string, unknown>,
  geo: [number, number] | null,
): [number, number] | null {
  if (geo) return geo;
  const loc = p['location'] as Record<string, unknown> | undefined;
  if (!loc) return null;
  const lat = Number(loc['latitude']);
  const lon = Number(loc['longitude']);
  return Number.isFinite(lat) && Number.isFinite(lon) ? [lon, lat] : null;
}

/**
 * Street, suburb and shire, as much of each as the record carries and no
 * more. The three overlap constantly — a warning's `location.value` is
 * "North West Cape, Western Australia" while its `suburbs` is "NORTH
 * WEST CAPE", so an exact-match dedupe leaves the same place named
 * twice in different cases. Parts are compared on containment, and the
 * state suffix comes off since every record here is in WA.
 */
function locationFor(p: Record<string, unknown>): string {
  const loc = p['location'] as Record<string, unknown> | undefined;
  const street = tidy(loc?.['value']).replace(/,?\s*Western Australia$/i, '');
  const parts: string[] = [];
  for (const part of [street, firstOf(p['suburbs']), firstOf(p['lga'])]) {
    if (!part) continue;
    const u = part.toUpperCase();
    if (parts.some((s) => s.toUpperCase().includes(u) || u.includes(s.toUpperCase()))) {
      continue;
    }
    parts.push(part);
  }
  return parts.join(', ');
}

/** WA stamps its times +08:00; show them in the timezone they were written in. */
function perthTime(iso: string): string {
  if (!iso) return '';
  const d = new Date(iso);
  return Number.isNaN(d.getTime())
    ? ''
    : d.toLocaleString('en-AU', { timeZone: 'Australia/Perth' });
}

const BLANK = {
  polygons: [] as number[][][],
  eventRef: '',
  incidentRef: '',
  headline: '',
  alertLine: '',
  whatToDo: '',
  peopleAffected: '',
  action: '',
  actionType: '',
  capSeverity: '',
  capUrgency: '',
  capCertainty: '',
  cadId: '',
  region: '',
};

/** One DFES incident, or null when it can't be placed. */
export function toWaIncident(raw: unknown): WaFeature | null {
  if (!raw || typeof raw !== 'object') return null;
  const p = raw as Record<string, unknown>;

  const id = asString(p['id']);
  if (!id) return null;

  const geo = geometryOf(p['geo-source']);
  const coords = pointFor(p, geo.point);
  if (!coords) return null;

  // `name` is the specific type; `incident-type` reads "Other Incident"
  // on three quarters of the feed.
  const kind = tidy(p['name']) || tidy(p['incident-type']);
  const suburb = firstOf(p['suburbs']);
  const iso = asString(p['updated-date-time']) || asString(p['issued-date-time']);

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: coords },
    properties: {
      ...BLANK,
      polygons: geo.polygons,
      // The feed has no headline, so build one the way Emergency WA
      // labels its own pins: what it is, and where.
      title: [kind, suburb].filter(Boolean).join(' - ') || 'DFES incident',
      guid: `wa:${id}`,
      status: tidy(p['incident-status']),
      location: locationFor(p),
      // Incidents carry no alert level; that is what the warnings feed
      // is for. Blank puts them in the "Not Applicable" bucket beside
      // RFS incidents without one.
      alertLevel: '',
      fireType: kind,
      responsibleAgency: 'DFES',
      updated: perthTime(iso),
      updatedISO: iso,
      layer: waLayerFor(kindText(p)),
      eventRef: asString(p['event']),
      cadId: asString(p['cad-id']),
      region: firstOf(p['dfes-regions']),
      is_active: true,
      agency: 'DFES',
      source: 'wa_incident',
    },
  };
}

/** One DFES public warning, or null when it can't be placed. */
export function toWaWarning(raw: unknown): WaFeature | null {
  if (!raw || typeof raw !== 'object') return null;
  const p = raw as Record<string, unknown>;

  const id = asString(p['id']);
  if (!id) return null;

  const geo = geometryOf(p['geo-source']);
  const coords = pointFor(p, geo.point);
  if (!coords) return null;

  const iso = asString(p['issued-date-time']) || asString(p['published-date-time']);

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: coords },
    properties: {
      ...BLANK,
      title:
        tidy(p['title']) || tidy(p['headline']) || tidy(p['warning-type']) || 'DFES warning',
      guid: `waw:${id}`,
      // A warning has no containment state. WA's nearest field is
      // `action-statement` ("Monitor conditions"), which is an
      // instruction rather than a status: putting it here would fill the
      // logs page STATUS facet with things that are not statuses. It is
      // carried as `action` instead. Warnings draw as alert triangles,
      // so nothing reads a status colour off them anyway.
      status: '',
      location: locationFor(p),
      alertLevel: waAlertLevel(p['warning-type'], p['entitySubType']),
      fireType: waFireType(p['warning-type']),
      responsibleAgency: 'DFES',
      updated: perthTime(iso),
      updatedISO: iso,
      layer: waLayerFor(kindText(p)),
      polygons: geo.polygons,
      // The event this warning is about. Its incident carries the same
      // id, which is how the two feeds fold into one pin.
      eventRef: asString(p['event']),
      incidentRef: asString(p['event']),
      headline: tidy(p['headline']),
      alertLine: stripHtml(p['alert-line']),
      whatToDo: stripHtml(p['what-to-do-note']),
      peopleAffected: stripHtml(p['people-and-areas-affected-note']),
      action: tidy(p['action-statement']),
      actionType: tidy(p['action-statement--type']),
      capSeverity: tidy(p['cap-severity']),
      capUrgency: tidy(p['cap-urgency']),
      capCertainty: tidy(p['cap-certainty']),
      is_active: true,
      agency: 'DFES',
      source: 'wa_warning',
    },
  };
}

async function fetchFeed(
  url: string,
  key: 'incidents' | 'warnings',
  map: (raw: unknown) => WaFeature | null,
): Promise<WaSnapshot> {
  const data = await fetchJson<unknown>(url, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
    timeoutMs: 20_000,
  });
  const root = data as Record<string, unknown> | null;
  const items = root && Array.isArray(root[key]) ? (root[key] as unknown[]) : null;
  if (!items) {
    // A reshaped payload is the failure signal, not the status code —
    // throwing is what makes the poller back off instead of publishing
    // an empty state as though WA had gone quiet.
    throw new Error(`wa_${key}: feed returned no ${key} array`);
  }
  const features: WaFeature[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    const f = map(item);
    if (!f) continue;
    if (seen.has(f.properties.guid)) continue;
    seen.add(f.properties.guid);
    features.push(f);
  }
  return { type: 'FeatureCollection', features, count: features.length };
}

export function fetchWaIncidents(): Promise<WaSnapshot> {
  return fetchFeed(INCIDENTS_URL, 'incidents', toWaIncident);
}

export function fetchWaWarnings(): Promise<WaSnapshot> {
  return fetchFeed(WARNINGS_URL, 'warnings', toWaWarning);
}

export default function register(): void {
  // archive_rfs, beside NSW RFS, NT Fire & Rescue, QFD and VIC.
  //
  // 120s matches upstream rather than guessing at it: the CloudFront
  // edge in front of these endpoints reports a ~2 minute Age, so polling
  // faster only re-reads the same cached body.
  registerSource<WaSnapshot>({
    name: 'wa_incident',
    family: 'rfs',
    intervalMs: 120_000,
    fetch: fetchWaIncidents,
  });
  registerSource<WaSnapshot>({
    name: 'wa_warning',
    family: 'rfs',
    intervalMs: 120_000,
    fetch: fetchWaWarnings,
  });
}

export function waIncidentsSnapshot(): WaSnapshot {
  return liveStore.getData<WaSnapshot>('wa_incident') ?? EMPTY;
}

export function waWarningsSnapshot(): WaSnapshot {
  return liveStore.getData<WaSnapshot>('wa_warning') ?? EMPTY;
}
