/**
 * Queensland Fire Department — live incidents and public warnings.
 *
 * Two ArcGIS feature services from the Public Safety Business Agency
 * (org `publicsafetyqld`, owner `qfes_FIMS`), both published as public
 * views:
 *
 *   ESCAD_Current_Incidents_Public   what is burning right now
 *   OCS_Warnings_Points_Public_View  what the public is being told
 *
 * They answer different questions, which is why both are ingested. The
 * warnings feed lists only fires QFD has issued a public warning for (9
 * of the 44 incidents when this was written), so on its own it would
 * show a fraction of the state's activity. The incidents feed has no
 * alert level, so on its own it could not colour anything by severity.
 *
 * PROPERTY NAMES ARE THE RFS ONES, deliberately — `alertLevel`,
 * `fireType`, `status`, `location`. archiveExtract promotes
 * category <- alertLevel and subcategory <- fireType generically, so
 * these get the same facets as NSW and NT for free, and map.html renders
 * all three agencies through one path.
 *
 * TWO THINGS THE DATA DICTATES.
 *
 * 1. The feeds cannot be joined yet. The warnings carry
 *    `MasterIncidentNum`, which matches the incidents'
 *    `Master_Incident_Number` by name, but upstream leaves it null on
 *    every record. So a fire with a warning appears twice — once as an
 *    incident, once as a warning. `warningIncidentKey()` exists so the
 *    join starts working the moment upstream fills the field in.
 *
 * 2. Nothing closes. Both feeds list only current records and drop them
 *    when they end, so there is no closing snapshot to archive the way
 *    NT's `_status: closed` gives one. The last stored state is whatever
 *    it was when the record vanished.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const BASE = 'https://services1.arcgis.com/vkTwD8kHw2woKBqV/arcgis/rest/services';
const INCIDENTS_URL = `${BASE}/ESCAD_Current_Incidents_Public/FeatureServer/0/query`;
const WARNINGS_URL = `${BASE}/OCS_Warnings_Points_Public_View/FeatureServer/0/query`;
const QUERY = '?where=1%3D1&outFields=*&outSR=4326&f=geojson';

/** Which map layer a record belongs on. */
export type QldLayer = 'fire' | 'hazard';

export interface QldFireFeature {
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
    layer: QldLayer;
    /** Set on warnings; the incident they belong to, when upstream says. */
    incidentRef: string;
    /** Warnings only — the public advice text, kept for the detail panel. */
    warningText: string;
    header: string;
    impacts: string;
    shouldDo: string;
    leaveSafely: string;
    furtherInformation: string;
    /** Incidents only — how much is turned out to it. */
    vehiclesAssigned: number | null;
    vehiclesOnRoute: number | null;
    vehiclesOnScene: number | null;
    is_active: boolean;
    agency: 'QFD';
    source: 'qld_fire' | 'qld_warning';
  };
}

export interface QldFireSnapshot {
  type: 'FeatureCollection';
  features: QldFireFeature[];
  count: number;
}

const EMPTY: QldFireSnapshot = { type: 'FeatureCollection', features: [], count: 0 };

function asString(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return '';
}

function tidy(v: unknown): string {
  return asString(v).replace(/\s+/g, ' ').trim();
}

function asInt(v: unknown): number | null {
  const n = typeof v === 'number' ? v : Number(asString(v));
  return Number.isFinite(n) ? n : null;
}

/**
 * These services publish times as epoch MILLISECONDS, not seconds and
 * not a string. Passing one through unconverted would date a record to
 * somewhere around the year 58,000.
 */
export function esriDateToIso(v: unknown): string {
  const ms = typeof v === 'number' ? v : Number(asString(v));
  if (!Number.isFinite(ms) || ms <= 0) return '';
  return new Date(ms).toISOString();
}

function point(geom: unknown): [number, number] | null {
  const g = geom as Record<string, unknown> | undefined;
  if (!g || g['type'] !== 'Point') return null;
  const c = g['coordinates'];
  if (!Array.isArray(c) || c.length < 2) return null;
  const lon = Number(c[0]);
  const lat = Number(c[1]);
  return Number.isFinite(lon) && Number.isFinite(lat) ? [lon, lat] : null;
}

/**
 * Where a QFD record belongs.
 * *
 * A FIRE SERVICE'S OWN JOBS STAY ON ITS OWN LAYER. Crashes, rescues,
 * alarms and assists are what these brigades spend most of their time
 * on, and pushing them to Hazards split one agency's work across two
 * layers — while NSW, whose feed has never been routed at all, kept its
 * MVA/Transport and Medical jobs on Fires the whole time. Only two
 * things are carved out: flooding, which belongs on Floods, and hazmat,
 * which is a different kind of emergency rather than a different kind
 * of fire.
 */
export function qldLayerFor(kind: string): QldLayer {
  // No flood branch: QldLayer has none, because neither QFD feed has
  // ever published one — swift-water jobs come through as RESCUE.
  const k = kind.toUpperCase();
  if (k.includes('HAZMAT') || k.includes('CHEMICAL')) return 'hazard';
  return 'fire';
}

/**
 * The incident a warning refers to, or ''.
 *
 * Upstream leaves `MasterIncidentNum` null on every warning today, so
 * this returns '' in practice and the map shows the warning as its own
 * pin. It is read anyway so the join begins working with no code change
 * once the field is populated.
 */
export function warningIncidentKey(props: Record<string, unknown>): string {
  return asString(props['MasterIncidentNum']);
}

const BLANK = {
  incidentRef: '',
  warningText: '',
  header: '',
  impacts: '',
  shouldDo: '',
  leaveSafely: '',
  furtherInformation: '',
  vehiclesAssigned: null,
  vehiclesOnRoute: null,
  vehiclesOnScene: null,
};

/** One ESCAD incident, or null when it can't be placed. */
export function toIncidentFeature(raw: unknown): QldFireFeature | null {
  if (!raw || typeof raw !== 'object') return null;
  const f = raw as Record<string, unknown>;
  const coords = point(f['geometry']);
  if (!coords) return null;
  const p = (f['properties'] as Record<string, unknown> | undefined) ?? {};

  const id = asString(p['Master_Incident_Number']);
  if (!id) return null;

  const kind = tidy(p['GroupedType']);
  const locality = tidy(p['Locality']);
  const street = tidy(p['Location']);
  const location = [street, locality].filter(Boolean).join(', ');
  const iso = esriDateToIso(p['LastUpdate']) || esriDateToIso(p['Response_Date']);

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: coords },
    properties: {
      ...BLANK,
      // The feed has no headline, so build one the way its own map
      // labels a pin: what it is, and where.
      title: [kind, locality || street].filter(Boolean).join(' - ') || 'QFD incident',
      guid: `qfd:${id}`,
      status: tidy(p['CurrentStatus']),
      location,
      // ESCAD has no alert level; that is what the warnings feed is for.
      // Left blank, these fall into the existing "Not Applicable"
      // bucket alongside RFS incidents without one.
      alertLevel: '',
      fireType: kind,
      responsibleAgency: 'QFD',
      updated: iso ? new Date(iso).toLocaleString('en-AU', { timeZone: 'Australia/Brisbane' }) : '',
      updatedISO: iso,
      layer: qldLayerFor(kind),
      vehiclesAssigned: asInt(p['VehiclesAssigned']),
      vehiclesOnRoute: asInt(p['VehiclesOnRoute']),
      vehiclesOnScene: asInt(p['VehiclesOnScene']),
      is_active: true,
      agency: 'QFD',
      source: 'qld_fire',
    },
  };
}

/** One OCS warning, or null when it can't be placed. */
export function toWarningFeature(raw: unknown): QldFireFeature | null {
  if (!raw || typeof raw !== 'object') return null;
  const f = raw as Record<string, unknown>;
  const coords = point(f['geometry']);
  if (!coords) return null;
  const p = (f['properties'] as Record<string, unknown> | undefined) ?? {};

  const id = asString(p['UniqueID']);
  if (!id) return null;

  const eventType = tidy(p['EventType']);
  const iso = esriDateToIso(p['ModifiedDate']);

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: coords },
    properties: {
      ...BLANK,
      title: tidy(p['WarningTitle']) || 'QFD warning',
      guid: `qfdw:${id}`,
      // CallToAction is the nearest thing to a status here: a warning
      // has no containment state, it tells people what to do.
      status: tidy(p['CallToAction']),
      location: tidy(p['WarningArea']),
      alertLevel: tidy(p['WarningLevel']),
      fireType: eventType,
      responsibleAgency: 'QFD',
      updated: iso ? new Date(iso).toLocaleString('en-AU', { timeZone: 'Australia/Brisbane' }) : '',
      updatedISO: iso,
      layer: qldLayerFor(eventType),
      incidentRef: warningIncidentKey(p),
      // The public advice, kept whole for the detail panel — the same
      // treatment NT's bushfire advice text gets. Newlines collapse so
      // it renders as prose rather than a ragged block.
      warningText: tidy(p['WarningText']),
      header: tidy(p['Header']),
      impacts: tidy(p['Impacts']),
      shouldDo: tidy(p['ShouldDo']),
      leaveSafely: tidy(p['LeaveSafely']),
      furtherInformation: tidy(p['FurtherInformation']),
      is_active: true,
      agency: 'QFD',
      source: 'qld_warning',
    },
  };
}

async function fetchFeed(
  url: string,
  map: (raw: unknown) => QldFireFeature | null,
  label: string,
): Promise<QldFireSnapshot> {
  const data = await fetchJson<unknown>(url + QUERY, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
    timeoutMs: 20_000,
  });
  const root = data as Record<string, unknown> | null;
  const items = root && Array.isArray(root['features']) ? (root['features'] as unknown[]) : null;
  if (!items) {
    // ArcGIS answers a bad query with 200 and an {error:{...}} body, so
    // a missing features array is the failure signal, not the status.
    throw new Error(`${label}: feed returned no features array`);
  }
  const features: QldFireFeature[] = [];
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

export function fetchQldIncidents(): Promise<QldFireSnapshot> {
  return fetchFeed(INCIDENTS_URL, toIncidentFeature, 'qld_fire');
}

export function fetchQldWarnings(): Promise<QldFireSnapshot> {
  return fetchFeed(WARNINGS_URL, toWarningFeature, 'qld_warning');
}

export default function register(): void {
  // archive_rfs, beside NSW RFS and NT Fire & Rescue — the logs page
  // presents all of them under one "Fires" provider.
  registerSource<QldFireSnapshot>({
    name: 'qld_fire',
    family: 'rfs',
    intervalMs: 120_000,
    fetch: fetchQldIncidents,
  });
  registerSource<QldFireSnapshot>({
    name: 'qld_warning',
    family: 'rfs',
    intervalMs: 120_000,
    fetch: fetchQldWarnings,
  });
}

export function qldIncidentsSnapshot(): QldFireSnapshot {
  return liveStore.getData<QldFireSnapshot>('qld_fire') ?? EMPTY;
}

export function qldWarningsSnapshot(): QldFireSnapshot {
  return liveStore.getData<QldFireSnapshot>('qld_warning') ?? EMPTY;
}
