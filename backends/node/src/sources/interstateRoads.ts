/**
 * Other states' road events, from each state's own feed.
 *
 * Live Traffic NSW re-shares SA, QLD, VIC and ACT road events in its
 * aggregate (all-feeds-web.json), and that used to be where these came from,
 * on a feed of their own (/api/traffic/works). Re-shared, they arrived late
 * and incomplete — SA's were cut off at 1,000 records of 1,446 — so each
 * state is now read from its publisher:
 *
 *   QLD  QLDTraffic's GeoJSON feed (events_v2.geojson, what qldtraffic.qld.gov.au
 *        itself loads), CC BY 4.0. Events Transport for NSW supplies to it are
 *        left out: the NSW feeds carry them already.
 *   SA   The Department for Infrastructure and Transport's open data
 *        (roadworks and incidents, CC BY 4.0), the closures and detours on
 *        Traffic SA's own map, and the outback road conditions (the northern
 *        and far-north road report).
 *   ACT  Roads ACT's approved traffic-management plans (planned closures for
 *        works and events) and its unplanned road closures, on the ACT
 *        Government's ArcGIS.
 *   VIC  Transport Victoria's planned and unplanned road disruption APIs when
 *        VIC_TRANSPORT_API_KEY is set; until then, or while Transport Victoria
 *        refuses every request, the Live Traffic aggregate's VIC records.
 *
 * Each record takes the Live Traffic record shape (sources/traffic.ts), with
 * its `state`, its publisher in `source` and the attribution it asks for in
 * `credit`, and its `type` is the hazard feed it belongs on: Incident,
 * Roadwork, Flood or Major Event. /api/traffic/{incidents,roadwork,flood,
 * majorevent} serve the NSW feed with every state's records of that type
 * (interstateRecords), and history files each under the same source as the
 * NSW feed's (archiveByType). Times are ISO 8601, in UTC.
 *
 * A state read with several requests keeps each part's last good answer for
 * a while when that request fails (PART_CARRY_MS), so one refusal does not
 * empty the state off the map; only when every part fails with nothing to
 * fall back on does the poll fail, and the snapshot already held stays.
 */
import { createHash } from 'node:crypto';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { liveStore } from '../store/live.js';
import type { ArchiveRow } from '../store/archive.js';
import { defaultArchiveItems } from '../services/archiveExtract.js';
import { registerSource } from '../services/sourceRegistry.js';
import { fetchJson } from './shared/http.js';
import { htmlToProse } from './shared/prose.js';
import { encodePolyline } from './shared/polyline.js';
import {
  fetchTrafficWorks,
  worksArchiveSource,
  type RoadSource,
  type RoadState,
  type TrafficFeature,
  type TrafficSnapshot,
} from './traffic.js';

/** The hazard feed a record belongs on — its `type`, as the NSW feeds label theirs. */
export type RoadType = 'Incident' | 'Roadwork' | 'Flood' | 'Major Event';

/** Where history files a record of each type: the same source as the NSW feed of that type. */
export const ARCHIVE_SOURCE_BY_TYPE: Readonly<Record<RoadType, string>> = {
  Incident: 'traffic_incident',
  Roadwork: 'traffic_roadwork',
  Flood: 'traffic_flood',
  'Major Event': 'traffic_majorevent',
};

const TYPE_BY_ARCHIVE_SOURCE: Readonly<Record<string, RoadType>> = Object.fromEntries(
  Object.entries(ARCHIVE_SOURCE_BY_TYPE).map(([t, s]) => [s, t as RoadType]),
);

/** One LiveStore snapshot per state. */
export const INTERSTATE_STORES: Readonly<Record<Exclude<RoadState, 'NSW'>, string>> = {
  QLD: 'roads_qld',
  SA: 'roads_sa',
  VIC: 'roads_vic',
  ACT: 'roads_act',
};

const QLD_CREDIT = 'QLDTraffic (Queensland Department of Transport and Main Roads)';
const SA_CREDIT = '© Copyright Government of South Australia (Department for Infrastructure and Transport)';
const ACT_CREDIT = 'ACT Government (Roads ACT)';
const VIC_CREDIT = 'Department of Transport and Planning, Victoria';

// ---------------------------------------------------------------------------
// The record shape
// ---------------------------------------------------------------------------

export interface RoadInput {
  id: string;
  lon: number;
  lat: number;
  /** The road the record covers, as [longitude, latitude] lines. */
  lines?: number[][][];
  type: RoadType;
  mainCategory: string;
  subCategory?: string;
  incidentType?: string;
  incidentKind?: string;
  title: string;
  roads?: string;
  otherAdvice?: string;
  adviceB?: string;
  adviceC?: string;
  affectedDirection?: string;
  speedLimit?: string;
  expectedDelay?: string;
  created?: string;
  lastUpdated?: string;
  start?: string;
  end?: string;
  isMajor?: boolean;
  region?: string;
  crossStreet?: string;
  secondLocation?: string;
  locationQualifier?: string;
  webLinks?: unknown[];
  orgName?: string;
  orgWebsite?: string;
  state: RoadState;
  source: RoadSource;
  credit: string;
}

/** A record in the Live Traffic shape. */
export function roadRecord(r: RoadInput): TrafficFeature {
  const lines = (r.lines ?? []).map((l) => encodePolyline(l)).filter(Boolean);
  const incidentType = r.incidentType ?? '';
  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [r.lon, r.lat] },
    properties: {
      id: r.id,
      type: r.type,
      incidentType,
      mainCategory: r.mainCategory,
      subCategory: r.subCategory ?? '',
      subcategory: incidentType || r.mainCategory,
      incidentKind: r.incidentKind ?? '',
      title: r.title,
      headline: '',
      displayName: r.title,
      subtitle: '',
      otherAdvice: r.otherAdvice ?? '',
      adviceB: r.adviceB ?? '',
      roads: r.roads ?? '',
      affectedDirection: r.affectedDirection ?? '',
      impactedLanes: [],
      speedLimit: r.speedLimit ?? '',
      expectedDelay: r.expectedDelay ?? '',
      diversions: '',
      encodedPolyline: lines[0] ?? '',
      encodedPolylines: lines.map((coords) => ({ coords, direction: '' })),
      created: r.created ?? r.start ?? '',
      lastUpdated: r.lastUpdated ?? '',
      start: r.start ?? '',
      end: r.end ?? '',
      isEnded: false,
      isMajor: r.isMajor ?? false,
      arrangement: '',
      periods: [],
      isLocalRoad: '',
      region: r.region ?? '',
      crossStreet: r.crossStreet ?? '',
      secondLocation: r.secondLocation ?? '',
      locationQualifier: r.locationQualifier ?? '',
      queueLength: null,
      duration: '',
      adviceC: r.adviceC ?? '',
      webLinks: r.webLinks ?? [],
      orgName: r.orgName ?? '',
      orgContact: '',
      orgEmail: '',
      orgWebsite: r.orgWebsite ?? '',
      state: r.state,
      source: r.source,
      credit: r.credit,
    },
  };
}

// ---------------------------------------------------------------------------
// Small readers
// ---------------------------------------------------------------------------

type Obj = Record<string, unknown>;

const obj = (v: unknown): Obj => (v && typeof v === 'object' && !Array.isArray(v) ? (v as Obj) : {});
const str = (v: unknown): string => (typeof v === 'string' ? v.trim() : typeof v === 'number' ? String(v) : '');
const num = (v: unknown): number | null => {
  const n = typeof v === 'number' ? v : typeof v === 'string' && v.trim() !== '' ? Number(v) : NaN;
  return Number.isFinite(n) ? n : null;
};

/** An ArcGIS attribute by name, whatever case the layer spells it in. */
function attr(a: Obj, name: string): unknown {
  if (name in a) return a[name];
  const up = name.toUpperCase();
  if (up in a) return a[up];
  const low = name.toLowerCase();
  if (low in a) return a[low];
  return undefined;
}

/** A time as UTC ISO 8601: epoch milliseconds, or text Date can read with its zone. */
export function isoTime(v: unknown): string {
  if (v === null || v === undefined || v === '') return '';
  const n = num(v);
  if (n !== null && n > 1e11) return new Date(n).toISOString();
  if (typeof v === 'string') {
    const ms = Date.parse(v);
    if (Number.isFinite(ms)) return new Date(ms).toISOString();
  }
  return '';
}

/** "MOUNT BARKER" → "Mount Barker"; text already in mixed case is left alone. */
function titleCase(s: string): string {
  if (!s || s !== s.toUpperCase()) return s;
  return s.toLowerCase().replace(/\b([a-z])/g, (m) => m.toUpperCase());
}

const joinText = (...parts: string[]): string => parts.map((p) => p.trim()).filter(Boolean).join(' ');

function isPoint(p: unknown): p is number[] {
  return Array.isArray(p) && p.length >= 2 && Number.isFinite(p[0]) && Number.isFinite(p[1]);
}

/** Every point and line a GeoJSON geometry holds, Polygon rings as lines. */
function geoParts(g: unknown): { points: number[][]; lines: number[][][] } {
  const out = { points: [] as number[][], lines: [] as number[][][] };
  const walk = (geom: unknown): void => {
    const o = obj(geom);
    const c = o['coordinates'];
    switch (str(o['type']).toLowerCase()) {
      case 'point':
        if (isPoint(c)) out.points.push(c);
        break;
      case 'multipoint':
        if (Array.isArray(c)) out.points.push(...c.filter(isPoint));
        break;
      case 'linestring':
        if (Array.isArray(c)) out.lines.push(c.filter(isPoint));
        break;
      case 'multilinestring':
      case 'polygon':
        if (Array.isArray(c)) for (const l of c) if (Array.isArray(l)) out.lines.push(l.filter(isPoint));
        break;
      case 'multipolygon':
        if (Array.isArray(c)) for (const poly of c) if (Array.isArray(poly)) for (const l of poly) if (Array.isArray(l)) out.lines.push(l.filter(isPoint));
        break;
      case 'geometrycollection':
        for (const sub of Array.isArray(o['geometries']) ? o['geometries'] : []) walk(sub);
        break;
    }
  };
  walk(g);
  out.lines = out.lines.filter((l) => l.length >= 2);
  return out;
}

/** An ArcGIS geometry (outSR 4326) as points and lines. */
function esriParts(g: unknown): { points: number[][]; lines: number[][][] } {
  const o = obj(g);
  const points: number[][] = [];
  const x = num(o['x']);
  const y = num(o['y']);
  if (x !== null && y !== null) points.push([x, y]);
  const lines = (Array.isArray(o['paths']) ? o['paths'] : [])
    .map((p) => (Array.isArray(p) ? p.filter(isPoint) : []))
    .filter((l) => l.length >= 2);
  return { points, lines };
}

/** Where to put the pin: the first point, else the middle of the longest line. */
function pinOf(parts: { points: number[][]; lines: number[][][] }): [number, number] | null {
  const p = parts.points[0];
  if (p) return [p[0]!, p[1]!];
  let best: number[][] | null = null;
  let bestLen = -1;
  for (const l of parts.lines) {
    let len = 0;
    for (let i = 1; i < l.length; i++) len += Math.hypot(l[i]![0]! - l[i - 1]![0]!, l[i]![1]! - l[i - 1]![1]!);
    if (len > bestLen) {
      bestLen = len;
      best = l;
    }
  }
  const mid = best?.[Math.floor(best.length / 2)];
  return mid ? [mid[0]!, mid[1]!] : null;
}

const shortHash = (s: string): string => createHash('sha1').update(s).digest('hex').slice(0, 16);

/** Over: an end time that has passed. Traffic SA's map layer keeps closures months after they end. */
const ended = (endIso: string, now: number): boolean => endIso !== '' && Date.parse(endIso) < now;

// ---------------------------------------------------------------------------
// QLD — QLDTraffic
// ---------------------------------------------------------------------------

export const QLD_EVENTS_URL = 'https://data.qldtraffic.qld.gov.au/events_v2.geojson';

const QLD_TYPE: Readonly<Record<string, RoadType>> = {
  Roadworks: 'Roadwork',
  Flooding: 'Flood',
  'Special event': 'Major Event',
};
const QLD_INCIDENT_TYPE: Readonly<Record<string, string>> = {
  Crash: 'CRASH',
  Hazard: 'HAZARD',
  Flooding: 'FLOODING',
  Roadworks: 'ROADWORK',
  'Special event': 'SPECIAL EVENT',
};

/** QLDTraffic's events as road records. */
export function qldRecords(data: unknown, now = Date.now()): TrafficFeature[] {
  const out: TrafficFeature[] = [];
  for (const f of Array.isArray(obj(data)['features']) ? (obj(data)['features'] as unknown[]) : []) {
    const p = obj(obj(f)['properties']);
    const id = str(p['id']);
    if (!id || str(p['status']) !== 'Published') continue;
    if (ended(isoTime(obj(p['duration'])['end']), now)) continue;
    const src = obj(p['source']);
    const providedBy = str(src['provided_by']);
    // Supplied to QLDTraffic by Transport for NSW: the NSW feeds have it.
    if (/transport for nsw/i.test(providedBy)) continue;
    const parts = geoParts(obj(f)['geometry']);
    const pin = pinOf(parts);
    if (!pin) continue;

    const eventType = str(p['event_type']);
    const subtype = str(p['event_subtype']);
    const sub = subtype && subtype !== 'N/A' ? subtype : '';
    const road = obj(p['road_summary']);
    const impact = obj(p['impact']);
    const duration = obj(p['duration']);
    const council = providedBy && !/transport and main roads/i.test(providedBy);
    const webLink = str(p['web_link']);
    out.push(
      roadRecord({
        id: `qld:${id}`,
        lon: pin[0],
        lat: pin[1],
        lines: parts.lines,
        type: QLD_TYPE[eventType] ?? 'Incident',
        mainCategory: eventType,
        subCategory: sub,
        incidentType: QLD_INCIDENT_TYPE[eventType] ?? '',
        incidentKind: eventType === 'Roadworks' || eventType === 'Special event' ? 'Planned' : 'Unplanned',
        title: sub ? `${eventType}: ${sub}` : eventType,
        roads: joinText(str(road['road_name']), str(road['locality'])),
        otherAdvice: htmlToProse(p['description']),
        adviceB: str(p['advice']),
        adviceC: htmlToProse(p['information']),
        affectedDirection: str(impact['direction']),
        expectedDelay: str(impact['delay']),
        created: isoTime(p['published']) || isoTime(duration['start']),
        lastUpdated: isoTime(p['last_updated']),
        start: isoTime(duration['start']),
        end: isoTime(duration['end']),
        isMajor: str(p['event_priority']) === 'High',
        region: str(road['district']),
        webLinks: webLink ? [{ url: webLink }] : [],
        orgName: council ? providedBy : '',
        orgWebsite: council ? str(src['provided_by_url']) : '',
        state: 'QLD',
        source: 'qldtraffic',
        credit: council ? `${providedBy}, via ${QLD_CREDIT}` : QLD_CREDIT,
      }),
    );
  }
  return out;
}

// ---------------------------------------------------------------------------
// SA — Department for Infrastructure and Transport
// ---------------------------------------------------------------------------

/**
 * Open data: roadworks (and a few incidents) across SA. Pages of 1,000.
 *
 * The layer is reloaded whole every minute or so, and each load numbers its
 * records afresh (ROADWORKS_AND_INCIDENTS_ID, GIS_LINK_ID, ESRI_OID) in an
 * order of its own — seen 2026-11-02: two loads 27 seconds apart, every id
 * new, every record the same. So a record is known by what it says
 * (saWorksRecords' `idFrom: 'content'`), and the pages are read in an order
 * of what the records say (SA_ROADWORKS_ORDER), so a reload between two
 * pages does not move records across the page boundary.
 */
export const SA_ROADWORKS_URL = 'https://maps.sa.gov.au/arcgis/rest/services/DPTIExtTransport/TrafficSAOpenData/MapServer/0/query';
export const SA_ROADWORKS_ORDER = 'START_DATE,LATITUDE,LONGITUDE,REC_TYPE,LOCAL_ROAD,SIDE_STREET,SUBURB,DESCRIPTION,END_DATE';
/** Open data: current incidents. */
export const SA_INCIDENTS_URL =
  'https://dit-cms-prod.geohub.sa.gov.au/arcgis/rest/services/TrafficSATime_OpenData/TrafficSATime_OpenData/MapServer/0/query';
/** Traffic SA's map: road closures and detours across the state, as lines. */
export const SA_CLOSURES_URL =
  'https://dit-cms-prod.geohub.sa.gov.au/arcgis/rest/services/TrafficSATime/TrafficSATime/MapServer/4/query';
/** The northern and far-north road report: outback road conditions, as lines. */
export const SA_OUTBACK_URL = 'https://maps.sa.gov.au/arcgis/rest/services/DPTIExtTransport/FNRR2/MapServer/0/query';

/** One ArcGIS query URL: everything, as WGS84. */
export function arcgisQuery(base: string, extra: Record<string, string> = {}): string {
  const q = new URLSearchParams({
    where: '1=1',
    outFields: '*',
    returnGeometry: 'true',
    outSR: '4326',
    f: 'json',
    ...extra,
  });
  return `${base}?${q.toString()}`;
}

/** An ArcGIS query's features, following exceededTransferLimit page by page. */
async function arcgisFeatures(base: string, extra: Record<string, string> = {}, pageSize = 1000, maxPages = 10): Promise<Obj[]> {
  const out: Obj[] = [];
  for (let page = 0; page < maxPages; page++) {
    const body = obj(
      await fetchJson<unknown>(
        arcgisQuery(base, { ...extra, resultOffset: String(page * pageSize), resultRecordCount: String(pageSize) }),
        { timeoutMs: 30_000 },
      ),
    );
    if (body['error']) throw new Error(`ArcGIS error: ${JSON.stringify(body['error']).slice(0, 200)}`);
    const feats = Array.isArray(body['features']) ? (body['features'] as unknown[]).map(obj) : [];
    out.push(...feats);
    if (body['exceededTransferLimit'] !== true || feats.length === 0) break;
  }
  return out;
}

/** SA's roadworks and incidents layers (the same fields, in either case). */
/**
 * What a roadworks-layer record says about where and what it is — not its
 * end, lanes or speed limit, which SA may change while the works go on — as
 * the base of an id that survives SA's reloads.
 */
const SA_IDENTITY = ['rec_type', 'latitude', 'longitude', 'start_date', 'local_road', 'side_street', 'suburb', 'traffic_dir', 'description'];
/** Numbers SA reissues on every load: never part of an id or of a record's content. */
const SA_RELOAD_NUMBERS = new Set(['roadworks_and_incidents_id', 'gis_link_id', 'esri_oid', 'objectid']);

/**
 * Ids for the roadworks layer, one per feature, from what each says. Records
 * alike in SA_IDENTITY (a handful — the same works listed twice, or differing
 * only in their end) are told apart by the rest of what they say, in a fixed
 * order, as `~2`, `~3`.
 */
export function saContentIds(features: readonly Obj[]): string[] {
  const rows = features.map((f) => {
    const a = obj(f['attributes']);
    const all = Object.keys(a)
      .filter((k) => !SA_RELOAD_NUMBERS.has(k.toLowerCase()))
      .sort((x, y) => x.toLowerCase().localeCompare(y.toLowerCase()))
      .map((k) => `${k.toLowerCase()}=${str(a[k])}`)
      .join('|');
    return { base: shortHash(SA_IDENTITY.map((k) => str(attr(a, k))).join('|')), all };
  });
  const groups = new Map<string, number[]>();
  rows.forEach((r, i) => (groups.get(r.base) ?? groups.set(r.base, []).get(r.base)!).push(i));
  const ids: string[] = new Array(features.length);
  for (const [base, idx] of groups) {
    idx.sort((x, y) => (rows[x]!.all < rows[y]!.all ? -1 : rows[x]!.all > rows[y]!.all ? 1 : 0));
    idx.forEach((i, n) => (ids[i] = n === 0 ? `rw:${base}` : `rw:${base}~${n + 1}`));
  }
  return ids;
}

export function saWorksRecords(
  features: readonly Obj[],
  now = Date.now(),
  idFrom: 'field' | 'content' = 'field',
): TrafficFeature[] {
  const out: TrafficFeature[] = [];
  const contentIds = idFrom === 'content' ? saContentIds(features) : null;
  for (const [i, f] of features.entries()) {
    const a = obj(f['attributes']);
    const id = contentIds ? contentIds[i]! : str(attr(a, 'roadworks_and_incidents_id')) || str(attr(a, 'incident_id'));
    const lat = num(attr(a, 'latitude')) ?? num(obj(f['geometry'])['y']);
    const lon = num(attr(a, 'longitude')) ?? num(obj(f['geometry'])['x']);
    if (!id || lat === null || lon === null) continue;
    if (ended(isoTime(attr(a, 'end_date')), now)) continue;
    const recType = str(attr(a, 'rec_type')).toUpperCase();
    const isWorks = recType.includes('ROADWORK');
    const lanes = num(attr(a, 'no_lanes_closed'));
    const dir = str(attr(a, 'traffic_dir'));
    out.push(
      roadRecord({
        id: `sa:${id}`,
        lon,
        lat,
        type: isWorks ? 'Roadwork' : 'Incident',
        mainCategory: titleCase(recType) || 'Roadworks',
        incidentType: isWorks ? 'ROADWORK' : '',
        incidentKind: isWorks ? 'Planned' : 'Unplanned',
        title: htmlToProse(attr(a, 'description')) || titleCase(recType),
        roads: joinText(str(attr(a, 'local_road')), titleCase(str(attr(a, 'suburb')))),
        crossStreet: str(attr(a, 'side_street')),
        otherAdvice: lanes ? `${lanes} lane${lanes === 1 ? '' : 's'} closed` : '',
        affectedDirection: dir === '[]' ? '' : dir.replace(/[[\]"]/g, ''),
        speedLimit: str(attr(a, 'speed_limit')),
        expectedDelay: str(attr(a, 'highest_delay')),
        start: isoTime(attr(a, 'start_date')),
        end: isoTime(attr(a, 'end_date')),
        state: 'SA',
        source: 'traffic_sa',
        credit: SA_CREDIT,
      }),
    );
  }
  return out;
}

/** Traffic SA's closures and detours still in force. */
export function saClosureRecords(features: readonly Obj[], now = Date.now()): TrafficFeature[] {
  const out: TrafficFeature[] = [];
  for (const f of features) {
    const a = obj(f['attributes']);
    const id = str(attr(a, 'location_id')).replace(/[{}]/g, '') || str(attr(a, 'ESRI_OID'));
    const parts = esriParts(f['geometry']);
    const pin = pinOf(parts);
    if (!id || !pin) continue;
    if (ended(isoTime(attr(a, 'end_date')), now)) continue;
    const detour = str(attr(a, 'plot_type')).toUpperCase().includes('DETOUR');
    const road = titleCase(str(attr(a, 'local_road_name')));
    const from = str(attr(a, 'start_side_street_name1'));
    const to = str(attr(a, 'end_side_street_name1'));
    out.push(
      roadRecord({
        id: `sa:closure:${id}`,
        lon: pin[0],
        lat: pin[1],
        lines: parts.lines,
        type: 'Incident',
        mainCategory: detour ? 'Detour' : 'Road closure',
        incidentType: detour ? '' : 'ROAD CLOSURE',
        title: htmlToProse(attr(a, 'plot_details')) || `${road || 'Road'} ${detour ? 'detour' : 'closed'}`,
        roads: joinText(road, titleCase(str(attr(a, 'start_suburb')))),
        crossStreet: titleCase(from),
        secondLocation: titleCase(to),
        locationQualifier: from && to ? 'between' : '',
        start: isoTime(attr(a, 'start_date')),
        end: isoTime(attr(a, 'end_date')),
        state: 'SA',
        source: 'traffic_sa',
        credit: SA_CREDIT,
      }),
    );
  }
  return out;
}

/** The outback road report: each section not simply open. */
export function saOutbackRecords(features: readonly Obj[]): TrafficFeature[] {
  const out: TrafficFeature[] = [];
  for (const f of features) {
    const a = obj(f['attributes']);
    const section = str(attr(a, 'ROAD_SECTION'));
    const code = num(attr(a, 'CODE'));
    const parts = esriParts(f['geometry']);
    const pin = pinOf(parts);
    if (!section || code === 1 || !pin) continue;
    const status = str(attr(a, 'DESCRIPTION'));
    out.push(
      roadRecord({
        id: `sa:outback:${shortHash(section)}`,
        lon: pin[0],
        lat: pin[1],
        lines: parts.lines,
        type: 'Incident',
        mainCategory: 'Outback road conditions',
        subCategory: status,
        incidentType: code === 5 ? 'ROAD CLOSURE' : '',
        title: status ? `${section}: ${status}` : section,
        roads: section,
        otherAdvice: htmlToProse(attr(a, 'COMMENTS')),
        region: str(attr(a, 'AREA_NAME')),
        state: 'SA',
        source: 'traffic_sa',
        credit: SA_CREDIT,
      }),
    );
  }
  return out;
}

// ---------------------------------------------------------------------------
// ACT — Roads ACT
// ---------------------------------------------------------------------------

const ACT_ARCGIS = 'https://services1.arcgis.com/E5n4f1VY84i0xSjy/arcgis/rest/services';
/** Approved traffic-management plans: planned closures for works and events. */
export const ACT_PLANS_URL = `${ACT_ARCGIS}/Road_Closures_public_view_HISTORICAL/FeatureServer/0/query`;
/** Unplanned road closures. */
export const ACT_UNPLANNED_URL = `${ACT_ARCGIS}/Public_Temporary_Road_Closures/FeatureServer/0/query`;

const ACT_KIND: Readonly<Record<string, { type: RoadType; label: string }>> = {
  roadWorks: { type: 'Roadwork', label: 'Roadworks' },
  buildingConstruction: { type: 'Roadwork', label: 'Building construction' },
  utilities: { type: 'Roadwork', label: 'Utilities' },
  lightRail: { type: 'Roadwork', label: 'Light rail' },
  telecommunications: { type: 'Roadwork', label: 'Telecommunications' },
  specialEvent: { type: 'Major Event', label: 'Special event' },
};

/** A plan's title without its permit number ("TTE22-ACT-2184 - …"). */
const actTitle = (t: string): string => t.replace(/^TT\w*-ACT-\d+\s*-\s*/i, '').trim() || t;

/** Roads ACT's closures still in force or yet to start. `unplanned` for the unplanned layer. */
export function actRecords(features: readonly Obj[], unplanned: boolean, now = Date.now()): TrafficFeature[] {
  const out: TrafficFeature[] = [];
  for (const f of features) {
    const a = obj(f['attributes']);
    const id = str(attr(a, 'globalid')).replace(/[{}]/g, '');
    const g = obj(f['geometry']);
    const lon = num(g['x']);
    const lat = num(g['y']);
    if (!id || lon === null || lat === null) continue;
    const end = num(attr(a, 'endTimeClosure'));
    if (end !== null && end < now) continue;
    const rawType = str(attr(a, 'type'));
    const kind = ACT_KIND[rawType];
    const suburb = str(attr(a, 'suburb1'));
    out.push(
      roadRecord({
        id: `act:${unplanned ? 'closure:' : ''}${id}`,
        lon,
        lat,
        type: unplanned ? 'Incident' : (kind?.type ?? 'Incident'),
        mainCategory: unplanned ? 'Unplanned road closure' : (kind?.label ?? 'Road closure'),
        subCategory: unplanned ? (kind?.label ?? '') : '',
        incidentType: unplanned ? 'ROAD CLOSURE' : kind?.type === 'Roadwork' ? 'ROADWORK' : '',
        incidentKind: unplanned ? 'Unplanned' : 'Planned',
        title: actTitle(str(attr(a, 'projectTitle'))) || 'Road closure',
        roads: str(attr(a, 'roadsClosed')),
        otherAdvice: htmlToProse(attr(a, 'reasonRoadClosure')) || htmlToProse(attr(a, 'describeActivity')),
        start: isoTime(attr(a, 'startTimeClosure')),
        end: isoTime(attr(a, 'endTimeClosure')),
        region: suburb && suburb !== 'OTHER' ? titleCase(suburb) : '',
        state: 'ACT',
        source: 'roads_act',
        credit: ACT_CREDIT,
      }),
    );
  }
  return out;
}

// ---------------------------------------------------------------------------
// VIC — Transport Victoria
// ---------------------------------------------------------------------------

const VIC_API = 'https://api.opendata.transport.vic.gov.au';
export const VIC_UNPLANNED_URL = `${VIC_API}/api/opendata/roads/disruptions/unplanned/v3`;
export const VIC_PLANNED_URL = `${VIC_API}/opendata/roads/disruptions/planned/v1/`;
/** Planned disruptions refresh upstream every 30 minutes: read them no more often than this. */
const VIC_PLANNED_EVERY_MS = 10 * 60_000;

/** The road type a Victorian event type reads as. */
function vicType(eventType: string, planned: boolean): RoadType {
  const t = eventType.toLowerCase();
  if (t.includes('flood')) return 'Flood';
  if (/event/.test(t)) return 'Major Event';
  if (/roadwork|works|maintenance|construction/.test(t)) return 'Roadwork';
  return planned ? 'Roadwork' : 'Incident';
}

const VIC_DONE = /^(inactive|cleared|resolved|completed|cancelled|closed)$/i;

/** Transport Victoria's disruptions, planned or unplanned, as road records. */
export function vicRecords(features: readonly unknown[], planned: boolean, now = Date.now()): TrafficFeature[] {
  const out: TrafficFeature[] = [];
  for (const f of features) {
    const p = obj(obj(f)['properties']);
    const id = str(p['id']) || str(p['eventId']);
    if (!id) continue;
    if (VIC_DONE.test(str(p['status'])) || VIC_DONE.test(str(p['eventLocationStatus']))) continue;
    const duration = obj(p['duration']);
    const end = isoTime(duration['end']) || isoTime(p['endTime']);
    if (end && Date.parse(end) < now) continue;
    const parts = geoParts(obj(f)['geometry']);
    const pin = pinOf(parts);
    if (!pin) continue;
    const ref = obj(p['reference']);
    const impact = obj(p['impact']);
    const eventType = str(p['eventType']);
    const sub = str(p['eventSubType']) || str(p['eventSubtype']);
    const type = vicType(eventType, planned);
    const road = str(p['closedRoadName']) || str(ref['localRoadName']);
    const locality = str(p['startIntersectionLocality']) || str(ref['startIntersectionLocality']);
    const link = str(p['weblinkURL']);
    out.push(
      roadRecord({
        id: `vic:${planned ? 'planned' : 'unplanned'}:${id}`,
        lon: pin[0],
        lat: pin[1],
        lines: parts.lines,
        type,
        mainCategory: eventType || (planned ? 'Planned disruption' : 'Unplanned disruption'),
        subCategory: sub,
        incidentType: type === 'Flood' ? 'FLOODING' : type === 'Roadwork' ? 'ROADWORK' : /crash|collision/i.test(eventType) ? 'CRASH' : '',
        incidentKind: planned ? 'Planned' : 'Unplanned',
        title: sub ? `${eventType || 'Disruption'}: ${sub}` : eventType || 'Disruption',
        roads: joinText(road, locality),
        crossStreet: str(p['startIntersectionRoadName']) || str(ref['startIntersectionRoadName']),
        otherAdvice: htmlToProse(p['description']),
        adviceB: str(p['eventDueTo']),
        affectedDirection: str(impact['direction']),
        expectedDelay: str(impact['delay']),
        speedLimit: str(impact['speedLimitOnSite']),
        created: isoTime(p['created']) || isoTime(duration['start']),
        lastUpdated: isoTime(p['lastUpdated']),
        start: isoTime(duration['start']) || isoTime(p['created']),
        end,
        region: str(p['localGovernmentArea']) || str(ref['localGovernmentArea']),
        webLinks: link ? [{ url: link }] : [],
        state: 'VIC',
        source: 'transport_vic',
        credit: VIC_CREDIT,
      }),
    );
  }
  return out;
}

function vicHeaders(key: string): Record<string, string> {
  // The portal documents KeyID; the planned API's own spec still names the
  // older subscription header. Both carry the same key, to the same host.
  return { KeyID: key, 'Ocp-Apim-Subscription-Key': key };
}

async function vicUnplanned(key: string): Promise<TrafficFeature[]> {
  const features: unknown[] = [];
  for (let page = 1; page <= 20; page++) {
    const body = obj(await fetchJson<unknown>(`${VIC_UNPLANNED_URL}?page=${page}&limit=500`, { headers: vicHeaders(key), timeoutMs: 30_000 }));
    const fc = obj(body['data']);
    const batch = Array.isArray(fc['features']) ? (fc['features'] as unknown[]) : [];
    features.push(...batch);
    const pages = num(obj(body['meta'])['total_pages']) ?? 1;
    if (page >= pages || batch.length === 0) break;
  }
  return vicRecords(features, false);
}

let _vicPlanned: { at: number; records: TrafficFeature[] } | null = null;

async function vicPlanned(key: string): Promise<TrafficFeature[]> {
  if (_vicPlanned && Date.now() - _vicPlanned.at < VIC_PLANNED_EVERY_MS) return _vicPlanned.records;
  const features: unknown[] = [];
  let token = '';
  for (let page = 0; page < 50; page++) {
    const headers: Record<string, string> = { ...vicHeaders(key), ...(token ? { NextPageToken: token } : {}) };
    const body = obj(await fetchJson<unknown>(`${VIC_PLANNED_URL}?format=GeoJson`, { headers, timeoutMs: 30_000 }));
    if (Array.isArray(body['features'])) features.push(...(body['features'] as unknown[]));
    const next = obj(body['nextPageDetails']);
    token = str(next['nextPageToken']);
    if (next['hasMoreRecords'] !== true || !token) break;
  }
  const records = vicRecords(features, true);
  _vicPlanned = { at: Date.now(), records };
  return records;
}

const AGGREGATE_TYPE: Readonly<Record<string, RoadType>> = TYPE_BY_ARCHIVE_SOURCE;

/** VIC's records as Live Traffic NSW re-shares them, typed by category. */
async function vicFromAggregate(): Promise<TrafficFeature[]> {
  const snap = await fetchTrafficWorks(new Set(['vicRoadInfo']));
  return snap.features.map((f) => {
    f.properties.type = AGGREGATE_TYPE[worksArchiveSource(f.properties.mainCategory)] ?? 'Incident';
    f.properties.state = 'VIC';
    return f;
  });
}

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

/** How long a part's last good answer stands in for a failed request. */
export const PART_CARRY_MS = 30 * 60_000;
const _lastGood = new Map<string, { at: number; records: TrafficFeature[] }>();

/** Forget every part's last good answer — tests need a clean slate. */
export function _resetInterstateMemory(): void {
  _lastGood.clear();
  _vicPlanned = null;
  _vicOnAggregate = false;
}

/**
 * Run a state's requests together. A part that fails answers with its last
 * good records while those are under PART_CARRY_MS old; if every part fails
 * with nothing to fall back on, the poll fails and the snapshot held stays.
 */
export async function collectParts(
  state: string,
  parts: ReadonlyArray<[name: string, run: () => Promise<TrafficFeature[]>]>,
): Promise<TrafficSnapshot> {
  const results = await Promise.allSettled(parts.map(([, run]) => run()));
  const features: TrafficFeature[] = [];
  let anyAnswer = false;
  let firstError: unknown = null;
  const now = Date.now();
  results.forEach((r, i) => {
    const key = `${state}:${parts[i]![0]}`;
    if (r.status === 'fulfilled') {
      _lastGood.set(key, { at: now, records: r.value });
      features.push(...r.value);
      anyAnswer = true;
      return;
    }
    firstError ??= r.reason;
    const kept = _lastGood.get(key);
    if (kept && now - kept.at < PART_CARRY_MS) {
      log.warn({ part: key, kept: kept.records.length, err: (r.reason as Error)?.message }, 'interstate roads: a part failed — keeping its last good records');
      features.push(...kept.records);
      anyAnswer = true;
    } else {
      log.warn({ part: key, err: (r.reason as Error)?.message }, 'interstate roads: a part failed with nothing to keep');
    }
  });
  if (!anyAnswer) throw firstError instanceof Error ? firstError : new Error(`${state}: every part failed`);
  // One record per id: SA's roadworks layer carries an incident its incidents layer does too.
  const byId = new Map<string, TrafficFeature>();
  for (const f of features) if (!byId.has(f.properties.id)) byId.set(f.properties.id, f);
  const unique = [...byId.values()];
  return { type: 'FeatureCollection', features: unique, count: unique.length };
}

export function fetchQldRoads(): Promise<TrafficSnapshot> {
  return collectParts('QLD', [['events', async () => qldRecords(await fetchJson<unknown>(QLD_EVENTS_URL, { timeoutMs: 30_000 }))]]);
}

export function fetchSaRoads(): Promise<TrafficSnapshot> {
  return collectParts('SA', [
    [
      'roadworks',
      async () =>
        saWorksRecords(
          await arcgisFeatures(SA_ROADWORKS_URL, { returnGeometry: 'false', orderByFields: SA_ROADWORKS_ORDER }),
          Date.now(),
          'content',
        ),
    ],
    ['incidents', async () => saWorksRecords(await arcgisFeatures(SA_INCIDENTS_URL, { returnGeometry: 'false' }))],
    ['closures', async () => saClosureRecords(await arcgisFeatures(SA_CLOSURES_URL, {}, 2000, 3))],
    ['outback', async () => saOutbackRecords(await arcgisFeatures(SA_OUTBACK_URL, { where: 'CODE<>1' }, 1000, 3))],
  ]);
}

export function fetchActRoads(): Promise<TrafficSnapshot> {
  return collectParts('ACT', [
    ['plans', async () => actRecords(await arcgisFeatures(ACT_PLANS_URL), false)],
    ['unplanned', async () => actRecords(await arcgisFeatures(ACT_UNPLANNED_URL), true)],
  ]);
}

/** Whether VIC is being read from the aggregate because the official API refused — logged when it changes. */
let _vicOnAggregate = false;

export async function fetchVicRoads(): Promise<TrafficSnapshot> {
  const key = config.VIC_TRANSPORT_API_KEY;
  if (!key) return collectParts('VIC', [['aggregate', vicFromAggregate]]);
  try {
    const snap = await collectParts('VIC', [
      ['unplanned', () => vicUnplanned(key)],
      ['planned', () => vicPlanned(key)],
    ]);
    if (_vicOnAggregate) log.info('interstate roads: VIC is read from Transport Victoria again');
    _vicOnAggregate = false;
    return snap;
  } catch (err) {
    // A key Transport Victoria refuses (a wrong one, or one it has revoked)
    // would otherwise hold VIC at its last snapshot indefinitely.
    if (!_vicOnAggregate) {
      log.warn(
        { err: (err as Error).message },
        'interstate roads: Transport Victoria refused every request — reading VIC from the Live Traffic aggregate until it answers (check VIC_TRANSPORT_API_KEY: the "API Key" under Subscription Keys on the portal profile)',
      );
    }
    _vicOnAggregate = true;
    return collectParts('VIC', [['aggregate', vicFromAggregate]]);
  }
}

/** Every state's records of one type, for the NSW feed of that type to carry too. */
export function interstateRecords(type: RoadType): TrafficFeature[] {
  const out: TrafficFeature[] = [];
  for (const store of Object.values(INTERSTATE_STORES)) {
    const snap = liveStore.getData<TrafficSnapshot>(store);
    for (const f of snap?.features ?? []) if (f.properties.type === type) out.push(f);
  }
  return out;
}

/** History: each record under the source of the NSW feed of its type. */
export function archiveByType(data: unknown, fetched_at: number, source: string): ArchiveRow[] {
  return defaultArchiveItems(source, data, fetched_at).map((row) => {
    const t = str(obj(row.data)['type']) as RoadType;
    return { ...row, source: ARCHIVE_SOURCE_BY_TYPE[t] ?? 'traffic_incident' };
  });
}

export default function register(): void {
  const sources: Array<[name: string, intervalMs: number, fetch: () => Promise<TrafficSnapshot>]> = [
    [INTERSTATE_STORES.QLD, 120_000, fetchQldRoads],
    [INTERSTATE_STORES.SA, 300_000, fetchSaRoads],
    [INTERSTATE_STORES.ACT, 300_000, fetchActRoads],
    [INTERSTATE_STORES.VIC, 120_000, fetchVicRoads],
  ];
  for (const [name, intervalMs, fetch] of sources) {
    registerSource<TrafficSnapshot>({ name, family: 'traffic', intervalMs, fetch, archiveItems: archiveByType });
  }
}
