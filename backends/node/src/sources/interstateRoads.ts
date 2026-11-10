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
import { placeOnRoad, roadStretch, saRoads, _resetSaRoads, type SaRoads } from './saRoads.js';
import {
  fetchTrafficWorks,
  worksArchiveSource,
  type RoadSite,
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
  sites?: RoadSite[];
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
      ...(r.sites ? { sites: r.sites } : {}),
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
 * The layer lists works site by site — a record for each cross street along
 * a job (seen 2026-11-02: 1,481 records, 499 jobs, one job 59 sites over
 * 314 km) — so saJobRecords makes one record of each job. It is reloaded
 * whole every minute or so, and each load numbers its records afresh
 * (ROADWORKS_AND_INCIDENTS_ID, GIS_LINK_ID, ESRI_OID) in an order of its own
 * — two loads 27 seconds apart, every id new, every record the same — so
 * those numbers are never an id, and the pages are read in an order of what
 * the records say (SA_ROADWORKS_ORDER), so a reload between two pages does
 * not move records across the page boundary.
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

/** SA's words for a site's lanes, as advice. */
const SA_LANES: Readonly<Record<string, string>> = {
  'shuttle flow': 'shuttle flow',
  'one lane': 'one lane closed',
  'two lanes': 'two lanes closed',
  'three lanes': 'three lanes closed',
  contraflow: 'contraflow',
  closure: 'road closed',
  'lateral shift': 'lanes shifted',
  'slip lane': 'slip lane closed',
  'bike lane': 'bike lane closed',
};

/** What a site does to the lanes, in words; '' for nothing. A number is lanes closed. */
function saLanes(v: unknown): string {
  const n = num(v);
  if (n !== null) return n > 0 ? `${n} lane${n === 1 ? '' : 's'} closed` : '';
  const s = str(v).toLowerCase();
  return s === '' || s === 'none' ? '' : (SA_LANES[s] ?? s);
}

/** A speed limit in km/h; '' for none ("No Restriction"). */
function saSpeed(v: unknown): string {
  const n = num(v);
  return n !== null && n > 0 ? String(n) : '';
}

/** "Both Directions", or the incidents layer's list (`[{"direction": "NORTHBOUND", …}]`) as "Northbound, Southbound". */
function saDirection(v: unknown): string {
  const s = str(v);
  if (!s.startsWith('[')) return s;
  try {
    const list = JSON.parse(s) as unknown;
    const dirs = (Array.isArray(list) ? list : []).map((d) => titleCase(str(obj(d)['direction']))).filter(Boolean);
    return [...new Set(dirs)].join(', ');
  } catch {
    return '';
  }
}

const sentence = (s: string): string => (s ? s[0]!.toUpperCase() + s.slice(1) : s);

/** Kilometres between two [longitude, latitude] points. */
function kmBetween(a: readonly number[], b: readonly number[]): number {
  const rad = Math.PI / 180;
  const dLat = (b[1]! - a[1]!) * rad;
  const dLon = (b[0]! - a[0]!) * rad;
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(a[1]! * rad) * Math.cos(b[1]! * rad) * Math.sin(dLon / 2) ** 2;
  return 12_742 * Math.asin(Math.min(1, Math.sqrt(h)));
}

/** One record per feature: SA's incidents layer, by its own ids (stable: incident number, position and road). */
export function saWorksRecords(
  features: readonly Obj[],
  now = Date.now(),
  idOf: (a: Obj) => string = (a) => str(attr(a, 'roadworks_and_incidents_id')) || str(attr(a, 'incident_id')),
): TrafficFeature[] {
  const out: TrafficFeature[] = [];
  for (const f of features) {
    const a = obj(f['attributes']);
    const id = idOf(a);
    const lat = num(attr(a, 'latitude')) ?? num(obj(f['geometry'])['y']);
    const lon = num(attr(a, 'longitude')) ?? num(obj(f['geometry'])['x']);
    if (!id || lat === null || lon === null) continue;
    if (ended(isoTime(attr(a, 'end_date')), now)) continue;
    const recType = str(attr(a, 'rec_type')).toUpperCase();
    const isWorks = recType.includes('ROADWORK');
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
        otherAdvice: sentence(saLanes(attr(a, 'no_lanes_closed'))),
        affectedDirection: saDirection(attr(a, 'traffic_dir')),
        speedLimit: saSpeed(attr(a, 'speed_limit')),
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

/**
 * What makes sites one job: the same works on the same road from the same
 * start. Not the end, which SA moves out as the works go on (most of its
 * 24-hour roadworks, some started months ago, "end" within the month), nor
 * the lanes or speed limit, which change while they do.
 */
const SA_JOB = ['rec_type', 'description', 'local_road', 'start_date'];

const bySite = (x: RoadSite, y: RoadSite): number =>
  x.lat - y.lat ||
  x.lon - y.lon ||
  [x.crossStreet, x.suburb, x.direction, x.lanes, x.speedLimit].join('|').localeCompare([y.crossStreet, y.suburb, y.direction, y.lanes, y.speedLimit].join('|'));

/** How far off its road's line (km) a site may lie and still be placed along it. */
const SA_ON_ROAD_KM = 0.2;

/**
 * The roadworks layer, one record per job. A site that restricts nothing —
 * no lane closed and no speed limit (222 of 1,481 on 2026-11-02) — is left
 * out, and a job with no site left goes with it. A job keeps its id while SA
 * adds sites, moves its end or changes its lanes. Its `sites` run along the
 * road from one end to the other; on a road in `roads` (DIT's drawing, see
 * saRoads.ts) its line is the road between the first and the last, and its
 * distance is along the road. It is pinned at the site nearest the middle of
 * its sites, its roads name the suburbs at either end, and its advice says
 * how many sites over what distance and what they do to the lanes. The
 * layer's few incidents stay records of their own.
 */
export function saJobRecords(features: readonly Obj[], now = Date.now(), roads: SaRoads | null = null): TrafficFeature[] {
  const jobs = new Map<string, { a: Obj; sites: RoadSite[]; ends: string[] }>();
  const incidents: Obj[] = [];
  for (const f of features) {
    const a = obj(f['attributes']);
    if (!str(attr(a, 'rec_type')).toUpperCase().includes('ROADWORK')) {
      incidents.push(f);
      continue;
    }
    const lat = num(attr(a, 'latitude'));
    const lon = num(attr(a, 'longitude'));
    const end = isoTime(attr(a, 'end_date'));
    if (lat === null || lon === null || ended(end, now)) continue;
    const lanes = saLanes(attr(a, 'no_lanes_closed'));
    const speedLimit = saSpeed(attr(a, 'speed_limit'));
    if (!lanes && !speedLimit) continue;
    const key = SA_JOB.map((k) => str(attr(a, k))).join('|');
    const job = jobs.get(key) ?? jobs.set(key, { a, sites: [], ends: [] }).get(key)!;
    job.sites.push({
      lat,
      lon,
      crossStreet: str(attr(a, 'side_street')),
      suburb: titleCase(str(attr(a, 'suburb'))),
      direction: saDirection(attr(a, 'traffic_dir')),
      lanes,
      speedLimit,
    });
    job.ends.push(end);
  }

  const out: TrafficFeature[] = [];
  for (const [key, { a, sites: found, ends }] of jobs) {
    found.sort(bySite);
    const at = (s: RoadSite): number[] => [s.lon, s.lat];
    // Its sites in order along the road: where DIT draws the road
    // (saRoads.ts) at every site, by where each is along it; otherwise from
    // one of the two sites furthest apart to the other. Its line is the road
    // between the furthest-apart sites DIT draws it at — part of the job
    // where DIT draws only part of the road (seen: R M Williams Way).
    const roadNo = String(num(attr(a, 'road_no')) ?? '');
    const placed = roads && roadNo ? found.map((s) => placeOnRoad(roads, roadNo, s.lon, s.lat)) : [];
    const onRoad = placed.map((p) => (p !== null && p.km <= SA_ON_ROAD_KM ? p.at : null));
    const ats = onRoad.filter((x): x is number => x !== null);
    const lines = ats.length >= 2 && Math.max(...ats) - Math.min(...ats) > 0.01 ? roadStretch(roads!, roadNo, Math.min(...ats), Math.max(...ats)) : [];
    let sites: RoadSite[];
    let span: number;
    if (found.length && ats.length === found.length) {
      const along = new Map(found.map((s, i) => [s, onRoad[i]!]));
      sites = [...found].sort((x, y) => along.get(x)! - along.get(y)! || bySite(x, y));
      span = Math.max(...ats) - Math.min(...ats);
    } else {
      let [x, y, d] = [found[0]!, found[0]!, 0];
      for (let i = 0; i < found.length; i++) {
        for (let j = i + 1; j < found.length; j++) {
          const dij = kmBetween(at(found[i]!), at(found[j]!));
          if (dij > d) [x, y, d] = [found[i]!, found[j]!, dij];
        }
      }
      const fromX = new Map(found.map((s) => [s, kmBetween(at(x), at(s))]));
      sites = [...found].sort((p, q) => fromX.get(p)! - fromX.get(q)! || bySite(p, q));
      span = d;
    }
    const from = sites[0]!;
    const to = sites[sites.length - 1]!;
    // Its pin: the site nearest the middle.
    const mid = [sites.reduce((n, s) => n + s.lon, 0) / sites.length, sites.reduce((n, s) => n + s.lat, 0) / sites.length];
    const pin = sites.reduce((best, s) => (kmBetween(at(s), mid) < kmBetween(at(best), mid) ? s : best));

    const road = str(attr(a, 'local_road'));
    const suburbs = new Set(sites.map((s) => s.suburb).filter(Boolean));
    const where =
      from.suburb && to.suburb && from.suburb !== to.suburb
        ? `${road}, ${from.suburb} to ${to.suburb}`
        : joinText(road, from.suburb || to.suburb || ([...suburbs][0] ?? ''));

    // What each site does: its lanes, else its speed limit (the bot shows no speedLimit).
    const does = (s: RoadSite): string => s.lanes || `${s.speedLimit} km/h limit`;
    const counts = new Map<string, number>();
    for (const s of sites) counts.set(does(s), (counts.get(does(s)) ?? 0) + 1);
    const said = [...counts]
      .sort((x, y) => y[1] - x[1] || x[0].localeCompare(y[0]))
      .map(([what, n]) => (n === sites.length ? `${what} at each` : `${what} at ${n}`))
      .join(', ');
    const advice =
      sites.length === 1
        ? sentence(does(sites[0]!))
        : `${sites.length} sites${span >= 1 ? ` over ${Math.round(span)} km` : ''}. ${sentence(said)}.`;

    const speeds = sites.map((s) => Number(s.speedLimit)).filter((n) => n > 0);
    const directions = [...new Set(sites.map((s) => s.direction).filter(Boolean))];
    const known = ends.filter(Boolean).sort();
    const recType = str(attr(a, 'rec_type')).toUpperCase();
    out.push(
      roadRecord({
        id: `sa:job:${shortHash(key)}`,
        lon: pin.lon,
        lat: pin.lat,
        lines,
        type: 'Roadwork',
        mainCategory: titleCase(recType),
        incidentType: 'ROADWORK',
        incidentKind: 'Planned',
        title: htmlToProse(attr(a, 'description')) || titleCase(recType),
        roads: where,
        crossStreet: from.crossStreet,
        secondLocation: sites.length > 1 ? to.crossStreet : '',
        locationQualifier: sites.length > 1 && from.crossStreet && to.crossStreet ? 'between' : '',
        otherAdvice: advice,
        affectedDirection: directions.length > 1 ? 'Both Directions' : (directions[0] ?? ''),
        speedLimit: speeds.length ? String(Math.min(...speeds)) : '',
        start: isoTime(attr(a, 'start_date')),
        end: known[known.length - 1] ?? '',
        state: 'SA',
        source: 'traffic_sa',
        credit: SA_CREDIT,
        sites,
      }),
    );
  }
  // The incidents layer does not carry these (seen: a ferry closed on the
  // Birdsville Track). Their incident number, else what they say, is their id.
  const incidentId = (a: Obj): string => {
    const n = num(attr(a, 'incident_id'));
    return `incident:${n !== null && n > 0 ? String(n) : shortHash(SA_JOB.map((k) => str(attr(a, k))).join('|'))}`;
  };
  return out.concat(saWorksRecords(incidents, now, incidentId));
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

// ---- Planned works: only what is working right now ------------------------
//
// Transport Victoria's planned feed is its permit register: ~10,000 records,
// median 177 days long, each with the hours work is permitted (mostly
// overnight). Measured 2026-11-06: 9,628 roadworks inside their dates, and
// 4,300-5,700 inside their working hours at any time, against ~330 roadworks
// for all of NSW. `status` does not say which: 570 of the 802 marked Active
// were outside their hours, and 571 marked Pending were inside them. So the
// layer keeps a planned record only while it is inside its own working hours,
// and a roadwork only when it closes the road (owner's choice, 2026-11-06):
// ~800-950 by day, ~2,000-2,400 overnight, before grouping per job.

const MEL_PARTS = new Intl.DateTimeFormat('en-AU', {
  timeZone: 'Australia/Melbourne', hourCycle: 'h23', weekday: 'long',
  year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit',
});

/** Melbourne wall clock at an instant. */
function melClock(ms: number): { weekday: string; minutes: number; asUtcMs: number } {
  const f: Record<string, string> = {};
  for (const p of MEL_PARTS.formatToParts(new Date(ms))) f[p.type] = p.value;
  const asUtcMs = Date.UTC(+f['year']!, +f['month']! - 1, +f['day']!, +f['hour']!, +f['minute']!, +f['second']!);
  return { weekday: f['weekday'] ?? '', minutes: +f['hour']! * 60 + +f['minute']!, asUtcMs };
}

/** A Transport Victoria time: Melbourne local unless it carries an offset. */
export function vicTime(v: unknown): number {
  const t = str(v);
  if (!t) return NaN;
  if (/(z|[+-]\d\d:?\d\d)$/i.test(t)) return Date.parse(t);
  const naive = Date.parse(`${t}Z`);
  if (!Number.isFinite(naive)) return NaN;
  // The offset in force at that wall-clock time (two passes settle DST).
  let ms = naive;
  for (let i = 0; i < 2; i++) ms = naive - (melClock(ms).asUtcMs - ms);
  return ms;
}

/** "9:30 AM" / "21:00" as minutes after midnight, or null. */
function vicClockMinutes(v: unknown): number | null {
  const m = /^(\d{1,2}):(\d\d)\s*(am|pm)?$/i.exec(str(v));
  if (!m) return null;
  let h = Number(m[1]);
  if (m[3]) h = (h % 12) + (m[3].toLowerCase() === 'pm' ? 12 : 0);
  return h * 60 + Number(m[2]);
}

/** "PT6H0M" as minutes. Overnight windows are written negative ("PT-15H0M"
 *  from 9 PM ends at 6 AM) and a whole day as "PT0H0M": both wrap a day. */
function vicSpanMinutes(v: unknown): number {
  const m = /^PT(-?\d+)H(?:(-?\d+)M)?$/i.exec(str(v));
  if (!m) return 0;
  const n = Number(m[1]) * 60 + Number(m[2] ?? 0);
  return n <= 0 ? n + 1440 : n;
}

const WEEKDAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

/**
 * Whether a planned record is inside its permitted hours at `now`: within its
 * dates, and (when it lists any) inside one of its weekly windows. A window
 * that starts late one day runs into the next.
 */
export function vicWorkingNow(duration: Obj, now: number): boolean {
  const start = vicTime(duration['start']);
  const end = vicTime(duration['end']);
  if (Number.isFinite(start) && now < start) return false;
  if (Number.isFinite(end) && now > end) return false;
  const windows = Array.isArray(duration['recurrences']) ? (duration['recurrences'] as unknown[]).map(obj) : [];
  if (windows.length === 0) return true;
  const clock = melClock(now);
  const today = WEEKDAYS.indexOf(clock.weekday);
  for (const w of windows) {
    const day = WEEKDAYS.indexOf(str(w['startDay']));
    if (day < 0) continue;
    const days = Math.max(1, num(w['daysDuration']) ?? 1);
    let from: number;
    let len: number;
    if (w['allDay'] === true) {
      from = 0;
      len = days * 1440;
    } else {
      const at = vicClockMinutes(w['startTime']);
      if (at === null) continue;
      from = at;
      len = vicSpanMinutes(w['duration']) + (days - 1) * 1440;
    }
    // Minutes since this window last opened, counting back up to a week.
    const back = (today - day + 7) % 7;
    for (const b of [back, back + 7]) {
      const since = clock.minutes + b * 1440 - from;
      if (since >= 0 && since < len) return true;
    }
  }
  return false;
}

/** One road record from a Transport Victoria disruption's properties. */
function vicRecord(
  p: Obj,
  parts: { points: number[][]; lines: number[][][] },
  pin: [number, number],
  planned: boolean,
  id: string,
  extra: { roads?: string; otherAdvice?: string } = {},
): TrafficFeature {
  const ref = obj(p['reference']);
  const impact = obj(p['impact']);
  const duration = obj(p['duration']);
  const eventType = str(p['eventType']);
  const sub = str(p['eventSubType']) || str(p['eventSubtype']);
  const type = vicType(eventType, planned);
  const road = str(p['closedRoadName']) || str(ref['localRoadName']);
  const locality = str(p['startIntersectionLocality']) || str(ref['startIntersectionLocality']);
  const link = str(p['weblinkURL']);
  return roadRecord({
    id,
    lon: pin[0],
    lat: pin[1],
    lines: parts.lines,
    type,
    mainCategory: eventType || (planned ? 'Planned disruption' : 'Unplanned disruption'),
    subCategory: sub,
    incidentType: type === 'Flood' ? 'FLOODING' : type === 'Roadwork' ? 'ROADWORK' : /crash|collision/i.test(eventType) ? 'CRASH' : '',
    incidentKind: planned ? 'Planned' : 'Unplanned',
    title: sub ? `${eventType || 'Disruption'}: ${sub}` : eventType || 'Disruption',
    roads: extra.roads ?? joinText(road, locality),
    crossStreet: str(p['startIntersectionRoadName']) || str(ref['startIntersectionRoadName']),
    otherAdvice: extra.otherAdvice ?? htmlToProse(p['description']),
    adviceB: str(p['eventDueTo']),
    affectedDirection: str(impact['direction']),
    expectedDelay: str(impact['delay']),
    speedLimit: str(impact['speedLimitOnSite']),
    created: isoTime(p['created']) || isoTime(duration['start']),
    lastUpdated: isoTime(p['lastUpdated']),
    start: isoTime(duration['start']) || isoTime(p['created']),
    end: isoTime(duration['end']) || isoTime(p['endTime']),
    region: str(p['localGovernmentArea']) || str(ref['localGovernmentArea']),
    webLinks: link ? [{ url: link }] : [],
    state: 'VIC',
    source: 'transport_vic',
    credit: VIC_CREDIT,
  });
}

/** Transport Victoria's unplanned disruptions, or its planned ones taken
 *  record by record (vicPlannedRecords is what the layer serves). */
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
    out.push(vicRecord(p, parts, pin, planned, `vic:${planned ? 'planned' : 'unplanned'}:${id}`));
  }
  return out;
}

/** What makes two planned roadwork records one job: the same works on the
 *  same road over the same dates, listed once per stretch of road. */
const vicJobKey = (p: Obj): string =>
  [
    str(p['eventDueTo']),
    str(p['closedRoadName']).trim(),
    htmlToProse(p['description']),
    str(obj(p['duration'])['start']),
    str(obj(p['duration'])['end']),
  ].join('|');

type VicMember = { p: Obj; parts: { points: number[][]; lines: number[][][] }; pin: [number, number] };

/**
 * The planned layer: what is inside its working hours right now, with
 * roadworks kept only where they close the road, and one record per job.
 *
 * Transport Victoria lists a job once per stretch of road (4,599 records were
 * 1,389 jobs on a Tuesday morning), so a job's stretches become one record
 * with every stretch drawn, pinned at the stretch nearest their middle. Its id
 * is the job's, so it keeps it while stretches come and go and each time its
 * hours come round again. Special events stay one record each.
 */
export function vicPlannedRecords(features: readonly unknown[], now = Date.now()): TrafficFeature[] {
  const out: TrafficFeature[] = [];
  const jobs = new Map<string, VicMember[]>();
  for (const f of features) {
    const p = obj(obj(f)['properties']);
    const id = str(p['id']) || str(p['eventId']);
    if (!id) continue;
    if (VIC_DONE.test(str(p['status']))) continue;
    if (!vicWorkingNow(obj(p['duration']), now)) continue;
    const parts = geoParts(obj(f)['geometry']);
    const pin = pinOf(parts);
    if (!pin) continue;
    if (vicType(str(p['eventType']), true) !== 'Roadwork') {
      out.push(vicRecord(p, parts, pin, true, `vic:planned:${id}`));
      continue;
    }
    if (str(obj(p['impact'])['impactType']).toLowerCase() !== 'closures') continue;
    const key = vicJobKey(p);
    const job = jobs.get(key);
    if (job) job.push({ p, parts, pin });
    else jobs.set(key, [{ p, parts, pin }]);
  }

  for (const [key, members] of jobs) {
    const mid = [
      members.reduce((n, m) => n + m.pin[0], 0) / members.length,
      members.reduce((n, m) => n + m.pin[1], 0) / members.length,
    ];
    const at = members.reduce((best, m) => (kmBetween(m.pin, mid) < kmBetween(best.pin, mid) ? m : best));
    const first = members[0]!.p;
    const road = titleCase(str(first['closedRoadName']).trim());
    const places = [
      ...new Set(
        members
          .flatMap((m) => [str(m.p['startIntersectionLocality']), str(m.p['endIntersectionLocality'])])
          .map((x) => titleCase(x.trim()))
          .filter(Boolean),
      ),
    ];
    const roads =
      places.length > 1 ? `${road}, ${places[0]} to ${places[places.length - 1]}` : joinText(road, places[0] ?? '');
    const said = htmlToProse(first['description']);
    out.push(
      vicRecord(at.p, { points: [], lines: members.flatMap((m) => m.parts.lines) }, at.pin, true, `vic:planned:job:${shortHash(key)}`, {
        roads,
        otherAdvice: members.length > 1 ? `${members.length} stretches of road. ${said}` : said,
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

/** The planned feed as last read. Kept raw: which records are inside their
 *  working hours changes between reads, so the layer is cut from it on every
 *  poll. */
let _vicPlanned: { at: number; features: unknown[] } | null = null;

async function vicPlanned(key: string): Promise<TrafficFeature[]> {
  if (_vicPlanned && Date.now() - _vicPlanned.at < VIC_PLANNED_EVERY_MS) return vicPlannedRecords(_vicPlanned.features);
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
  _vicPlanned = { at: Date.now(), features };
  return vicPlannedRecords(features);
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
  _resetSaRoads();
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
  // One record per id, should two parts ever carry the same record.
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
        saJobRecords(
          await arcgisFeatures(SA_ROADWORKS_URL, { returnGeometry: 'false', orderByFields: SA_ROADWORKS_ORDER }),
          Date.now(),
          await saRoads(),
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
