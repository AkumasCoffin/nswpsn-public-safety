/**
 * Ausgrid outage-map poller.
 *
 * Two upstream calls:
 *   1. /webapi/OutageMapData/GetCurrentUnplannedOutageMarkersAndPolygons
 *      ?bottomleft.lat=...&bottomleft.lng=...&topright.lat=...&topright.lng=...
 *      &zoom=...
 *      Returns a flat array of outage markers (each with WebId, Area,
 *      MarkerLocation, OutageDisplayType, Customers, etc).
 *   2. /webapi/outagemapdata/GetCurrentOutageStats
 *      Returns aggregate counters for the front-page banner.
 *
 * Per-outage detail (Streets, Reason, JobId, EndDateTime) lives behind
 * a third endpoint (/webapi/OutageMapData/GetOutage?WebId=...). For the
 * Node port we **skip the per-outage enrichment** and surface only the
 * marker payload — same fields the Python side stores when the detail
 * call fails. The TODO at the bottom of this file tracks bringing back
 * detail enrichment with a bounded LRU cache once the rest of W4 lands.
 *
 * One register() call binds two registry entries:
 *   - ausgrid           → markers payload, family=power
 *   - ausgrid_stats     → aggregate counters, family=power
 *
 * Cadences: active 120s / idle 300s for both. Stats is cosmetic, but
 * polling on the same cadence keeps wiring simple and the upstream load
 * is negligible.
 *
 * Field shape exactly mirrors `_normalise_ausgrid_outage` at line
 * 3656-3727 of external_api_proxy.py — both PascalCase and camelCase
 * variants of every key, because legacy bot consumers read PascalCase
 * and frontend code reads camelCase.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';

// Bounding box covers Sydney + Central Coast + Hunter Valley — the full
// Ausgrid distribution territory plus ~0.05deg buffer so border outages
// don't get clipped. Mirrors the Python `_AUSGRID_BBOX` constant.
const AUSGRID_BBOX = {
  'bottomleft.lat': '-34.55',
  'bottomleft.lng': '150.20',
  'topright.lat': '-32.20',
  'topright.lng': '152.80',
  zoom: '9',
} as const;

const OUTAGES_URL =
  'https://www.ausgrid.com.au/webapi/OutageMapData/GetCurrentUnplannedOutageMarkersAndPolygons';
const STATS_URL =
  'https://www.ausgrid.com.au/webapi/outagemapdata/GetCurrentOutageStats';

// Browser-style headers — Ausgrid's CDN 403s anything that smells like
// a bot. Same set the Python prewarm fetch uses.
const BROWSER_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
  Accept: 'application/json, text/plain, */*',
  'Accept-Language': 'en-AU,en;q=0.9',
  Referer: 'https://www.ausgrid.com.au/Outages/View-current-outages',
  Origin: 'https://www.ausgrid.com.au',
  'Sec-Fetch-Site': 'same-origin',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Dest': 'empty',
} as const;

interface RawAusgridMarker {
  WebId?: string | number | null;
  Area?: string | null;
  MarkerLocation?: { lat?: number | null; lng?: number | null } | null;
  OutageDisplayType?: string | null;
  Customers?: number | string | null;
  CustomersAffectedText?: string | null;
  Cause?: string | null;
  StartDateTime?: string | null;
  EstRestTime?: string | null;
  Status?: number | string | null;
  Classification?: string | null;
  Polygons?: unknown[] | null;
}

export interface AusgridOutage {
  // PascalCase fields (legacy bot consumers).
  OutageId: string | number | null;
  JobId: string;
  Suburb: string;
  StreetName: string;
  Streets: string;
  Postcode: string;
  CustomersAffected: number;
  OutageType: 'Planned' | 'Unplanned';
  Cause: string;
  Reason: string;
  Detail: string;
  StatusText: string;
  StartTime: string;
  EstRestoration: string;
  EndDateTime: string;
  Latitude: number | null;
  Longitude: number | null;
  Status: number | string | null;
  Classification: string | null;
  Polygons: unknown[];
  // camelCase aliases (frontend consumers).
  outageId: string | number | null;
  suburb: string;
  streetName: string;
  streets: string;
  postcode: string;
  customersAffected: number;
  outageType: 'Planned' | 'Unplanned';
  cause: string;
  startTime: string;
  estRestoration: string;
  endDateTime: string;
  latitude: number | null;
  longitude: number | null;
}

export interface AusgridOutagesPayload {
  Markers: AusgridOutage[];
  Polygons: unknown[];
}

function parseCustomers(raw: unknown): number {
  if (raw === null || raw === undefined || raw === '') return 0;
  const n = Number.parseInt(String(raw).trim(), 10);
  return Number.isFinite(n) ? n : 0;
}

/** Normalise one upstream marker into the bot/frontend-friendly shape. */
export function normaliseAusgridOutage(item: RawAusgridMarker): AusgridOutage | null {
  if (!item || typeof item !== 'object') return null;
  const loc = item.MarkerLocation && typeof item.MarkerLocation === 'object' ? item.MarkerLocation : null;
  const lat = loc?.lat ?? null;
  const lng = loc?.lng ?? null;
  const display = (item.OutageDisplayType ?? '').toUpperCase();
  const outageType: 'Planned' | 'Unplanned' = display === 'P' ? 'Planned' : 'Unplanned';

  // The Python normaliser also peeks into a per-outage detail record.
  // We don't fetch that yet — see the TODO at the top — so detail-only
  // fields (Streets, Reason, JobId, etc.) come back empty.
  const cause = item.Cause ?? '';
  const startTime = item.StartDateTime ?? '';
  const endTime = item.EstRestTime ?? '';
  const customers = parseCustomers(item.Customers ?? item.CustomersAffectedText);

  return {
    OutageId: item.WebId ?? null,
    outageId: item.WebId ?? null,
    JobId: '',
    Suburb: item.Area ?? '',
    suburb: item.Area ?? '',
    StreetName: '',
    streetName: '',
    Streets: '',
    streets: '',
    Postcode: '',
    postcode: '',
    CustomersAffected: customers,
    customersAffected: customers,
    OutageType: outageType,
    outageType,
    Cause: cause,
    cause,
    Reason: '',
    Detail: '',
    StatusText: '',
    StartTime: startTime,
    startTime,
    EstRestoration: endTime,
    estRestoration: endTime,
    EndDateTime: endTime,
    endDateTime: endTime,
    Latitude: lat,
    latitude: lat,
    Longitude: lng,
    longitude: lng,
    Status: item.Status ?? null,
    Classification: item.Classification ?? null,
    Polygons: Array.isArray(item.Polygons) ? item.Polygons : [],
  };
}

export async function fetchAusgridOutages(): Promise<AusgridOutagesPayload> {
  const url = new URL(OUTAGES_URL);
  for (const [k, v] of Object.entries(AUSGRID_BBOX)) {
    url.searchParams.set(k, v);
  }
  const payload = await fetchJson<unknown>(url.toString(), {
    headers: BROWSER_HEADERS,
  });
  // New Ausgrid schema is a flat array. Old shape (cached by us) is
  // `{Markers, Polygons}`; pass-through if we see it. Anything else is
  // upstream returning garbage and we should fail loudly.
  if (Array.isArray(payload)) {
    const markers: AusgridOutage[] = [];
    const polygons: unknown[] = [];
    for (const item of payload as RawAusgridMarker[]) {
      const norm = normaliseAusgridOutage(item);
      if (!norm) continue;
      markers.push(norm);
      if (Array.isArray(item.Polygons)) {
        for (const p of item.Polygons) polygons.push(p);
      }
    }
    return { Markers: markers, Polygons: polygons };
  }
  if (payload && typeof payload === 'object' && 'Markers' in payload) {
    const obj = payload as { Markers?: unknown; Polygons?: unknown };
    return {
      Markers: Array.isArray(obj.Markers) ? (obj.Markers as AusgridOutage[]) : [],
      Polygons: Array.isArray(obj.Polygons) ? (obj.Polygons as unknown[]) : [],
    };
  }
  return { Markers: [], Polygons: [] };
}

export async function fetchAusgridStats(): Promise<unknown> {
  return fetchJson<unknown>(STATS_URL, { headers: BROWSER_HEADERS });
}

export function register(): void {
  registerSource<AusgridOutagesPayload>({
    name: 'ausgrid',
    family: 'power',
    intervalActiveMs: 120_000,
    intervalIdleMs: 300_000,
    fetch: fetchAusgridOutages,
  });
  registerSource<unknown>({
    name: 'ausgrid_stats',
    family: 'power',
    intervalActiveMs: 120_000,
    intervalIdleMs: 300_000,
    fetch: fetchAusgridStats,
  });
}

export default register;

// TODO(power-detail-enrichment): replace the empty Streets/Reason/JobId
// fields by fetching /webapi/OutageMapData/GetOutage?WebId=... per
// marker, behind a 5-min LRU cache (Python uses one keyed on
// (web_id, displayType) at line 3625-3653). Punted from W4 because the
// frontend's outage card tolerates the missing fields and the extra
// network cost (~N requests per poll) deserves its own pass.
