/**
 * Endeavour Energy outage poller.
 *
 * Endeavour migrated from a Sitecore proxy to a public Supabase project.
 * Two calls per refresh, joined client-side:
 *   1. POST /rpc/get_outage_areas_fast        — aggregated incidents (GPS
 *                                              centroid, customer count,
 *                                              outage type, status)
 *   2. GET  /outage-points?select=...        — per-point enrichment
 *                                              (cause, suburb, postcode,
 *                                              start/end timestamps)
 *
 * One fetch produces three logical sub-feeds, written separately into
 * LiveStore so each route can hit its own key:
 *   - endeavour_current      → unplanned outages, currently affecting
 *                              customers
 *   - endeavour_maintenance  → planned maintenance currently active
 *   - endeavour_planned      → planned maintenance scheduled for future
 *
 * Cadences (per W4 plan):
 *   - endeavour_current      active 60s / idle 300s
 *   - endeavour_planned      active 300s / idle 600s
 *   - endeavour_maintenance  active 300s / idle 600s
 *
 * Because all three derive from the same upstream pair of calls, we
 * fetch once per `endeavour_current` tick and stash the planned/
 * maintenance partitions in a small in-module memo, so the planned
 * and maintenance pollers can read the latest split without re-paying
 * the network cost. The memo's TTL just has to exceed the longest
 * idle interval for the three feeds (600s) so it's never stale by
 * accident — we use 30s freshness, matching the Python lock window
 * around `_endeavour_supabase_cache`.
 *
 * Field shape mirrors the Python normaliser at line 949-979 of
 * external_api_proxy.py — same key names, same casing, same types.
 */
import { fetchJson, HttpError } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { config } from '../config.js';
import { log } from '../lib/log.js';

export interface EndeavourOutage {
  id: string;
  suburb: string;
  streets: string;
  customersAffected: number;
  status: string;
  cause: string;
  outageType: 'Unplanned' | 'Current Maintenance' | 'Future Maintenance';
  startTime: string | null;
  estimatedRestoration: string | null;
  lastUpdated: string;
  latitude: number | null;
  longitude: number | null;
  postcode: string;
  hasGPS: boolean;
  /** Only set on Planned outages, mirrors Python's `endTime`. */
  endTime?: string | null;
  /** Only set on Planned outages, mirrors Python's `duration`. */
  duration?: string;
}

export interface EndeavourSplit {
  current: EndeavourOutage[];
  current_maintenance: EndeavourOutage[];
  future_maintenance: EndeavourOutage[];
}

interface SupabaseArea {
  incident_id?: string;
  outage_type?: string;
  incident_status?: string;
  customers_affected?: number | null;
  center_lat?: number | null;
  center_lng?: number | null;
}

interface SupabasePoint {
  incident_id?: string;
  cause?: string | null;
  sub_cause?: string | null;
  start_date_time?: string | null;
  end_date_time?: string | null;
  etr?: string | null;
  cityname?: string | null;
  postcode?: string | null;
  street_name?: string | null;
  updated_at?: string | null;
}

const STATUS_MAP: Record<string, string> = {
  SUBMITTED: 'Active',
  NEW: 'Active',
  LODGED: 'Scheduled',
  PREPARED: 'Scheduled',
  SCHEDULED: 'Scheduled',
  DESPATCHED: 'Crew Dispatched',
  'DAMAGE ASSESSED': 'Damage Assessed',
};

const ACTIVE_PLANNED_STATUSES = new Set([
  'SUBMITTED',
  'NEW',
  'DESPATCHED',
  'DAMAGE ASSESSED',
  'REPAIR',
  'REPAIR CONTROL ROOM',
]);

/** Memo so the three pollers don't each pay the network cost. */
let _memo: { at: number; data: EndeavourSplit } | null = null;
const MEMO_TTL_MS = 30_000; // matches Python's 30s in-process cache

function titleCase(s: string): string {
  // Equivalent of Python's str.title() for the suburb field. Capitalises
  // the first letter of each word, lowercases the rest. Doesn't try to
  // be smart about apostrophes ("D'Aguilar" → "D'Aguilar" stays correct
  // because str.title in Python has the same artefact).
  return s
    .toLowerCase()
    .replace(/(?:^|\s|-|')([a-z])/g, (m) => m.toUpperCase());
}

/**
 * Best-effort ISO-8601 sanity check. Mirrors Python's `is_valid_datetime`
 * loosely — we just verify Date.parse can handle it AND year is in a
 * sane range, since Supabase occasionally emits "0001-01-01..." sentinels.
 */
function isValidDatetime(s: string): boolean {
  if (!s) return false;
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) return false;
  const year = new Date(ms).getUTCFullYear();
  return year >= 2000 && year <= 2100;
}

export async function callSupabase(
  endpoint: string,
  init: { method: 'GET' | 'POST'; body?: unknown; query?: Record<string, string> } = {
    method: 'GET',
  },
): Promise<unknown> {
  const base = config.ENDEAVOUR_SUPABASE_URL;
  const key = config.ENDEAVOUR_SUPABASE_KEY;
  if (!base || !key) {
    throw new HttpError('endeavour: ENDEAVOUR_SUPABASE_URL/_KEY not configured', null, endpoint);
  }
  // Plain string concat (mirrors python's f"{base}{endpoint}"). The
  // WHATWG `new URL(endpoint, base)` constructor treats a leading "/"
  // in `endpoint` as origin-absolute, which strips any base path —
  // e.g. base "https://x.supabase.co/rest/v1" + endpoint "/rpc/foo"
  // resolves to ".../rpc/foo" not ".../rest/v1/rpc/foo". Endeavour's
  // env URL ends in /rest/v1 so concat is what we want.
  const trimmedBase = base.replace(/\/+$/, '');
  const path = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
  const url = new URL(`${trimmedBase}${path}`);
  if (init.query) {
    for (const [k, v] of Object.entries(init.query)) url.searchParams.set(k, v);
  }
  return fetchJson<unknown>(url.toString(), {
    method: init.method,
    headers: {
      apikey: key,
      Authorization: `Bearer ${key}`,
      'Content-Type': 'application/json',
    },
    body: init.method === 'POST' ? JSON.stringify(init.body ?? {}) : undefined,
    timeoutMs: 20_000,
  });
}

/**
 * Pull both Supabase calls and merge into the three-partition shape the
 * Python backend exposes. Throws on hard failures; partial enrichment
 * (e.g. /outage-points 5xx but areas OK) is treated as success because
 * the area record alone still produces a usable outage card — same
 * behaviour as Python's enrichment lookup defaulting to {} on miss.
 */
export async function fetchEndeavourSplit(now: Date = new Date()): Promise<EndeavourSplit> {
  const areasRaw = await callSupabase('/rpc/get_outage_areas_fast', {
    method: 'POST',
    body: {},
  });
  if (!Array.isArray(areasRaw)) {
    throw new HttpError(
      'endeavour: get_outage_areas_fast returned non-array',
      null,
      '/rpc/get_outage_areas_fast',
    );
  }
  const areas = areasRaw as SupabaseArea[];

  // Enrichment fetch — best-effort: if it dies we just lose the cause/
  // suburb/timestamp fields. A hard throw would mean a single 500 on
  // /outage-points wipes the whole feed, which is worse for the UI.
  let enrichmentRaw: unknown = null;
  try {
    enrichmentRaw = await callSupabase('/outage-points', {
      method: 'GET',
      query: {
        select: 'incident_id,cause,sub_cause,start_date_time,end_date_time,etr,cityname,postcode,street_name,updated_at',
        order: 'incident_id.asc',
      },
    });
  } catch (err) {
    log.warn({ err }, 'endeavour: outage-points enrichment failed; continuing without');
  }
  const enrichment = new Map<string, SupabasePoint>();
  if (Array.isArray(enrichmentRaw)) {
    for (const pt of enrichmentRaw as SupabasePoint[]) {
      const iid = pt.incident_id;
      if (iid && !enrichment.has(iid)) enrichment.set(iid, pt);
    }
  }

  const current: EndeavourOutage[] = [];
  const currentMaintenance: EndeavourOutage[] = [];
  const futureMaintenance: EndeavourOutage[] = [];

  for (const area of areas) {
    const incidentId = area.incident_id ?? '';
    const outageTypeRaw = (area.outage_type ?? '').toUpperCase();
    const isPlanned = outageTypeRaw === 'PLANNED';
    const enrich = (incidentId && enrichment.get(incidentId)) || ({} as SupabasePoint);

    let startDate: string | null = (enrich.start_date_time ?? '') || null;
    let endDate: string | null = (enrich.end_date_time ?? enrich.etr ?? '') || null;
    const updated = enrich.updated_at ?? '';

    if (startDate && !isValidDatetime(startDate)) startDate = null;
    if (endDate && !isValidDatetime(endDate)) endDate = null;

    let isCurrentMaintenance = false;
    if (isPlanned) {
      const statusRaw = (area.incident_status ?? '').toUpperCase();
      if (ACTIVE_PLANNED_STATUSES.has(statusRaw)) {
        isCurrentMaintenance = true;
      } else if (startDate) {
        // Falls into "current maintenance" if the start date is in the
        // past, even if the status string doesn't say so.
        const startMs = Date.parse(startDate);
        if (Number.isFinite(startMs) && startMs <= now.getTime()) {
          isCurrentMaintenance = true;
        }
      }
    }

    const outageTypeLabel: EndeavourOutage['outageType'] = !isPlanned
      ? 'Unplanned'
      : isCurrentMaintenance
        ? 'Current Maintenance'
        : 'Future Maintenance';

    const statusRaw = (area.incident_status ?? 'Active').toUpperCase();
    // Match python's status_raw.title() — capitalise each word, not just
    // the first character. e.g. "REPAIR CONTROL ROOM" should render as
    // "Repair Control Room", not "Repair control room".
    const status =
      STATUS_MAP[statusRaw] ??
      statusRaw.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());

    const suburb = enrich.cityname ? titleCase(enrich.cityname) : 'Unknown';
    const causePlannedDefault = isPlanned ? 'Planned maintenance' : '';
    const cause = enrich.cause || enrich.sub_cause || causePlannedDefault;

    const lat = area.center_lat ?? null;
    const lng = area.center_lng ?? null;

    const outage: EndeavourOutage = {
      id: incidentId,
      suburb,
      streets: enrich.street_name ?? '',
      customersAffected: area.customers_affected ?? 0,
      status,
      cause,
      outageType: outageTypeLabel,
      startTime: startDate,
      estimatedRestoration: endDate,
      lastUpdated: updated,
      latitude: lat,
      longitude: lng,
      postcode: enrich.postcode ?? '',
      hasGPS: Boolean(lat) && Boolean(lng),
    };

    if (isPlanned) {
      outage.endTime = endDate;
      outage.duration = '';
      if (isCurrentMaintenance) currentMaintenance.push(outage);
      else futureMaintenance.push(outage);
    } else {
      current.push(outage);
    }
  }

  return {
    current,
    current_maintenance: currentMaintenance,
    future_maintenance: futureMaintenance,
  };
}

/** Returns the memo if it's fresh, else fetches and caches. */
async function getOrFetch(): Promise<EndeavourSplit> {
  if (_memo && Date.now() - _memo.at < MEMO_TTL_MS) return _memo.data;
  const data = await fetchEndeavourSplit();
  _memo = { at: Date.now(), data };
  return data;
}

/**
 * Reset the memo — used by tests to avoid bleed-through between cases.
 * Not on the public surface for production code.
 */
export function _resetEndeavourMemo(): void {
  _memo = null;
}

export function register(): void {
  // Drives the network fetch itself. The other two pollers piggyback on
  // the memo populated here.
  registerSource<EndeavourOutage[]>({
    name: 'endeavour_current',
    family: 'power',
    intervalActiveMs: 60_000,
    intervalIdleMs: 300_000,
    fetch: async () => (await getOrFetch()).current,
  });
  registerSource<EndeavourOutage[]>({
    name: 'endeavour_planned',
    family: 'power',
    intervalActiveMs: 300_000,
    intervalIdleMs: 600_000,
    fetch: async () => (await getOrFetch()).future_maintenance,
  });
  registerSource<EndeavourOutage[]>({
    name: 'endeavour_maintenance',
    family: 'power',
    intervalActiveMs: 300_000,
    intervalIdleMs: 600_000,
    fetch: async () => (await getOrFetch()).current_maintenance,
  });
}

export default register;
