/**
 * Ausgrid outage-map poller.
 *
 * Ausgrid's outage map reads two public endpoints, and so does this:
 *   1. /api/outages-map/outages/list?futureDayLimit=N
 *      A flat array of outage events. N=0 is today only: every unplanned
 *      outage plus today's planned work (about 90 records). N=30 adds the
 *      planned work of the next 30 days (about 1,400 records, 860 KB).
 *      Without the parameter it answers everything out to 2039, so it is
 *      never called that way.
 *   2. /api/outages-map/outages/stats
 *      Ausgrid's own counters, passed on as they are.
 *
 * Unplanned outages only ever appear in the today list; the days ahead hold
 * planned work only. So every poll reads the small today list, and the large
 * 30-day list is read at most every half hour and kept in memory between
 * reads. The two are merged by EventId with today's record winning, since it
 * is the fresher one.
 *
 * An event carries everything the old per-outage detail call used to give
 * (streets, reason, status), so there is no second request per outage.
 * Events that are over (restored, completed, cancelled) are left out.
 *
 * One register() call binds two registry entries:
 *   - ausgrid           → {Markers, Polygons}, family=power
 *   - ausgrid_stats     → the counters, family=power
 * Both every two minutes.
 *
 * Every field is written twice, PascalCase and camelCase, because the
 * Discord bot reads the PascalCase names and the pages the camelCase ones.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';
import { log } from '../lib/log.js';
import type { ArchiveRow } from '../store/archive.js';

const LIST_URL = 'https://www.ausgrid.com.au/api/outages-map/outages/list';
const STATS_URL = 'https://www.ausgrid.com.au/api/outages-map/outages/stats';

/** How many days of planned work to read ahead. */
const FUTURE_DAYS = 30;
/** Planned work is booked days ahead and rarely changes, so half an hour is fresh enough. */
const FUTURE_REFRESH_MS = 30 * 60_000;
const FUTURE_RETRY_MS = 5 * 60_000;
/** The 30-day list is about 860 KB; give it longer than the default 15 s. */
const FUTURE_TIMEOUT_MS = 30_000;

// The endpoints answer a plain GET today. The browser-style headers stay
// because Ausgrid's CDN has refused requests that look like a bot before.
// The network area (outages/boundaries.ts) is read from the same site with them.
export const BROWSER_HEADERS = {
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

/** One outage event as Ausgrid's list gives it. Times are ISO 8601 UTC with a Z. */
export interface RawAusgridEvent {
  EventId?: number | string | null;
  /** 'P' planned, 'U' unplanned. */
  OutageDisplayType?: string | null;
  /**
   * Unplanned: InProgress, Restored. Planned: Planned, ProceedingAsScheduled,
   * InProgress, RestorationHasCommenced, Completed, Cancelled.
   */
  PowerStatus?: string | null;
  Cause?: string | null;
  Reason?: string | null;
  /** Locality names. */
  Area?: string[] | string | null;
  WhenUpdated?: string | null;
  StartDateTime?: string | null;
  EstRestTime?: string | null;
  ActualRestoreTime?: string | null;
  DelayInHours?: number | null;
  MarkerLocation?: { lat?: number | null; lng?: number | null } | null;
  /** A bounding box around the outage, not its outline. */
  Bounds?: {
    bottomleftlat?: number | null;
    bottomleftlng?: number | null;
    toprightlat?: number | null;
    toprightlng?: number | null;
  } | null;
  StreetsAffected?: string[] | string | null;
  AffectedCustomers?: number | string | null;
}

export interface AusgridOutage {
  // PascalCase fields (the Discord bot reads these).
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
  WhenUpdated: string;
  Latitude: number | null;
  Longitude: number | null;
  Status: number | string | null;
  Classification: string | null;
  Polygons: unknown[];
  // camelCase copies (the pages read these).
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

const text = (v: unknown): string => (typeof v === 'string' ? v.trim() : '');
const coord = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);

/** A list of names as written; a single string is taken as a list of one. */
function names(v: unknown): string[] {
  if (Array.isArray(v)) return v.map(text).filter(Boolean);
  const one = text(v);
  return one ? [one] : [];
}

/** "ProceedingAsScheduled" → "Proceeding as scheduled". */
function inWords(status: string): string {
  const words = status.replace(/([a-z0-9])([A-Z])/g, '$1 $2').toLowerCase();
  return words.charAt(0).toUpperCase() + words.slice(1);
}

/** The power is back, or the work was finished or called off. */
function isOver(planned: boolean, status: string): boolean {
  const s = status.toLowerCase();
  return planned ? s === 'completed' || s === 'cancelled' : s === 'restored';
}

/**
 * One event in the shape the bot and the pages read. Null for an event that
 * is over, and for anything that is not an event at all.
 */
export function normaliseAusgridOutage(event: RawAusgridEvent): AusgridOutage | null {
  if (!event || typeof event !== 'object' || Array.isArray(event)) return null;
  const planned = text(event.OutageDisplayType).toUpperCase() === 'P';
  const status = text(event.PowerStatus);
  if (isOver(planned, status)) return null;

  const outageType: 'Planned' | 'Unplanned' = planned ? 'Planned' : 'Unplanned';
  const id = event.EventId ?? null;
  const suburb = names(event.Area).join(', ');
  const streetList = names(event.StreetsAffected);
  const streets = streetList.join(', ');
  const streetName = streetList[0] ?? '';
  const customers = parseCustomers(event.AffectedCustomers);
  const cause = text(event.Cause);
  const startTime = text(event.StartDateTime);
  const endTime = text(event.EstRestTime);
  const loc = event.MarkerLocation && typeof event.MarkerLocation === 'object' ? event.MarkerLocation : null;
  const lat = coord(loc?.lat);
  const lng = coord(loc?.lng);

  return {
    OutageId: id,
    outageId: id,
    // Ausgrid publishes no job number, postcode or free-text detail.
    JobId: '',
    Suburb: suburb,
    suburb,
    StreetName: streetName,
    streetName,
    Streets: streets,
    streets,
    Postcode: '',
    postcode: '',
    CustomersAffected: customers,
    customersAffected: customers,
    OutageType: outageType,
    outageType,
    Cause: cause,
    cause,
    Reason: text(event.Reason),
    Detail: '',
    StatusText: status ? inWords(status) : '',
    StartTime: startTime,
    startTime,
    EstRestoration: endTime,
    estRestoration: endTime,
    EndDateTime: endTime,
    endDateTime: endTime,
    WhenUpdated: text(event.WhenUpdated),
    Latitude: lat,
    latitude: lat,
    Longitude: lng,
    longitude: lng,
    Status: status || null,
    Classification: null,
    // Ausgrid gives only Bounds, a box around the outage. A box is not the
    // outage's outline and drawing it would overstate the area, so there is
    // no polygon.
    Polygons: [],
  };
}

/** True for planned work that has not started yet: nobody's power is off. */
export function ausgridScheduled(m: AusgridOutage, nowMs: number): boolean {
  if (!m || m.OutageType !== 'Planned' || typeof m.StartTime !== 'string') return false;
  const start = Date.parse(m.StartTime);
  return Number.isFinite(start) && start > nowMs;
}

// The last 30-day list read, and when it was last asked for. Module memory
// only: after a restart the first poll reads it again.
let futureEvents: RawAusgridEvent[] = [];
let futureAskedAt: number | null = null;

/** Test hook: forget the cached 30-day list. */
export function __resetAusgridFutureCache(): void {
  futureEvents = [];
  futureAskedAt = null;
}

const listUrl = (days: number): string => `${LIST_URL}?futureDayLimit=${days}`;

/** The 30-day list, re-read when the copy in memory is half an hour old. */
async function futureList(nowMs: number): Promise<RawAusgridEvent[]> {
  if (futureAskedAt !== null && nowMs - futureAskedAt < FUTURE_REFRESH_MS) return futureEvents;
  // Stamped before the read, so a slow or failing read is not started again
  // by the next poll.
  futureAskedAt = nowMs;
  try {
    const payload = await fetchJson<unknown>(listUrl(FUTURE_DAYS), {
      headers: BROWSER_HEADERS,
      timeoutMs: FUTURE_TIMEOUT_MS,
    });
    if (!Array.isArray(payload)) throw new Error('not a list');
    futureEvents = payload as RawAusgridEvent[];
  } catch (err) {
    // Today's outages matter more than the days ahead: keep the previous
    // copy and carry on with it.
    log.warn({ err: (err as Error).message }, 'ausgrid: 30-day list read failed; keeping the previous copy');
    // Tried again in five minutes rather than in half an hour: a failure on
    // the first read after a restart would otherwise leave the days ahead
    // empty for thirty minutes.
    futureAskedAt = nowMs - FUTURE_REFRESH_MS + FUTURE_RETRY_MS;
  }
  return futureEvents;
}

const eventKey = (e: RawAusgridEvent): string | null =>
  e && typeof e === 'object' && e.EventId !== null && e.EventId !== undefined ? String(e.EventId) : null;

export async function fetchAusgridOutages(): Promise<AusgridOutagesPayload> {
  const today = await fetchJson<unknown>(listUrl(0), { headers: BROWSER_HEADERS });
  // Anything but a list is Ausgrid answering something else (an error page,
  // a changed API). Throwing records a failed poll and leaves the last good
  // snapshot in place; answering empty would say there are no outages.
  if (!Array.isArray(today)) throw new Error('ausgrid: outage list is not an array');
  const future = await futureList(Date.now());

  // Merged before the over events are dropped, so that an outage the cached
  // 30-day list still has as planned, and today's list has as completed or
  // cancelled, goes.
  const merged = new Map<string, RawAusgridEvent>();
  const unkeyed: RawAusgridEvent[] = [];
  for (const e of today as RawAusgridEvent[]) {
    const key = eventKey(e);
    if (key === null) unkeyed.push(e);
    else merged.set(key, e);
  }
  for (const e of future) {
    const key = eventKey(e);
    if (key !== null && !merged.has(key)) merged.set(key, e);
  }

  const markers: AusgridOutage[] = [];
  for (const e of [...merged.values(), ...unkeyed]) {
    const norm = normaliseAusgridOutage(e);
    if (norm) markers.push(norm);
  }
  return { Markers: markers, Polygons: [] };
}

export async function fetchAusgridStats(): Promise<unknown> {
  return fetchJson<unknown>(STATS_URL, { headers: BROWSER_HEADERS });
}

/**
 * Per-outage archive fan-out. The default extractor only handles GeoJSON
 * FeatureCollections and flat arrays, and the Ausgrid snapshot is
 * `{ Markers, Polygons }`, so without this every poll would land as one
 * "Unknown" wrapper row in archive_power.
 *
 * Every outage is passed on, the planned work to come included (about 1,400
 * rows a poll). That is affordable only because the archive writer hashes
 * each row and inserts it again only when its data has changed
 * (store/archive.ts, "Write-time dedup"); an unchanged outage costs a hash
 * lookup and a last-seen bump. Nothing here changes from poll to poll unless
 * Ausgrid changes the outage, so keep it that way: no per-poll timestamp in
 * `data`.
 */
function ausgridArchiveItems(
  data: unknown,
  fetched_at: number,
  source: string,
): ArchiveRow[] {
  const snap = (data as AusgridOutagesPayload | null) ?? null;
  if (!snap || !Array.isArray(snap.Markers)) return [];
  const out: ArchiveRow[] = [];
  for (const m of snap.Markers) {
    if (!m || typeof m !== 'object') continue;
    const title = m.Suburb || m.Cause || 'Outage';
    out.push({
      source,
      source_id: m.OutageId !== null && m.OutageId !== undefined ? String(m.OutageId) : null,
      fetched_at,
      lat: typeof m.Latitude === 'number' && Number.isFinite(m.Latitude) ? m.Latitude : null,
      lng: typeof m.Longitude === 'number' && Number.isFinite(m.Longitude) ? m.Longitude : null,
      category: m.OutageType || null,
      subcategory: m.Cause || null,
      data: { ...m, title },
    });
  }
  return out;
}

// Polling is on unless AUSGRID_DISABLED=true. It was off by default while
// Ausgrid's old /webapi/ endpoints answered 404; the flag stays as a switch
// for the next time the upstream breaks, so a dead URL is not polled every
// two minutes and shown as a failing source.
const AUSGRID_DISABLED = (process.env['AUSGRID_DISABLED'] ?? '').trim().toLowerCase() === 'true';

export function register(): void {
  if (AUSGRID_DISABLED) {
    // LiveStore hydrates every saved snapshot at startup, before sources
    // register. With polling off nothing replaces them, so the last outages
    // fetched before it was switched off were served as current, indefinitely.
    // Empty is the truth while Ausgrid is not being read.
    liveStore.delete('ausgrid');
    liveStore.delete('ausgrid_stats');
    return;
  }
  registerSource<AusgridOutagesPayload>({
    name: 'ausgrid',
    family: 'power',
    intervalMs: 120_000,
    fetch: fetchAusgridOutages,
    archiveItems: ausgridArchiveItems,
  });
  registerSource<unknown>({
    name: 'ausgrid_stats',
    family: 'power',
    intervalMs: 120_000,
    fetch: fetchAusgridStats,
  });
}

export default register;
