/**
 * AusNet (eastern and north-eastern Victoria) outages.
 *
 * One public endpoint, the one AusNet's outage tracker reads, answers
 * `{data: [...], errors, success}` with every incident it knows about: about
 * 2,300 records and 2 MB, of which two thirds are cancelled planned work and
 * a handful are leftovers from 2024 and 2025 that were never closed. What is
 * kept is what is under way or still to come:
 *
 *   - not cancelled, completed, restored or de-logged, and with no actual
 *     restoration time;
 *   - planned work whose last planned time is no more than a day old;
 *   - planned work that has a date at all. The leftovers have none, and no
 *     customers or towns either.
 *
 * Planned work runs from plannedStartTime to plannedEndTime. An unplanned
 * outage starts at unplannedStartTime and its restore time is AusNet's latest
 * estimate. On planned work unplannedStartTime is when the job was entered,
 * weeks before the work, so it is not read there.
 *
 * Times are Melbourne wall clock ("2026-11-13 06:55PM").
 */
import { fetchJson } from '../shared/http.js';
import { STATE_ZONE, num, outageOf, registerOutages, text, titleCase, zonedUtc, type OutageRecord } from './common.js';

const FEED_URL = 'https://outagetrackerservice.ausnetservices.com.au/api/v1/outages/combinedoutage';
/** The answer is about 2 MB; give it longer than the default 15 s. */
const TIMEOUT_MS = 30_000;
const ZONE = STATE_ZONE.VIC;
/** Planned work that ended longer ago than this is a record AusNet never closed. */
const STALE_MS = 24 * 60 * 60_000;

type Raw = Record<string, unknown>;

const isObject = (v: unknown): v is Raw => !!v && typeof v === 'object' && !Array.isArray(v);

/** "CONSTRUCTION RESPONDING" → "Construction responding". */
function inWords(v: unknown): string {
  const s = text(v).replace(/\s+/g, ' ').toLowerCase();
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/**
 * incidentLastUpdated is written "2026-11-13 16:58:03.163723 Australia/Melbourne"
 * and sometimes "… +11:00". The zone's name is taken off, since it is the
 * zone the time is read in anyway; an offset is left for zonedUtc.
 */
const incidentTime = (v: unknown): string | null => zonedUtc(text(v).replace(/\s+[A-Za-z]+\/[A-Za-z_]+$/, ''), ZONE);

/** The later of two ISO times; either may be null. */
const later = (a: string | null, b: string | null): string | null => (a && b ? (a > b ? a : b) : a ?? b);

/** The work was called off or finished, or the power is back. */
function isOver(r: Raw): boolean {
  const incident = text(r['incidentStatus']).toLowerCase();
  const status = text(r['status']).toLowerCase();
  // "Delog" is an incident taken off AusNet's log. The one in the feed is
  // from 2024.
  if (incident === 'cancelled' || incident === 'completed' || incident === 'delog') return true;
  if (status === 'cancelled' || status === 'restored') return true;
  return text(r['actualTimeOfRestoration']) !== '';
}

function one(r: Raw, nowMs: number): OutageRecord | null {
  const id = text(r['id']) || text(r['incident']);
  if (!id || isOver(r)) return null;
  const planned = text(r['type']).toLowerCase() === 'planned';

  let startText = '';
  let start_at: string | null;
  let restore_at: string | null;
  if (planned) {
    // A few records give the day of the work only as plannedDate.
    startText = text(r['plannedStartTime']) || text(r['plannedDate']);
    const endText = text(r['plannedEndTime']);
    if (!startText && !endText) return null;
    start_at = zonedUtc(startText, ZONE);
    restore_at = zonedUtc(endText, ZONE);
    const last = restore_at ?? start_at;
    if (last && Date.parse(last) < nowMs - STALE_MS) return null;
  } else {
    start_at = zonedUtc(r['unplannedStartTime'], ZONE);
    restore_at = zonedUtc(r['latestEstimatedTimeToRestoration'], ZONE) ?? zonedUtc(r['initialEstimatedTimeToRestoration'], ZONE);
  }

  const towns: string[] = [];
  const postcodes: string[] = [];
  let townCustomers = 0;
  // `details` is a list of {townName, postCode, customerCount}, empty when
  // AusNet has not worked out who is affected.
  for (const d of Array.isArray(r['details']) ? r['details'] : []) {
    if (!isObject(d)) continue;
    const town = titleCase(d['townName']);
    if (town && !towns.includes(town)) towns.push(town);
    const postcode = text(d['postCode']);
    if (postcode && !postcodes.includes(postcode)) postcodes.push(postcode);
    townCustomers += num(d['customerCount']) ?? 0;
  }

  const details: Record<string, unknown> = {};
  const status = inWords(r['status']);
  if (status) details['status'] = status;
  const incidentStatus = text(r['incidentStatus']);
  if (incidentStatus) details['incident_status'] = incidentStatus;
  if (postcodes.length) details['postcode'] = postcodes.join(', ');
  const delayReason = text(r['delayReason']);
  if (delayReason) details['delay_reason'] = delayReason;
  const etrReason = text(r['etrUpdateReason']);
  if (etrReason) details['estimate_reason'] = etrReason;
  if (text(r['plannedDelayed']).toLowerCase() === 'true') details['delayed'] = true;
  if (text(r['emergencyFlag']).toUpperCase() === 'Y') details['emergency'] = true;

  const record = outageOf('ausnet', {
    id,
    kind: planned ? 'planned' : 'unplanned',
    area: towns.join(', '),
    customers_affected: num(r['nmiCount']) || townCustomers,
    cause: text(r['cause']),
    start_at,
    restore_at,
    updated_at: later(incidentTime(r['incidentLastUpdated']), zonedUtc(r['statusLastUpdated'], ZONE)),
    lat: num(r['latitude']),
    lon: num(r['longitude']),
    details,
  });
  // Only when the start is written in a way that cannot be read: AusNet's
  // word is then all there is to say the work has not begun.
  if (planned && !start_at && text(r['status']).toUpperCase() === 'SCHEDULED') record.status = 'scheduled';
  return record;
}

/**
 * Every AusNet outage under way or to come. Throws when the answer is not
 * `{success: true, data: [...]}`. `nowMs` is what "more than a day ago" is
 * measured from.
 */
export function parseAusnet(payload: unknown, nowMs: number = Date.now()): OutageRecord[] {
  if (!isObject(payload) || payload['success'] !== true || !Array.isArray(payload['data'])) {
    throw new Error('ausnet: the answer is not a successful list of outages');
  }
  const out: OutageRecord[] = [];
  for (const r of payload['data']) {
    const record = isObject(r) ? one(r, nowMs) : null;
    if (record) out.push(record);
  }
  return out;
}

export async function fetchAusnet(): Promise<OutageRecord[]> {
  return parseAusnet(await fetchJson<unknown>(FEED_URL, { timeoutMs: TIMEOUT_MS }));
}

export default function register(): void {
  registerOutages({ name: 'ausnet', intervalMs: 300_000, fetch: fetchAusnet });
}
