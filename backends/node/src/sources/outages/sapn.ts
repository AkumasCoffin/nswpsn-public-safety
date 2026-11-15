/**
 * SA Power Networks outage poller.
 *
 * The outage map reads two public endpoints, and so does this:
 *   /Outages/GetPublicisedCurrentOutages/   { currentOutages, lastUpdated }
 *     Unplanned outages under way. A handful of records.
 *   /Outages/GetPublicisedPlannedOutages/   { plannedOutages, lastUpdated }
 *     Planned work for the next fortnight, today's included whether it has
 *     started, finished or neither. About 270 records, 400 KB.
 *
 * Times are written without a zone, on Adelaide's clock (UTC+10:30 in summer,
 * UTC+9:30 in winter). An outage has no marker, only `geometry`, the outline
 * of the area as a list of {lat, lng}; its position is the middle of that
 * outline.
 *
 * The planned list keeps the day's work after it has ended, still marked
 * "Scheduled", so anything whose end time has passed is left out, here and
 * again each poll (the list is read every fifteen minutes; work ends in
 * between).
 *
 * One job can be listed several times, once per interruption: a morning and
 * an afternoon cut on the same day, or a second area for part of the night.
 * They share a jobID, and the merged feed keeps one record per id, so each
 * interruption of such a job takes the job number with its own times. A job
 * listed once, which is nearly all of them, keeps the bare number.
 *
 * `lastUpdated` is the time the answer was made. It moves with every poll and
 * is left off the records.
 *
 * Registers `sapn`, Outage[], every three minutes.
 */
import { fetchJson } from '../shared/http.js';
import {
  STATE_ZONE,
  keepParts,
  num,
  outageOf,
  registerOutages,
  ring,
  text,
  titleCase,
  zonedUtc,
  type OutageRecord,
} from './common.js';

const BASE = 'https://outage.apps.sapowernetworks.com.au/Outages';
export const SAPN_CURRENT_URL = `${BASE}/GetPublicisedCurrentOutages/`;
export const SAPN_PLANNED_URL = `${BASE}/GetPublicisedPlannedOutages/`;

const POLL_MS = 180_000;
/** Planned work is booked days ahead and rarely changes. */
const PLANNED_REFRESH_MS = 900_000;
const TIMEOUT_MS = 20_000;
/** The planned list is about 400 KB. */
const PLANNED_TIMEOUT_MS = 30_000;
const ZONE = STATE_ZONE.SA;

/** Cancelled, completed, or the power is back. */
const OVER = /cancel|complete|restored/i;

type Raw = Record<string, unknown>;

/** The list under `key`; throws when the answer does not hold one. */
function listOf(payload: unknown, key: string): Raw[] {
  const list = payload && typeof payload === 'object' ? (payload as Raw)[key] : null;
  if (!Array.isArray(list)) throw new Error(`sapn: ${key} is not a list`);
  return list.filter((o): o is Raw => !!o && typeof o === 'object' && !Array.isArray(o));
}

/**
 * The outline(s) of an outage and the middle of them. `geometry` has been a
 * single list of points in every answer seen; a list of such lists is read
 * as several areas.
 */
function shape(geometry: unknown): { lat: number | null; lon: number | null; polygons: number[][][] } {
  const areas: unknown[] = Array.isArray(geometry) && Array.isArray(geometry[0]) ? geometry : [geometry];
  const polygons = areas.map((a) => ring(a)).filter((r) => r.length > 0);
  // The plain average of the outline's points: near enough to the middle to
  // put a pin on, and SAPN gives nothing better.
  let lon = 0;
  let lat = 0;
  let n = 0;
  for (const r of polygons) {
    for (const [x, y] of r) {
      lon += x!;
      lat += y!;
      n++;
    }
  }
  return n ? { lat: lat / n, lon: lon / n, polygons } : { lat: null, lon: null, polygons };
}

/** Locality names for `area`, and their postcodes (some localities have none). */
function suburbs(v: unknown): { area: string; postcodes: string[] } {
  const names: string[] = [];
  const postcodes = new Set<string>();
  for (const s of Array.isArray(v) ? (v as Array<Raw | null>) : []) {
    if (!s || typeof s !== 'object') continue;
    const name = titleCase(s['name']);
    if (name && !names.includes(name)) names.push(name);
    const postcode = text(s['postcode']);
    if (postcode) postcodes.add(postcode);
  }
  return { area: names.join(', '), postcodes: [...postcodes] };
}

/** Unplanned outages under way. Throws when the answer is not the current list. */
export function parseSapnCurrent(payload: unknown): OutageRecord[] {
  const out: OutageRecord[] = [];
  for (const o of listOf(payload, 'currentOutages')) {
    const status = text(o['status']);
    if (OVER.test(status)) continue;
    const { area, postcodes } = suburbs(o['affectedSuburbs']);
    const details: Record<string, unknown> = {};
    if (status) details['status'] = status;
    if (postcodes.length) details['postcodes'] = postcodes;
    // SAPN's own flag, false on every outage seen; what it stands for is not
    // in the data, so it is passed on as given and shown nowhere.
    if (typeof o['isPaw'] === 'boolean') details['is_paw'] = o['isPaw'];
    out.push(
      outageOf('sapn', {
        id: text(o['jobID']),
        kind: 'unplanned',
        area,
        customers_affected: num(o['affectedCustomers']),
        cause: text(o['reason']),
        start_at: zonedUtc(o['startDateTime'], ZONE),
        restore_at: zonedUtc(o['estimatedRestoration'], ZONE),
        ...shape(o['geometry']),
        details,
      }),
    );
  }
  return out;
}

/** Planned work whose end time has passed. */
const ended = (r: OutageRecord, nowMs: number): boolean =>
  r.kind === 'planned' && r.restore_at !== null && Date.parse(r.restore_at) <= nowMs;

/**
 * Planned work under way or to come, as of `nowMs`. Throws when the answer
 * is not the planned list.
 */
export function parseSapnPlanned(payload: unknown, nowMs: number = Date.now()): OutageRecord[] {
  const list = listOf(payload, 'plannedOutages');
  const listed = new Map<string, number>();
  for (const o of list) {
    const job = text(o['jobID']);
    listed.set(job, (listed.get(job) ?? 0) + 1);
  }

  const out: OutageRecord[] = [];
  for (const o of list) {
    const status = text(o['status']);
    if (OVER.test(status)) continue;
    const job = text(o['jobID']);
    const start = text(o['startDateTime']);
    const end = text(o['endDateTime']);
    const { area, postcodes } = suburbs(o['affectedSuburbs']);
    const details: Record<string, unknown> = {};
    if (status) details['status'] = status;
    if (postcodes.length) details['postcodes'] = postcodes;

    // One interruption of a job that has several: the job number alone would
    // be the same id for each. Two can start together and end apart, so both
    // times are in it, to the minute.
    let id = job;
    if (job && (listed.get(job) ?? 0) > 1) {
      id = `${job}@${start.slice(0, 16)}~${end.slice(0, 16)}`;
      details['job_id'] = job;
    }

    const record = outageOf('sapn', {
      id,
      kind: 'planned',
      area,
      customers_affected: num(o['affectedCustomers']),
      cause: text(o['reason']),
      start_at: zonedUtc(start, ZONE),
      restore_at: zonedUtc(end, ZONE),
      ...shape(o['geometry']),
      details,
    });
    if (!ended(record, nowMs)) out.push(record);
  }
  return out;
}

export async function fetchSapn(): Promise<OutageRecord[]> {
  const records = await keepParts<OutageRecord>('sapn', [
    ['current', async () => parseSapnCurrent(await fetchJson<unknown>(SAPN_CURRENT_URL, { timeoutMs: TIMEOUT_MS }))],
    [
      'planned',
      async () => parseSapnPlanned(await fetchJson<unknown>(SAPN_PLANNED_URL, { timeoutMs: PLANNED_TIMEOUT_MS })),
      PLANNED_REFRESH_MS,
    ],
  ]);
  // The planned list in hand can be a quarter of an hour old, or older when
  // the read is failing: work that has ended since goes.
  const now = Date.now();
  return records.filter((r) => !ended(r, now));
}

export function register(): void {
  registerOutages({ name: 'sapn', intervalMs: POLL_MS, fetch: fetchSapn });
}

export default register;
