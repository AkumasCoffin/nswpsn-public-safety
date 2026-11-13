/**
 * CitiPower (inner Melbourne) and Powercor (western Victoria) outages.
 *
 * The two distributors are one company and publish one file between them:
 * `{ROWSET: {createDateTime, ROW: [...]}}`, each row saying in BUSINESS which
 * network it is on. The file lists outages that are on now; it has no planned
 * work and nothing that says a row is planned, so every row is unplanned.
 *
 * Each distributor is its own source, so that each has its own store, history
 * and CITIPOWER_DISABLED / POWERCOR_DISABLED switch. Both read the same file,
 * and the poller runs them within seconds of each other, so the request is
 * kept for a minute and the second source is answered from the first one's
 * download.
 *
 * Times are Melbourne wall clock ("15:53 13-11-2026"). createDateTime is when
 * the file was written, not when any outage changed, and is not read.
 */
import { fetchJson } from '../shared/http.js';
import { STATE_ZONE, num, outageOf, registerOutages, text, titleCase, zonedUtc, type OutageRecord } from './common.js';

const FEED_URL = 'https://s3-ap-southeast-2.amazonaws.com/cppc-outage/outages.json';
const TIMEOUT_MS = 20_000;
const ZONE = STATE_ZONE.VIC;
/** How long one download answers both sources. Well under the 180 s between polls. */
const SHARE_MS = 60_000;

export type CppcBusiness = 'citipower' | 'powercor';

type Raw = Record<string, unknown>;

const isObject = (v: unknown): v is Raw => !!v && typeof v === 'object' && !Array.isArray(v);

/** "Crews Attending" → "Crews attending". */
function inWords(v: unknown): string {
  const s = text(v).replace(/\s+/g, ' ').toLowerCase();
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/**
 * The rows of the file. ROW is a list, but a file converted from XML writes
 * a single row as the object itself and no rows as nothing at all, so both
 * are taken. Throws for anything else.
 */
function rows(payload: unknown): Raw[] {
  const rowset = isObject(payload) ? payload['ROWSET'] : null;
  if (!isObject(rowset)) throw new Error('cppc: the answer has no ROWSET');
  const row = rowset['ROW'];
  if (row === undefined || row === null || row === '') return [];
  if (Array.isArray(row)) return row.filter(isObject);
  if (isObject(row)) return [row];
  throw new Error('cppc: ROW is neither a list nor a row');
}

function one(r: Raw): OutageRecord | null {
  const business = text(r['BUSINESS']).toLowerCase();
  if (business !== 'citipower' && business !== 'powercor') return null;
  const id = text(r['ORDER_ID']);
  if (!id) return null;
  const crew = text(r['CREW_STATUS']);
  // The file drops an outage once it is over. This is for a row that stays a
  // poll longer; "Partially Restored" still has customers off.
  if (/^(power )?restored$|^complete|^cancel/i.test(crew)) return null;

  // centre is [longitude, latitude].
  const centre = Array.isArray(r['centre']) ? r['centre'] : [];

  const details: Record<string, unknown> = {};
  if (crew) details['crew_status'] = inWords(crew);
  // PRIVATISED is the street, or the streets, the outage is on.
  const streets = titleCase(r['PRIVATISED']).replace(/\s*,\s*/g, ', ');
  if (streets) details['streets'] = streets;
  const postcode = text(r['POSTCODE']);
  if (postcode) details['postcode'] = postcode;
  // AREA is the council or depot area the town is in.
  const region = titleCase(r['AREA']);
  if (region) details['region'] = region;

  return outageOf(business, {
    id,
    kind: 'unplanned',
    area: titleCase(r['TOWN']),
    customers_affected: num(r['CUSTOMERS']),
    cause: text(r['CAUSE']),
    start_at: zonedUtc(r['START_TIME'], ZONE),
    restore_at: zonedUtc(r['ETR'], ZONE),
    lon: num(centre[0]),
    lat: num(centre[1]),
    // The feed's `geometry` is a 73-point circle about 50 m across drawn
    // around `centre`, not the area without power. Passing it on as the
    // outage's outline would claim a shape nobody measured.
    polygons: [],
    details,
  });
}

/**
 * Every outage in the file, CitiPower's and Powercor's both; each record's
 * `distributor` says whose it is. Throws when the answer has no ROWSET.
 */
export function parseCppc(payload: unknown): OutageRecord[] {
  const out: OutageRecord[] = [];
  for (const r of rows(payload)) {
    const record = one(r);
    if (record) out.push(record);
  }
  return out;
}

let shared: { at: number; request: Promise<OutageRecord[]> } | null = null;

/** Test hook: forget the shared download. */
export function _resetCppcShare(): void {
  shared = null;
}

/** The file's outages, downloaded at most once a minute. */
function readFile(): Promise<OutageRecord[]> {
  const now = Date.now();
  if (shared && now - shared.at < SHARE_MS) return shared.request;
  // The request itself is kept, not its answer, so a second source that asks
  // while the first is still downloading waits for the same one. A failure
  // is shared the same way: both polls fail and both keep their last good
  // snapshot, and the next poll, three minutes on, asks again.
  const request = fetchJson<unknown>(FEED_URL, { timeoutMs: TIMEOUT_MS }).then(parseCppc);
  shared = { at: now, request };
  return request;
}

export async function fetchCppc(business: CppcBusiness): Promise<OutageRecord[]> {
  return (await readFile()).filter((o) => o.distributor === business);
}

export default function register(): void {
  registerOutages({ name: 'citipower', intervalMs: 180_000, fetch: () => fetchCppc('citipower') });
  registerOutages({ name: 'powercor', intervalMs: 180_000, fetch: () => fetchCppc('powercor') });
}
