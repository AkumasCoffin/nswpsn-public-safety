/**
 * Horizon Power outages (regional WA: Pilbara, Kimberley, Gascoyne, Mid West,
 * Esperance).
 *
 * One public endpoint, the one Horizon's outage map reads:
 *
 *   { statusCode: 200, isError: null, importedDateTime,
 *     result: { outages: [{ entityId, outageSK, outageType: 'Planned' | 'Unplanned',
 *       status: 'Started' | 'Scheduled', externalId: 'INCD-229775-a',
 *       startDateTime, endDate, estimatedFixDateTime, region: 'Mid West',
 *       serviceArea: 'Sandstone', createdDate, updatedDateTime, depotName,
 *       depotContactNumber, customersAffected: '17', centerLatitude,
 *       centerLongitude, radius, importedDateTime, affected }] } }
 *
 * Times have no zone and are Perth time. The centre is rounded to two decimal
 * places (about a kilometre) and `radius` is in metres, never under 100.
 *
 * An entry is one patch of an incident, not the incident: a job that switches
 * off three parts of Carnarvon is three entries with the same externalId and
 * the same times, each with its own customers, centre and radius. The stores
 * hold one record per id (registerOutages keeps the first and drops the rest),
 * so the patches of an incident are joined here into one record: customers
 * added up, placed at the patch with the most customers.
 *
 * `importedDateTime`, on the answer and on every entry, is when Horizon last
 * copied its data across. It moves with every import and is not kept.
 *
 * Horizon is slow to clear its list. The November 2026 file still held an
 * unplanned outage "Started" in August and planned work "Scheduled" for a
 * week earlier, all with no endDate. They are passed on as Horizon has them:
 * nothing in an entry tells stale from long-running.
 */
import { fetchJson } from '../shared/http.js';
import { STATE_ZONE, num, outageOf, registerOutages, text, titleCase, zonedUtc, type OutageKind, type OutageRecord } from './common.js';

const ENDPOINT = 'https://www.horizonpower.com.au/api/outage/getdetailedoutages';
const ZONE = STATE_ZONE.WA;

/**
 * Only "Started" and "Scheduled" have been seen. These are the words a
 * finished or called-off outage would most likely carry if Horizon ever lists
 * one, alongside the endDate it would also get.
 */
const OVER = /complet|cancel|restor|finish|resolv|closed/i;

/** One entry of the list, read. */
interface Patch {
  id: string;
  sk: number;
  kind: OutageKind;
  status: string;
  area: string;
  region: string;
  customers: number;
  start_at: string | null;
  restore_at: string | null;
  updated_at: string | null;
  created_at: string | null;
  lat: number | null;
  lon: number | null;
  radius: number | null;
  depot: string;
  depotPhone: string;
}

function patchOf(r: Record<string, unknown>, nowMs: number): Patch | null {
  const id = text(r['externalId']) || text(r['entityId']);
  if (!id) return null;
  const status = text(r['status']);
  if (OVER.test(status)) return null;
  const ended = zonedUtc(r['endDate'], ZONE);
  if (ended && Date.parse(ended) <= nowMs) return null;
  return {
    id,
    sk: num(r['outageSK']) ?? Number.MAX_SAFE_INTEGER,
    kind: /^planned/i.test(text(r['outageType'])) ? 'planned' : 'unplanned',
    status,
    area: titleCase(r['serviceArea']),
    region: text(r['region']),
    customers: num(r['customersAffected']) ?? 0,
    start_at: zonedUtc(r['startDateTime'], ZONE),
    // The estimate, or the end Horizon has set for an outage still running.
    restore_at: zonedUtc(r['estimatedFixDateTime'], ZONE) ?? ended,
    updated_at: zonedUtc(r['updatedDateTime'], ZONE),
    created_at: zonedUtc(r['createdDate'], ZONE),
    lat: num(r['centerLatitude']),
    lon: num(r['centerLongitude']),
    radius: num(r['radius']),
    depot: text(r['depotName']),
    depotPhone: text(r['depotContactNumber']),
  };
}

// ISO 8601 UTC text sorts in time order.
const earliest = (times: Array<string | null>): string | null => times.filter((t): t is string => !!t).sort()[0] ?? null;
const latest = (times: Array<string | null>): string | null => times.filter((t): t is string => !!t).sort().at(-1) ?? null;

/** The patches of one incident as one record. */
function incidentOf(patches: Patch[]): OutageRecord {
  // The patch with the most customers speaks for the incident. Ties go to the
  // one Horizon numbered first, so the choice does not depend on the order of
  // the list, which would rewrite the record's history row for nothing.
  const [main] = [...patches].sort((a, b) => b.customers - a.customers || a.sk - b.sk) as [Patch, ...Patch[]];
  const areas: string[] = [];
  for (const p of patches) if (p.area && !areas.includes(p.area)) areas.push(p.area);
  const start_at = earliest(patches.map((p) => p.start_at));

  const details: Record<string, unknown> = {};
  if (main.region) details['region'] = main.region;
  if (main.status) details['status'] = main.status;
  if (main.radius !== null) details['radius_m'] = main.radius;
  const created = earliest(patches.map((p) => p.created_at));
  if (created) details['created_at'] = created;
  if (patches.length > 1) details['patches'] = patches.length;
  if (main.depot) details['depot'] = main.depot;
  if (main.depotPhone) details['depot_phone'] = main.depotPhone;

  return outageOf('horizon', {
    id: main.id,
    kind: main.kind,
    // Horizon's word for planned work, needed only when there is no start
    // time for the clock to be asked about.
    ...(main.kind === 'planned' && !start_at && /^scheduled/i.test(main.status) ? { status: 'scheduled' as const } : {}),
    area: areas.join(', '),
    customers_affected: patches.reduce((sum, p) => sum + p.customers, 0),
    start_at,
    restore_at: latest(patches.map((p) => p.restore_at)),
    updated_at: latest(patches.map((p) => p.updated_at)),
    lat: main.lat,
    lon: main.lon,
    details,
  });
}

/**
 * Horizon's answer as outage records, one per incident, as of `nowMs`. Left
 * out: entries whose endDate has passed and entries whose status says they
 * are over. Throws when Horizon reports an error, when there is no list, or
 * when nothing in a list could be read as an outage, so the last good
 * snapshot stays.
 */
export function parseHorizon(payload: unknown, nowMs: number = Date.now()): OutageRecord[] {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error('horizon: answer is not an object');
  const body = payload as { isError?: unknown; message?: unknown; result?: unknown };
  // null when all is well, not false.
  if (body.isError === true) throw new Error(`horizon: upstream reported an error${text(body.message) ? `: ${text(body.message)}` : ''}`);
  const list = body.result && typeof body.result === 'object' ? (body.result as { outages?: unknown }).outages : undefined;
  if (!Array.isArray(list)) throw new Error('horizon: result.outages is not an array');

  const incidents = new Map<string, Patch[]>();
  let readable = 0;
  for (const item of list) {
    if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
    const r = item as Record<string, unknown>;
    if (text(r['externalId']) || text(r['entityId'])) readable++;
    const patch = patchOf(r, nowMs);
    if (!patch) continue;
    const patches = incidents.get(patch.id);
    if (patches) patches.push(patch);
    else incidents.set(patch.id, [patch]);
  }
  if (list.length > 0 && readable === 0) throw new Error('horizon: no outage in the list could be read');
  return [...incidents.values()].map(incidentOf);
}

export async function fetchHorizon(): Promise<OutageRecord[]> {
  return parseHorizon(await fetchJson<unknown>(ENDPOINT, { timeoutMs: 20_000 }));
}

export default function register(): void {
  // Off by default. The site sits behind Cloudflare, which answers this
  // server's HTTP client with a challenge page (403) on every request, though
  // it lets a plain command-line request with the same User-Agent through: it
  // is the client it objects to, not the address or the name. That is the
  // site's bot protection saying no, and it is not worked around. Set
  // HORIZON_DISABLED=false to turn it on if that changes.
  registerOutages({ name: 'horizon', intervalMs: 180_000, fetch: fetchHorizon, offByDefault: true });
}
