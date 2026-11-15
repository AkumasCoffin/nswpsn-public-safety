/**
 * Western Power outages (WA: Perth and the south-west grid).
 *
 * One public endpoint, the one Western Power's own outage map reads, answers
 * a flat array with every outage under way and every planned one to come:
 *
 *   { outageId: 'INCD-203314-X', outageType: 'F', startTime, restorationTime,
 *     onMap, outageUpdatedTime, affectedCustomers, areas: ['GREENMOUNT', …],
 *     latitudeCentroid, longitudeCentroid }
 *
 * outageType is one letter. In the November 2026 file:
 *   F  planned work still to come. All 130 start on a later day, on the hour
 *      or half hour, and run for a working day.
 *   P  planned work under way. Its start is the second the power went off
 *      (09:39:55), and its restoration time is on the hour.
 *   U  unplanned. It started to the second earlier the same day.
 * The letter at the end of the id is not the type: most F records end in -U.
 *
 * There is no cause, no status text and no outline, only a centre point.
 * Nothing in the list says an outage is over; Western Power takes it off the
 * list instead, so nothing is dropped here.
 *
 * Times are written 20261119T073000+08:00 and carry their own offset. The
 * update time is written in UTC (+00:00).
 */
import { fetchJson } from '../shared/http.js';
import { STATE_ZONE, num, outageOf, registerOutages, text, titleCase, zonedUtc, type OutageKind, type OutageRecord } from './common.js';

const ENDPOINT = 'https://www.westernpower.com.au/api/corp/outage/all-outages';
const ZONE = STATE_ZONE.WA;

const KINDS: Readonly<Record<string, OutageKind>> = { F: 'planned', P: 'planned', U: 'unplanned' };

/** Locality names in the order given, each once. */
function areaOf(v: unknown): string {
  const list = Array.isArray(v) ? v : [v];
  const names: string[] = [];
  for (const item of list) {
    const name = titleCase(item);
    if (name && !names.includes(name)) names.push(name);
  }
  return names.join(', ');
}

/**
 * Western Power's list as outage records. Throws when the answer is not a
 * list, or is a list in which nothing could be read as an outage: either way
 * it is not the file this was written for, and the last good snapshot should
 * stay.
 */
export function parseWesternPower(payload: unknown): OutageRecord[] {
  if (!Array.isArray(payload)) throw new Error('westernpower: outage list is not an array');
  const out: OutageRecord[] = [];
  for (const item of payload) {
    if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
    const r = item as Record<string, unknown>;
    const id = text(r['outageId']);
    const type = text(r['outageType']).toUpperCase();
    const kind = KINDS[type];
    // A letter not seen before could as well mean "cancelled" as a new kind
    // of outage, so it is left out rather than shown as a power cut.
    if (!id || !kind) continue;

    const start_at = zonedUtc(r['startTime'], ZONE);
    const details: Record<string, unknown> = { type };
    // False only on outages of one or two customers, which Western Power lists but
    // does not draw. It goes with the outage, not with the poll.
    if (typeof r['onMap'] === 'boolean') details['on_map'] = r['onMap'];

    out.push(
      outageOf('westernpower', {
        id,
        kind,
        // "Still to come" is Western Power's own word for an F record; it is
        // only needed when the start time that would say so cannot be read.
        ...(type === 'F' && !start_at ? { status: 'scheduled' as const } : {}),
        area: areaOf(r['areas']),
        customers_affected: num(r['affectedCustomers']),
        start_at,
        restore_at: zonedUtc(r['restorationTime'], ZONE),
        updated_at: zonedUtc(r['outageUpdatedTime'], ZONE),
        lat: num(r['latitudeCentroid']),
        lon: num(r['longitudeCentroid']),
        details,
      }),
    );
  }
  if (payload.length > 0 && out.length === 0) throw new Error('westernpower: no outage in the list could be read');
  return out;
}

export async function fetchWesternPower(): Promise<OutageRecord[]> {
  return parseWesternPower(await fetchJson<unknown>(ENDPOINT, { timeoutMs: 20_000 }));
}

export default function register(): void {
  registerOutages({ name: 'westernpower', intervalMs: 180_000, fetch: fetchWesternPower });
}
