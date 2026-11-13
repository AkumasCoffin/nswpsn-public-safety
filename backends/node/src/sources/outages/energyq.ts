/**
 * Energex and Ergon Energy (Queensland) outage pollers.
 *
 * The two are one publisher (Energy Queensland) and write the same files, so
 * one parser reads both. Each has three GeoJSON files behind its outage map:
 *   ..._map_current_unplanned.geojson   outages under way
 *   ..._map_current_planned.geojson     planned work under way
 *   ..._map_future_planned.geojson      planned work to come
 * A file is a FeatureCollection with `company`, `state` ('Current' or
 * 'Future') and `extracted` (when it was written) beside the features.
 *
 * An unplanned outage's geometry is a GeometryCollection, a Point for the
 * marker and a Polygon for the area without power; planned work is a Point
 * only. Times are written "5:50PM 13 Nov 2026" on Brisbane's clock, which has
 * no daylight saving.
 *
 * The two publish different words in STATUS. Energex: Awaiting, In Progress,
 * Scheduled, Cancelled, Completed. Ergon writes a phrase: "Outage reported",
 * "Crews on route", "Crews working on-site", "Work scheduled", "On hold until
 * further notice". Work that is cancelled, completed or restored is left out;
 * work on hold is still listed by Ergon with its times, so it stays, with the
 * phrase in details.
 *
 * Both sites sit behind Cloudflare, whose bot check answers some requests
 * with an HTML challenge page (HTTP 403, "Just a moment…") in place of the
 * file. That is the site saying no to this request, and it is taken as that:
 * the read of that one file has failed. Nothing here tries to get past the
 * check: no browser-like headers or cookies, no solving the challenge, and no
 * asking again before the next poll. What keeps the map filled in is
 * keepParts: each file's last good answer stands in for a failed read for
 * half an hour, and the poll fails (leaving the stored snapshot as it is)
 * only when no file has anything to give. The three files are asked for one
 * after another, never together, and each distributor every five minutes, so
 * a poll is three single requests and not a burst.
 *
 * A job can be in the current-planned and the future-planned file at once
 * (work spread over several weeks); the current file's copy is the one kept.
 *
 * Registers `energex` and `ergon`, each Outage[] under its own name.
 */
import { fetchJson } from '../shared/http.js';
import {
  STATE_ZONE,
  keepParts,
  num,
  outageOf,
  outerRings,
  registerOutages,
  text,
  titleCase,
  zonedUtc,
  type OutageKind,
  type OutagePart,
  type OutageRecord,
} from './common.js';

export type EnergyqStore = 'energex' | 'ergon';

const BASE: Readonly<Record<EnergyqStore, string>> = {
  energex: 'https://www.energex.com.au/static/PRD/ex_map_',
  ergon: 'https://www.ergon.com.au/static/PRD/ee_map_',
};

/** The three files, in the order their records are kept: an outage under way before the same one to come. */
const FILES: ReadonlyArray<readonly [part: string, file: string]> = [
  ['unplanned', 'current_unplanned'],
  ['planned', 'current_planned'],
  ['future', 'future_planned'],
];

export const energyqUrl = (distributor: EnergyqStore, file: string): string => `${BASE[distributor]}${file}.geojson`;

const POLL_MS = 300_000;
const TIMEOUT_MS = 20_000;
const ZONE = STATE_ZONE.QLD;

/** Cancelled, completed, or the power is back. */
const OVER = /cancel|complete|restored/i;

/** "ARATULA, FRAZERVIEW" → "Aratula, Frazerview". */
const names = (v: unknown): string =>
  text(v)
    .split(',')
    .map((n) => titleCase(n))
    .filter(Boolean)
    .join(', ');

/**
 * The marker and the outline of one feature. A GeometryCollection gives a
 * Point and one or more Polygons; planned work is a bare Point.
 */
function shape(geometry: unknown): { lat: number | null; lon: number | null; polygons: number[][][] } {
  const out: { lat: number | null; lon: number | null; polygons: number[][][] } = { lat: null, lon: null, polygons: [] };
  if (!geometry || typeof geometry !== 'object') return out;
  const g = geometry as { type?: unknown; geometries?: unknown };
  const members: unknown[] = g.type === 'GeometryCollection' ? (Array.isArray(g.geometries) ? g.geometries : []) : [g];
  for (const member of members) {
    if (!member || typeof member !== 'object') continue;
    const m = member as { type?: unknown; coordinates?: unknown };
    if (m.type === 'Point') {
      if (out.lat !== null || !Array.isArray(m.coordinates)) continue;
      const lon = num(m.coordinates[0]);
      const lat = num(m.coordinates[1]);
      if (lon !== null && lat !== null) {
        out.lon = lon;
        out.lat = lat;
      }
    } else {
      out.polygons.push(...outerRings(m));
    }
  }
  return out;
}

/**
 * One file's outages. Throws when the payload is not a feature collection:
 * an error page read as "no outages" would empty the map.
 *
 * `extracted`, the time the file was written, is left off the records on
 * purpose: it moves with every poll and says nothing about any one outage.
 */
export function parseEnergyq(payload: unknown, distributor: EnergyqStore): OutageRecord[] {
  const file = payload as { features?: unknown; state?: unknown } | null;
  if (!file || typeof file !== 'object' || !Array.isArray(file.features)) {
    throw new Error(`${distributor}: outage file is not a feature collection`);
  }
  const future = text(file.state).toLowerCase() === 'future';

  const out: OutageRecord[] = [];
  for (const feature of file.features as Array<{ properties?: unknown; geometry?: unknown } | null>) {
    const p = feature && typeof feature === 'object' ? feature.properties : null;
    if (!p || typeof p !== 'object') continue;
    const props = p as Record<string, unknown>;

    const status = text(props['STATUS']);
    if (OVER.test(status)) continue;

    const kind: OutageKind = text(props['TYPE']).toUpperCase() === 'PLANNED' ? 'planned' : 'unplanned';
    const start_at = zonedUtc(props['START'], ZONE);
    const fix = text(props['EST_FIX_TIME']);
    const restore_at = zonedUtc(fix, ZONE);

    const details: Record<string, unknown> = {};
    const streets = names(props['STREETS']);
    if (streets) details['streets'] = streets;
    if (status) details['status'] = status;
    // Ergon writes "Under Investigation" where it has no estimate yet.
    if (fix && !restore_at) details['est_fix'] = fix;
    // Energex only, 'N' on every outage seen. Passed on as written.
    const etr = text(props['ETR_FLAG']);
    if (etr) details['etr_flag'] = etr;

    out.push(
      outageOf(distributor, {
        id: text(props['EVENT_ID']),
        kind,
        // Only where the start cannot be read: the future file's word for it.
        ...(kind === 'planned' && !start_at && future ? { status: 'scheduled' as const } : {}),
        area: names(props['SUBURBS']),
        customers_affected: num(props['CUSTOMERS_AFFECTED']),
        cause: text(props['REASON']).replace(/\s+/g, ' '),
        start_at,
        restore_at,
        ...shape(feature!.geometry),
        details,
      }),
    );
  }
  return out;
}

/**
 * Every outage a distributor lists, from its three files.
 *
 * keepParts starts all its parts at once, so each read here waits for the one
 * before it to finish, whether it answered or failed.
 */
export async function fetchEnergyq(distributor: EnergyqStore): Promise<OutageRecord[]> {
  let before: Promise<unknown> = Promise.resolve();
  const parts: Array<OutagePart<OutageRecord>> = FILES.map(([part, file]) => [
    part,
    () => {
      const read = before.then(async () =>
        parseEnergyq(await fetchJson<unknown>(energyqUrl(distributor, file), { timeoutMs: TIMEOUT_MS }), distributor),
      );
      before = read.catch(() => undefined);
      return read;
    },
  ]);
  const records = await keepParts(distributor, parts);

  // The same job in the current and the future file: the first one, which is
  // the current file's.
  const seen = new Set<string>();
  return records.filter((r) => {
    if (!r.id) return true;
    if (seen.has(r.id)) return false;
    seen.add(r.id);
    return true;
  });
}

export const fetchEnergex = (): Promise<OutageRecord[]> => fetchEnergyq('energex');
export const fetchErgon = (): Promise<OutageRecord[]> => fetchEnergyq('ergon');

export function register(): void {
  // Off by default. The site sits behind Cloudflare, which answers this
  // server's HTTP client with a challenge page (403) on every request, though
  // it lets a plain command-line request with the same User-Agent through: it
  // is the client it objects to, not the address or the name. That is the
  // site's bot protection saying no, and it is not worked around. Set
  // ENERGEX_DISABLED=false / ERGON_DISABLED=false to turn it on if that changes.
  registerOutages({ name: 'energex', intervalMs: POLL_MS, fetch: fetchEnergex, offByDefault: true });
  registerOutages({ name: 'ergon', intervalMs: POLL_MS, fetch: fetchErgon, offByDefault: true });
}

export default register;
