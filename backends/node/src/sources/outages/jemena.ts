/**
 * Jemena (north-western Melbourne) outages.
 *
 * One public file, a list of events. An event stays in the file for a while
 * after it is over, with Status "Completed" or "Complete" and a
 * RestorationTime; those are left out.
 *
 * An event's top level describes what is still off: where, which suburbs and
 * streets, how many customers. `History` holds the parts of the outage that
 * have already been restored, each with its own place, suburbs, customer
 * count and RestorationTime. It is roughly oldest first but not strictly so.
 * When the top level has no place or no suburbs, they are taken from the
 * part restored last, which is the nearest thing to where the outage still
 * is. A customer count is taken from there only when the top level gives
 * none at all: a 0 at the top level means everybody listed is back on.
 *
 * Times without a zone are Melbourne wall clock; LastUpdatedTime has a Z.
 */
import { fetchJson } from '../shared/http.js';
import { STATE_ZONE, num, outageOf, outerRings, registerOutages, ring, text, titleCase, zonedUtc, type OutageRecord } from './common.js';

const FEED_URL = 'https://poweroutages.jemena.com.au/data/all-outages.json';
const TIMEOUT_MS = 20_000;
const ZONE = STATE_ZONE.VIC;

type Raw = Record<string, unknown>;

const isObject = (v: unknown): v is Raw => !!v && typeof v === 'object' && !Array.isArray(v);

/** "In Progress (ZM10)" → "In progress": the bracketed code is Jemena's own and means nothing to a reader. */
function statusWords(v: unknown): string {
  const s = text(v).replace(/\s*\([^)]*\)/g, '').replace(/\s+/g, ' ').trim().toLowerCase();
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/** The restored parts of an event, the one restored last first. */
function restoredParts(event: Raw): Raw[] {
  const parts = (Array.isArray(event['History']) ? event['History'] : []).filter(isObject);
  const when = (p: Raw): number => {
    const t = zonedUtc(p['RestorationTime'], ZONE);
    return t ? Date.parse(t) : Number.NEGATIVE_INFINITY;
  };
  // Later in the list wins between two parts with the same time, or with none.
  return parts
    .map((part, i) => ({ part, i, t: when(part) }))
    .sort((a, b) => b.t - a.t || b.i - a.i)
    .map((x) => x.part);
}

/** The first of the event and its restored parts for which `read` finds something. */
function firstWith<T>(sources: Raw[], read: (o: Raw) => T | null): T | null {
  for (const o of sources) {
    const found = read(o);
    if (found !== null) return found;
  }
  return null;
}

function position(o: Raw): { lat: number; lon: number } | null {
  const loc = o['Location'];
  if (isObject(loc)) {
    const lat = num(loc['Latitude']);
    const lon = num(loc['Longitude']);
    if (lat !== null && lon !== null) return { lat, lon };
  }
  // The same point as GeoJSON, [longitude, latitude].
  const feature = o['LocationGeoJson'];
  const geometry = isObject(feature) ? feature['geometry'] : null;
  const point = isObject(geometry) && geometry['type'] === 'Point' && Array.isArray(geometry['coordinates']) ? geometry['coordinates'] : [];
  const lon = num(point[0]);
  const lat = num(point[1]);
  return lat !== null && lon !== null ? { lat, lon } : null;
}

function suburbs(o: Raw): Raw[] | null {
  const list = (Array.isArray(o['ImpactedSuburbs']) ? o['ImpactedSuburbs'] : []).filter(isObject);
  return list.length ? list : null;
}

function outline(o: Raw): number[][][] | null {
  const fromGeoJson = outerRings(o['ImpactedAreaGeoJson']);
  if (fromGeoJson.length) return fromGeoJson;
  // The same outline as rings of {Latitude, Longitude}. ring() reads those
  // names in lower case only, so each point is handed over as a pair.
  const rings = (Array.isArray(o['ImpactedArea']) ? o['ImpactedArea'] : [])
    .map((r) => ring((Array.isArray(r) ? r : []).map((p) => (isObject(p) ? [p['Longitude'], p['Latitude']] : p))))
    .filter((r) => r.length > 0);
  return rings.length ? rings : null;
}

/** The `properties` Jemena writes on an event's GeoJSON, where a few fields live only. */
function properties(event: Raw): Raw {
  for (const key of ['LocationGeoJson', 'ImpactedAreaGeoJson']) {
    const feature = event[key];
    if (isObject(feature) && isObject(feature['properties'])) return feature['properties'];
  }
  return {};
}

function one(event: Raw): OutageRecord | null {
  const id = text(event['EventId']);
  if (!id) return null;
  const status = statusWords(event['Status']);
  if (/^complete/i.test(status) || /^(restored|cancelled)$/i.test(status)) return null;
  if (text(event['RestorationTime'])) return null;

  const props = properties(event);
  const planned = text(event['Type']).toLowerCase() === 'planned';
  const time = (...values: unknown[]): string | null => {
    for (const v of values) {
      const t = zonedUtc(v, ZONE);
      if (t) return t;
    }
    return null;
  };
  // Planned work is listed from its planned start, so that it reads as
  // scheduled until then; StartTime is when the power actually went off.
  const start_at = planned
    ? time(event['PlannedStartTime'], props['plannedStartTime'], event['StartTime'])
    : time(event['StartTime'], props['startTime']);
  const restore_at = planned
    ? time(event['EstimatedRestorationTime'], props['etr'], event['PlannedEndTime'], props['plannedEndTime'])
    : time(event['EstimatedRestorationTime'], props['etr']);

  const sources = [event, ...restoredParts(event)];
  const at = firstWith(sources, position);
  const where = firstWith(sources, suburbs) ?? [];
  const customers = firstWith(sources, (o) => num(o['ImpactedCustomers'])) ?? num(props['customersOff']);

  const names: string[] = [];
  const postcodes: string[] = [];
  const streets: string[] = [];
  for (const s of where) {
    const name = titleCase(s['SuburbName']);
    if (name && !names.includes(name)) names.push(name);
    const postcode = text(s['PostCode']);
    if (postcode && !postcodes.includes(postcode)) postcodes.push(postcode);
    for (const street of Array.isArray(s['Streets']) ? s['Streets'] : []) {
      const st = titleCase(street);
      if (st && !streets.includes(st)) streets.push(st);
    }
  }

  const details: Record<string, unknown> = {};
  if (status) details['status'] = status;
  if (streets.length) details['streets'] = streets.join(', ');
  if (postcodes.length) details['postcode'] = postcodes.join(', ');
  // The GeoJSON carries a finer cause than the event ("Vehicle Damage-" under
  // "Third party asset damage").
  const cause = text(event['Cause']);
  const reason = text(props['cause']).replace(/[\s-]+$/, '');
  if (reason && reason.toLowerCase() !== cause.toLowerCase()) details['reason'] = reason;

  return outageOf('jemena', {
    id,
    kind: planned ? 'planned' : 'unplanned',
    area: names.join(', '),
    customers_affected: customers,
    cause: cause || reason,
    start_at,
    restore_at,
    // LastUpdatedTime is the same on every event in the file: it is when the
    // file was written. The GeoJSON's lastUpdateTime is the event's own.
    updated_at: time(props['lastUpdateTime'], event['LastUpdatedTime']),
    lat: at?.lat ?? null,
    lon: at?.lon ?? null,
    polygons: firstWith(sources, outline) ?? [],
    details,
  });
}

/** Every Jemena outage under way or to come. Throws when the answer is not a list. */
export function parseJemena(payload: unknown): OutageRecord[] {
  if (!Array.isArray(payload)) throw new Error('jemena: the answer is not a list of outages');
  const out: OutageRecord[] = [];
  for (const event of payload) {
    const record = isObject(event) ? one(event) : null;
    if (record) out.push(record);
  }
  return out;
}

export async function fetchJemena(): Promise<OutageRecord[]> {
  return parseJemena(await fetchJson<unknown>(FEED_URL, { timeoutMs: TIMEOUT_MS }));
}

export default function register(): void {
  registerOutages({ name: 'jemena', intervalMs: 180_000, fetch: fetchJemena });
}
