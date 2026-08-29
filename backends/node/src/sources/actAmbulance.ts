/**
 * ACT Ambulance Service responses — ESA incident feed -> GeoJSON.
 *
 * ACT Emergency Services Agency publishes one public JSON array covering
 * everything it knows about, which is NOT all ACT and NOT all ambulance:
 *
 *   region 'nsw'  — NSW RFS incidents, re-published by ESA. Their `id` is
 *                   a mangled RFS API URL
 *                   ('https___incidents_rfs_nsw_gov_au_api_v1_incidents_673202'),
 *                   i.e. the exact incidents our own `rfs` source already
 *                   ingests. Keeping them would double every NSW fire on
 *                   the map.
 *   region 'act', agency 'Fire' — ACT fire + hazard-reduction burns, which
 *                   the RFS feed also carries.
 *
 * So we keep only `region === 'act' && agency === 'Ambulance'`, and drop
 * responses already marked Finished — a completed job is not worth a row.
 *
 * Output is the same shape every other incident source emits, so
 * defaultArchiveItems() fans it out to one archive row per response with
 * no custom archiveItems hook:
 *
 *   { type: 'FeatureCollection', features: [...], count }
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';
import { sydneyUnixFromNaive } from '../lib/sydneyTime.js';

const ACT_ESA_URL = 'https://esa.act.gov.au/act-gov-esa/incidents/feed';

/** Raw feed record. Every value arrives as a string. */
interface EsaRecord {
  agency?: string;
  region?: string;
  event_type?: string;
  id?: string;
  title?: string;
  location?: string;
  latitude?: string;
  longitude?: string;
  type?: string;
  status?: string;
  status_filter_label?: string;
  alert_level?: string;
  time_of_call?: string;
  updated?: string;
  /** Feed generation time — deliberately NOT carried through, see below. */
  date?: string;
}

export interface ActAmbulanceFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    id: string;
    title: string;
    location_text: string;
    status: string;
    type: string;
    agency: string;
    /** Promoted to the archive's `category` column. */
    category: string;
    /** Promoted to the archive's `subcategory` column. */
    subcategory: string;
    /** Epoch seconds — becomes source_timestamp_unix. */
    timestamp: number | null;
    time_of_call: string;
    updated: string;
    is_active: boolean;
  };
}

export interface ActAmbulanceSnapshot {
  type: 'FeatureCollection';
  features: ActAmbulanceFeature[];
  count: number;
}

const EMPTY: ActAmbulanceSnapshot = { type: 'FeatureCollection', features: [], count: 0 };

const MONTHS: Record<string, number> = {
  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
  jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
};

/**
 * Parse the feed's '29 Aug 2026 06:20:16' stamps to epoch seconds.
 *
 * These are ACT wall-clock times, and Canberra shares Sydney's zone.
 * `new Date(s)` would read them in the server's zone (UTC in prod) and
 * land 10–11 hours in the future, so go through sydneyUnixFromNaive,
 * which resolves the offset at the actual instant and handles the DST
 * changeover days.
 */
export function parseEsaTimestamp(raw: string | undefined): number | null {
  const s = (raw ?? '').trim();
  if (!s) return null;
  const m = /^(\d{1,2})\s+([A-Za-z]{3})[a-z]*\s+(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/.exec(s);
  if (!m) return null;
  const mo = MONTHS[(m[2] ?? '').toLowerCase()];
  if (!mo) return null;
  return sydneyUnixFromNaive(
    Number(m[3]),
    mo,
    Number(m[1]),
    Number(m[4] ?? 0),
    Number(m[5] ?? 0),
    Number(m[6] ?? 0),
  );
}

/** Keep only live ACT ambulance responses. */
export function isWantedRecord(r: EsaRecord): boolean {
  if ((r.region ?? '').trim().toLowerCase() !== 'act') return false;
  if ((r.agency ?? '').trim().toLowerCase() !== 'ambulance') return false;
  // A response the feed has already closed out.
  if ((r.status ?? '').trim().toLowerCase() === 'finished') return false;
  return true;
}

/**
 * Map one feed record to a GeoJSON feature, or null when it can't be
 * placed on a map.
 *
 * Note what is NOT copied across: the feed's `date` field is its own
 * generation time — the same value on every record, changing on every
 * poll. It isn't in the archive writer's DEDUP_HASH_IGNORE, so carrying
 * it would make every record hash differently every minute and defeat
 * write-time dedup, inserting ~20 junk rows a minute forever.
 */
export function toFeature(r: EsaRecord): ActAmbulanceFeature | null {
  const id = (r.id ?? '').trim();
  if (!id) return null; // no source_id => never reaches the _latest sidecar

  const lat = Number.parseFloat((r.latitude ?? '').trim());
  const lng = Number.parseFloat((r.longitude ?? '').trim());
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;

  const status = (r.status ?? '').trim();
  const location = (r.location ?? '').trim();
  const agency = (r.agency ?? '').trim() || 'Ambulance';

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [lng, lat] },
    properties: {
      id,
      // Set explicitly: applyAliases only derives a title from
      // name/headline/streets/suburb, none of which this feed has, so
      // without it the logs page renders every response as "Unknown".
      title: (r.title ?? '').trim() || `Ambulance response — ${location || 'ACT'}`,
      // Ambulance locations are suburb-only; ESA publishes them coarse.
      location_text: location,
      status,
      type: (r.type ?? '').trim(),
      agency,
      // Agency, not type: `type` is always 'AMBULANCE RESPONSE', and
      // keeping the agency here leaves room for ACT Fire under the same
      // provider later. Status carries the useful filter dimension.
      category: agency,
      subcategory: status,
      timestamp: parseEsaTimestamp(r.updated) ?? parseEsaTimestamp(r.time_of_call),
      time_of_call: (r.time_of_call ?? '').trim(),
      updated: (r.updated ?? '').trim(),
      // Finished responses are filtered out above, so anything stored is live.
      is_active: true,
    },
  };
}

/** Throws on upstream failure so the poller's backoff engages. */
export async function fetchActAmbulance(): Promise<ActAmbulanceSnapshot> {
  const raw = await fetchJson<unknown>(ACT_ESA_URL, { timeoutMs: 20_000 });
  if (!Array.isArray(raw)) {
    throw new Error('act_ambulance: feed returned a non-array payload');
  }
  const features: ActAmbulanceFeature[] = [];
  for (const rec of raw as EsaRecord[]) {
    if (!rec || typeof rec !== 'object') continue;
    if (!isWantedRecord(rec)) continue;
    const f = toFeature(rec);
    if (f) features.push(f);
  }
  return { type: 'FeatureCollection', features, count: features.length };
}

export default function register(): void {
  registerSource<ActAmbulanceSnapshot>({
    name: 'act_ambulance',
    family: 'misc',
    intervalMs: 60_000,
    fetch: fetchActAmbulance,
  });
}

/** Helper for the route handler — live snapshot, or an empty collection
 *  while the poller hasn't filled it yet. */
export function actAmbulanceSnapshot(): ActAmbulanceSnapshot {
  return liveStore.getData<ActAmbulanceSnapshot>('act_ambulance') ?? EMPTY;
}
