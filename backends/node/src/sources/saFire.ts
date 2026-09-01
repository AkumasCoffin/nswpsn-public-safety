/**
 * South Australia — CFS (country) and MFS (metropolitan) incidents.
 *
 * SA ESS publishes its dispatch feeds as flat JSON on S3:
 *
 *   /prod/cfs/criimson/cfs_current_incidents.json   country SA
 *   /prod/mfs/criimson/mfs_current_incidents.json   Adelaide metro
 *
 * Two agencies rather than two feeds of one agency — the Country Fire
 * Service and the Metropolitan Fire Service are separate services the
 * way the RFS and Fire & Rescue NSW are, so each gets its own provider.
 *
 * PROPERTY NAMES ARE THE RFS ONES, deliberately — `alertLevel`,
 * `fireType`, `status`, `location`. archiveExtract promotes
 * category <- alertLevel and subcategory <- fireType generically, so
 * these get the same facets as every other agency for free.
 *
 * NEITHER FEED IS GEOJSON. A record is a flat object, and its position
 * is a STRING: `"Location": "-34.735,138.798"`. It is reshaped into a
 * normal Point FeatureCollection here so the frontend path is unchanged.
 *
 * THREE THINGS THE DATA DICTATES.
 *
 * 1. A missing file comes back as HTTP 200 with an HTML body — SA ESS
 *    serves "SA ESS - File Unavailable" rather than a 404. Trusting the
 *    status code would parse an error page as data. fetchJson throws on
 *    the parse, and the array check below catches anything that parses
 *    but is not a feed.
 * 2. The two agencies share one dispatch sequence: 1722523 is a CFS
 *    incident and 1722545 an MFS one. They do not overlap today — no
 *    shared IncidentNo, no shared coordinates, and `Region` partitions
 *    them cleanly — but the guid is namespaced per agency so the
 *    sequence can never collide in the archive.
 * 3. Times are wall-clock with no offset, in a state that runs on the
 *    half hour. See adelaideIso().
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const CFS_URL = 'https://data.eso.sa.gov.au/prod/cfs/criimson/cfs_current_incidents.json';
const MFS_URL = 'https://data.eso.sa.gov.au/prod/mfs/criimson/mfs_current_incidents.json';

/** Which map layer a record belongs on. */
export type SaLayer = 'fire' | 'flood' | 'hazard';

/** Which of the two services published it. */
export type SaAgency = 'SACFS' | 'SAMFS';

export interface SaFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    title: string;
    guid: string;
    status: string;
    location: string;
    alertLevel: string;
    fireType: string;
    responsibleAgency: string;
    updated: string;
    updatedISO: string;
    layer: SaLayer;
    /** Fire ban district — "MOUNT LOFTY RANGES", "ADELAIDE METROPOLITAN". */
    district: string;
    /** CFS only: appliances and aircraft turned out to it. */
    resources: number | null;
    aircraft: number | null;
    /** CFS only: the public message, when one has been written. */
    text: string;
    url: string;
    is_active: boolean;
    agency: SaAgency;
    source: 'sa_cfs' | 'sa_mfs';
  };
}

export interface SaSnapshot {
  type: 'FeatureCollection';
  features: SaFeature[];
  count: number;
}

const EMPTY: SaSnapshot = { type: 'FeatureCollection', features: [], count: 0 };

function asString(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return '';
}

function tidy(v: unknown): string {
  return asString(v).replace(/\s+/g, ' ').trim();
}

function asInt(v: unknown): number | null {
  if (v === null || v === undefined || v === '') return null;
  const n = typeof v === 'number' ? v : Number(asString(v));
  return Number.isFinite(n) ? n : null;
}

/**
 * The Australian Warning System level, from the CFS's 1/2/3 scale.
 *
 * Their own definition, taken as published: Level 1 is an Advice, 2 a
 * Watch and Act, 3 an Emergency Warning. `Level` arrives as a number in
 * the CFS feed and as a string in the MFS one, so it is coerced before
 * comparing.
 */
export function saAlertLevel(level: unknown): string {
  switch (asInt(level)) {
    case 1:
      return 'Advice';
    case 2:
      return 'Watch and Act';
    case 3:
      return 'Emergency Warning';
    default:
      return '';
  }
}

/**
 * Where an SA record belongs, decided by the EVENT and not the
 * publisher — the rule Victoria established. Both services turn out to
 * far more than fires.
 *
 * ORDER IS LOAD-BEARING, and `accident` is the addition SA forces: it
 * writes "Vehicle Accident", which contains none of the words the other
 * states' crash patterns look for and would otherwise fall through to
 * the fire test and land a car crash on the bushfire layer.
 */
export function saLayerFor(text: string): SaLayer {
  const t = text.toLowerCase();
  if (/flood|water over|storm surge|tsunami/.test(t)) return 'flood';
  if (/hazmat|hazardous material|chemical|cbrne|radiolog|toxic/.test(t)) {
    return 'hazard';
  }
  if (/accident|road crash|crash|rescue|collision/.test(t)) return 'hazard';
  if (/fire|burn|smoke|alarm/.test(t)) return 'fire';
  return 'hazard';
}

/**
 * The wall-clock time these feeds publish, as a real instant.
 *
 * `Date` is DD/MM/YYYY and `Time` is HH:mm, with no offset — and South
 * Australia runs on the half hour, ACST +09:30 in winter and ACDT
 * +10:30 in summer. So the offset has to be resolved for that instant
 * rather than assumed.
 *
 * `new Date("01/09/2026")` is NOT an option: that string is the first
 * of September here and the ninth of January to a JavaScript engine.
 *
 * The offset is looked up twice. The first pass asks what Adelaide's
 * offset was at roughly the right moment; the second re-asks at the
 * corrected instant, which matters only for a time within a couple of
 * hours of a daylight-saving change but is cheap insurance.
 */
export function adelaideIso(dateStr: unknown, timeStr: unknown): string {
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(asString(dateStr));
  if (!m) return '';
  const dd = m[1]!.padStart(2, '0');
  const mm = m[2]!.padStart(2, '0');
  const yyyy = m[3]!;
  const t = /^(\d{1,2}):(\d{2})/.exec(asString(timeStr));
  const hh = (t ? t[1]! : '0').padStart(2, '0');
  const mi = t ? t[2]! : '00';

  const wall = Date.UTC(Number(yyyy), Number(mm) - 1, Number(dd), Number(hh), Number(mi));
  if (!Number.isFinite(wall)) return '';

  let offset = adelaideOffsetMinutes(wall);
  offset = adelaideOffsetMinutes(wall - offset * 60_000);

  const sign = offset >= 0 ? '+' : '-';
  const abs = Math.abs(offset);
  const oh = String(Math.floor(abs / 60)).padStart(2, '0');
  const om = String(abs % 60).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:00${sign}${oh}:${om}`;
}

/** Adelaide's UTC offset, in minutes, at a given instant. */
function adelaideOffsetMinutes(utcMs: number): number {
  const dtf = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Australia/Adelaide',
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  const parts: Record<string, string> = {};
  for (const p of dtf.formatToParts(new Date(utcMs))) parts[p.type] = p.value;
  const asUtc = Date.UTC(
    Number(parts['year']),
    Number(parts['month']) - 1,
    Number(parts['day']),
    Number(parts['hour']) % 24,
    Number(parts['minute']),
    Number(parts['second']),
  );
  return (asUtc - utcMs) / 60_000;
}

/** `"-34.735,138.798"` -> `[lon, lat]`, or null when it isn't one. */
function pointFrom(location: unknown): [number, number] | null {
  const parts = asString(location).split(',');
  if (parts.length < 2) return null;
  const lat = Number(parts[0]);
  const lon = Number(parts[1]);
  return Number.isFinite(lat) && Number.isFinite(lon) ? [lon, lat] : null;
}

/**
 * Where it is, without saying the same thing twice.
 *
 * `Location_name` already reads "HUMBUG SCRUB, KELLY HILL RD"; the fire
 * ban district adds the region around it. Compared on containment, not
 * equality, because the two overlap on metro records where the district
 * is "ADELAIDE METROPOLITAN" and the location names an Adelaide street.
 */
function locationFor(name: string, district: string): string {
  const parts: string[] = [];
  for (const part of [name, district]) {
    if (!part) continue;
    const u = part.toUpperCase();
    if (parts.some((p) => p.toUpperCase().includes(u) || u.includes(p.toUpperCase()))) {
      continue;
    }
    parts.push(part);
  }
  return parts.join(', ');
}

/** How much is turned out to it; '' when the feed says nothing. */
function resourcesFor(resources: number | null, aircraft: number | null): string {
  const bits: string[] = [];
  if (resources && resources > 0) {
    bits.push(`${resources} ${resources === 1 ? 'appliance' : 'appliances'}`);
  }
  if (aircraft && aircraft > 0) {
    bits.push(`${aircraft} ${aircraft === 1 ? 'aircraft' : 'aircraft'}`);
  }
  return bits.join(', ');
}

const AGENCY_URL: Record<SaAgency, string> = {
  SACFS: 'https://www.cfs.sa.gov.au/warnings-restrictions/warnings/incidents-warnings/',
  SAMFS: 'https://www.mfs.sa.gov.au/',
};

/** One SA record, or null when it can't be placed. */
export function toSaFeature(raw: unknown, agency: SaAgency): SaFeature | null {
  if (!raw || typeof raw !== 'object') return null;
  const p = raw as Record<string, unknown>;

  const id = asString(p['IncidentNo']);
  if (!id) return null;

  const coords = pointFrom(p['Location']);
  if (!coords) return null;

  const kind = tidy(p['Type']);
  const name = tidy(p['Location_name']);
  const district = tidy(p['FBD']);
  const status = tidy(p['Status']);
  const iso = adelaideIso(p['Date'], p['Time']);
  const resources = asInt(p['Resources']);
  const aircraft = asInt(p['Aircraft']);
  const isCfs = agency === 'SACFS';

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: coords },
    properties: {
      // Neither feed has a headline, so build one the way the CFS's own
      // incident table reads a row: what it is, and where.
      title: [kind, name].filter(Boolean).join(' - ') || `${agency} incident`,
      // Namespaced per agency: the two share one dispatch sequence.
      guid: `sa:${isCfs ? 'cfs' : 'mfs'}:${id}`,
      status,
      location: locationFor(name, district),
      alertLevel: saAlertLevel(p['Level']),
      fireType: kind,
      responsibleAgency: agency,
      updated: iso
        ? new Date(iso).toLocaleString('en-AU', { timeZone: 'Australia/Adelaide' })
        : '',
      updatedISO: iso,
      layer: saLayerFor(`${kind} ${name}`),
      district,
      resources,
      aircraft,
      text: tidy(p['Message']),
      url: tidy(p['Message_link']) || AGENCY_URL[agency],
      // COMPLETE is the feed's own word for finished. Everything else —
      // GOING, CONTROLLED — is still running.
      is_active: status.toUpperCase() !== 'COMPLETE',
      agency,
      source: isCfs ? 'sa_cfs' : 'sa_mfs',
    },
  };
}

/** The resources line, exposed so the frontend need not rebuild it. */
export function saResourcesText(f: SaFeature): string {
  return resourcesFor(f.properties.resources, f.properties.aircraft);
}

async function fetchFeed(url: string, agency: SaAgency, label: string): Promise<SaSnapshot> {
  const data = await fetchJson<unknown>(url, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
    timeoutMs: 20_000,
  });
  // These feeds are bare arrays. Anything else means SA ESS handed us
  // its "File Unavailable" page — which it serves with a 200, so the
  // status code proves nothing — or reshaped the feed. Throwing is what
  // makes the poller back off instead of publishing an empty state as
  // though South Australia had gone quiet.
  if (!Array.isArray(data)) {
    throw new Error(`${label}: feed did not return an array`);
  }
  const features: SaFeature[] = [];
  const seen = new Set<string>();
  for (const item of data) {
    const f = toSaFeature(item, agency);
    if (!f) continue;
    if (seen.has(f.properties.guid)) continue;
    seen.add(f.properties.guid);
    features.push(f);
  }
  return { type: 'FeatureCollection', features, count: features.length };
}

export function fetchSaCfs(): Promise<SaSnapshot> {
  return fetchFeed(CFS_URL, 'SACFS', 'sa_cfs');
}

export function fetchSaMfs(): Promise<SaSnapshot> {
  return fetchFeed(MFS_URL, 'SAMFS', 'sa_mfs');
}

export default function register(): void {
  // archive_rfs, beside every other fire agency.
  registerSource<SaSnapshot>({
    name: 'sa_cfs',
    family: 'rfs',
    intervalMs: 120_000,
    fetch: fetchSaCfs,
  });
  registerSource<SaSnapshot>({
    name: 'sa_mfs',
    family: 'rfs',
    intervalMs: 120_000,
    fetch: fetchSaMfs,
  });
}

export function saCfsSnapshot(): SaSnapshot {
  return liveStore.getData<SaSnapshot>('sa_cfs') ?? EMPTY;
}

export function saMfsSnapshot(): SaSnapshot {
  return liveStore.getData<SaSnapshot>('sa_mfs') ?? EMPTY;
}
