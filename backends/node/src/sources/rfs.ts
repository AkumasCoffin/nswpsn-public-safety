/**
 * RFS Major Incidents — XML feed -> GeoJSON FeatureCollection.
 *
 * Mirrors the Python `/api/rfs/incidents` route at
 * external_api_proxy.py:10621. We pull from the same upstream feed and
 * preserve the same JSON shape so the frontend doesn't have to know
 * which backend served the request:
 *
 *   { type: 'FeatureCollection', features: [...], count }
 *
 * Each feature's properties include the parsed-out alertLevel /
 * location / size / status / etc. fields the Python parser extracts
 * from the RFS description's pipe-delimited free text.
 *
 * CAP. The RFS also publishes majorIncidentsCAP.xml - the same 33
 * incidents as proper CAP-AU alerts inside an EDXL-DE envelope - and it
 * carries what the RSS free text does not: severity, urgency, certainty,
 * responseType, the public instruction, and structured parameters like
 * fuel type and fire danger class. Both feeds are read and joined on the
 * CAP <incidents> element, which is the incident id at the tail of the
 * RSS <guid> (33/33 when this was written).
 *
 * The join is best-effort on purpose. CAP is enrichment, not the record:
 * if that feed is down or reshaped, the incidents still publish without
 * it rather than the whole layer going dark. Only the RSS fetch failing
 * throws, because that is the one that means we have no incidents.
 */
import { fetchText } from './shared/http.js';
import { asArray, parseXml, textOf } from './shared/xml.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const RFS_URL = 'https://www.rfs.nsw.gov.au/feeds/majorIncidents.xml';
const RFS_CAP_URL = 'https://www.rfs.nsw.gov.au/feeds/majorIncidentsCAP.xml';

export interface RfsFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    title: string;
    link: string;
    guid: string;
    description: string;
    status: string;
    location: string;
    size: string;
    alertLevel: string;
    fireType: string;
    councilArea: string;
    responsibleAgency: string;
    updated: string;
    updatedISO: string;
    polygons: string[];
    /**
     * The CAP <info> block, when the CAP feed carries this incident.
     * Empty strings rather than undefined so the frontend can render
     * them without a presence check, the same shape the Victorian
     * source emits.
     */
    capEvent: string;
    capCategory: string;
    capSeverity: string;
    capUrgency: string;
    capCertainty: string;
    capResponseType: string;
    capSender: string;
    capHeadline: string;
    capInstruction: string;
    capEffective: string;
    capExpires: string;
    capWeb: string;
    /** Structured CAP <parameter> entries the RSS text has no field for. */
    capFuelType: string;
    capFireDangerClass: string;
    capControlAuthority: string;
    capAllocatedResources: string;
    source: 'rfs';
  };
}

export interface RfsSnapshot {
  type: 'FeatureCollection';
  features: RfsFeature[];
  count: number;
}

interface ParsedDescription {
  alertLevel: string;
  location: string;
  councilArea: string;
  status: string;
  fireType: string;
  size: string;
  responsibleAgency: string;
  updated: string;
  updatedISO: string;
}

/**
 * Convert a free-text RFS local time like "7 Jan 2026 13:35" to an ISO
 * string with the actual Australia/Sydney offset for that wall-clock
 * minute. Mirrors python's `parse_rfs_local_time` which uses
 * `zoneinfo('Australia/Sydney')`.
 *
 * Earlier revisions used a month-bucketed approximation (Apr–Oct →
 * +10:00, otherwise +11:00) — that's wrong for the ~14 days each year
 * around the DST boundary (1st Sunday in October / 1st Sunday in
 * April). Use Intl.DateTimeFormat with timeZone='Australia/Sydney'
 * to derive the actual offset Postgres / consumers expect.
 */
function sydneyOffset(year: number, month: number, day: number, hour: number, minute: number): string {
  // Build a Date that represents the *wall-clock* moment in Sydney by
  // first parsing it as UTC, then asking Intl what offset Sydney would
  // emit for that instant. The 1-tick correction handles DST gap minutes
  // (Sydney has none in spring-forward — 02:00 jumps to 03:00 — but
  // formatToParts is well-defined for any input).
  const utcGuess = new Date(Date.UTC(year, month - 1, day, hour, minute));
  const fmt = new Intl.DateTimeFormat('en-AU', {
    timeZone: 'Australia/Sydney',
    timeZoneName: 'longOffset',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false,
  });
  const parts = fmt.formatToParts(utcGuess);
  const tzName = parts.find((p) => p.type === 'timeZoneName')?.value ?? 'GMT+10:00';
  // longOffset emits like "GMT+10:00" or "GMT+11:00".
  const m = /GMT([+-]\d{2}:\d{2})/.exec(tzName);
  return m ? (m[1] ?? '+10:00') : '+10:00';
}

function parseRfsLocalTime(s: string): string {
  if (!s) return '';
  const m = s.trim().match(/^(\d{1,2})\s+(\w{3})\s+(\d{4})\s+(\d{1,2}):(\d{2})$/);
  if (!m) return s;
  const [, dStr, monStr, yStr, hStr, miStr] = m;
  if (!dStr || !monStr || !yStr || !hStr || !miStr) return s;
  const months: Record<string, number> = {
    jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
    jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
  };
  const month = months[monStr.toLowerCase()];
  if (!month) return s;
  const day = Number(dStr);
  const year = Number(yStr);
  const hour = Number(hStr);
  const minute = Number(miStr);
  const offset = sydneyOffset(year, month, day, hour, minute);
  const pad = (n: number): string => String(n).padStart(2, '0');
  return `${year}-${pad(month)}-${pad(day)}T${pad(hour)}:${pad(minute)}:00${offset}`;
}

/**
 * Parse the RFS description's pipe-delimited fields. Format is
 *   ALERT LEVEL: x <br />LOCATION: x <br />COUNCIL AREA: x ...
 * After cleaning <br /> -> '|', we regex out each labelled section.
 */
export function parseRfsDescription(desc: string): ParsedDescription {
  const result: ParsedDescription = {
    alertLevel: '',
    location: '',
    councilArea: '',
    status: '',
    fireType: '',
    size: '',
    responsibleAgency: '',
    updated: '',
    updatedISO: '',
  };
  if (!desc) return result;

  let clean = desc.replace(/<br\s*\/?>/gi, ' | ');
  clean = clean.replace(/<[^>]+>/g, '');
  clean = clean.replace(/\s+/g, ' ').trim();

  // Leading short-form alert level e.g. "Advice: ..."
  const lead = clean.match(/^(Advice|Watch and Act|Emergency Warning|Emergency)\s*[:|]?\s*/i);
  if (lead?.[1]) result.alertLevel = lead[1].trim();

  // Long form "ALERT LEVEL: <value>"
  const alvl = clean.match(/ALERT\s*LEVEL:\s*([^|]+?)(?=\s*\||$)/i);
  if (alvl?.[1]) result.alertLevel = alvl[1].trim();

  const fields: Array<[keyof ParsedDescription, RegExp]> = [
    ['location', /LOCATION:\s*([^|]+?)(?=\s*\||COUNCIL|STATUS|TYPE|FIRE|SIZE|RESPONSIBLE|UPDATED|$)/i],
    ['councilArea', /COUNCIL\s*AREA:\s*([^|]+?)(?=\s*\||STATUS|TYPE|FIRE|SIZE|RESPONSIBLE|UPDATED|$)/i],
    ['status', /STATUS:\s*([^|]+?)(?=\s*\||TYPE|FIRE|SIZE|RESPONSIBLE|UPDATED|$)/i],
    ['fireType', /TYPE:\s*([^|]+?)(?=\s*\||FIRE:|SIZE|RESPONSIBLE|UPDATED|$)/i],
    ['size', /SIZE:\s*([^|]+?)(?=\s*\||RESPONSIBLE|UPDATED|$)/i],
    ['responsibleAgency', /RESPONSIBLE\s*AGENCY:\s*([^|]+?)(?=\s*\||UPDATED|$)/i],
    ['updated', /UPDATED:\s*([^|]+?)(?=\s*\||$)/i],
  ];
  for (const [name, pat] of fields) {
    const m = clean.match(pat);
    if (m?.[1]) {
      const value = m[1].replace(/\s*\|\s*$/, '').trim();
      result[name] = value;
    }
  }
  if (result.updated) {
    result.updatedISO = parseRfsLocalTime(result.updated);
  }
  return result;
}

/** The CAP fields carried onto a feature, all blank when unmatched. */
export interface RfsCapInfo {
  capEvent: string;
  capCategory: string;
  capSeverity: string;
  capUrgency: string;
  capCertainty: string;
  capResponseType: string;
  capSender: string;
  capHeadline: string;
  capInstruction: string;
  capEffective: string;
  capExpires: string;
  capWeb: string;
  capFuelType: string;
  capFireDangerClass: string;
  capControlAuthority: string;
  capAllocatedResources: string;
}

const EMPTY_CAP: RfsCapInfo = {
  capEvent: '',
  capCategory: '',
  capSeverity: '',
  capUrgency: '',
  capCertainty: '',
  capResponseType: '',
  capSender: '',
  capHeadline: '',
  capInstruction: '',
  capEffective: '',
  capExpires: '',
  capWeb: '',
  capFuelType: '',
  capFireDangerClass: '',
  capControlAuthority: '',
  capAllocatedResources: '',
};

/**
 * The incident id a record is keyed by: the tail of the RSS
 * `<guid>` (".../incidents/673561"), which is exactly what the CAP
 * alert puts in `<incidents>`.
 */
export function rfsIncidentId(guidOrId: string): string {
  const t = String(guidOrId ?? '').trim();
  if (!t) return '';
  const tail = t.split('/').pop() ?? '';
  return tail.trim();
}

/**
 * Read the CAP-AU alerts out of the EDXL-DE envelope, keyed by incident
 * id. Alerts sit at
 * EDXLDistribution > contentObject[] > xmlContent > embeddedXMLContent >
 * alert, and fast-xml-parser strips the inline default namespaces, so
 * the elements come through under their bare local names.
 */
export function parseRfsCap(xml: string): Map<string, RfsCapInfo> {
  const out = new Map<string, RfsCapInfo>();
  const root = parseXml(xml);
  const dist =
    (root['EDXLDistribution'] as Record<string, unknown> | undefined) ?? root;
  const objects = asArray(
    (dist as Record<string, unknown>)['contentObject'],
  ) as Array<Record<string, unknown>>;

  for (const obj of objects) {
    const xmlContent = obj['xmlContent'] as Record<string, unknown> | undefined;
    const embedded = xmlContent?.['embeddedXMLContent'] as
      | Record<string, unknown>
      | undefined;
    const alert = embedded?.['alert'] as Record<string, unknown> | undefined;
    if (!alert) continue;

    // <incidents> is the incident id outright; the identifier's trailing
    // segment is the same value and covers a record that omits it.
    const id =
      rfsIncidentId(textOf(alert, 'incidents')) ||
      (textOf(alert, 'identifier').split(':').pop() ?? '').trim();
    if (!id) continue;

    // A CAP alert may carry several <info> blocks (one per language).
    // The RFS sends one; take the first either way.
    const info = asArray(alert['info'])[0] as Record<string, unknown> | undefined;
    if (!info) continue;

    // <parameter> is CAP's escape hatch for agency-specific fields, as
    // name/value pairs. The RFS puts the things its own map shows there
    // - fuel type, fire danger class, allocated resources.
    const params = new Map<string, string>();
    for (const p of asArray(info['parameter']) as Array<Record<string, unknown>>) {
      const name = textOf(p, 'valueName');
      if (name) params.set(name.toLowerCase(), textOf(p, 'value'));
    }

    out.set(id, {
      capEvent: textOf(info, 'event'),
      capCategory: textOf(info, 'category'),
      capSeverity: textOf(info, 'severity'),
      capUrgency: textOf(info, 'urgency'),
      capCertainty: textOf(info, 'certainty'),
      capResponseType: textOf(info, 'responseType'),
      capSender: textOf(info, 'senderName'),
      capHeadline: textOf(info, 'headline'),
      capInstruction: textOf(info, 'instruction'),
      capEffective: textOf(info, 'effective'),
      capExpires: textOf(info, 'expires'),
      capWeb: textOf(info, 'web'),
      capFuelType: params.get('fueltype') ?? '',
      capFireDangerClass: params.get('firedangerclass') ?? '',
      capControlAuthority: params.get('controlauthority') ?? '',
      capAllocatedResources: params.get('allocatedresources') ?? '',
    });
  }
  return out;
}

export async function fetchRfs(): Promise<RfsSnapshot> {
  // The incidents feed is the record and its failure must propagate so
  // the poller backs off. CAP is enrichment - a failure there costs the
  // extra fields, not the layer.
  const [xml, capXml] = await Promise.all([
    fetchText(RFS_URL, { headers: { 'User-Agent': 'Mozilla/5.0' } }),
    fetchText(RFS_CAP_URL, { headers: { 'User-Agent': 'Mozilla/5.0' } }).catch(
      () => '',
    ),
  ]);
  let capById = new Map<string, RfsCapInfo>();
  if (capXml) {
    try {
      capById = parseRfsCap(capXml);
    } catch {
      // A reshaped CAP envelope is not a reason to drop the incidents.
      capById = new Map();
    }
  }
  const root = parseXml(xml);
  const rss = (root['rss'] as Record<string, unknown> | undefined) ?? root;
  const channel =
    rss && typeof rss === 'object'
      ? ((rss as Record<string, unknown>)['channel'] as Record<string, unknown> | undefined)
      : undefined;
  const items = asArray(channel?.['item']) as Array<Record<string, unknown>>;

  const features: RfsFeature[] = [];
  for (const item of items) {
    const title = textOf(item, 'title');
    const link = textOf(item, 'link');
    const desc = textOf(item, 'description');
    const guid = textOf(item, 'guid');
    const category = textOf(item, 'category');
    // The RFS feed declares the GeoRSS namespace inline on each <point> /
    // <polygon> element (xmlns="http://www.georss.org/georss") rather than
    // via a prefix on the root. fast-xml-parser strips the inline xmlns
    // and exposes the elements under their bare local names, so we look
    // up both `georss:point` (legacy / prefixed feeds) and `point`.
    const pointText = textOf(item, 'georss:point') || textOf(item, 'point');
    if (!pointText) continue;

    const parts = pointText.trim().split(/\s+/);
    if (parts.length < 2) continue;
    const lat = Number(parts[0]);
    const lon = Number(parts[1]);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    // Polygon may live under either `georss:polygon` or bare `polygon`
    // depending on whether the upstream uses a prefix or an inline xmlns.
    const rawPolys =
      (item as Record<string, unknown>)['georss:polygon'] ??
      (item as Record<string, unknown>)['polygon'];
    const polygons: string[] = [];
    if (rawPolys !== undefined && rawPolys !== null) {
      const arr = asArray(rawPolys);
      for (const p of arr) {
        if (typeof p === 'string') {
          const t = p.trim();
          if (t) polygons.push(t);
        } else if (typeof p === 'object' && p !== null && '#text' in p) {
          const t = String((p as Record<string, unknown>)['#text'] ?? '').trim();
          if (t) polygons.push(t);
        }
      }
    }

    const parsed = parseRfsDescription(desc);
    if (!parsed.alertLevel && category) parsed.alertLevel = category;

    const cleanDesc = (desc || '')
      .replace(/<[^>]+>/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [lon, lat] },
      properties: {
        title,
        link,
        guid,
        description: cleanDesc,
        status: parsed.status,
        location: parsed.location,
        size: parsed.size,
        alertLevel: parsed.alertLevel,
        fireType: parsed.fireType,
        councilArea: parsed.councilArea,
        responsibleAgency: parsed.responsibleAgency,
        updated: parsed.updated,
        updatedISO: parsed.updatedISO,
        polygons,
        ...(capById.get(rfsIncidentId(guid)) ?? EMPTY_CAP),
        source: 'rfs',
      },
    });
  }

  return {
    type: 'FeatureCollection',
    features,
    count: features.length,
  };
}

/** Raw passthrough used by /api/rfs/incidents/raw. Independent fetch
 *  because it caches differently in Python; we just expose the
 *  channel + item view of the same XML. */
export interface RfsRawSnapshot {
  channel: { title: string; description: string; pubDate: string };
  items: Array<{
    title: string;
    link: string;
    description: string;
    pubDate: string;
    guid: string;
    category: string;
    point: string | null;
  }>;
  count: number;
}

export async function fetchRfsRaw(): Promise<RfsRawSnapshot> {
  const xml = await fetchText(RFS_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
  const root = parseXml(xml);
  const rss = (root['rss'] as Record<string, unknown> | undefined) ?? root;
  const channel =
    (rss && typeof rss === 'object'
      ? ((rss as Record<string, unknown>)['channel'] as Record<string, unknown> | undefined)
      : undefined) ?? {};
  const items = asArray(channel['item']) as Array<Record<string, unknown>>;

  const out = items.map((item) => {
    // Same dual-key lookup as fetchRfs — see comment there.
    const pointText = (
      textOf(item, 'georss:point') || textOf(item, 'point')
    ).trim();
    return {
      title: textOf(item, 'title'),
      link: textOf(item, 'link'),
      description: textOf(item, 'description'),
      pubDate: textOf(item, 'pubDate'),
      guid: textOf(item, 'guid'),
      category: textOf(item, 'category'),
      point: pointText ? pointText : null,
    };
  });

  return {
    channel: {
      title: textOf(channel, 'title'),
      description: textOf(channel, 'description'),
      pubDate: textOf(channel, 'pubDate'),
    },
    items: out,
    count: out.length,
  };
}

export default function register(): void {
  registerSource<RfsSnapshot>({
    name: 'rfs_incidents',
    // Archive rows under the canonical python source name so historical
    // data + this poller's output share the same `source` value.
    archiveSource: 'rfs',
    family: 'rfs',
    intervalMs: 60_000,
    fetch: fetchRfs,
  });
}

/** Helper for route handlers — returns the live snapshot or an empty
 *  feature collection when the poller hasn't filled it yet. */
export function rfsSnapshot(): RfsSnapshot {
  return (
    liveStore.getData<RfsSnapshot>('rfs_incidents') ?? {
      type: 'FeatureCollection',
      features: [],
      count: 0,
    }
  );
}
