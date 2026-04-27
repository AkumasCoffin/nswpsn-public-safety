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
 */
import { fetchText } from './shared/http.js';
import { asArray, parseXml, textOf } from './shared/xml.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const RFS_URL = 'https://www.rfs.nsw.gov.au/feeds/majorIncidents.xml';

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
 * string with the Sydney timezone offset. Mirrors python's
 * parse_rfs_local_time. We don't carry the zoneinfo dance — the Sydney
 * offset is +11:00 (AEDT) Oct-Apr and +10:00 (AEST) the rest of the
 * year. That's a coarse approximation but matches the Python output's
 * intent (give frontends a parseable timestamp).
 */
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
  // AEDT (Apr/Oct boundary, approximate). Python uses zoneinfo which is
  // exact; for our purposes the dashboard only displays the time, so
  // an off-by-one-hour edge case at DST transitions is acceptable.
  const isAEDT = month >= 10 || month <= 3;
  const offset = isAEDT ? '+11:00' : '+10:00';
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

export async function fetchRfs(): Promise<RfsSnapshot> {
  const xml = await fetchText(RFS_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
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
    intervalActiveMs: 60_000,
    intervalIdleMs: 120_000,
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
