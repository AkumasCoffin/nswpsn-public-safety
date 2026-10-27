/**
 * RFS Major Incidents — the RFS GeoJSON feed -> our FeatureCollection.
 *
 * Mirrors the Python `/api/rfs/incidents` route at
 * external_api_proxy.py:10621, and preserves its JSON shape so the
 * frontend, the bot and the archive don't have to know where it came from:
 *
 *   { type: 'FeatureCollection', features: [...], count }
 *
 * Each feature's properties include the parsed-out alertLevel /
 * location / size / status / etc. fields the Python parser extracts
 * from the RFS description's pipe-delimited free text.
 *
 * THE UPSTREAM IS majorIncidents.json, not the .xml it used to be. The two
 * carry the same incidents, the same description text and the same
 * geometry; the JSON's is plain GeoJSON — a Point, or a GeometryCollection
 * of the point and the fire-ground Polygons — where the XML spread it over
 * GeoRSS elements whose namespace handling varied. The fire-ground
 * polygons are still emitted as GeoRSS "lat lon lat lon …" strings, which
 * is what the map and the archive have always read. The feed itself, as
 * published, is kept from the last poll for /api/v1/rfs/incidents/raw.
 */
import { fetchJson, fetchText } from './shared/http.js';
import { asArray, parseXml, textOf } from './shared/xml.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const RFS_URL = 'https://www.rfs.nsw.gov.au/feeds/majorIncidents.json';
/** The same feed as RSS — read only by the old /api/rfs/incidents/raw. */
const RFS_XML_URL = 'https://www.rfs.nsw.gov.au/feeds/majorIncidents.xml';

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

/** majorIncidents.json as the RFS publishes it (the fields seen; it may carry more). */
export interface RfsFeed {
  type: 'FeatureCollection';
  features: Array<{
    type: 'Feature';
    geometry: { type: string; coordinates?: unknown; geometries?: unknown[] };
    properties: {
      title: string;
      link: string;
      category: string;
      guid: string;
      guid_isPermaLink: string;
      pubDate: string;
      description: string;
      [k: string]: unknown;
    };
  }>;
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

/**
 * Fifteen significant digits, as the XML feed wrote its numbers. The JSON
 * carries the full doubles (143.58525085400004 where the XML said
 * 143.585250854, -33.406180796999934 where it said -33.4061807969999);
 * cutting them the same way keeps every incident byte-identical to what the
 * XML gave, so the switch did not rewrite every archived record.
 */
const sig15 = (v: number) => Number(v.toPrecision(15));

/** A GeoJSON position we can use: finite [lon, lat]. */
function lonLat(v: unknown): [number, number] | null {
  if (!Array.isArray(v) || v.length < 2) return null;
  const lon = Number(v[0]);
  const lat = Number(v[1]);
  return Number.isFinite(lon) && Number.isFinite(lat) ? [sig15(lon), sig15(lat)] : null;
}

/**
 * The pin and the fire-ground rings out of a feed geometry, however nested:
 * the first Point is the pin, and each Polygon's (or MultiPolygon part's)
 * outer ring is a fire ground. Holes are dropped — the GeoRSS strings the map
 * reads have no way to say "hole", and the XML feed never sent one either.
 */
function readGeometry(g: unknown, out: { point: [number, number] | null; rings: Array<Array<[number, number]>> }): void {
  if (!g || typeof g !== 'object') return;
  const geom = g as { type?: unknown; coordinates?: unknown; geometries?: unknown };
  const outerRing = (poly: unknown) => {
    const ring: unknown = Array.isArray(poly) ? poly[0] : null;
    if (!Array.isArray(ring)) return;
    const pts = ring.map(lonLat).filter((p): p is [number, number] => p !== null);
    if (pts.length >= 3) out.rings.push(pts);
  };
  if (geom.type === 'Point') {
    out.point ??= lonLat(geom.coordinates);
  } else if (geom.type === 'Polygon') {
    outerRing(geom.coordinates);
  } else if (geom.type === 'MultiPolygon' && Array.isArray(geom.coordinates)) {
    for (const poly of geom.coordinates) outerRing(poly);
  } else if (geom.type === 'GeometryCollection' && Array.isArray(geom.geometries)) {
    for (const member of geom.geometries) readGeometry(member, out);
  }
}

/** The feed as last fetched, for /api/v1/rfs/incidents/raw. */
let lastFeed: RfsFeed | null = null;

export async function fetchRfs(): Promise<RfsSnapshot> {
  const feed = await fetchJson<RfsFeed>(RFS_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
  // A body that parses but is not the feed (an error page served as JSON,
  // say) throws, so the poller keeps the last good snapshot rather than
  // publishing an empty map.
  if (!feed || feed.type !== 'FeatureCollection' || !Array.isArray(feed.features)) {
    throw new Error('rfs: majorIncidents.json is not a FeatureCollection');
  }

  const features: RfsFeature[] = [];
  for (const f of feed.features) {
    const props = (f?.properties ?? {}) as Record<string, unknown>;
    const str = (k: string) => (typeof props[k] === 'string' ? (props[k] as string).trim() : '');
    const title = str('title');
    const link = str('link');
    const desc = str('description');
    const guid = str('guid');
    const category = str('category');

    const geo: { point: [number, number] | null; rings: Array<Array<[number, number]>> } = { point: null, rings: [] };
    readGeometry(f?.geometry, geo);
    // No pin, no feature — as the XML parser skipped an item with no point.
    if (!geo.point) continue;
    const [lon, lat] = geo.point;
    const polygons = geo.rings.map((ring) => ring.map(([x, y]) => `${y} ${x}`).join(' '));

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

  lastFeed = feed;
  return {
    type: 'FeatureCollection',
    features,
    count: features.length,
  };
}

/** The RFS feed as it was published at the last poll; empty before the first. */
export function rfsFeedSnapshot(): RfsFeed {
  return lastFeed ?? { type: 'FeatureCollection', features: [] };
}

/** Raw passthrough used by the OLD /api/rfs/incidents/raw: the channel +
 *  item view of the RSS version of the feed, fetched per request because it
 *  cached differently in Python. Kept as it was until that path is retired;
 *  /api/v1/rfs/incidents/raw serves rfsFeedSnapshot() instead. */
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
  const xml = await fetchText(RFS_XML_URL, {
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

// ---------------------------------------------------------------------------
// Fire danger ratings and total fire bans — /api/rfs/fdr
// ---------------------------------------------------------------------------

/**
 * One <District> record per NSW fire weather area: its rating and fire ban
 * status for today and tomorrow. The same feed live.html reads in the
 * browser. (fdrTobanRSS.xml carries the same fields flattened into an HTML
 * description; this one has them as elements.)
 */
const RFS_FDR_URL = 'https://www.rfs.nsw.gov.au/feeds/fdrToban.xml';

export interface RfsFireDangerRating {
  /** The district's name — kept as `title` from when this route read RSS. */
  title: string;
  /** The ratings and bans as one line of text, for display. */
  description: string;
  source: 'rfs_fdr';
  district: string;
  region_number: number | null;
  councils: string[];
  /** As the RFS publishes it: NONE (no rating), MODERATE, HIGH, EXTREME, CATASTROPHIC. */
  danger_today: string;
  danger_tomorrow: string;
  fire_ban_today: boolean;
  fire_ban_tomorrow: boolean;
}

export interface RfsFireDanger {
  ratings: RfsFireDangerRating[];
  /** When the RFS was last read successfully (ISO 8601); null before the first read. */
  fetched_at: string | null;
}

/** The districts out of fdrToban.xml; a record without a name is dropped. */
export function parseFireDangerMap(xml: string): RfsFireDangerRating[] {
  const root = parseXml(xml);
  const map = root['FireDangerMap'] as Record<string, unknown> | undefined;
  const out: RfsFireDangerRating[] = [];
  for (const d of asArray(map?.['District']) as Array<Record<string, unknown>>) {
    const district = textOf(d, 'Name').trim();
    if (!district) continue;
    const region = Number(textOf(d, 'RegionNumber'));
    const dangerToday = textOf(d, 'DangerLevelToday').trim().toUpperCase();
    const dangerTomorrow = textOf(d, 'DangerLevelTomorrow').trim().toUpperCase();
    const banToday = /^yes$/i.test(textOf(d, 'FireBanToday').trim());
    const banTomorrow = /^yes$/i.test(textOf(d, 'FireBanTomorrow').trim());
    const level = (v: string) => (v && v !== 'NONE' ? v : 'no rating');
    const ban = (b: boolean) => (b ? 'total fire ban' : 'no total fire ban');
    out.push({
      title: district,
      description: `Today: ${level(dangerToday)}, ${ban(banToday)}. Tomorrow: ${level(dangerTomorrow)}, ${ban(banTomorrow)}.`,
      source: 'rfs_fdr',
      district,
      region_number: Number.isFinite(region) && textOf(d, 'RegionNumber').trim() !== '' ? region : null,
      councils: textOf(d, 'Councils')
        .split(';')
        .map((c) => c.trim())
        .filter(Boolean),
      danger_today: dangerToday,
      danger_tomorrow: dangerTomorrow,
      fire_ban_today: banToday,
      fire_ban_tomorrow: banTomorrow,
    });
  }
  return out;
}

/**
 * The ratings change a few times a day, and the RFS should not be read once
 * per API request: the last good answer is kept for FDR_TTL_MS. A failed read
 * keeps serving the last good one and is not retried for FDR_RETRY_MS, so an
 * RFS outage does not turn every request into another attempt.
 */
const FDR_TTL_MS = 10 * 60_000;
const FDR_RETRY_MS = 60_000;
let fdrGood: { value: RfsFireDanger; at: number } | null = null;
let fdrFailedAt = 0;
let fdrInFlight: Promise<RfsFireDanger> | null = null;

/** The fire danger ratings, fresh within ten minutes. Throws only when there has never been a good read. */
export async function rfsFireDanger(): Promise<RfsFireDanger> {
  const now = Date.now();
  if (fdrGood && now - fdrGood.at < FDR_TTL_MS) return fdrGood.value;
  if (now - fdrFailedAt < FDR_RETRY_MS) {
    if (fdrGood) return fdrGood.value;
    throw new Error('rfs fdr: the last read failed; not retrying yet');
  }
  fdrInFlight ??= (async () => {
    try {
      const xml = await fetchText(RFS_FDR_URL, { timeoutMs: 15_000, headers: { 'User-Agent': 'Mozilla/5.0' } });
      const ratings = parseFireDangerMap(xml);
      if (!ratings.length) throw new Error('rfs fdr: no districts in fdrToban.xml');
      const value: RfsFireDanger = { ratings, fetched_at: new Date().toISOString() };
      fdrGood = { value, at: Date.now() };
      return value;
    } catch (err) {
      fdrFailedAt = Date.now();
      throw err;
    } finally {
      fdrInFlight = null;
    }
  })();
  try {
    return await fdrInFlight;
  } catch (err) {
    if (fdrGood) return fdrGood.value;
    throw err;
  }
}

/** Test seam. */
export function _resetRfsFireDanger(): void {
  fdrGood = null;
  fdrFailedAt = 0;
  fdrInFlight = null;
}

