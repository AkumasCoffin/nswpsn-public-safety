/**
 * Essential Energy outage feed.
 *
 * Two KML files, both public:
 *   - current.kml  — active outages right now (planned + unplanned mixed)
 *   - future.kml   — scheduled future planned outages
 *
 * KML payload shape: a flat list of <Placemark> records; each carries
 *   - id attribute             — incident id (e.g. "INCD-118773-r")
 *   - <name>                   — suburb / locality label
 *   - <styleUrl>               — contains "planned" or "unplanned"
 *   - <description> CDATA HTML — Time Off, Est. Time On, Customers,
 *                                Reason, Last Updated rows
 *   - <Point>/<Polygon>        — coordinates (lon,lat)
 *
 * register() binds two LiveStore keys:
 *   - essential_current  → flat list of normalised outages from current.kml
 *   - essential_future   → flat list from future.kml
 *
 * The HTTP route handler then derives planned/unplanned/all by filtering
 * across the two LiveStore entries — same approach Python takes
 * (cache_get('essential_energy') + cache_get('essential_energy_future')).
 *
 * Cadences: active 180s / idle 600s for both feeds.
 *
 * Per-record output mirrors the Python `_parse_essential_energy_kml`
 * dict at line 3943-3961 of external_api_proxy.py.
 */
import { fetchText } from './shared/http.js';
import { parseKmlPlacemarks } from './shared/kml.js';
import { registerSource } from '../services/sourceRegistry.js';
import type { ArchiveRow } from '../store/archive.js';

const FEEDS = {
  current: 'https://www.essentialenergy.com.au/Assets/kmz/current.kml',
  future: 'https://www.essentialenergy.com.au/Assets/kmz/future.kml',
} as const;

export type EssentialFeedType = 'current' | 'future';

export interface EssentialOutage {
  incidentId: string;
  title: string;
  suburb: string;
  outageType: 'planned' | 'unplanned';
  feedType: EssentialFeedType;
  latitude: number | null;
  longitude: number | null;
  cause: string;
  customersAffected: number;
  timeOff: string;
  estTimeOn: string;
  lastUpdated: string;
  status: 'active' | 'scheduled';
  source: 'essential_current' | 'essential_planned' | 'essential_future';
  sourceTimestamp: string | null;
  /** Polygon as [[lon, lat], ...] outer ring, or null if absent. */
  polygon: Array<[number, number]> | null;
  provider: 'Essential Energy';
}

/** Pull a `<span>Label:</span>VALUE</div>` field out of the description blob. */
function extractField(html: string, label: string): string {
  // Mirror the Python regex precisely: `Label:</span>(.*?)</div>` with
  // a non-greedy capture. Escape the label since Python uses a literal.
  const pattern = new RegExp(
    `${label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:</span>(.*?)</div>`,
    'i',
  );
  const m = pattern.exec(html);
  return m && m[1] ? m[1].trim() : '';
}

function extractCustomers(html: string): number {
  // Python: r'Customers affected:</span>\s*(\d+)' — different spacing
  // than the other fields because the count isn't wrapped in the same
  // <div>. Match upstream exactly.
  const m = /Customers affected:<\/span>\s*(\d+)/i.exec(html);
  return m && m[1] ? Number.parseInt(m[1], 10) : 0;
}

/** Convert "DD/MM/YYYY HH:mm:ss" to "YYYY-MM-DDTHH:mm:ss". */
function timeOffToIso(timeOff: string): string | null {
  if (!timeOff) return null;
  const m = /^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$/.exec(timeOff.trim());
  if (!m) return timeOff; // Python falls back to the raw string on parse failure.
  const [, dd, mm, yyyy, hh, mi, ss] = m;
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}`;
}

export function parseEssentialKml(
  xml: string,
  feedType: EssentialFeedType,
): EssentialOutage[] {
  const placemarks = parseKmlPlacemarks(xml);
  const out: EssentialOutage[] = [];
  for (const pm of placemarks) {
    // Deliberate deviation from Python (line 3860 of external_api_proxy.py):
    // Python uses `'planned' in style_url.lower()` which incorrectly matches
    // "unplanned" too — the substring check accepts both. Real KML
    // styleUrls in production today are `#planned` and `#unplanned`, so
    // checking `unplanned` first disambiguates them. This is a bug-fix
    // not a contract change: the field value (`planned`/`unplanned`) is
    // exactly what Python's docstring promises, just classified correctly.
    const styleUrl = pm.styleUrl;
    const outageType: 'planned' | 'unplanned' = styleUrl.includes('unplanned')
      ? 'unplanned'
      : styleUrl.includes('planned')
        ? 'planned'
        : 'unplanned';

    const desc = pm.description ?? '';
    const timeOff = extractField(desc, 'Time Off');
    const estTimeOn = extractField(desc, 'Est. Time On');
    const customers = extractCustomers(desc);
    const reason = extractField(desc, 'Reason');
    const lastUpdated = extractField(desc, 'Last Updated');

    let lat: number | null = null;
    let lon: number | null = null;
    if (pm.point) {
      [lon, lat] = pm.point;
    }

    let source: EssentialOutage['source'];
    let status: EssentialOutage['status'];
    if (feedType === 'future') {
      source = 'essential_future';
      status = 'scheduled';
    } else {
      source = outageType === 'unplanned' ? 'essential_current' : 'essential_planned';
      status = 'active';
    }

    out.push({
      incidentId: pm.id,
      title: pm.name || reason || 'Power Outage',
      suburb: pm.name,
      outageType,
      feedType,
      latitude: lat,
      longitude: lon,
      cause: reason || 'Unknown',
      customersAffected: customers,
      timeOff,
      estTimeOn,
      lastUpdated,
      status,
      source,
      sourceTimestamp: timeOffToIso(timeOff),
      polygon: pm.polygon.length > 0 ? pm.polygon : null,
      provider: 'Essential Energy',
    });
  }
  return out;
}

export async function fetchFeed(feedType: EssentialFeedType): Promise<EssentialOutage[]> {
  const url = FEEDS[feedType];
  const xml = await fetchText(url, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
    timeoutMs: 15_000,
  });
  return parseEssentialKml(xml, feedType);
}

/**
 * Per-outage archive fan-out that respects each row's own `source`
 * field. The default extractor would tag every row from the
 * `essential_current` LiveStore key as `essential_current`, but
 * current.kml mixes planned + unplanned and python writes them under
 * different source values (external_api_proxy.py:3940). Without this
 * the logs page filter for "essential_planned" misses the planned-
 * from-current rows entirely.
 */
function essentialArchiveItems(
  data: unknown,
  fetched_at: number,
  _source: string,
): ArchiveRow[] {
  if (!Array.isArray(data)) return [];
  const out: ArchiveRow[] = [];
  for (const o of data as EssentialOutage[]) {
    if (!o || typeof o !== 'object') continue;
    out.push({
      source: o.source,
      source_id: o.incidentId || null,
      fetched_at,
      lat: o.latitude,
      lng: o.longitude,
      category: o.outageType,
      subcategory: o.status,
      data: { ...o, title: o.title || o.suburb || 'Outage' },
    });
  }
  return out;
}

export function register(): void {
  registerSource<EssentialOutage[]>({
    name: 'essential_current',
    family: 'power',
    intervalActiveMs: 180_000,
    intervalIdleMs: 600_000,
    fetch: () => fetchFeed('current'),
    archiveItems: essentialArchiveItems,
  });
  registerSource<EssentialOutage[]>({
    name: 'essential_future',
    family: 'power',
    intervalActiveMs: 180_000,
    intervalIdleMs: 600_000,
    fetch: () => fetchFeed('future'),
    archiveItems: essentialArchiveItems,
  });
}

export default register;
