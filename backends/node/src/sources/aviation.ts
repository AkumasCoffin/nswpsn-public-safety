/**
 * Airservices Australia weather cameras.
 *
 * Mirrors python external_api_proxy.py:
 *   - get_airservices_nonce()       (7544-7601)
 *   - aviation_cameras()            (7604-7677) — list endpoint
 *   - aviation_camera_detail()      (7680-7758) — per-airport modal
 *
 * The list of airports is polled into LiveStore on a 5 min cadence; the
 * per-airport modal is fetched on demand by the route handler with a
 * tiny in-process TTL cache (matching python's `@cached(ttl=120)` on
 * the detail handler).
 *
 * The whole flow gates on a nonce that Airservices' WordPress install
 * embeds in the public weathercams page. The nonce rotates ~1h, so we
 * scrape it, cache for 1h, and on parse failure cache a known-working
 * fallback for 5 min before retrying.
 */
import { fetchText, fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface AviationFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    id: string;
    title: string;
    name: string;
    state: string;
    state_full: string;
    link: string;
    imageUrl: string;
    source: 'airservices_australia';
  };
}

export interface AviationSnapshot {
  type: 'FeatureCollection';
  features: AviationFeature[];
  count: number;
}

export interface AviationCameraAngle {
  direction: string;
  label: string;
  imageUrl: string;
  thumbnailUrl: string;
  angle: string;
}

export interface AviationDetail {
  airport: string;
  cameras: AviationCameraAngle[];
  count: number;
}

// ---------------------------------------------------------------------------
// Nonce caching
// ---------------------------------------------------------------------------

interface NonceCache {
  nonce: string;
  expiresAt: number;
}

const FALLBACK_NONCE = 'da9010b391';
let nonceCache: NonceCache = { nonce: '', expiresAt: 0 };

const NONCE_PATTERNS: RegExp[] = [
  /["']nonce["']\s*:\s*["']([a-f0-9]+)["']/i,
  /nonce[=:]\s*["']([a-f0-9]+)["']/i,
  /&nonce=([a-f0-9]+)/i,
  /nonce%22%3A%22([a-f0-9]+)/i,
];

export async function getAirservicesNonce(): Promise<string> {
  const now = Date.now();
  if (nonceCache.nonce && now < nonceCache.expiresAt) {
    return nonceCache.nonce;
  }
  try {
    const html = await fetchText('https://weathercams.airservicesaustralia.com/', {
      timeoutMs: 15_000,
      headers: {
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
    });
    for (const pat of NONCE_PATTERNS) {
      const m = html.match(pat);
      if (m?.[1]) {
        nonceCache = { nonce: m[1], expiresAt: now + 60 * 60_000 };
        return m[1];
      }
    }
  } catch {
    // fall through to fallback
  }
  // Fallback for 5 min so we don't hammer the upstream while it's broken.
  nonceCache = { nonce: FALLBACK_NONCE, expiresAt: now + 5 * 60_000 };
  return FALLBACK_NONCE;
}

// Test seam — clears the nonce cache so unit tests can re-stub.
export function _resetNonceCacheForTests(): void {
  nonceCache = { nonce: '', expiresAt: 0 };
}

// ---------------------------------------------------------------------------
// List poller
// ---------------------------------------------------------------------------

const AJAX_URL =
  'https://weathercams.airservicesaustralia.com/wp-admin/admin-ajax.php';

interface RawAirport {
  id?: string | number;
  title?: string;
  name?: string;
  state?: string;
  state_full?: string;
  link?: string;
  thumbnail?: string;
  lat?: string | number;
  long?: string | number;
}

interface AirportListResponse {
  airport_list?: RawAirport[];
}

export async function fetchAviationCameras(): Promise<AviationSnapshot> {
  const nonce = await getAirservicesNonce();
  const params = new URLSearchParams({
    action: 'get_airports_list',
    filter: 'all',
    type: 'map',
    filter_type: 'normal',
    nonce,
  });
  const data = await fetchJson<AirportListResponse>(`${AJAX_URL}?${params}`, {
    timeoutMs: 15_000,
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
  const list = Array.isArray(data?.airport_list) ? data.airport_list : [];

  const features: AviationFeature[] = [];
  for (const airport of list) {
    if (!airport || typeof airport !== 'object') continue;
    const latRaw = airport.lat;
    const lonRaw = airport.long;
    if (latRaw === undefined || lonRaw === undefined) continue;
    const lat = Number(latRaw);
    const lon = Number(lonRaw);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [lon, lat] },
      properties: {
        id: String(airport.id ?? ''),
        title: airport.title ?? 'Airport Camera',
        name: airport.name ?? '',
        state: airport.state ?? '',
        state_full: airport.state_full ?? '',
        link: airport.link ?? '',
        imageUrl: airport.thumbnail ?? '',
        source: 'airservices_australia',
      },
    });
  }
  return {
    type: 'FeatureCollection',
    features,
    count: features.length,
  };
}

// ---------------------------------------------------------------------------
// Detail (per-airport modal) — on-demand with 2 min cache
// ---------------------------------------------------------------------------

interface RawModal {
  modal?: Record<string, unknown>;
}

const DIRECTIONS = ['north', 'east', 'south', 'west'] as const;
const DIRECTION_LABELS: Record<(typeof DIRECTIONS)[number], string> = {
  north: 'North',
  east: 'East',
  south: 'South',
  west: 'West',
};

interface DetailCacheEntry {
  detail: AviationDetail;
  expiresAt: number;
}
const detailCache = new Map<string, DetailCacheEntry>();
const DETAIL_TTL_MS = 120_000; // 2 min, matches python @cached(ttl=120)

export function _resetDetailCacheForTests(): void {
  detailCache.clear();
}

export async function fetchAviationCameraDetail(
  airport: string,
): Promise<AviationDetail> {
  const now = Date.now();
  const cached = detailCache.get(airport);
  if (cached && now < cached.expiresAt) return cached.detail;

  const nonce = await getAirservicesNonce();
  const params = new URLSearchParams({
    action: 'get_airport_modal',
    airport,
    nonce,
  });
  const data = await fetchJson<RawModal>(`${AJAX_URL}?${params}`, {
    timeoutMs: 15_000,
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
  const modal = (data?.modal ?? {}) as Record<string, unknown>;

  const cameras: AviationCameraAngle[] = [];
  for (const direction of DIRECTIONS) {
    const imageKey = `${direction}_image`;
    const thumbKey = `${direction}_thumbnail`;
    const angleKey = `${direction}_angle`;
    const imageUrl = String(modal[imageKey] ?? '');
    const thumbnailUrl = String(modal[thumbKey] ?? '');
    const angle = String(modal[angleKey] ?? '');
    if (!imageUrl) continue;
    cameras.push({
      direction,
      label: DIRECTION_LABELS[direction],
      imageUrl,
      thumbnailUrl,
      angle,
    });
  }

  const detail: AviationDetail = {
    airport,
    cameras,
    count: cameras.length,
  };
  detailCache.set(airport, { detail, expiresAt: now + DETAIL_TTL_MS });
  return detail;
}

// ---------------------------------------------------------------------------
// Source registration
// ---------------------------------------------------------------------------

export default function register(): void {
  registerSource<AviationSnapshot>({
    name: 'aviation_cameras',
    family: 'misc',
    intervalActiveMs: 5 * 60_000,
    intervalIdleMs: 10 * 60_000,
    fetch: fetchAviationCameras,
  });
}

export function aviationSnapshot(): AviationSnapshot {
  return (
    liveStore.getData<AviationSnapshot>('aviation_cameras') ?? {
      type: 'FeatureCollection',
      features: [],
      count: 0,
    }
  );
}
