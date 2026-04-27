/**
 * Beach data — two upstream sources:
 *
 *   beachwatch  - NSW DCCEEW water quality GeoJSON, returned verbatim.
 *   beachsafe   - Surf Life Saving NSW summary list, normalised into the
 *                 shape the Python `/api/beachsafe` route returns.
 *
 * No per-beach detail polling — that endpoint is request-scoped and
 * Python pre-fetches a dict by slug on the prewarm. We don't need to
 * keep that pre-fetch dict warm in W3; the route handler returns
 * whatever LiveStore happens to have under `beachsafe_details`.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const BEACHWATCH_URL = 'https://api.beachwatch.nsw.gov.au/public/sites/geojson';
const BEACHSAFE_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  Accept: 'application/json',
  Referer: 'https://beachsafe.org.au/',
};

const NSW_NE = { lat: -28.0, lon: 154.0 };
const NSW_SW = { lat: -37.5, lon: 149.0 };

export type BeachwatchSnapshot = unknown; // upstream GeoJSON, returned as-is

export interface BeachsafeBeach {
  id: unknown;
  name: string;
  lat: number | null;
  lng: number | null;
  url: string;
  slug: string;
  patrolled: boolean;
  status: string;
  hasToilet: boolean;
  hasParking: boolean;
  dogsAllowed: boolean;
  image: string;
  weather: Record<string, unknown>;
  hazards: unknown[];
  isPatrolledToday: boolean;
  patrolStart: string;
  patrolEnd: string;
  patrol: number | string;
}

function asNumberOrNull(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function asString(v: unknown): string {
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return '';
}

export async function fetchBeachwatch(): Promise<BeachwatchSnapshot> {
  return fetchJson<unknown>(BEACHWATCH_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
}

export async function fetchBeachsafe(): Promise<BeachsafeBeach[]> {
  // Upstream rejects integer-valued bbox params with HTTP 422. JS's
  // string coercion drops the trailing zero (`-28.0` → `-28`), so we
  // use `.toFixed(1)` to force a decimal point. Verified against the
  // python implementation which sends `-28.0` / `154.0` literally.
  const fmt = (n: number): string => n.toFixed(1);
  const url =
    `https://beachsafe.org.au/api/v4/map/beaches` +
    `?neCoords[]=${fmt(NSW_NE.lat)}&neCoords[]=${fmt(NSW_NE.lon)}` +
    `&swCoords[]=${fmt(NSW_SW.lat)}&swCoords[]=${fmt(NSW_SW.lon)}`;
  const data = await fetchJson<unknown>(url, { headers: BEACHSAFE_HEADERS });

  let beaches: unknown[] = [];
  if (data && typeof data === 'object' && !Array.isArray(data)) {
    const obj = data as Record<string, unknown>;
    const raw = obj['beaches'];
    if (Array.isArray(raw)) beaches = raw;
  } else if (Array.isArray(data)) {
    return data as BeachsafeBeach[];
  }

  const out: BeachsafeBeach[] = [];
  for (const b of beaches) {
    if (!b || typeof b !== 'object') continue;
    const obj = b as Record<string, unknown>;
    const beachUrl = asString(obj['url']);
    const slug = beachUrl ? beachUrl.replace(/\/$/, '').split('/').pop() ?? '' : '';

    const patrolToday =
      (obj['is_patrolled_today'] as Record<string, unknown> | undefined) ?? {};
    const patrolTodayObj =
      typeof patrolToday === 'object' && patrolToday !== null && !Array.isArray(patrolToday)
        ? (patrolToday as Record<string, unknown>)
        : {};

    const hazards = Array.isArray(obj['hazards']) ? (obj['hazards'] as unknown[]) : [];
    const weather =
      typeof obj['weather'] === 'object' && obj['weather'] !== null
        ? (obj['weather'] as Record<string, unknown>)
        : {};

    const patrolVal = obj['patrol'];
    const patrol = typeof patrolVal === 'number' || typeof patrolVal === 'string' ? patrolVal : 0;

    out.push({
      id: obj['id'],
      name: asString(obj['title']) || 'Unknown Beach',
      lat: asNumberOrNull(obj['latitude']),
      lng: asNumberOrNull(obj['longitude']),
      url: beachUrl,
      slug,
      patrolled: asString(obj['status']).toLowerCase() === 'patrolled',
      status: asString(obj['status']) || 'Unknown',
      hasToilet: Boolean(obj['has_toilet']),
      hasParking: Boolean(obj['has_parking']),
      dogsAllowed: Boolean(obj['has_dogs_allowed']),
      image: asString(obj['image']),
      weather,
      hazards,
      isPatrolledToday: Boolean(patrolTodayObj['flag']),
      patrolStart: asString(patrolTodayObj['start']),
      patrolEnd: asString(patrolTodayObj['end']),
      patrol,
    });
  }
  return out;
}

export default function register(): void {
  registerSource<BeachwatchSnapshot>({
    name: 'beachwatch',
    family: 'misc',
    intervalActiveMs: 600_000,
    intervalIdleMs: 1_200_000,
    fetch: fetchBeachwatch,
  });
  registerSource<BeachsafeBeach[]>({
    name: 'beachsafe',
    family: 'misc',
    intervalActiveMs: 600_000,
    intervalIdleMs: 1_200_000,
    fetch: fetchBeachsafe,
  });
}

export function beachwatchSnapshot(): BeachwatchSnapshot {
  return (
    liveStore.getData<BeachwatchSnapshot>('beachwatch') ?? {
      type: 'FeatureCollection',
      features: [],
    }
  );
}

export function beachsafeSnapshot(): BeachsafeBeach[] {
  return liveStore.getData<BeachsafeBeach[]>('beachsafe') ?? [];
}

export function beachsafeDetailsSnapshot(): Record<string, unknown> {
  return liveStore.getData<Record<string, unknown>>('beachsafe_details') ?? {};
}
