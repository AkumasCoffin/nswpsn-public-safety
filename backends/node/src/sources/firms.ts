/**
 * NASA FIRMS fire-hotspot source.
 *
 * Pulls active-fire detections from the FIRMS Area API:
 *   https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/{SOURCE}/{W,S,E,N}/{DAYS}/{YYYY-MM-DD}
 *
 * Each detection is a satellite pixel. The CSV gives lat/lon, brightness,
 * fire-radiative-power (FRP), confidence, and the pixel scan/track in km
 * — we use scan/track to draw a true pixel-footprint polygon for each hit
 * rather than a generic dot, so the map shows the actual area the satellite
 * flagged as burning.
 *
 * VIIRS-NOAA20-NRT is the default source: 375 m resolution, near-real-time
 * (typically 3 h after acquisition). MODIS_NRT could be added but is 1 km
 * pixels, much chunkier on the map.
 *
 * Bbox covers NSW + a small buffer so border fires that affect NSW air
 * quality / RFS dispatches show up too.
 */
import { fetchText } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';
import { config } from '../config.js';
import { log } from '../lib/log.js';

// NSW bbox + small Vic/Qld/SA fringe so cross-border fires near the
// state line still render. Order: WEST,SOUTH,EAST,NORTH (matches FIRMS).
const NSW_BBOX = '140.0,-38.0,154.0,-28.0';

// VIIRS sensor flies on three platforms (S-NPP, NOAA-20, NOAA-21), each
// gives a separate ~12 h orbital pass over NSW. Polling all three
// roughly doubles the temporal coverage versus NOAA-20 alone, which is
// the difference between catching a fire flare-up at hour 0 vs hour 12.
// Each detection gets tagged with `source` so the frontend can offer
// a per-satellite toggle.
//
// All 375 m resolution NRT. Free MAP_KEY allows ~5000 calls per 10 min
// — three sources at our 15 min cadence is 12 calls/h, comfortably
// under the limit.
interface FirmsSource {
  id: string;          // FIRMS source slug used in the URL
  label: string;       // human label for the frontend
  satellite: string;   // canonical satellite tag in our properties
}
const FIRMS_SOURCES: readonly FirmsSource[] = [
  { id: 'VIIRS_SNPP_NRT',   label: 'VIIRS Suomi-NPP', satellite: 'S-NPP' },
  { id: 'VIIRS_NOAA20_NRT', label: 'VIIRS NOAA-20',   satellite: 'NOAA-20' },
  { id: 'VIIRS_NOAA21_NRT', label: 'VIIRS NOAA-21',   satellite: 'NOAA-21' },
] as const;

const DAY_RANGE = 1; // last 24 hours
const FIRMS_API_BASE = 'https://firms.modaps.eosdis.nasa.gov/api/area/csv';

// Approx degrees per km — used to convert FIRMS scan/track (km) into
// the lat/lon offsets needed to draw the pixel footprint polygon.
const DEG_PER_KM_LAT = 1 / 111.0; // good across all latitudes for lat
function degPerKmLon(latDeg: number): number {
  // 1 deg of longitude shrinks as cos(lat). Clamp at 70° so polygons
  // near a pole don't explode (NSW is comfortably below 40°S anyway).
  const lat = Math.max(-70, Math.min(70, latDeg));
  return 1 / (111.0 * Math.cos((lat * Math.PI) / 180));
}

export interface FirmsHotspotProperties {
  latitude: number;
  longitude: number;
  brightness: number | null;
  scan_km: number | null;
  track_km: number | null;
  acq_date: string;
  acq_time: string;
  acq_datetime: string; // ISO UTC, derived from acq_date + acq_time
  satellite: string;
  instrument: string;
  confidence: string; // 'low' | 'nominal' | 'high' for VIIRS, '0-100' for MODIS
  version: string;
  bright_t31: number | null; // bright_ti5 for VIIRS (named bright_t31 in MODIS)
  frp: number | null; // fire radiative power (MW)
  daynight: 'D' | 'N' | string;
  source: string;       // FIRMS source slug (e.g. VIIRS_NOAA20_NRT)
  satellite_tag: string; // canonical platform label (S-NPP / NOAA-20 / NOAA-21)
}

export interface FirmsFeature {
  type: 'Feature';
  geometry: { type: 'Polygon'; coordinates: [Array<[number, number]>] };
  properties: FirmsHotspotProperties;
}

export interface FirmsSnapshot {
  type: 'FeatureCollection';
  features: FirmsFeature[];
  count: number;
  /** Comma-joined list of FIRMS source slugs that contributed. */
  sources: string[];
  /** Per-source success / count breakdown so the admin / status panel
   *  can see at a glance which platforms returned data. */
  source_status: Array<{
    source: string;
    satellite: string;
    label: string;
    ok: boolean;
    count: number;
    error?: string;
  }>;
  bbox: string;
  fetched_at: string;
}

const EMPTY_SNAPSHOT: FirmsSnapshot = {
  type: 'FeatureCollection',
  features: [],
  count: 0,
  sources: FIRMS_SOURCES.map((s) => s.id),
  source_status: FIRMS_SOURCES.map((s) => ({
    source: s.id,
    satellite: s.satellite,
    label: s.label,
    ok: false,
    count: 0,
  })),
  bbox: NSW_BBOX,
  fetched_at: new Date(0).toISOString(),
};

/**
 * Build the rectangular pixel footprint for one detection. scan is the
 * across-track size (longitude direction), track is the along-track size
 * (latitude direction). Both are in kilometres. Falls back to a 0.375 km
 * square when the CSV row omits them (rare, but happens on edge passes).
 */
function buildPixelPolygon(
  lat: number,
  lon: number,
  scanKm: number | null,
  trackKm: number | null,
): [Array<[number, number]>] {
  const scan = scanKm && scanKm > 0 ? scanKm : 0.375;
  const track = trackKm && trackKm > 0 ? trackKm : 0.375;
  const halfLat = (track / 2) * DEG_PER_KM_LAT;
  const halfLon = (scan / 2) * degPerKmLon(lat);
  const ring: Array<[number, number]> = [
    [lon - halfLon, lat - halfLat],
    [lon + halfLon, lat - halfLat],
    [lon + halfLon, lat + halfLat],
    [lon - halfLon, lat + halfLat],
    [lon - halfLon, lat - halfLat],
  ];
  return [ring];
}

/**
 * Parse a FIRMS CSV body into Features. The CSV is tiny (a few thousand
 * rows even on a busy day) and well-formed; a hand-rolled splitter avoids
 * pulling in a CSV library.
 */
export function parseFirmsCsv(
  csv: string,
  src: FirmsSource = FIRMS_SOURCES[0] as FirmsSource,
): FirmsFeature[] {
  const text = csv.replace(/^﻿/, '').trim();
  if (!text) return [];
  const lines = text.split(/\r?\n/);
  if (lines.length < 2) return [];

  const header = (lines[0] ?? '').split(',').map((h) => h.trim().toLowerCase());
  const idx = (name: string): number => header.indexOf(name);
  const iLat = idx('latitude');
  const iLon = idx('longitude');
  // VIIRS uses bright_ti4/bright_ti5; MODIS uses brightness/bright_t31.
  // Pick whichever is present.
  const iBright = idx('bright_ti4') !== -1 ? idx('bright_ti4') : idx('brightness');
  const iScan = idx('scan');
  const iTrack = idx('track');
  const iAcqDate = idx('acq_date');
  const iAcqTime = idx('acq_time');
  const iSatellite = idx('satellite');
  const iInstrument = idx('instrument');
  const iConfidence = idx('confidence');
  const iVersion = idx('version');
  const iBrightT31 = idx('bright_ti5') !== -1 ? idx('bright_ti5') : idx('bright_t31');
  const iFrp = idx('frp');
  const iDayNight = idx('daynight');

  if (iLat === -1 || iLon === -1) {
    // Not a FIRMS CSV — likely an error page or rate-limit text.
    return [];
  }

  const out: FirmsFeature[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line) continue;
    const cells = line.split(',');
    const lat = Number(cells[iLat]);
    const lon = Number(cells[iLon]);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    const acqDate = (cells[iAcqDate] ?? '').trim();
    const acqTime = (cells[iAcqTime] ?? '').trim().padStart(4, '0');
    // acq_time is HHMM (24h) UTC. Compose an ISO timestamp.
    const acqIso =
      acqDate && acqTime.length >= 4
        ? `${acqDate}T${acqTime.slice(0, 2)}:${acqTime.slice(2, 4)}:00Z`
        : '';

    const numOrNull = (j: number): number | null => {
      if (j === -1) return null;
      const v = Number(cells[j]);
      return Number.isFinite(v) ? v : null;
    };
    const strOrEmpty = (j: number): string =>
      j === -1 ? '' : (cells[j] ?? '').trim();

    const scanKm = numOrNull(iScan);
    const trackKm = numOrNull(iTrack);

    const props: FirmsHotspotProperties = {
      latitude: lat,
      longitude: lon,
      brightness: numOrNull(iBright),
      scan_km: scanKm,
      track_km: trackKm,
      acq_date: acqDate,
      acq_time: acqTime,
      acq_datetime: acqIso,
      satellite: strOrEmpty(iSatellite),
      instrument: strOrEmpty(iInstrument),
      confidence: strOrEmpty(iConfidence),
      version: strOrEmpty(iVersion),
      bright_t31: numOrNull(iBrightT31),
      frp: numOrNull(iFrp),
      daynight: strOrEmpty(iDayNight) as 'D' | 'N' | string,
      source: src.id,
      satellite_tag: src.satellite,
    };

    out.push({
      type: 'Feature',
      geometry: {
        type: 'Polygon',
        coordinates: buildPixelPolygon(lat, lon, scanKm, trackKm),
      },
      properties: props,
    });
  }
  return out;
}

/**
 * Fetch one FIRMS source. Bounded retry on transient errors (US-hosted
 * gateway flakes from AU). On hard failure returns `{ ok: false, error }`
 * so the parent fetcher can mark just that source down without losing
 * the other satellites.
 */
async function fetchOneFirmsSource(
  key: string,
  src: FirmsSource,
): Promise<{ ok: true; features: FirmsFeature[] } | { ok: false; error: string }> {
  const url = `${FIRMS_API_BASE}/${encodeURIComponent(key)}/${src.id}/${NSW_BBOX}/${DAY_RANGE}`;
  let csv: string;
  try {
    try {
      csv = await fetchText(url, { timeoutMs: 60_000 });
    } catch (err) {
      const msg = (err as Error).message ?? '';
      const transient =
        /ETIMEDOUT|ECONNRESET|ECONNREFUSED|ENOTFOUND|EAI_AGAIN|timeout|fetch failed/i.test(msg);
      if (!transient) throw err;
      log.warn({ err: msg, source: src.id }, 'firms: upstream timed out, retrying once');
      await new Promise((r) => setTimeout(r, 5_000));
      csv = await fetchText(url, { timeoutMs: 90_000 });
    }
  } catch (err) {
    return { ok: false, error: (err as Error).message ?? 'unknown error' };
  }
  const features = parseFirmsCsv(csv, src);
  if (features.length === 0 && csv.length > 0 && !csv.toLowerCase().includes('latitude')) {
    log.warn(
      { source: src.id, head: csv.slice(0, 200) },
      'firms: upstream returned non-CSV body',
    );
  }
  return { ok: true, features };
}

export async function fetchFirmsHotspots(): Promise<FirmsSnapshot> {
  const key = config.MAP_KEY;
  if (!key) {
    // No key — return empty so the frontend just shows nothing.
    return { ...EMPTY_SNAPSHOT, fetched_at: new Date().toISOString() };
  }
  // Fan out to every VIIRS platform in parallel. Per-source failures
  // don't fail the whole snapshot — operators get a partial result
  // (e.g. NOAA-20 timed out but S-NPP + NOAA-21 returned) which is
  // strictly more useful than 0 detections.
  const results = await Promise.all(
    FIRMS_SOURCES.map((src) => fetchOneFirmsSource(key, src)),
  );
  const features: FirmsFeature[] = [];
  const sourceStatus: FirmsSnapshot['source_status'] = [];
  for (let i = 0; i < FIRMS_SOURCES.length; i++) {
    const src = FIRMS_SOURCES[i];
    const r = results[i];
    if (!src || !r) continue;
    if (r.ok) {
      features.push(...r.features);
      sourceStatus.push({
        source: src.id,
        satellite: src.satellite,
        label: src.label,
        ok: true,
        count: r.features.length,
      });
    } else {
      sourceStatus.push({
        source: src.id,
        satellite: src.satellite,
        label: src.label,
        ok: false,
        count: 0,
        error: r.error,
      });
    }
  }
  return {
    type: 'FeatureCollection',
    features,
    count: features.length,
    sources: FIRMS_SOURCES.map((s) => s.id),
    source_status: sourceStatus,
    bbox: NSW_BBOX,
    fetched_at: new Date().toISOString(),
  };
}

export default function register(): void {
  registerSource<FirmsSnapshot>({
    name: 'firms_hotspots',
    family: 'misc',
    // 15 min active / 30 min idle. FIRMS NRT updates ~every few hours;
    // polling more often just wastes API quota.
    intervalActiveMs: 15 * 60_000,
    intervalIdleMs: 30 * 60_000,
    fetch: fetchFirmsHotspots,
  });
}

export function firmsSnapshot(): FirmsSnapshot {
  return liveStore.getData<FirmsSnapshot>('firms_hotspots') ?? EMPTY_SNAPSHOT;
}
