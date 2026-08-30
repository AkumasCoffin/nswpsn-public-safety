/**
 * QLD Traffic cameras — data.qldtraffic.qld.gov.au -> GeoJSON.
 *
 * Two upstream files, same schema, different things:
 *   webcameras.geojson   ~137 roadside traffic cameras, images refreshed
 *                        upstream about once a minute.
 *   floodcameras.geojson ~119 cameras watching flood-prone crossings.
 *                        Many are council-supplied and, per their own
 *                        `extra_info`, only refresh every 30 minutes.
 *
 * They are registered as two sources so one feed failing can't take the
 * other down, and so the map can toggle them apart — a flood camera at a
 * causeway is a different question from a motorway camera.
 *
 * Both emit the SAME property names as the NSW camera source
 * (sources/traffic.ts, `livetraffic_cameras`) so map.html renders all of
 * them through one code path; `source` is what tells them apart.
 *
 * Field mapping, upstream -> ours:
 *   description        -> title      "Mossman - Mossman Daintree Rd at Foxton bridge - North"
 *   locality           -> view       the suburb, which is all NSW's `view` is
 *   direction          -> direction  compass word, e.g. 'NorthEast'
 *   district           -> region     QLD's own regions: Metropolitan, Far North, ...
 *   image_url          -> imageUrl   a direct JPEG
 *   image_sourced_from -> credit     the supplying agency/council (flood cams)
 *   extra_info         -> note       upstream's own caveats, e.g. refresh cadence
 *
 * Upstream quirks worth knowing:
 *   - geometry.crs says EPSG:7844 (GDA2020). Coordinates are still
 *     lon/lat degrees and agree with WGS84 to well under a metre, so they
 *     go through untouched.
 *   - the two files have SEPARATE id spaces that merely don't collide
 *     today (webcams 1-331, flood 190-348). Ids are prefixed per feed so
 *     they can never merge into one marker.
 *   - `image_sourced_from` arrives with inconsistent spelling and
 *     trailing spaces ('… and Main Roads ' / '… & Main Roads'), so it is
 *     whitespace-normalised before display.
 *   - the per-camera `url` field points at api.qldtraffic.qld.gov.au/v1,
 *     which DOES want an apikey. We never call it — the bulk .geojson and
 *     the image host are both keyless — so no config is needed.
 *
 * Live-only, like the other camera sources: both names are in the
 * poller's SKIP_ARCHIVE, so nothing reaches the archive or the logs page.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const WEBCAMS_URL = 'https://data.qldtraffic.qld.gov.au/webcameras.geojson';
const FLOODCAMS_URL = 'https://data.qldtraffic.qld.gov.au/floodcameras.geojson';

export type QldCameraSource = 'qldtraffic_cameras' | 'qldtraffic_floodcameras';

export interface QldCameraFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    id: string;
    title: string;
    view: string;
    direction: string;
    region: string;
    imageUrl: string;
    path: string;
    /** Supplying agency or council, when upstream names one. */
    credit: string;
    /** Upstream's own caveat text, e.g. its refresh cadence. */
    note: string;
    source: QldCameraSource;
  };
}

export interface QldCamerasSnapshot {
  type: 'FeatureCollection';
  features: QldCameraFeature[];
  count: number;
}

const EMPTY: QldCamerasSnapshot = { type: 'FeatureCollection', features: [], count: 0 };

function asString(v: unknown): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  return '';
}

function asNumber(v: unknown): number | null {
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/** Collapse the runs of whitespace upstream leaves in free-text fields. */
function tidy(v: unknown): string {
  return asString(v).replace(/\s+/g, ' ').trim();
}

/**
 * Map one upstream feature, or null when it can't be placed or shown.
 *
 * `idPrefix` keeps the two feeds' id spaces apart — see the header note.
 */
export function toCameraFeature(
  raw: unknown,
  source: QldCameraSource,
  idPrefix: string,
): QldCameraFeature | null {
  if (!raw || typeof raw !== 'object') return null;
  const f = raw as Record<string, unknown>;

  const geom = f['geometry'] as Record<string, unknown> | undefined;
  if (!geom || geom['type'] !== 'Point') return null;
  const coords = geom['coordinates'];
  if (!Array.isArray(coords) || coords.length < 2) return null;
  const lon = asNumber(coords[0]);
  const lat = asNumber(coords[1]);
  if (lon === null || lat === null) return null;

  const props = (f['properties'] as Record<string, unknown> | undefined) ?? {};
  const id = asString(props['id']);
  if (!id) return null;

  // A camera with no image is just a dot that does nothing when clicked.
  const imageUrl = asString(props['image_url']);
  if (!imageUrl) return null;

  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [lon, lat] },
    properties: {
      id: `${idPrefix}${id}`,
      title: tidy(props['description']) || 'QLD Traffic Camera',
      view: tidy(props['locality']),
      direction: tidy(props['direction']),
      region: tidy(props['district']),
      imageUrl,
      // NSW carries a `path` for its deep links; QLD has no equivalent,
      // and map.html only reads it when non-empty.
      path: '',
      credit: tidy(props['image_sourced_from']),
      note: tidy(props['extra_info']),
      source,
    },
  };
}

/** Shared fetch/parse for both feeds. Throws so the poller backs off. */
async function fetchFeed(
  url: string,
  source: QldCameraSource,
  idPrefix: string,
  label: string,
): Promise<QldCamerasSnapshot> {
  const data = await fetchJson<unknown>(url, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
    timeoutMs: 20_000,
  });
  const raw = data as Record<string, unknown> | null;
  const items = raw && Array.isArray(raw['features']) ? (raw['features'] as unknown[]) : null;
  if (!items) {
    throw new Error(`${label}: feed returned no features array`);
  }
  const features: QldCameraFeature[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    const f = toCameraFeature(item, source, idPrefix);
    if (!f) continue;
    // Ids are unique upstream today; guard anyway so one duplicate can't
    // stack two markers on the same spot.
    if (seen.has(f.properties.id)) continue;
    seen.add(f.properties.id);
    features.push(f);
  }
  return { type: 'FeatureCollection', features, count: features.length };
}

export function fetchQldCameras(): Promise<QldCamerasSnapshot> {
  return fetchFeed(WEBCAMS_URL, 'qldtraffic_cameras', 'qld:', 'qld_cameras');
}

export function fetchQldFloodCameras(): Promise<QldCamerasSnapshot> {
  return fetchFeed(FLOODCAMS_URL, 'qldtraffic_floodcameras', 'qldflood:', 'qld_flood_cameras');
}

export default function register(): void {
  // The camera LISTS change rarely; it's the images that move. Matching
  // the NSW camera cadence keeps the layers in step.
  registerSource<QldCamerasSnapshot>({
    name: 'qld_cameras',
    family: 'traffic',
    intervalMs: 60_000,
    fetch: fetchQldCameras,
  });
  registerSource<QldCamerasSnapshot>({
    name: 'qld_flood_cameras',
    family: 'traffic',
    intervalMs: 60_000,
    fetch: fetchQldFloodCameras,
  });
}

/** Route helpers — live snapshot, or an empty collection while the
 *  poller hasn't filled it yet. */
export function qldCamerasSnapshot(): QldCamerasSnapshot {
  return liveStore.getData<QldCamerasSnapshot>('qld_cameras') ?? EMPTY;
}

export function qldFloodCamerasSnapshot(): QldCamerasSnapshot {
  return liveStore.getData<QldCamerasSnapshot>('qld_flood_cameras') ?? EMPTY;
}
