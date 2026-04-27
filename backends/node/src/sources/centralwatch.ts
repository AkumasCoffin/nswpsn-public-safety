/**
 * Central Watch fire-tower cameras.
 *
 * Now owned end-to-end by the Node backend. The refresh loop fetches the
 * upstream camera list every 30 minutes via the Playwright browser worker
 * (which solves the Vercel Security Checkpoint), normalises the response,
 * and atomically writes backends/data/centralwatch_cameras.json. The
 * file readers below stay backwards-compatible — when the writer is
 * disabled (no Playwright / CENTRALWATCH_DISABLED=true), the readers
 * still serve the last-good JSON file so deploys without chromium keep
 * working.
 *
 * Mirrors python `_refresh_centralwatch_data` and `_update_centralwatch_json`
 * (external_api_proxy.py:8321-8438).
 */
import { promises as fs } from 'node:fs';
import * as path from 'node:path';
import { log } from '../lib/log.js';
import { centralwatchBrowser } from '../services/centralwatchBrowser.js';

const JSON_PATH = path.resolve(
  process.cwd(),
  '..',
  'data',
  'centralwatch_cameras.json',
);
const REFRESH_INTERVAL_MS = 60_000; // file-reader cache invalidation
const API_REFRESH_INTERVAL_MS = 30 * 60 * 1000; // 30 min — match python
const API_ENDPOINT = 'https://centralwatch.watchtowers.io/au/api/cameras';

export interface CentralwatchSite {
  name: string;
  latitude: number;
  longitude: number;
  altitude: number | null;
  state: string;
}

export interface CentralwatchCameraRaw {
  id: string;
  name: string;
  siteId: string;
  time: string;
}

export interface CentralwatchCamera {
  id: string;
  name: string;
  siteName: string;
  siteId: string;
  latitude: number | null;
  longitude: number | null;
  altitude: number | null;
  state: string;
  imageUrl: string;
  time: string;
  source: 'centralwatch';
}

interface RawJson {
  lastUpdated?: string;
  source?: string;
  sites?: Record<string, CentralwatchSite>;
  cameras?: CentralwatchCameraRaw[];
}

interface CacheEntry {
  cameras: CentralwatchCamera[];
  sites: Record<string, CentralwatchSite>;
  loadedAt: number;
  fileMtimeMs: number;
}

let cache: CacheEntry | null = null;
let refreshTimer: NodeJS.Timeout | null = null;
let refreshInFlight = false;
let lastRefreshOk = 0;

function normaliseTime(raw: string): string {
  if (!raw) return '';
  if (raw.includes('+') && !raw.endsWith('Z')) {
    const idx = raw.indexOf('+');
    return `${raw.slice(0, idx)}Z`;
  }
  return raw;
}

function joinCameras(raw: RawJson): CentralwatchCamera[] {
  const sites = raw.sites ?? {};
  const cameras = Array.isArray(raw.cameras) ? raw.cameras : [];
  const out: CentralwatchCamera[] = [];
  for (const cam of cameras) {
    if (!cam || typeof cam !== 'object') continue;
    const site = sites[cam.siteId];
    if (!site) continue;
    if (typeof site.latitude !== 'number' || typeof site.longitude !== 'number') {
      continue;
    }
    out.push({
      id: cam.id,
      name: cam.name || 'Fire Watch Camera',
      siteName: site.name || '',
      siteId: cam.siteId,
      latitude: site.latitude,
      longitude: site.longitude,
      altitude: site.altitude ?? null,
      state: site.state || '',
      imageUrl: `/api/centralwatch/image/${cam.id}`,
      time: normaliseTime(cam.time || ''),
      source: 'centralwatch',
    });
  }
  return out;
}

async function loadFromDisk(): Promise<CacheEntry | null> {
  let stat;
  try {
    stat = await fs.stat(JSON_PATH);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
      log.warn(
        { err: (err as Error).message, path: JSON_PATH },
        'centralwatch JSON stat failed',
      );
    }
    return null;
  }
  if (cache && stat.mtimeMs === cache.fileMtimeMs) {
    cache.loadedAt = Date.now();
    return cache;
  }
  try {
    const text = await fs.readFile(JSON_PATH, 'utf8');
    const parsed = JSON.parse(text) as RawJson;
    const cameras = joinCameras(parsed);
    const entry: CacheEntry = {
      cameras,
      sites: parsed.sites ?? {},
      loadedAt: Date.now(),
      fileMtimeMs: stat.mtimeMs,
    };
    cache = entry;
    return entry;
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'centralwatch JSON parse failed',
    );
    return cache;
  }
}

export async function getCentralwatchCameras(): Promise<CentralwatchCamera[]> {
  if (cache && Date.now() - cache.loadedAt < REFRESH_INTERVAL_MS) {
    return cache.cameras;
  }
  const entry = await loadFromDisk();
  return entry?.cameras ?? [];
}

export async function getCentralwatchSites(): Promise<
  Array<{
    siteId: string;
    siteName: string;
    latitude: number | null;
    longitude: number | null;
    altitude: number | null;
    state: string;
    cameras: Array<{ id: string; name: string; imageUrl: string }>;
  }>
> {
  const cams = await getCentralwatchCameras();
  const groups = new Map<
    string,
    {
      siteId: string;
      siteName: string;
      latitude: number | null;
      longitude: number | null;
      altitude: number | null;
      state: string;
      cameras: Array<{ id: string; name: string; imageUrl: string }>;
    }
  >();
  for (const cam of cams) {
    const key = cam.siteId || cam.id;
    let group = groups.get(key);
    if (!group) {
      group = {
        siteId: key,
        siteName: cam.siteName || 'Unknown Site',
        latitude: cam.latitude,
        longitude: cam.longitude,
        altitude: cam.altitude,
        state: cam.state,
        cameras: [],
      };
      groups.set(key, group);
    }
    group.cameras.push({
      id: cam.id,
      name: cam.name,
      imageUrl: cam.imageUrl,
    });
  }
  return Array.from(groups.values());
}

// =============================================================================
// Writer + refresh loop (W8 port).
// =============================================================================

interface UpstreamSite {
  id?: string;
  name?: string;
  latitude?: number | null;
  longitude?: number | null;
  altitude?: number | null;
  state?: string | null;
}

interface UpstreamCamera {
  id?: string;
  name?: string;
  siteId?: string;
  time?: string;
}

interface UpstreamPayload {
  sites?: UpstreamSite[];
  cameras?: UpstreamCamera[];
}

/**
 * Atomic write — temp file + rename. Mirrors python's
 * `_update_centralwatch_json` (line ~8369).
 */
export async function writeCentralwatchJson(api: UpstreamPayload): Promise<boolean> {
  if (!api || typeof api !== 'object') return false;
  const rawSites = Array.isArray(api.sites) ? api.sites : [];
  const rawCameras = Array.isArray(api.cameras) ? api.cameras : [];
  if (rawSites.length === 0 || rawCameras.length === 0) {
    log.warn(
      { sites: rawSites.length, cameras: rawCameras.length },
      'centralwatch JSON update skipped: empty upstream payload',
    );
    return false;
  }

  const sitesDict: Record<string, CentralwatchSite> = {};
  for (const s of rawSites) {
    if (!s || !s.id) continue;
    sitesDict[s.id] = {
      name: s.name ?? '',
      latitude: typeof s.latitude === 'number' ? s.latitude : (null as unknown as number),
      longitude:
        typeof s.longitude === 'number' ? s.longitude : (null as unknown as number),
      altitude: s.altitude ?? null,
      state: s.state ?? '',
    };
  }

  const camerasList: CentralwatchCameraRaw[] = [];
  for (const cam of rawCameras) {
    if (!cam || !cam.id) continue;
    let camTime = cam.time ?? '';
    if (camTime && camTime.includes('+') && !camTime.endsWith('Z')) {
      camTime = `${camTime.split('+')[0]}Z`;
    }
    camerasList.push({
      id: cam.id,
      name: cam.name ?? '',
      siteId: cam.siteId ?? '',
      time: camTime,
    });
  }

  const lastUpdated = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
  const payload = {
    lastUpdated,
    source: API_ENDPOINT,
    sites: sitesDict,
    cameras: camerasList,
  };

  const tmpPath = `${JSON_PATH}.tmp`;
  try {
    await fs.mkdir(path.dirname(JSON_PATH), { recursive: true });
    await fs.writeFile(tmpPath, JSON.stringify(payload, null, 2), 'utf8');
    await fs.rename(tmpPath, JSON_PATH);
    // Drop the in-memory cache so the next reader sees fresh data.
    cache = null;
    log.info(
      { sites: Object.keys(sitesDict).length, cameras: camerasList.length },
      'centralwatch JSON updated',
    );
    return true;
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'centralwatch JSON write failed',
    );
    try {
      await fs.unlink(tmpPath);
    } catch {
      // ignore
    }
    return false;
  }
}

/**
 * One-shot refresh: pull from the upstream API via the browser worker,
 * normalise, atomic-write the JSON file. Returns true on success.
 */
export async function refreshCentralwatchJson(): Promise<boolean> {
  if (!centralwatchBrowser.isReady()) {
    return false;
  }
  try {
    const data = (await centralwatchBrowser.fetchJson(API_ENDPOINT)) as
      | UpstreamPayload
      | null;
    if (!data || typeof data !== 'object') {
      log.warn('centralwatch refresh: upstream returned no data');
      return false;
    }
    if (!Array.isArray(data.cameras) || data.cameras.length === 0) {
      log.warn('centralwatch refresh: upstream returned no cameras');
      return false;
    }
    const ok = await writeCentralwatchJson(data);
    if (ok) {
      lastRefreshOk = Date.now();
      log.info(
        { count: data.cameras.length },
        'centralwatch refresh: data refreshed',
      );
    }
    return ok;
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'centralwatch refresh: threw',
    );
    return false;
  }
}

export function startCentralwatchRefreshLoop(): void {
  if (refreshTimer) return;
  if (process.env['CENTRALWATCH_DISABLED'] === 'true') {
    log.info('centralwatch refresh loop disabled via env');
    return;
  }
  const tick = async (): Promise<void> => {
    if (refreshInFlight) return;
    if (Date.now() - lastRefreshOk < API_REFRESH_INTERVAL_MS) return;
    refreshInFlight = true;
    try {
      await refreshCentralwatchJson();
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'centralwatch refresh tick failed',
      );
    } finally {
      refreshInFlight = false;
    }
  };
  // Run on a 5-min cadence; the 30-min throttle inside `tick` keeps
  // the actual upstream traffic to once per half-hour but lets us
  // recover quickly from a missed window after a restart.
  refreshTimer = setInterval(() => void tick(), 5 * 60 * 1000);
  // First tick a few seconds after boot to let the browser settle.
  setTimeout(() => void tick(), 5_000);
}

export function stopCentralwatchRefreshLoop(): void {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

export function _resetCentralwatchCacheForTests(): void {
  cache = null;
  lastRefreshOk = 0;
}

export const _testHooks = {
  setCacheForTests(entry: CacheEntry | null): void {
    cache = entry;
  },
};
