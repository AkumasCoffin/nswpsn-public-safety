/**
 * Central Watch fire-tower cameras.
 *
 * The python service maintains the canonical data file at
 * backends/data/centralwatch_cameras.json (Vercel-bypass via Playwright,
 * 30-min refresh, atomic write). Porting that worker is out of W8 scope —
 * we'd need to bring chromium into the Node runtime. So we treat the
 * JSON file as the source of truth and ride along.
 *
 * Refresh strategy: re-read the JSON every 60s (only when its mtime has
 * changed) so the Node service picks up python's atomic-rename writes
 * without hammering the filesystem.
 *
 * The image proxy (/api/centralwatch/image/:id) stays on python — its
 * cache is populated by the Playwright worker that solves the Vercel
 * challenge. Apache pins that route to python.
 */
import { promises as fs } from 'node:fs';
import * as path from 'node:path';
import { log } from '../lib/log.js';

const JSON_PATH = path.resolve(
  process.cwd(),
  '..',
  'data',
  'centralwatch_cameras.json',
);
const REFRESH_INTERVAL_MS = 60_000;

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

function normaliseTime(raw: string): string {
  // python normalises "2026-02-26T02:25:55.366+00:00" -> "...Z" before
  // passing the timestamp through. Match that so frontend cache-busting
  // stays consistent across both backends.
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
  // Re-read only when the python writer has touched the file.
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

export function _resetCentralwatchCacheForTests(): void {
  cache = null;
}

export const _testHooks = {
  setCacheForTests(entry: CacheEntry | null): void {
    cache = entry;
  },
};
