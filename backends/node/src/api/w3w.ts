/**
 * What3Words proxy endpoints.
 *
 * Mirrors python external_api_proxy.py:10808-10926. The mapapi.what3words.com
 * upstream is unauthenticated (browser-grade key-less endpoints), so this
 * is a thin pass-through with input validation + a 24h cache on the
 * grid-section call (the grid never changes for a given bbox).
 *
 *   GET /api/w3w/convert-to-coordinates  ?words=word.word.word
 *   GET /api/w3w/convert-to-3wa          ?coordinates=lat,lon  (or ?lat= ?lon=)
 *   GET /api/w3w/grid-section            ?bounding-box=south,west,north,east
 */
import { Hono } from 'hono';
import { fetchRaw } from '../sources/shared/http.js';
import { log } from '../lib/log.js';

const W3W_BASE = 'https://mapapi.what3words.com/api';

export const w3wRouter = new Hono();

interface W3WErrorBody {
  error?: { message?: string };
}

async function pass<T = unknown>(
  c: { json: (b: unknown, s?: number) => Response },
  endpoint: string,
  params: Record<string, string>,
  errLabel: string,
): Promise<Response> {
  const url = `${W3W_BASE}${endpoint}?${new URLSearchParams(params).toString()}`;
  try {
    const res = await fetchRaw(url, {
      timeoutMs: 10_000,
      allow_non_2xx: true,
    });
    let data: unknown;
    try {
      data = JSON.parse(res.text);
    } catch {
      data = { error: { message: 'Invalid JSON from upstream' } };
    }
    const errMsg = (data as W3WErrorBody)?.error?.message;
    if (res.status !== 200 || errMsg) {
      return c.json(
        { error: errMsg ?? 'Unknown error' },
        (res.status !== 200 ? res.status : 400) as 400,
      );
    }
    return c.json(data as T);
  } catch (err) {
    log.warn({ err: (err as Error).message }, `${errLabel} fetch failed`);
    return c.json({ error: 'Failed to contact What3Words API' }, 502);
  }
}

w3wRouter.get('/api/w3w/convert-to-coordinates', async (c) => {
  const url = new URL(c.req.url);
  const words = (url.searchParams.get('words') ?? '').trim().toLowerCase();
  if (!words || (words.match(/\./g) ?? []).length !== 2) {
    return c.json(
      {
        error:
          'Invalid what3words address. Expected format: word.word.word',
      },
      400,
    );
  }
  return pass(c, '/convert-to-coordinates', { words, format: 'json' }, 'w3w convert-to-coordinates');
});

w3wRouter.get('/api/w3w/convert-to-3wa', async (c) => {
  const url = new URL(c.req.url);
  let coordinates = (url.searchParams.get('coordinates') ?? '').trim();
  if (!coordinates) {
    const lat = url.searchParams.get('lat') ?? '';
    const lon = url.searchParams.get('lon') ?? '';
    if (lat && lon) coordinates = `${lat},${lon}`;
  }
  if (!coordinates) {
    return c.json({ error: 'Missing coordinates parameter' }, 400);
  }
  return pass(
    c,
    '/convert-to-3wa',
    { coordinates, language: 'en', format: 'json' },
    'w3w convert-to-3wa',
  );
});

// Grid-section cache: bbox -> {json, timestamp}. 24h TTL, 500-entry cap
// with LRU-by-timestamp eviction.
interface GridCacheEntry {
  payload: unknown;
  ts: number;
}
const gridCache = new Map<string, GridCacheEntry>();
const GRID_CACHE_TTL_MS = 24 * 60 * 60_000;
const GRID_CACHE_MAX = 500;

function roundBbox(bbox: string, precision = 3): string {
  const parts = bbox.split(',').map((s) => Number(s.trim()));
  if (parts.length !== 4 || !parts.every(Number.isFinite)) return bbox;
  const factor = 10 ** precision;
  return parts
    .map((p) => Math.round(p * factor) / factor)
    .join(',');
}

w3wRouter.get('/api/w3w/grid-section', async (c) => {
  const url = new URL(c.req.url);
  const bbox = (url.searchParams.get('bounding-box') ?? '').trim();
  if (!bbox) {
    return c.json({ error: 'Missing bounding-box parameter' }, 400);
  }
  const key = roundBbox(bbox);
  const now = Date.now();
  const cached = gridCache.get(key);
  if (cached && now - cached.ts < GRID_CACHE_TTL_MS) {
    return c.json(cached.payload);
  }
  try {
    const res = await fetchRaw(
      `${W3W_BASE}/grid-section?${new URLSearchParams({
        'bounding-box': bbox,
        format: 'geojson',
      })}`,
      { timeoutMs: 10_000, allow_non_2xx: true },
    );
    let data: unknown;
    try {
      data = JSON.parse(res.text);
    } catch {
      return c.json({ error: 'Invalid JSON from upstream' }, 502);
    }
    const errMsg = (data as W3WErrorBody)?.error?.message;
    if (res.status !== 200 || errMsg) {
      return c.json(
        { error: errMsg ?? 'Unknown error' },
        (res.status !== 200 ? res.status : 400) as 400,
      );
    }
    if (gridCache.size >= GRID_CACHE_MAX) {
      // Drop the oldest entry — Map iteration order is insertion order,
      // so the first key is the least-recently-inserted. We don't refresh
      // ts on cache hits (matches python).
      const first = gridCache.keys().next().value;
      if (first) gridCache.delete(first);
    }
    gridCache.set(key, { payload: data, ts: now });
    return c.json(data);
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'w3w grid-section fetch failed');
    return c.json({ error: 'Failed to contact What3Words API' }, 502);
  }
});

export function _resetW3wCacheForTests(): void {
  gridCache.clear();
}
