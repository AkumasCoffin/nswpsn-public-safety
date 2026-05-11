/**
 * Beach endpoints.
 *
 *   GET /api/beachwatch              — NSW DCCEEW water-quality GeoJSON
 *   GET /api/beachsafe                — Surf Life Saving NSW summary
 *   GET /api/beachsafe/details        — pre-fetched detail dict by slug
 *   GET /api/beachsafe/beach/:slug    — single beach detail (live fetch)
 *
 * Mirrors python's routes at external_api_proxy.py:5644 / 5670 / 5743 /
 * 5813. The summary + details routes serve from LiveStore. The
 * `/beach/:slug` route hits upstream live, identical to the python
 * implementation which caches it at the HTTP layer.
 */
import { Hono } from 'hono';
import {
  beachsafeDetailsSnapshot,
  beachsafeSnapshot,
  beachwatchSnapshot,
} from '../sources/beach.js';
import { fetchJson, HttpError } from '../sources/shared/http.js';
import { log } from '../lib/log.js';

export const beachRouter = new Hono();

beachRouter.get('/api/beachwatch', (c) => c.json(beachwatchSnapshot()));

beachRouter.get('/api/beachsafe', (c) => c.json(beachsafeSnapshot()));

beachRouter.get('/api/beachsafe/details', (c) =>
  c.json(beachsafeDetailsSnapshot()),
);

beachRouter.get('/api/beachsafe/beach/:slug', async (c) => {
  const rawSlug = c.req.param('slug') ?? '';
  const slug = rawSlug.trim().toLowerCase().replace(/\s+/g, '-');
  if (!slug || slug.length > 100) {
    return c.json({ error: 'Invalid slug' }, 400);
  }

  try {
    const data = await fetchJson<unknown>(
      `https://beachsafe.org.au/api/v4/beach/${encodeURIComponent(slug)}`,
      {
        timeoutMs: 10_000,
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          Accept: 'application/json',
          Referer: 'https://beachsafe.org.au/',
        },
      },
    );
    return c.json(shapeBeachDetail(data));
  } catch (err) {
    if (err instanceof HttpError) {
      log.warn({ slug, status: err.status }, 'beachsafe detail fetch failed');
    } else {
      log.warn({ slug, err: (err as Error).message }, 'beachsafe detail error');
    }
    return c.json({});
  }
});

interface BeachDetailResponse {
  weather: Record<string, unknown>;
  currentTide: unknown;
  currentUV: unknown;
  latestAttendance: { date: string; entries: unknown } | null;
  todays_marine_warnings: unknown[];
  patrol: number | string;
  patrolStart: string;
  patrolEnd: string;
  isPatrolledToday: boolean;
  status: string;
  hazard: number | string;
}

function shapeBeachDetail(data: unknown): BeachDetailResponse {
  const root = data && typeof data === 'object' && !Array.isArray(data)
    ? (data as Record<string, unknown>)
    : {};
  const beach = root['beach'] && typeof root['beach'] === 'object' && !Array.isArray(root['beach'])
    ? (root['beach'] as Record<string, unknown>)
    : {};

  const attendances =
    typeof beach['attendances'] === 'object' && beach['attendances'] !== null
      ? (beach['attendances'] as Record<string, unknown>)
      : {};
  const attKeys = Object.keys(attendances);
  let latestAttendance: { date: string; entries: unknown } | null = null;
  if (attKeys.length > 0) {
    const lastKey = attKeys[attKeys.length - 1];
    if (lastKey) {
      const entries = attendances[lastKey];
      if (entries) latestAttendance = { date: lastKey, entries };
    }
  }

  const patrolToday =
    typeof beach['is_patrolled_today'] === 'object' &&
    beach['is_patrolled_today'] !== null &&
    !Array.isArray(beach['is_patrolled_today'])
      ? (beach['is_patrolled_today'] as Record<string, unknown>)
      : {};

  const patrolVal = beach['patrol'];
  const hazardVal = beach['hazard'];
  return {
    weather:
      typeof beach['weather'] === 'object' && beach['weather'] !== null
        ? (beach['weather'] as Record<string, unknown>)
        : {},
    currentTide: beach['currentTide'] ?? null,
    currentUV: beach['currentUV'] ?? null,
    latestAttendance,
    todays_marine_warnings: Array.isArray(beach['todays_marine_warnings'])
      ? (beach['todays_marine_warnings'] as unknown[])
      : [],
    patrol: typeof patrolVal === 'number' || typeof patrolVal === 'string' ? patrolVal : 0,
    patrolStart: stringField(patrolToday['start']),
    patrolEnd: stringField(patrolToday['end']),
    isPatrolledToday: Boolean(patrolToday['flag']),
    status: stringField(beach['status']) || 'Unknown',
    hazard: typeof hazardVal === 'number' || typeof hazardVal === 'string' ? hazardVal : 0,
  };
}

function stringField(v: unknown): string {
  return typeof v === 'string' ? v : '';
}
