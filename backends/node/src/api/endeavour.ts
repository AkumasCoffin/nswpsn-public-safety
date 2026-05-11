/**
 * Endeavour Energy outage routes.
 *
 * Reads from LiveStore — the poller (src/sources/endeavour.ts) keeps
 * three keys fresh: endeavour_current, endeavour_planned (future
 * maintenance), endeavour_maintenance (current maintenance).
 *
 * Response shapes mirror Python (external_api_proxy.py):
 *   - /api/endeavour/current         → array (line 6799-6816)
 *   - /api/endeavour/maintenance     → array (line 5308-5325)
 *   - /api/endeavour/future          → array (line 5328-5345)
 *   - /api/endeavour/planned         → array of planned + future merged
 *                                       (line 5348-5364)
 *
 * `/raw` and `/all` variants from Python directly hit Supabase per
 * request and skip the normalisation step. We deliberately punt those
 * to the legacy backend for now; clients use the normalised endpoints.
 * See TODO at the bottom of this file.
 *
 * Empty LiveStore returns `[]` rather than 500 — same as Python's
 * `cached_data is not None` branch returning the live-fetch fallback,
 * which itself returns `[]` on error.
 */
import { Hono } from 'hono';
import { liveStore } from '../store/live.js';
import type { EndeavourOutage } from '../sources/endeavour.js';
import { callSupabase } from '../sources/endeavour.js';
import { log } from '../lib/log.js';

export const endeavourRouter = new Hono();

interface SupabaseAreaRaw {
  outage_type?: string;
  [k: string]: unknown;
}

async function fetchAreas(): Promise<SupabaseAreaRaw[]> {
  const data = await callSupabase('/rpc/get_outage_areas_fast', {
    method: 'POST',
    body: {},
  });
  return Array.isArray(data) ? (data as SupabaseAreaRaw[]) : [];
}

function readArray(key: string): EndeavourOutage[] {
  const data = liveStore.getData<EndeavourOutage[]>(key);
  return Array.isArray(data) ? data : [];
}

endeavourRouter.get('/api/endeavour/current', (c) =>
  c.json(readArray('endeavour_current')),
);

endeavourRouter.get('/api/endeavour/maintenance', (c) =>
  c.json(readArray('endeavour_maintenance')),
);

// Python `/api/endeavour/future` returns the future_maintenance bucket,
// which we store under the LiveStore key `endeavour_planned` (matching
// Python's source-name registry). The route name stays `future` so the
// frontend keeps working.
endeavourRouter.get('/api/endeavour/future', (c) =>
  c.json(readArray('endeavour_planned')),
);

// `/planned` is the bot-canonical alias: current maintenance + future
// scheduled, flat list. Mirrors Python line 5348-5364.
endeavourRouter.get('/api/endeavour/planned', (c) => {
  const items: EndeavourOutage[] = [];
  for (const k of ['endeavour_maintenance', 'endeavour_planned'] as const) {
    items.push(...readArray(k));
  }
  return c.json(items);
});

// Raw Supabase passthroughs. Python (lines 5367, 5383, 6819, 6835) forwards
// directly to /rpc/get_outage_areas_fast and filters by outage_type. We do
// the same — no LiveStore involvement — for clients that need the un-
// normalised area record. Failures degrade to [] so a Supabase blip
// doesn't 500 the route.
endeavourRouter.get('/api/endeavour/current/raw', async (c) => {
  try {
    const areas = await fetchAreas();
    return c.json(
      areas.filter((a) => (a.outage_type ?? '').toUpperCase() !== 'PLANNED'),
    );
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'endeavour current/raw failed');
    return c.json([]);
  }
});

endeavourRouter.get('/api/endeavour/current/all', async (c) => {
  try {
    const areas = await fetchAreas();
    return c.json(
      areas.filter((a) => (a.outage_type ?? '').toUpperCase() !== 'PLANNED'),
    );
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'endeavour current/all failed');
    return c.json([]);
  }
});

endeavourRouter.get('/api/endeavour/future/raw', async (c) => {
  try {
    const areas = await fetchAreas();
    return c.json(
      areas.filter((a) => (a.outage_type ?? '').toUpperCase() === 'PLANNED'),
    );
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'endeavour future/raw failed');
    return c.json([]);
  }
});

endeavourRouter.get('/api/endeavour/future/all', async (c) => {
  try {
    const areas = await fetchAreas();
    return c.json(
      areas.filter((a) => (a.outage_type ?? '').toUpperCase() === 'PLANNED'),
    );
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'endeavour future/all failed');
    return c.json([]);
  }
});

// /api/endeavour/postcodes — distinct postcodes pulled from /outage-points.
// Python caches for 1h via @cached(ttl=3600). We add a tiny module-local
// cache here matching that.
let postcodesCache: { data: { postcodes: string[]; count: number }; expiresAt: number } | null = null;
const POSTCODE_TTL_MS = 60 * 60_000;

interface OutagePointRow { postcode?: string | null }

endeavourRouter.get('/api/endeavour/postcodes', async (c) => {
  const now = Date.now();
  if (postcodesCache && now < postcodesCache.expiresAt) {
    return c.json(postcodesCache.data);
  }
  try {
    const data = await callSupabase('/outage-points', {
      method: 'GET',
      query: { select: 'postcode', limit: '5000' },
    });
    if (!Array.isArray(data)) return c.json({ postcodes: [], count: 0 });
    const seen = new Set<string>();
    for (const row of data as OutagePointRow[]) {
      const pc = row.postcode;
      if (typeof pc === 'string' && pc.length > 0) seen.add(pc);
    }
    const sorted = Array.from(seen).sort();
    const payload = { postcodes: sorted, count: sorted.length };
    postcodesCache = { data: payload, expiresAt: now + POSTCODE_TTL_MS };
    return c.json(payload);
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'endeavour postcodes failed');
    return c.json({ postcodes: [], count: 0 });
  }
});

export function _resetEndeavourPostcodesCacheForTests(): void {
  postcodesCache = null;
}
