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

export const endeavourRouter = new Hono();

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

// TODO(endeavour-raw-passthrough): Python exposes /api/endeavour/{current,
// future}/{raw,all} which forward to Supabase per request. Punted from W4
// because the frontend doesn't consume them; only debug tooling does.
// When ported, those handlers should call directly into endeavour.ts's
// callSupabase() helper rather than read from LiveStore.
