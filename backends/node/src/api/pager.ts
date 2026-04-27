/**
 * GET /api/pager/hits — pager hits as a GeoJSON FeatureCollection.
 *
 * Mirrors python's `/api/pager/hits` route at external_api_proxy.py:12449.
 *
 * The Python implementation queries Postgres directly and supports
 * `?capcode=` and `?incident_id=` filters. The Node port reads the live
 * snapshot the poller fills (max 100 messages from upstream) and
 * supports the two filters the frontend actually uses today: `?hours=`
 * and `?limit=`. Capcode + incident_id filters are TODO until the
 * archive layer (W4+) gives us deeper history to query against.
 */
import { Hono } from 'hono';
import { pagerSnapshot, type PagerMessage } from '../sources/pager.js';

export const pagerRouter = new Hono();

interface PagerFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    id: string;
    pager_msg_id: string | number;
    incident_id: string;
    capcode: string;
    alias: string;
    agency: string;
    message: string;
    incident_time: string | null;
    timestamp: number | null;
    is_live: boolean;
    lat: number;
    lon: number;
  };
}

pagerRouter.get('/api/pager/hits', (c) => {
  const url = new URL(c.req.url);
  const hoursParam = url.searchParams.get('hours');
  const limitParam = url.searchParams.get('limit');

  let hours = Number.parseInt(hoursParam ?? '24', 10);
  if (!Number.isFinite(hours) || hours <= 0) hours = 24;
  hours = Math.min(hours, 168); // Match python's 7-day cap.

  let limit = Number.parseInt(limitParam ?? '500', 10);
  if (!Number.isFinite(limit) || limit <= 0) limit = 500;
  limit = Math.min(limit, 2000);

  const cutoff = Math.floor(Date.now() / 1000) - hours * 3600;
  const snap = pagerSnapshot();
  const filtered: PagerMessage[] = [];
  for (const m of snap.messages) {
    if (m.timestamp !== null && m.timestamp < cutoff) continue;
    filtered.push(m);
    if (filtered.length >= limit) break;
  }

  const features: PagerFeature[] = filtered.map((m) => ({
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [m.lon, m.lat] },
    properties: {
      // The python version sets `id` to the database `source_id`; for
      // the proxy we don't have a DB row so we re-use the upstream
      // pager message id. Same uniqueness guarantee from the frontend's
      // perspective (it's how python derives source_id anyway).
      id: String(m.id),
      pager_msg_id: m.id,
      incident_id: m.incident_id,
      capcode: m.capcode,
      alias: m.alias,
      agency: m.agency,
      message: m.message,
      incident_time: m.incident_time,
      timestamp: m.timestamp,
      is_live: true,
      lat: m.lat,
      lon: m.lon,
    },
  }));

  return c.json({
    type: 'FeatureCollection',
    features,
    count: features.length,
    hours,
  });
});
