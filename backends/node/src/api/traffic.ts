/**
 * Live Traffic NSW endpoints.
 *
 *   GET /api/traffic/incidents        — parsed FeatureCollection
 *   GET /api/traffic/incidents/raw    — upstream JSON pass-through
 *   GET /api/traffic/roadwork         — parsed
 *   GET /api/traffic/roadwork/raw     — pass-through
 *   GET /api/traffic/flood            — parsed
 *   GET /api/traffic/flood/raw        — pass-through
 *   GET /api/traffic/fire             — parsed
 *   GET /api/traffic/fire/raw         — pass-through
 *   GET /api/traffic/majorevent       — parsed
 *   GET /api/traffic/majorevent/raw   — pass-through
 *   GET /api/traffic/cameras          — camera FeatureCollection
 *
 * Mirrors python's routes at external_api_proxy.py:7239+. The /raw
 * variants hit upstream every time (Python caches them at HTTP TTL
 * level); the parsed variants serve out of LiveStore.
 */
import { Hono } from 'hono';
import {
  trafficCamerasSnapshot,
  trafficHazardSnapshot,
  fetchHazardRaw,
} from '../sources/traffic.js';
import { log } from '../lib/log.js';

export const trafficRouter = new Hono();

const HAZARDS: Array<{ path: string; storeKey: string; rawEndpoint: string }> = [
  { path: 'incidents', storeKey: 'traffic_incidents', rawEndpoint: 'incident' },
  { path: 'roadwork', storeKey: 'traffic_roadwork', rawEndpoint: 'roadwork' },
  { path: 'flood', storeKey: 'traffic_flood', rawEndpoint: 'flood' },
  { path: 'fire', storeKey: 'traffic_fire', rawEndpoint: 'fire' },
  { path: 'majorevent', storeKey: 'traffic_majorevent', rawEndpoint: 'majorevent' },
];

for (const h of HAZARDS) {
  trafficRouter.get(`/api/traffic/${h.path}`, (c) =>
    c.json(trafficHazardSnapshot(h.storeKey)),
  );
  trafficRouter.get(`/api/traffic/${h.path}/raw`, async (c) => {
    try {
      const raw = await fetchHazardRaw(h.rawEndpoint);
      return c.json(raw);
    } catch (err) {
      log.warn(
        { err: (err as Error).message, kind: h.path },
        'traffic raw fetch failed',
      );
      return c.json({ type: 'FeatureCollection', features: [] });
    }
  });
}

trafficRouter.get('/api/traffic/cameras', (c) =>
  c.json(trafficCamerasSnapshot()),
);
