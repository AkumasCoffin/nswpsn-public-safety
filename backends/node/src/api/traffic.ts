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
import { fetchJson } from '../sources/shared/http.js';
import { log } from '../lib/log.js';

export const trafficRouter = new Hono();

interface AllFeedsItem {
  eventType?: string;
  eventCategory?: string;
  [k: string]: unknown;
}

interface LgaItem {
  [k: string]: unknown;
}

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

// /api/traffic/lga-incidents — regional LGA incident feed. Mirrors python
// line 5415-5432. Hits upstream on every request (Python wraps in @cached
// at the HTTP layer; for now we fetch each call — we can add a TTL cache
// later if upstream complains). Response is a FeatureCollection with
// minimal parsing; the upstream is already JSON-shaped per-incident.
trafficRouter.get('/api/traffic/lga-incidents', async (c) => {
  try {
    const data = await fetchJson<unknown>(
      'https://www.livetraffic.com/traffic/hazards/regional/lga-incidents.json',
      { timeoutMs: 15_000, headers: { 'User-Agent': 'Mozilla/5.0' } },
    );
    const items: LgaItem[] = Array.isArray(data)
      ? (data as LgaItem[])
      : Array.isArray((data as { features?: LgaItem[] })?.features)
        ? ((data as { features: LgaItem[] }).features)
        : [];
    return c.json({ type: 'FeatureCollection', features: items });
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'traffic lga-incidents fetch failed',
    );
    return c.json({ type: 'FeatureCollection', features: [] });
  }
});

// /api/traffic/all-feeds — pass-through with eventType grouping.
// Mirrors python line 9132-9162.
trafficRouter.get('/api/traffic/all-feeds', async (c) => {
  try {
    const data = await fetchJson<AllFeedsItem[]>(
      'https://www.livetraffic.com/datajson/all-feeds-web.json',
      { timeoutMs: 15_000, headers: { 'User-Agent': 'Mozilla/5.0' } },
    );
    if (!Array.isArray(data)) {
      return c.json({ raw: [], grouped: {}, eventTypes: [], totalCount: 0 });
    }
    const grouped: Record<string, AllFeedsItem[]> = {};
    for (const item of data) {
      const key = item.eventType ?? item.eventCategory ?? 'unknown';
      if (!grouped[key]) grouped[key] = [];
      grouped[key].push(item);
    }
    return c.json({
      raw: data,
      grouped,
      eventTypes: Object.keys(grouped),
      totalCount: data.length,
    });
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'traffic all-feeds fetch failed');
    return c.json({
      error: (err as Error).message,
      raw: [],
      grouped: {},
      totalCount: 0,
    });
  }
});
