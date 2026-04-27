/**
 * GET /api/waze/{police,hazards,roadwork,metrics}
 *
 * All read from the in-memory WazeIngestCache (LiveStore-backed). No DB
 * on the request path. Response shapes match the Python backend so the
 * frontend doesn't need to know which backend served the request.
 */
import { Hono } from 'hono';
import { snapshot } from '../store/wazeIngestCache.js';
import {
  isHazardAlert,
  isPoliceAlert,
  isRoadworkAlert,
  parseWazeAlert,
  parseWazeJam,
} from '../services/wazeAlerts.js';

export const wazeRouter = new Hono();

wazeRouter.get('/api/waze/police', (c) => {
  const { alerts } = snapshot();
  const features = alerts
    .filter(isPoliceAlert)
    .map((a) => parseWazeAlert(a, 'Police'))
    .filter((f) => f !== null);
  return c.json({
    type: 'FeatureCollection',
    features,
    count: features.length,
  });
});

wazeRouter.get('/api/waze/roadwork', (c) => {
  const { alerts } = snapshot();
  const features = alerts
    .filter(isRoadworkAlert)
    .map((a) => parseWazeAlert(a, 'Roadwork'))
    .filter((f) => f !== null);
  return c.json({
    type: 'FeatureCollection',
    features,
    count: features.length,
  });
});

wazeRouter.get('/api/waze/hazards', (c) => {
  const { alerts, jams } = snapshot();
  const features = alerts
    .filter(isHazardAlert)
    .map((a) => parseWazeAlert(a, 'Hazard'))
    .filter((f) => f !== null);
  const jamFeatures = jams
    .map((j) => parseWazeJam(j))
    .filter((f) => f !== null);
  return c.json({
    type: 'FeatureCollection',
    features,
    jams: jamFeatures,
    count: features.length,
    jamCount: jamFeatures.length,
  });
});

/**
 * Lightweight metrics endpoint. Mirrors python /api/waze/metrics.
 * Just exposes the LiveStore counters; no rolling block-rate window
 * yet (the userscript is the only ingest path now and there's no
 * "Waze blocked us" failure mode for this backend to track).
 */
wazeRouter.get('/api/waze/metrics', (c) => {
  const s = snapshot();
  return c.json({
    regions_cached: s.regions_cached,
    last_ingest_age_secs: s.last_ingest_age_secs,
    alert_count: s.alerts.length,
    jam_count: s.jams.length,
  });
});
