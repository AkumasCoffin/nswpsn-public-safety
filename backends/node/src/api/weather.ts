/**
 * Weather endpoints.
 *
 *   GET /api/weather/current — Open-Meteo current conditions for ~100
 *                              NSW locations as a FeatureCollection
 *   GET /api/weather/radar   — RainViewer tile metadata pass-through
 *
 * Mirrors python's routes at external_api_proxy.py:6999 + 6847.
 */
import { Hono } from 'hono';
import {
  weatherCurrentSnapshot,
  weatherRadarSnapshot,
} from '../sources/weather.js';

export const weatherRouter = new Hono();

weatherRouter.get('/api/weather/current', (c) =>
  c.json(weatherCurrentSnapshot()),
);

weatherRouter.get('/api/weather/radar', (c) => c.json(weatherRadarSnapshot()));
