/**
 * Live ADS-B aircraft endpoint.
 *
 *   GET /api/adsb/aircraft — merged aircraft positions over NSW from
 *                            adsb.lol / adsb.fi / airplanes.live,
 *                            deduped by ICAO hex. Live-only snapshot;
 *                            deliberately NOT in CACHEABLE_PATHS — a
 *                            30 s max-age would defeat the frontend's
 *                            15 s refresh.
 */
import { Hono } from 'hono';
import { adsbSnapshot } from '../sources/adsb.js';

export const adsbRouter = new Hono();

adsbRouter.get('/api/adsb/aircraft', (c) => c.json(adsbSnapshot()));
