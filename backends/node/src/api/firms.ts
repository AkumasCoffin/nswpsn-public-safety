/**
 * NASA FIRMS fire-hotspot endpoint.
 *
 *   GET /api/firms/hotspots — last-24h hotspot detections over Australia as a
 *                              GeoJSON FeatureCollection. Each feature is
 *                              a Polygon representing the satellite pixel
 *                              footprint (scan × track in km).
 *
 * Also served at /api/v1/ (server.ts mirrors it).
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { firmsSnapshot, type FirmsSnapshot } from '../sources/firms.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { geo } from './v1/respond.js';
import {
  assertShape,
  feature,
  featureCollection,
  GATED_ERRORS,
  LonLat,
  okJson,
  type SameShape,
} from './v1/schemas.js';

export const firmsRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_HAZARDS = scoped({ source: 'firms', type: 'hazards', window: 'live' });

// What /api/v1/ documents (api/v1/openapi.ts).
const FirmsHotspots = featureCollection(
  feature(
    z.object({
      type: z.literal('Polygon'),
      coordinates: z.tuple([z.array(LonLat)]).describe('The satellite pixel footprint, one ring of [longitude, latitude]'),
    }),
    z.object({
      latitude: z.number(),
      longitude: z.number(),
      brightness: z.number().nullable().describe('Brightness temperature, kelvin'),
      scan_km: z.number().nullable().describe('Pixel size along the scan, km'),
      track_km: z.number().nullable().describe('Pixel size along the track, km'),
      acq_date: z.string().describe('Acquisition date as FIRMS gives it (UTC)'),
      acq_time: z.string().describe('Acquisition time as FIRMS gives it (UTC, HHMM)'),
      acq_datetime: z.string().describe('Acquisition time, ISO 8601 UTC'),
      satellite: z.string(),
      instrument: z.string(),
      confidence: z.string().describe('As FIRMS gives it; for VIIRS l, n or h (low, nominal, high)'),
      version: z.string(),
      bright_t31: z.number().nullable().describe('Second-channel brightness temperature, kelvin'),
      frp: z.number().nullable().describe('Fire radiative power, MW'),
      daynight: z.string().describe('D (day) or N (night)'),
      source: z.string().describe('The FIRMS product, e.g. VIIRS_NOAA20_NRT'),
      satellite_tag: z.string().describe('The platform: S-NPP, NOAA-20 or NOAA-21'),
    }),
  ),
  {
    sources: z.array(z.string()).describe('The FIRMS products queried; source_status says how each fared'),
    source_status: z
      .array(
        z.object({
          source: z.string(),
          satellite: z.string(),
          label: z.string(),
          ok: z.boolean(),
          count: z.number().int(),
          stale: z.boolean().optional().describe("true while serving the product's last good detections through an outage"),
          error: z.string().optional(),
        }),
      )
      .describe('How each product fared on the last fetch'),
    bbox: z.string().describe('The area queried, "west,south,east,north"'),
    fetched_at: z.string().describe('When FIRMS was last fetched, ISO 8601'),
  },
).meta({ id: 'FirmsHotspots' });
assertShape<SameShape<z.infer<typeof FirmsHotspots>, FirmsSnapshot>>();

firmsRouter.get(
  '/api/firms/hotspots',
  SCOPE_HAZARDS,
  describeRoute({
    operationId: 'getFirmsHotspots',
    tags: ['Hazards'],
    summary: 'Satellite fire hotspots',
    description:
      'NASA FIRMS VIIRS hotspot detections over Australia from the last 24 hours, each as its pixel footprint. A hotspot is a heat signature seen from orbit, not a confirmed fire.',
    responses: { ...okJson(FirmsHotspots, 'A GeoJSON FeatureCollection of hotspots'), ...GATED_ERRORS },
  }),
  (c) => geo(c, firmsSnapshot()),
);
