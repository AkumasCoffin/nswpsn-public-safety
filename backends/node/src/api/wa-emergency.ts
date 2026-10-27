/**
 * GET /api/wa-emergency/incidents — live DFES incidents
 * GET /api/wa-emergency/warnings  — DFES public warnings
 *
 * Both serve out of LiveStore in the same property shape as
 * /api/rfs/incidents, so the map renders every agency on the Fires
 * layer through one path. Both are also served at /api/v1/ (server.ts
 * mirrors them).
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { waIncidentsSnapshot, waWarningsSnapshot, type WaSnapshot } from '../sources/waEmergency.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { geo } from './v1/respond.js';
import {
  assertShape,
  feature,
  featureCollection,
  GATED_ERRORS,
  incidentProperties,
  Layer,
  okJson,
  PointGeometry,
  Rings,
  type SameShape,
} from './v1/schemas.js';

export const waEmergencyRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'dfes', type: 'incidents', window: 'live' });
const SCOPE_WARNINGS = scoped({ source: 'dfes', type: 'warnings', window: 'live' });

// What /api/v1/ documents (api/v1/openapi.ts). Incidents and warnings share
// one record shape; the fields that belong to only one are empty in the other.
const WaRecords = featureCollection(
  feature(
    PointGeometry,
    incidentProperties({
      layer: Layer(['fire', 'flood', 'hazard']),
      polygons: Rings.describe('Warning areas, as polygon outer rings of [longitude, latitude]'),
      eventRef: z.string().describe('The event the record belongs to; incidents and warnings join on it'),
      incidentRef: z.string().describe("Warnings: the incident's event id"),
      headline: z.string().describe('Warnings: the public advice'),
      alertLine: z.string(),
      whatToDo: z.string(),
      peopleAffected: z.string(),
      action: z.string().describe('Warnings: e.g. "Monitor conditions"'),
      actionType: z.string(),
      capSeverity: z.string(),
      capUrgency: z.string(),
      capCertainty: z.string(),
      cadId: z.string().describe('Incidents only'),
      region: z.string(),
      is_active: z.boolean(),
      agency: z.literal('DFES'),
      source: z.enum(['wa_incident', 'wa_warning']),
    }),
  ),
).meta({ id: 'WaEmergencyRecords' });
assertShape<SameShape<z.infer<typeof WaRecords>, WaSnapshot>>();

waEmergencyRouter.get(
  '/api/wa-emergency/incidents',
  SCOPE_INCIDENTS,
  describeRoute({
    operationId: 'getWaEmergencyIncidents',
    tags: ['Incidents'],
    summary: 'WA DFES incidents',
    description: 'Current WA Department of Fire and Emergency Services incidents.',
    responses: { ...okJson(WaRecords, 'A GeoJSON FeatureCollection of incidents'), ...GATED_ERRORS },
  }),
  (c) => geo(c, waIncidentsSnapshot()),
);
waEmergencyRouter.get(
  '/api/wa-emergency/warnings',
  SCOPE_WARNINGS,
  describeRoute({
    operationId: 'getWaEmergencyWarnings',
    tags: ['Warnings'],
    summary: 'WA DFES warnings',
    description: 'Current WA Department of Fire and Emergency Services public warnings with their warning areas.',
    responses: { ...okJson(WaRecords, 'A GeoJSON FeatureCollection of warnings'), ...GATED_ERRORS },
  }),
  (c) => geo(c, waWarningsSnapshot()),
);
