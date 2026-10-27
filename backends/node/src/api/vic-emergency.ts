/**
 * GET /api/vic-emergency/events — live Victorian emergency events.
 *
 * One feed covering CFA, SES, DEECA and the EMV warning layer, in the
 * same property shape as /api/rfs/incidents. Each record carries a
 * `layer` of fire / flood / hazard so the map can route it — Victoria
 * publishes fires, floods and hazmat through one endpoint.
 *
 * Also served at /api/v1/ (server.ts mirrors it).
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { vicSnapshot, type VicSnapshot } from '../sources/vicEmergency.js';
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

export const vicEmergencyRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'vic', type: 'incidents', window: 'live' });

// What /api/v1/ documents (api/v1/openapi.ts).
const VicEvents = featureCollection(
  feature(
    PointGeometry,
    incidentProperties({
      layer: Layer(['fire', 'flood', 'hazard']),
      agency: z.string().describe("The publishing organisation: CFA, SES, DEECA, EMV or ESTA, else the feed's own name for it"),
      polygons: Rings.describe('Warning areas, as polygon outer rings of [longitude, latitude]'),
      capEvent: z.string().describe('Warnings: the CAP event'),
      capSeverity: z.string(),
      capUrgency: z.string(),
      capCertainty: z.string(),
      capResponseType: z.string(),
      capSender: z.string(),
      text: z.string().describe('Plain-text advice'),
      action: z.string(),
      resources: z.string(),
      sizeFmt: z.string().describe('Size as the feed formats it'),
      url: z.string(),
      is_active: z.boolean(),
      source: z.literal('vic_emergency'),
    }),
  ),
).meta({ id: 'VicEmergencyEvents' });
assertShape<SameShape<z.infer<typeof VicEvents>, VicSnapshot>>();

vicEmergencyRouter.get(
  '/api/vic-emergency/events',
  SCOPE_INCIDENTS,
  describeRoute({
    operationId: 'getVicEmergencyEvents',
    tags: ['Incidents'],
    summary: 'VIC Emergency incidents and warnings',
    description:
      'Current incidents and warnings from the emergency.vic.gov.au feed (CFA, SES, DEECA, EMV and others). Each record says which map layer it belongs on — fire, flood or hazard.',
    responses: { ...okJson(VicEvents, 'A GeoJSON FeatureCollection of events'), ...GATED_ERRORS },
  }),
  (c) => geo(c, vicSnapshot()),
);
