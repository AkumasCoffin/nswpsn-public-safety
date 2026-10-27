/**
 * GET /api/act-ambulance/incidents — live ACT Ambulance Service responses
 *                                    as a GeoJSON FeatureCollection.
 *
 * Serves out of LiveStore (filled by the poller in
 * sources/actAmbulance.ts). The upstream ESA feed also carries NSW RFS
 * incidents and ACT fire; both are filtered out there, so this route
 * only ever returns ACT ambulance responses that are still running.
 *
 * Also served at /api/v1/ (server.ts mirrors it).
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { actAmbulanceSnapshot, type ActAmbulanceSnapshot } from '../sources/actAmbulance.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { geo } from './v1/respond.js';
import {
  assertShape,
  feature,
  featureCollection,
  GATED_ERRORS,
  okJson,
  PointGeometry,
  type SameShape,
} from './v1/schemas.js';

export const actAmbulanceRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'actas', type: 'incidents', window: 'live' });

// What /api/v1/ documents (api/v1/openapi.ts).
const ActAmbulanceIncidents = featureCollection(
  feature(
    PointGeometry,
    z.object({
      id: z.string(),
      title: z.string(),
      location_text: z.string(),
      status: z.string(),
      type: z.string(),
      agency: z.string(),
      category: z.string(),
      subcategory: z.string(),
      timestamp: z.number().nullable().describe('Last update (else time of call), unix seconds'),
      time_of_call: z.string().describe('As the ESA feed writes it (ACT local time)'),
      updated: z.string().describe('As the ESA feed writes it (ACT local time)'),
      is_active: z.boolean(),
    }),
  ),
).meta({ id: 'ActAmbulanceIncidents' });
assertShape<SameShape<z.infer<typeof ActAmbulanceIncidents>, ActAmbulanceSnapshot>>();

actAmbulanceRouter.get(
  '/api/act-ambulance/incidents',
  SCOPE_INCIDENTS,
  describeRoute({
    operationId: 'getActAmbulanceIncidents',
    tags: ['Incidents'],
    summary: 'ACT Ambulance Service responses',
    description: 'ACT Ambulance Service responses still running, from the ACT ESA feed.',
    responses: { ...okJson(ActAmbulanceIncidents, 'A GeoJSON FeatureCollection of responses'), ...GATED_ERRORS },
  }),
  (c) => geo(c, actAmbulanceSnapshot()),
);
