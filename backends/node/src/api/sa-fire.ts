/**
 * GET /api/sa-fire/cfs — SA Country Fire Service incidents
 * GET /api/sa-fire/mfs — SA Metropolitan Fire Service incidents
 *
 * Both serve out of LiveStore in the same property shape as
 * /api/rfs/incidents, so the map renders every agency on the Fires
 * layer through one path.
 *
 * COMPLETE incidents are NOT returned here. SA keeps finished jobs in
 * the feed after they close — a sampled sa_mfs poll had 2 of 7 already
 * COMPLETE — and serving them put finished work on the map as live pins.
 * They are still archived, the way NT's closed records are; `?all=1`
 * returns them too, for debugging.
 *
 * Both are also served at /api/v1/ (server.ts mirrors them).
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import {
  saCfsSnapshot,
  saMfsSnapshot,
  saCfsActiveSnapshot,
  saMfsActiveSnapshot,
  type SaSnapshot,
} from '../sources/saFire.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { geo } from './v1/respond.js';
import {
  assertShape,
  feature,
  featureCollection,
  flag,
  GATED_ERRORS,
  incidentProperties,
  Layer,
  okJson,
  PointGeometry,
  queryValidator,
  type SameShape,
} from './v1/schemas.js';

export const saFireRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'safire', type: 'incidents', window: 'live' });

// What /api/v1/ documents (api/v1/openapi.ts).
const SaIncidents = featureCollection(
  feature(
    PointGeometry,
    incidentProperties({
      layer: Layer(['fire', 'flood', 'hazard']),
      district: z.string().describe('Fire ban district'),
      resources: z.number().nullable().describe('CFS: appliances turned out'),
      aircraft: z.number().nullable().describe('CFS: aircraft turned out'),
      resourcesText: z.string().describe('The two counts as one line, e.g. "3 appliances, 1 aircraft"; empty when none'),
      text: z.string().describe('CFS: the public message, when one has been written'),
      url: z.string(),
      is_active: z.boolean().describe('False once the incident is complete (only with all=1)'),
      agency: z.enum(['SACFS', 'SAMFS']),
      source: z.enum(['sa_cfs', 'sa_mfs']),
    }),
  ),
).meta({ id: 'SaFireIncidents' });
assertShape<SameShape<z.infer<typeof SaIncidents>, SaSnapshot>>();

const Query = z.object({ all: flag('1 includes completed incidents as well') });

saFireRouter.get(
  '/api/sa-fire/cfs',
  SCOPE_INCIDENTS,
  describeRoute({
    operationId: 'getSaCfsIncidents',
    tags: ['Incidents'],
    summary: 'SA Country Fire Service incidents',
    description: 'Current SA Country Fire Service incidents. Completed incidents are left out unless all=1.',
    responses: { ...okJson(SaIncidents, 'A GeoJSON FeatureCollection of incidents'), ...GATED_ERRORS },
  }),
  queryValidator(Query),
  (c) => geo(c, c.req.query('all') === '1' ? saCfsSnapshot() : saCfsActiveSnapshot()),
);
saFireRouter.get(
  '/api/sa-fire/mfs',
  SCOPE_INCIDENTS,
  describeRoute({
    operationId: 'getSaMfsIncidents',
    tags: ['Incidents'],
    summary: 'SA Metropolitan Fire Service incidents',
    description: 'Current SA Metropolitan Fire Service incidents. Completed incidents are left out unless all=1.',
    responses: { ...okJson(SaIncidents, 'A GeoJSON FeatureCollection of incidents'), ...GATED_ERRORS },
  }),
  queryValidator(Query),
  (c) => geo(c, c.req.query('all') === '1' ? saMfsSnapshot() : saMfsActiveSnapshot()),
);
