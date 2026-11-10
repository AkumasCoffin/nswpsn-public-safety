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
 * Both are this site's routes, for the map and the Discord bot; neither is
 * on /api/v1/. API keys read the CFS and MFS alongside every other fire
 * service at /api/v1/fires (api/fires.ts).
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

// No scope: API keys read the fire services together, through /api/v1/fires (api/fires.ts).

// The answers' shape, for describeRoute() below. The routes are not on
// /api/v1/, so the API description (api/v1/openapi.ts) leaves them out.
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
