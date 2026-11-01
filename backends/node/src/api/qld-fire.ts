/**
 * GET /api/qld-fire/incidents — live QFD incidents (ESCAD)
 * GET /api/qld-fire/warnings  — QFD public warnings (OCS)
 *
 * Both serve out of LiveStore in the same property shape as
 * /api/rfs/incidents, so the map renders every agency on the Fires
 * layer through one path.
 *
 * Both are this site's routes, for the map and the Discord bot, and neither
 * is on /api/v1/: API keys read QFD's incidents and warnings alongside every
 * other fire service at /api/v1/fires (api/fires.ts). Neither declares a
 * scope, so no scoped key reaches them.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { qldIncidentsSnapshot, qldWarningsSnapshot, type QldFireSnapshot } from '../sources/qldFire.js';
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

export const qldFireRouter = new Hono();

// The answers' shape. Incidents and warnings share one record shape; the
// fields that belong to only one are empty or null in the other.
const QldFire = featureCollection(
  feature(
    PointGeometry,
    incidentProperties({
      polygons: Rings.optional().describe('Warning areas, as polygon outer rings of [longitude, latitude]; warnings only'),
      layer: Layer(['fire', 'hazard']),
      incidentRef: z.string().describe('Warnings: the incident the warning belongs to, when QFD says'),
      warningText: z.string().describe('Warnings: the public advice text'),
      header: z.string(),
      impacts: z.string(),
      shouldDo: z.string(),
      leaveSafely: z.string(),
      furtherInformation: z.string(),
      vehiclesAssigned: z.number().nullable().describe('Incidents: appliances assigned'),
      vehiclesOnRoute: z.number().nullable(),
      vehiclesOnScene: z.number().nullable(),
      is_active: z.boolean(),
      agency: z.literal('QFD'),
      source: z.enum(['qld_fire', 'qld_warning']),
    }),
  ),
).meta({ id: 'QldFireRecords' });
assertShape<SameShape<z.infer<typeof QldFire>, QldFireSnapshot>>();

qldFireRouter.get(
  '/api/qld-fire/incidents',
  describeRoute({
    operationId: 'getQldFireIncidents',
    tags: ['Incidents'],
    summary: 'QLD Fire Department incidents',
    description: 'Current Queensland Fire Department incidents.',
    responses: { ...okJson(QldFire, 'A GeoJSON FeatureCollection of incidents'), ...GATED_ERRORS },
  }),
  (c) => geo(c, qldIncidentsSnapshot()),
);

qldFireRouter.get(
  '/api/qld-fire/warnings',
  describeRoute({
    operationId: 'getQldFireWarnings',
    tags: ['Fires'],
    summary: 'QLD Fire Department warnings',
    description: 'Current Queensland Fire Department public warnings with their warning areas.',
    responses: { ...okJson(QldFire, 'A GeoJSON FeatureCollection of warnings'), ...GATED_ERRORS },
  }),
  (c) => geo(c, qldWarningsSnapshot()),
);
