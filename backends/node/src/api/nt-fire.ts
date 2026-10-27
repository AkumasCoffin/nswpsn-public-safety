/**
 * GET /api/nt-fire/incidents — live NT Fire & Rescue incidents.
 *
 * Serves out of LiveStore (filled by sources/ntFire.ts) in the same
 * property shape as /api/rfs/incidents, so the map renders NSW RFS and
 * NTFRS through one path.
 *
 * Closed incidents are NOT returned here. They are still archived — the
 * logs page shows how each one progressed — but a closed job does not
 * belong on a live map. `?all=1` returns them too, for debugging.
 *
 * Also served at /api/v1/ (server.ts mirrors it). The feed asks not to be
 * re-published (sources/ntFire.ts); the owner chose to carry it, so its
 * description credits NTFRS and asks API users to do the same.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { ntFireSnapshot, ntFireActiveSnapshot, type NtFireSnapshot } from '../sources/ntFire.js';
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
  LonLat,
  okJson,
  queryValidator,
  Rings,
  type SameShape,
} from './v1/schemas.js';

export const ntFireRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'ntfrs', type: 'incidents', window: 'live' });

// What /api/v1/ documents (api/v1/openapi.ts).
const NtFireIncidents = featureCollection(
  feature(
    z.union([
      z.object({ type: z.literal('Point'), coordinates: LonLat }),
      z.object({ type: z.literal('Polygon'), coordinates: Rings }),
    ]),
    incidentProperties({
      layer: Layer(['fire', 'hazard']),
      polygons: Rings.describe('The fire ground, as polygon outer rings of [longitude, latitude]'),
      ntCategory: z.string().describe("The feed's own category"),
      currentSituation: z.string().describe('Bushfires NT records only; empty otherwise'),
      risks: z.string(),
      whatToDo: z.string(),
      adviceToPublic: z.string(),
      is_active: z.boolean().describe('False once the incident is closed (only with all=1)'),
      agency: z.literal('NTFRS'),
      source: z.literal('nt_fire'),
    }),
  ),
).meta({ id: 'NtFireIncidents' });
assertShape<SameShape<z.infer<typeof NtFireIncidents>, NtFireSnapshot>>();

const Query = z.object({ all: flag('1 includes closed incidents as well') });

ntFireRouter.get(
  '/api/nt-fire/incidents',
  SCOPE_INCIDENTS,
  describeRoute({
    operationId: 'getNtFireIncidents',
    tags: ['Incidents'],
    summary: 'NT Fire and Rescue incidents',
    description:
      'NT Fire and Rescue Service incidents from the NT Police, Fire and Emergency Services incident map feed, including Bushfires NT warnings. Closed incidents are left out unless all=1. The data is NTFRS’s: credit NT Fire and Rescue Service wherever you show it.',
    responses: { ...okJson(NtFireIncidents, 'A GeoJSON FeatureCollection of incidents'), ...GATED_ERRORS },
  }),
  queryValidator(Query),
  (c) => geo(c, c.req.query('all') === '1' ? ntFireSnapshot() : ntFireActiveSnapshot()),
);
