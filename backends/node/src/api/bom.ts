/**
 * GET /api/bom/warnings — combined NSW BOM warnings (land + marine).
 *
 * Mirrors python's `/api/bom/warnings` route at
 * external_api_proxy.py:5602. Reads the LiveStore snapshot the poller
 * fills; if nothing's there yet, returns the same empty shape Python
 * returns on cache miss + fetch failure.
 *
 * Also served at /api/v1/ (server.ts mirrors it), where the body is
 * wrapped in `data` (api/v1/respond.ts).
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { describeRoute } from 'hono-openapi';
import { bomSnapshot, type BomSnapshot } from '../sources/bom.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { ok } from './v1/respond.js';
import { assertShape, envelope, GATED_ERRORS, okJson, type SameShape } from './v1/schemas.js';

export const bomRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_WARNINGS = scoped({ source: 'bom', type: 'warnings', window: 'live' });

// What /api/v1/ documents (api/v1/openapi.ts).
const BomWarnings = z.object({
  warnings: z.array(
    z.object({
      title: z.string(),
      type: z.string(),
      category: z.enum(['land', 'marine']),
      warningType: z.string().describe('The kind of warning, from the title: Flood, Severe Weather, Wind, …'),
      severity: z.enum(['severe', 'warning', 'watch', 'advice', 'info']).describe('Read from the title'),
      description: z.string(),
      area: z.string(),
      issued: z.string().describe('As the Bureau writes it'),
      expiry: z.string().describe('As the Bureau writes it'),
      link: z.string(),
      source: z.literal('bom'),
    }),
  ),
  count: z.number().int(),
  counts: z.object({ land: z.number().int(), marine: z.number().int() }),
}).meta({ id: 'BomWarnings' });
assertShape<SameShape<z.infer<typeof BomWarnings>, BomSnapshot>>();

bomRouter.get(
  '/api/bom/warnings',
  SCOPE_WARNINGS,
  describeRoute({
    operationId: 'getBomWarnings',
    tags: ['Warnings'],
    summary: 'Bureau of Meteorology warnings for NSW',
    description: 'Current Bureau of Meteorology land and marine warnings for NSW.',
    responses: { ...okJson(envelope(BomWarnings), 'Current warnings, with counts'), ...GATED_ERRORS },
  }),
  (c) => ok(c, bomSnapshot()),
);
