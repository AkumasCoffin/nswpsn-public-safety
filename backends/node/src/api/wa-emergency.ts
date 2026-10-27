/**
 * GET /api/wa-emergency/incidents — live DFES incidents
 * GET /api/wa-emergency/warnings  — DFES public warnings
 *
 * Both serve out of LiveStore in the same property shape as
 * /api/rfs/incidents, so the map renders every agency on the Fires
 * layer through one path.
 */
import { Hono } from 'hono';
import { waIncidentsSnapshot, waWarningsSnapshot } from '../sources/waEmergency.js';
import { scoped } from '../services/auth/apiKeyScopes.js';

export const waEmergencyRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_INCIDENTS = scoped({ source: 'dfes', type: 'incidents', window: 'live' });
const SCOPE_WARNINGS = scoped({ source: 'dfes', type: 'warnings', window: 'live' });

waEmergencyRouter.get('/api/wa-emergency/incidents', SCOPE_INCIDENTS, (c) => c.json(waIncidentsSnapshot()));
waEmergencyRouter.get('/api/wa-emergency/warnings', SCOPE_WARNINGS, (c) => c.json(waWarningsSnapshot()));
