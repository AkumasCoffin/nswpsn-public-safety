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

export const waEmergencyRouter = new Hono();

waEmergencyRouter.get('/api/wa-emergency/incidents', (c) => c.json(waIncidentsSnapshot()));
waEmergencyRouter.get('/api/wa-emergency/warnings', (c) => c.json(waWarningsSnapshot()));
