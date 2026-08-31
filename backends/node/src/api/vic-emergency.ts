/**
 * GET /api/vic-emergency/events — live Victorian emergency events.
 *
 * One feed covering CFA, SES, DEECA and the EMV warning layer, in the
 * same property shape as /api/rfs/incidents. Each record carries a
 * `layer` of fire / flood / hazard so the map can route it — Victoria
 * publishes fires, floods and hazmat through one endpoint.
 */
import { Hono } from 'hono';
import { vicSnapshot } from '../sources/vicEmergency.js';

export const vicEmergencyRouter = new Hono();

vicEmergencyRouter.get('/api/vic-emergency/events', (c) => c.json(vicSnapshot()));
