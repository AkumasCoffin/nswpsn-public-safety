/**
 * GET /api/bom/warnings — combined NSW BOM warnings (land + marine).
 *
 * Mirrors python's `/api/bom/warnings` route at
 * external_api_proxy.py:5602. Reads the LiveStore snapshot the poller
 * fills; if nothing's there yet, returns the same empty shape Python
 * returns on cache miss + fetch failure.
 */
import { Hono } from 'hono';
import { bomSnapshot } from '../sources/bom.js';
import { scoped } from '../services/auth/apiKeyScopes.js';

export const bomRouter = new Hono();

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_WARNINGS = scoped({ source: 'bom', type: 'warnings', window: 'live' });

bomRouter.get('/api/bom/warnings', SCOPE_WARNINGS, (c) => c.json(bomSnapshot()));
