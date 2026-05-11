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

export const bomRouter = new Hono();

bomRouter.get('/api/bom/warnings', (c) => c.json(bomSnapshot()));
