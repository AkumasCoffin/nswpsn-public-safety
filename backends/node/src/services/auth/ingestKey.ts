/**
 * X-Ingest-Key auth middleware for /api/waze/ingest.
 *
 * Separate from the main API-key middleware because it has a separate
 * shared secret (WAZE_INGEST_KEY) and a separate consumer (the
 * Tampermonkey userscript). Keeps the userscript from needing the
 * full backend NSWPSN_API_KEY.
 *
 * Mirrors python external_api_proxy.py around line 9620 area:
 *   supplied = request.headers.get('X-Ingest-Key', '')
 *   if not WAZE_INGEST_KEY or supplied != WAZE_INGEST_KEY:
 *       return jsonify({'error': 'unauthorized'}), 401
 */
import type { MiddlewareHandler } from 'hono';
import { config } from '../../config.js';

export const requireIngestKey: MiddlewareHandler = async (c, next) => {
  const expected = config.WAZE_INGEST_KEY;
  if (!expected) {
    // No key configured server-side — reject everything. Avoids
    // accidentally accepting unauthenticated POSTs in dev.
    return c.json({ error: 'ingest disabled' }, 403);
  }
  const supplied = c.req.header('X-Ingest-Key') ?? '';
  if (supplied !== expected) {
    return c.json({ error: 'unauthorized' }, 401);
  }
  await next();
};
