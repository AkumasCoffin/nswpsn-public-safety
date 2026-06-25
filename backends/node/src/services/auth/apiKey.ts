/**
 * NSWPSN_API_KEY auth middleware for /api/* routes that aren't public.
 *
 * Mirrors python external_api_proxy.py:5046-5125 (the require_api_key
 * decorator + the @app.before_request global hook). The Python backend
 * accepts the key via three locations, in priority order:
 *   1. Authorization: Bearer <key>
 *   2. X-API-Key: <key>
 *   3. ?api_key=<key> query param
 *
 * The error response shapes are byte-for-byte the python ones — clients
 * that already key off `error: "API key required"` / `Invalid API key`
 * keep working during the strangler-fig migration.
 *
 * OPTIONS preflights bypass auth entirely (matches Python's `if
 * request.method == 'OPTIONS': return '', 200` short-circuit).
 *
 * The PUBLIC_ENDPOINTS / PUBLIC_ENDPOINT_PREFIXES sets are mirrored from
 * python here so the middleware can be mounted on a router that covers
 * a mix of public + private endpoints (e.g. the editor router serves
 * both `POST /api/editor-requests` (public — anyone can submit) and
 * `GET /api/editor-requests` (private — admins only)).
 */
import { timingSafeEqual } from 'node:crypto';
import type { MiddlewareHandler } from 'hono';
import { config } from '../../config.js';

/**
 * Constant-time string comparison. Guards against length mismatch first
 * (timingSafeEqual throws on unequal-length buffers); leaking the length
 * of the secret is not a meaningful side-channel here.
 */
function safeEqual(a: string, b: string): boolean {
  const aBuf = Buffer.from(a, 'utf8');
  const bBuf = Buffer.from(b, 'utf8');
  if (aBuf.length !== bBuf.length) return false;
  return timingSafeEqual(aBuf, bBuf);
}

// Exact-match public endpoints (no API key required). Mirror of python
// external_api_proxy.py:5027-5039.
export const PUBLIC_ENDPOINTS = new Set<string>([
  '/api/health',
  '/',
  '/api/config',
  '/api/heartbeat',
  '/api/editor-requests',
  '/api/waze/ingest',
  '/api/data/history/filters',
  '/api/status',
  // The cameras collection endpoint itself is public; sub-paths are
  // covered by the '/api/centralwatch/cameras/' prefix. Listed here as
  // an exact match so the prefix doesn't over-match sibling routes like
  // '/api/centralwatch/cameras-admin'.
  '/api/centralwatch/cameras',
]);

// Prefix-match public endpoints — mirrors python's PUBLIC_ENDPOINT_PREFIXES
// at external_api_proxy.py:5041-5044.
export const PUBLIC_ENDPOINT_PREFIXES: readonly string[] = [
  '/api/check-editor/',
  '/api/centralwatch/image/',
  '/api/centralwatch/cameras/',
  '/api/dashboard/',
  // Vessel image proxy is loaded via <img src> which can't add the
  // X-API-Key header, so it has to be public (the upstream MT photo
  // is itself public-domain anyway).
  '/api/marinetraffic/vessel-image/',
];

function extractKey(authHeader: string | undefined, xApiKey: string | undefined, qsKey: string | null): string {
  if (authHeader && authHeader.startsWith('Bearer ')) {
    return authHeader.slice(7);
  }
  if (xApiKey) return xApiKey;
  return qsKey ?? '';
}

function isPublic(path: string): boolean {
  if (PUBLIC_ENDPOINTS.has(path)) return true;
  for (const prefix of PUBLIC_ENDPOINT_PREFIXES) {
    if (path.startsWith(prefix)) return true;
  }
  // Mirror python's `if not request.path.startswith('/api/'): return None`
  // — non-/api paths skip auth entirely. We still apply this here so the
  // middleware is safe to mount globally.
  if (!path.startsWith('/api/')) return true;
  return false;
}

/**
 * Hono middleware that enforces NSWPSN_API_KEY on private /api/ routes.
 * OPTIONS preflights, public endpoints, and non-/api paths pass through.
 */
export const requireApiKey: MiddlewareHandler = async (c, next) => {
  if (c.req.method === 'OPTIONS') {
    await next();
    return;
  }

  const url = new URL(c.req.url);
  if (isPublic(url.pathname)) {
    await next();
    return;
  }

  const provided = extractKey(
    c.req.header('Authorization'),
    c.req.header('X-API-Key'),
    url.searchParams.get('api_key'),
  );

  if (!provided) {
    return c.json(
      {
        error: 'API key required',
        message:
          'Provide API key via Authorization: Bearer <key> header or X-API-Key header',
      },
      401,
    );
  }

  if (!safeEqual(provided, config.NSWPSN_API_KEY)) {
    return c.json(
      {
        error: 'Invalid API key',
        message: 'The provided API key is not valid',
      },
      403,
    );
  }

  await next();
};
