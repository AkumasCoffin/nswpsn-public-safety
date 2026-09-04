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
  '/api/data/history/filters',
  '/api/status',
  // The cameras collection endpoint itself is public; sub-paths are
  // covered by the '/api/centralwatch/cameras/' prefix. Listed here as
  // an exact match so the prefix doesn't over-match sibling routes like
  // '/api/centralwatch/cameras-admin'.
  '/api/centralwatch/cameras',
  // Agency reference tables — replaces the public static agency-extended.json,
  // so the read stays public (the agency page needs no login). Write/edit
  // endpoints under /api/agency/ are gated per-handler, NOT listed here.
  '/api/agency/extended',
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
  // Feeder-node call relay. The node agent authenticates with its own
  // feeder token (X-Node-Token/X-Node-Install), verified inside the
  // handler — NOT the site NSWPSN_API_KEY — so it must skip this gate.
  '/api/node-ingest/',
  // Scanner feed: a third-party rdio DOWNSTREAM cannot send custom headers, so
  // it authenticates with its key as a form field, verified in the handler.
  '/api/scanner-ingest/',
  // Node self-update manifest. Same story: the node authenticates with its
  // feeder token (X-Node-Token), verified inside the handler.
  '/api/node-updates/',
  // Per-agency reference tables (/api/agency/extended/:slug) — public read,
  // matching the exact /api/agency/extended above.
  '/api/agency/extended/',
  // Wire link-unfurl metadata. Consumed by the Cloudflare Worker on the /wire
  // route to inject per-post Open Graph tags for social crawlers, which can't
  // send an API key. Only exposes OG fields of already-published posts.
  '/api/wire/og/',
  // Whisper status + drain. The PC's idle watcher is a headless script with
  // no user session and no reason to hold the site key; both routes verify
  // WHISPER_ADMIN_TOKEN (or, for status, a staff role) inside the handler.
  //
  // NOT /api/whisper/v1/ — the transcription path deliberately KEEPS this
  // gate. rdio has an API key field of its own to put NSWPSN_API_KEY in, and
  // an endpoint that spends GPU time should not be the one open route.
  '/api/whisper/status',
  '/api/whisper/drain',
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

  // A request already authenticated as a Supabase user (optionalSupabaseJwt
  // verified the JWT and set userId) passes the key gate — a logged-in user
  // is at least as trusted as the public NSWPSN_API_KEY. Privileged routes
  // still enforce their own role check (requireRole) on top of this.
  if (c.get('userId')) {
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
