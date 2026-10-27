/**
 * How the versioned API answers.
 *
 * A route mirrored under /api/v1/ (mirror.ts) runs the same handler on both
 * paths, so the handler answers through these helpers and they shape the body
 * for the path it was called on:
 *
 *   ok(c, data)     /api/v1/: { "data": data }       /api/: data, as it always was
 *   geo(c, fc)      a GeoJSON FeatureCollection, bare on both — GeoJSON
 *                   clients expect the collection at the top level
 *   fail(c, …)      /api/v1/: { "error": { "code", "message", "details"? } }
 *                   /api/: the body that route always sent, byte for byte
 *
 * The error codes are a closed list (ERROR_CODES); docs/api.md says what each
 * means. The credential gate (services/auth/apiKey.ts) uses v1ErrorBody for
 * its refusals on /api/v1/ paths, and the app's not-found and error handlers
 * below do the same, so every v1 error has one shape.
 */
import type { Context, ErrorHandler, NotFoundHandler } from 'hono';
import type { ContentfulStatusCode } from 'hono/utils/http-status';
import { isV1Path } from '../../lib/apiVersion.js';
import { log } from '../../lib/log.js';

export const ERROR_CODES = [
  'validation_failed',
  'unauthorized',
  'forbidden',
  'invalid_api_key',
  'token_origin',
  'token_expired',
  'token_invalid',
  'api_key_browser_use',
  'api_key_path_forbidden',
  'api_key_scope_forbidden',
  'auth_unavailable',
  'rate_limited',
  'not_found',
  'service_unavailable',
  'internal_error',
] as const;
export type ErrorCode = (typeof ERROR_CODES)[number];

export interface V1ErrorBody {
  error: { code: ErrorCode; message: string; details?: unknown };
}

export function v1ErrorBody(code: ErrorCode, message: string, details?: unknown): V1ErrorBody {
  return { error: details === undefined ? { code, message } : { code, message, details } };
}

/** Was this request made on a /api/v1/ path? */
export function isV1(c: Context): boolean {
  return isV1Path(c.req.path);
}

/** A JSON answer: wrapped in `data` on /api/v1/, as it always was on /api/. */
export function ok(c: Context, data: unknown, status: ContentfulStatusCode = 200): Response {
  return c.json(isV1(c) ? { data } : data, status);
}

/** A GeoJSON FeatureCollection — never wrapped, on either path. */
export function geo(c: Context, featureCollection: unknown): Response {
  return c.json(featureCollection);
}

/**
 * An error. On /api/v1/ the shared error shape; on /api/ the route's own old
 * body (`legacy`), which clients of the old path may key off — or, when the
 * route never had a special one, `{ "error": message }`.
 */
export function fail(
  c: Context,
  status: ContentfulStatusCode,
  code: ErrorCode,
  message: string,
  legacy?: Record<string, unknown>,
  details?: unknown,
): Response {
  if (isV1(c)) return c.json(v1ErrorBody(code, message, details), status);
  return c.json(legacy ?? { error: message }, status);
}

function codeForStatus(status: number): ErrorCode {
  if (status === 401) return 'unauthorized';
  if (status === 403) return 'forbidden';
  if (status === 404) return 'not_found';
  if (status === 429) return 'rate_limited';
  if (status === 503) return 'service_unavailable';
  return status < 500 ? 'validation_failed' : 'internal_error';
}

/** No route matched. Hono's own plain-text 404 stays on every other path. */
export const notFound: NotFoundHandler = (c) => {
  if (isV1(c)) return c.json(v1ErrorBody('not_found', 'No endpoint at this path.'), 404);
  return c.text('404 Not Found', 404);
};

/**
 * A handler threw. On /api/v1/ a JSON 500 that says nothing about the cause;
 * elsewhere exactly what Hono did before there was a handler (an HTTPException
 * answers with its own response, anything else is a plain-text 500).
 */
export const onError: ErrorHandler = (err, c) => {
  const http = err as Error & { getResponse?: () => Response; status?: number };
  if (typeof http.getResponse === 'function') {
    if (!isV1(c)) return http.getResponse();
    const status = (http.status ?? 500) as ContentfulStatusCode;
    return c.json(v1ErrorBody(codeForStatus(status), err.message || 'Request failed.'), status);
  }
  log.error({ err, path: c.req.path }, 'unhandled route error');
  if (isV1(c)) return c.json(v1ErrorBody('internal_error', 'Something went wrong on our side.'), 500);
  return c.text('Internal Server Error', 500);
};
