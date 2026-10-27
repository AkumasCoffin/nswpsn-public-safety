/**
 * The versioned API's paths.
 *
 * The public data API is served at /api/v1/… as well as at its original
 * /api/… path (api/v1/mirror.ts). A v1 path and its unversioned twin are the
 * same endpoint: the same handler, the same scope, the same public/gated
 * status. Only the response envelope and the error shape differ (api/v1/
 * respond.ts). These helpers turn one into the other.
 */

export const V1_PREFIX = '/api/v1/';

export function isV1Path(path: string): boolean {
  return path.startsWith(V1_PREFIX);
}

/** '/api/rfs/incidents' → '/api/v1/rfs/incidents'. */
export function toV1Path(path: string): string {
  if (!path.startsWith('/api/') || isV1Path(path)) throw new Error(`not an unversioned /api/ path: ${path}`);
  return V1_PREFIX + path.slice('/api/'.length);
}

/** '/api/v1/rfs/incidents' → '/api/rfs/incidents'; anything else unchanged. */
export function unversionedPath(path: string): string {
  return isV1Path(path) ? '/api/' + path.slice(V1_PREFIX.length) : path;
}
