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

/**
 * Where a v1 path is not simply its /api/ path with v1/ added: stored history
 * is /api/v1/history, not /api/v1/data/history (owner, 2026-11-02). Each entry
 * renames the path and everything under it.
 */
const V1_RENAMES: ReadonlyArray<readonly [legacy: string, v1: string]> = [['/api/data/history', '/api/v1/history']];

/**
 * Renamed paths whose old v1 path still answers, for this site's pages until
 * they move to the new one: a page and the backend deploy minutes apart, and
 * a page left open keeps asking for what it loaded with. The old path is not
 * described (api/v1/openapi.ts) and goes when old paths are retired.
 */
const V1_OLD_NAMES_KEPT: readonly string[] = ['/api/data/history'];

export function isV1Path(path: string): boolean {
  return path.startsWith(V1_PREFIX);
}

/** `path` is `prefix` or something under it — '/api/v1/history/filters' is under '/api/v1/history'. */
const under = (path: string, prefix: string): boolean => path === prefix || path.startsWith(`${prefix}/`);

/** '/api/v1/' + the rest of an /api/ path, renames aside. */
function plainV1Path(path: string): string {
  if (!path.startsWith('/api/') || isV1Path(path)) throw new Error(`not an unversioned /api/ path: ${path}`);
  return V1_PREFIX + path.slice('/api/'.length);
}

/** '/api/rfs/incidents' → '/api/v1/rfs/incidents'; '/api/data/history' → '/api/v1/history'. */
export function toV1Path(path: string): string {
  for (const [legacy, v1] of V1_RENAMES) if (under(path, legacy)) return v1 + path.slice(legacy.length);
  return plainV1Path(path);
}

/** The old v1 paths a renamed path still answers at (V1_OLD_NAMES_KEPT); none for most. */
export function oldV1Paths(path: string): string[] {
  return V1_OLD_NAMES_KEPT.some((legacy) => under(path, legacy)) ? [plainV1Path(path)] : [];
}

/** Is this an old v1 path kept only until this site's pages move off it? */
export function isOldV1Path(path: string): boolean {
  return V1_OLD_NAMES_KEPT.some((legacy) => under(path, plainV1Path(legacy)));
}

/** '/api/v1/rfs/incidents' → '/api/rfs/incidents'; '/api/v1/history' → '/api/data/history'; anything else unchanged. */
export function unversionedPath(path: string): string {
  if (!isV1Path(path)) return path;
  for (const [legacy, v1] of V1_RENAMES) if (under(path, v1)) return legacy + path.slice(v1.length);
  return '/api/' + path.slice(V1_PREFIX.length);
}
