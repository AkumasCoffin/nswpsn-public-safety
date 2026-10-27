/**
 * Serving a router's routes under /api/v1/ as well as /api/.
 *
 * Every router registers absolute /api/… paths and is mounted at '/'
 * (server.ts), and the tests mount them the same way. Rather than rewrite
 * them, mirror() registers the routes named in `only` a second time, under
 * /api/v1/, with the SAME handler chain — the same middleware, the same
 * scoped() declaration (so the credential gate judges both paths alike), the
 * same zod validators and describeRoute() metadata. Only what a handler does
 * with isV1(c) (respond.ts) differs between the two.
 *
 * `only` is an allow-list on purpose: what is versioned is a decision per
 * route (the public data API — docs/api.md), and a route added to a router
 * later does not appear on /api/v1/ by accident. A name in `only` that the
 * router does not have throws, so a rename cannot drop a v1 route silently.
 *
 * Router-level middleware (`router.use(...)`) travels with the routes it
 * covers, matched the way Hono matches it (`/api/x/*` covers `/api/x` too).
 * A route that is NOT mirrored but would run before a mirrored one on the
 * same request — a method-specific guard such as `router.get('/api/x/*', …)`,
 * or a `/:param` route registered first — throws: the v1 twin would run
 * without it, and that could drop a role check. A wildcard anywhere but at
 * the end throws too, rather than be guessed at.
 */
import type { Hono, MiddlewareHandler } from 'hono';
import { toV1Path } from '../../lib/apiVersion.js';

/** One route on /api/v1/, as mirror() registered it. */
export interface MirroredRoute {
  /** GET, POST, … */
  method: string;
  /** The v1 pattern: '/api/v1/agency/extended/:slug'. */
  path: string;
  /** The pattern it mirrors: '/api/agency/extended/:slug'. */
  legacyPath: string;
  /** The handler chain, router middleware first — where its scope is declared. */
  handlers: readonly unknown[];
}

const registry = new Map<string, MirroredRoute>();

/** Every route mirror() has put on /api/v1/ so far (the API description reads this). */
export function mirroredRoutes(): MirroredRoute[] {
  return [...registry.values()];
}

/**
 * Could a handler registered on `pattern` run for a request to the route
 * `path`? Both are Hono patterns. A `:param` on either side matches any one
 * segment (so the answer errs towards "yes"); a trailing `/*` matches the
 * rest, including nothing.
 */
function covers(pattern: string, path: string): boolean {
  if (pattern === path || pattern === '*' || pattern === '/*') return true;
  const tail = pattern.endsWith('/*');
  const pSegs = (tail ? pattern.slice(0, -2) : pattern).split('/');
  if (pSegs.some((seg) => seg.includes('*'))) {
    throw new Error(`mirror: cannot tell whether "${pattern}" applies to "${path}"; declare it on the route`);
  }
  const segs = path.split('/');
  if (tail ? segs.length < pSegs.length : segs.length !== pSegs.length) return false;
  return pSegs.every((seg, i) => seg === segs[i] || seg.startsWith(':') || segs[i]!.startsWith(':'));
}

export function mirror(app: Hono, router: Hono, only: readonly string[]): void {
  const wanted = new Set(only);
  // Per wanted path, its chain in registration order: router middleware
  // (method ALL) and route handlers alike, as Hono would run them.
  const chains = new Map<string, Array<{ method: string; handler: unknown }>>();
  // The methods each wanted path has, to tell a route that shadows one from a
  // route of another method that merely shares its pattern.
  const wantedMethods = new Map<string, Set<string>>();
  for (const r of router.routes) {
    if (r.method !== 'ALL' && wanted.has(r.path)) {
      wantedMethods.get(r.path)?.add(r.method) ?? wantedMethods.set(r.path, new Set([r.method]));
    }
  }
  for (const r of router.routes) {
    if (r.method === 'ALL') {
      for (const p of only) {
        if (!covers(r.path, p)) continue;
        app.use(toV1Path(p), r.handler as MiddlewareHandler);
        chains.get(p)?.push({ method: 'ALL', handler: r.handler }) ?? chains.set(p, [{ method: 'ALL', handler: r.handler }]);
      }
      continue;
    }
    if (!wanted.has(r.path)) {
      // Registered before a mirrored route of its method and able to run for
      // it: on /api/ it runs first, on /api/v1/ it would not run at all.
      const shadows = only.find(
        (p) =>
          wantedMethods.get(p)?.has(r.method) &&
          !chains.get(p)?.some((e) => e.method === r.method) &&
          covers(r.path, p),
      );
      if (shadows) {
        throw new Error(`mirror: "${r.method} ${r.path}" runs before "${shadows}" but is not mirrored; mirror it too or register it after`);
      }
      continue;
    }
    app.on(r.method, toV1Path(r.path), r.handler as MiddlewareHandler);
    chains.get(r.path)?.push({ method: r.method, handler: r.handler }) ?? chains.set(r.path, [{ method: r.method, handler: r.handler }]);
  }

  const missing = only.filter((p) => !chains.get(p)?.some((e) => e.method !== 'ALL'));
  if (missing.length) throw new Error(`mirror: router has no route ${missing.join(', ')}`);

  for (const [legacyPath, chain] of chains) {
    const methods = new Set(chain.filter((e) => e.method !== 'ALL').map((e) => e.method));
    for (const method of methods) {
      const path = toV1Path(legacyPath);
      registry.set(`${method} ${path}`, {
        method,
        path,
        legacyPath,
        handlers: chain.filter((e) => e.method === 'ALL' || e.method === method).map((e) => e.handler),
      });
    }
  }
}
