/**
 * Shared zod schemas and OpenAPI pieces for the versioned API.
 *
 * Two jobs:
 *
 *  - Inputs. queryValidator() checks a route's query string with zod, on both
 *    of its paths (mirror.ts runs one chain for /api/ and /api/v1/). Every
 *    schema keeps the tolerance the hand-written parsing had — a field that
 *    used to fall back to its default on junk still does (`.catch`), and a
 *    repeated parameter is read by its first value, as c.req.query(name) reads
 *    it — so the old path answers exactly as before. Where a route did refuse
 *    bad input, the refusal is the v1 error shape on /api/v1/ and the route's
 *    old body on /api/ (respond.ts).
 *
 *  - Outputs, for the API description only. Response schemas never run against
 *    a response. Where a handler returns a named TypeScript type, the router
 *    asserts at compile time that the schema and that type are the same shape
 *    (assertShape), so the description cannot drift from what is sent; a body
 *    built inline in the handler is checked with `satisfies` instead.
 */
import { z } from 'zod';
import type { MiddlewareHandler } from 'hono';
import { resolver, uniqueSymbol, validator } from 'hono-openapi';
import type { DescribeRouteOptions } from 'hono-openapi';
import { fail } from './respond.js';
import { ERROR_CODES } from './respond.js';

// ---------------------------------------------------------------------------
// Compile-time shape check
// ---------------------------------------------------------------------------

/** true when A and B are assignable both ways — the same shape. */
export type SameShape<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

/**
 * `assertShape<SameShape<z.infer<typeof Schema>, TheType>>()` fails to compile
 * when a response schema and the type its handler returns part ways.
 */
export function assertShape<T extends true>(): void {
  void (0 as unknown as T);
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/**
 * `?name=1` switches something on; anything else, or nothing, leaves it off —
 * how the routes always read such flags (`c.req.query('all') === '1'`).
 */
export const flag = (description: string) =>
  z.enum(['0', '1']).optional().catch(undefined).describe(description);

/**
 * zod over the query string. On failure: /api/v1/ answers 400
 * validation_failed with the issues; /api/ answers `legacy` (the route's old
 * 400 body) or `{ error: <first issue> }`. The parsed values are what
 * `c.req.valid('query')` returns.
 *
 * Each parameter is read by its first value (`c.req.query()`), the way the
 * routes always read them. hono-openapi's own validator() would see a repeated
 * parameter as an array and refuse it, so it is used only for what it
 * contributes to the API description, which this middleware carries.
 */
export function queryValidator<S extends z.ZodType>(schema: S, legacy?: Record<string, unknown>) {
  const described = validator('query', schema);
  const mw: MiddlewareHandler = async (c, next) => {
    const result = await schema.safeParseAsync(c.req.query());
    if (!result.success) {
      const issues = result.error.issues.map((i) => ({ path: i.path.map(String).join('.'), message: i.message }));
      const first = issues[0];
      const message = first ? `${first.path ? `${first.path}: ` : ''}${first.message}` : 'Invalid query string.';
      return fail(c, 400, 'validation_failed', message, legacy ?? { error: message }, { issues });
    }
    c.req.addValidatedData('query', result.data as Record<string, unknown>);
    await next();
  };
  return Object.assign(mw, {
    [uniqueSymbol]: (described as unknown as Record<symbol, unknown>)[uniqueSymbol],
  }) as unknown as typeof described;
}

// ---------------------------------------------------------------------------
// Outputs: GeoJSON
// ---------------------------------------------------------------------------

/** [longitude, latitude], WGS84 — GeoJSON order. */
export const LonLat = z.tuple([z.number(), z.number()]).describe('[longitude, latitude] (WGS84)');

/** Outer rings as GeoJSON polygons write them: rings → points → [lon, lat]. */
export const Rings = z.array(z.array(z.array(z.number()))).describe('Polygon outer rings, each a list of [longitude, latitude] points');

export const PointGeometry = z.object({ type: z.literal('Point'), coordinates: LonLat });

/** A GeoJSON FeatureCollection with the feed's own extra top-level fields. */
export function featureCollection<F extends z.ZodType, X extends z.ZodRawShape = Record<never, never>>(
  feature: F,
  extra?: X,
) {
  return z.object({
    type: z.literal('FeatureCollection'),
    features: z.array(feature),
    count: z.number().int().describe('Number of features'),
    ...(extra ?? ({} as X)),
  });
}

export function feature<G extends z.ZodType, P extends z.ZodType>(geometry: G, properties: P) {
  return z.object({ type: z.literal('Feature'), geometry, properties });
}

/**
 * The properties every fire and emergency feed shares — each source maps its
 * agency's feed onto this shape so the map renders them through one path —
 * plus whatever each adds (`incidentProperties({ … })`).
 */
export function incidentProperties<X extends z.ZodRawShape>(extra: X) {
  return z.object({
    title: z.string(),
    guid: z.string().describe("A stable id for the record across polls, built from the agency's own id where it has one"),
    status: z.string().describe('As the agency words it'),
    location: z.string(),
    alertLevel: z
      .string()
      .describe('Warning level as published (Advice, Watch and Act, Emergency Warning), or what the feed has in its place — RFS mostly says "Not Applicable"'),
    fireType: z.string().describe('Kind of incident as the agency classes it'),
    responsibleAgency: z.string(),
    updated: z.string().describe("Last update for display: the agency's own wording (RFS, NT) or a local-time rendering of updatedISO (the others)"),
    updatedISO: z
      .string()
      .describe('Last update as an ISO 8601 timestamp; empty when there is none, and for RFS the agency text itself when it cannot be read as a time'),
    ...extra,
  });
}

/** The map layer a record belongs on. */
export const Layer = <const T extends readonly [string, ...string[]]>(values: T) =>
  z.enum(values).describe('Which map layer the record belongs on');

// ---------------------------------------------------------------------------
// Outputs: envelope and errors
// ---------------------------------------------------------------------------

/** The v1 body of every non-GeoJSON answer: `{ "data": … }`. */
export function envelope<S extends z.ZodType>(data: S) {
  return z.object({ data });
}

export const ErrorBody = z
  .object({
    error: z.object({
      code: z.enum(ERROR_CODES).describe('Stable, machine-readable reason'),
      message: z.string().describe('For people; may change'),
      details: z.unknown().optional().describe('Extra context for some codes, such as the failed fields for validation_failed'),
    }),
  })
  .meta({ id: 'Error' });

type Responses = NonNullable<DescribeRouteOptions['responses']>;

const errorResponse = (description: string) => ({
  description,
  content: { 'application/json': { schema: resolver(ErrorBody) } },
});

/** A 200 with a JSON body. */
export function okJson(schema: z.ZodType, description: string): Responses {
  return { 200: { description, content: { 'application/json': { schema: resolver(schema) } } } };
}

/** What every key-gated operation can also answer. */
export const GATED_ERRORS: Responses = {
  401: errorResponse('No credential, or a session token that is not valid here (unauthorized, token_*)'),
  403: errorResponse(
    'The key is not valid, is used from a web page, or its scopes do not cover this endpoint (invalid_api_key, api_key_browser_use, api_key_scope_forbidden, api_key_path_forbidden)',
  ),
  429: errorResponse("The key is over its per-minute limit (rate_limited)"),
  503: errorResponse('The key could not be checked just now; try again shortly (auth_unavailable)'),
};

export const VALIDATION_ERROR: Responses = {
  400: errorResponse('A query parameter is not valid (validation_failed)'),
};

export const NOT_FOUND_ERROR: Responses = {
  404: errorResponse('Nothing by that name (not_found)'),
};

export const UNAVAILABLE_ERROR: Responses = {
  503: errorResponse('A dependency is down; try again shortly (service_unavailable)'),
};
