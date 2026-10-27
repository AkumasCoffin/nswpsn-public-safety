/**
 * The API description — OpenAPI 3.1 — and who sees how much of it.
 *
 * buildSpec() generates the document from the app itself: every route on
 * /api/v1/ that carries describeRoute() metadata (hono-openapi), with its
 * query schema from the route's zod validator. On top of what hono-openapi
 * writes, each operation gets its credential requirement from the same
 * places the gate reads: `security: []` when the path is public
 * (services/auth/apiKey.ts), otherwise the ApiKey scheme plus `x-scopes` —
 * the scopes its scoped() declaration asks for.
 *
 * The generated document is committed as backends/node/openapi.json
 * (`npm run openapi`; a test fails when it is stale), and that file is what
 * GET /api/v1/openapi.json serves: read once, filtered per request.
 *
 *   a named key   only the operations its scopes reach (a `read` key: the old
 *                 feed list) plus the public ones — what the docs page shows
 *   anyone else   who got past the gate (a signed-in person, the site's own
 *                 pages, the static key): the whole document
 *
 * The route is declared ANY_KEY, so every valid key may fetch it — what it
 * gets back is the filtering above.
 */
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Hono } from 'hono';
import { generateSpecs } from 'hono-openapi';
import {
  ANY_KEY,
  declaredNeedOf,
  keyCanRead,
  needScopes,
  scoped,
  type ScopeNeed,
} from '../../services/auth/apiKeyScopes.js';
import { isPublicPath } from '../../services/auth/apiKey.js';
import { log } from '../../lib/log.js';
import { mirroredRoutes } from './mirror.js';
import { fail } from './respond.js';

export type OpenApiDoc = Record<string, any> & { paths: Record<string, Record<string, any>> };

const METHODS = new Set(['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace']);

const DESCRIPTION = `Live and recent public-safety data for Australia — fire and emergency incidents, warnings and hazards — as AusAware relays it from the agencies' own public feeds.

**Keys.** Every feed needs an API key (the reference and service endpoints are public), sent as \`X-API-Key: <key>\` or \`Authorization: Bearer <key>\`. Request one from your profile at nswpsn.forcequit.xyz (API keys); staff choose what it may read. Keys are for scripts and servers — a request carrying a browser \`Origin\` header is refused.

**Scopes.** A key reads an endpoint when its scopes cover the endpoint's source, type and window — listed on each operation as \`x-scopes\`. This document shows the public endpoints and the ones your key's scopes reach.

**Responses.** GeoJSON endpoints answer with a bare FeatureCollection. Everything else answers \`{ "data": … }\`. Errors are \`{ "error": { "code", "message" } }\` with a stable \`code\`.

AusAware is an independent project. It is not affiliated with, endorsed by or acting for any agency whose data it carries; the data is theirs, as published.`;

export const DOCUMENTATION = {
  info: {
    title: 'AusAware API',
    version: '1',
    description: DESCRIPTION,
  },
  servers: [{ url: 'https://api.forcequit.xyz' }],
  tags: [
    { name: 'Incidents', description: 'Fire, rescue and ambulance incidents happening now' },
    { name: 'Warnings', description: 'Public warnings: emergency warnings, fire danger, weather' },
    { name: 'Hazards', description: 'Hazards on the ground: road incidents and works, satellite hotspots' },
    { name: 'Cameras', description: 'Traffic, airport and fire-tower cameras' },
    { name: 'Transport', description: 'Public transport vehicles, stops, lines and alerts' },
    { name: 'Aircraft', description: 'Aircraft positions and tracks from ADS-B' },
    { name: 'Ships', description: 'Vessel positions from AIS' },
    { name: 'Reference', description: 'Slow-changing reference data' },
    { name: 'Service', description: 'Whether the API is up' },
  ],
  components: {
    securitySchemes: {
      ApiKey: {
        type: 'apiKey' as const,
        in: 'header' as const,
        name: 'X-API-Key',
        description: 'Your API key. `Authorization: Bearer <key>` works too.',
      },
    },
  },
};

/** ':slug' → '{slug}', ':id{[0-9]+}' → '{id}' — how OpenAPI writes a Hono pattern. */
export function openApiPath(pattern: string): string {
  return pattern
    .split('/')
    .map((seg) => (seg.startsWith(':') ? `{${seg.slice(1).replace(/\{.*\}$/, '').replace(/\?$/, '')}}` : seg))
    .join('/');
}

/** Generate the document from an app (createApp()). */
export async function buildSpec(app: Hono): Promise<OpenApiDoc> {
  const spec = (await generateSpecs(app, {
    documentation: DOCUMENTATION,
    // Only the versioned API is described; /api/… is its unversioned past.
    exclude: [/^\/api\/(?!v1\/)/],
    // A route lists its own 400 (schemas.ts VALIDATION_ERROR) when its input
    // can actually be refused; most query schemas only fall back to defaults.
    defaultValidationErrorResponse: false,
  })) as unknown as OpenApiDoc;

  for (const route of mirroredRoutes()) {
    const op = spec.paths[openApiPath(route.path)]?.[route.method.toLowerCase()];
    if (!op) continue;
    if (isPublicPath(route.path)) {
      op['security'] = [];
      continue;
    }
    op['security'] = [{ ApiKey: [] }];
    const need = declaredNeedOf(route.handlers);
    if (need && !('anyKey' in need)) op['x-scopes'] = needScopes(need);
  }
  return spec;
}

// ---------------------------------------------------------------------------
// Filtering for a key
// ---------------------------------------------------------------------------

/** x-scopes back into a need: ['source:rfs', 'type:incidents', 'window:live|history']. */
function needFromXScopes(x: unknown): ScopeNeed | null {
  if (!Array.isArray(x)) return null;
  const get = (dim: string) => x.find((s): s is string => typeof s === 'string' && s.startsWith(`${dim}:`))?.slice(dim.length + 1);
  const source = get('source');
  const type = get('type');
  const window = get('window');
  if (!source || !type || !window) return null;
  return { source, type, window: window.includes('|') ? 'any' : (window as 'live' | 'history') };
}

function collectRefs(node: unknown, into: Set<string>): void {
  if (Array.isArray(node)) {
    for (const v of node) collectRefs(v, into);
  } else if (node && typeof node === 'object') {
    for (const [k, v] of Object.entries(node)) {
      if (k === '$ref' && typeof v === 'string') into.add(v);
      else collectRefs(v, into);
    }
  }
}

/** Components the kept operations still refer to, directly or through other components. */
function pruneComponents(components: Record<string, any> | undefined, paths: unknown): Record<string, any> | undefined {
  if (!components) return components;
  const refs = new Set<string>();
  collectRefs(paths, refs);
  const kept = new Set<string>();
  const queue = [...refs];
  while (queue.length) {
    const ref = queue.pop()!;
    if (kept.has(ref)) continue;
    kept.add(ref);
    const m = /^#\/components\/([^/]+)\/(.+)$/.exec(ref);
    const target = m ? components[m[1]!]?.[m[2]!] : undefined;
    if (target) {
      const inner = new Set<string>();
      collectRefs(target, inner);
      queue.push(...inner);
    }
  }
  const out: Record<string, any> = {};
  for (const [type, entries] of Object.entries(components)) {
    if (type === 'securitySchemes') {
      out[type] = entries;
      continue;
    }
    const left = Object.fromEntries(
      Object.entries(entries as Record<string, unknown>).filter(([name]) => kept.has(`#/components/${type}/${name}`)),
    );
    if (Object.keys(left).length) out[type] = left;
  }
  return out;
}

/** The document as a key with these scopes may see it. */
export function filterSpecForKey(doc: OpenApiDoc, scopes: readonly string[]): OpenApiDoc {
  const paths: OpenApiDoc['paths'] = {};
  const tags = new Set<string>();
  for (const [p, item] of Object.entries(doc.paths)) {
    const kept: Record<string, any> = {};
    let ops = 0;
    for (const [m, op] of Object.entries(item)) {
      if (!METHODS.has(m)) {
        kept[m] = op;
        continue;
      }
      const isPublic = Array.isArray(op?.security) && op.security.length === 0;
      if (!isPublic && !keyCanRead(scopes, p, needFromXScopes(op?.['x-scopes']))) continue;
      kept[m] = op;
      ops++;
      for (const t of op?.tags ?? []) tags.add(t);
    }
    if (ops) paths[p] = kept;
  }
  return {
    ...doc,
    tags: Array.isArray(doc['tags']) ? doc['tags'].filter((t: { name?: string }) => t.name && tags.has(t.name)) : doc['tags'],
    paths,
    components: pruneComponents(doc['components'], paths),
  };
}

// ---------------------------------------------------------------------------
// GET /api/v1/openapi.json
// ---------------------------------------------------------------------------

const HERE = path.dirname(fileURLToPath(import.meta.url));
/** <backend>/openapi.json from dist/api/v1 and src/api/v1 alike. */
export const SPEC_FILE = path.resolve(HERE, '../../../openapi.json');

let committed: OpenApiDoc | null | undefined;

function committedSpec(): OpenApiDoc | null {
  if (committed !== undefined) return committed;
  try {
    committed = JSON.parse(readFileSync(SPEC_FILE, 'utf8')) as OpenApiDoc;
  } catch (err) {
    log.error({ err, file: SPEC_FILE }, 'openapi.json missing or unreadable; /api/v1/openapi.json will 503');
    committed = null;
  }
  return committed;
}

/** Test seam: forget the loaded document, or use this one instead. */
export function _setCommittedSpec(doc: OpenApiDoc | null | undefined): void {
  committed = doc;
}

export const openapiRouter = new Hono();

openapiRouter.get('/api/v1/openapi.json', scoped(ANY_KEY), (c) => {
  const doc = committedSpec();
  if (!doc) return fail(c, 503, 'service_unavailable', 'The API description is not available on this server.');
  // Per credential, so never cached by anything in between.
  c.header('Cache-Control', 'private, no-store');
  const scopes = c.get('authClass') === 'apikey' ? c.get('apiKeyScopes') : undefined;
  return c.json(scopes ? filterSpecForKey(doc, scopes) : doc);
});

/** How the document is written to openapi.json — one place, so the staleness test compares like with like. */
export function serialiseSpec(doc: OpenApiDoc): string {
  return `${JSON.stringify(doc, null, 2)}\n`;
}
