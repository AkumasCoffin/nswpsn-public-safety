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
 *                 feed list) plus the public ones
 *   anyone else   who got past the gate (a signed-in person, the site's own
 *                 pages, the static key): the whole document
 *
 * The route is declared ANY_KEY, so every valid key may fetch it — what it
 * gets back is the filtering above.
 *
 * The site's API reference page (/api-docs) reads the same document through
 * GET /api/api-keys/openapi.json (api/apiKeys.ts) instead: signed in, filtered
 * to every active key the person holds (filterSpecForKeys), whole for staff.
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
  needFromScope,
  needScopes,
  scoped,
  type ScopeNeed,
} from '../../services/auth/apiKeyScopes.js';
import { isPublicPath } from '../../services/auth/apiKey.js';
import { isOldV1Path } from '../../lib/apiVersion.js';
import { log } from '../../lib/log.js';
import { mirroredRoutes } from './mirror.js';
import { fail } from './respond.js';

export type OpenApiDoc = Record<string, any> & { paths: Record<string, Record<string, any>> };

const METHODS = new Set(['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace']);

const DESCRIPTION = `Live and recent public-safety data for Australia — fire and emergency incidents, warnings and hazards — as AusAware relays it from the agencies' own public feeds.

**Keys.** Every feed needs an API key (the reference and service endpoints are public), sent as \`X-API-Key: <key>\` or \`Authorization: Bearer <key>\`. Request one from your profile at nswpsn.forcequit.xyz (API keys); staff choose what it may read. Keys are for scripts and servers — a request carrying a browser \`Origin\` header is refused.

**Scopes.** A key is granted feeds, each live, history or both — written \`fires:live\`, \`fires:history\`. Each operation lists the one it needs as \`x-scopes\`; stored history needs the history of at least one stored feed (\`*:history\`) and answers only for the feeds the key holds. This document shows the public endpoints and the ones your keys reach.

**Responses.** GeoJSON endpoints answer with a bare FeatureCollection. Everything else answers \`{ "data": … }\`. Errors are \`{ "error": { "code", "message" } }\` with a stable \`code\`.

AusAware is an independent project. It is not affiliated with, endorsed by or acting for any agency whose data it carries; the data is theirs, as published.`;

export const DOCUMENTATION = {
  info: {
    title: 'AusAware API',
    version: '1',
    description: DESCRIPTION,
  },
  servers: [{ url: 'https://api.forcequit.xyz' }],
  // One tag per feed a key is granted (FEEDS in services/auth/apiKeyScopes.ts),
  // in the same order, so the reference reads like the table on the profile.
  tags: [
    { name: 'Fires', description: 'Every fire service in one feed with the QLD and WA public warnings, satellite hotspots and fire danger ratings' },
    { name: 'Weather warnings', description: 'Bureau of Meteorology land and marine warnings' },
    { name: 'Ambulance', description: 'ACT Ambulance Service incidents' },
    { name: 'Power outages', description: "Every electricity distributor's outages in one feed — Endeavour Energy, Essential Energy and Ausgrid — unplanned and planned" },
    { name: 'Road hazards', description: "Road incidents, roadworks, floods and events: NSW's from Live Traffic NSW, QLD's, VIC's, SA's and the ACT's from their own feeds" },
    { name: 'Public transport', description: 'Public transport vehicles, stops, lines and alerts' },
    { name: 'Aircraft', description: 'Aircraft positions and tracks from ADS-B' },
    { name: 'Ships', description: 'Vessel positions from AIS' },
    { name: 'Weather', description: 'Current conditions, forecasts and radar' },
    { name: 'Beaches & sharks', description: 'Beach water quality and conditions, and shark reports' },
    { name: 'Pager', description: 'Emergency service pager messages' },
    { name: 'Radio summaries', description: 'Hourly and ad-hoc summaries of radio receptions' },
    { name: 'History', description: 'Stored records over time, for every feed your key holds the history of' },
    { name: 'Reference', description: 'Slow-changing reference data: agencies' },
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
  // A renamed route's old v1 path answers only until this site's pages move
  // off it (lib/apiVersion.ts); it is not part of the API.
  for (const p of Object.keys(spec.paths)) if (isOldV1Path(p)) delete spec.paths[p];

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

/** x-scopes back into a need: ['fires:live'], or ['*:history'] for stored history. */
function needFromXScopes(x: unknown): ScopeNeed | null {
  if (!Array.isArray(x) || typeof x[0] !== 'string') return null;
  return needFromScope(x[0]);
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
  return filterSpecForKeys(doc, [scopes]);
}

/**
 * The document as someone holding these keys may see it: an operation stays
 * when at least one of the keys reaches it. Judged key by key rather than on
 * the keys' scopes pooled into one list — a `read` key's scope next to
 * another key's `fires:live` would make the pooled list a `read` list, and
 * hide what the second key reads.
 */
export function filterSpecForKeys(doc: OpenApiDoc, keys: ReadonlyArray<readonly string[]>): OpenApiDoc {
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
      const need = needFromXScopes(op?.['x-scopes']);
      if (!isPublic && !keys.some((scopes) => keyCanRead(scopes, p, need))) continue;
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

/** The committed document, read on first use and kept; null when it is missing. Never mutate it. */
export function committedSpec(): OpenApiDoc | null {
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
