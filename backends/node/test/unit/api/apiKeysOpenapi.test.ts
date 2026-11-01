/**
 * GET /api/api-keys/openapi.json — the API description the site's reference
 * page (/api-docs) reads, signed in rather than with a key.
 *
 *  - Signed in only; a key or nothing gets nowhere.
 *  - Filtered to the person's ACTIVE keys — an operation stays when any one
 *    of them reads it — plus the public endpoints. Revoked and expired keys
 *    count for nothing; with no active key the answer is 403 no_active_key.
 *  - Owner and staff see the whole document.
 *  - Never cached.
 *
 * The document is a small one set here, in the scopes' own form, so these
 * tests do not move when the committed openapi.json is regenerated.
 */
import { describe, it, expect, beforeEach, afterAll, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
const calls: Call[] = [];
let keyRows: Array<Record<string, unknown>> = [];

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return { rows: keyRows, rowCount: keyRows.length };
  }),
};

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => fakePool),
}));

vi.mock('../../../src/services/auth/supabaseJwt.js', () => ({
  requireSupabaseJwt: async (c: { get: (k: string) => unknown; json: (b: unknown, s: number) => unknown }, next: () => Promise<void>) => {
    if (!c.get('userId')) return c.json({ error: 'auth required' }, 401);
    await next();
  },
}));

let staff = false;
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canManageUsers: vi.fn(async () => staff) };
});

vi.mock('../../../src/services/wireComments.js', () => ({
  notify: vi.fn(async () => undefined),
  displayNameMap: vi.fn(async () => new Map<string, string>()),
}));
vi.mock('../../../src/services/staffNotify.js', () => ({ notifyStaff: () => undefined }));
vi.mock('../../../src/api/users.js', () => ({ getUsername: vi.fn(async () => 'Staff Person') }));

const { apiKeysRouter } = await import('../../../src/api/apiKeys.js');
const { _setCommittedSpec } = await import('../../../src/api/v1/openapi.js');
const { isPublicPath } = await import('../../../src/services/auth/apiKey.js');
type OpenApiDoc = import('../../../src/api/v1/openapi.js').OpenApiDoc;

const ref = (name: string) => ({
  '200': { description: 'OK', content: { 'application/json': { schema: { $ref: `#/components/schemas/${name}` } } } },
});
const gated = (tag: string, scope: string, model: string) => ({
  get: { operationId: `get${model}`, tags: [tag], security: [{ ApiKey: [] }], 'x-scopes': [scope], responses: ref(model) },
});

const DOC: OpenApiDoc = {
  openapi: '3.1.0',
  info: { title: 'AusAware API', version: '1' },
  tags: [
    { name: 'Fires' }, { name: 'Warnings' }, { name: 'Aircraft' }, { name: 'History' }, { name: 'Service' },
  ],
  paths: {
    '/api/v1/fires': gated('Fires', 'fires:live', 'Fires'),
    '/api/v1/bom/warnings': gated('Warnings', 'warnings:live', 'BomWarnings'),
    '/api/v1/adsb/aircraft': gated('Aircraft', 'aircraft:live', 'Aircraft'),
    '/api/v1/data/history': gated('History', '*:history', 'History'),
    '/api/v1/health': { get: { operationId: 'getHealth', tags: ['Service'], security: [], responses: ref('Health') } },
  },
  components: {
    schemas: {
      Fires: { type: 'object' },
      BomWarnings: { type: 'object' },
      Aircraft: { type: 'object' },
      History: { type: 'object' },
      Health: { type: 'object' },
    },
    securitySchemes: { ApiKey: { type: 'apiKey', in: 'header', name: 'X-API-Key' } },
  },
};

const DAY = 24 * 60 * 60 * 1000;
const KEY = (over: Record<string, unknown> = {}) => ({
  id: 'k1', user_id: 'user-1', name: 'cli', prefix: 'abcdefghij', key_hash: 'deadbeef',
  scopes: ['fires:live'], rate_limit_per_min: 120, created_at: 't', last_used_at: null,
  expires_at: null, revoked_at: null, deleted_at: null, ...over,
});

function makeApp(userId: string | null = 'user-1') {
  const app = new Hono();
  app.use('*', async (c, next) => {
    if (userId) c.set('userId', userId);
    await next();
  });
  app.route('/', apiKeysRouter);
  return app;
}

async function get(userId: string | null = 'user-1') {
  const res = await makeApp(userId).request('/api/api-keys/openapi.json');
  const body = (await res.json()) as Record<string, any>;
  return { res, body, paths: Object.keys((body as OpenApiDoc).paths ?? {}) };
}

beforeEach(() => {
  calls.length = 0;
  keyRows = [];
  staff = false;
  _setCommittedSpec(DOC);
});
afterAll(() => _setCommittedSpec(undefined));

describe('GET /api/api-keys/openapi.json', () => {
  it('401 without a session, and reads nothing', async () => {
    const { res } = await get(null);
    expect(res.status).toBe(401);
    expect(calls).toHaveLength(0);
  });

  it('is not a public path: the credential gate runs before it, and no scope declares it, so no key reaches it', () => {
    expect(isPublicPath('/api/api-keys/openapi.json')).toBe(false);
  });

  it('403 no_active_key when the person holds no key', async () => {
    const { res, body } = await get();
    expect(res.status).toBe(403);
    expect(body.error).toBe('no_active_key');
    expect(body.message).toBeTruthy();
    expect(res.headers.get('Cache-Control')).toContain('no-store');
  });

  it('403 no_active_key when every key is revoked or expired', async () => {
    keyRows = [
      KEY({ revoked_at: new Date(Date.now() - DAY).toISOString() }),
      KEY({ id: 'k2', expires_at: new Date(Date.now() - DAY).toISOString() }),
    ];
    const { res, body } = await get();
    expect(res.status).toBe(403);
    expect(body.error).toBe('no_active_key');
  });

  it('reads only the caller’s keys, and not the ones they deleted', async () => {
    keyRows = [KEY()];
    await get();
    expect(calls).toHaveLength(1);
    expect(calls[0]!.sql).toMatch(/FROM api_keys WHERE user_id = \$1 AND deleted_at IS NULL/);
    expect(calls[0]!.params).toEqual(['user-1']);
  });

  it('two active keys: the union of what each reads, plus the public endpoints, never cached', async () => {
    keyRows = [KEY({ scopes: ['fires:live'] }), KEY({ id: 'k2', scopes: ['warnings:live'] })];
    const { res, body, paths } = await get();
    expect(res.status).toBe(200);
    expect(res.headers.get('Cache-Control')).toContain('no-store');
    expect(paths.sort()).toEqual(['/api/v1/bom/warnings', '/api/v1/fires', '/api/v1/health']);
    // Tags and models follow the operations left.
    expect((body.tags as Array<{ name: string }>).map((t) => t.name).sort()).toEqual(['Fires', 'Service', 'Warnings']);
    expect(Object.keys(body.components.schemas).sort()).toEqual(['BomWarnings', 'Fires', 'Health']);
    expect(body.components.securitySchemes.ApiKey).toBeDefined();
  });

  it('a revoked or expired key adds nothing to what the active ones read', async () => {
    keyRows = [
      KEY({ scopes: ['fires:live'] }),
      KEY({ id: 'k2', scopes: ['warnings:live'], revoked_at: new Date(Date.now() - DAY).toISOString() }),
      KEY({ id: 'k3', scopes: ['aircraft:live'], expires_at: new Date(Date.now() - DAY).toISOString() }),
      KEY({ id: 'k4', scopes: ['pager:history'], expires_at: new Date(Date.now() + DAY).toISOString() }),
    ];
    const { res, paths } = await get();
    expect(res.status).toBe(200);
    // k4 has not expired yet: the history of an archived feed opens stored history.
    expect(paths.sort()).toEqual(['/api/v1/data/history', '/api/v1/fires', '/api/v1/health']);
  });

  it('history the archive does not hold (aircraft) does not open stored history', async () => {
    keyRows = [KEY({ scopes: ['aircraft:history', 'aircraft:live'] })];
    const { paths } = await get();
    expect(paths.sort()).toEqual(['/api/v1/adsb/aircraft', '/api/v1/health']);
  });

  it('a key holding only retired scopes adds nothing but the public endpoints', async () => {
    keyRows = [KEY({ scopes: ['source:rfs', 'type:incidents', 'window:live'] })];
    const { res, paths } = await get();
    expect(res.status).toBe(200);
    expect(paths).toEqual(['/api/v1/health']);
  });

  it('a `read` key next to a scoped one: each judged by its own rule, not pooled into a `read` list', async () => {
    keyRows = [KEY({ scopes: ['read'] }), KEY({ id: 'k2', scopes: ['fires:live'] })];
    const { paths } = await get();
    expect(paths).toContain('/api/v1/bom/warnings'); // on the old feed list
    expect(paths).toContain('/api/v1/fires'); // the scoped key's
    expect(paths).not.toContain('/api/v1/data/history'); // neither reaches it
  });

  it('owner and staff see the whole document, with or without keys of their own', async () => {
    staff = true;
    const { res, body } = await get('staff-1');
    expect(res.status).toBe(200);
    expect(res.headers.get('Cache-Control')).toContain('no-store');
    expect(body).toEqual(DOC);
    expect(calls).toHaveLength(0);
  });

  it('503 when the server has no description to give', async () => {
    _setCommittedSpec(null);
    keyRows = [KEY()];
    const { res } = await get();
    expect(res.status).toBe(503);
  });
});

describe('the old /api/docs page', () => {
  it('is gone from the API: nothing there is public any more', () => {
    expect(isPublicPath('/api/docs')).toBe(false);
    expect(isPublicPath('/api/docs/scalar.js')).toBe(false);
  });
});
