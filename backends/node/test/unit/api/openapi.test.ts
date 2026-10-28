/**
 * The API description and the documentation page.
 *
 *  - The committed openapi.json is exactly what `npm run openapi` writes now.
 *  - Every /api/v1/ route is described, and says what credential it takes:
 *    public, or the ApiKey scheme with the scopes its scoped() declaration
 *    asks for.
 *  - GET /api/v1/openapi.json shows a key only what that key can read.
 *  - /api/docs and its two scripts are served as the page expects.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import { readFileSync } from 'node:fs';
import { brotliDecompressSync } from 'node:zlib';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Hono } from 'hono';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  getWriterPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

import { _clearApiKeyCache, lookupPrefix } from '../../../src/services/auth/apiKeys.js';
import {
  buildSpec,
  filterSpecForKey,
  openApiPath,
  serialiseSpec,
  SPEC_FILE,
  _setCommittedSpec,
  type OpenApiDoc,
} from '../../../src/api/v1/openapi.js';
import { mirroredRoutes } from '../../../src/api/v1/mirror.js';
import { isPublicPath } from '../../../src/services/auth/apiKey.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ASSETS = path.resolve(HERE, '../../../assets');

let app: Hono;
let fresh: OpenApiDoc;
beforeAll(async () => {
  const { createApp } = await import('../../../src/server.js');
  app = createApp();
  fresh = await buildSpec(app);
});

const ops = (doc: OpenApiDoc) =>
  Object.entries(doc.paths).flatMap(([p, item]) => Object.entries(item).map(([m, op]) => ({ path: p, method: m, op })));

describe('openapi.json', () => {
  it('the committed file is current — run `npm run openapi` and commit it if this fails', () => {
    expect(readFileSync(SPEC_FILE, 'utf8')).toBe(serialiseSpec(fresh));
  });

  it('describes every route on /api/v1/ and nothing else', () => {
    const described = new Set(ops(fresh).map((o) => `${o.method.toUpperCase()} ${o.path}`));
    const mirrored = new Set(mirroredRoutes().map((r) => `${r.method} ${openApiPath(r.path)}`));
    expect(described).toEqual(mirrored);
    for (const p of Object.keys(fresh.paths)) expect(p.startsWith('/api/v1/')).toBe(true);
  });

  it('every operation is a documented GET with a stable id, one tag and a 200', () => {
    const ids = new Set<string>();
    for (const { path: p, method, op } of ops(fresh)) {
      expect(method, p).toBe('get');
      expect(op.operationId, p).toMatch(/^get[A-Z]/);
      expect(ids.has(op.operationId), op.operationId).toBe(false);
      ids.add(op.operationId);
      expect(op.summary, p).toBeTruthy();
      expect(op.tags, p).toHaveLength(1);
      expect((fresh['tags'] as Array<{ name: string }>).map((t) => t.name), p).toContain(op.tags[0]);
      expect(op.responses['200'], p).toBeDefined();
    }
  });

  it('every operation says what credential it takes, from the same rules the gate uses', () => {
    for (const { path: p, op } of ops(fresh)) {
      if (isPublicPath(p)) {
        expect(op.security, p).toEqual([]);
        expect(op['x-scopes'], p).toBeUndefined();
      } else {
        expect(op.security, p).toEqual([{ ApiKey: [] }]);
        expect(op['x-scopes'], p).toHaveLength(3);
        expect(Object.keys(op.responses), p).toEqual(expect.arrayContaining(['401', '403', '429']));
      }
    }
  });

  it('never names a host but the API itself', () => {
    expect(fresh['servers']).toEqual([{ url: 'https://api.forcequit.xyz' }]);
  });
});

describe('filterSpecForKey', () => {
  const RFS_ONLY = ['source:rfs', 'type:incidents', 'window:live'];

  it('a scoped key sees what its scopes reach, plus the public operations', () => {
    const seen = ops(filterSpecForKey(fresh, RFS_ONLY)).map((o) => o.path);
    expect(seen).toContain('/api/v1/rfs/incidents');
    expect(seen).toContain('/api/v1/rfs/incidents/raw');
    expect(seen).not.toContain('/api/v1/qld-fire/incidents'); // another source
    expect(seen).not.toContain('/api/v1/bom/warnings');
    expect(seen).toContain('/api/v1/health');
    expect(seen).toContain('/api/v1/agency/extended/{slug}');
  });

  it('drops the tags and models the key has no operation for', () => {
    const doc = filterSpecForKey(fresh, RFS_ONLY);
    expect((doc['tags'] as Array<{ name: string }>).map((t) => t.name)).not.toContain('Warnings');
    expect(Object.keys(doc['components'].schemas)).toContain('RfsIncidents');
    expect(Object.keys(doc['components'].schemas)).not.toContain('BomWarnings');
    expect(doc['components'].schemas.Error).toBeDefined();
    expect(doc['components'].securitySchemes.ApiKey).toBeDefined();
  });

  it('wildcards and window "any" read as the gate reads them', () => {
    const history = ops(filterSpecForKey(fresh, ['source:*', 'type:*', 'window:history'])).map((o) => o.path);
    expect(history).not.toContain('/api/v1/rfs/incidents'); // live only
    expect(history).toContain('/api/v1/adsb/history');
    const live = ops(filterSpecForKey(fresh, ['source:*', 'type:*', 'window:live'])).map((o) => o.path);
    expect(live).not.toContain('/api/v1/adsb/history'); // history only
    // Everything else is live, public, or reference data that takes either window.
    const historyOnly = ops(fresh).filter((o) => (o.op['x-scopes'] ?? []).includes('window:history'));
    expect(live).toHaveLength(ops(fresh).length - historyOnly.length);
    const both = ops(filterSpecForKey(fresh, ['source:*', 'type:*', 'window:*'])).length;
    expect(both).toBe(ops(fresh).length);
  });

  it('stored history: one source scope with window:history is enough to see it', () => {
    const seen = ops(filterSpecForKey(fresh, ['source:rfs', 'type:incidents', 'window:history'])).map((o) => o.path);
    expect(seen).toContain('/api/v1/data/history');
    expect(seen).toContain('/api/v1/data/history/incident/{source}/{source_id}');
    expect(seen).not.toContain('/api/v1/adsb/history'); // another source
    expect(seen).not.toContain('/api/v1/rfs/incidents'); // live
  });

  it('a `read` key sees the feeds on the old list', () => {
    const seen = ops(filterSpecForKey(fresh, ['read'])).map((o) => o.path);
    expect(seen).toContain('/api/v1/bom/warnings');
    expect(seen).toContain('/api/v1/sa-fire/cfs');
  });

  it('leaves the document it was given alone', () => {
    const before = JSON.stringify(fresh);
    filterSpecForKey(fresh, RFS_ONLY);
    expect(JSON.stringify(fresh)).toBe(before);
  });
});

describe('GET /api/v1/openapi.json', () => {
  const KEY = 'ak_abcdefghij_' + 'K'.repeat(32);
  let scopes: string[];
  beforeEach(() => {
    scopes = ['source:rfs', 'type:incidents', 'window:live'];
    _clearApiKeyCache();
    _setCommittedSpec(fresh);
    queryMock.mockReset();
    queryMock.mockImplementation(async (sql: string) =>
      /FROM api_keys/.test(sql)
        ? {
            rows: [{
              id: 'k1', user_id: 'u1', name: 'cli', prefix: lookupPrefix(KEY),
              key_hash: createHash('sha256').update(KEY).digest('hex'), scopes, rate_limit_per_min: 100,
              created_at: 'x', last_used_at: null, expires_at: null, revoked_at: null,
            }],
            rowCount: 1,
          }
        : { rows: [], rowCount: 0 });
  });

  it('a key gets its own view, never cached', async () => {
    const res = await app.request('/api/v1/openapi.json', { headers: { 'X-API-Key': KEY } });
    expect(res.status).toBe(200);
    expect(res.headers.get('Cache-Control')).toBe('private, no-store');
    const doc = (await res.json()) as OpenApiDoc;
    expect(Object.keys(doc.paths)).toContain('/api/v1/rfs/incidents');
    expect(Object.keys(doc.paths)).not.toContain('/api/v1/bom/warnings');
  });

  it('a `read` key may fetch it too (it is not on the old feed list)', async () => {
    scopes = ['read'];
    const res = await app.request('/api/v1/openapi.json', { headers: { 'X-API-Key': KEY } });
    expect(res.status).toBe(200);
    expect(Object.keys(((await res.json()) as OpenApiDoc).paths)).toContain('/api/v1/bom/warnings');
  });

  it('the static key sees the whole document', async () => {
    const res = await app.request('/api/v1/openapi.json', { headers: { 'X-API-Key': 'test-api-key' } });
    expect(Object.keys(((await res.json()) as OpenApiDoc).paths)).toHaveLength(Object.keys(fresh.paths).length);
  });

  it('nobody without a credential', async () => {
    expect((await app.request('/api/v1/openapi.json')).status).toBe(401);
  });

  it('503 in the v1 shape when the file is missing', async () => {
    _setCommittedSpec(null);
    const res = await app.request('/api/v1/openapi.json', { headers: { 'X-API-Key': 'test-api-key' } });
    expect(res.status).toBe(503);
    expect((await res.json()).error.code).toBe('service_unavailable');
  });
});

describe('/api/docs', () => {
  it('serves the page to anyone, locked to this origin', async () => {
    const res = await app.request('/api/docs');
    expect(res.status).toBe(200);
    const csp = res.headers.get('Content-Security-Policy') ?? '';
    expect(csp).toContain("default-src 'none'");
    expect(csp).toContain("connect-src 'self'");
    expect(csp).toContain("script-src 'self'");
    expect(csp).not.toMatch(/https?:/);
    const html = await res.text();
    expect(html).not.toContain('__');
    expect(html).toMatch(/docs\.js\?v=[0-9a-f]{12}"/);
    expect(html).toContain('data-scalar="/api/docs/scalar.js?v=1.76.0"');
  });

  it('serves its script, immutable', async () => {
    const res = await app.request('/api/docs/docs.js');
    expect(res.status).toBe(200);
    expect(res.headers.get('Content-Type')).toContain('javascript');
    expect(res.headers.get('Cache-Control')).toContain('immutable');
  });

  it('sends the viewer pre-compressed to a browser that takes br, and as-is otherwise', async () => {
    const raw = readFileSync(path.join(ASSETS, 'scalar/standalone.js'));
    const br = await app.request('/api/docs/scalar.js', { headers: { 'Accept-Encoding': 'gzip, deflate, br' } });
    expect(br.headers.get('Content-Encoding')).toBe('br');
    expect(brotliDecompressSync(Buffer.from(await br.arrayBuffer())).equals(raw)).toBe(true);
    const plain = await app.request('/api/docs/scalar.js', { headers: { 'Accept-Encoding': 'identity' } });
    expect(plain.headers.get('Content-Encoding')).toBeNull();
    expect(Buffer.from(await plain.arrayBuffer()).equals(raw)).toBe(true);
  });

  it('the vendored viewer is the one its README names', () => {
    const raw = readFileSync(path.join(ASSETS, 'scalar/standalone.js'));
    const readme = readFileSync(path.join(ASSETS, 'scalar/README.md'), 'utf8');
    expect(readme).toContain(createHash('sha256').update(raw).digest('hex'));
  });
});
