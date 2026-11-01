/**
 * The API description.
 *
 *  - The committed openapi.json is exactly what `npm run openapi` writes now.
 *  - Every /api/v1/ route is described, and says what credential it takes:
 *    public, or the ApiKey scheme with the scopes its scoped() declaration
 *    asks for.
 *  - GET /api/v1/openapi.json shows a key only what that key can read.
 *
 * The signed-in copy the site's reference page reads
 * (/api/api-keys/openapi.json) is tested in apiKeysOpenapi.test.ts.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
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
import { FEEDS, needFromScope } from '../../../src/services/auth/apiKeyScopes.js';

/** Every cell of the staff table. */
const EVERY_CELL = Object.entries(FEEDS).flatMap(([feed, f]) => f.windows.map((w) => `${feed}:${w}`));

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
        // One cell of the staff table, or `*:history` for stored history.
        expect(op['x-scopes'], p).toHaveLength(1);
        const scope = op['x-scopes'][0] as string;
        expect(scope === '*:history' || EVERY_CELL.includes(scope), `${p} ${scope}`).toBe(true);
        expect(needFromScope(scope), p).not.toBeNull();
        expect(Object.keys(op.responses), p).toEqual(expect.arrayContaining(['401', '403', '429']));
      }
    }
  });

  it('every fire service is one operation, under the fires feed', () => {
    const op = fresh.paths['/api/v1/fires']?.['get'];
    expect(op).toBeDefined();
    expect(op.tags).toEqual(['Fires']);
    expect(op['x-scopes']).toEqual(['fires:live']);
    expect(fresh.paths['/api/v1/data/history']?.['get']['x-scopes']).toEqual(['*:history']);
  });

  it('describes none of this site\'s own routes', () => {
    const paths = Object.keys(fresh.paths);
    for (const gone of [
      '/api/v1/rfs/incidents',
      '/api/v1/rfs/incidents/raw',
      '/api/v1/nt-fire/incidents',
      '/api/v1/qld-fire/incidents',
      '/api/v1/vic-emergency/events',
      '/api/v1/wa-emergency/incidents',
      '/api/v1/sa-fire/cfs',
      '/api/v1/sa-fire/mfs',
      '/api/v1/radio/grn-sites',
      '/api/v1/rdio/calls/{callId}',
    ]) {
      expect(paths, gone).not.toContain(gone);
    }
    expect(paths.filter((p) => p.startsWith('/api/v1/boundaries'))).toEqual([]);
  });

  it('one tag per feed: a feed\'s operations share a tag no other feed uses, and every tag is used', () => {
    const used = new Set(ops(fresh).flatMap((o) => o.op.tags as string[]));
    expect((fresh['tags'] as Array<{ name: string }>).map((t) => t.name).filter((t) => !used.has(t))).toEqual([]);
    const tagsOfFeed = new Map<string, Set<string>>();
    for (const { op } of ops(fresh)) {
      const feed = String(op['x-scopes']?.[0] ?? '').split(':')[0];
      if (!feed || feed === '*') continue;
      (tagsOfFeed.get(feed) ?? tagsOfFeed.set(feed, new Set()).get(feed)!).add(op.tags[0]);
    }
    expect([...tagsOfFeed.keys()].sort()).toEqual(Object.keys(FEEDS).sort());
    const owner = new Map<string, string>();
    for (const [feed, tags] of tagsOfFeed) {
      expect([...tags], feed).toHaveLength(1);
      const tag = [...tags][0]!;
      expect(owner.get(tag) ?? feed, `${tag} is used by ${owner.get(tag)} and ${feed}`).toBe(feed);
      owner.set(tag, feed);
    }
  });

  it('never names a host but the API itself', () => {
    expect(fresh['servers']).toEqual([{ url: 'https://api.forcequit.xyz' }]);
  });
});

describe('filterSpecForKey', () => {
  const FIRES_ONLY = ['fires:live'];

  it('a scoped key sees what its scopes reach, plus the public operations', () => {
    const seen = ops(filterSpecForKey(fresh, FIRES_ONLY)).map((o) => o.path);
    expect(seen).toContain('/api/v1/fires');
    expect(seen).toContain('/api/v1/rfs/fdr');
    expect(seen).toContain('/api/v1/firms/hotspots');
    expect(seen).not.toContain('/api/v1/qld-fire/warnings'); // another feed
    expect(seen).not.toContain('/api/v1/bom/warnings');
    expect(seen).not.toContain('/api/v1/data/history'); // history
    expect(seen).toContain('/api/v1/health');
    expect(seen).toContain('/api/v1/agency/extended/{slug}');
  });

  it('drops the tags and models the key has no operation for', () => {
    const doc = filterSpecForKey(fresh, FIRES_ONLY);
    const tags = (doc['tags'] as Array<{ name: string }>).map((t) => t.name);
    expect(tags).toContain('Fires');
    expect(tags).not.toContain('Warnings');
    expect(Object.keys(doc['components'].schemas)).toContain('Fires');
    expect(Object.keys(doc['components'].schemas)).not.toContain('BomWarnings');
    expect(doc['components'].schemas.Error).toBeDefined();
    expect(doc['components'].securitySchemes.ApiKey).toBeDefined();
  });

  it('cells read as the gate reads them: every cell sees everything, the live cells all but history', () => {
    expect(ops(filterSpecForKey(fresh, EVERY_CELL))).toHaveLength(ops(fresh).length);
    const live = ops(filterSpecForKey(fresh, EVERY_CELL.filter((s) => s.endsWith(':live')))).map((o) => o.path);
    const historyOps = ops(fresh).filter((o) => String(o.op['x-scopes']?.[0] ?? '').endsWith(':history'));
    expect(historyOps.map((o) => o.path)).toEqual(
      expect.arrayContaining(['/api/v1/adsb/history', '/api/v1/data/history', '/api/v1/summaries']),
    );
    expect(live).toHaveLength(ops(fresh).length - historyOps.length);
    expect(live).not.toContain('/api/v1/adsb/history');
    const history = ops(filterSpecForKey(fresh, EVERY_CELL.filter((s) => s.endsWith(':history')))).map((o) => o.path);
    expect(history).not.toContain('/api/v1/fires'); // live only
    expect(history).toContain('/api/v1/adsb/history');
  });

  it('stored history: the history cell of one archived feed is enough to see it, and nothing else of the feed\'s', () => {
    const seen = ops(filterSpecForKey(fresh, ['pager:history'])).map((o) => o.path);
    expect(seen).toContain('/api/v1/data/history');
    expect(seen).toContain('/api/v1/data/history/incident/{source}/{source_id}');
    expect(seen).not.toContain('/api/v1/adsb/history'); // another feed's history
    expect(seen).not.toContain('/api/v1/pager/hits'); // live
  });

  it('a `read` key sees the feeds on the old list, the merged fire feed among them', () => {
    const seen = ops(filterSpecForKey(fresh, ['read'])).map((o) => o.path);
    expect(seen).toContain('/api/v1/bom/warnings');
    expect(seen).toContain('/api/v1/traffic/incidents');
    expect(seen).toContain('/api/v1/rfs/fdr');
    expect(seen).toContain('/api/v1/fires');
    expect(seen).not.toContain('/api/v1/summaries');
    expect(seen).not.toContain('/api/v1/data/history');
  });

  it('stored history is not shown to a key whose history cells the archive does not hold', () => {
    const seen = ops(filterSpecForKey(fresh, ['aircraft:history', 'summaries:history'])).map((o) => o.path);
    expect(seen).not.toContain('/api/v1/data/history');
    expect(seen).toContain('/api/v1/adsb/history');
    expect(seen).toContain('/api/v1/summaries');
  });

  it('a key holding only retired scopes sees the public operations and nothing else', () => {
    const seen = ops(filterSpecForKey(fresh, ['source:rfs', 'type:incidents', 'window:live']));
    expect(seen.length).toBeGreaterThan(0);
    for (const o of seen) expect(o.op.security, o.path).toEqual([]);
  });

  it('leaves the document it was given alone', () => {
    const before = JSON.stringify(fresh);
    filterSpecForKey(fresh, FIRES_ONLY);
    expect(JSON.stringify(fresh)).toBe(before);
  });
});

describe('GET /api/v1/openapi.json', () => {
  const KEY = 'ak_abcdefghij_' + 'K'.repeat(32);
  let scopes: string[];
  beforeEach(() => {
    scopes = ['fires:live'];
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
    expect(Object.keys(doc.paths)).toContain('/api/v1/fires');
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
