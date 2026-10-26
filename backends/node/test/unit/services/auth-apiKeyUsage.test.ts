/**
 * Per-key usage counting (apiKeyUsage.ts): what a path is recorded as, the
 * per-hour path cap that stops a scan growing the table, the once-a-minute
 * flush (one upsert + the lifetime total, dropped rather than doubled on
 * failure), and that only requests the gate attributed are counted.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
const calls: Call[] = [];
let failOn: RegExp | null = null;
const client = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    if (failOn && failOn.test(sql)) throw new Error('db down');
    return { rows: [] };
  }),
  release: vi.fn(),
};
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => null),
  getWriterPool: vi.fn(async () => ({ connect: async () => client })),
  closePool: vi.fn(),
}));
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

const {
  normaliseUsagePath,
  recordApiKeyUse,
  flushApiKeyUsage,
  apiKeyUsage,
  _resetApiKeyUsage,
  _pendingApiKeyUsage,
  MAX_PATHS_PER_KEY_HOUR,
  OTHER_PATHS,
} = await import('../../../src/services/auth/apiKeyUsage.js');

const K = '0b6e1f6a-6d0c-4b8e-9a51-2f3c4d5e6f70';
const K2 = '1c7f2a7b-7e1d-4c9f-8b62-3a4d5e6f7a81';
const T = Date.UTC(2026, 9, 27, 3, 15); // 03:15Z
const HOUR = Date.UTC(2026, 9, 27, 3);

beforeEach(() => {
  _resetApiKeyUsage();
  calls.length = 0;
  failOn = null;
});

describe('normaliseUsagePath', () => {
  it('folds identifiers so one route is one row', () => {
    expect(normaliseUsagePath('GET', '/api/rfs/incidents')).toBe('/api/rfs/incidents');
    expect(normaliseUsagePath('GET', '/api/rfs/fire/123456')).toBe('/api/rfs/fire/:id');
    expect(normaliseUsagePath('GET', '/api/adsb/aircraft/7c6b2d1a')).toBe('/api/adsb/aircraft/:id');
    expect(normaliseUsagePath('GET', '/api/x/0b6e1f6a-6d0c-4b8e-9a51-2f3c4d5e6f70/y')).toBe('/api/x/:id/y');
    expect(normaliseUsagePath('GET', '/api/x/' + 'a'.repeat(30))).toBe('/api/x/:id');
    // Short words and short numbers that are part of a route survive.
    expect(normaliseUsagePath('GET', '/api/weather/v1/nsw')).toBe('/api/weather/v1/nsw');
  });

  it('keeps a write attempt visible as one, and caps the length', () => {
    expect(normaliseUsagePath('POST', '/api/rfs/incidents')).toBe('POST /api/rfs/incidents');
    expect(normaliseUsagePath('HEAD', '/api/rfs/incidents')).toBe('/api/rfs/incidents');
    expect(normaliseUsagePath('GET', '/api/' + 'abc/'.repeat(60)).length).toBe(120);
  });
});

describe('recordApiKeyUse', () => {
  it('tallies by key, hour, path and status', () => {
    recordApiKeyUse(K, 'GET', '/api/rfs/incidents', 200, T);
    recordApiKeyUse(K, 'GET', '/api/rfs/incidents', 200, T + 60_000);
    recordApiKeyUse(K, 'GET', '/api/rfs/incidents', 404, T);
    recordApiKeyUse(K2, 'GET', '/api/rfs/incidents', 200, T);
    const p = _pendingApiKeyUsage();
    expect(p).toContainEqual({ keyId: K, hour: HOUR, path: '/api/rfs/incidents', status: 200, requests: 2 });
    expect(p).toContainEqual({ keyId: K, hour: HOUR, path: '/api/rfs/incidents', status: 404, requests: 1 });
    expect(p).toContainEqual({ keyId: K2, hour: HOUR, path: '/api/rfs/incidents', status: 200, requests: 1 });
  });

  it('folds paths past the per-hour cap into one row, and the cap outlives a flush within the hour', async () => {
    for (let i = 0; i < MAX_PATHS_PER_KEY_HOUR; i++) recordApiKeyUse(K, 'GET', `/api/rfs/p${i}`, 404, T);
    recordApiKeyUse(K, 'GET', '/api/rfs/one-too-many', 404, T);
    // A path already seen this hour still counts under its own name.
    recordApiKeyUse(K, 'GET', '/api/rfs/p0', 404, T);
    let p = _pendingApiKeyUsage();
    expect(p.find((r) => r.path === OTHER_PATHS)?.requests).toBe(1);
    expect(p.find((r) => r.path === '/api/rfs/p0')?.requests).toBe(2);

    await flushApiKeyUsage(T + 30_000);
    recordApiKeyUse(K, 'GET', '/api/rfs/another', 404, T + 60_000);
    p = _pendingApiKeyUsage();
    expect(p).toEqual([{ keyId: K, hour: HOUR, path: OTHER_PATHS, status: 404, requests: 1 }]);

    // Next hour: a clean slate.
    await flushApiKeyUsage(HOUR + 3_600_000 + 1);
    recordApiKeyUse(K, 'GET', '/api/rfs/another', 404, HOUR + 3_600_000 + 1);
    expect(_pendingApiKeyUsage()[0]!.path).toBe('/api/rfs/another');
  });

  it('a path containing a pipe survives the round trip', () => {
    recordApiKeyUse(K, 'GET', '/api/rfs/a|b', 200, T);
    expect(_pendingApiKeyUsage()[0]!.path).toBe('/api/rfs/a|b');
  });
});

describe('flushApiKeyUsage', () => {
  it('writes one upsert and one lifetime-total update in a transaction, then empties the tally', async () => {
    recordApiKeyUse(K, 'GET', '/api/rfs/incidents', 200, T);
    recordApiKeyUse(K, 'GET', '/api/rfs/incidents', 200, T);
    recordApiKeyUse(K, 'GET', '/api/rfs/nope', 404, T);
    recordApiKeyUse(K2, 'GET', '/api/rfs/incidents', 429, T);
    await flushApiKeyUsage(T);

    expect(calls.map((c) => c.sql.trim().split(/\s+/)[0])).toEqual(['BEGIN', 'INSERT', 'UPDATE', 'COMMIT']);
    const ins = calls[1]!;
    expect(ins.sql).toContain('ON CONFLICT (key_id, hour, path, status) DO UPDATE');
    expect(ins.sql).toContain('api_key_usage_hourly.requests + EXCLUDED.requests');
    const [ids, hours, paths, statuses, counts] = ins.params as [string[], string[], string[], number[], number[]];
    expect(ids).toHaveLength(3);
    expect(hours.every((h) => h === new Date(HOUR).toISOString())).toBe(true);
    const rows = ids.map((id, i) => [id, paths[i], statuses[i], counts[i]]);
    expect(rows).toContainEqual([K, '/api/rfs/incidents', 200, 2]);
    expect(rows).toContainEqual([K, '/api/rfs/nope', 404, 1]);
    expect(rows).toContainEqual([K2, '/api/rfs/incidents', 429, 1]);

    const tot = calls[2]!;
    expect(tot.sql).toContain('requests_total = k.requests_total + t.n');
    const [tIds, tN] = tot.params as [string[], number[]];
    expect(Object.fromEntries(tIds.map((id, i) => [id, tN[i]]))).toEqual({ [K]: 3, [K2]: 1 });
    expect(_pendingApiKeyUsage()).toEqual([]);
    expect(client.release).toHaveBeenCalled();
  });

  it('does nothing when nothing was counted', async () => {
    await flushApiKeyUsage(T);
    expect(calls).toHaveLength(0);
  });

  it('a failed write rolls back and drops the minute rather than counting it twice', async () => {
    recordApiKeyUse(K, 'GET', '/api/rfs/incidents', 200, T);
    failOn = /^\s*INSERT/;
    await flushApiKeyUsage(T);
    expect(calls.map((c) => c.sql.trim().split(/\s+/)[0])).toEqual(['BEGIN', 'INSERT', 'ROLLBACK']);
    failOn = null;
    calls.length = 0;
    await flushApiKeyUsage(T);
    expect(calls).toHaveLength(0);
  });
});

describe('apiKeyUsage middleware', () => {
  it('counts only requests the gate attributed, with the status they finally got', async () => {
    const app = new Hono();
    app.use('*', apiKeyUsage);
    app.use('*', async (c, next) => {
      if (c.req.header('x-key')) c.set('apiKeyUsageId', c.req.header('x-key')!);
      await next();
    });
    app.get('/api/rfs/incidents', (c) => c.json({ ok: true }));
    app.get('/api/rfs/refused', (c) => c.json({ error: 'no' }, 403));

    await app.request('/api/rfs/incidents', { headers: { 'x-key': K } });
    await app.request('/api/rfs/refused', { headers: { 'x-key': K } });
    await app.request('/api/rfs/missing', { headers: { 'x-key': K } });
    await app.request('/api/rfs/incidents'); // not attributed: no key matched
    const p = _pendingApiKeyUsage().map((r) => [r.path, r.status, r.requests]);
    expect(p).toHaveLength(3);
    expect(p).toContainEqual(['/api/rfs/incidents', 200, 1]);
    expect(p).toContainEqual(['/api/rfs/refused', 403, 1]);
    expect(p).toContainEqual(['/api/rfs/missing', 404, 1]);
  });
});
