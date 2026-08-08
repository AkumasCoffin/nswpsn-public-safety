/**
 * Radio Data-tab rollups (owner|dev only): talkgroups + radios paging.
 *
 * The staff Data tab now mounts Talkgroups/Radios INSIDE a system drill-down,
 * so the UI always passes ?system=<p25 systemId>. These tests mock the pg pool
 * and assert the endpoints thread that filter into the grouped WHERE (and stay
 * fleet-wide when it is omitted, for backward safety).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const queryMock = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));

// requireRole stays real; canManageNodes is stubbed true so the routes run.
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canManageNodes: vi.fn(() => Promise.resolve(true)) };
});

async function setupApp() {
  const { nodeDataRouter } = await import('../../../src/api/node-data.js');
  const app = new Hono();
  app.use('*', async (c, next) => {
    c.set('userId', 'u1');
    await next();
  });
  app.route('/', nodeDataRouter);
  return app;
}

// Capture every (sql, params) pair the handler runs. Empty page rows keep the
// enrich lateral from firing, so we only see the count + page queries.
type Call = { sql: string; params: unknown[] };
function captureCalls(): Call[] {
  const calls: Call[] = [];
  queryMock.mockImplementation((sql: string, params: unknown[] = []) => {
    calls.push({ sql, params });
    if (sql.includes('AS n')) return { rows: [{ n: 0 }] };
    return { rows: [] };
  });
  return calls;
}

beforeEach(() => {
  queryMock.mockReset();
});

describe('GET /api/node-data/talkgroups', () => {
  it('scopes the rollup to ?system=<id>', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, talkgroup'));
    expect(grouped).toBeDefined();
    // $1 is the window interval, so the system predicate binds $2 = 721.
    expect(grouped?.sql).toContain('system = $2');
    expect(grouped?.params).toContain(721);
  });

  it('stays fleet-wide with no system param', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, talkgroup'));
    expect(grouped?.sql).not.toContain('system = $');
  });

  it('rejects a non-numeric system with 400', async () => {
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?system=abc');
    expect(res.status).toBe(400);
  });
});

describe('GET /api/node-data/radios', () => {
  it('scopes the rollup to ?system=<id>', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/radios?window=7d&system=721');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, source_unit'));
    expect(grouped).toBeDefined();
    expect(grouped?.sql).toContain('system = $2');
    expect(grouped?.params).toContain(721);
  });

  it('stays fleet-wide with no system param', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/radios?window=7d');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, source_unit'));
    expect(grouped?.sql).not.toContain('system = $');
  });
});
