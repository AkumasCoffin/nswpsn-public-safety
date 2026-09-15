/**
 * Referral-code router tests — GET /api/referral-code. Mocks getPool() and
 * intercepts SQL (same harness as editor.test.ts); keeps requireRole real and
 * stubs the DB-backed canRefer so the gate can be flipped per test.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
const calls: Call[] = [];
let resultQueue: Array<{ rows: unknown[] }> = [];
let getPoolReturn: 'pool' | 'null' = 'pool';

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return resultQueue.shift() ?? { rows: [] };
  }),
};

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => (getPoolReturn === 'pool' ? fakePool : null)),
}));

vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canRefer: vi.fn(async () => true) };
});

const { referralsRouter, generateReferralCode } = await import('../../../src/api/referrals.js');
const roles = await import('../../../src/services/auth/roles.js');

function makeApp(opts: { authed?: boolean } = {}) {
  const app = new Hono();
  if (opts.authed !== false) {
    app.use('*', async (c, next) => {
      c.set('userId', 'contrib-1');
      await next();
    });
  }
  app.route('/', referralsRouter);
  return app;
}

beforeEach(() => {
  calls.length = 0;
  resultQueue = [];
  getPoolReturn = 'pool';
  fakePool.query.mockClear();
  vi.mocked(roles.canRefer).mockResolvedValue(true);
});

describe('GET /api/referral-code', () => {
  it('401 without a verified user', async () => {
    const res = await makeApp({ authed: false }).request('/api/referral-code');
    expect(res.status).toBe(401);
  });

  it('403 when the user holds no referring role', async () => {
    vi.mocked(roles.canRefer).mockResolvedValue(false);
    const res = await makeApp().request('/api/referral-code');
    expect(res.status).toBe(403);
  });

  it('returns the existing code with usage stats', async () => {
    resultQueue = [{ rows: [{ code: 'ABCD2345' }] }, { rows: [{ uses: 3, approved: 1 }] }];
    const res = await makeApp().request('/api/referral-code');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ code: 'ABCD2345', uses: 3, approved: 1 });
    expect(calls[0]?.params).toEqual(['contrib-1']);
    expect(calls[1]?.sql).toContain("FILTER (WHERE status = 'approved')");
  });

  it('mints a code on first call', async () => {
    resultQueue = [{ rows: [] }, { rows: [{ code: 'NEWCODE2' }] }, { rows: [{ uses: 0, approved: 0 }] }];
    const res = await makeApp().request('/api/referral-code');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ code: 'NEWCODE2', uses: 0, approved: 0 });
    expect(calls[1]?.sql).toContain('ON CONFLICT (user_id)');
    const generated = (calls[1]?.params ?? [])[1] as string;
    expect(generated).toMatch(/^[A-Z0-9]{8}$/);
    expect(generated).not.toMatch(/[0O1IL]/); // unambiguous alphabet
  });

  it('retries when a generated code collides', async () => {
    fakePool.query
      .mockImplementationOnce(async (sql: string, params?: unknown[]) => {
        calls.push({ sql, ...(params ? { params } : {}) });
        return { rows: [] }; // no existing code
      })
      .mockImplementationOnce(async (sql: string, params?: unknown[]) => {
        calls.push({ sql, ...(params ? { params } : {}) });
        throw new Error('duplicate key value violates unique constraint');
      });
    resultQueue = [{ rows: [{ code: 'SECOND22' }] }, { rows: [{ uses: 0, approved: 0 }] }];
    const res = await makeApp().request('/api/referral-code');
    expect(res.status).toBe(200);
    expect((await res.json()).code).toBe('SECOND22');
  });

  it('503 when the database is unavailable', async () => {
    getPoolReturn = 'null';
    const res = await makeApp().request('/api/referral-code');
    expect(res.status).toBe(503);
  });
});

describe('generateReferralCode', () => {
  it('draws 8 chars from the unambiguous alphabet', () => {
    for (let i = 0; i < 200; i++) {
      const code = generateReferralCode();
      expect(code).toMatch(/^[ABCDEFGHJKMNPQRSTUVWXYZ23456789]{8}$/);
    }
  });
});
