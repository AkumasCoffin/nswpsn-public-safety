/**
 * Comments / likes / restrictions API.
 *
 * The security-relevant guarantees pinned here:
 *   - an ACTIVE restriction blocks commenting, an EXPIRED one does not
 *   - only a known reason key can be stored (the label shown to a restricted
 *     user must never be attacker-supplied text)
 *   - a third party can't delete someone else's comment
 *   - a user can't restrict themselves, and can't mark someone else's
 *     notifications read
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
let calls: Call[] = [];
let resultQueue: Array<{ rows: unknown[]; rowCount?: number }> = [];
let nextResult: { rows: unknown[]; rowCount?: number } = { rows: [], rowCount: 0 };
let getPoolReturn: 'pool' | 'null' = 'pool';

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, params });
    const q = resultQueue.shift();
    return q ?? nextResult;
  }),
};
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => (getPoolReturn === 'pool' ? fakePool : null)),
}));

// Role helpers: canModerateWire is toggled per test.
let moderatorMock = false;
vi.mock('../../../src/services/auth/roles.js', async (orig) => ({
  ...(await (orig as () => Promise<Record<string, unknown>>)()),
  canModerateWire: vi.fn(async () => moderatorMock),
  isOwner: vi.fn(async () => true), // satisfies the soft-launch read gate
  requireRole: () => async (c: { get: (k: string) => unknown; json: (b: unknown, s?: number) => unknown }, next: () => Promise<void>) => {
    if (!moderatorMock) return c.json({ error: 'forbidden' }, 403);
    await next();
  },
}));
// requireSupabaseJwt: accept whatever userId the test app injected.
vi.mock('../../../src/services/auth/supabaseJwt.js', () => ({
  requireSupabaseJwt: async (c: { get: (k: string) => unknown; json: (b: unknown, s?: number) => unknown }, next: () => Promise<void>) => {
    if (!c.get('userId')) return c.json({ error: 'authentication required' }, 401);
    await next();
  },
  optionalSupabaseJwt: async (_c: unknown, next: () => Promise<void>) => { await next(); },
}));

const { wireCommentsRouter } = await import('../../../src/api/wireComments.js');

function makeApp(userId: string | null = 'user-1') {
  const app = new Hono();
  app.use('*', async (c, next) => {
    if (userId) { c.set('userId', userId); c.set('userName', 'Tester'); }
    await next();
  });
  app.route('/', wireCommentsRouter);
  return app;
}
const post = (app: ReturnType<typeof makeApp>, path: string, body: unknown) =>
  app.request(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });

beforeEach(() => {
  calls = []; resultQueue = []; nextResult = { rows: [], rowCount: 0 };
  getPoolReturn = 'pool'; moderatorMock = false;
  fakePool.query.mockClear();
});

describe('POST /api/wire/comments — restriction gate', () => {
  it('403s while a timeout is ACTIVE, and reports the reason', async () => {
    resultQueue = [
      // activeRestriction lookup
      { rows: [{ id: '5', kind: 'timeout', reason: 'spam', note: null, expires_at: new Date(Date.now() + 3600_000) }], rowCount: 1 },
    ];
    const res = await post(makeApp(), '/api/wire/comments', { type: 'media', id: 'p1', body: 'hi' });
    expect(res.status).toBe(403);
    const body = (await res.json()) as Record<string, any>;
    expect(body['restriction'].kind).toBe('timeout');
    expect(body['restriction'].reasonLabel).toBe('Spam');
    // Must NOT have inserted a comment.
    expect(calls.some((c) => c.sql.includes('INSERT INTO wire_comments'))).toBe(false);
  });

  it('allows commenting when the restriction has EXPIRED (no active row)', async () => {
    resultQueue = [
      { rows: [], rowCount: 0 },                                              // no active restriction
      { rows: [{ author_id: 'author-9', co_authors: [], title: 'T' }], rowCount: 1 }, // loadParent
      { rows: [{ id: 'c1', created_at: new Date() }], rowCount: 1 },          // insert
    ];
    const res = await post(makeApp(), '/api/wire/comments', { type: 'media', id: 'p1', body: 'hello' });
    expect(res.status).toBe(201);
    expect(calls.some((c) => c.sql.includes('INSERT INTO wire_comments'))).toBe(true);
  });

  it('rejects an empty body', async () => {
    const res = await post(makeApp(), '/api/wire/comments', { type: 'media', id: 'p1', body: '   ' });
    expect(res.status).toBe(400);
  });

  it('401s when not logged in', async () => {
    const res = await post(makeApp(null), '/api/wire/comments', { type: 'media', id: 'p1', body: 'x' });
    expect(res.status).toBe(401);
  });
});

describe('DELETE /api/wire/comments/:id', () => {
  it('lets the AUTHOR delete their own comment', async () => {
    resultQueue = [{ rows: [{ author_id: 'user-1', parent_type: 'media_post', parent_id: 'p1', deleted_at: null }], rowCount: 1 }];
    const res = await makeApp().request('/api/wire/comments/c1', { method: 'DELETE' });
    expect(res.status).toBe(200);
    expect(calls.some((c) => c.sql.includes('SET deleted_at = now()'))).toBe(true);
  });

  it('refuses a third party who is not a moderator', async () => {
    moderatorMock = false;
    resultQueue = [{ rows: [{ author_id: 'someone-else', parent_type: 'media_post', parent_id: 'p1', deleted_at: null }], rowCount: 1 }];
    const res = await makeApp().request('/api/wire/comments/c1', { method: 'DELETE' });
    expect(res.status).toBe(403);
    expect(calls.some((c) => c.sql.includes('SET deleted_at = now()'))).toBe(false);
  });

  it('lets a MODERATOR delete and notifies the author', async () => {
    moderatorMock = true;
    resultQueue = [{ rows: [{ author_id: 'someone-else', parent_type: 'media_post', parent_id: 'p1', deleted_at: null }], rowCount: 1 }];
    const res = await makeApp().request('/api/wire/comments/c1', {
      method: 'DELETE', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ reason: 'spam' }),
    });
    expect(res.status).toBe(200);
    expect(calls.some((c) => c.sql.includes('INSERT INTO notifications'))).toBe(true);
  });
});

describe('POST /api/wire/comment-restrictions', () => {
  it('rejects an unknown reason key', async () => {
    moderatorMock = true;
    const res = await post(makeApp(), '/api/wire/comment-restrictions', {
      userId: 'u2', kind: 'timeout', hours: 1, reason: '<script>evil</script>',
    });
    expect(res.status).toBe(400);
    expect(calls.some((c) => c.sql.includes('INSERT INTO comment_restrictions'))).toBe(false);
  });

  it('rejects a timeout duration outside the offered set', async () => {
    moderatorMock = true;
    const res = await post(makeApp(), '/api/wire/comment-restrictions', {
      userId: 'u2', kind: 'timeout', hours: 999, reason: 'spam',
    });
    expect(res.status).toBe(400);
  });

  it('refuses restricting yourself', async () => {
    moderatorMock = true;
    const res = await post(makeApp(), '/api/wire/comment-restrictions', {
      userId: 'user-1', kind: 'pause', reason: 'spam',
    });
    expect(res.status).toBe(400);
  });

  it('creates a pause with no expiry and notifies the user', async () => {
    moderatorMock = true;
    resultQueue = [{ rows: [{ id: '7' }], rowCount: 1 }];
    const res = await post(makeApp(), '/api/wire/comment-restrictions', {
      userId: 'u2', kind: 'pause', reason: 'harassment',
    });
    expect(res.status).toBe(201);
    expect((await res.json() as Record<string, unknown>)['expiresAt']).toBeNull();
    const ins = calls.find((c) => c.sql.includes('INSERT INTO comment_restrictions'));
    expect(ins?.params?.[4]).toBeNull(); // expires_at
    expect(calls.some((c) => c.sql.includes('INSERT INTO notifications'))).toBe(true);
  });

  it('403s for a non-moderator', async () => {
    moderatorMock = false;
    const res = await post(makeApp(), '/api/wire/comment-restrictions', {
      userId: 'u2', kind: 'pause', reason: 'spam',
    });
    expect(res.status).toBe(403);
  });
});

describe('POST /api/wire/like — toggle', () => {
  it('likes when no row existed', async () => {
    resultQueue = [
      { rows: [{ author_id: 'author-9', co_authors: [], title: 'T' }], rowCount: 1 }, // loadParent
      { rows: [{ user_id: 'user-1' }], rowCount: 1 },                                 // insert won
      { rows: [{ n: 1 }], rowCount: 1 },                                              // count
    ];
    const res = await post(makeApp(), '/api/wire/like', { type: 'media', id: 'p1' });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ liked: true, count: 1 });
  });

  it('UNlikes when the row already existed (idempotent per user)', async () => {
    resultQueue = [
      { rows: [{ author_id: 'author-9', co_authors: [], title: 'T' }], rowCount: 1 },
      { rows: [], rowCount: 0 },        // ON CONFLICT DO NOTHING → already liked
      { rows: [{ n: 0 }], rowCount: 1 },
    ];
    const res = await post(makeApp(), '/api/wire/like', { type: 'media', id: 'p1' });
    expect(await res.json()).toMatchObject({ liked: false, count: 0 });
    expect(calls.some((c) => c.sql.includes('DELETE FROM wire_likes'))).toBe(true);
  });
});

describe('POST /api/notifications/read', () => {
  it('always scopes the update to the caller', async () => {
    await post(makeApp(), '/api/notifications/read', { ids: [1, 2] });
    const upd = calls.find((c) => c.sql.includes('UPDATE notifications'));
    expect(upd?.sql).toContain('user_id = $1');
    expect(upd?.params?.[0]).toBe('user-1');
  });
});
