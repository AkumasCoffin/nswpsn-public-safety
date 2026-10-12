/**
 * API key request flow — who may do what, the caps, the claim transaction
 * (plaintext once, never stored, never to staff), the approve/reject claim
 * race, and the notification matrix (Discord on request, bell on decision).
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
const calls: Call[] = [];
// A queued { error } entry makes that query throw instead — how a unique
// violation or a dead connection is simulated.
let resultQueue: Array<{ rows: unknown[]; rowCount?: number; error?: Error }> = [];

function next() {
  const r = resultQueue.shift() ?? { rows: [] };
  if (r.error) throw r.error;
  return r;
}
// Transaction bookends and the per-user advisory lock return nothing and
// consume nothing from the queue, so tests only script the data queries.
const PLUMBING = /^(BEGIN|COMMIT|ROLLBACK)|pg_advisory_xact_lock/;

const fakeClient = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    if (PLUMBING.test(sql)) return { rows: [] };
    return next();
  }),
  release: vi.fn(),
};
const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    const r = next();
    return { rowCount: r.rows.length, ...r };
  }),
  connect: vi.fn(async () => fakeClient),
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

const bells: Array<Record<string, unknown>> = [];
vi.mock('../../../src/services/wireComments.js', () => ({
  notify: vi.fn(async (_pool: unknown, n: Record<string, unknown>) => { bells.push(n); }),
  displayNameMap: vi.fn(async () => new Map<string, string>([['user-1', 'Current Name']])),
}));

const discord: Array<Record<string, unknown>> = [];
vi.mock('../../../src/services/staffNotify.js', () => ({
  notifyStaff: (_pool: unknown, input: Record<string, unknown>) => { discord.push(input); },
}));

vi.mock('../../../src/api/users.js', () => ({
  getUsername: vi.fn(async () => 'Staff Person'),
}));

const { apiKeysRouter, MAX_ACTIVE_KEYS_PER_USER } = await import('../../../src/api/apiKeys.js');

function makeApp(opts: { userId?: string | null; userName?: string | null } = {}) {
  const app = new Hono();
  app.use('*', async (c, next) => {
    if (opts.userId !== null) {
      c.set('userId', opts.userId ?? 'user-1');
      if (opts.userName !== null) c.set('userName', opts.userName ?? 'Test User');
    }
    await next();
  });
  app.route('/', apiKeysRouter);
  return app;
}

const json = (body: unknown) => ({
  method: 'POST' as const,
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(body),
});

const REQUEST = (over: Record<string, unknown> = {}) => ({
  id: 5, user_id: 'user-1', user_name: 'Test User', name: 'home-assistant', reason: 'Pull incidents into my dashboard',
  status: 'pending', rate_limit_per_min: null, expires_at: null, review_note: null,
  reviewed_by: null, reviewed_by_name: null, reviewed_at: null, key_id: null, claimed_at: null, created_at: 't', ...over,
});

const KEY = (over: Record<string, unknown> = {}) => ({
  id: 'k-uuid', user_id: 'user-1', name: 'home-assistant', prefix: 'abcdefghij', key_hash: 'deadbeef',
  scopes: ['read'], rate_limit_per_min: 120, created_at: 't', last_used_at: null, expires_at: null, revoked_at: null, ...over,
});

beforeEach(() => {
  calls.length = 0;
  resultQueue = [];
  bells.length = 0;
  discord.length = 0;
  staff = false;
});

describe('user routes', () => {
  it('401 without a session', async () => {
    expect((await makeApp({ userId: null }).request('/api/api-keys/me')).status).toBe(401);
    expect((await makeApp({ userId: null }).request('/api/api-keys/requests', json({}))).status).toBe(401);
  });

  it('GET /me never leaks a hash and reports whether a request may be made', async () => {
    resultQueue = [
      { rows: [REQUEST({ status: 'approved', key_id: 'k-uuid' })] },
      { rows: [KEY(), KEY({ prefix: 'zzzzzzzzzz', revoked_at: 'r' })] },
    ];
    const res = await makeApp().request('/api/api-keys/me');
    expect(res.status).toBe(200);
    const body = await res.json() as Record<string, unknown>;
    expect(JSON.stringify(body)).not.toContain('deadbeef');
    expect(body.canRequest).toBe(true);
    expect(body.limits).toEqual({ maxActiveKeys: MAX_ACTIVE_KEYS_PER_USER });
    const keys = body.keys as Array<Record<string, unknown>>;
    expect(keys[0]).toMatchObject({ prefix: 'abcdefghij', active: true });
    expect(keys[1]).toMatchObject({ active: false });
    expect(keys[0]).not.toHaveProperty('key_hash');
    expect(keys[0]).not.toHaveProperty('userId');
    expect((body.requests as Array<Record<string, unknown>>)[0]).toMatchObject({ claimed: true });
  });

  it('GET /me says no new request while one is pending or approved-but-ungenerated', async () => {
    resultQueue = [{ rows: [REQUEST()] }, { rows: [] }];
    let body = await (await makeApp().request('/api/api-keys/me')).json() as Record<string, unknown>;
    expect(body.canRequest).toBe(false);

    resultQueue = [{ rows: [REQUEST({ status: 'approved' })] }, { rows: [] }];
    body = await (await makeApp().request('/api/api-keys/me')).json() as Record<string, unknown>;
    expect(body.canRequest).toBe(false);
    expect((body.requests as Array<Record<string, unknown>>)[0]).toMatchObject({ lapsed: false });
  });

  it('GET /me: a lapsed grant (approved, ungenerated, expiry passed) no longer blocks and is flagged', async () => {
    const past = new Date(Date.now() - 1000).toISOString();
    resultQueue = [{ rows: [REQUEST({ status: 'approved', expires_at: past })] }, { rows: [] }];
    const body = await (await makeApp().request('/api/api-keys/me')).json() as Record<string, unknown>;
    expect(body.canRequest).toBe(true);
    expect((body.requests as Array<Record<string, unknown>>)[0]).toMatchObject({ lapsed: true, claimed: false });
  });

  it('rejects a short name or reason', async () => {
    const app = makeApp();
    expect((await app.request('/api/api-keys/requests', json({ name: 'x', reason: 'long enough reason' }))).status).toBe(400);
    expect((await app.request('/api/api-keys/requests', json({ name: 'fine', reason: 'short' }))).status).toBe(400);
    expect((await app.request('/api/api-keys/requests', json({ name: 'x'.repeat(61), reason: 'long enough reason' }))).status).toBe(400);
    expect(calls).toHaveLength(0);
  });

  it('409 when a request is already open; 409 at the live-key cap — both rolled back', async () => {
    resultQueue = [{ rows: [{ n: '1' }] }];
    expect((await makeApp().request('/api/api-keys/requests', json({ name: 'fine', reason: 'long enough reason' }))).status).toBe(409);
    expect(calls.some((q) => q.sql.startsWith('ROLLBACK'))).toBe(true);
    // The open-request count is the SQL twin of the /me rule: pending OR approved-and-ungenerated.
    const open = calls.find((q) => q.sql.includes('FROM api_key_requests WHERE user_id'));
    expect(open?.sql).toContain("status = 'approved' AND key_id IS NULL");

    calls.length = 0;
    resultQueue = [{ rows: [{ n: '0' }] }, { rows: [{ n: String(MAX_ACTIVE_KEYS_PER_USER) }] }];
    expect((await makeApp().request('/api/api-keys/requests', json({ name: 'fine', reason: 'long enough reason' }))).status).toBe(409);
    expect(calls.some((q) => q.sql.startsWith('COMMIT'))).toBe(false);
    expect(discord).toHaveLength(0);
  });

  it('creates a pending request under the per-user lock and alerts Discord with the display name only', async () => {
    resultQueue = [{ rows: [{ n: '0' }] }, { rows: [{ n: '0' }] }, { rows: [REQUEST()] }];
    const res = await makeApp().request('/api/api-keys/requests', json({ name: 'home-assistant', reason: 'Pull incidents into my dashboard' }));
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ ok: true, request: { id: 5, status: 'pending', claimed: false } });
    const order = calls.map((q) => q.sql.split(/\s+/)[0]);
    expect(order[0]).toBe('BEGIN');
    expect(calls[1]!.sql).toContain('pg_advisory_xact_lock');
    expect(calls[1]!.params).toEqual(['api-keys:user-1']);
    expect(order[order.length - 1]).toBe('COMMIT');
    const ins = calls.find((q) => q.sql.includes('INSERT INTO api_key_requests'));
    expect(ins?.params).toEqual(['user-1', 'Test User', 'home-assistant', 'Pull incidents into my dashboard']);
    expect(discord).toHaveLength(1);
    expect(discord[0]).toMatchObject({ kind: 'api_key_request', event: 'new', ref: '5' });
    const fields = discord[0]!.fields as Array<{ name: string; value: string }>;
    expect(fields.find((f) => f.name === 'From')?.value).toBe('Test User');
    expect(JSON.stringify(discord[0])).not.toContain('user-1');
    expect(bells).toHaveLength(0);
  });

  it('a nameless account is identified to Discord by an id fragment, never the whole id', async () => {
    resultQueue = [{ rows: [{ n: '0' }] }, { rows: [{ n: '0' }] }, { rows: [REQUEST({ user_id: 'abcdefgh-1234-5678' })] }];
    const res = await makeApp({ userId: 'abcdefgh-1234-5678', userName: null }).request(
      '/api/api-keys/requests', json({ name: 'home-assistant', reason: 'Pull incidents into my dashboard' }));
    expect(res.status).toBe(200);
    const fields = discord[0]!.fields as Array<{ name: string; value: string }>;
    expect(fields.find((f) => f.name === 'From')?.value).toBe('abcdefgh');
    expect(JSON.stringify(discord[0])).not.toContain('abcdefgh-1234-5678');
  });

  it('a unique violation on the pending index is reported as the same 409, not a 500', async () => {
    const dup = Object.assign(new Error('duplicate key value violates unique constraint'), { code: '23505' });
    resultQueue = [{ rows: [{ n: '0' }] }, { rows: [{ n: '0' }] }, { rows: [], error: dup }];
    const res = await makeApp().request('/api/api-keys/requests', json({ name: 'fine', reason: 'long enough reason' }));
    expect(res.status).toBe(409);
    expect(calls.some((q) => q.sql.startsWith('ROLLBACK'))).toBe(true);
    expect(discord).toHaveLength(0);
  });

  describe('claim', () => {
    it('404 for a request that is not yours (or does not exist)', async () => {
      resultQueue = [{ rows: [] }];
      const res = await makeApp().request('/api/api-keys/requests/5/claim', { method: 'POST' });
      expect(res.status).toBe(404);
      expect(calls.some((q) => q.sql.startsWith('ROLLBACK'))).toBe(true);
      const sel = calls.find((q) => q.sql.includes('FOR UPDATE'));
      expect(sel?.params).toEqual([5, 'user-1']);
    });

    it('409 while pending, 409 when rejected, 409 once already claimed', async () => {
      const app = makeApp();
      resultQueue = [{ rows: [REQUEST({ status: 'pending' })] }];
      expect((await app.request('/api/api-keys/requests/5/claim', { method: 'POST' })).status).toBe(409);
      resultQueue = [{ rows: [REQUEST({ status: 'rejected' })] }];
      expect((await app.request('/api/api-keys/requests/5/claim', { method: 'POST' })).status).toBe(409);
      resultQueue = [{ rows: [REQUEST({ status: 'approved', key_id: 'k-uuid' })] }];
      const twice = await app.request('/api/api-keys/requests/5/claim', { method: 'POST' });
      expect(twice.status).toBe(409);
      expect(((await twice.json()) as { error: string }).error).toContain('only once');
      expect(calls.some((q) => q.sql.includes('INSERT INTO api_keys'))).toBe(false);
    });

    it('409 for a lapsed grant, and nothing is minted or consumed', async () => {
      const past = new Date(Date.now() - 60_000).toISOString();
      resultQueue = [{ rows: [REQUEST({ status: 'approved', expires_at: past })] }];
      const res = await makeApp().request('/api/api-keys/requests/5/claim', { method: 'POST' });
      expect(res.status).toBe(409);
      expect(((await res.json()) as { error: string }).error).toContain('lapsed');
      expect(calls.some((q) => q.sql.includes('INSERT INTO api_keys'))).toBe(false);
      expect(calls.some((q) => q.sql.includes('SET key_id'))).toBe(false);
      expect(calls.some((q) => q.sql.startsWith('ROLLBACK'))).toBe(true);
    });

    it('takes the per-user lock before reading the request', async () => {
      resultQueue = [{ rows: [] }];
      await makeApp().request('/api/api-keys/requests/5/claim', { method: 'POST' });
      expect(calls[0]!.sql).toBe('BEGIN');
      expect(calls[1]!.sql).toContain('pg_advisory_xact_lock');
      expect(calls[2]!.sql).toContain('FOR UPDATE');
    });

    it('409 at the cap even when approved', async () => {
      resultQueue = [
        { rows: [REQUEST({ status: 'approved' })] },
        { rows: [{ n: String(MAX_ACTIVE_KEYS_PER_USER) }] },
      ];
      expect((await makeApp().request('/api/api-keys/requests/5/claim', { method: 'POST' })).status).toBe(409);
      expect(calls.some((q) => q.sql.includes('INSERT INTO api_keys'))).toBe(false);
    });

    it('mints inside the transaction with the granted limit/expiry, links the key, returns the plaintext once', async () => {
      resultQueue = [
        { rows: [REQUEST({ status: 'approved', rate_limit_per_min: 30, expires_at: '2027-01-01T00:00:00.000Z' })] },
        { rows: [{ n: '0' }] },
        { rows: [KEY({ rate_limit_per_min: 30, expires_at: '2027-01-01T00:00:00.000Z' })] }, // INSERT api_keys RETURNING
        { rows: [] },                                                                      // link request
      ];
      const res = await makeApp().request('/api/api-keys/requests/5/claim', { method: 'POST' });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { plaintext: string; key: Record<string, unknown> };
      expect(body.plaintext).toMatch(/^ak_[A-Za-z0-9]{10}_[A-Za-z0-9_-]{32}$/);
      expect(body.key).not.toHaveProperty('key_hash');

      const ins = calls.find((q) => q.sql.includes('INSERT INTO api_keys'));
      expect(ins).toBeDefined();
      // userId, name, prefix, hash, scopes, rpm, expires
      expect(ins!.params![0]).toBe('user-1');
      expect(ins!.params![1]).toBe('home-assistant');
      expect(ins!.params![5]).toBe(30);
      expect(ins!.params![6]).toBeInstanceOf(Date);
      // The hash is sha256 of the plaintext, and the plaintext itself never reaches SQL.
      expect(String(ins!.params![3])).toMatch(/^[0-9a-f]{64}$/);
      expect(calls.some((q) => (q.params ?? []).includes(body.plaintext))).toBe(false);

      const link = calls.find((q) => q.sql.includes('SET key_id'));
      expect(link?.params).toEqual([5, 'k-uuid']);
      const order = calls.map((q) => q.sql.split(/\s+/)[0]);
      expect(order[0]).toBe('BEGIN');
      expect(order[order.length - 1]).toBe('COMMIT');
      expect(bells).toHaveLength(0);
      expect(discord).toHaveLength(0);
    });

    it('a failing mint rolls back and tells the user nothing was issued', async () => {
      resultQueue = [
        { rows: [REQUEST({ status: 'approved' })] },
        { rows: [{ n: '0' }] },
        { rows: [] }, // INSERT returns no row → createApiKey throws
      ];
      const res = await makeApp().request('/api/api-keys/requests/5/claim', { method: 'POST' });
      expect(res.status).toBe(500);
      expect(calls.some((q) => q.sql.startsWith('ROLLBACK'))).toBe(true);
      expect(calls.some((q) => q.sql.startsWith('COMMIT'))).toBe(false);
    });
  });

  it('self-revoke: only own live keys, prefix validated', async () => {
    const app = makeApp();
    expect((await app.request('/api/api-keys/not-a-prefix!/revoke', { method: 'POST' })).status).toBe(400);

    resultQueue = [{ rows: [] }];
    expect((await app.request('/api/api-keys/abcdefghij/revoke', { method: 'POST' })).status).toBe(404);
    const sel = calls.find((q) => q.sql.includes('FROM api_keys WHERE prefix'));
    expect(sel?.params).toEqual(['abcdefghij', 'user-1']);

    resultQueue = [{ rows: [{ revoked_at: 'r' }] }];
    expect((await app.request('/api/api-keys/abcdefghij/revoke', { method: 'POST' })).status).toBe(409);

    resultQueue = [{ rows: [{ revoked_at: null }] }, { rows: [], rowCount: 1 }];
    expect((await app.request('/api/api-keys/abcdefghij/revoke', { method: 'POST' })).status).toBe(200);
    expect(calls.some((q) => q.sql.includes('SET revoked_at = now()'))).toBe(true);
    expect(bells).toHaveLength(0);

    // Lost the race (staff revoked it between the read and the write): the
    // UPDATE touches nothing, and that is reported, not papered over.
    resultQueue = [{ rows: [{ revoked_at: null }] }, { rows: [], rowCount: 0 }];
    expect((await app.request('/api/api-keys/abcdefghij/revoke', { method: 'POST' })).status).toBe(409);
  });
});

describe('staff routes', () => {
  it('403 for a non-staff user on every staff route; 401 with no user', async () => {
    const app = makeApp();
    expect((await app.request('/api/staff/api-keys/requests')).status).toBe(403);
    expect((await app.request('/api/staff/api-keys')).status).toBe(403);
    expect((await app.request('/api/staff/api-keys/requests/5/approve', json({}))).status).toBe(403);
    expect((await app.request('/api/staff/api-keys/abcdefghij/revoke', { method: 'POST' })).status).toBe(403);
    expect((await makeApp({ userId: null }).request('/api/staff/api-keys/requests')).status).toBe(401);
    expect(calls).toHaveLength(0);
  });

  it('lists requests with the owner id + current name, defaulting to pending', async () => {
    staff = true;
    resultQueue = [{ rows: [REQUEST()] }, { rows: [{ n: 1 }] }];
    const res = await makeApp({ userId: 'staff-1' }).request('/api/staff/api-keys/requests');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { requests: Array<Record<string, unknown>>; pendingCount: number };
    expect(body.pendingCount).toBe(1);
    expect(body.requests[0]).toMatchObject({ id: 5, userId: 'user-1', userName: 'Current Name', status: 'pending' });
    expect(calls[0]!.params).toEqual(['pending']);

    resultQueue = [{ rows: [] }, { rows: [{ n: 0 }] }];
    await makeApp({ userId: 'staff-1' }).request('/api/staff/api-keys/requests?status=all');
    expect(calls.find((q) => q.sql.includes('FROM api_key_requests ORDER BY'))).toBeDefined();
  });

  it('approve validates limit / expiry / note', async () => {
    staff = true;
    const app = makeApp({ userId: 'staff-1' });
    expect((await app.request('/api/staff/api-keys/requests/5/approve', json({ rateLimitPerMin: 0 }))).status).toBe(400);
    expect((await app.request('/api/staff/api-keys/requests/5/approve', json({ rateLimitPerMin: 1.5 }))).status).toBe(400);
    expect((await app.request('/api/staff/api-keys/requests/5/approve', json({ rateLimitPerMin: 99999 }))).status).toBe(400);
    expect((await app.request('/api/staff/api-keys/requests/5/approve', json({ expiresAt: '2001-01-01' }))).status).toBe(400);
    expect((await app.request('/api/staff/api-keys/requests/5/approve', json({ expiresAt: 'soon' }))).status).toBe(400);
    expect((await app.request('/api/staff/api-keys/requests/5/approve', json({ note: 'n'.repeat(501) }))).status).toBe(400);
    expect(calls).toHaveLength(0);
  });

  it('approve is an atomic pending→approved claim that stores the grant, bells the user, resolves Discord', async () => {
    staff = true;
    const in1y = new Date(Date.now() + 365 * 24 * 3600 * 1000);
    resultQueue = [{ rows: [REQUEST({ status: 'approved', rate_limit_per_min: 60, review_note: 'enjoy' })] }];
    const res = await makeApp({ userId: 'staff-1' }).request(
      '/api/staff/api-keys/requests/5/approve',
      json({ rateLimitPerMin: 60, expiresAt: in1y.toISOString(), note: 'enjoy' }),
    );
    expect(res.status).toBe(200);
    const upd = calls.find((q) => q.sql.includes("SET status = 'approved'"));
    expect(upd?.sql).toContain("status = 'pending'");
    expect(upd?.params?.[0]).toBe(5);
    expect(upd?.params?.[1]).toBe(60);
    expect(upd?.params?.[2]).toBeInstanceOf(Date);
    expect(upd?.params?.[3]).toBe('enjoy');
    expect(upd?.params?.[4]).toBe('staff-1');
    expect(bells).toHaveLength(1);
    expect(bells[0]).toMatchObject({ userId: 'user-1', type: 'api_key_approved', link: '/#api-keys' });
    expect(String(bells[0]!.body)).toContain('enjoy');
    expect(discord[0]).toMatchObject({ kind: 'api_key_request', event: 'resolved', ref: '5', status: 'approved', actor: 'Staff Person' });
    // Approval never mints.
    expect(calls.some((q) => q.sql.includes('INSERT INTO api_keys'))).toBe(false);
  });

  it('approve loses the claim → 409 already-<status>, 404 when gone', async () => {
    staff = true;
    const app = makeApp({ userId: 'staff-1' });
    resultQueue = [{ rows: [] }, { rows: [{ status: 'rejected' }] }];
    const lost = await app.request('/api/staff/api-keys/requests/5/approve', json({}));
    expect(lost.status).toBe(409);
    expect(((await lost.json()) as { error: string }).error).toBe('already rejected');
    resultQueue = [{ rows: [] }, { rows: [] }];
    expect((await app.request('/api/staff/api-keys/requests/5/approve', json({}))).status).toBe(404);
    expect(bells).toHaveLength(0);
  });

  it('reject stores the note and sends it to the user; 409 when not pending', async () => {
    staff = true;
    const app = makeApp({ userId: 'staff-1' });
    resultQueue = [{ rows: [REQUEST({ status: 'rejected', review_note: 'no automated scraping' })] }];
    const res = await app.request('/api/staff/api-keys/requests/5/reject', json({ note: 'no automated scraping' }));
    expect(res.status).toBe(200);
    expect(bells[0]).toMatchObject({ userId: 'user-1', type: 'api_key_rejected', body: 'no automated scraping', link: '/#api-keys' });
    expect(discord[0]).toMatchObject({ kind: 'api_key_request', event: 'resolved', status: 'rejected' });

    resultQueue = [{ rows: [] }];
    expect((await app.request('/api/staff/api-keys/requests/5/reject', json({}))).status).toBe(409);
  });

  it('withdraw flips only an approved-and-ungenerated grant to rejected and tells the user', async () => {
    staff = true;
    const app = makeApp({ userId: 'staff-1' });
    resultQueue = [{ rows: [REQUEST({ status: 'rejected', review_note: 'changed our mind' })] }];
    const res = await app.request('/api/staff/api-keys/requests/5/withdraw', json({ note: 'changed our mind' }));
    expect(res.status).toBe(200);
    const upd = calls.find((q) => q.sql.includes("SET status = 'rejected'"));
    expect(upd?.sql).toContain("status = 'approved' AND key_id IS NULL");
    expect(bells[0]).toMatchObject({ userId: 'user-1', type: 'api_key_rejected', link: '/#api-keys' });
    expect(String(bells[0]!.title)).toContain('withdrawn');
    expect(discord[0]).toMatchObject({ kind: 'api_key_request', event: 'resolved', status: 'withdrawn' });

    // Already generated (or pending, or gone): nothing to withdraw.
    resultQueue = [{ rows: [] }];
    expect((await app.request('/api/staff/api-keys/requests/5/withdraw', json({}))).status).toBe(409);
  });

  it('lists every key with owner names and no hashes', async () => {
    staff = true;
    resultQueue = [{ rows: [KEY(), KEY({ user_id: 'user-2', prefix: 'zzzzzzzzzz' })] }];
    const res = await makeApp({ userId: 'staff-1' }).request('/api/staff/api-keys');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { keys: Array<Record<string, unknown>> };
    expect(JSON.stringify(body)).not.toContain('deadbeef');
    expect(body.keys[0]).toMatchObject({ prefix: 'abcdefghij', userId: 'user-1', userName: 'Current Name', active: true });
    expect(body.keys[1]).toMatchObject({ userId: 'user-2', userName: null });
  });

  it('staff revoke works on any key and bells the owner (not themselves)', async () => {
    staff = true;
    const app = makeApp({ userId: 'staff-1' });
    resultQueue = [{ rows: [{ user_id: 'user-1', name: 'home-assistant', revoked_at: null }] }, { rows: [], rowCount: 1 }];
    expect((await app.request('/api/staff/api-keys/abcdefghij/revoke', { method: 'POST' })).status).toBe(200);
    expect(calls.some((q) => q.sql.includes('SET revoked_at = now()'))).toBe(true);
    expect(bells[0]).toMatchObject({ userId: 'user-1', type: 'api_key_revoked', exceptUserId: 'staff-1' });

    resultQueue = [{ rows: [{ user_id: 'user-1', name: 'x', revoked_at: 'r' }] }];
    expect((await app.request('/api/staff/api-keys/abcdefghij/revoke', { method: 'POST' })).status).toBe(409);
    resultQueue = [{ rows: [] }];
    expect((await app.request('/api/staff/api-keys/abcdefghij/revoke', { method: 'POST' })).status).toBe(404);

    // Lost race: the owner revoked it between staff's read and write → 409 and NO second bell.
    bells.length = 0;
    resultQueue = [{ rows: [{ user_id: 'user-1', name: 'x', revoked_at: null }] }, { rows: [], rowCount: 0 }];
    expect((await app.request('/api/staff/api-keys/abcdefghij/revoke', { method: 'POST' })).status).toBe(409);
    expect(bells).toHaveLength(0);
  });
});
