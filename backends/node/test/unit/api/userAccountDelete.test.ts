/**
 * DELETE /api/users/:userId — the owner-only account removal.
 *
 * Its own file because it needs SUPABASE_URL + a service-role key present
 * (users.test.ts deliberately runs with them unset to pin the 503), and the
 * config mock is module-wide.
 *
 * The guarantee under test: removing the account removes every credential it
 * holds. resolveApiKey only ever asks whether a key is live, never whether its
 * owner still exists, and an expiry is optional — so a key left behind is
 * permanent scripted access for someone who no longer has an account.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
const txCalls: Call[] = [];
let txQueue: Array<{ rows: unknown[]; rowCount?: number }> = [];

const fakeClient = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    txCalls.push({ sql, ...(params ? { params } : {}) });
    return txQueue.shift() ?? { rows: [], rowCount: 0 };
  }),
  release: vi.fn(),
};
const fakePool = {
  query: vi.fn(async () => ({ rows: [], rowCount: 0 })),
  connect: vi.fn(async () => fakeClient),
};
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => fakePool),
}));

vi.mock('../../../src/config.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/config.js')>();
  return {
    ...actual,
    config: {
      ...actual.config,
      SUPABASE_URL: 'https://test.supabase.co',
      SUPABASE_SERVICE_ROLE_KEY: 'service-role-key',
    },
  };
});

vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, isOwner: vi.fn(async () => true), invalidateUserRolesCache: vi.fn() };
});

const revokeApiKey = vi.fn(async () => true);
vi.mock('../../../src/services/auth/apiKeys.js', () => ({
  revokeApiKey: (prefix: string) => revokeApiKey(prefix),
}));

const { usersRouter } = await import('../../../src/api/users.js');

function makeApp() {
  const app = new Hono();
  app.use('*', async (c, next) => { c.set('userId', 'owner-1'); await next(); });
  app.route('/', usersRouter);
  return app;
}

beforeEach(() => {
  txCalls.length = 0;
  txQueue = [];
  revokeApiKey.mockClear();
  vi.stubGlobal('fetch', vi.fn(async () => new Response(null, { status: 204 })));
});

describe('DELETE /api/users/:userId', () => {
  it('revokes the account\'s API keys and closes its open key requests', async () => {
    txQueue = [
      { rows: [], rowCount: 0 },                       // BEGIN
      { rows: [], rowCount: 1 },                       // DELETE user_roles
      { rows: [], rowCount: 0 },                       // DELETE nodes
      { rows: [{ prefix: 'ak_abc' }], rowCount: 1 },   // UPDATE api_keys
      { rows: [], rowCount: 1 },                       // UPDATE api_key_requests
    ];
    const res = await makeApp().request('/api/users/gone-1', { method: 'DELETE' });
    expect(res.status).toBe(200);

    const keys = txCalls.find((c) => /UPDATE api_keys/.test(c.sql));
    expect(keys?.sql).toContain('revoked_at = now()');
    expect(keys?.params).toEqual(['gone-1']);
    // An approved-but-unclaimed request is a key waiting to be minted, and the
    // deleted account's JWT stays valid for up to an hour.
    const reqs = txCalls.find((c) => /UPDATE api_key_requests/.test(c.sql));
    expect(reqs?.sql).toContain("status = 'rejected'");
    expect(reqs?.sql).toContain('claimed_at IS NULL');
    // Same transaction as the role + node cleanup.
    expect(txCalls.filter((c) => /^\s*(BEGIN|COMMIT)/.test(c.sql)).length).toBe(2);
    // ...and the prefix is evicted from the in-process resolve cache, which
    // would otherwise keep answering "live" for up to its TTL.
    expect(revokeApiKey).toHaveBeenCalledWith('ak_abc');
  });

  it('refuses to delete the acting owner', async () => {
    const res = await makeApp().request('/api/users/owner-1', { method: 'DELETE' });
    expect(res.status).toBe(400);
    expect(txCalls).toHaveLength(0);
  });
});
