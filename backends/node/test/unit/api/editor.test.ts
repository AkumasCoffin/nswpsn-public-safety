/**
 * Editor router tests — covers /api/editor-requests*, /api/check-editor,
 * /api/check-admin. Mocks getPool() and intercepts SQL.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
const calls: Call[] = [];
// Multi-step routes need different rows per call. Maintain a queue.
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

const { editorRouter } = await import('../../../src/api/editor.js');
const { _resetRolesCacheForTests } = await import('../../../src/services/auth/roles.js');

function makeApp() {
  const app = new Hono();
  app.route('/', editorRouter);
  return app;
}

beforeEach(() => {
  calls.length = 0;
  resultQueue = [];
  getPoolReturn = 'pool';
  fakePool.query.mockClear();
  _resetRolesCacheForTests();
});

describe('POST /api/editor-requests (public submit)', () => {
  it('400 when email missing', async () => {
    const app = makeApp();
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: 'Valid email is required' });
  });

  it('409 when a pending request already exists for the email', async () => {
    resultQueue = [{ rows: [{ id: 7 }] }];
    const app = makeApp();
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email: 'a@b.com',
        discord_id: 'dxyz',
        about: 'I want in',
        request_type: ['editor'],
      }),
    });
    expect(res.status).toBe(409);
  });

  it('201 with request_id and stores comma-joined arrays', async () => {
    resultQueue = [{ rows: [] }, { rows: [{ id: 42 }] }];
    const app = makeApp();
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email: 'a@b.com',
        discord_id: 'd1',
        about: 'about me',
        request_type: ['editor', 'pager_feeder'],
        tech_experience: ['ts', 'sql'],
        experience_level: 4,
      }),
    });
    expect(res.status).toBe(201);
    const body = (await res.json()) as { request_id: number; success: boolean };
    expect(body.request_id).toBe(42);
    // Insert is the 2nd call (1st was the existing-row check).
    const params = calls[1]?.params ?? [];
    expect(params[4]).toBe('editor,pager_feeder');
    expect(params[10]).toBe('ts,sql');
    expect(params[11]).toBe(4);
  });

  it('clamps experience_level outside 1-5 to null', async () => {
    resultQueue = [{ rows: [] }, { rows: [{ id: 1 }] }];
    const app = makeApp();
    await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email: 'x@y.com',
        discord_id: 'd',
        about: 'hi',
        request_type: ['editor'],
        experience_level: 99,
      }),
    });
    const params = calls[1]?.params ?? [];
    expect(params[11]).toBeNull();
  });
});

describe('GET /api/editor-requests', () => {
  it('lists with request_type split back into an array', async () => {
    resultQueue = [{
      rows: [{
        id: 1, email: 'a@b.com', discord_id: 'd', website: null, about: null,
        request_type: 'editor,pager_feeder', region: null, background: null,
        background_details: null, has_existing_setup: null, setup_details: null,
        tech_experience: 'ts', experience_level: 3, status: 'pending',
        created_at: 1700000000, reviewed_at: null, notes: null,
      }],
    }];
    const app = makeApp();
    const res = await app.request('/api/editor-requests');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { requests: Array<Record<string, unknown>>; count: number };
    expect(body.count).toBe(1);
    expect(body.requests[0]?.['request_type']).toEqual(['editor', 'pager_feeder']);
  });

  it('filters by status when ?status=approved', async () => {
    resultQueue = [{ rows: [] }];
    const app = makeApp();
    await app.request('/api/editor-requests?status=approved');
    expect(calls[0]?.sql).toContain('WHERE status = $1');
    expect(calls[0]?.params).toEqual(['approved']);
  });
});

describe('POST /api/editor-requests/:id/approve', () => {
  it('404 when request not found', async () => {
    resultQueue = [{ rows: [] }];
    const app = makeApp();
    const res = await app.request('/api/editor-requests/99/approve', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    expect(res.status).toBe(404);
  });

  it('400 when already approved', async () => {
    resultQueue = [{ rows: [{ id: 1, email: 'a@b.com', discord_id: 'd', status: 'approved' }] }];
    const app = makeApp();
    const res = await app.request('/api/editor-requests/1/approve', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    expect(res.status).toBe(400);
  });

  it('updates status to approved with notes', async () => {
    resultQueue = [
      { rows: [{ id: 1, email: 'a@b.com', discord_id: 'd', status: 'pending' }] },
      { rows: [] }, // UPDATE
    ];
    const app = makeApp();
    const res = await app.request('/api/editor-requests/1/approve', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ roles: ['map_editor', 'pager_contributor'] }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['success']).toBe(true);
    expect(body['email']).toBe('a@b.com');
    expect(body['roles']).toEqual(['map_editor', 'pager_contributor']);
    const updateCall = calls[1];
    expect(updateCall?.sql).toContain("status = 'approved'");
    expect(updateCall?.params?.[1]).toContain('Roles: map_editor,pager_contributor');
  });

  it('creates a Supabase account, inserts roles, and surfaces temp password when create_account is true', async () => {
    process.env['SUPABASE_URL'] = 'https://test.supabase.co';
    process.env['SUPABASE_SERVICE_ROLE_KEY'] = 'srv-test-key';
    // Re-evaluate config since the editor module reads it at module
    // import time. Vitest hoists vi.mock, but config is a const. The
    // safest path: stub global.fetch so the route's fetch call hits
    // our handler, and rely on the already-imported config snapshot.
    // The two env vars above feed the next config reload but for this
    // test we additionally patch the config object directly via the
    // dynamic import.
    const cfgMod = await import('../../../src/config.js');
    const origUrl = cfgMod.config.SUPABASE_URL;
    const origKey = cfgMod.config.SUPABASE_SERVICE_ROLE_KEY;
    (cfgMod.config as { SUPABASE_URL: string }).SUPABASE_URL =
      'https://test.supabase.co';
    (cfgMod.config as { SUPABASE_SERVICE_ROLE_KEY: string }).SUPABASE_SERVICE_ROLE_KEY =
      'srv-test-key';

    const fetchSpy = vi
      .spyOn(global, 'fetch')
      .mockResolvedValue(
        new Response(JSON.stringify({ id: 'uuid-of-new-user' }), {
          status: 201,
          headers: { 'Content-Type': 'application/json' },
        }),
      );

    try {
      // 4 queries: SELECT request, INSERT role x2, UPDATE editor_requests
      resultQueue = [
        { rows: [{ id: 1, email: 'a@b.com', discord_id: 'd99', status: 'pending' }] },
        { rows: [] }, // INSERT role 1
        { rows: [] }, // INSERT role 2
        { rows: [] }, // UPDATE editor_requests
      ];
      const app = makeApp();
      const res = await app.request('/api/editor-requests/1/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          roles: ['map_editor', 'pager_contributor'],
          create_account: true,
        }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as Record<string, unknown>;
      expect(body['supabase_account_created']).toBe(true);
      expect(body['supabase_error']).toBeUndefined();
      expect(typeof body['temp_password']).toBe('string');
      expect((body['temp_password'] as string).startsWith('Changeme-')).toBe(true);

      // Supabase admin endpoint was called with email + force_password_change.
      expect(fetchSpy).toHaveBeenCalledOnce();
      const [url, init] = fetchSpy.mock.calls[0]!;
      expect(String(url)).toBe('https://test.supabase.co/auth/v1/admin/users');
      const sentBody = JSON.parse(String((init as RequestInit).body));
      expect(sentBody.email).toBe('a@b.com');
      expect(sentBody.email_confirm).toBe(true);
      expect(sentBody.user_metadata.discord_id).toBe('d99');
      expect(sentBody.user_metadata.force_password_change).toBe(true);

      // Two role INSERTs landed against the new Supabase user id.
      expect(calls[1]?.sql).toContain('INSERT INTO user_roles');
      expect(calls[1]?.params?.[0]).toBe('uuid-of-new-user');
      expect(calls[1]?.params?.[1]).toBe('map_editor');
      expect(calls[2]?.params?.[1]).toBe('pager_contributor');

      // Notes string captures the temp password and the success line.
      const updateCall = calls[3];
      expect(updateCall?.sql).toContain("status = 'approved'");
      const notes = updateCall?.params?.[1] as string;
      expect(notes).toContain('Temp password: Changeme-');
      expect(notes).toContain('Supabase account created');
    } finally {
      fetchSpy.mockRestore();
      (cfgMod.config as { SUPABASE_URL?: string }).SUPABASE_URL = origUrl;
      (cfgMod.config as { SUPABASE_SERVICE_ROLE_KEY?: string }).SUPABASE_SERVICE_ROLE_KEY =
        origKey;
    }
  });

  it('returns supabase_error and skips role insertion when Supabase rejects creation', async () => {
    const cfgMod = await import('../../../src/config.js');
    const origUrl = cfgMod.config.SUPABASE_URL;
    const origKey = cfgMod.config.SUPABASE_SERVICE_ROLE_KEY;
    (cfgMod.config as { SUPABASE_URL: string }).SUPABASE_URL =
      'https://test.supabase.co';
    (cfgMod.config as { SUPABASE_SERVICE_ROLE_KEY: string }).SUPABASE_SERVICE_ROLE_KEY =
      'srv-test-key';

    const fetchSpy = vi.spyOn(global, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ message: 'email_exists' }), {
        status: 422,
        headers: { 'Content-Type': 'application/json' },
      }),
    );
    try {
      resultQueue = [
        { rows: [{ id: 1, email: 'a@b.com', discord_id: 'd99', status: 'pending' }] },
        { rows: [] }, // UPDATE only — role inserts must be skipped
      ];
      const app = makeApp();
      const res = await app.request('/api/editor-requests/1/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ roles: ['map_editor'], create_account: true }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as Record<string, unknown>;
      expect(body['supabase_account_created']).toBe(false);
      expect(body['supabase_error']).toBe('email_exists');
      expect(typeof body['temp_password']).toBe('string'); // generated even on failure (matches python)

      // Only 2 DB calls: SELECT request, UPDATE editor_requests. No role inserts.
      expect(calls).toHaveLength(2);
      expect(calls[1]?.sql).toContain("status = 'approved'");
      expect(calls[1]?.params?.[1]).toContain('Supabase error: email_exists');
    } finally {
      fetchSpy.mockRestore();
      (cfgMod.config as { SUPABASE_URL?: string }).SUPABASE_URL = origUrl;
      (cfgMod.config as { SUPABASE_SERVICE_ROLE_KEY?: string }).SUPABASE_SERVICE_ROLE_KEY =
        origKey;
    }
  });

  it('annotates notes with "Supabase not configured" when create_account=true and env unset', async () => {
    const cfgMod = await import('../../../src/config.js');
    const origUrl = cfgMod.config.SUPABASE_URL;
    const origKey = cfgMod.config.SUPABASE_SERVICE_ROLE_KEY;
    (cfgMod.config as { SUPABASE_URL?: string }).SUPABASE_URL = undefined;
    (cfgMod.config as { SUPABASE_SERVICE_ROLE_KEY?: string }).SUPABASE_SERVICE_ROLE_KEY =
      undefined;

    const fetchSpy = vi.spyOn(global, 'fetch');
    try {
      resultQueue = [
        { rows: [{ id: 1, email: 'a@b.com', discord_id: 'd', status: 'pending' }] },
        { rows: [] },
      ];
      const app = makeApp();
      const res = await app.request('/api/editor-requests/1/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ roles: ['map_editor'], create_account: true }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as Record<string, unknown>;
      expect(body['supabase_account_created']).toBe(false);
      expect(typeof body['temp_password']).toBe('string');
      expect(fetchSpy).not.toHaveBeenCalled();
      expect(calls[1]?.params?.[1]).toContain('Supabase not configured');
    } finally {
      fetchSpy.mockRestore();
      (cfgMod.config as { SUPABASE_URL?: string }).SUPABASE_URL = origUrl;
      (cfgMod.config as { SUPABASE_SERVICE_ROLE_KEY?: string }).SUPABASE_SERVICE_ROLE_KEY =
        origKey;
    }
  });
});

describe('POST /api/editor-requests/:id/reject', () => {
  it('updates status to rejected with provided reason', async () => {
    resultQueue = [
      { rows: [{ id: 5, email: 'a@b.com', status: 'pending' }] },
      { rows: [] },
    ];
    const app = makeApp();
    const res = await app.request('/api/editor-requests/5/reject', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ reason: 'spam' }),
    });
    expect(res.status).toBe(200);
    expect(calls[1]?.sql).toContain("status = 'rejected'");
    expect(calls[1]?.params?.[1]).toBe('spam');
  });

  it('uses default "Rejected" when reason omitted', async () => {
    resultQueue = [
      { rows: [{ id: 5, email: 'a@b.com', status: 'pending' }] },
      { rows: [] },
    ];
    const app = makeApp();
    await app.request('/api/editor-requests/5/reject', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    expect(calls[1]?.params?.[1]).toBe('Rejected');
  });
});

describe('GET /api/check-editor/:userId', () => {
  it('returns role booleans + has_access', async () => {
    resultQueue = [{ rows: [{ role: 'map_editor' }, { role: 'pager_contributor' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-editor/user-1');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['user_id']).toBe('user-1');
    expect(body['has_access']).toBe(true);
    expect(body['is_owner']).toBe(false);
    expect(body['is_team_member']).toBe(false);
    expect(body['is_map_editor']).toBe(true);
    expect(body['roles']).toEqual(['map_editor', 'pager_contributor']);
  });

  it('team_member alone does NOT grant has_access', async () => {
    resultQueue = [{ rows: [{ role: 'team_member' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-editor/u2');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['has_access']).toBe(false);
    expect(body['is_team_member']).toBe(true);
  });
});

describe('GET /api/check-admin/:userId', () => {
  it('owner sees all three tabs', async () => {
    resultQueue = [{ rows: [{ role: 'owner' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-admin/u-owner');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['is_admin']).toBe(true);
    expect(body['is_owner']).toBe(true);
    expect(body['can_manage_users']).toBe(true);
    expect(body['can_assign_privileged_roles']).toBe(true);
    expect(body['tabs']).toEqual({ requests: true, users: true, dev: true });
  });

  it('team_member sees requests + users but NOT dev', async () => {
    resultQueue = [{ rows: [{ role: 'team_member' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-admin/u-tm');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['can_assign_privileged_roles']).toBe(false);
    expect(body['tabs']).toEqual({ requests: true, users: true, dev: false });
  });

  it('dev sees only the Dev tab', async () => {
    resultQueue = [{ rows: [{ role: 'dev' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-admin/u-dev');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['tabs']).toEqual({ requests: false, users: false, dev: true });
  });

  it('grants first-time owner when no owners exist anywhere', async () => {
    // First query: user has no roles. Second query: SELECT owners → empty.
    resultQueue = [{ rows: [] }, { rows: [] }];
    const app = makeApp();
    const res = await app.request('/api/check-admin/u-first');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['is_admin']).toBe(true);
    expect(body['is_owner']).toBe(true);
    expect((body['tabs'] as Record<string, boolean>)?.['users']).toBe(true);
  });

  it('does NOT grant first-time owner when an owner already exists for someone else', async () => {
    resultQueue = [{ rows: [] }, { rows: [{ user_id: 'someone-else' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-admin/u-randomer');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['is_admin']).toBe(false);
    expect(body['is_owner']).toBe(false);
  });

  it('response includes the exact keys editor-requests.html and dashboard.html depend on', async () => {
    resultQueue = [{ rows: [{ role: 'owner' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-admin/u-owner-2');
    const body = (await res.json()) as Record<string, unknown>;
    for (const key of [
      'user_id', 'is_admin', 'is_owner', 'is_team_member', 'is_dev',
      'can_manage_users', 'can_assign_privileged_roles', 'tabs', 'roles',
    ]) {
      expect(body).toHaveProperty(key);
    }
    expect(body['tabs']).toHaveProperty('requests');
    expect(body['tabs']).toHaveProperty('users');
    expect(body['tabs']).toHaveProperty('dev');
  });
});

describe('503 when DB is unavailable', () => {
  it('returns 503 from /api/check-admin', async () => {
    getPoolReturn = 'null';
    const app = makeApp();
    const res = await app.request('/api/check-admin/u');
    expect(res.status).toBe(503);
    expect(await res.json()).toEqual({ error: 'database unavailable' });
  });
});
