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

// Keep requireRole real; stub the DB-backed canManageUsers so management
// routes (GET list, approve, reject) don't need a live user_roles table.
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return {
    ...actual,
    canManageUsers: vi.fn(async () => true),
    canAssignPrivilegedRoles: vi.fn(async () => true),
  };
});

// Keep accountIsIncomplete REAL (it's the safety-critical rule, driven here by
// the mocked pool's resultQueue), but stub deleteAccount so the discard tests
// never touch the real Supabase admin API — we only assert it's invoked.
vi.mock('../../../src/services/orphanCleanup.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/orphanCleanup.js')>();
  return {
    ...actual,
    deleteAccount: vi.fn(async () => true),
  };
});

const { editorRouter } = await import('../../../src/api/editor.js');
const roles = await import('../../../src/services/auth/roles.js');
const orphan = await import('../../../src/services/orphanCleanup.js');
const { _resetRolesCacheForTests } = roles;

// Injects a verified user id by default (POST /api/editor-requests is public
// and unaffected); pass {authed:false} to exercise the 401 path.
function makeApp(opts: { authed?: boolean } = {}) {
  const app = new Hono();
  if (opts.authed !== false) {
    app.use('*', async (c, next) => {
      c.set('userId', 'owner-1');
      await next();
    });
  }
  app.route('/', editorRouter);
  return app;
}

beforeEach(() => {
  calls.length = 0;
  resultQueue = [];
  getPoolReturn = 'pool';
  fakePool.query.mockClear();
  vi.mocked(orphan.deleteAccount).mockClear();
  vi.mocked(orphan.deleteAccount).mockResolvedValue(true);
  _resetRolesCacheForTests();
});

describe('POST /api/editor-requests (public submit)', () => {
  it('400 when email missing on an ANONYMOUS submission', async () => {
    // Email is only required when there's no linked account to identify the
    // requester (a JWT-linked request is identified by its account instead).
    const app = makeApp({ authed: false });
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: 'Valid email is required' });
  });

  it('a JWT-linked submission may omit email (Discord account identifies it)', async () => {
    // Discord OAuth accounts may not share a verified email; the JWT link is
    // enough, so a full submit with no email still creates the request.
    resultQueue = [{ rows: [] }, { rows: [{ id: 101 }] }];
    const app = makeApp(); // userId 'owner-1' (linked)
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ discord_id: '123', about: 'hi', request_type: ['editor'] }), // no email
    });
    expect(res.status).toBe(201);
    expect((await res.json()).request_id).toBe(101);
    expect(calls[1]?.sql).toContain('INSERT INTO editor_requests');
  });

  it('updates (upserts) an existing pending request instead of erroring', async () => {
    // existing-row check returns a pending request → the handler UPDATEs it.
    resultQueue = [{ rows: [{ id: 7, status: 'pending' }] }, { rows: [] }];
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
    expect(res.status).toBe(200);
    expect((await res.json()).request_id).toBe(7);
    expect(calls[1]?.sql).toContain('UPDATE editor_requests');
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

  it('stores the verified userId (JWT) as supabase_user_id, never a body value', async () => {
    resultQueue = [{ rows: [] }, { rows: [{ id: 5 }] }];
    const app = makeApp(); // sets userId 'owner-1'
    await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email: 'a@b.com',
        discord_id: 'd1',
        about: 'hi',
        request_type: ['editor'],
        supabase_user_id: 'attacker-chosen-id', // must be ignored
      }),
    });
    const params = calls[1]?.params ?? [];
    expect(params[13]).toBe('owner-1');
  });

  it('stores null supabase_user_id for anonymous submissions', async () => {
    resultQueue = [{ rows: [] }, { rows: [{ id: 6 }] }];
    const app = makeApp({ authed: false });
    await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email: 'a@b.com',
        discord_id: 'd1',
        about: 'hi',
        request_type: ['editor'],
      }),
    });
    const params = calls[1]?.params ?? [];
    expect(params[13]).toBeNull();
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
  it('403 when a team member tries to assign a privileged role', async () => {
    vi.mocked(roles.canAssignPrivilegedRoles).mockResolvedValueOnce(false);
    const app = makeApp();
    const res = await app.request('/api/editor-requests/1/approve', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ roles: ['map_editor', 'team_member'] }),
    });
    expect(res.status).toBe(403);
    expect(((await res.json()) as { error: string }).error).toContain('Only owners');
    // Denied before any DB work — the request row is never fetched.
    expect(calls).toHaveLength(0);
  });

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

  it('assigns roles to the linked account and skips account creation', async () => {
    const fetchSpy = vi.spyOn(global, 'fetch');
    try {
      resultQueue = [
        { rows: [{ id: 1, email: 'a@b.com', discord_id: 'd', status: 'pending', supabase_user_id: 'linked-uid-1' }] },
        { rows: [] }, // INSERT role 1
        { rows: [] }, // INSERT role 2
        { rows: [] }, // UPDATE editor_requests
      ];
      const app = makeApp();
      const res = await app.request('/api/editor-requests/1/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        // create_account true must NOT create an account for linked requests
        body: JSON.stringify({ roles: ['map_editor', 'radio_contributor'], create_account: true }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as Record<string, unknown>;
      expect(body['roles_assigned_to_linked_account']).toBe(true);
      expect(body['supabase_account_created']).toBe(false);
      expect(body['temp_password']).toBeUndefined();
      // No Supabase admin API call was made.
      expect(fetchSpy).not.toHaveBeenCalled();
      // Roles inserted for the linked user id.
      const roleInserts = calls.filter((c2) => c2.sql.includes('INSERT INTO user_roles'));
      expect(roleInserts).toHaveLength(2);
      expect(roleInserts[0]?.params?.[0]).toBe('linked-uid-1');
      // Notes record the linked assignment.
      const updateCall = calls.find((c2) => c2.sql.includes("status = 'approved'"));
      expect(updateCall?.params?.[1]).toContain('linked account linked-uid-1');
    } finally {
      fetchSpy.mockRestore();
    }
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

      // Supabase admin endpoint was called with email; the dead
      // force_password_change flag is no longer sent (no first-login-change flow).
      expect(fetchSpy).toHaveBeenCalledOnce();
      const [url, init] = fetchSpy.mock.calls[0]!;
      expect(String(url)).toBe('https://test.supabase.co/auth/v1/admin/users');
      const sentBody = JSON.parse(String((init as RequestInit).body));
      expect(sentBody.email).toBe('a@b.com');
      expect(sentBody.email_confirm).toBe(true);
      expect(sentBody.user_metadata.discord_id).toBe('d99');
      expect(sentBody.user_metadata.force_password_change).toBeUndefined();

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
    expect(body['tabs']).toEqual({ requests: true, users: true, dev: true, nodes: true, data: true });
  });

  it('team_member sees requests + users but NOT dev', async () => {
    resultQueue = [{ rows: [{ role: 'team_member' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-admin/u-tm');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['can_assign_privileged_roles']).toBe(false);
    expect(body['tabs']).toEqual({ requests: true, users: true, dev: false, nodes: false, data: false });
  });

  it('dev sees only the Dev tab', async () => {
    resultQueue = [{ rows: [{ role: 'dev' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-admin/u-dev');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['tabs']).toEqual({ requests: false, users: false, dev: true, nodes: true, data: true });
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

  it('response includes the exact keys staff.html and dashboard.html depend on', async () => {
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

describe('POST /api/account/discard-incomplete (self-service)', () => {
  // Configure Supabase so the delete branch isn't short-circuited to 503.
  // Restored after each test.
  let cfgMod: typeof import('../../../src/config.js');
  let origUrl: string | undefined;
  let origKey: string | undefined;
  async function withSupabaseConfigured() {
    cfgMod = await import('../../../src/config.js');
    origUrl = cfgMod.config.SUPABASE_URL;
    origKey = cfgMod.config.SUPABASE_SERVICE_ROLE_KEY;
    (cfgMod.config as { SUPABASE_URL?: string }).SUPABASE_URL = 'https://test.supabase.co';
    (cfgMod.config as { SUPABASE_SERVICE_ROLE_KEY?: string }).SUPABASE_SERVICE_ROLE_KEY = 'srv-test-key';
  }
  function restoreConfig() {
    if (!cfgMod) return;
    (cfgMod.config as { SUPABASE_URL?: string }).SUPABASE_URL = origUrl;
    (cfgMod.config as { SUPABASE_SERVICE_ROLE_KEY?: string }).SUPABASE_SERVICE_ROLE_KEY = origKey;
  }

  it('401 when no verified user (no JWT)', async () => {
    const app = makeApp({ authed: false });
    const res = await app.request('/api/account/discard-incomplete', { method: 'POST' });
    expect(res.status).toBe(401);
    // The safety-critical delete helper is never even reached.
    expect(orphan.deleteAccount).not.toHaveBeenCalled();
  });

  it('deletes an incomplete account (no roles, no request) → deleted:true', async () => {
    await withSupabaseConfigured();
    try {
      // accountIsIncomplete runs 2 queries: user_roles (none) then editor_requests (none).
      resultQueue = [
        { rows: [] },              // SELECT 1 FROM user_roles → no role
        { rows: [], rowCount: 0 }, // SELECT 1 FROM editor_requests → no request
      ];
      const app = makeApp(); // userId 'owner-1'
      const res = await app.request('/api/account/discard-incomplete', { method: 'POST' });
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({ deleted: true });
      // The reused delete path was invoked for the CALLER's own id only.
      expect(orphan.deleteAccount).toHaveBeenCalledTimes(1);
      expect(vi.mocked(orphan.deleteAccount).mock.calls[0]?.[1]).toBe('owner-1');
    } finally {
      restoreConfig();
    }
  });

  it('does NOT delete an account WITH a pending request → deleted:false', async () => {
    await withSupabaseConfigured();
    try {
      resultQueue = [
        { rows: [] },                    // no role
        { rows: [{ x: 1 }], rowCount: 1 }, // HAS an editor_request → keep
      ];
      const app = makeApp();
      const res = await app.request('/api/account/discard-incomplete', { method: 'POST' });
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({ deleted: false, reason: 'has_roles_or_request' });
      // A real pending user is NEVER deleted.
      expect(orphan.deleteAccount).not.toHaveBeenCalled();
    } finally {
      restoreConfig();
    }
  });

  it('does NOT delete an account that already has a role → deleted:false', async () => {
    await withSupabaseConfigured();
    try {
      // First query (user_roles) returns a row → short-circuits as "has role".
      resultQueue = [{ rows: [{ role: 'map_editor' }], rowCount: 1 }];
      const app = makeApp();
      const res = await app.request('/api/account/discard-incomplete', { method: 'POST' });
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({ deleted: false, reason: 'has_roles_or_request' });
      expect(orphan.deleteAccount).not.toHaveBeenCalled();
    } finally {
      restoreConfig();
    }
  });

  it('503 when incomplete but Supabase is not configured (no silent no-op)', async () => {
    cfgMod = await import('../../../src/config.js');
    origUrl = cfgMod.config.SUPABASE_URL;
    origKey = cfgMod.config.SUPABASE_SERVICE_ROLE_KEY;
    (cfgMod.config as { SUPABASE_URL?: string }).SUPABASE_URL = undefined;
    (cfgMod.config as { SUPABASE_SERVICE_ROLE_KEY?: string }).SUPABASE_SERVICE_ROLE_KEY = undefined;
    try {
      resultQueue = [
        { rows: [] },              // no role
        { rows: [], rowCount: 0 }, // no request → incomplete
      ];
      const app = makeApp();
      const res = await app.request('/api/account/discard-incomplete', { method: 'POST' });
      expect(res.status).toBe(503);
      expect(orphan.deleteAccount).not.toHaveBeenCalled();
    } finally {
      restoreConfig();
    }
  });

  it('502 when the delete helper fails', async () => {
    await withSupabaseConfigured();
    vi.mocked(orphan.deleteAccount).mockResolvedValueOnce(false);
    try {
      resultQueue = [
        { rows: [] },
        { rows: [], rowCount: 0 },
      ];
      const app = makeApp();
      const res = await app.request('/api/account/discard-incomplete', { method: 'POST' });
      expect(res.status).toBe(502);
    } finally {
      restoreConfig();
    }
  });
});

describe('management routes require an authorized user', () => {
  it('GET /api/editor-requests 401 without a verified user', async () => {
    const app = makeApp({ authed: false });
    const res = await app.request('/api/editor-requests');
    expect(res.status).toBe(401);
  });

  it('approve 403 when authenticated but lacks canManageUsers', async () => {
    vi.mocked(roles.canManageUsers).mockResolvedValueOnce(false);
    const app = makeApp();
    const res = await app.request('/api/editor-requests/1/approve', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    expect(res.status).toBe(403);
  });

  it('POST /api/editor-requests stays public (no auth needed)', async () => {
    const app = makeApp({ authed: false });
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    // 400 (validation), NOT 401 — proves the public submit isn't gated.
    expect(res.status).toBe(400);
  });
});
