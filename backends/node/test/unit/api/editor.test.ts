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

// Capture what would be posted to the staff Discord channel. These payloads
// leave our control the moment they are sent, so what is IN them is worth
// asserting on directly.
const notified: Array<Record<string, unknown>> = [];
vi.mock('../../../src/services/staffNotify.js', () => ({
  notifyStaff: (_pool: unknown, input: Record<string, unknown>) => {
    notified.push(input);
  },
}));

const { editorRouter } = await import('../../../src/api/editor.js');
const roles = await import('../../../src/services/auth/roles.js');
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
  notified.length = 0;
  resultQueue = [];
  getPoolReturn = 'pool';
  fakePool.query.mockClear();
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

  // --- referral attribution -------------------------------------------------
  // Params 14/15 are referred_by / referred_by_name (appended after
  // supabase_user_id at 13 so existing assertions keep their indices).
  it('records the referrer when the body carries a known code', async () => {
    resultQueue = [{ rows: [{ user_id: 'ref-1' }] }, { rows: [] }, { rows: [{ id: 9 }] }];
    const app = makeApp();
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: 'a@b.com', about: 'x', request_type: ['editor'], referral_code: 'abcd2345' }),
    });
    expect(res.status).toBe(201);
    expect(calls[0]?.sql).toContain('FROM referral_codes');
    expect(calls[0]?.params).toEqual(['ABCD2345']); // uppercased -> case-insensitive
    const params = calls[2]?.params ?? [];
    expect(params[13]).toBe('owner-1'); // supabase_user_id still at 13
    expect(params[14]).toBe('ref-1');
  });

  it('ignores an unknown code without failing the signup', async () => {
    resultQueue = [{ rows: [] }, { rows: [] }, { rows: [{ id: 10 }] }];
    const app = makeApp();
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: 'a@b.com', about: 'x', request_type: ['editor'], referral_code: 'NOPENOPE' }),
    });
    expect(res.status).toBe(201);
    expect((calls[2]?.params ?? [])[14]).toBeNull();
  });

  it('ignores a self-referral', async () => {
    resultQueue = [{ rows: [{ user_id: 'owner-1' }] }, { rows: [] }, { rows: [{ id: 11 }] }];
    const app = makeApp(); // userId 'owner-1'
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: 'a@b.com', about: 'x', request_type: ['editor'], referral_code: 'SELFCODE' }),
    });
    expect(res.status).toBe(201);
    expect((calls[2]?.params ?? [])[14]).toBeNull();
  });

  it('a failed code lookup never fails the signup', async () => {
    fakePool.query.mockRejectedValueOnce(new Error('pg down'));
    resultQueue = [{ rows: [] }, { rows: [{ id: 12 }] }];
    const app = makeApp();
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: 'a@b.com', about: 'x', request_type: ['editor'], referral_code: 'ABCD2345' }),
    });
    expect(res.status).toBe(201);
  });

  it('issues no referral query for a malformed code', async () => {
    resultQueue = [{ rows: [] }, { rows: [{ id: 13 }] }];
    const app = makeApp();
    await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: 'a@b.com', about: 'x', request_type: ['editor'], referral_code: 'no!' }),
    });
    expect(calls[0]?.sql).toContain('SELECT id, status FROM editor_requests');
  });

  it('a re-submit never overwrites the original referrer', async () => {
    resultQueue = [{ rows: [{ user_id: 'ref-2' }] }, { rows: [{ id: 7, status: 'pending' }] }, { rows: [] }];
    const app = makeApp();
    const res = await app.request('/api/editor-requests', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: 'a@b.com', about: 'x', request_type: ['editor'], referral_code: 'ABCD2345' }),
    });
    expect(res.status).toBe(200);
    expect(calls[2]?.sql).toContain('COALESCE(referred_by, $14)');
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
        referred_by: 'ref-1', referred_by_name: 'Alex',
      }],
    }];
    const app = makeApp();
    const res = await app.request('/api/editor-requests');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { requests: Array<Record<string, unknown>>; count: number };
    expect(body.count).toBe(1);
    expect(body.requests[0]?.['request_type']).toEqual(['editor', 'pager_feeder']);
    // Referral attribution reaches the staff UI.
    expect(body.requests[0]?.['referred_by']).toBe('ref-1');
    expect(body.requests[0]?.['referred_by_name']).toBe('Alex');
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
      body: JSON.stringify({ roles: ['map:editor', 'staff'] }),
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
      // A role is required to approve, so send one — otherwise this 400s on
      // validation before it ever looks the request up.
      body: JSON.stringify({ roles: ['map:editor'] }),
    });
    expect(res.status).toBe(404);
  });

  it('400s when approving with no roles selected', async () => {
    const app = makeApp();
    const res = await app.request('/api/editor-requests/1/approve', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ roles: [] }),
    });
    expect(res.status).toBe(400);
    // Must not have touched the request row.
    expect(calls.some((c) => c.sql.includes("status = 'approved'"))).toBe(false);
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
      body: JSON.stringify({ roles: ['map:editor', 'feeder:pager'] }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['success']).toBe(true);
    expect(body['email']).toBe('a@b.com');
    expect(body['roles']).toEqual(['map:editor', 'feeder:pager']);
    const updateCall = calls[1];
    expect(updateCall?.sql).toContain("status = 'approved'");
    expect(updateCall?.params?.[1]).toContain('Roles: map:editor,feeder:pager');
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
        body: JSON.stringify({ roles: ['map:editor', 'feeder:radio'], create_account: true }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as Record<string, unknown>;
      expect(body['roles_assigned_to_linked_account']).toBe(true);
      expect(body['pending_first_sign_in']).toBe(false);
      // No Supabase admin API call was made.
      expect(fetchSpy).not.toHaveBeenCalled();
      // Roles inserted for the linked user id.
      const roleInserts = calls.filter((c2) => c2.sql.includes('INSERT INTO user_roles'));
      // Approved roles + the implicit base 'authed' grant.
      expect(roleInserts).toHaveLength(3);
      expect(roleInserts.map((r) => r.params?.[1])).toContain('authed');
      expect(roleInserts[0]?.params?.[0]).toBe('linked-uid-1');
      // Notes record the linked assignment.
      const updateCall = calls.find((c2) => c2.sql.includes("status = 'approved'"));
      expect(updateCall?.params?.[1]).toContain('linked account linked-uid-1');
    } finally {
      fetchSpy.mockRestore();
    }
  });

  it('records the roles and waits when the request has no account yet', async () => {
    // A request filed while email confirmation was still pending. There used
    // to be a second Supabase account created here, with a password we
    // generated and staff had to pass on by hand — which for an email that
    // already had an account simply failed, granting nothing.
    const fetchSpy = vi.spyOn(global, 'fetch');
    try {
      resultQueue = [
        { rows: [{ id: 1, email: 'a@b.com', discord_id: 'd', status: 'pending', supabase_user_id: null }] },
        { rows: [] }, // UPDATE editor_requests
      ];
      const app = makeApp();
      const res = await app.request('/api/editor-requests/1/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        // create_account is gone from the contract; sending it changes nothing.
        body: JSON.stringify({ roles: ['map:editor'], create_account: true }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as Record<string, unknown>;
      expect(body['pending_first_sign_in']).toBe(true);
      expect(body['roles_assigned_to_linked_account']).toBe(false);

      // Nothing was created anywhere: no Supabase admin call, no role rows
      // against an account that does not exist yet.
      expect(fetchSpy).not.toHaveBeenCalled();
      expect(calls.filter((c2) => c2.sql.includes('INSERT INTO user_roles'))).toHaveLength(0);

      // The grant is recorded on the request, which is what the first
      // signed-in page load reads to apply it.
      const updateCall = calls.find((c2) => c2.sql.includes("status = 'approved'"));
      expect(updateCall?.sql).toContain('approved_roles');
      expect(String(updateCall?.params?.[2])).toContain('map:editor');
      expect(String(updateCall?.params?.[2])).toContain('authed');
      expect(updateCall?.params?.[1]).toContain('Awaiting first sign-in');
    } finally {
      fetchSpy.mockRestore();
    }
  });

  it('never issues a password, on either path', async () => {
    // The guard for the whole reason this branch was removed. Asserted over
    // every string the route emits — the response, the stored note and the
    // staff notification — because the leak happened by one of them picking up
    // a value it had no business carrying.
    for (const linked of [null, 'linked-uid-1']) {
      calls.length = 0;
      notified.length = 0;
      resultQueue = [
        { rows: [{ id: 1, email: 'a@b.com', discord_id: 'd', status: 'pending', supabase_user_id: linked }] },
        { rows: [] }, { rows: [] }, { rows: [] },
      ];
      const res = await makeApp().request('/api/editor-requests/1/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ roles: ['map:editor'] }),
      });
      expect(res.status).toBe(200);
      const everything = JSON.stringify([await res.json(), calls, notified]);
      expect(everything).not.toContain('Changeme-');
      expect(everything.toLowerCase()).not.toContain('temp password');
      expect(everything.toLowerCase()).not.toContain('temp_password');
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
    resultQueue = [{ rows: [{ role: 'map:editor' }, { role: 'feeder:pager' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-editor/user-1');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['user_id']).toBe('user-1');
    expect(body['has_access']).toBe(true);
    expect(body['is_owner']).toBe(false);
    expect(body['is_team_member']).toBe(false);
    expect(body['is_map_editor']).toBe(true);
    expect(body['roles']).toEqual(['map:editor', 'feeder:pager']);
  });

  it('team_member alone does NOT grant has_access', async () => {
    resultQueue = [{ rows: [{ role: 'staff' }] }];
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
    expect(body['tabs']).toEqual({ requests: true, users: true, dev: true, nodes: true, data: true, data_changes: true });
  });

  it('team_member sees requests + users but NOT dev', async () => {
    resultQueue = [{ rows: [{ role: 'staff' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-admin/u-tm');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['can_assign_privileged_roles']).toBe(false);
    expect(body['tabs']).toEqual({ requests: true, users: true, dev: false, nodes: false, data: false, data_changes: true });
  });

  it('feeder:manager sees node/data tabs but not users (dev role removed)', async () => {
    resultQueue = [{ rows: [{ role: 'feeder:manager' }] }];
    const app = makeApp();
    const res = await app.request('/api/check-admin/u-fm');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['tabs']).toEqual({ requests: true, users: false, dev: false, nodes: true, data: true, data_changes: true });
    expect(body['is_dev']).toBe(false);
    expect(body['can_manage_nodes']).toBe(true);
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
