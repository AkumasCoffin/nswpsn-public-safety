/**
 * Incidents router tests.
 *
 * Mocks `getPool()` so we can drive each handler against a fake pg
 * `Pool`. We assert the exact SQL and params the handler issues, plus
 * the response shape — both have to stay byte-for-byte python-compatible
 * during cutover.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

// Capture what the handler calls. The fake gets reset per-test.
type Call = { sql: string; params?: unknown[] };
const calls: Call[] = [];
let nextResult: { rows: unknown[]; rowCount?: number } = { rows: [] };
// Optional per-query sequence — when non-empty each query shifts one result;
// otherwise the shared `nextResult` is returned. Lets the multi-query
// ownership/suggestion handlers drive distinct results per step.
let resultQueue: Array<{ rows: unknown[]; rowCount?: number }> = [];
let getPoolReturn: 'pool' | 'null' = 'pool';

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return resultQueue.length ? resultQueue.shift() : nextResult;
  }),
  connect: vi.fn(),
};

// Find the captured call whose SQL contains a substring — robust to the
// ownership SELECT that now precedes several mutations.
function callWith(substr: string): Call | undefined {
  return calls.find((c) => c.sql.includes(substr));
}

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => (getPoolReturn === 'pool' ? fakePool : null)),
}));

// Mutating incident routes are editor-gated (requireRole(canEditIncidents)),
// and edit/delete additionally require creator-or-admin. Stub the DB-backed
// role checks: canEditIncidents→true (any editor) and canManageUsers→true
// (treat the injected user as a site admin) so the CRUD tests exercise the
// handler logic. Ownership DENIAL is covered by its own test that overrides
// canManageUsers→false.
const canManageUsersMock = vi.fn(async () => true);
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return {
    ...actual,
    canEditIncidents: vi.fn(async () => true),
    canManageUsers: (...a: unknown[]) => canManageUsersMock(...(a as [])),
  };
});

const { incidentsRouter } = await import('../../../src/api/incidents.js');

function makeApp() {
  const app = new Hono();
  // Simulate optionalSupabaseJwt having verified a logged-in editor.
  app.use('*', async (c, next) => {
    c.set('userId' as never, 'editor-1' as never);
    c.set('userName' as never, 'Test Editor' as never);
    await next();
  });
  app.route('/', incidentsRouter);
  return app;
}

beforeEach(() => {
  calls.length = 0;
  nextResult = { rows: [] };
  resultQueue = [];
  getPoolReturn = 'pool';
  canManageUsersMock.mockReset();
  canManageUsersMock.mockResolvedValue(true);
  fakePool.query.mockClear();
});

describe('GET /api/incidents', () => {
  it('returns rows with JSONB columns parsed and timestamps as ISO strings', async () => {
    const created = new Date('2024-01-02T03:04:05Z');
    nextResult = {
      rows: [
        {
          id: 'abc',
          title: 't',
          description: '',
          lat: 1,
          lng: 2,
          location: 'Sydney',
          // Stored as JSON-serialised string — python's json.loads path.
          type: '["fire"]',
          status: 'Going',
          size: '-',
          responding_agencies: ['rfs'],
          created_at: created,
          updated_at: created,
          expires_at: null,
          is_rfs_stub: false,
        },
      ],
    };
    const app = makeApp();
    const res = await app.request('/api/incidents');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Array<Record<string, unknown>>;
    expect(body[0]?.['type']).toEqual(['fire']);
    expect(body[0]?.['responding_agencies']).toEqual(['rfs']);
    expect(body[0]?.['created_at']).toBe(created.toISOString());
    expect(calls[0]?.sql).toContain('ORDER BY created_at DESC');
    expect(calls[0]?.sql).not.toContain('expires_at >');
  });

  it('?active=true filters to live rows in JS (no fragile SQL comparison)', async () => {
    const future = new Date(Date.now() + 60 * 60 * 1000);
    const past = new Date(Date.now() - 60 * 60 * 1000);
    nextResult = {
      rows: [
        { id: 'live', title: 'a', description: '', lat: 0, lng: 0, location: '', type: [], status: 'Going', size: '-', responding_agencies: [], created_at: future, updated_at: future, expires_at: future, is_rfs_stub: false },
        { id: 'expired', title: 'b', description: '', lat: 0, lng: 0, location: '', type: [], status: 'Going', size: '-', responding_agencies: [], created_at: past, updated_at: past, expires_at: past, is_rfs_stub: false },
        { id: 'nullexp', title: 'c', description: '', lat: 0, lng: 0, location: '', type: [], status: 'Going', size: '-', responding_agencies: [], created_at: past, updated_at: past, expires_at: null, is_rfs_stub: false },
      ],
    };
    const app = makeApp();
    const res = await app.request('/api/incidents?active=true');
    expect(res.status).toBe(200);
    // The handler runs the plain, comparison-free query (robust to the
    // expires_at column type) and filters on the derived is_live flag.
    expect(calls[0]?.sql).not.toContain('expires_at >');
    expect(calls[0]?.sql).toContain('ORDER BY created_at DESC');
    const body = (await res.json()) as Array<Record<string, unknown>>;
    expect(body.map((r) => r['id'])).toEqual(['live']);
  });

  it('returns 503 when pool is null', async () => {
    getPoolReturn = 'null';
    const app = makeApp();
    const res = await app.request('/api/incidents');
    expect(res.status).toBe(503);
    expect(await res.json()).toEqual({ error: 'database unavailable' });
  });
});

describe('GET /api/incidents/:id', () => {
  it('returns 404 when not found', async () => {
    nextResult = { rows: [] };
    const app = makeApp();
    const res = await app.request('/api/incidents/missing');
    expect(res.status).toBe(404);
  });
  it('returns the row when found', async () => {
    nextResult = {
      rows: [{
        id: 'x', title: 't', description: '', lat: 0, lng: 0, location: '',
        type: [], status: 'Going', size: '-', responding_agencies: [],
        created_at: null, updated_at: null, expires_at: null, is_rfs_stub: false,
      }],
    };
    const app = makeApp();
    const res = await app.request('/api/incidents/x');
    expect(res.status).toBe(200);
    expect((await res.json() as { id: string }).id).toBe('x');
  });
});

describe('POST /api/incidents', () => {
  it('inserts without supplied id and returns 201', async () => {
    nextResult = { rows: [{ id: 'new-uuid' }] };
    const app = makeApp();
    const res = await app.request('/api/incidents', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: 'Fire', lat: -33.86, lng: 151.21 }),
    });
    expect(res.status).toBe(201);
    expect(await res.json()).toEqual({ id: 'new-uuid', success: true });
    const sql = calls[0]?.sql ?? '';
    expect(sql).toContain('INSERT INTO incidents');
    expect(sql).not.toContain('ON CONFLICT (id) DO NOTHING');
  });

  it('uses supplied id with ON CONFLICT DO NOTHING', async () => {
    nextResult = { rows: [{ id: 'supplied' }] };
    const app = makeApp();
    await app.request('/api/incidents', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: 'supplied', title: 'F' }),
    });
    expect(calls[0]?.sql).toContain('ON CONFLICT (id) DO NOTHING');
  });

  it('JSON-stringifies type and responding_agencies', async () => {
    nextResult = { rows: [{ id: 'a' }] };
    const app = makeApp();
    await app.request('/api/incidents', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: ['fire'], responding_agencies: ['rfs', 'fire'] }),
    });
    const params = calls[0]?.params ?? [];
    // No-id INSERT param order: title, lat, lng, location, type,
    // description, status, size, responding_agencies, expires_at,
    // is_rfs_stub. So type=4, responding_agencies=8.
    expect(params[4]).toBe('["fire"]');
    expect(params[8]).toBe('["rfs","fire"]');
  });
});

describe('PUT /api/incidents/:id', () => {
  it('returns 400 when no fields are supplied', async () => {
    const app = makeApp();
    const res = await app.request('/api/incidents/x', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ unknown_field: 'ignored' }),
    });
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ error: 'No fields to update' });
  });

  it('builds dynamic SET clause for whitelisted fields and stringifies JSONB', async () => {
    // Gate SELECT finds the incident (owner row), then the UPDATE runs.
    nextResult = { rows: [{ created_by: 'editor-1' }], rowCount: 1 };
    const app = makeApp();
    await app.request('/api/incidents/inc1', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: 'New', type: ['fire'] }),
    });
    const upd = callWith('UPDATE incidents SET');
    expect(upd?.sql).toContain('title = $1');
    expect(upd?.sql).toContain('type = $2');
    expect(upd?.params).toEqual(['New', '["fire"]', 'inc1']);
    // The gate SELECT ran first.
    expect(callWith('SELECT created_by FROM incidents')?.params).toEqual(['inc1']);
  });

  it('sanitizes units (trim/uppercase/dedupe) and upserts the callsign dictionary', async () => {
    nextResult = { rows: [{ created_by: 'editor-1' }], rowCount: 1 };
    const app = makeApp();
    const res = await app.request('/api/incidents/inc1', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ units: [' pum391 ', 'PUM391', 'RFS Wyee 1', 42, ''] }),
    });
    expect(res.status).toBe(200);
    const upd = callWith('UPDATE incidents SET');
    expect(upd?.sql).toContain('units = $1');
    expect(upd?.params?.[0]).toBe(JSON.stringify(['PUM391', 'RFS WYEE 1']));
    // Both surviving callsigns were remembered for tab completion.
    const upserts = calls.filter((c) => c.sql.includes('INSERT INTO callsigns'));
    expect(upserts.map((u) => u.params?.[0])).toEqual(['PUM391', 'RFS WYEE 1']);
  });

  it('GET /api/incidents/callsigns returns the dictionary', async () => {
    nextResult = { rows: [{ callsign: 'PUM391' }, { callsign: 'RFS WYEE 1' }] };
    const app = makeApp();
    const res = await app.request('/api/incidents/callsigns');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ callsigns: ['PUM391', 'RFS WYEE 1'] });
  });

  it('allows a units-only update from a non-owner editor (collaborative field)', async () => {
    canManageUsersMock.mockResolvedValue(false); // not an admin
    nextResult = { rows: [{ created_by: 'someone-else' }], rowCount: 1 };
    const app = makeApp();
    const res = await app.request('/api/incidents/inc1', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ units: ['PUM391'], updated_at: new Date() }),
    });
    expect(res.status).toBe(200);
    canManageUsersMock.mockResolvedValue(true);
  });

  it('403s when a non-owner, non-admin tries to edit', async () => {
    canManageUsersMock.mockResolvedValue(false); // not an admin
    nextResult = { rows: [{ created_by: 'someone-else' }], rowCount: 1 };
    const app = makeApp();
    const res = await app.request('/api/incidents/inc1', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: 'New' }),
    });
    expect(res.status).toBe(403);
    // Must NOT have issued the UPDATE.
    expect(callWith('UPDATE incidents SET')).toBeUndefined();
  });
});

describe('DELETE /api/incidents/:id', () => {
  it('soft-deletes (sets deleted_at) and returns success (owner/admin)', async () => {
    nextResult = { rows: [{ created_by: 'editor-1' }], rowCount: 1 };
    const app = makeApp();
    const res = await app.request('/api/incidents/del-id', { method: 'DELETE' });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ success: true });
    // Soft delete: the row is marked, never removed here — the hourly
    // cleanup purges it after DATA_RETENTION_DAYS.
    const del = callWith('UPDATE incidents SET deleted_at = NOW() WHERE id = $1');
    expect(del?.params).toEqual(['del-id']);
    expect(callWith('DELETE FROM incidents')).toBeUndefined();
    expect(callWith('DELETE FROM incident_updates')).toBeUndefined();
    expect(callWith('DELETE FROM incident_suggestions')).toBeUndefined();
  });

  it('403s when a non-owner, non-admin tries to delete', async () => {
    canManageUsersMock.mockResolvedValue(false);
    nextResult = { rows: [{ created_by: 'someone-else' }], rowCount: 1 };
    const app = makeApp();
    const res = await app.request('/api/incidents/del-id', { method: 'DELETE' });
    expect(res.status).toBe(403);
    expect(callWith('DELETE FROM incidents WHERE id = $1')).toBeUndefined();
  });
});

describe('incident_updates routes', () => {
  it('GET /api/incidents/:id/updates orders by created_at DESC', async () => {
    nextResult = { rows: [] };
    const app = makeApp();
    await app.request('/api/incidents/inc1/updates');
    expect(calls[0]?.sql).toContain('FROM incident_updates WHERE incident_id = $1');
    expect(calls[0]?.sql).toContain('ORDER BY created_at DESC');
  });

  it('POST /api/incidents/:id/updates returns 201 with new id (author stamped)', async () => {
    nextResult = { rows: [{ id: 'upd-1' }] };
    const app = makeApp();
    const res = await app.request('/api/incidents/inc1/updates', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'hello' }),
    });
    expect(res.status).toBe(201);
    expect(await res.json()).toEqual({ id: 'upd-1', success: true });
    // created_by + created_by_name (username only) are recorded.
    expect(callWith('INSERT INTO incident_updates')?.params).toEqual(['inc1', 'hello', 'editor-1', 'Test Editor']);
  });

  it('PUT /api/incidents/updates/:id updates the message column (author/owner/admin)', async () => {
    // Authority SELECT returns the log author + incident owner.
    nextResult = { rows: [{ update_by: 'editor-1', incident_by: 'editor-1' }], rowCount: 1 };
    const app = makeApp();
    const res = await app.request('/api/incidents/updates/u1', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'edited' }),
    });
    expect(res.status).toBe(200);
    const upd = callWith('UPDATE incident_updates SET message = $1 WHERE id = $2');
    expect(upd?.params).toEqual(['edited', 'u1']);
  });

  it('DELETE /api/incidents/updates/:id removes the row (author/owner/admin)', async () => {
    nextResult = { rows: [{ update_by: 'editor-1', incident_by: 'editor-1' }], rowCount: 1 };
    const app = makeApp();
    const res = await app.request('/api/incidents/updates/u1', { method: 'DELETE' });
    expect(res.status).toBe(200);
    expect(callWith('DELETE FROM incident_updates WHERE id = $1')?.params).toEqual(['u1']);
  });

  it('403s when a non-author, non-admin edits a log (author-only)', async () => {
    canManageUsersMock.mockResolvedValue(false); // not an admin either
    nextResult = { rows: [{ update_by: 'other', incident_by: 'editor-1' }], rowCount: 1 };
    const app = makeApp();
    const res = await app.request('/api/incidents/updates/u1', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'x' }),
    });
    // Even the INCIDENT owner can't edit someone else's log entry.
    expect(res.status).toBe(403);
    expect(callWith('UPDATE incident_updates SET message')).toBeUndefined();
    canManageUsersMock.mockResolvedValue(true);
  });

  it('the author may edit their own log entry', async () => {
    canManageUsersMock.mockResolvedValue(false);
    nextResult = { rows: [{ update_by: 'editor-1', incident_by: 'other' }], rowCount: 1 };
    const app = makeApp();
    const res = await app.request('/api/incidents/updates/u1', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'x' }),
    });
    expect(res.status).toBe(200);
    expect(callWith('UPDATE incident_updates SET message')?.params).toEqual(['x', 'u1']);
    canManageUsersMock.mockResolvedValue(true);
  });
});

describe('suggestion workflow', () => {
  it('POST /suggestions creates an edit suggestion (any editor)', async () => {
    resultQueue = [
      { rows: [{ n: 1 }], rowCount: 1 }, // incident exists
      { rows: [{ id: '77' }], rowCount: 1 }, // insert returns id
    ];
    const app = makeApp();
    const res = await app.request('/api/incidents/inc1/suggestions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ kind: 'edit', changes: { title: 'Better title', bogus: 'x' } }),
    });
    expect(res.status).toBe(201);
    expect(await res.json()).toEqual({ id: '77', success: true });
    const ins = callWith('INSERT INTO incident_suggestions');
    // Only the whitelisted field survives into the stored changes JSON.
    expect(ins?.params?.[2]).toBe(JSON.stringify({ title: 'Better title' }));
    expect(ins?.params?.[4]).toBe('editor-1'); // suggested_by
  });

  it('POST /suggestions rejects an unknown kind', async () => {
    const app = makeApp();
    const res = await app.request('/api/incidents/inc1/suggestions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ kind: 'delete' }),
    });
    expect(res.status).toBe(400);
  });

  it('GET /suggestions 403s for a non-owner non-admin', async () => {
    canManageUsersMock.mockResolvedValue(false);
    nextResult = { rows: [{ created_by: 'someone-else' }], rowCount: 1 };
    const app = makeApp();
    const res = await app.request('/api/incidents/inc1/suggestions');
    expect(res.status).toBe(403);
  });

  it('approve auto-applies an edit suggestion and marks it approved', async () => {
    resultQueue = [
      { rows: [{ created_by: 'editor-1' }], rowCount: 1 }, // owner check
      { rows: [{ id: '77', incident_id: 'inc1', kind: 'edit', changes: { status: 'Contained' }, message: null, suggested_by: 'other', suggested_by_name: 'Other', status: 'pending' }], rowCount: 1 }, // load suggestion
      { rows: [], rowCount: 1 }, // UPDATE incidents (apply)
      { rows: [{ id: 'log' }], rowCount: 1 }, // INSERT audit log
      { rows: [], rowCount: 1 }, // UPDATE suggestion status
    ];
    const app = makeApp();
    const res = await app.request('/api/incidents/inc1/suggestions/77/approve', { method: 'POST' });
    expect(res.status).toBe(200);
    // It applied the whitelisted change to the incident...
    const applied = callWith('UPDATE incidents SET');
    expect(applied?.sql).toContain('status = $1');
    // ...and marked the suggestion approved.
    expect(callWith("SET status = 'approved'")).toBeDefined();
  });

  it('reject marks the suggestion rejected (owner/admin)', async () => {
    resultQueue = [
      { rows: [{ created_by: 'editor-1' }], rowCount: 1 }, // owner check
      { rows: [], rowCount: 1 }, // UPDATE ... rejected
    ];
    const app = makeApp();
    const res = await app.request('/api/incidents/inc1/suggestions/77/reject', { method: 'POST' });
    expect(res.status).toBe(200);
    expect(callWith("SET status = 'rejected'")).toBeDefined();
  });
});

describe('incident archive', () => {
  it('POST /:id/archive snapshots + soft-deletes (staff/owner)', async () => {
    resultQueue = [
      { rows: [{ id: 'arc-1', title: 'Major fire', location: 'Somewhere', status: 'Going' }], rowCount: 1 }, // SELECT incident
      { rows: [{ id: 'log1', incident_id: 'arc-1', message: 'm', created_at: '2026-01-01' }] },              // SELECT logs
      { rows: [] }, // INSERT archived_incidents
      { rows: [] }, // UPDATE soft delete
    ];
    const app = makeApp();
    const res = await app.request('/api/incidents/arc-1/archive', { method: 'POST' });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ success: true });
    const ins = callWith('INSERT INTO archived_incidents');
    expect(ins).toBeDefined();
    expect(ins?.params?.[0]).toBe('arc-1');
    expect(ins?.params?.[1]).toBe('Major fire');
    const soft = callWith('UPDATE incidents SET deleted_at = NOW()');
    expect(soft?.params).toEqual(['arc-1']);
  });

  it('403s archive for non-staff editors', async () => {
    canManageUsersMock.mockResolvedValue(false);
    const app = makeApp();
    const res = await app.request('/api/incidents/arc-1/archive', { method: 'POST' });
    expect(res.status).toBe(403);
    expect(callWith('INSERT INTO archived_incidents')).toBeUndefined();
  });

  it('404s archive when the incident is missing or already soft-deleted', async () => {
    resultQueue = [{ rows: [], rowCount: 0 }];
    const app = makeApp();
    const res = await app.request('/api/incidents/ghost/archive', { method: 'POST' });
    expect(res.status).toBe(404);
  });

  it('GET /archived returns results + total and searches with ILIKE', async () => {
    nextResult = {
      rows: [{ id: 'arc-1', title: 'Major fire', location: 'X', status: 'Going', type: '["Bush Fire"]', original_created_at: '2026-01-01', archived_at: '2026-01-02', total: '1' }],
    };
    const app = makeApp();
    const res = await app.request('/api/incidents/archived?q=fire&limit=5');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { total: number; results: Array<Record<string, unknown>> };
    expect(body.total).toBe(1);
    expect(body.results[0]?.['title']).toBe('Major fire');
    const sel = callWith('FROM archived_incidents');
    expect(sel?.sql).toContain('ILIKE');
    expect(sel?.params?.[0]).toBe('%fire%');
  });

  it('GET /archived/:id returns the snapshot or 404', async () => {
    nextResult = { rows: [{ id: 'arc-1', title: 'Major fire', incident: {}, logs: [] }] };
    const app = makeApp();
    const ok = await app.request('/api/incidents/archived/arc-1');
    expect(ok.status).toBe(200);

    nextResult = { rows: [] };
    const miss = await app.request('/api/incidents/archived/nope');
    expect(miss.status).toBe(404);
  });
});
