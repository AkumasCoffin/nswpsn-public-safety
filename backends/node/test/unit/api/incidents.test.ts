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
let nextResult: { rows: unknown[] } = { rows: [] };
let getPoolReturn: 'pool' | 'null' = 'pool';

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return nextResult;
  }),
  connect: vi.fn(),
};

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => (getPoolReturn === 'pool' ? fakePool : null)),
}));

const { incidentsRouter } = await import('../../../src/api/incidents.js');

function makeApp() {
  const app = new Hono();
  app.route('/', incidentsRouter);
  return app;
}

beforeEach(() => {
  calls.length = 0;
  nextResult = { rows: [] };
  getPoolReturn = 'pool';
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

  it('?active=true filters by expires_at', async () => {
    const app = makeApp();
    await app.request('/api/incidents?active=true');
    expect(calls[0]?.sql).toContain('expires_at > now()');
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
    const app = makeApp();
    await app.request('/api/incidents/inc1', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: 'New', type: ['fire'] }),
    });
    const sql = calls[0]?.sql ?? '';
    const params = calls[0]?.params ?? [];
    expect(sql).toContain('UPDATE incidents SET');
    expect(sql).toContain('title = $1');
    expect(sql).toContain('type = $2');
    expect(params).toEqual(['New', '["fire"]', 'inc1']);
  });
});

describe('DELETE /api/incidents/:id', () => {
  it('issues DELETE and returns success', async () => {
    const app = makeApp();
    const res = await app.request('/api/incidents/del-id', { method: 'DELETE' });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ success: true });
    expect(calls[0]?.sql).toContain('DELETE FROM incidents WHERE id = $1');
    expect(calls[0]?.params).toEqual(['del-id']);
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

  it('POST /api/incidents/:id/updates returns 201 with new id', async () => {
    nextResult = { rows: [{ id: 'upd-1' }] };
    const app = makeApp();
    const res = await app.request('/api/incidents/inc1/updates', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'hello' }),
    });
    expect(res.status).toBe(201);
    expect(await res.json()).toEqual({ id: 'upd-1', success: true });
    expect(calls[0]?.params).toEqual(['inc1', 'hello']);
  });

  it('PUT /api/incidents/updates/:id updates the message column', async () => {
    const app = makeApp();
    const res = await app.request('/api/incidents/updates/u1', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'edited' }),
    });
    expect(res.status).toBe(200);
    expect(calls[0]?.sql).toContain('UPDATE incident_updates SET message = $1 WHERE id = $2');
    expect(calls[0]?.params).toEqual(['edited', 'u1']);
  });

  it('DELETE /api/incidents/updates/:id removes the row', async () => {
    const app = makeApp();
    const res = await app.request('/api/incidents/updates/u1', { method: 'DELETE' });
    expect(res.status).toBe(200);
    expect(calls[0]?.sql).toContain('DELETE FROM incident_updates WHERE id = $1');
    expect(calls[0]?.params).toEqual(['u1']);
  });
});
