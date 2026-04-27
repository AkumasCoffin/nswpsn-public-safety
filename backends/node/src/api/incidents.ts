/**
 * Incidents CRUD + incident-update CRUD.
 *
 * Mirrors python external_api_proxy.py:14174-14391. The DB schema is
 * defined by python's init_postgres.py (incidents + incident_updates
 * tables, both already deployed). We don't write a new migration —
 * we just read/write the existing columns.
 *
 * Routes (every one requires NSWPSN_API_KEY via the parent router's
 * requireApiKey middleware):
 *   GET    /api/incidents                       — list, optional ?active=true filter
 *   GET    /api/incidents/:id                   — single (additive — Python doesn't have it,
 *                                                  but it's idiomatic for any client that already
 *                                                  knows an id and wants a single-row refresh
 *                                                  without scanning the full list)
 *   POST   /api/incidents                       — create
 *   PUT    /api/incidents/:id                   — partial update (whitelist of editable cols)
 *   DELETE /api/incidents/:id                   — delete (cascades to incident_updates)
 *   GET    /api/incidents/:id/updates           — list updates for an incident
 *   POST   /api/incidents/:id/updates           — append an update
 *   PUT    /api/incidents/updates/:updateId     — edit one update
 *   DELETE /api/incidents/updates/:updateId     — delete one update
 *
 * Response shapes are byte-for-byte python:
 *   - JSONB columns `type` and `responding_agencies` are normalised to
 *     arrays in the response (python json.loads's them when they come
 *     back as strings; `pg` already gives us native arrays/objects, so
 *     we only do the loads-fallback for the case where the column was
 *     stored as a string — defensive parity).
 *   - Timestamps are ISO strings (python's `.isoformat()`).
 *   - Successful create returns 201 with `{ id, success: true }`.
 *   - Errors are 500 with `{ error: '<message>' }` matching python's
 *     literal strings.
 */
import { Hono } from 'hono';
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';

export const incidentsRouter = new Hono();

const DB_UNAVAILABLE = { error: 'database unavailable' } as const;

// Whitelist of columns that PUT /api/incidents/:id may update. Mirrors
// the python `allowed` list at external_api_proxy.py:14251.
const UPDATABLE_FIELDS = [
  'title',
  'description',
  'lat',
  'lng',
  'location',
  'type',
  'status',
  'size',
  'responding_agencies',
  'expires_at',
  'updated_at',
] as const;
type UpdatableField = (typeof UPDATABLE_FIELDS)[number];
const JSONB_FIELDS: ReadonlySet<string> = new Set(['type', 'responding_agencies']);

interface IncidentRow {
  id: string;
  title: string | null;
  description: string | null;
  lat: number | null;
  lng: number | null;
  location: string | null;
  type: unknown;
  status: string | null;
  size: string | null;
  responding_agencies: unknown;
  created_at: Date | string | null;
  updated_at: Date | string | null;
  expires_at: Date | string | null;
  is_rfs_stub: boolean | null;
}

function normaliseIncident(row: IncidentRow): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  for (const k of ['type', 'responding_agencies']) {
    const v = out[k];
    if (typeof v === 'string') {
      try {
        out[k] = JSON.parse(v);
      } catch {
        // leave as-is; python's json.loads would also raise here, but it
        // wraps the whole handler in a try/except returning 500. The row
        // is already corrupt in the DB if we reach this branch.
      }
    }
  }
  for (const k of ['created_at', 'updated_at', 'expires_at']) {
    const v = out[k];
    if (v instanceof Date) {
      out[k] = v.toISOString();
    }
  }
  return out;
}

interface IncidentUpdateRow {
  id: string;
  incident_id: string;
  message: string | null;
  created_at: Date | string | null;
}

function normaliseIncidentUpdate(row: IncidentUpdateRow): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  if (out['created_at'] instanceof Date) {
    out['created_at'] = (out['created_at'] as Date).toISOString();
  }
  return out;
}

async function withPool<T>(
  fn: (pool: Pool) => Promise<T>,
): Promise<T | { _dbUnavailable: true }> {
  const pool = await getPool();
  if (!pool) return { _dbUnavailable: true };
  return fn(pool);
}

function isUnavailable<T>(v: T | { _dbUnavailable: true }): v is { _dbUnavailable: true } {
  return typeof v === 'object' && v !== null && (v as { _dbUnavailable?: boolean })._dbUnavailable === true;
}

// ---------------------------------------------------------------------------
// GET /api/incidents
// ---------------------------------------------------------------------------
incidentsRouter.get('/api/incidents', async (c) => {
  try {
    const url = new URL(c.req.url);
    const activeOnly = url.searchParams.get('active') === 'true';

    const result = await withPool(async (pool) => {
      const sql = activeOnly
        ? 'SELECT * FROM incidents WHERE expires_at > now() ORDER BY created_at DESC'
        : 'SELECT * FROM incidents ORDER BY created_at DESC';
      const r = await pool.query<IncidentRow>(sql);
      return r.rows.map(normaliseIncident);
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json(result);
  } catch (err) {
    log.error({ err }, 'Error fetching incidents');
    return c.json({ error: 'Failed to fetch incidents' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/incidents/:id  (additive vs python — see file header)
// ---------------------------------------------------------------------------
incidentsRouter.get('/api/incidents/:id', async (c) => {
  const id = c.req.param('id');
  // Don't let the bare /:id route swallow `/updates/<x>` paths — that
  // collision is theoretical (Hono routes longer paths first) but cheap
  // to defend against.
  if (id === 'updates') return c.notFound();
  try {
    const result = await withPool(async (pool) => {
      const r = await pool.query<IncidentRow>(
        'SELECT * FROM incidents WHERE id = $1',
        [id],
      );
      return r.rows[0] ? normaliseIncident(r.rows[0]) : null;
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    if (!result) return c.json({ error: 'Incident not found' }, 404);
    return c.json(result);
  } catch (err) {
    log.error({ err, id }, 'Error fetching incident');
    return c.json({ error: 'Failed to fetch incident' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/incidents
// ---------------------------------------------------------------------------
incidentsRouter.post('/api/incidents', async (c) => {
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const result = await withPool(async (pool) => {
      const title = (data['title'] as string | undefined) ?? '';
      const lat = (data['lat'] as number | undefined) ?? 0;
      const lng = (data['lng'] as number | undefined) ?? 0;
      const location = (data['location'] as string | undefined) ?? '';
      const typeJson = JSON.stringify(data['type'] ?? []);
      const description = (data['description'] as string | undefined) ?? '';
      const status = (data['status'] as string | undefined) ?? 'Going';
      const size = (data['size'] as string | undefined) ?? '-';
      const agenciesJson = JSON.stringify(data['responding_agencies'] ?? []);
      const expiresAt = data['expires_at'] ?? null;
      const isRfsStub = (data['is_rfs_stub'] as boolean | undefined) ?? false;

      if (typeof data['id'] === 'string' && data['id']) {
        const r = await pool.query<{ id: string }>(
          `INSERT INTO incidents
            (id, title, lat, lng, location, type, description, status, size, responding_agencies, expires_at, is_rfs_stub)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
          ON CONFLICT (id) DO NOTHING
          RETURNING id`,
          [data['id'], title, lat, lng, location, typeJson, description, status, size, agenciesJson, expiresAt, isRfsStub],
        );
        return r.rows[0]?.id ?? null;
      }
      const r = await pool.query<{ id: string }>(
        `INSERT INTO incidents
          (title, lat, lng, location, type, description, status, size, responding_agencies, expires_at, is_rfs_stub)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        RETURNING id`,
        [title, lat, lng, location, typeJson, description, status, size, agenciesJson, expiresAt, isRfsStub],
      );
      return r.rows[0]?.id ?? null;
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json({ id: result, success: true }, 201);
  } catch (err) {
    log.error({ err }, 'Error creating incident');
    return c.json({ error: 'Failed to create incident' }, 500);
  }
});

// ---------------------------------------------------------------------------
// PUT /api/incidents/:id
// ---------------------------------------------------------------------------
incidentsRouter.put('/api/incidents/:id', async (c) => {
  const id = c.req.param('id');
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const sets: string[] = [];
    const vals: unknown[] = [];
    for (const key of UPDATABLE_FIELDS) {
      if (Object.prototype.hasOwnProperty.call(data, key)) {
        const raw = data[key as UpdatableField];
        const val = JSONB_FIELDS.has(key) ? JSON.stringify(raw) : raw;
        sets.push(`${key} = $${sets.length + 1}`);
        vals.push(val);
      }
    }
    if (sets.length === 0) {
      return c.json({ error: 'No fields to update' }, 400);
    }
    vals.push(id);
    const sql = `UPDATE incidents SET ${sets.join(', ')} WHERE id = $${vals.length}`;
    const result = await withPool(async (pool) => {
      await pool.query(sql, vals);
      return true;
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'Error updating incident');
    return c.json({ error: 'Failed to update incident' }, 500);
  }
});

// ---------------------------------------------------------------------------
// DELETE /api/incidents/:id
// ---------------------------------------------------------------------------
incidentsRouter.delete('/api/incidents/:id', async (c) => {
  const id = c.req.param('id');
  if (id === 'updates') return c.notFound();
  try {
    const result = await withPool(async (pool) => {
      await pool.query('DELETE FROM incidents WHERE id = $1', [id]);
      return true;
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'Error deleting incident');
    return c.json({ error: 'Failed to delete incident' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/incidents/:id/updates
// ---------------------------------------------------------------------------
incidentsRouter.get('/api/incidents/:id/updates', async (c) => {
  const id = c.req.param('id');
  try {
    const result = await withPool(async (pool) => {
      const r = await pool.query<IncidentUpdateRow>(
        'SELECT * FROM incident_updates WHERE incident_id = $1 ORDER BY created_at DESC',
        [id],
      );
      return r.rows.map(normaliseIncidentUpdate);
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json(result);
  } catch (err) {
    log.error({ err, id }, 'Error fetching incident updates');
    return c.json({ error: 'Failed to fetch incident updates' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/incidents/:id/updates
// ---------------------------------------------------------------------------
incidentsRouter.post('/api/incidents/:id/updates', async (c) => {
  const id = c.req.param('id');
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const message = (data['message'] as string | undefined) ?? '';
    const result = await withPool(async (pool) => {
      const r = await pool.query<{ id: string }>(
        'INSERT INTO incident_updates (incident_id, message) VALUES ($1, $2) RETURNING id',
        [id, message],
      );
      return r.rows[0]?.id ?? null;
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json({ id: result, success: true }, 201);
  } catch (err) {
    log.error({ err, id }, 'Error creating incident update');
    return c.json({ error: 'Failed to create incident update' }, 500);
  }
});

// ---------------------------------------------------------------------------
// PUT /api/incidents/updates/:updateId
// ---------------------------------------------------------------------------
incidentsRouter.put('/api/incidents/updates/:updateId', async (c) => {
  const updateId = c.req.param('updateId');
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const message = (data['message'] as string | undefined) ?? '';
    const result = await withPool(async (pool) => {
      await pool.query(
        'UPDATE incident_updates SET message = $1 WHERE id = $2',
        [message, updateId],
      );
      return true;
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, updateId }, 'Error updating incident update');
    return c.json({ error: 'Failed to update incident update' }, 500);
  }
});

// ---------------------------------------------------------------------------
// DELETE /api/incidents/updates/:updateId
// ---------------------------------------------------------------------------
incidentsRouter.delete('/api/incidents/updates/:updateId', async (c) => {
  const updateId = c.req.param('updateId');
  try {
    const result = await withPool(async (pool) => {
      await pool.query('DELETE FROM incident_updates WHERE id = $1', [updateId]);
      return true;
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, updateId }, 'Error deleting incident update');
    return c.json({ error: 'Failed to delete incident update' }, 500);
  }
});
