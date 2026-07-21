/**
 * Incidents CRUD + incident-update CRUD.
 *
 * Mirrors python external_api_proxy.py:14174-14391. The DB schema is
 * defined by python's init_postgres.py (incidents + incident_updates
 * tables, both already deployed). We don't write a new migration —
 * we just read/write the existing columns.
 *
 * Routes (reads require NSWPSN_API_KEY via the parent router's
 * requireApiKey middleware; mutating routes additionally require a
 * logged-in editor — see requireRole(canEditIncidents) below, added
 * because the API key is public via /api/config and key-only gating
 * left incident create/update/delete open to anyone):
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
import {
  requireRole,
  canEditIncidents,
  canManageUsers,
} from '../services/auth/roles.js';
import { archiveWriter } from '../store/archive.js';
import {
  userIncidentArchiveRow,
  type UserIncidentRow,
} from '../sources/userIncidents.js';
import { randomUUID } from 'node:crypto';
import {
  MAX_IMAGE_BYTES,
  MAX_IMAGES_PER_INCIDENT,
  ImageTooLargeError,
  ImageTypeMismatchError,
  deleteIncidentImageFile,
  isSafeIdSegment,
  normaliseContentType,
  parseIncidentImages,
  saveIncidentImageStream,
  type IncidentImage,
} from '../services/incidentImages.js';
import { MetadataStripError } from '../services/imageMetadata.js';

export const incidentsRouter = new Hono();

// Baseline gate: every mutating incident route requires an editor login
// (owner / team_member / map_editor). Registered before the handlers so
// it runs for POST/PUT/DELETE only — reads stay key-gated for the map/live
// pages and the discord bot. Per-incident OWNERSHIP (only the creator or a
// site admin may edit/delete a given incident) is enforced inside the
// individual handlers via assertCanModifyIncident, since it needs a DB
// lookup of the incident's created_by.
incidentsRouter.use('/api/incidents', async (c, next) => {
  if (c.req.method === 'GET') return next();
  return requireRole(canEditIncidents)(c, next);
});
incidentsRouter.use('/api/incidents/*', async (c, next) => {
  if (c.req.method === 'GET') return next();
  return requireRole(canEditIncidents)(c, next);
});

/** The verified Supabase user id set by optionalSupabaseJwt, or undefined. */
function currentUserId(c: { get: (k: string) => unknown }): string | undefined {
  const v = c.get('userId');
  return typeof v === 'string' && v.length > 0 ? v : undefined;
}

/**
 * True when `userId` may edit/delete an incident whose creator is
 * `createdBy`: the creator themselves, OR a site admin (owner/team_member).
 * Legacy incidents with a NULL creator are admin-only. This is the
 * ownership rule on top of the baseline canEditIncidents gate.
 */
async function userCanModifyIncident(
  userId: string | undefined,
  createdBy: string | null,
): Promise<boolean> {
  if (!userId) return false;
  if (createdBy && createdBy === userId) return true;
  return canManageUsers(userId); // owner / team_member override
}

/**
 * Authority to edit/delete a single log line: its author, the parent
 * incident's creator, or a site admin. `updateBy` is the log author,
 * `incidentBy` the incident creator (both may be null on legacy rows).
 */
function currentUserName(c: { get: (k: string) => unknown }): string | null {
  const v = c.get('userName');
  return typeof v === 'string' && v ? v : null;
}

/** Log entries may only be edited/deleted by their AUTHOR (site admins
 * retain a moderation override). */
async function userCanModifyUpdate(
  userId: string | undefined,
  updateBy: string | null,
): Promise<boolean> {
  if (!userId) return false;
  if (updateBy && updateBy === userId) return true;
  return canManageUsers(userId);
}

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
  'units',
  'expires_at',
  'updated_at',
] as const;
type UpdatableField = (typeof UPDATABLE_FIELDS)[number];
const JSONB_FIELDS: ReadonlySet<string> = new Set(['type', 'responding_agencies', 'units']);

/**
 * Sanitize an editor-supplied units list: strings only, trimmed,
 * uppercased, de-duped, bounded (24 chars each, 50 units max) so a
 * malformed payload can't bloat the row or the callsign dictionary.
 */
export function sanitizeUnits(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const u of raw) {
    if (typeof u !== 'string') continue;
    const s = u.trim().toUpperCase().slice(0, 24);
    if (!s || seen.has(s)) continue;
    seen.add(s);
    out.push(s);
    if (out.length >= 50) break;
  }
  return out;
}

/** Upsert saved callsigns into the persistent dictionary (best-effort —
 * a failure here must never fail the incident save). */
async function rememberCallsigns(pool: Pool, units: string[]): Promise<void> {
  for (const cs of units) {
    try {
      await pool.query(
        `INSERT INTO callsigns (callsign) VALUES ($1)
         ON CONFLICT (callsign) DO UPDATE
           SET last_used = now(), use_count = callsigns.use_count + 1`,
        [cs],
      );
    } catch (err) {
      log.warn({ err, cs }, 'callsigns: upsert failed');
      return;
    }
  }
}

/**
 * Coerce an untrusted JSON value into a finite coordinate number.
 * Accepts numbers and numeric strings; returns null for anything that
 * isn't a finite number so callers can reject/null it rather than
 * passing strings or NaN through to the geo columns.
 */
function coerceCoord(raw: unknown): number | null {
  if (typeof raw === 'number') return Number.isFinite(raw) ? raw : null;
  if (typeof raw === 'string' && raw.trim() !== '') {
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

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
  created_by: string | null;
}

function normaliseIncident(row: IncidentRow): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  for (const k of ['type', 'responding_agencies', 'units', 'images']) {
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
  // Compute is_live from expires_at — the python schema doesn't have
  // an is_live column on the incidents table, but the frontend (live.html
  // / map.html) expects every record to carry one. Derive it before the
  // Date->isoformat coercion below so we still have the raw timestamp.
  const expires = out['expires_at'];
  let expiresMs: number | null = null;
  if (expires instanceof Date) expiresMs = expires.getTime();
  else if (typeof expires === 'string' && expires) {
    const t = Date.parse(expires);
    expiresMs = Number.isFinite(t) ? t : null;
  }
  out['is_live'] = expiresMs === null ? false : expiresMs > Date.now();
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
  created_by: string | null;
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
      // Always run the plain, comparison-free query and apply the
      // active/expiry filter in JS off the `is_live` field that
      // normaliseIncident already derives. The previous SQL
      // `WHERE expires_at > now()` 500s when the deployed incidents table
      // (created by the legacy python init_postgres.py, not migration 001)
      // stores expires_at as TEXT — `text > timestamptz` has no operator.
      // Filtering in JS makes active=true exactly as robust as the
      // unfiltered list (which works), regardless of the column's type.
      const r = await pool.query<IncidentRow>(
        'SELECT * FROM incidents WHERE deleted_at IS NULL ORDER BY created_at DESC',
      );
      const rows = r.rows.map(normaliseIncident);
      return activeOnly ? rows.filter((row) => row['is_live'] === true) : rows;
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json(result);
  } catch (err) {
    log.error({ err }, 'Error fetching incidents');
    return c.json({ error: 'Failed to fetch incidents' }, 500);
  }
});

// ---------------------------------------------------------------------------
// ARCHIVE — permanent snapshots of major incidents (staff/owner only).
// Archived rows live in their own table, are exempt from data retention,
// and are publicly searchable from the logs page.
// ---------------------------------------------------------------------------

// GET /api/incidents/callsigns — the persistent callsign dictionary for
// the editor's unit-input tab completion. Registered before /:id so the
// static segment wins the route match. Tolerates a missing table (un-run
// migration) by returning an empty list rather than 500ing the editor.
incidentsRouter.get('/api/incidents/callsigns', async (c) => {
  try {
    const result = await withPool(async (pool) => {
      try {
        const r = await pool.query<{ callsign: string }>(
          'SELECT callsign FROM callsigns ORDER BY use_count DESC, last_used DESC LIMIT 500',
        );
        return r.rows.map((row) => row.callsign);
      } catch {
        return [] as string[];
      }
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json({ callsigns: result });
  } catch (err) {
    log.error({ err }, 'Error fetching callsigns');
    return c.json({ error: 'Failed to fetch callsigns' }, 500);
  }
});

// GET /api/incidents/archived — public search (?q=&limit=&offset=).
// Registered before /:id so the static segment wins the route match.
incidentsRouter.get('/api/incidents/archived', async (c) => {
  try {
    const url = new URL(c.req.url);
    const q = (url.searchParams.get('q') ?? '').trim();
    const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') ?? 20) || 20));
    const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

    const result = await withPool(async (pool) => {
      const vals: unknown[] = [];
      let where = '';
      if (q) {
        vals.push(`%${q}%`);
        where = `WHERE (title ILIKE $1 OR location ILIKE $1 OR incident->>'description' ILIKE $1)`;
      }
      vals.push(limit, offset);
      const r = await pool.query(
        `SELECT id, title, location, archived_at,
                incident->>'status' AS status,
                incident->'type' AS type,
                incident->>'created_at' AS original_created_at,
                COUNT(*) OVER() AS total
           FROM archived_incidents ${where}
          ORDER BY archived_at DESC
          LIMIT $${vals.length - 1} OFFSET $${vals.length}`,
        vals,
      );
      const total = r.rows.length > 0 ? Number(r.rows[0].total) || 0 : 0;
      return {
        total,
        results: r.rows.map((row) => ({
          id: row.id,
          title: row.title,
          location: row.location,
          status: row.status,
          type: row.type,
          original_created_at: row.original_created_at,
          archived_at: row.archived_at,
        })),
      };
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    return c.json(result);
  } catch (err) {
    log.error({ err }, 'Error searching archived incidents');
    return c.json({ error: 'Failed to search archive' }, 500);
  }
});

// GET /api/incidents/archived/:id — public detail (snapshot + logs).
incidentsRouter.get('/api/incidents/archived/:id', async (c) => {
  const id = c.req.param('id');
  try {
    const result = await withPool(async (pool) => {
      const r = await pool.query(
        'SELECT * FROM archived_incidents WHERE id = $1',
        [id],
      );
      return r.rows[0] ?? null;
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    if (!result) return c.json({ error: 'Archived incident not found' }, 404);
    return c.json(result);
  } catch (err) {
    log.error({ err, id }, 'Error fetching archived incident');
    return c.json({ error: 'Failed to fetch archived incident' }, 500);
  }
});

// POST /api/incidents/:id/archive — staff/owner only. Snapshots the live
// incident + its logs into archived_incidents (upsert), then soft-deletes
// the live pin so it leaves the map; the archive copy stays forever.
incidentsRouter.post('/api/incidents/:id/archive', requireRole(canManageUsers), async (c) => {
  const id = c.req.param('id');
  try {
    const result = await withPool(async (pool) => {
      const r = await pool.query<IncidentRow>(
        'SELECT * FROM incidents WHERE id = $1 AND deleted_at IS NULL',
        [id],
      );
      if (r.rowCount === 0) return { notFound: true as const };
      const inc = normaliseIncident(r.rows[0]!);

      const logs = await pool.query<IncidentUpdateRow>(
        'SELECT * FROM incident_updates WHERE incident_id = $1 ORDER BY created_at ASC',
        [id],
      );
      const logsJson = logs.rows.map(normaliseIncidentUpdate);

      await pool.query(
        `INSERT INTO archived_incidents (id, title, location, incident, logs, archived_by)
         VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)
         ON CONFLICT (id) DO UPDATE SET
           title = EXCLUDED.title,
           location = EXCLUDED.location,
           incident = EXCLUDED.incident,
           logs = EXCLUDED.logs,
           archived_at = NOW(),
           archived_by = EXCLUDED.archived_by`,
        [
          id,
          (inc['title'] as string) || '',
          (inc['location'] as string) || null,
          JSON.stringify(inc),
          JSON.stringify(logsJson),
          currentUserId(c) ?? null,
        ],
      );
      // Off the live map; the archive copy is the permanent record.
      await pool.query('UPDATE incidents SET deleted_at = NOW() WHERE id = $1', [id]);
      // Pull the incident's snapshots out of the regular logs feed —
      // archived incidents live ONLY in the "Archived Incidents" area,
      // non-archived ones stay in the feed until retention. Best-effort:
      // a missing archive table must not fail the archive action itself.
      try {
        await pool.query(
          `DELETE FROM archive_misc WHERE source = 'user_incident' AND source_id = $1`,
          [id],
        );
        await pool.query(
          `DELETE FROM archive_misc_latest WHERE source = 'user_incident' AND source_id = $1`,
          [id],
        );
      } catch (err) {
        log.warn({ err, id }, 'archive: failed to remove logs-feed snapshots');
      }
      return { ok: true as const };
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    if ('notFound' in result) return c.json({ error: 'Incident not found' }, 404);
    log.info({ id }, 'Incident archived');
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'Error archiving incident');
    return c.json({ error: 'Failed to archive incident' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/incidents/:id  (additive vs python — see file header)
// ---------------------------------------------------------------------------
incidentsRouter.get('/api/incidents/:id', async (c) => {
  const id = c.req.param('id');
  if (id === 'archived') return c.notFound();
  // Don't let the bare /:id route swallow `/updates/<x>` paths — that
  // collision is theoretical (Hono routes longer paths first) but cheap
  // to defend against.
  if (id === 'updates') return c.notFound();
  try {
    const result = await withPool(async (pool) => {
      const r = await pool.query<IncidentRow>(
        'SELECT * FROM incidents WHERE id = $1 AND deleted_at IS NULL',
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
    // Stamp the creating editor so edit/delete can later be restricted to
    // them (or a site admin). Guaranteed present — the baseline gate above
    // requires a verified editor JWT.
    const createdBy = currentUserId(c) ?? null;
    const result = await withPool(async (pool) => {
      const title = (data['title'] as string | undefined) ?? '';
      const lat = coerceCoord(data['lat']) ?? 0;
      const lng = coerceCoord(data['lng']) ?? 0;
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
            (id, title, lat, lng, location, type, description, status, size, responding_agencies, expires_at, is_rfs_stub, created_by)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
          ON CONFLICT (id) DO NOTHING
          RETURNING id`,
          [data['id'], title, lat, lng, location, typeJson, description, status, size, agenciesJson, expiresAt, isRfsStub, createdBy],
        );
        return r.rows[0]?.id ?? null;
      }
      const r = await pool.query<{ id: string }>(
        `INSERT INTO incidents
          (title, lat, lng, location, type, description, status, size, responding_agencies, expires_at, is_rfs_stub, created_by)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        RETURNING id`,
        [title, lat, lng, location, typeJson, description, status, size, agenciesJson, expiresAt, isRfsStub, createdBy],
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
    // Units are COLLABORATIVE dispatch info: any editor may attach or
    // remove callsigns on any incident (incl. shared RFS/pager stubs),
    // so a units-only update bypasses the creator-or-admin gate that
    // protects the descriptive fields.
    const unitsOnly =
      Object.prototype.hasOwnProperty.call(data, 'units') &&
      Object.keys(data).every((k) => k === 'units' || k === 'updated_at');

    // Ownership gate: only the creator or a site admin may edit. A
    // non-owner editor must use the suggestion flow instead.
    const gate = await withPool(async (pool) => {
      const r = await pool.query<{ created_by: string | null }>(
        'SELECT created_by FROM incidents WHERE id = $1 AND deleted_at IS NULL',
        [id],
      );
      if (r.rowCount === 0) return { notFound: true as const };
      return { createdBy: r.rows[0]?.created_by ?? null };
    });
    if (isUnavailable(gate)) return c.json(DB_UNAVAILABLE, 503);
    if ('notFound' in gate) return c.json({ error: 'Incident not found' }, 404);
    if (!unitsOnly && !(await userCanModifyIncident(currentUserId(c), gate.createdBy))) {
      return c.json(
        { error: 'Only the incident creator or an admin can edit it; suggest an edit instead.' },
        403,
      );
    }

    const sets: string[] = [];
    const vals: unknown[] = [];
    for (const key of UPDATABLE_FIELDS) {
      if (Object.prototype.hasOwnProperty.call(data, key)) {
        const raw = data[key as UpdatableField];
        let val: unknown;
        if (key === 'units') {
          val = JSON.stringify(sanitizeUnits(raw));
        } else if (JSONB_FIELDS.has(key)) {
          val = JSON.stringify(raw);
        } else if (key === 'lat' || key === 'lng') {
          // Never trust unvalidated JSON straight into the geo columns —
          // reject non-finite values rather than persisting a string/NaN.
          const coord = coerceCoord(raw);
          if (coord === null) {
            return c.json({ error: `Invalid ${key}` }, 400);
          }
          val = coord;
        } else {
          val = raw;
        }
        sets.push(`${key} = $${sets.length + 1}`);
        vals.push(val);
      }
    }
    if (sets.length === 0) {
      return c.json({ error: 'No fields to update' }, 400);
    }
    vals.push(id);
    const sql = `UPDATE incidents SET ${sets.join(', ')} WHERE id = $${vals.length}`;
    const savedUnits = Object.prototype.hasOwnProperty.call(data, 'units')
      ? sanitizeUnits(data['units'])
      : [];
    const result = await withPool(async (pool) => {
      await pool.query(sql, vals);
      // Feed the callsign dictionary so future unit inputs can
      // tab-complete what anyone has typed before.
      if (savedUnits.length) await rememberCallsigns(pool, savedUnits);
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
      // Ownership gate — only the creator or a site admin may delete.
      const owner = await pool.query<{ created_by: string | null }>(
        'SELECT created_by FROM incidents WHERE id = $1 AND deleted_at IS NULL',
        [id],
      );
      if (owner.rowCount === 0) return { notFound: true as const };
      if (!(await userCanModifyIncident(currentUserId(c), owner.rows[0]?.created_by ?? null))) {
        return { forbidden: true as const };
      }
      // Soft delete: hide the pin (and with it, its logs + suggestions)
      // from the API but keep everything in the database. The hourly
      // cleanup hard-deletes rows once deleted_at ages past
      // DATA_RETENTION_DAYS, so accidental deletes stay recoverable.
      const deleted = await pool.query<UserIncidentRow & { is_rfs_stub: boolean | null }>(
        'UPDATE incidents SET deleted_at = NOW() WHERE id = $1 RETURNING *',
        [id],
      );
      // Push a final is_active:false snapshot to the logs-page archive.
      // The user_incidents poller no longer sees soft-deleted rows, so
      // without this the incident's last archived state would read
      // "active" until retention drops it.
      const row = deleted.rows[0];
      if (row && row.is_rfs_stub !== true) {
        archiveWriter.push(
          'archive_misc',
          userIncidentArchiveRow(row, Math.floor(Date.now() / 1000), {
            forceInactive: true,
          }),
        );
      }
      return { ok: true as const };
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    if ('notFound' in result) return c.json({ error: 'Incident not found' }, 404);
    if ('forbidden' in result) {
      return c.json(
        { error: 'Only the incident creator or an admin can delete it.' },
        403,
      );
    }
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'Error deleting incident');
    return c.json({ error: 'Failed to delete incident' }, 500);
  }
});

// ---------------------------------------------------------------------------
// INCIDENT IMAGES
//
// POST   /api/incidents/:id/images            — upload one photo (raw body)
// DELETE /api/incidents/:id/images/:imageId   — remove one photo
//
// Uploading is collaborative (any editor, like units) so a responding
// editor can add photos to someone else's pin; DELETING is restricted to
// the uploader, with the usual admin moderation override.
//
// `images` is deliberately absent from UPDATABLE_FIELDS: the column is
// only writable through these two handlers, so a crafted PUT can't forge
// entries pointing at arbitrary paths.
// ---------------------------------------------------------------------------

// Bound concurrent uploads: each holds up to MAX_IMAGE_BYTES of streamed
// disk + the metadata-strip pass. Without a global rate limiter this cap
// is what stops a burst of parallel uploads from exhausting the box.
const MAX_CONCURRENT_UPLOADS = 6;
let _uploadsInFlight = 0;
function acquireUploadSlot(): boolean {
  if (_uploadsInFlight >= MAX_CONCURRENT_UPLOADS) return false;
  _uploadsInFlight += 1;
  return true;
}
function releaseUploadSlot(): void {
  if (_uploadsInFlight > 0) _uploadsInFlight -= 1;
}

/** Read the current images array under a row lock. Returns null if the
 *  incident is missing or soft-deleted. */
async function lockIncidentImages(
  client: { query: (sql: string, params?: unknown[]) => Promise<{ rows: Array<{ images: unknown }>; rowCount: number | null }> },
  id: string,
): Promise<IncidentImage[] | null> {
  const r = await client.query(
    'SELECT images FROM incidents WHERE id = $1 AND deleted_at IS NULL FOR UPDATE',
    [id],
  );
  if (!r.rowCount) return null;
  return parseIncidentImages(r.rows[0]?.images);
}

incidentsRouter.post(
  '/api/incidents/:id/images',
  // NOTE: deliberately NOT using hono's bodyLimit here. On a chunked
  // request (no Content-Length) it buffers the ENTIRE body into memory
  // before the handler runs — a 50MB heap allocation per request, which
  // is exactly the OOM the streaming write avoids. saveIncidentImageStream
  // enforces MAX_IMAGE_BYTES against bytes as they arrive instead, and the
  // Content-Length pre-check below rejects an honest oversized upload for
  // free.
  async (c) => {
    const id = c.req.param('id');
    if (!isSafeIdSegment(id)) return c.json({ error: 'Incident not found' }, 404);

    const contentType = normaliseContentType(c.req.header('content-type'));
    if (!contentType) {
      return c.json(
        { error: 'Unsupported image type. Use JPEG, PNG, WebP or GIF.' },
        415,
      );
    }
    // Cheap early-out on an honest Content-Length; the streaming counter is
    // the authoritative limit for chunked/lying uploads.
    const declaredLen = Number(c.req.header('content-length'));
    if (Number.isFinite(declaredLen) && declaredLen > MAX_IMAGE_BYTES) {
      return c.json({ error: 'Image is too large (50MB max).' }, 413);
    }
    const body = c.req.raw.body;
    if (!body) return c.json({ error: 'Empty upload' }, 400);

    // Bound concurrent in-flight uploads so a burst of parallel 50MB
    // streams can't pin unbounded RAM + disk at once (there is no global
    // rate limiter in front of this).
    if (!acquireUploadSlot()) {
      return c.json({ error: 'Too many uploads in progress. Try again shortly.' }, 503);
    }

    const imageId = randomUUID();
    let saved: Awaited<ReturnType<typeof saveIncidentImageStream>> | null = null;
    try {
      const pool = await getPool();
      if (!pool) return c.json(DB_UNAVAILABLE, 503);

      // Cheap pre-checks before spending disk on the stream. The
      // authoritative capacity check happens under the row lock below.
      const pre = await pool.query<{ images: unknown }>(
        'SELECT images FROM incidents WHERE id = $1 AND deleted_at IS NULL',
        [id],
      );
      if (!pre.rowCount) return c.json({ error: 'Incident not found' }, 404);
      if (parseIncidentImages(pre.rows[0]?.images).length >= MAX_IMAGES_PER_INCIDENT) {
        return c.json(
          { error: `This incident already has ${MAX_IMAGES_PER_INCIDENT} photos.` },
          409,
        );
      }

      saved = await saveIncidentImageStream(id, imageId, contentType, body);

      const entry: IncidentImage = {
        id: imageId,
        file: saved.publicPath,
        size: saved.size,
        content_type: contentType,
        uploaded_by: currentUserId(c) ?? null,
        uploaded_by_name: currentUserName(c),
        uploaded_at: new Date().toISOString(),
      };

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const current = await lockIncidentImages(client, id);
        if (current === null) {
          await client.query('ROLLBACK');
          await deleteIncidentImageFile(id, saved.publicPath);
          return c.json({ error: 'Incident not found' }, 404);
        }
        // Re-check under the lock: two concurrent uploads could both pass
        // the pre-check above and push the incident to 5 photos.
        if (current.length >= MAX_IMAGES_PER_INCIDENT) {
          await client.query('ROLLBACK');
          await deleteIncidentImageFile(id, saved.publicPath);
          return c.json(
            { error: `This incident already has ${MAX_IMAGES_PER_INCIDENT} photos.` },
            409,
          );
        }
        await client.query(
          'UPDATE incidents SET images = $1::jsonb, updated_at = NOW() WHERE id = $2',
          [JSON.stringify([...current, entry]), id],
        );
        await client.query('COMMIT');
        // Past this point the row references the file; a later throw must
        // NOT unlink it (that would dangle the reference). Clear `saved`
        // so the outer catch's cleanup skips it.
        saved = null;
      } catch (err) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw err;
      } finally {
        client.release();
      }

      log.info({ id, imageId, size: entry.size }, 'Incident image uploaded');
      return c.json({ success: true, image: entry }, 201);
    } catch (err) {
      // Never leave an orphan file behind when the DB half failed.
      if (saved) await deleteIncidentImageFile(id, saved.publicPath);
      if (err instanceof ImageTooLargeError) {
        return c.json({ error: 'Image is too large (50MB max).' }, 413);
      }
      if (err instanceof ImageTypeMismatchError) {
        return c.json(
          { error: 'That file is not a valid JPEG, PNG, WebP or GIF image.' },
          415,
        );
      }
      // Fail-closed: we could not scrub the file's metadata, so we refuse
      // to publish it rather than leak whatever EXIF/GPS it carries.
      if (err instanceof MetadataStripError) {
        log.warn({ id, err: (err as Error).message }, 'Rejected image: metadata strip failed');
        return c.json(
          { error: 'That image could not be processed. Try re-saving or exporting it first.' },
          415,
        );
      }
      log.error({ err, id }, 'Error uploading incident image');
      return c.json({ error: 'Failed to upload image' }, 500);
    } finally {
      releaseUploadSlot();
    }
  },
);

incidentsRouter.delete('/api/incidents/:id/images/:imageId', async (c) => {
  const id = c.req.param('id');
  const imageId = c.req.param('imageId');
  if (!isSafeIdSegment(id) || !isSafeIdSegment(imageId)) {
    return c.json({ error: 'Image not found' }, 404);
  }
  try {
    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    let removed: IncidentImage | null = null;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const current = await lockIncidentImages(client, id);
      if (current === null) {
        await client.query('ROLLBACK');
        return c.json({ error: 'Incident not found' }, 404);
      }
      const target = current.find((img) => img.id === imageId);
      if (!target) {
        await client.query('ROLLBACK');
        return c.json({ error: 'Image not found' }, 404);
      }
      // Author-only, with the same admin moderation override log entries use.
      if (!(await userCanModifyUpdate(currentUserId(c), target.uploaded_by))) {
        await client.query('ROLLBACK');
        return c.json(
          { error: 'Only the editor who uploaded this photo (or an admin) can remove it.' },
          403,
        );
      }
      await client.query(
        'UPDATE incidents SET images = $1::jsonb, updated_at = NOW() WHERE id = $2',
        [JSON.stringify(current.filter((img) => img.id !== imageId)), id],
      );
      await client.query('COMMIT');
      removed = target;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }

    // Best-effort: the row is already updated, so a stray file is cosmetic.
    if (removed) await deleteIncidentImageFile(id, removed.file);
    log.info({ id, imageId }, 'Incident image removed');
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id, imageId }, 'Error removing incident image');
    return c.json({ error: 'Failed to remove image' }, 500);
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
    // Stamp the author id + display name (username only) so every log
    // entry shows who wrote it.
    const createdBy = currentUserId(c) ?? null;
    const createdByName = currentUserName(c);
    const result = await withPool(async (pool) => {
      const r = await pool.query<{ id: string }>(
        'INSERT INTO incident_updates (incident_id, message, created_by, created_by_name) VALUES ($1, $2, $3, $4) RETURNING id',
        [id, message, createdBy, createdByName],
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
      const owner = await loadUpdateAuthority(pool, updateId);
      if (!owner) return { notFound: true as const };
      if (!(await userCanModifyUpdate(currentUserId(c), owner.updateBy))) {
        return { forbidden: true as const };
      }
      await pool.query(
        'UPDATE incident_updates SET message = $1 WHERE id = $2',
        [message, updateId],
      );
      return { ok: true as const };
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    if ('notFound' in result) return c.json({ error: 'Update not found' }, 404);
    if ('forbidden' in result) {
      return c.json({ error: 'Only the log author (or an admin) can edit it.' }, 403);
    }
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
      const owner = await loadUpdateAuthority(pool, updateId);
      if (!owner) return { notFound: true as const };
      if (!(await userCanModifyUpdate(currentUserId(c), owner.updateBy))) {
        return { forbidden: true as const };
      }
      await pool.query('DELETE FROM incident_updates WHERE id = $1', [updateId]);
      return { ok: true as const };
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    if ('notFound' in result) return c.json({ error: 'Update not found' }, 404);
    if ('forbidden' in result) {
      return c.json({ error: 'Only the log author (or an admin) can delete it.' }, 403);
    }
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, updateId }, 'Error deleting incident update');
    return c.json({ error: 'Failed to delete incident update' }, 500);
  }
});

// ---------------------------------------------------------------------------
// Helpers + routes for the suggestion workflow.
// ---------------------------------------------------------------------------

/** Load a log line's author + its parent incident's creator, or null if
 *  the update doesn't exist. */
async function loadUpdateAuthority(
  pool: Pool,
  updateId: string,
): Promise<{ updateBy: string | null; incidentBy: string | null } | null> {
  const r = await pool.query<{ update_by: string | null; incident_by: string | null }>(
    `SELECT u.created_by AS update_by, i.created_by AS incident_by
       FROM incident_updates u
       LEFT JOIN incidents i ON i.id = u.incident_id
      WHERE u.id = $1`,
    [updateId],
  );
  if (r.rowCount === 0) return null;
  return {
    updateBy: r.rows[0]?.update_by ?? null,
    incidentBy: r.rows[0]?.incident_by ?? null,
  };
}

/** Filter an arbitrary object down to whitelisted, coerced incident field
 *  changes — reused by suggestion create + approve so a suggestion can
 *  never touch a non-updatable column. Returns null if nothing valid. */
function sanitiseIncidentChanges(
  data: Record<string, unknown>,
): Record<string, unknown> | null {
  const out: Record<string, unknown> = {};
  for (const key of UPDATABLE_FIELDS) {
    if (!Object.prototype.hasOwnProperty.call(data, key)) continue;
    const raw = data[key as UpdatableField];
    if (key === 'lat' || key === 'lng') {
      const coord = coerceCoord(raw);
      if (coord === null) continue; // skip invalid coords rather than fail the whole suggestion
      out[key] = coord;
    } else {
      out[key] = raw;
    }
  }
  return Object.keys(out).length > 0 ? out : null;
}

interface SuggestionRow {
  id: string;
  incident_id: string;
  kind: string;
  changes: unknown;
  message: string | null;
  suggested_by: string | null;
  suggested_by_name: string | null;
  status: string;
  reviewed_by: string | null;
  reviewed_at: Date | string | null;
  created_at: Date | string | null;
}

// POST /api/incidents/:id/suggestions — any editor proposes an edit or a
// note. (Baseline gate already required an editor login.)
incidentsRouter.post('/api/incidents/:id/suggestions', async (c) => {
  const id = c.req.param('id');
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const kind = String(data['kind'] ?? '').toLowerCase();
    if (kind !== 'edit' && kind !== 'note') {
      return c.json({ error: "kind must be 'edit' or 'note'" }, 400);
    }
    const suggestedBy = currentUserId(c) ?? null;
    const suggestedByName =
      typeof data['suggested_by_name'] === 'string'
        ? (data['suggested_by_name'] as string).slice(0, 200)
        : null;

    let changesJson = '{}';
    let message: string | null = null;
    if (kind === 'edit') {
      const changes = sanitiseIncidentChanges(
        (data['changes'] as Record<string, unknown>) ?? {},
      );
      if (!changes) {
        return c.json({ error: 'No valid field changes to suggest.' }, 400);
      }
      changesJson = JSON.stringify(changes);
    } else {
      message = typeof data['message'] === 'string' ? (data['message'] as string) : '';
      if (!message.trim()) {
        return c.json({ error: 'A note suggestion needs a message.' }, 400);
      }
    }

    const result = await withPool(async (pool) => {
      // Confirm the incident exists so we don't accrue orphan suggestions.
      const inc = await pool.query('SELECT 1 FROM incidents WHERE id = $1 AND deleted_at IS NULL', [id]);
      if (inc.rowCount === 0) return { notFound: true as const };
      const r = await pool.query<{ id: string }>(
        `INSERT INTO incident_suggestions
           (incident_id, kind, changes, message, suggested_by, suggested_by_name)
         VALUES ($1, $2, $3::jsonb, $4, $5, $6)
         RETURNING id`,
        [id, kind, changesJson, message, suggestedBy, suggestedByName],
      );
      return { id: r.rows[0]?.id ?? null };
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    if ('notFound' in result) return c.json({ error: 'Incident not found' }, 404);
    return c.json({ id: result.id, success: true }, 201);
  } catch (err) {
    log.error({ err, id }, 'Error creating incident suggestion');
    return c.json({ error: 'Failed to create suggestion' }, 500);
  }
});

// GET /api/incidents/:id/suggestions — owner/admin only (review queue).
// This is a GET so it bypasses the baseline editor gate; it enforces
// owner-or-admin itself. The frontend must send the reviewer's JWT.
incidentsRouter.get('/api/incidents/:id/suggestions', async (c) => {
  const id = c.req.param('id');
  const userId = currentUserId(c);
  if (!userId) return c.json({ error: 'authentication required' }, 401);
  const status = new URL(c.req.url).searchParams.get('status') ?? 'pending';
  try {
    const result = await withPool(async (pool) => {
      const owner = await pool.query<{ created_by: string | null }>(
        'SELECT created_by FROM incidents WHERE id = $1 AND deleted_at IS NULL',
        [id],
      );
      if (owner.rowCount === 0) return { notFound: true as const };
      if (!(await userCanModifyIncident(userId, owner.rows[0]?.created_by ?? null))) {
        return { forbidden: true as const };
      }
      const r =
        status === 'all'
          ? await pool.query<SuggestionRow>(
              'SELECT * FROM incident_suggestions WHERE incident_id = $1 ORDER BY created_at DESC',
              [id],
            )
          : await pool.query<SuggestionRow>(
              'SELECT * FROM incident_suggestions WHERE incident_id = $1 AND status = $2 ORDER BY created_at DESC',
              [id, status],
            );
      return { rows: r.rows.map(normaliseSuggestion) };
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    if ('notFound' in result) return c.json({ error: 'Incident not found' }, 404);
    if ('forbidden' in result) {
      return c.json({ error: 'Only the incident owner or an admin can view suggestions.' }, 403);
    }
    return c.json(result.rows);
  } catch (err) {
    log.error({ err, id }, 'Error listing incident suggestions');
    return c.json({ error: 'Failed to list suggestions' }, 500);
  }
});

function normaliseSuggestion(row: SuggestionRow): Record<string, unknown> {
  const out: Record<string, unknown> = { ...row };
  if (typeof out['changes'] === 'string') {
    try {
      out['changes'] = JSON.parse(out['changes'] as string);
    } catch {
      /* leave as-is */
    }
  }
  for (const k of ['created_at', 'reviewed_at']) {
    if (out[k] instanceof Date) out[k] = (out[k] as Date).toISOString();
  }
  return out;
}

// POST /api/incidents/:id/suggestions/:sid/approve — owner/admin. Auto-
// applies an 'edit' suggestion to the incident (or promotes a 'note' to a
// log), marks it approved.
incidentsRouter.post('/api/incidents/:id/suggestions/:sid/approve', async (c) => {
  const id = c.req.param('id');
  const sid = c.req.param('sid');
  const userId = currentUserId(c);
  try {
    const result = await withPool(async (pool) => {
      const owner = await pool.query<{ created_by: string | null }>(
        'SELECT created_by FROM incidents WHERE id = $1 AND deleted_at IS NULL',
        [id],
      );
      if (owner.rowCount === 0) return { notFound: true as const };
      if (!(await userCanModifyIncident(userId, owner.rows[0]?.created_by ?? null))) {
        return { forbidden: true as const };
      }
      const sug = await pool.query<SuggestionRow>(
        "SELECT * FROM incident_suggestions WHERE id = $1 AND incident_id = $2 AND status = 'pending'",
        [sid, id],
      );
      if (sug.rowCount === 0) return { badSuggestion: true as const };
      const row = sug.rows[0]!;

      if (row.kind === 'edit') {
        const changes =
          typeof row.changes === 'string'
            ? (JSON.parse(row.changes) as Record<string, unknown>)
            : ((row.changes as Record<string, unknown>) ?? {});
        const clean = sanitiseIncidentChanges(changes);
        if (clean) {
          const sets: string[] = [];
          const vals: unknown[] = [];
          for (const [k, v] of Object.entries(clean)) {
            sets.push(`${k} = $${sets.length + 1}`);
            vals.push(JSONB_FIELDS.has(k) ? JSON.stringify(v) : v);
          }
          vals.push(id);
          await pool.query(
            `UPDATE incidents SET ${sets.join(', ')} WHERE id = $${vals.length}`,
            vals,
          );
        }
        // Leave a trail in the public log so the change is auditable.
        const who = row.suggested_by_name || row.suggested_by || 'an editor';
        await pool.query(
          'INSERT INTO incident_updates (incident_id, message, created_by) VALUES ($1, $2, $3)',
          [id, `Applied suggested edit from ${who}.`, userId ?? null],
        );
      } else {
        // 'note' → promote to an official log, crediting the suggester.
        await pool.query(
          'INSERT INTO incident_updates (incident_id, message, created_by) VALUES ($1, $2, $3)',
          [id, row.message ?? '', row.suggested_by ?? userId ?? null],
        );
      }

      await pool.query(
        "UPDATE incident_suggestions SET status = 'approved', reviewed_by = $1, reviewed_at = now() WHERE id = $2",
        [userId ?? null, sid],
      );
      return { ok: true as const };
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    if ('notFound' in result) return c.json({ error: 'Incident not found' }, 404);
    if ('forbidden' in result) {
      return c.json({ error: 'Only the incident owner or an admin can approve suggestions.' }, 403);
    }
    if ('badSuggestion' in result) {
      return c.json({ error: 'Suggestion not found or already reviewed.' }, 404);
    }
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id, sid }, 'Error approving suggestion');
    return c.json({ error: 'Failed to approve suggestion' }, 500);
  }
});

// POST /api/incidents/:id/suggestions/:sid/reject — owner/admin.
incidentsRouter.post('/api/incidents/:id/suggestions/:sid/reject', async (c) => {
  const id = c.req.param('id');
  const sid = c.req.param('sid');
  const userId = currentUserId(c);
  try {
    const result = await withPool(async (pool) => {
      const owner = await pool.query<{ created_by: string | null }>(
        'SELECT created_by FROM incidents WHERE id = $1 AND deleted_at IS NULL',
        [id],
      );
      if (owner.rowCount === 0) return { notFound: true as const };
      if (!(await userCanModifyIncident(userId, owner.rows[0]?.created_by ?? null))) {
        return { forbidden: true as const };
      }
      const r = await pool.query(
        "UPDATE incident_suggestions SET status = 'rejected', reviewed_by = $1, reviewed_at = now() WHERE id = $2 AND incident_id = $3 AND status = 'pending'",
        [userId ?? null, sid, id],
      );
      if (r.rowCount === 0) return { badSuggestion: true as const };
      return { ok: true as const };
    });
    if (isUnavailable(result)) return c.json(DB_UNAVAILABLE, 503);
    if ('notFound' in result) return c.json({ error: 'Incident not found' }, 404);
    if ('forbidden' in result) {
      return c.json({ error: 'Only the incident owner or an admin can reject suggestions.' }, 403);
    }
    if ('badSuggestion' in result) {
      return c.json({ error: 'Suggestion not found or already reviewed.' }, 404);
    }
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id, sid }, 'Error rejecting suggestion');
    return c.json({ error: 'Failed to reject suggestion' }, 500);
  }
});
