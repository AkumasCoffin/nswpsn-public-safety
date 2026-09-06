/**
 * The Wire — Fleet tab. A directory of emergency-service vehicles: each row
 * is a reference sheet (callsign, agency, spec fields, radio IDs, one photo),
 * not a news post.
 *
 * Same auth model as the rest of the Wire:
 *   - GET (feed/detail) — public, key-gated, behind the same soft-launch gate
 *   - POST create — requireRole(canFeedMedia); pending unless the author can
 *     moderate or approval is switched off (mirrors articles)
 *   - PUT/DELETE — author, or an admin override (canManageUsers)
 *   - review/remove — requireRole(canModerateWire)
 *   - POST :id/view — public, deduped via wire_views (parent_type 'fleet')
 *
 * ONE image, stored as an R2 object key on the row (no wire_media children);
 * uploads reuse POST /api/wire/upload-url without a hash, so the articles'
 * once-per-image rule deliberately doesn't apply here.
 */
import { Hono } from 'hono';
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import {
  requireRole,
  canFeedMedia,
  canModerateWire,
  canManageUsers,
} from '../services/auth/roles.js';
import { wirePublic } from '../services/wireSettings.js';
import { r2PublicUrl, deleteR2Object, viewerHash } from '../services/wire.js';

export const fleetRouter = new Hono();

const DB_UNAVAILABLE = { error: 'database unavailable' } as const;

const STATES = new Set(['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']);
const CATEGORIES = new Set(['police', 'fire', 'ses', 'ambulance', 'marine', 'rescue', 'other']);
const MAX_RADIO_IDS = 12;

// ---- context helpers (same shapes as api/wire.ts) --------------------------
function currentUserId(c: { get: (k: string) => unknown }): string | undefined {
  const v = c.get('userId');
  return typeof v === 'string' && v.length > 0 ? v : undefined;
}
function currentUserName(c: { get: (k: string) => unknown }): string | null {
  const v = c.get('userName');
  return typeof v === 'string' && v ? v : null;
}
function clientIp(c: { req: { header: (k: string) => string | undefined } }): string {
  const fwd = c.req.header('cf-connecting-ip') || c.req.header('x-forwarded-for') || '';
  return fwd.split(',')[0]?.trim() || 'unknown';
}
function isoOrNull(v: unknown): string | null {
  if (v instanceof Date) return v.toISOString();
  return typeof v === 'string' ? v : null;
}

/** Same soft-launch gate as api/wire.ts — the fleet follows the posts. */
async function wireReadable(c: { get: (k: string) => unknown }): Promise<boolean> {
  if (await wirePublic(await getPool())) return true;
  const uid = currentUserId(c);
  if (!uid) return false;
  return (await canFeedMedia(uid)) || (await canModerateWire(uid));
}

/** Same approval toggle articles honour (app_settings.wire_approval_required). */
async function approvalRequired(pool: Pool): Promise<boolean> {
  try {
    const r = await pool.query<{ value: string }>(
      `SELECT value FROM app_settings WHERE key = 'wire_approval_required'`);
    return r.rowCount === 0 ? true : r.rows[0]!.value !== 'false';
  } catch {
    return true;
  }
}

// ---- validation ------------------------------------------------------------

function str(v: unknown, cap: number): string | null {
  return typeof v === 'string' ? v.trim().slice(0, cap) || null : null;
}
function intIn(v: unknown, lo: number, hi: number): number | null {
  const n = typeof v === 'number' ? v : typeof v === 'string' && v.trim() !== '' ? Number(v) : NaN;
  return Number.isInteger(n) && n >= lo && n <= hi ? n : null;
}
/** Radio IDs are 7-digit PSN radio identities; dedupe and cap the list. */
function radioList(v: unknown): string[] {
  if (!Array.isArray(v)) return [];
  const out: string[] = [];
  for (const x of v) {
    const s = String(x ?? '').trim();
    if (!/^\d{7}$/.test(s) || out.includes(s)) continue;
    out.push(s);
    if (out.length >= MAX_RADIO_IDS) break;
  }
  return out;
}

interface VehicleFields {
  callsign: string;
  state: string;
  lga: string;
  suburb: string | null;
  agency: string;
  agency_category: string | null;
  station: string | null;
  cad_code: string | null;
  aerial_id: string | null;
  vehicle_type: string | null;
  registration: string | null;
  make_model: string | null;
  production_year: number | null;
  crew_capacity: number | null;
  radio_ids: { cab: string[]; mobile: string[] };
  specs: Record<string, number | boolean>;
  image_key: string | null;
}

function parseVehicle(data: Record<string, unknown>): VehicleFields | { error: string } {
  const callsign = str(data['callsign'], 80);
  if (!callsign) return { error: 'a call sign is required' };
  const state = typeof data['state'] === 'string' ? data['state'].trim().toUpperCase() : '';
  if (!STATES.has(state)) return { error: 'a valid state is required' };
  const lga = str(data['lga'], 120);
  if (!lga) return { error: 'a Local Government Area is required' };

  const catRaw = typeof data['agency_category'] === 'string' ? data['agency_category'].trim() : '';
  const agency = str(data['agency'], 120);
  if (!agency) return { error: 'an agency is required' };

  const radioRaw = (data['radio_ids'] ?? {}) as Record<string, unknown>;
  const badId = ([] as unknown[])
    .concat(Array.isArray(radioRaw['cab']) ? (radioRaw['cab'] as unknown[]) : [])
    .concat(Array.isArray(radioRaw['mobile']) ? (radioRaw['mobile'] as unknown[]) : [])
    .find((x) => !/^\d{7}$/.test(String(x ?? '').trim()));
  if (badId !== undefined) return { error: 'radio IDs must be exactly 7 digits' };

  const specsRaw = (data['specs'] ?? {}) as Record<string, unknown>;
  const specs: Record<string, number | boolean> = {};
  const wt = intIn(specsRaw['water_tank_l'], 0, 1_000_000);
  if (wt !== null) specs['water_tank_l'] = wt;
  const ft = intIn(specsRaw['foam_tank_l'], 0, 1_000_000);
  if (ft !== null) specs['foam_tank_l'] = ft;
  if (typeof specsRaw['cafs'] === 'boolean') specs['cafs'] = specsRaw['cafs'];
  const ba = intIn(specsRaw['ba_sets'], 0, 99);
  if (ba !== null) specs['ba_sets'] = ba;
  const st = intIn(specsRaw['stretchers'], 0, 99);
  if (st !== null) specs['stretchers'] = st;

  return {
    callsign,
    state,
    lga,
    suburb: str(data['suburb'], 120),
    agency,
    agency_category: CATEGORIES.has(catRaw) ? catRaw : null,
    station: str(data['station'], 120),
    cad_code: str(data['cad_code'], 60),
    aerial_id: str(data['aerial_id'], 60),
    vehicle_type: str(data['vehicle_type'], 120),
    registration: str(data['registration'], 30),
    make_model: str(data['make_model'], 200),
    production_year: intIn(data['production_year'], 1900, 2100),
    crew_capacity: intIn(data['crew_capacity'], 0, 99),
    radio_ids: { cab: radioList(radioRaw['cab']), mobile: radioList(radioRaw['mobile']) },
    specs,
    image_key: str(data['image_key'], 300),
  };
}

// ---- shaping ---------------------------------------------------------------
/* eslint-disable @typescript-eslint/no-explicit-any */
function shapeVehicle(row: any, includeKeys = false): Record<string, unknown> {
  const radio = row.radio_ids && typeof row.radio_ids === 'object' ? row.radio_ids : {};
  return {
    id: row.id,
    kind: 'fleet',
    // `title` mirrors the other Wire kinds so the pending queue and any
    // generic card can render it without knowing about call signs.
    title: row.callsign,
    callsign: row.callsign,
    state: row.state,
    lga: row.lga,
    suburb: row.suburb,
    agency: row.agency,
    agency_category: row.agency_category,
    station: row.station,
    cad_code: row.cad_code,
    aerial_id: row.aerial_id,
    vehicle_type: row.vehicle_type,
    registration: row.registration,
    make_model: row.make_model,
    production_year: row.production_year,
    crew_capacity: row.crew_capacity,
    radio_ids: {
      cab: Array.isArray(radio.cab) ? radio.cab : [],
      mobile: Array.isArray(radio.mobile) ? radio.mobile : [],
    },
    specs: row.specs && typeof row.specs === 'object' ? row.specs : {},
    image_url: row.image_key ? r2PublicUrl(row.image_key) : null,
    ...(includeKeys ? { image_key: row.image_key ?? null } : {}),
    views: Number(row.views) || 0,
    status: row.status,
    review_note: row.review_note ?? null,
    author: { id: row.author_id, name: row.author_name },
    created_at: isoOrNull(row.created_at),
    updated_at: isoOrNull(row.updated_at),
  };
}
/* eslint-enable @typescript-eslint/no-explicit-any */

const VEHICLE_COLS = `callsign, state, lga, suburb, agency, agency_category, station, cad_code,
  aerial_id, vehicle_type, registration, make_model, production_year, crew_capacity,
  radio_ids, specs, image_key`;

function vehicleVals(v: VehicleFields): unknown[] {
  return [
    v.callsign, v.state, v.lga, v.suburb, v.agency, v.agency_category, v.station, v.cad_code,
    v.aerial_id, v.vehicle_type, v.registration, v.make_model, v.production_year, v.crew_capacity,
    JSON.stringify(v.radio_ids), JSON.stringify(v.specs), v.image_key,
  ];
}

// ---- feed ------------------------------------------------------------------

fleetRouter.get('/api/wire/fleet', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  if (!(await wireReadable(c))) return c.json({ vehicles: [], visible: false });
  try {
    const url = new URL(c.req.url);
    const mine = url.searchParams.get('mine') === '1';
    const uid = currentUserId(c);
    const state = (url.searchParams.get('state') || '').toUpperCase();
    const agency = url.searchParams.get('agency');
    const q = (url.searchParams.get('q') || '').trim();
    const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') ?? 48) || 48));
    const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

    const vals: unknown[] = [];
    const where: string[] = [];
    if (mine && uid) {
      vals.push(uid);
      where.push(`author_id = $${vals.length} AND status <> 'removed'`);
    } else {
      where.push(`status = 'published'`);
    }
    if (STATES.has(state)) { vals.push(state); where.push(`state = $${vals.length}`); }
    if (agency) { vals.push(agency); where.push(`agency = $${vals.length}`); }
    // Case-insensitive exact LGA match (the filter offers the same ABS names
    // the composer stores, but typed entries shouldn't miss on case).
    const lga = url.searchParams.get('lga');
    if (lga) { vals.push(lga.trim()); where.push(`lga ILIKE $${vals.length}`); }
    if (q) {
      vals.push(`%${q}%`);
      where.push(`(callsign ILIKE $${vals.length} OR station ILIKE $${vals.length} OR registration ILIKE $${vals.length})`);
    }
    vals.push(limit, offset);
    const r = await pool.query(
      `SELECT * FROM fleet_vehicles WHERE ${where.join(' AND ')}
        ORDER BY created_at DESC LIMIT $${vals.length - 1} OFFSET $${vals.length}`,
      vals,
    );
    return c.json({ vehicles: r.rows.map((row) => shapeVehicle(row)) });
  } catch (err) {
    log.error({ err }, 'fleet: list failed');
    return c.json({ error: 'failed to list fleet' }, 500);
  }
});

fleetRouter.get('/api/wire/fleet/:id', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  if (!(await wireReadable(c))) return c.json({ error: 'not found' }, 404);
  const id = c.req.param('id');
  try {
    const r = await pool.query('SELECT * FROM fleet_vehicles WHERE id = $1', [id]);
    if (r.rowCount === 0) return c.json({ error: 'not found' }, 404);
    const row = r.rows[0];
    const uid = currentUserId(c);
    const isAuthor = !!(uid && row.author_id === uid);
    const isAdmin = !!(uid && (await canManageUsers(uid)));
    if (row.status !== 'published' && !isAuthor && !isAdmin) {
      return c.json({ error: 'not found' }, 404);
    }
    return c.json({ vehicle: shapeVehicle(row, isAuthor || isAdmin) });
  } catch (err) {
    log.error({ err, id }, 'fleet: get failed');
    return c.json({ error: 'failed to fetch vehicle' }, 500);
  }
});

// ---- create / edit / delete ------------------------------------------------

fleetRouter.post('/api/wire/fleet', requireRole(canFeedMedia), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const v = parseVehicle(data);
    if ('error' in v) return c.json({ error: v.error }, 400);
    const authorId = currentUserId(c)!;
    const status = (!(await approvalRequired(pool)) || (await canModerateWire(authorId)))
      ? 'published' : 'pending';
    const cols = VEHICLE_COLS.replace(/\s+/g, ' ');
    const vals = vehicleVals(v);
    const ph = vals.map((_, i) => (i === 14 || i === 15 ? `$${i + 1}::jsonb` : `$${i + 1}`)).join(',');
    const ins = await pool.query<{ id: string }>(
      `INSERT INTO fleet_vehicles (author_id, author_name, status, ${cols})
       VALUES ($${vals.length + 1}, $${vals.length + 2}, $${vals.length + 3}, ${ph}) RETURNING id`,
      [...vals, authorId, currentUserName(c), status],
    );
    return c.json({ id: ins.rows[0]!.id, success: true, status }, 201);
  } catch (err) {
    log.error({ err }, 'fleet: create failed');
    return c.json({ error: 'failed to create vehicle' }, 500);
  }
});

fleetRouter.put('/api/wire/fleet/:id', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
    const uid = currentUserId(c);
    if (!uid) return c.json({ error: 'unauthorized' }, 401);
    const prev = await pool.query('SELECT author_id, image_key, status FROM fleet_vehicles WHERE id = $1', [id]);
    if (prev.rowCount === 0) return c.json({ error: 'not found' }, 404);
    const isAuthor = prev.rows[0].author_id === uid;
    if (!isAuthor && !(await canManageUsers(uid))) return c.json({ error: 'forbidden' }, 403);
    if (prev.rows[0].status === 'removed') return c.json({ error: 'this vehicle was removed by a moderator' }, 403);

    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const v = parseVehicle(data);
    if ('error' in v) return c.json({ error: v.error }, 400);

    const cols = VEHICLE_COLS.split(',').map((s) => s.trim());
    const sets = cols.map((col, i) =>
      `${col} = $${i + 1}${col === 'radio_ids' || col === 'specs' ? '::jsonb' : ''}`);
    const vals = vehicleVals(v);
    await pool.query(
      `UPDATE fleet_vehicles SET ${sets.join(', ')}, updated_at = now() WHERE id = $${vals.length + 1}`,
      [...vals, id],
    );
    // A replaced photo leaves its old object behind — clean it up, best-effort.
    const oldKey = prev.rows[0].image_key as string | null;
    if (oldKey && oldKey !== v.image_key) deleteR2Object(oldKey).catch(() => {});
    return c.json({ id, success: true });
  } catch (err) {
    log.error({ err, id }, 'fleet: update failed');
    return c.json({ error: 'failed to update vehicle' }, 500);
  }
});

fleetRouter.delete('/api/wire/fleet/:id', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
    const uid = currentUserId(c);
    if (!uid) return c.json({ error: 'unauthorized' }, 401);
    const prev = await pool.query('SELECT author_id, image_key FROM fleet_vehicles WHERE id = $1', [id]);
    if (prev.rowCount === 0) return c.json({ error: 'not found' }, 404);
    if (prev.rows[0].author_id !== uid && !(await canManageUsers(uid))) {
      return c.json({ error: 'forbidden' }, 403);
    }
    await pool.query('DELETE FROM fleet_vehicles WHERE id = $1', [id]);
    const key = prev.rows[0].image_key as string | null;
    if (key) deleteR2Object(key).catch(() => {});
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'fleet: delete failed');
    return c.json({ error: 'failed to delete vehicle' }, 500);
  }
});

// ---- moderation ------------------------------------------------------------

fleetRouter.post('/api/wire/fleet/:id/remove', requireRole(canModerateWire), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
    const r = await pool.query(
      `UPDATE fleet_vehicles SET status='removed', removed_by=$2, removed_by_name=$3, removed_at=now(), updated_at=now()
        WHERE id = $1 AND status <> 'removed' RETURNING id`,
      [id, currentUserId(c) ?? null, currentUserName(c)],
    );
    if (r.rowCount === 0) return c.json({ error: 'not found' }, 404);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'fleet: remove failed');
    return c.json({ error: 'failed to remove vehicle' }, 500);
  }
});

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function review(c: any, action: 'approve' | 'reject') {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
    const d = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const note = typeof d['note'] === 'string' ? d['note'].trim().slice(0, 2000) || null : null;
    const status = action === 'approve' ? 'published' : 'rejected';
    const r = await pool.query(
      `UPDATE fleet_vehicles
          SET status=$2, review_note=$3, reviewed_by=$4, reviewed_by_name=$5, reviewed_at=now(), updated_at=now()
        WHERE id = $1 AND status = 'pending' RETURNING id`,
      [id, status, note, currentUserId(c) ?? null, currentUserName(c)],
    );
    if (r.rowCount === 0) return c.json({ error: 'not found or already reviewed' }, 404);
    return c.json({ success: true, status });
  } catch (err) {
    log.error({ err, id, action }, 'fleet: review failed');
    return c.json({ error: 'failed to review vehicle' }, 500);
  }
}
fleetRouter.post('/api/wire/fleet/:id/approve', requireRole(canModerateWire), (c) => review(c, 'approve'));
fleetRouter.post('/api/wire/fleet/:id/reject', requireRole(canModerateWire), (c) => review(c, 'reject'));

// ---- views (dedup via wire_views, parent_type 'fleet') ---------------------

fleetRouter.post('/api/wire/fleet/:id/view', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
    const uid = currentUserId(c);
    if (uid) {
      const a = await pool.query<{ author_id: string; views: string }>(
        'SELECT author_id, views FROM fleet_vehicles WHERE id = $1', [id]);
      if (a.rowCount === 0) return c.json({ error: 'not found' }, 404);
      if (a.rows[0]!.author_id === uid) return c.json({ views: Number(a.rows[0]!.views) || 0, self: true });
    }
    const hash = viewerHash(clientIp(c), c.req.header('user-agent') || '');
    const ins = await pool.query(
      `INSERT INTO wire_views (parent_type, parent_id, viewer_hash, day)
       VALUES ('fleet',$1,$2, CURRENT_DATE) ON CONFLICT DO NOTHING RETURNING parent_id`,
      [id, hash],
    );
    if ((ins.rowCount ?? 0) > 0) {
      const upd = await pool.query<{ views: string }>(
        'UPDATE fleet_vehicles SET views = views + 1 WHERE id = $1 RETURNING views', [id]);
      return c.json({ views: Number(upd.rows[0]?.views ?? 0) });
    }
    const cur = await pool.query<{ views: string }>('SELECT views FROM fleet_vehicles WHERE id = $1', [id]);
    return c.json({ views: Number(cur.rows[0]?.views ?? 0), deduped: true });
  } catch (err) {
    log.error({ err, id }, 'fleet: view count failed');
    return c.json({ error: 'failed to record view' }, 500);
  }
});

export { shapeVehicle as shapeFleetVehicle };
