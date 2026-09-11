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
import { notifyStaff } from '../services/staffNotify.js';
import {
  requireRole,
  canFeedMedia,
  canModerateWire,
  canManageUsers,
} from '../services/auth/roles.js';
import { wirePublic } from '../services/wireSettings.js';
import { r2PublicUrl, deleteR2Object, viewerHash, normaliseLicense, licenseLabel } from '../services/wire.js';
import { RECOVERY_DAYS } from '../services/wirePurge.js';
import { displayNameMap } from '../services/wireComments.js';

export const fleetRouter = new Hono();

const DB_UNAVAILABLE = { error: 'database unavailable' } as const;

const STATES = new Set(['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']);
const CATEGORIES = new Set(['police', 'fire', 'ses', 'ambulance', 'marine', 'rescue', 'other']);
const MAX_RADIO_IDS = 12;
const MAX_IMAGES = 4;
const IMAGE_SIDES = new Set(['front', 'rear', 'left', 'right', 'other']);

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

/** The byline name: the profile's chosen display name when set, else the
 *  JWT's (which can lag behind a rename until the token refreshes). */
async function authorNameFor(pool: Pool, uid: string, fallback: string | null): Promise<string | null> {
  try {
    const r = await pool.query<{ display_name: string | null }>(
      'SELECT display_name FROM user_profiles WHERE user_id = $1', [uid]);
    return (r.rows[0]?.display_name || '').trim() || fallback;
  } catch {
    return fallback;
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
  make: string | null;
  model: string | null;
  cab_chassis: string | null;
  production_year: number | null;
  crew_capacity: number | null;
  license: string;
  credit: string | null;
  rights_affirmed: true;
  watermark: boolean;
  radio_ids: { cab: string[]; mobile: string[] };
  specs: Record<string, number | boolean | string>;
  images: { key: string; side: string | null }[];
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
  // Same publish-time affirmation articles require -- fleet posts have no
  // draft state, so it applies to every create AND edit.
  if (data['rights_affirmed'] !== true) {
    return { error: 'you must confirm you own or have the rights to publish this' };
  }

  const radioRaw = (data['radio_ids'] ?? {}) as Record<string, unknown>;
  const badId = ([] as unknown[])
    .concat(Array.isArray(radioRaw['cab']) ? (radioRaw['cab'] as unknown[]) : [])
    .concat(Array.isArray(radioRaw['mobile']) ? (radioRaw['mobile'] as unknown[]) : [])
    .find((x) => !/^\d{7}$/.test(String(x ?? '').trim()));
  if (badId !== undefined) return { error: 'radio IDs must be exactly 7 digits' };

  const specsRaw = (data['specs'] ?? {}) as Record<string, unknown>;
  const specs: Record<string, number | boolean | string> = {};
  const dt = typeof specsRaw['drive_type'] === 'string' ? specsRaw['drive_type'].trim() : '';
  if (['2WD', '4WD', '6WD', 'unknown'].includes(dt)) specs['drive_type'] = dt;
  const pt = typeof specsRaw['pump_type'] === 'string' ? specsRaw['pump_type'].trim().slice(0, 120) : '';
  if (pt) specs['pump_type'] = pt;
  const wt = intIn(specsRaw['water_tank_l'], 0, 1_000_000);
  if (wt !== null) specs['water_tank_l'] = wt;
  const ft = intIn(specsRaw['foam_tank_l'], 0, 1_000_000);
  if (ft !== null) specs['foam_tank_l'] = ft;
  const fb = intIn(specsRaw['foam_b_tank_l'], 0, 1_000_000);
  if (fb !== null) specs['foam_b_tank_l'] = fb;
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
    make: str(data['make'], 80),
    model: str(data['model'], 80),
    cab_chassis: str(data['cab_chassis'], 120),
    production_year: intIn(data['production_year'], 1900, 2100),
    crew_capacity: intIn(data['crew_capacity'], 0, 99),
    license: normaliseLicense(data['license']),
    credit: str(data['credit'], 200),
    rights_affirmed: true,
    watermark: data['watermark'] === true,
    radio_ids: { cab: radioList(radioRaw['cab']), mobile: radioList(radioRaw['mobile']) },
    specs,
    images: imageList(data['images']),
  };
}

/** Up to MAX_IMAGES photos, each optionally labelled with the vehicle side
 *  it shows. Keys are deduped; unknown side values become null. */
function imageList(v: unknown): { key: string; side: string | null }[] {
  if (!Array.isArray(v)) return [];
  const seen = new Set<string>();
  const out: { key: string; side: string | null }[] = [];
  for (const item of v) {
    const o = item as Record<string, unknown> | null;
    const key = typeof o?.['key'] === 'string' ? o['key'].trim().slice(0, 300) : '';
    if (!key || seen.has(key)) continue;
    seen.add(key);
    const sideRaw = typeof o?.['side'] === 'string' ? o['side'].trim().toLowerCase() : '';
    out.push({ key, side: IMAGE_SIDES.has(sideRaw) ? sideRaw : null });
    if (out.length >= MAX_IMAGES) break;
  }
  return out;
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
    make: row.make,
    model: row.model,
    cab_chassis: row.cab_chassis,
    production_year: row.production_year,
    crew_capacity: row.crew_capacity,
    radio_ids: {
      cab: Array.isArray(radio.cab) ? radio.cab : [],
      mobile: Array.isArray(radio.mobile) ? radio.mobile : [],
    },
    specs: row.specs && typeof row.specs === 'object' ? row.specs : {},
    // The gallery, in upload order. `image_url` stays as the first photo so
    // cards and the profile rows keep one thing to thumbnail.
    images: (Array.isArray(row.images) ? row.images : []).map((im: { key?: string; side?: string | null }) => ({
      url: im.key ? r2PublicUrl(im.key) : null,
      side: im.side ?? null,
      ...(includeKeys ? { key: im.key ?? null } : {}),
    })),
    image_url: Array.isArray(row.images) && row.images[0]?.key ? r2PublicUrl(row.images[0].key) : null,
    license: row.license || 'credit',
    license_label: licenseLabel(row.license || 'credit'),
    credit: row.credit || null,
    watermark: row.watermark === true,
    views: Number(row.views) || 0,
    status: row.status,
    review_note: row.review_note ?? null,
    deleted_at: isoOrNull(row.deleted_at),
    delete_after: row.deleted_at instanceof Date
      ? new Date(row.deleted_at.getTime() + RECOVERY_DAYS * 86_400_000).toISOString()
      : null,
    author: { id: row.author_id, name: row.author_name },
    created_at: isoOrNull(row.created_at),
    updated_at: isoOrNull(row.updated_at),
  };
}
/* eslint-enable @typescript-eslint/no-explicit-any */

const VEHICLE_COLS = `callsign, state, lga, suburb, agency, agency_category, station, cad_code,
  aerial_id, vehicle_type, registration, make, model, cab_chassis, production_year, crew_capacity,
  license, credit, rights_affirmed, watermark,
  radio_ids, specs, images`;

// The jsonb values sit at these indexes of vehicleVals -- the INSERT
// placeholder builder casts them. Keep all three in sync.
const JSONB_IDX = new Set([20, 21, 22]);

function vehicleVals(v: VehicleFields): unknown[] {
  return [
    v.callsign, v.state, v.lga, v.suburb, v.agency, v.agency_category, v.station, v.cad_code,
    v.aerial_id, v.vehicle_type, v.registration, v.make, v.model, v.cab_chassis, v.production_year, v.crew_capacity,
    v.license, v.credit, v.rights_affirmed, v.watermark,
    JSON.stringify(v.radio_ids), JSON.stringify(v.specs), JSON.stringify(v.images),
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
    // ?deleted=1 (with mine) lists the author's vehicles awaiting purge.
    const wantDeleted = mine && url.searchParams.get('deleted') === '1';
    where.push(wantDeleted ? 'deleted_at IS NOT NULL' : 'deleted_at IS NULL');
    if (mine && uid) {
      vals.push(uid);
      where.push(`author_id = $${vals.length} AND status <> 'removed'`);
    } else {
      where.push(`status = 'published'`);
    }
    if (STATES.has(state)) { vals.push(state); where.push(`state = $${vals.length}`); }
    // agency + lga accept comma-separated multi-selections.
    const agencies = (agency || '').split(',').map((x) => x.trim()).filter(Boolean).slice(0, 20);
    if (agencies.length) { vals.push(agencies); where.push(`agency = ANY($${vals.length}::text[])`); }
    const lgas = (url.searchParams.get('lga') || '').split(',').map((x) => x.trim()).filter(Boolean).slice(0, 30);
    if (lgas.length) { vals.push(lgas); where.push(`lga ILIKE ANY($${vals.length}::text[])`); }
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
    // Bylines show the CURRENT chosen username, not the one stored at
    // write time (stale after renames).
    const names = await displayNameMap(pool, r.rows.map((row) => row.author_id));
    const vehicles = r.rows.map((row) => {
      const v = shapeVehicle(row);
      const live = names.get(row.author_id);
      if (live) (v['author'] as Record<string, unknown>)['name'] = live;
      return v;
    });
    return c.json({ vehicles });
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
    if ((row.status !== 'published' || row.deleted_at) && !isAuthor && !isAdmin) {
      return c.json({ error: 'not found' }, 404);
    }
    const v = shapeVehicle(row, isAuthor || isAdmin);
    const names = await displayNameMap(pool, [row.author_id]);
    const live = names.get(row.author_id);
    if (live) (v['author'] as Record<string, unknown>)['name'] = live;
    return c.json({ vehicle: v });
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
    const ph = vals.map((_, i) => (JSONB_IDX.has(i) ? `$${i + 1}::jsonb` : `$${i + 1}`)).join(',');
    const ins = await pool.query<{ id: string }>(
      `INSERT INTO fleet_vehicles (author_id, author_name, status, ${cols})
       VALUES ($${vals.length + 1}, $${vals.length + 2}, $${vals.length + 3}, ${ph}) RETURNING id`,
      [...vals, authorId, await authorNameFor(pool, authorId, currentUserName(c)), status],
    );
    if (status === 'pending') {
      notifyStaff(pool, {
        kind: 'wire_approval',
        event: 'new',
        ref: `fleet:${ins.rows[0]!.id}`,
        title: v.callsign || 'Fleet vehicle',
        subtitle: 'Fleet vehicle awaiting approval',
      });
    }
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
    const prev = await pool.query('SELECT author_id, images, status FROM fleet_vehicles WHERE id = $1', [id]);
    if (prev.rowCount === 0) return c.json({ error: 'not found' }, 404);
    const isAuthor = prev.rows[0].author_id === uid;
    if (!isAuthor && !(await canManageUsers(uid))) return c.json({ error: 'forbidden' }, 403);
    if (prev.rows[0].status === 'removed') return c.json({ error: 'this vehicle was removed by a moderator' }, 403);

    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const v = parseVehicle(data);
    if ('error' in v) return c.json({ error: v.error }, 400);

    const cols = VEHICLE_COLS.split(',').map((s) => s.trim());
    const sets = cols.map((col, i) =>
      `${col} = $${i + 1}${col === 'radio_ids' || col === 'specs' || col === 'images' ? '::jsonb' : ''}`);
    const vals = vehicleVals(v);
    // An edit also refreshes the author's byline (only for the author's own
    // edits -- an admin fixing a typo mustn't take over the credit).
    const nameSet = isAuthor ? `, author_name = $${vals.length + 2}` : '';
    const nameVal = isAuthor ? [await authorNameFor(pool, uid, currentUserName(c))] : [];
    await pool.query(
      `UPDATE fleet_vehicles SET ${sets.join(', ')}, updated_at = now()${nameSet} WHERE id = $${vals.length + 1}`,
      [...vals, id, ...nameVal],
    );
    // Photos dropped in this edit leave their objects behind — clean up.
    const prevImages = Array.isArray(prev.rows[0].images) ? (prev.rows[0].images as { key?: string }[]) : [];
    const keep = new Set(v.images.map((im) => im.key));
    for (const im of prevImages) {
      if (im.key && !keep.has(im.key)) deleteR2Object(im.key).catch(() => {});
    }
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
    const prev = await pool.query('SELECT author_id FROM fleet_vehicles WHERE id = $1', [id]);
    if (prev.rowCount === 0) return c.json({ error: 'not found' }, 404);
    if (prev.rows[0].author_id !== uid && !(await canManageUsers(uid))) {
      return c.json({ error: 'forbidden' }, 403);
    }
    // Soft delete: hidden at once, purged (row + photo) by wirePurge.ts
    // after the recovery window.
    await pool.query('UPDATE fleet_vehicles SET deleted_at = now(), updated_at = now() WHERE id = $1 AND deleted_at IS NULL', [id]);
    return c.json({ success: true, recovery_days: RECOVERY_DAYS });
  } catch (err) {
    log.error({ err, id }, 'fleet: delete failed');
    return c.json({ error: 'failed to delete vehicle' }, 500);
  }
});

// Undo a deletion while it's still inside the recovery window.
fleetRouter.post('/api/wire/fleet/:id/recover', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
    const uid = currentUserId(c);
    if (!uid) return c.json({ error: 'unauthorized' }, 401);
    const prev = await pool.query('SELECT author_id FROM fleet_vehicles WHERE id = $1 AND deleted_at IS NOT NULL', [id]);
    if (prev.rowCount === 0) return c.json({ error: 'not found' }, 404);
    if (prev.rows[0].author_id !== uid && !(await canManageUsers(uid))) {
      return c.json({ error: 'forbidden' }, 403);
    }
    await pool.query('UPDATE fleet_vehicles SET deleted_at = NULL, updated_at = now() WHERE id = $1', [id]);
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'fleet: recover failed');
    return c.json({ error: 'failed to recover vehicle' }, 500);
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
    notifyStaff(pool, {
      kind: 'wire_approval',
      event: 'resolved',
      ref: `fleet:${id}`,
      title: 'Fleet vehicle',
      status: action === 'approve' ? 'approved' : 'rejected',
      actor: currentUserName(c),
    });
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
