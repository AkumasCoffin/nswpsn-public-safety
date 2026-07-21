/**
 * User + role management.
 *
 * Mirrors python external_api_proxy.py:13996-14169.
 *   GET    /api/users
 *   PUT    /api/users/:userId/roles            — replace all roles (atomic)
 *   POST   /api/users/:userId/roles            — add a single role
 *   DELETE /api/users/:userId/roles/:role      — remove a single role
 *
 * GET /api/users combines a Supabase Auth Admin API listing (the source
 * of truth for the user objects themselves — email, last sign-in, etc.)
 * with the user_roles rows in our Postgres. Without SUPABASE_URL and
 * SUPABASE_SERVICE_ROLE_KEY the python handler returns 503 with
 *   { error: 'Supabase not configured' }
 * — we match that exactly.
 *
 * Auth: these endpoints require a verified Supabase user (JWT) with the
 * right role — NOT just the public NSWPSN_API_KEY (which is handed to
 * every visitor via /api/config, so key-only gating was effectively
 * unauthenticated). Reads require owner|team_member (canManageUsers).
 *
 * Role mutations are tiered: owners and team members can add/remove the
 * feature roles (map_editor, pager_contributor, radio_contributor), but
 * ONLY owners may add or remove the privileged roles (team_member, dev,
 * owner). Each mutating handler checks the specific roles being touched
 * against the actor's ownership, so a team member's PUT that carries an
 * unchanged privileged set through the atomic replace still succeeds.
 */
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { config } from '../config.js';
import {
  invalidateUserRolesCache,
  isPrivilegedRole,
  requireRole,
  canManageUsers,
  canAssignPrivilegedRoles,
} from '../services/auth/roles.js';

export const usersRouter = new Hono();

const DB_UNAVAILABLE = { error: 'database unavailable' } as const;
const PRIVILEGED_ONLY = {
  error: 'Only owners can add or remove the team_member, dev, or owner roles.',
} as const;

interface SupabaseUser {
  id?: string;
  email?: string;
  created_at?: string;
  last_sign_in_at?: string;
  email_confirmed_at?: string | null;
  user_metadata?: Record<string, unknown>;
}

/**
 * Display username for the admin panel. Same priority chain as the
 * JWT-side displayNameFromClaims: explicit username first, then the
 * display/full-name variants (Discord OAuth populates full_name/name),
 * null when nothing is set.
 */
function usernameFromMetadata(meta: Record<string, unknown> | undefined): string | null {
  if (!meta) return null;
  for (const key of ['username', 'display_name', 'full_name', 'name', 'user_name', 'preferred_username']) {
    const v = meta[key];
    if (typeof v === 'string' && v.trim()) return v.trim();
  }
  return null;
}
interface SupabaseUsersResponse {
  users?: SupabaseUser[];
}

interface UserRoleRow {
  user_id: string;
  role: string;
  created_at: Date | string | null;
  granted_by: string | null;
  id: number;
}

// ---------------------------------------------------------------------------
// GET /api/users
// ---------------------------------------------------------------------------
usersRouter.get('/api/users', requireRole(canManageUsers), async (c) => {
  try {
    if (!config.SUPABASE_URL || !config.SUPABASE_SERVICE_ROLE_KEY) {
      return c.json({ error: 'Supabase not configured' }, 503);
    }

    const usersUrl = `${config.SUPABASE_URL}/auth/v1/admin/users`;
    const headers: Record<string, string> = {
      apikey: config.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${config.SUPABASE_SERVICE_ROLE_KEY}`,
      'Content-Type': 'application/json',
    };

    const usersResponse = await fetch(usersUrl, {
      headers,
      signal: AbortSignal.timeout(15_000),
    });

    if (!usersResponse.ok) {
      const text = await usersResponse.text().catch(() => '');
      log.error({ status: usersResponse.status, text }, 'Failed to fetch users');
      return c.json({ error: 'Failed to fetch users from Supabase' }, 500);
    }

    const usersData = (await usersResponse.json()) as SupabaseUsersResponse;
    const usersList = usersData.users ?? [];

    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    const allRoles = await pool.query<UserRoleRow>(
      'SELECT user_id, role, created_at, granted_by, id FROM user_roles',
    );

    const userRolesMap = new Map<string, Array<Record<string, unknown>>>();
    for (const row of allRoles.rows) {
      const list = userRolesMap.get(row.user_id) ?? [];
      list.push({
        role: row.role,
        // Match python's `str(created_at)` — it relies on psycopg's
        // default datetime-to-string repr, which produces 'YYYY-MM-DD
        // HH:MM:SS.ffffff+TZ'. ISO-8601 from `Date.toISOString()` is a
        // close-enough superset (admins read this for audit only — no
        // client parses it). Documented as a deviation.
        granted_at: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
        granted_by: row.granted_by,
        id: row.id,
      });
      userRolesMap.set(row.user_id, list);
    }

    const result = usersList.map((u) => ({
      id: u.id,
      email: u.email,
      username: usernameFromMetadata(u.user_metadata),
      created_at: u.created_at,
      last_sign_in: u.last_sign_in_at,
      email_confirmed: u.email_confirmed_at !== null && u.email_confirmed_at !== undefined,
      roles: u.id ? (userRolesMap.get(u.id) ?? []) : [],
    }));

    result.sort((a, b) => {
      const ae = (a.email ?? '').toLowerCase();
      const be = (b.email ?? '').toLowerCase();
      return ae < be ? -1 : ae > be ? 1 : 0;
    });

    return c.json({ users: result, count: result.length });
  } catch (err) {
    log.error({ err }, 'Error listing users');
    return c.json({ error: 'Failed to list users' }, 500);
  }
});

// ---------------------------------------------------------------------------
// PUT /api/users/:userId/roles  — atomic replace.
// ---------------------------------------------------------------------------
usersRouter.put('/api/users/:userId/roles', requireRole(canManageUsers), async (c) => {
  const userId = c.req.param('userId');
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const newRolesRaw = data['roles'];
    const newRoles = Array.isArray(newRolesRaw)
      ? newRolesRaw.filter((x): x is string => typeof x === 'string')
      : [];

    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    // Team members may replace the feature roles but must leave the
    // privileged set exactly as it is — the replace is all-roles-atomic,
    // so compare the target's CURRENT privileged roles against the
    // incoming set and 403 on any difference (add or remove).
    if (!(await canAssignPrivilegedRoles(c.get('userId') as string))) {
      const existing = await pool.query<{ role: string }>(
        'SELECT role FROM user_roles WHERE user_id = $1',
        [userId],
      );
      const currentPriv = existing.rows
        .map((r) => r.role)
        .filter(isPrivilegedRole)
        .sort();
      const newPriv = [...new Set(newRoles.filter(isPrivilegedRole))].sort();
      if (currentPriv.join('\x00') !== newPriv.join('\x00')) {
        return c.json(PRIVILEGED_ONLY, 403);
      }
    }

    // Atomic: DELETE then INSERT in one transaction so a concurrent
    // reader never sees a partial role set. Python issues both
    // statements on the same connection but without a wrapping
    // BEGIN/COMMIT — that's a latent bug in python; we fix it on the
    // way through (the externally observable result is identical).
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('DELETE FROM user_roles WHERE user_id = $1', [userId]);
      for (const role of newRoles) {
        await client.query(
          `INSERT INTO user_roles (user_id, role, granted_by)
           VALUES ($1, $2, 'admin')
           ON CONFLICT (user_id, role) DO NOTHING`,
          [userId, role],
        );
      }
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }

    invalidateUserRolesCache(userId);
    log.info({ userId, roles: newRoles }, 'Updated user roles');
    return c.json({ success: true, user_id: userId, roles: newRoles });
  } catch (err) {
    log.error({ err, userId }, 'Error updating user roles');
    return c.json({ error: 'Failed to update roles' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/users/:userId/roles  — add a single role.
// ---------------------------------------------------------------------------
usersRouter.post('/api/users/:userId/roles', requireRole(canManageUsers), async (c) => {
  const userId = c.req.param('userId');
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const role = data['role'];
    if (typeof role !== 'string' || !role) {
      return c.json({ error: 'Role is required' }, 400);
    }
    if (isPrivilegedRole(role) && !(await canAssignPrivilegedRoles(c.get('userId') as string))) {
      return c.json(PRIVILEGED_ONLY, 403);
    }

    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    await pool.query(
      `INSERT INTO user_roles (user_id, role, granted_by)
       VALUES ($1, $2, 'admin')
       ON CONFLICT (user_id, role) DO NOTHING`,
      [userId, role],
    );

    invalidateUserRolesCache(userId);
    log.info({ userId, role }, 'Added role to user');
    return c.json({ success: true, user_id: userId, added_role: role });
  } catch (err) {
    log.error({ err, userId }, 'Error adding user role');
    return c.json({ error: 'Failed to add role' }, 500);
  }
});

// ---------------------------------------------------------------------------
// DELETE /api/users/:userId/roles/:role
// ---------------------------------------------------------------------------
usersRouter.delete('/api/users/:userId/roles/:role', requireRole(canManageUsers), async (c) => {
  const userId = c.req.param('userId');
  const role = c.req.param('role');
  try {
    if (isPrivilegedRole(role) && !(await canAssignPrivilegedRoles(c.get('userId') as string))) {
      return c.json(PRIVILEGED_ONLY, 403);
    }

    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

    await pool.query(
      'DELETE FROM user_roles WHERE user_id = $1 AND role = $2',
      [userId, role],
    );

    invalidateUserRolesCache(userId);
    log.info({ userId, role }, 'Removed role from user');
    return c.json({ success: true, user_id: userId, removed_role: role });
  } catch (err) {
    log.error({ err, userId, role }, 'Error removing user role');
    return c.json({ error: 'Failed to remove role' }, 500);
  }
});

export { isPrivilegedRole };
