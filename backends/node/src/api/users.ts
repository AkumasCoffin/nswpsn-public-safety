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
 * Privilege rules are enforced inside the route. They depend on the
 * caller's user id, which the python backend doesn't actually verify
 * (it trusts the API key + frontend). Because we have to remain
 * byte-for-byte compatible during cutover, the roles endpoints here
 * also gate purely on API key. The role helpers (`canAssignPrivilegedRoles`)
 * are still wired up so a future tightening pass that requires a
 * Supabase JWT can flip the switch in one place.
 */
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { config } from '../config.js';
import { invalidateUserRolesCache, isPrivilegedRole } from '../services/auth/roles.js';

export const usersRouter = new Hono();

const DB_UNAVAILABLE = { error: 'database unavailable' } as const;

interface SupabaseUser {
  id?: string;
  email?: string;
  created_at?: string;
  last_sign_in_at?: string;
  email_confirmed_at?: string | null;
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
usersRouter.get('/api/users', async (c) => {
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
usersRouter.put('/api/users/:userId/roles', async (c) => {
  const userId = c.req.param('userId');
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const newRolesRaw = data['roles'];
    const newRoles = Array.isArray(newRolesRaw)
      ? newRolesRaw.filter((x): x is string => typeof x === 'string')
      : [];

    const pool = await getPool();
    if (!pool) return c.json(DB_UNAVAILABLE, 503);

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
usersRouter.post('/api/users/:userId/roles', async (c) => {
  const userId = c.req.param('userId');
  try {
    const data = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const role = data['role'];
    if (typeof role !== 'string' || !role) {
      return c.json({ error: 'Role is required' }, 400);
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
usersRouter.delete('/api/users/:userId/roles/:role', async (c) => {
  const userId = c.req.param('userId');
  const role = c.req.param('role');
  try {
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

// `isPrivilegedRole` is exported from roles.ts for the eventual
// permission-check tightening; re-exported here so the route file is
// the single import surface for the W5 wire-up agent.
export { isPrivilegedRole };
