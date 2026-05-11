/**
 * Role helpers backed by the user_roles table.
 *
 * Mirrors the role-checking logic spread across python
 * external_api_proxy.py:13860-13993. The python version maintains a
 * 60s in-process cache (`_role_cache`) keyed by `editor_<uid>` /
 * `admin_<uid>` — we replicate that cache here so repeated checks
 * inside the same process don't hammer the DB.
 *
 * Privilege model (from python comment block at 13957-13965):
 *   - owner        : everything, including assigning privileged roles.
 *   - team_member  : editor-request approvals + user management; cannot
 *                    assign privileged roles (team_member / dev / owner).
 *   - dev          : Dev tab visibility only.
 *   - map_editor   : Map editor page access.
 *   - pager_contributor / radio_contributor : feature roles, no admin.
 */
import { getPool } from '../../db/pool.js';

const PRIVILEGED_ROLES: ReadonlySet<string> = new Set(['owner', 'team_member', 'dev']);
const ROLE_CACHE_TTL_MS = 60_000;

interface CacheEntry {
  ts: number;
  roles: string[];
}
const roleCache = new Map<string, CacheEntry>();

export function _resetRolesCacheForTests(): void {
  roleCache.clear();
}

export function invalidateUserRolesCache(userId: string): void {
  roleCache.delete(userId);
}

/**
 * Fetch all role strings assigned to a user. Returns [] if the user
 * has none, or if the DB is unavailable (caller is responsible for
 * deciding what "no roles" means in their context — for the public
 * /api/check-editor endpoint, no roles == no access, which is the
 * correct fallback).
 */
export async function getUserRoles(userId: string): Promise<string[]> {
  if (!userId) return [];

  const cached = roleCache.get(userId);
  if (cached && Date.now() - cached.ts < ROLE_CACHE_TTL_MS) {
    return cached.roles;
  }

  const pool = await getPool();
  if (!pool) return [];

  const result = await pool.query<{ role: string }>(
    'SELECT role FROM user_roles WHERE user_id = $1',
    [userId],
  );
  const roles = result.rows.map((r) => r.role);
  roleCache.set(userId, { ts: Date.now(), roles });
  return roles;
}

/**
 * True if the user has at least one of the named roles. Names are
 * matched case-sensitively, mirroring python's literal `'owner' in
 * user_roles` checks.
 */
export async function hasRole(userId: string, roleNames: readonly string[]): Promise<boolean> {
  const roles = await getUserRoles(userId);
  for (const wanted of roleNames) {
    if (roles.includes(wanted)) return true;
  }
  return false;
}

export async function isOwner(userId: string): Promise<boolean> {
  return hasRole(userId, ['owner']);
}

/**
 * Owner OR team_member — gates the editor-request management screens
 * and the Users tab. Mirrors python's
 *   can_view_users = is_owner or is_team_member
 */
export async function canManageUsers(userId: string): Promise<boolean> {
  return hasRole(userId, ['owner', 'team_member']);
}

/**
 * Only owners can grant the privileged roles (team_member, dev, owner).
 * Team members can edit users but not promote them. Mirrors python's
 *   'can_assign_privileged_roles': is_owner
 */
export async function canAssignPrivilegedRoles(userId: string): Promise<boolean> {
  return isOwner(userId);
}

export function isPrivilegedRole(role: string): boolean {
  return PRIVILEGED_ROLES.has(role);
}
