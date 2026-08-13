/**
 * Role helpers backed by the user_roles table.
 *
 * Role model (2026-08 refactor, migration 059): a role is either top-level or a
 * namespaced "main:sub" string. The MAIN part is a grouping label for the UI —
 * permissions are granted by the FULL string (the subrole), never by the main
 * part alone.
 *
 *   - authed             : base role every account gets on login (future
 *                          comment/like gate). Staff "Users" tab = only authed;
 *                          "Members" = authed + at least one real role.
 *   - owner              : everything, including assigning privileged roles.
 *   - staff              : user/role management + request review (was
 *                          team_member); cannot assign privileged roles.
 *   - feeder:radio       : radio feeder node (was radio_contributor)
 *   - feeder:pager       : pager feeder node (was pager_contributor)
 *   - feeder:agency_data : agency reference-table editing (was data_feeder)
 *   - feeder:monitor     : view-only Data/Nodes pages (was node_monitor)
 *   - feeder:manager     : feeder node/config management + data-change review
 *   - wire:contributor   : posts to The Wire (was media_feeder)
 *   - wire:manager       : Wire approvals + takedowns
 *   - map:editor         : map-editor page access (was map_editor)
 *   - map:manager        : map-editor oversight
 *
 * The 'dev' role was REMOVED in this refactor; its powers moved to owner +
 * feeder:manager. Managers manage their AREA's content/config only — approving
 * signups and assigning roles stays with staff/owner.
 *
 * A 60s in-process cache mirrors the old python backend (_role_cache) so
 * repeated checks inside one process don't hammer the DB.
 */
import type { MiddlewareHandler } from 'hono';
import { getPool } from '../../db/pool.js';

/**
 * Legacy-name compatibility shim for the migration-059 rename. The migration
 * rewrites the stored rows, but this keeps BOTH names resolving during the
 * cutover — so a stale cached role list, an un-migrated database, or any
 * literal old name still passed to hasRole() keeps working instead of silently
 * locking someone out. getUserRoles() expands each stored role to include its
 * counterpart. Remove once the rename has fully settled.
 */
const ROLE_ALIASES: Readonly<Record<string, string>> = {
  team_member: 'staff',
  radio_contributor: 'feeder:radio',
  pager_contributor: 'feeder:pager',
  data_feeder: 'feeder:agency_data',
  node_monitor: 'feeder:monitor',
  media_feeder: 'wire:contributor',
  map_editor: 'map:editor',
};
const ROLE_ALIASES_REVERSE: Readonly<Record<string, string>> = Object.fromEntries(
  Object.entries(ROLE_ALIASES).map(([oldName, newName]) => [newName, oldName]),
);

/** Expand a stored role list so old and new names both resolve. */
function expandAliases(roles: readonly string[]): string[] {
  const out = new Set(roles);
  for (const r of roles) {
    const fwd = ROLE_ALIASES[r];
    if (fwd) out.add(fwd);
    const rev = ROLE_ALIASES_REVERSE[r];
    if (rev) out.add(rev);
  }
  return [...out];
}

/**
 * Canonicalise a role name (legacy → current). Use at every ASSIGNMENT site so
 * newly granted roles are always stored under the new name.
 */
export function canonicalRole(role: string): string {
  return ROLE_ALIASES[role] ?? role;
}

/**
 * Every role that may be assigned. Assignment endpoints validate against this
 * so a typo'd or junk role name can't be written into user_roles (the column is
 * free-form TEXT with no DB-level constraint). Legacy names are accepted too —
 * canonicalRole() maps them to the new name before the check.
 */
export const KNOWN_ROLES: ReadonlySet<string> = new Set([
  'authed',
  'owner',
  'staff',
  'feeder:radio',
  'feeder:pager',
  'feeder:agency_data',
  'feeder:monitor',
  'feeder:manager',
  'wire:contributor',
  'wire:manager',
  'map:editor',
  'map:manager',
]);

/** True if `role` (after canonicalisation) is assignable. */
export function isKnownRole(role: string): boolean {
  return KNOWN_ROLES.has(canonicalRole(role));
}

// 'team_member' stays listed so the legacy name is still treated as privileged
// if it ever reaches these checks pre-migration. 'dev' removed with the role.
const PRIVILEGED_ROLES: ReadonlySet<string> = new Set(['owner', 'staff', 'team_member']);
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
 * The CANONICAL (current-name) view of a role list, for DISPLAY — e.g. the
 * `roles` array returned to the staff UI. Drops legacy names from an expanded
 * list so a user isn't shown both `team_member` and `staff` for the same grant
 * (expansion always adds the current name alongside a legacy one). Derived from
 * the already-fetched list, so it costs no extra query.
 */
export function canonicalRoles(roles: readonly string[]): string[] {
  return roles.filter((r) => !ROLE_ALIASES[r]);
}

/**
 * Fetch a user's roles as CANONICAL current names (one query, alias-collapsed).
 * Convenience wrapper over getUserRoles + canonicalRoles for display callers.
 */
export async function getUserRolesRaw(userId: string): Promise<string[]> {
  return canonicalRoles(await getUserRoles(userId));
}

/**
 * Fetch all role strings assigned to a user, EXPANDED so legacy and current
 * names both resolve (see ROLE_ALIASES) — this is what permission checks run
 * against. Returns [] if the user has none, or if the DB is unavailable
 * (caller decides what "no roles" means in their context — for the public
 * /api/check-editor endpoint, no roles == no access, the correct fallback).
 */
export async function getUserRoles(userId: string): Promise<string[]> {
  if (!userId) return [];

  const cached = roleCache.get(userId);
  if (cached && Date.now() - cached.ts < ROLE_CACHE_TTL_MS) {
    // Return a copy so callers can't mutate the cached array in place.
    return [...cached.roles];
  }

  const pool = await getPool();
  if (!pool) return [];

  const result = await pool.query<{ role: string }>(
    'SELECT role FROM user_roles WHERE user_id = $1',
    [userId],
  );
  // Expand legacy<->current names so a check for either passes during the
  // migration-059 cutover (see ROLE_ALIASES).
  const roles = expandAliases(result.rows.map((r) => r.role));
  roleCache.set(userId, { ts: Date.now(), roles });
  // Return a copy so callers can't mutate the array now held in cache.
  return [...roles];
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
 * Owner OR staff — gates the editor-request management screens and the Auth
 * (Members/Users) tab. Managers deliberately do NOT get this: they manage their
 * area's content/config, not user accounts or role grants.
 */
export async function canManageUsers(userId: string): Promise<boolean> {
  return hasRole(userId, ['owner', 'staff']);
}

/**
 * Owner OR feeder:manager — gates the feeder-node management screens (Nodes
 * tab) and the staff /api/nodes endpoints. (Was owner|dev; 'dev' was removed
 * in the migration-059 refactor and its node powers moved to feeder:manager.)
 */
export async function canManageNodes(userId: string): Promise<boolean> {
  return hasRole(userId, ['owner', 'feeder:manager']);
}

/**
 * Owner, feeder:manager, OR feeder:monitor — gates READ access to the staff
 * Data page and the Nodes-page views (GET /api/node-data/*, GET /api/nodes/*).
 * feeder:monitor is view-only: every mutating node route stays on
 * canManageNodes.
 */
export async function canViewNodeData(userId: string): Promise<boolean> {
  return hasRole(userId, ['owner', 'feeder:manager', 'feeder:monitor']);
}

/**
 * Only owners can grant the privileged roles (staff, owner). Staff can edit
 * users but not promote them.
 */
export async function canAssignPrivilegedRoles(userId: string): Promise<boolean> {
  return isOwner(userId);
}

/**
 * Owner, staff, map:editor, or map:manager — gates the incident CRUD used by
 * map-editor.html. Key-only gating was effectively unauthenticated
 * (NSWPSN_API_KEY is public via /api/config), so mutating incidents
 * requires a real editor login like the other privileged routes.
 */
export async function canEditIncidents(userId: string): Promise<boolean> {
  return hasRole(userId, ['owner', 'staff', 'map:editor', 'map:manager']);
}

/**
 * Owner OR feeder:agency_data — may edit agency reference tables (add/update/
 * delete rows) on the agency page. Owner edits apply instantly; contributor
 * edits become pending data-change requests (see canReviewAgencyData).
 */
export async function canEditAgencyData(userId: string): Promise<boolean> {
  return hasRole(userId, ['owner', 'feeder:agency_data']);
}

/**
 * Owner, staff, OR feeder:manager — may review (approve/reject) pending agency
 * data-change requests. The feeder manager owns that area's data.
 */
export async function canReviewAgencyData(userId: string): Promise<boolean> {
  return hasRole(userId, ['owner', 'staff', 'feeder:manager']);
}

/**
 * Owner, wire:contributor, OR wire:manager — may publish/edit The Wire media
 * posts and articles. Managers post as well as moderate (and their own posts
 * skip the approval queue — see canModerateWire, which the create handlers use
 * to decide published-vs-pending). Per-item edit/delete is further restricted
 * to the author (or an admin override via canManageUsers) inside the handlers.
 */
export async function canFeedMedia(userId: string): Promise<boolean> {
  return hasRole(userId, ['owner', 'wire:contributor', 'wire:manager']);
}

/**
 * Owner, staff, OR wire:manager — may approve/reject pending Wire posts, soft-
 * remove any post/article, and action takedown notices.
 */
export async function canModerateWire(userId: string): Promise<boolean> {
  return hasRole(userId, ['owner', 'staff', 'wire:manager']);
}

export function isPrivilegedRole(role: string): boolean {
  return PRIVILEGED_ROLES.has(role);
}

/**
 * Middleware factory that gates a route on a role check. Requires a
 * verified Supabase user (`c.get('userId')`, set upstream by
 * optionalSupabaseJwt / requireSupabaseJwt) and that `check(userId)`
 * resolves true.
 *
 *   - 401 when no verified user is present (only the public API key was
 *     supplied, or no/invalid JWT) — these admin routes need a real user.
 *   - 403 when the user is authenticated but lacks the role.
 *
 * Use on privileged routes (role management, editor-request approval,
 * admin DB ops) so the public NSWPSN_API_KEY alone can no longer reach
 * them — only a logged-in user with the right role can.
 */
export function requireRole(
  check: (userId: string) => Promise<boolean>,
): MiddlewareHandler {
  return async (c, next) => {
    const userId = c.get('userId');
    if (!userId) {
      return c.json({ error: 'authentication required' }, 401);
    }
    let allowed = false;
    try {
      allowed = await check(userId);
    } catch {
      allowed = false;
    }
    if (!allowed) {
      return c.json({ error: 'forbidden' }, 403);
    }
    await next();
  };
}
