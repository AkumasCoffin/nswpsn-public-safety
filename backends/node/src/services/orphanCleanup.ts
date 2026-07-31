/**
 * Remove "incomplete signups" — Supabase auth accounts that have NO roles AND
 * NO editor request (matched by both supabase_user_id AND email).
 *
 * These are minted when someone uses Discord OAuth from the LOGIN page (any
 * first OAuth creates the account) but never completes the signup form, so no
 * editor_request is ever created. A finished signup ALWAYS has a request; an
 * approved user ALWAYS has a role — so those are never touched.
 *
 * The signal is structural (no role + no request), not a long timer. The only
 * time element is a small race guard (default 15 min) that skips a just-created
 * account whose request POST may still be in flight during a normal signup.
 */
import type { Pool } from 'pg';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { invalidateUserRolesCache } from './auth/roles.js';

export interface OrphanUser {
  id: string;
  email: string | null;
  created_at: string;
}

interface SupaUser {
  id: string;
  email: string | null;
  created_at: string;
}

const RACE_GUARD_MS =
  (() => {
    const m = Number(process.env['ORPHAN_SIGNUP_RACE_GUARD_MINS']);
    return Number.isFinite(m) && m >= 0 ? m : 15;
  })() * 60_000;

/** Auto-sweep is opt-in — the owner button works regardless. Set
 *  ORPHAN_SIGNUP_CLEANUP=true once you've verified the button removes the right
 *  accounts. */
export function orphanAutoSweepEnabled(): boolean {
  return process.env['ORPHAN_SIGNUP_CLEANUP'] === 'true';
}

async function supaHeaders(): Promise<Record<string, string> | null> {
  if (!config.SUPABASE_URL || !config.SUPABASE_SERVICE_ROLE_KEY) return null;
  return {
    apikey: config.SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${config.SUPABASE_SERVICE_ROLE_KEY}`,
    'Content-Type': 'application/json',
  };
}

/** Page through the Supabase admin users list (default page size is small). */
async function listAllSupabaseUsers(): Promise<SupaUser[]> {
  const headers = await supaHeaders();
  if (!headers) return [];
  const out: SupaUser[] = [];
  const perPage = 200;
  for (let page = 1; page <= 100; page++) {
    const url = `${config.SUPABASE_URL}/auth/v1/admin/users?page=${page}&per_page=${perPage}`;
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(15_000) });
    if (!res.ok) {
      log.warn({ status: res.status }, 'orphanCleanup: failed to list Supabase users');
      break;
    }
    const body = (await res.json()) as { users?: SupaUser[] };
    const users = body.users ?? [];
    out.push(...users);
    if (users.length < perPage) break;
  }
  return out;
}

/** Delete a Supabase account + its local rows (roles, nodes). Mirrors the
 *  DELETE /api/users handler. Per-node tokens live on the node row, so deleting
 *  the nodes revokes them too. */
export async function deleteAccount(pool: Pool, userId: string): Promise<boolean> {
  const headers = await supaHeaders();
  if (!headers) return false;
  const url = `${config.SUPABASE_URL}/auth/v1/admin/users/${encodeURIComponent(userId)}`;
  const res = await fetch(url, {
    method: 'DELETE',
    headers,
    signal: AbortSignal.timeout(15_000),
  });
  // 404 = already gone; still clean up our own rows.
  if (![200, 204, 404].includes(res.status)) {
    log.warn({ userId, status: res.status }, 'orphanCleanup: Supabase account delete failed');
    return false;
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM user_roles WHERE user_id = $1', [userId]);
    await client.query('DELETE FROM nodes WHERE user_id = $1', [userId]);
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    log.warn({ err: (err as Error).message, userId }, 'orphanCleanup: local cleanup failed after delete');
  } finally {
    client.release();
  }
  invalidateUserRolesCache(userId);
  return true;
}

/**
 * The safety-critical rule, pure + exported for tests: which accounts are
 * "incomplete signups". An account is selected for removal ONLY when it has
 * NONE of:
 *   - a role (approved user), OR
 *   - an editor request linked by its id (signed up), OR
 *   - an editor request linked by its email (signed up), OR
 *   - a created_at newer than the race guard (an in-flight signup).
 *
 * So anyone who submitted the signup form is ALWAYS kept, even before an owner
 * assigns them roles — the pending request protects them.
 */
export function selectOrphans(
  users: ReadonlyArray<OrphanUser>,
  opts: {
    roleIds: ReadonlySet<string>;
    reqIds: ReadonlySet<string>;
    reqEmails: ReadonlySet<string>;
    nowMs: number;
    raceGuardMs: number;
  },
): OrphanUser[] {
  const cutoff = opts.nowMs - opts.raceGuardMs;
  const orphans: OrphanUser[] = [];
  for (const u of users) {
    if (opts.roleIds.has(u.id)) continue; // approved user — keep
    if (opts.reqIds.has(u.id)) continue; // signed up (request linked by id) — keep
    const email = (u.email ?? '').trim().toLowerCase();
    if (email && opts.reqEmails.has(email)) continue; // signed up (by email) — keep
    if (u.created_at && new Date(u.created_at).getTime() > cutoff) continue; // too fresh — keep
    orphans.push({ id: u.id, email: u.email, created_at: u.created_at });
  }
  return orphans;
}

/** Accounts with no roles AND no editor request. `respectRaceGuard` skips very
 *  recently created accounts (an in-flight signup). */
export async function findOrphanSignups(pool: Pool, respectRaceGuard = true): Promise<OrphanUser[]> {
  const users = await listAllSupabaseUsers();
  if (!users.length) return [];

  const roleRows = await pool.query<{ user_id: string }>('SELECT DISTINCT user_id FROM user_roles');
  const roleIds = new Set(roleRows.rows.map((r) => r.user_id));

  const reqRows = await pool.query<{ supabase_user_id: string | null; email: string | null }>(
    'SELECT supabase_user_id, email FROM editor_requests',
  );
  const reqIds = new Set<string>();
  const reqEmails = new Set<string>();
  for (const r of reqRows.rows) {
    if (r.supabase_user_id) reqIds.add(r.supabase_user_id);
    if (r.email) reqEmails.add(r.email.trim().toLowerCase());
  }

  return selectOrphans(users, {
    roleIds,
    reqIds,
    reqEmails,
    nowMs: Date.now(),
    raceGuardMs: respectRaceGuard ? RACE_GUARD_MS : 0,
  });
}

/** Find + delete incomplete signups. `dryRun` lists only. */
export async function sweepOrphanSignups(
  pool: Pool,
  opts: { dryRun?: boolean } = {},
): Promise<{ found: number; deleted: number; orphans: OrphanUser[] }> {
  const orphans = await findOrphanSignups(pool, true);
  if (opts.dryRun || orphans.length === 0) {
    return { found: orphans.length, deleted: 0, orphans };
  }
  let deleted = 0;
  for (const o of orphans) {
    if (await deleteAccount(pool, o.id)) deleted++;
  }
  log.info({ found: orphans.length, deleted }, 'orphanCleanup: swept incomplete signups');
  return { found: orphans.length, deleted, orphans };
}
