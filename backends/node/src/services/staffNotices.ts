/**
 * Manual notifications: a staff member tells users something directly.
 *
 * Every other user-facing message on this site is a side effect — an approval,
 * a ticket reply, a moderation action. This is the one that exists because a
 * person decided to say it, so it is held to a different standard than the
 * ambient ones: the fan-out is a single transaction and its errors surface,
 * because somebody is waiting to be told it went.
 *
 * WHO EXISTS. The roster is `user_roles`, not Supabase: every account is
 * granted `authed` at first sign-in (api/profiles.ts), the table is indexed by
 * role, and it is the only enumeration that does not page through an admin API
 * with a 1000-row ceiling. An account that has somehow never synced has no row
 * here and cannot be reached by any audience — including Everyone.
 *
 * WHERE IT LANDS. The `notifications` table, like everything else, picked up
 * by the bell on its 60s poll. There is no outbound path in this codebase: a
 * notice waits for its recipient to open the site.
 */
import type { Pool, PoolClient } from 'pg';

import { log } from '../lib/log.js';
import { storedRoleNames } from './auth/roles.js';

/** The notification `type` these are filed under, for the bell and for stats. */
export const NOTICE_TYPE = 'staff.notice';

/** How many people one notice may name individually. */
export const MAX_NAMED_RECIPIENTS = 200;

export type NoticeAudience = 'user' | 'users' | 'role' | 'all';

export interface NoticeTarget {
  audience: NoticeAudience;
  /** For 'user' / 'users'. */
  userIds?: readonly string[];
  /** For 'role'. */
  role?: string | null;
}

export interface SendNoticeInput {
  sentBy: string;
  sentByName: string | null;
  target: NoticeTarget;
  title: string;
  body: string;
  link?: string | null;
}

/**
 * Everyone this notice is for, deduped.
 *
 * A role target asks for every name the role may be STORED under, not just the
 * canonical one: migration 059 renamed the roles but left existing rows alone,
 * so selecting `wire:contributor` alone would silently miss everyone still
 * recorded as `media_feeder`.
 *
 * Named recipients are intersected with the roster rather than trusted. The
 * ids come from a picker and should be real, but an id that is not in
 * user_roles belongs to no account this site knows about, and writing a
 * notification row for it would be writing into nobody's inbox forever.
 */
export async function resolveRecipients(pool: Pool, target: NoticeTarget): Promise<string[]> {
  if (target.audience === 'all') {
    const r = await pool.query<{ user_id: string }>(
      `SELECT DISTINCT user_id FROM user_roles WHERE role = 'authed'`,
    );
    return r.rows.map((x) => x.user_id);
  }

  if (target.audience === 'role') {
    const role = (target.role ?? '').trim();
    if (!role) return [];
    const r = await pool.query<{ user_id: string }>(
      `SELECT DISTINCT user_id FROM user_roles WHERE role = ANY($1::text[])`,
      [storedRoleNames(role)],
    );
    return r.rows.map((x) => x.user_id);
  }

  const wanted = [...new Set((target.userIds ?? []).map((u) => String(u).trim()).filter(Boolean))];
  if (wanted.length === 0) return [];
  const r = await pool.query<{ user_id: string }>(
    `SELECT DISTINCT user_id FROM user_roles WHERE user_id = ANY($1::text[])`,
    [wanted],
  );
  return r.rows.map((x) => x.user_id);
}

export interface SentNotice {
  noticeId: string;
  recipients: number;
}

/**
 * Record the send and deliver it, or do neither.
 *
 * One transaction on purpose. The alternative — write the record, then fan out
 * best-effort like the ambient notifications do — can report "sent to 340
 * people" when nobody got it, and there is no way to tell afterwards which
 * happened.
 */
export async function sendNotice(
  pool: Pool,
  input: SendNoticeInput,
  recipients: readonly string[],
): Promise<SentNotice> {
  const targets = [...new Set(recipients.filter(Boolean))];
  if (targets.length === 0) {
    throw new Error('a notice needs at least one recipient');
  }

  const client: PoolClient = await pool.connect();
  try {
    await client.query('BEGIN');
    const notice = await client.query<{ id: string }>(
      `INSERT INTO staff_notices
         (sent_by, sent_by_name, audience, target_role, target_user_ids, recipients, title, body, link)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       RETURNING id`,
      [
        input.sentBy,
        input.sentByName,
        input.target.audience,
        input.target.audience === 'role' ? (input.target.role ?? null) : null,
        // Only worth keeping for a hand-picked audience: for a role or for
        // everyone the membership is the record, and it moves.
        input.target.audience === 'user' || input.target.audience === 'users' ? targets : null,
        targets.length,
        input.title,
        input.body,
        input.link ?? null,
      ],
    );
    const noticeId = notice.rows[0]!.id;

    await client.query(
      `INSERT INTO notifications (user_id, type, title, body, link, meta)
       SELECT u, $2, $3, $4, $5, $6::jsonb FROM unnest($1::text[]) AS u`,
      [
        targets,
        NOTICE_TYPE,
        input.title,
        input.body,
        input.link ?? null,
        JSON.stringify({ noticeId: Number(noticeId) }),
      ],
    );

    await client.query('COMMIT');
    return { noticeId, recipients: targets.length };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    log.error({ err, by: input.sentBy, audience: input.target.audience }, 'staff notice failed');
    throw err;
  } finally {
    client.release();
  }
}

export interface NoticeRow {
  id: string;
  sentBy: string;
  sentByName: string | null;
  audience: NoticeAudience;
  targetRole: string | null;
  recipients: number;
  title: string;
  body: string;
  link: string | null;
  createdAt: string;
}

/** What has been sent, newest first. */
export async function listNotices(pool: Pool, limit = 20): Promise<NoticeRow[]> {
  const n = Math.max(1, Math.min(100, Math.trunc(limit) || 20));
  const r = await pool.query<{
    id: string; sent_by: string; sent_by_name: string | null; audience: NoticeAudience;
    target_role: string | null; recipients: number; title: string; body: string;
    link: string | null; created_at: Date;
  }>(
    `SELECT id, sent_by, sent_by_name, audience, target_role, recipients, title, body, link, created_at
       FROM staff_notices
      ORDER BY created_at DESC
      LIMIT $1`,
    [n],
  );
  return r.rows.map((x) => ({
    id: String(x.id),
    sentBy: x.sent_by,
    sentByName: x.sent_by_name,
    audience: x.audience,
    targetRole: x.target_role,
    recipients: x.recipients,
    title: x.title,
    body: x.body,
    link: x.link,
    createdAt: x.created_at instanceof Date ? x.created_at.toISOString() : String(x.created_at),
  }));
}

export interface TargetCounts {
  all: number;
  byRole: Array<{ role: string; count: number }>;
}

/**
 * How many people each audience would reach, so the compose form can say so
 * BEFORE the send rather than after it. One grouped scan of a small table.
 *
 * Legacy and canonical names are folded together here the same way
 * resolveRecipients expands them, so the number shown is the number that will
 * actually be written.
 */
export async function targetCounts(pool: Pool): Promise<TargetCounts> {
  const r = await pool.query<{ role: string; user_id: string }>(
    `SELECT DISTINCT role, user_id FROM user_roles`,
  );
  const everyone = new Set<string>();
  const byRole = new Map<string, Set<string>>();
  for (const row of r.rows) {
    if (row.role === 'authed') {
      everyone.add(row.user_id);
      continue;
    }
    // A legacy row counts under the canonical name — that is what the picker
    // offers and what resolveRecipients will ask for, so the two agree.
    const canonical = storedRoleNames(row.role)[0]!;
    const set = byRole.get(canonical) ?? new Set<string>();
    set.add(row.user_id);
    byRole.set(canonical, set);
  }
  return {
    all: everyone.size,
    byRole: [...byRole.entries()]
      .map(([role, users]) => ({ role, count: users.size }))
      .sort((a, b) => a.role.localeCompare(b.role)),
  };
}
