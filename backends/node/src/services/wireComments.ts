/**
 * Comment moderation + notification helpers for The Wire.
 *
 * Two responsibilities:
 *   - Commenting restrictions: a `timeout` (timed, lapses on its own) or a
 *     `pause` (indefinite, until lifted). Both block commenting site-wide and
 *     carry a reason the user is told about.
 *   - Notifications: single-row inserts written inline by whatever action
 *     caused them. Every write is best-effort — a notification failure must
 *     never break the comment/like/moderation action that triggered it.
 *
 * No queue: these are single-row inserts (largest fan-out is one INSERT…SELECT)
 * in the request that already writes the comment. A queue would add an external
 * failure mode and turn an atomic write into one that can half-fail. If
 * OUTBOUND delivery (email/push/Discord DM) is added later, that's the layer to
 * put a queue in front of — it doesn't change anything here.
 */
import type { Pool } from 'pg';
import { log } from '../lib/log.js';

/**
 * Reasons a manager can cite. The client sends a KEY and the server resolves
 * the label, so the text shown to a restricted user is always one we control
 * (never attacker-supplied free text).
 */
export const RESTRICTION_REASONS: Readonly<Record<string, string>> = {
  spam: 'Spam',
  off_topic: 'Unrelated to the post',
  swearing: 'Swearing',
  harassment: 'Harassment',
  hate_speech: 'Hate speech',
  misinformation: 'Misinformation',
  other: 'Other',
};

export function reasonLabel(key: unknown): string | null {
  return typeof key === 'string' && RESTRICTION_REASONS[key] ? RESTRICTION_REASONS[key] : null;
}

/** Durations offered for a timeout, in hours. `null` (pause) is indefinite. */
export const TIMEOUT_HOURS: readonly number[] = [1, 6, 24, 168];

export interface ActiveRestriction {
  id: number;
  kind: 'timeout' | 'pause';
  reason: string;      // key
  reasonLabel: string; // resolved label
  note: string | null;
  expiresAt: string | null; // ISO, null = indefinite
}

/**
 * The live restriction for a user, or null. A timeout lapses purely by time —
 * `expires_at <= now()` simply stops matching, so there's no cron to run and no
 * risk of a stale block outliving its window.
 */
export async function activeRestriction(pool: Pool, userId: string): Promise<ActiveRestriction | null> {
  if (!userId) return null;
  try {
    const r = await pool.query<{
      id: string; kind: 'timeout' | 'pause'; reason: string; note: string | null; expires_at: Date | null;
    }>(
      `SELECT id, kind, reason, note, expires_at
         FROM comment_restrictions
        WHERE user_id = $1
          AND lifted_at IS NULL
          AND (expires_at IS NULL OR expires_at > now())
        ORDER BY expires_at IS NULL DESC, expires_at DESC
        LIMIT 1`,
      [userId],
    );
    const row = r.rows[0];
    if (!row) return null;
    return {
      id: Number(row.id),
      kind: row.kind,
      reason: row.reason,
      reasonLabel: reasonLabel(row.reason) ?? 'Other',
      note: row.note,
      expiresAt: row.expires_at ? new Date(row.expires_at).toISOString() : null,
    };
  } catch (err) {
    // Fail OPEN on a lookup error: a DB hiccup shouldn't silently mute everyone.
    // The moderation action itself is still recorded, so this only affects the
    // window in which the DB is unhealthy.
    log.warn({ err, userId }, 'comment restriction lookup failed — allowing');
    return null;
  }
}

export interface NotifyInput {
  userId: string;
  type: string;
  title: string;
  body?: string | null;
  link?: string | null;
  meta?: Record<string, unknown>;
  /** Never notify this user (used to skip notifying yourself about your own action). */
  exceptUserId?: string | null;
}

/** Insert one notification. Best-effort; never throws. */
export async function notify(pool: Pool, n: NotifyInput): Promise<void> {
  if (!n.userId) return;
  if (n.exceptUserId && n.userId === n.exceptUserId) return; // no self-notifications
  try {
    await pool.query(
      `INSERT INTO notifications (user_id, type, title, body, link, meta)
       VALUES ($1,$2,$3,$4,$5,$6::jsonb)`,
      [n.userId, n.type, n.title, n.body ?? null, n.link ?? null, JSON.stringify(n.meta ?? {})],
    );
  } catch (err) {
    log.debug({ err, userId: n.userId, type: n.type }, 'notify failed');
  }
}

/** Insert the same notification for many users in one statement. Best-effort. */
export async function notifyMany(
  pool: Pool,
  userIds: readonly string[],
  n: Omit<NotifyInput, 'userId'>,
): Promise<void> {
  const targets = [...new Set(userIds.filter((u) => u && u !== n.exceptUserId))];
  if (targets.length === 0) return;
  try {
    await pool.query(
      `INSERT INTO notifications (user_id, type, title, body, link, meta)
       SELECT u, $2, $3, $4, $5, $6::jsonb FROM unnest($1::text[]) AS u`,
      [targets, n.type, n.title, n.body ?? null, n.link ?? null, JSON.stringify(n.meta ?? {})],
    );
  } catch (err) {
    log.debug({ err, count: targets.length, type: n.type }, 'notifyMany failed');
  }
}

/**
 * Like notifications, batched. A popular post would otherwise ring the bell
 * once per like — instead, fold into the recipient's existing UNREAD like
 * notification for the same post from the last hour, bumping a count in meta.
 */
export async function notifyLikeBatched(
  pool: Pool,
  opts: { ownerId: string; actorId: string; parentType: string; parentId: string; title: string; link: string },
): Promise<void> {
  if (!opts.ownerId || opts.ownerId === opts.actorId) return;
  try {
    const upd = await pool.query(
      `UPDATE notifications
          SET meta = jsonb_set(meta, '{count}',
                     to_jsonb(COALESCE((meta->>'count')::int, 1) + 1)),
              title = $4,
              created_at = now()
        WHERE id = (SELECT id FROM notifications
                     WHERE user_id = $1 AND type = 'wire.like' AND read_at IS NULL
                       AND meta->>'parent_id' = $2 AND meta->>'parent_type' = $3
                       AND created_at > now() - interval '1 hour'
                     ORDER BY created_at DESC LIMIT 1)
        RETURNING id`,
      [opts.ownerId, opts.parentId, opts.parentType, `New likes on "${opts.title}"`],
    );
    if ((upd.rowCount ?? 0) > 0) return; // folded into the existing one
    await notify(pool, {
      userId: opts.ownerId,
      type: 'wire.like',
      title: `Someone liked "${opts.title}"`,
      link: opts.link,
      meta: { parent_type: opts.parentType, parent_id: opts.parentId, count: 1 },
      exceptUserId: opts.actorId,
    });
  } catch (err) {
    log.debug({ err }, 'notifyLikeBatched failed');
  }
}

/** Everyone who has commented on a post (for "activity on a post you commented on"). */
export async function commenterIds(pool: Pool, parentType: string, parentId: string): Promise<string[]> {
  try {
    const r = await pool.query<{ author_id: string }>(
      `SELECT DISTINCT author_id FROM wire_comments
        WHERE parent_type = $1 AND parent_id = $2 AND deleted_at IS NULL`,
      [parentType, parentId],
    );
    return r.rows.map((x) => x.author_id);
  } catch {
    return [];
  }
}

/**
 * user_id -> public avatar URL, for a batch of users.
 *
 * Same precedence as the profile page (api/profiles.ts): a custom uploaded pfp
 * wins, otherwise the stored Discord avatar. Batched so rendering a comment
 * thread or a feed page stays one query instead of one per author.
 */
export async function avatarMap(pool: Pool, userIds: readonly string[]): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  const ids = [...new Set(userIds.filter(Boolean))];
  if (ids.length === 0) return map;
  try {
    const { avatarUrl } = await import('./wire.js');
    const r = await pool.query<{ user_id: string; avatar_key: string | null; discord_avatar_url: string | null }>(
      'SELECT user_id, avatar_key, discord_avatar_url FROM user_profiles WHERE user_id = ANY($1::text[])',
      [ids],
    );
    for (const row of r.rows) {
      const url = avatarUrl(row.avatar_key, row.discord_avatar_url);
      if (url) map.set(row.user_id, url);
    }
  } catch (err) {
    // Avatars are decoration — never fail a read over them.
    log.debug({ err }, 'avatarMap failed');
  }
  return map;
}
