/**
 * The Wire — comments, likes, comment moderation and notifications.
 *
 * Split out of api/wire.ts (which is already ~1400 lines) but shares its
 * conventions: soft (parent_type, parent_id) refs, `currentUserId(c)` from the
 * globally-mounted optionalSupabaseJwt, and the same 503 DB_UNAVAILABLE shape.
 *
 * Auth model:
 *   - GET comments/likes  — public read, behind the same soft-launch gate as
 *                           the rest of the Wire.
 *   - POST comment / like — requireSupabaseJwt ("any logged-in user"). This is
 *                           the first real consumer of the `authed` base role.
 *   - DELETE comment      — the comment's author, OR canModerateWire.
 *   - restrictions        — requireRole(canModerateWire).
 *   - notifications       — requireSupabaseJwt; you only ever see your own.
 */
import { Hono } from 'hono';
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { config } from '../config.js';
import { requireSupabaseJwt } from '../services/auth/supabaseJwt.js';
import { requireRole, canModerateWire, canFeedMedia } from '../services/auth/roles.js';
import {
  RESTRICTION_REASONS,
  TIMEOUT_HOURS,
  reasonLabel,
  activeRestriction,
  notify,
  notifyMany,
  notifyLikeBatched,
  commenterIds,
  avatarMap,
} from '../services/wireComments.js';

export const wireCommentsRouter = new Hono();

const DB_UNAVAILABLE = { error: 'database unavailable' } as const;
const MAX_COMMENT_LEN = 280;

const TABLE_FOR: Record<string, string> = { media_post: 'media_posts', article: 'articles' };

function currentUserId(c: { get: (k: string) => unknown }): string | undefined {
  const v = c.get('userId');
  return typeof v === 'string' && v.length > 0 ? v : undefined;
}
function currentUserName(c: { get: (k: string) => unknown }): string | null {
  const v = c.get('userName');
  return typeof v === 'string' && v ? v : null;
}
function isoOrNull(v: unknown): string | null {
  if (v instanceof Date) return v.toISOString();
  return typeof v === 'string' ? v : null;
}

/** Same soft-launch gate as api/wire.ts — comments follow the posts. */
async function wireReadable(c: { get: (k: string) => unknown }): Promise<boolean> {
  if (config.WIRE_PUBLIC === 'true') return true;
  const uid = currentUserId(c);
  if (!uid) return false;
  return (await canFeedMedia(uid)) || (await canModerateWire(uid));
}

/** Normalise the ?type= param to a stored parent_type. */
function parentTypeOf(raw: unknown): 'media_post' | 'article' | null {
  const s = String(raw ?? '').trim();
  if (s === 'media_post' || s === 'media') return 'media_post';
  if (s === 'article' || s === 'articles') return 'article';
  return null;
}

/** The post row backing a comment/like, or null. Used for notification targets. */
async function loadParent(
  pool: Pool,
  parentType: string,
  parentId: string,
): Promise<{ author_id: string; co_authors: unknown; title: string; slug?: string } | null> {
  const table = TABLE_FOR[parentType];
  if (!table) return null;
  const cols = parentType === 'article' ? 'author_id, co_authors, title, slug' : 'author_id, co_authors, title';
  const r = await pool.query(`SELECT ${cols} FROM ${table} WHERE id = $1`, [parentId]);
  return (r.rows[0] as { author_id: string; co_authors: unknown; title: string; slug?: string }) ?? null;
}

/** Deep link back to a post, for a notification. */
function postLink(parentType: string, parent: { slug?: string }, parentId: string): string {
  return parentType === 'article'
    ? `/wire?tab=articles&article=${encodeURIComponent(parent.slug ?? parentId)}`
    : `/wire?tab=media&post=${encodeURIComponent(parentId)}`;
}

// ---------------------------------------------------------------------------
// Comments
// ---------------------------------------------------------------------------

wireCommentsRouter.get('/api/wire/comments', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  if (!(await wireReadable(c))) return c.json({ comments: [] });
  const url = new URL(c.req.url);
  const parentType = parentTypeOf(url.searchParams.get('type'));
  const parentId = (url.searchParams.get('id') ?? '').trim();
  if (!parentType || !parentId) return c.json({ error: 'type and id are required' }, 400);
  const limit = Math.max(1, Math.min(200, Number(url.searchParams.get('limit') ?? 100) || 100));
  try {
    const uid = currentUserId(c);
    const isMod = !!(uid && (await canModerateWire(uid)));
    const r = await pool.query(
      `SELECT id, author_id, author_name, body, created_at,
              deleted_at, deleted_by_name, delete_reason
         FROM wire_comments
        WHERE parent_type = $1 AND parent_id = $2
        ORDER BY created_at ASC
        LIMIT $3`,
      [parentType, parentId, limit],
    );
    // One batched lookup for every author on the page (no N+1).
    const avatars = await avatarMap(pool, r.rows.map((row) => row.author_id));
    // A deleted comment is tombstoned for everyone; only a moderator sees who
    // removed it and why (and never the original body).
    const comments = r.rows
      .filter((row) => !row.deleted_at || isMod)
      .map((row) => ({
        id: row.id,
        author: { id: row.author_id, name: row.author_name, avatar_url: avatars.get(row.author_id) ?? null },
        body: row.deleted_at ? null : row.body,
        deleted: !!row.deleted_at,
        deleted_by_name: isMod ? (row.deleted_by_name ?? null) : null,
        delete_reason: isMod ? (reasonLabel(row.delete_reason) ?? row.delete_reason ?? null) : null,
        created_at: isoOrNull(row.created_at),
        can_delete: !!uid && (row.author_id === uid || isMod),
      }));
    return c.json({ comments, count: comments.filter((x) => !x.deleted).length });
  } catch (err) {
    log.error({ err }, 'wire: list comments failed');
    return c.json({ error: 'failed to load comments' }, 500);
  }
});

wireCommentsRouter.post('/api/wire/comments', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = currentUserId(c)!;
  try {
    const d = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const parentType = parentTypeOf(d['type']);
    const parentId = typeof d['id'] === 'string' ? d['id'].trim() : '';
    const body = typeof d['body'] === 'string' ? d['body'].trim().slice(0, MAX_COMMENT_LEN) : '';
    if (!parentType || !parentId) return c.json({ error: 'type and id are required' }, 400);
    if (!body) return c.json({ error: 'comment cannot be empty' }, 400);

    // Restriction gate — tell the user WHY and (for a timeout) until when.
    const restriction = await activeRestriction(pool, uid);
    if (restriction) {
      return c.json(
        {
          error: restriction.kind === 'pause'
            ? 'Your commenting has been paused.'
            : 'You are timed out from commenting.',
          restriction,
        },
        403,
      );
    }

    const parent = await loadParent(pool, parentType, parentId);
    if (!parent) return c.json({ error: 'post not found' }, 404);

    const ins = await pool.query<{ id: string; created_at: Date }>(
      `INSERT INTO wire_comments (parent_type, parent_id, author_id, author_name, body)
       VALUES ($1,$2,$3,$4,$5) RETURNING id, created_at`,
      [parentType, parentId, uid, currentUserName(c), body],
    );
    const row = ins.rows[0]!;

    // Notifications (best-effort, never block the write):
    //  - the post's author + credited co-authors
    //  - anyone else who has commented on this post ("activity on a thread
    //    you're in" — the flat-list equivalent of a reply notification)
    const link = postLink(parentType, parent, parentId);
    const coIds = Array.isArray(parent.co_authors)
      ? (parent.co_authors as { id?: string }[]).map((x) => x?.id).filter((x): x is string => !!x)
      : [];
    const owners = [parent.author_id, ...coIds];
    await notifyMany(pool, owners, {
      type: 'wire.comment',
      title: `New comment on "${parent.title}"`,
      body: body.slice(0, 140),
      link,
      meta: { parent_type: parentType, parent_id: parentId, comment_id: row.id },
      exceptUserId: uid,
    });
    const others = (await commenterIds(pool, parentType, parentId)).filter(
      (id) => id !== uid && !owners.includes(id),
    );
    await notifyMany(pool, others, {
      type: 'wire.comment',
      title: `New activity on "${parent.title}"`,
      body: 'Someone else commented on a post you commented on.',
      link,
      meta: { parent_type: parentType, parent_id: parentId, comment_id: row.id },
      exceptUserId: uid,
    });

    return c.json({
      success: true,
      comment: {
        id: row.id,
        author: { id: uid, name: currentUserName(c), avatar_url: (await avatarMap(pool, [uid])).get(uid) ?? null },
        body,
        deleted: false,
        created_at: isoOrNull(row.created_at),
        can_delete: true,
      },
    }, 201);
  } catch (err) {
    log.error({ err, uid }, 'wire: create comment failed');
    return c.json({ error: 'failed to post comment' }, 500);
  }
});

wireCommentsRouter.delete('/api/wire/comments/:id', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = currentUserId(c)!;
  const id = c.req.param('id');
  try {
    const d = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const cur = await pool.query<{ author_id: string; parent_type: string; parent_id: string; deleted_at: unknown }>(
      'SELECT author_id, parent_type, parent_id, deleted_at FROM wire_comments WHERE id = $1',
      [id],
    );
    const row = cur.rows[0];
    if (!row) return c.json({ error: 'not found' }, 404);
    const isMod = await canModerateWire(uid);
    const isAuthor = row.author_id === uid;
    if (!isAuthor && !isMod) return c.json({ error: 'forbidden' }, 403);
    if (row.deleted_at) return c.json({ success: true, already: true });

    // A moderator must cite a reason; an author deleting their own comment
    // doesn't need one.
    const reasonKey = typeof d['reason'] === 'string' ? d['reason'] : null;
    const label = reasonLabel(reasonKey);
    if (isMod && !isAuthor && reasonKey && !label) {
      return c.json({ error: 'unknown reason' }, 400);
    }

    await pool.query(
      `UPDATE wire_comments
          SET deleted_at = now(), deleted_by = $1, deleted_by_name = $2, delete_reason = $3, updated_at = now()
        WHERE id = $4`,
      [uid, currentUserName(c), isAuthor && !isMod ? 'author' : (reasonKey ?? 'other'), id],
    );

    // Tell the author when someone ELSE removed their comment.
    if (!isAuthor) {
      await notify(pool, {
        userId: row.author_id,
        type: 'wire.moderation',
        title: 'Your comment was removed',
        body: label ? `Reason: ${label}` : 'A moderator removed your comment.',
        link: '/wire',
        meta: { action: 'comment_deleted', reason: reasonKey ?? 'other' },
        exceptUserId: uid,
      });
    }
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'wire: delete comment failed');
    return c.json({ error: 'failed to delete comment' }, 500);
  }
});

// ---------------------------------------------------------------------------
// Likes (posts only) — toggle
// ---------------------------------------------------------------------------

wireCommentsRouter.post('/api/wire/like', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = currentUserId(c)!;
  try {
    const d = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const parentType = parentTypeOf(d['type']);
    const parentId = typeof d['id'] === 'string' ? d['id'].trim() : '';
    if (!parentType || !parentId) return c.json({ error: 'type and id are required' }, 400);

    const parent = await loadParent(pool, parentType, parentId);
    if (!parent) return c.json({ error: 'post not found' }, 404);

    // Toggle: try to insert; if the row already existed, remove it instead.
    const ins = await pool.query(
      `INSERT INTO wire_likes (parent_type, parent_id, user_id)
       VALUES ($1,$2,$3) ON CONFLICT DO NOTHING RETURNING user_id`,
      [parentType, parentId, uid],
    );
    const liked = (ins.rowCount ?? 0) > 0;
    if (!liked) {
      await pool.query(
        'DELETE FROM wire_likes WHERE parent_type=$1 AND parent_id=$2 AND user_id=$3',
        [parentType, parentId, uid],
      );
    }
    const cnt = await pool.query<{ n: string }>(
      'SELECT COUNT(*)::int AS n FROM wire_likes WHERE parent_type=$1 AND parent_id=$2',
      [parentType, parentId],
    );

    if (liked) {
      await notifyLikeBatched(pool, {
        ownerId: parent.author_id,
        actorId: uid,
        parentType,
        parentId,
        title: parent.title,
        link: postLink(parentType, parent, parentId),
      });
    }
    return c.json({ liked, count: Number(cnt.rows[0]?.n ?? 0) });
  } catch (err) {
    log.error({ err, uid }, 'wire: like toggle failed');
    return c.json({ error: 'failed to update like' }, 500);
  }
});

// ---------------------------------------------------------------------------
// Comment restrictions (wire:manager)
// ---------------------------------------------------------------------------

/** The reason list + timeout durations, so the UI never hardcodes them. */
wireCommentsRouter.get('/api/wire/comment-restrictions/options', requireRole(canModerateWire), (c) =>
  c.json({
    reasons: Object.entries(RESTRICTION_REASONS).map(([key, label]) => ({ key, label })),
    timeoutHours: TIMEOUT_HOURS,
  }),
);

wireCommentsRouter.get('/api/wire/comment-restrictions', requireRole(canModerateWire), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  try {
    const r = await pool.query(
      `SELECT id, user_id, kind, reason, note, expires_at, created_by_name, created_at
         FROM comment_restrictions
        WHERE lifted_at IS NULL AND (expires_at IS NULL OR expires_at > now())
        ORDER BY created_at DESC LIMIT 200`,
    );
    return c.json({
      restrictions: r.rows.map((row) => ({
        id: Number(row.id),
        userId: row.user_id,
        kind: row.kind,
        reason: row.reason,
        reasonLabel: reasonLabel(row.reason) ?? row.reason,
        note: row.note,
        expiresAt: isoOrNull(row.expires_at),
        createdByName: row.created_by_name,
        createdAt: isoOrNull(row.created_at),
      })),
      count: r.rowCount ?? 0,
    });
  } catch (err) {
    log.error({ err }, 'wire: list restrictions failed');
    return c.json({ error: 'failed to load restrictions' }, 500);
  }
});

wireCommentsRouter.post('/api/wire/comment-restrictions', requireRole(canModerateWire), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const actor = currentUserId(c)!;
  try {
    const d = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const userId = typeof d['userId'] === 'string' ? d['userId'].trim() : '';
    const kind = d['kind'] === 'pause' ? 'pause' : 'timeout';
    const reasonKey = typeof d['reason'] === 'string' ? d['reason'] : '';
    const label = reasonLabel(reasonKey);
    const note = typeof d['note'] === 'string' ? d['note'].trim().slice(0, 500) || null : null;
    if (!userId) return c.json({ error: 'userId is required' }, 400);
    if (!label) return c.json({ error: 'a valid reason is required' }, 400);
    if (userId === actor) return c.json({ error: 'you cannot restrict yourself' }, 400);

    // A timeout needs a duration from the offered set; a pause is indefinite.
    let expiresAt: string | null = null;
    if (kind === 'timeout') {
      const hours = Number(d['hours']);
      if (!TIMEOUT_HOURS.includes(hours)) {
        return c.json({ error: `hours must be one of ${TIMEOUT_HOURS.join(', ')}` }, 400);
      }
      expiresAt = new Date(Date.now() + hours * 3_600_000).toISOString();
    }

    const ins = await pool.query<{ id: string }>(
      `INSERT INTO comment_restrictions (user_id, kind, reason, note, expires_at, created_by, created_by_name)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
      [userId, kind, reasonKey, note, expiresAt, actor, currentUserName(c)],
    );

    // The user must know they've been restricted, and why.
    await notify(pool, {
      userId,
      type: 'wire.moderation',
      title: kind === 'pause' ? 'Your commenting has been paused' : 'You have been timed out from commenting',
      body: expiresAt
        ? `Reason: ${label}. You can comment again after ${new Date(expiresAt).toLocaleString('en-AU')}.`
        : `Reason: ${label}. Commenting is paused until a moderator lifts it.`,
      link: '/wire',
      meta: { action: kind, reason: reasonKey, expires_at: expiresAt },
    });

    return c.json({ success: true, id: Number(ins.rows[0]?.id), kind, expiresAt }, 201);
  } catch (err) {
    log.error({ err }, 'wire: create restriction failed');
    return c.json({ error: 'failed to apply restriction' }, 500);
  }
});

wireCommentsRouter.delete('/api/wire/comment-restrictions/:id', requireRole(canModerateWire), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const id = c.req.param('id');
  try {
    const r = await pool.query<{ user_id: string }>(
      `UPDATE comment_restrictions
          SET lifted_at = now(), lifted_by = $1, lifted_by_name = $2
        WHERE id = $3 AND lifted_at IS NULL
        RETURNING user_id`,
      [currentUserId(c) ?? null, currentUserName(c), id],
    );
    if (r.rowCount === 0) return c.json({ error: 'not found or already lifted' }, 404);
    await notify(pool, {
      userId: r.rows[0]!.user_id,
      type: 'wire.moderation',
      title: 'You can comment again',
      body: 'A moderator has lifted your commenting restriction.',
      link: '/wire',
      meta: { action: 'lifted' },
    });
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, id }, 'wire: lift restriction failed');
    return c.json({ error: 'failed to lift restriction' }, 500);
  }
});

/** Whether the CALLER may currently comment — drives the composer's state. */
wireCommentsRouter.get('/api/wire/my-comment-status', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = currentUserId(c)!;
  const restriction = await activeRestriction(pool, uid);
  return c.json({ canComment: !restriction, restriction });
});

// ---------------------------------------------------------------------------
// Notifications (own only)
// ---------------------------------------------------------------------------

wireCommentsRouter.get('/api/notifications', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = currentUserId(c)!;
  const url = new URL(c.req.url);
  const limit = Math.max(1, Math.min(50, Number(url.searchParams.get('limit') ?? 20) || 20));
  try {
    const [list, unread] = await Promise.all([
      pool.query(
        `SELECT id, type, title, body, link, meta, read_at, created_at
           FROM notifications WHERE user_id = $1
          ORDER BY created_at DESC LIMIT $2`,
        [uid, limit],
      ),
      pool.query<{ n: string }>(
        'SELECT COUNT(*)::int AS n FROM notifications WHERE user_id = $1 AND read_at IS NULL',
        [uid],
      ),
    ]);
    return c.json({
      unreadCount: Number(unread.rows[0]?.n ?? 0),
      notifications: list.rows.map((row) => ({
        id: Number(row.id),
        type: row.type,
        title: row.title,
        body: row.body,
        link: row.link,
        meta: row.meta ?? {},
        read: !!row.read_at,
        created_at: isoOrNull(row.created_at),
      })),
    });
  } catch (err) {
    log.error({ err, uid }, 'notifications: list failed');
    return c.json({ error: 'failed to load notifications' }, 500);
  }
});

// Clear the caller's notification list outright. Distinct from marking read:
// "read" keeps the history and only quiets the badge, this empties the list.
// Always scoped by user_id, so there is no way to clear anyone else's.
wireCommentsRouter.delete('/api/notifications', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = currentUserId(c)!;
  try {
    const r = await pool.query('DELETE FROM notifications WHERE user_id = $1', [uid]);
    return c.json({ success: true, cleared: r.rowCount ?? 0 });
  } catch (err) {
    log.error({ err, uid }, 'notifications: clear failed');
    return c.json({ error: 'failed to clear notifications' }, 500);
  }
});

wireCommentsRouter.post('/api/notifications/read', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = currentUserId(c)!;
  try {
    const d = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const ids = Array.isArray(d['ids'])
      ? (d['ids'] as unknown[]).map((x) => Number(x)).filter((n) => Number.isFinite(n))
      : null;
    // Always scoped by user_id, so a caller can't mark someone else's read.
    if (ids && ids.length > 0) {
      await pool.query(
        'UPDATE notifications SET read_at = now() WHERE user_id = $1 AND id = ANY($2::bigint[]) AND read_at IS NULL',
        [uid, ids],
      );
    } else {
      await pool.query('UPDATE notifications SET read_at = now() WHERE user_id = $1 AND read_at IS NULL', [uid]);
    }
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, uid }, 'notifications: mark read failed');
    return c.json({ error: 'failed to mark read' }, 500);
  }
});
