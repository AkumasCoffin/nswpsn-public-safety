/**
 * Profile tags (badges).
 *
 * Decoration only — these grant NOTHING. Permissions live in user_roles and
 * are checked per request; a tag is a label shown next to a name. Keeping the
 * two apart means a mis-typed badge can never widen someone's access.
 *
 * Two are awarded automatically when someone posts (see awardPostingTags);
 * all of them can also be granted or revoked by hand from the staff panel.
 */
import type { Pool } from 'pg';
import { config } from '../config.js';
import { log } from '../lib/log.js';

export interface TagDef {
  key: string;
  label: string;
  /** Font Awesome class — the site uses FA throughout, not emoji. */
  icon: string;
  color: string;
  description: string;
  /** True when the system grants it; staff can still add/remove by hand. */
  auto?: boolean;
}

/**
 * The whole vocabulary. Adding a badge is one entry here plus a deploy —
 * deliberately not a DB enum or a CHECK constraint, so this stays a code
 * change rather than a migration.
 */
export const USER_TAGS: readonly TagDef[] = [
  {
    key: 'og_contributor',
    label: 'OG Contributor',
    icon: 'fa-solid fa-star',
    color: '#f59e0b',
    description: 'Posted to The Wire before it went public.',
    auto: true,
  },
  {
    key: 'first_contributor',
    label: 'First Contributor',
    icon: 'fa-solid fa-medal',
    color: '#eab308',
    description: 'Made the very first post on The Wire.',
    auto: true,
  },
  {
    key: 'verified',
    label: 'Verified',
    icon: 'fa-solid fa-circle-check',
    color: '#38bdf8',
    description: 'Identity confirmed by staff.',
  },
] as const;

const BY_KEY = new Map(USER_TAGS.map((t) => [t.key, t]));

export function isKnownTag(tag: unknown): tag is string {
  return typeof tag === 'string' && BY_KEY.has(tag);
}

export function tagDef(key: string): TagDef | undefined {
  return BY_KEY.get(key);
}

/** Shape sent to clients: the key plus everything needed to render it. */
export function shapeTag(key: string): Record<string, unknown> | null {
  const d = BY_KEY.get(key);
  if (!d) return null;
  return { key: d.key, label: d.label, icon: d.icon, color: d.color, description: d.description };
}

/**
 * Tags for a set of users, as user_id -> shaped tags. Bulk because the feed
 * renders a byline chip per post and per comment; one query per author would
 * be a query storm on a busy page.
 *
 * Never throws — a badge failing to load must not take a post with it.
 */
export async function tagMap(
  pool: Pool,
  userIds: Array<string | null | undefined>,
): Promise<Map<string, Array<Record<string, unknown>>>> {
  const map = new Map<string, Array<Record<string, unknown>>>();
  const ids = [...new Set(userIds.filter((x): x is string => !!x))];
  if (ids.length === 0) return map;
  try {
    const r = await pool.query<{ user_id: string; tag: string }>(
      'SELECT user_id, tag FROM user_tags WHERE user_id = ANY($1::text[])',
      [ids],
    );
    for (const row of r.rows) {
      const shaped = shapeTag(row.tag);
      if (!shaped) continue;   // vocabulary shrank; ignore the orphan
      const list = map.get(row.user_id) ?? [];
      list.push(shaped);
      map.set(row.user_id, list);
    }
    // Stable order so a byline doesn't reshuffle between renders.
    for (const list of map.values()) {
      list.sort((a, b) =>
        USER_TAGS.findIndex((t) => t.key === a['key']) - USER_TAGS.findIndex((t) => t.key === b['key']));
    }
  } catch (err) {
    log.warn({ err }, 'userTags: bulk fetch failed');
  }
  return map;
}

/** Tags for one user. */
export async function tagsFor(pool: Pool, userId: string): Promise<Array<Record<string, unknown>>> {
  return (await tagMap(pool, [userId])).get(userId) ?? [];
}

/** Grant a tag. Idempotent. Returns false if the tag is already taken by
 *  someone else (first_contributor is single-holder by DB index). */
export async function grantTag(
  pool: Pool,
  userId: string,
  tag: string,
  grantedBy: string | null,
): Promise<{ ok: true } | { ok: false; reason: string }> {
  if (!isKnownTag(tag)) return { ok: false, reason: 'unknown tag' };
  try {
    await pool.query(
      `INSERT INTO user_tags (user_id, tag, granted_by) VALUES ($1,$2,$3)
       ON CONFLICT (user_id, tag) DO NOTHING`,
      [userId, tag, grantedBy],
    );
    return { ok: true };
  } catch (err) {
    // The partial unique index on first_contributor. Report it plainly rather
    // than as a 500 — "someone else already has it" is a normal answer.
    const code = (err as { code?: string }).code;
    if (code === '23505') {
      return { ok: false, reason: `${tagDef(tag)?.label ?? tag} is already held by another user — remove it there first` };
    }
    log.error({ err, userId, tag }, 'userTags: grant failed');
    return { ok: false, reason: 'could not grant tag' };
  }
}

export async function revokeTag(pool: Pool, userId: string, tag: string): Promise<void> {
  await pool.query('DELETE FROM user_tags WHERE user_id = $1 AND tag = $2', [userId, tag]);
}

/**
 * Award the automatic badges to someone who has just published.
 *
 * - og_contributor  — while the Wire is still private. The whole point is that
 *   it can't be earned once the Wire is public, so it's gated on WIRE_PUBLIC
 *   at the moment of posting, not on any backfill.
 * - first_contributor — to whoever gets there first. The DB index guarantees a
 *   single holder, so this can be attempted unconditionally: if someone
 *   already has it the insert is a harmless no-op.
 *
 * Best-effort throughout. A badge is not worth failing a publish over.
 */
export async function awardPostingTags(pool: Pool, userId: string): Promise<void> {
  if (!userId) return;
  try {
    if (config.WIRE_PUBLIC !== 'true') {
      await grantTag(pool, userId, 'og_contributor', 'system');
    }
    await grantTag(pool, userId, 'first_contributor', 'system');
  } catch (err) {
    log.warn({ err, userId }, 'userTags: auto-award failed');
  }
}
