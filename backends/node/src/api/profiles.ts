/**
 * Public contributor profiles — a custom avatar (overrides Discord) + social
 * links, shown on Wire posts. Stored in Postgres (user_profiles) so any viewer
 * can read another user's public profile; the avatar image lives in R2.
 *
 *   GET  /api/profiles/:userId        — public read (key-gated like other reads)
 *   PUT  /api/profiles                — the caller upserts their OWN profile
 *   POST /api/profiles/avatar-url     — R2 presigned PUT for the caller's pfp
 */
import { Hono } from 'hono';
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { requireSupabaseJwt } from '../services/auth/supabaseJwt.js';
import { invalidateUserRolesCache } from '../services/auth/roles.js';
import { avatarUrl, createImageUploadUrl, r2Configured, readR2ObjectBytes, deleteR2Object } from '../services/wire.js';
import { tagsFor } from '../services/userTags.js';

export const profilesRouter = new Hono();

const DB_UNAVAILABLE = { error: 'database unavailable' } as const;

/** Normalise a social/website value to an http(s) URL, or null. */
function normUrl(v: unknown): string | null {
  if (typeof v !== 'string') return null;
  const s = v.trim().slice(0, 300);
  if (!s) return null;
  const url = /^https?:\/\//i.test(s) ? s : `https://${s}`;
  // Reject anything that could break out of an href/attribute if the frontend
  // ever mis-escapes it (defence in depth on top of frontend escaping).
  if (/["'<>`\\]/.test(url)) return null;
  return /^https?:\/\/[^\s.]+\.[^\s]+$/i.test(url) ? url : null;
}

interface ProfileRow {
  user_id: string;
  display_name: string | null;
  bio: string | null;
  avatar_key: string | null;
  discord_avatar_url: string | null;
  twitter: string | null;
  facebook: string | null;
  instagram: string | null;
  youtube: string | null;
  website: string | null;
}

function shapeProfile(userId: string, row?: ProfileRow): Record<string, unknown> {
  return {
    user_id: userId,
    display_name: row?.display_name ?? null,
    bio: row?.bio ?? null,
    // Custom pfp wins; otherwise fall back to the stored Discord avatar so the
    // picture shows to other viewers (who can't read the user's Supabase metadata).
    avatar_url: avatarUrl(row?.avatar_key, row?.discord_avatar_url, 'large'),
    has_custom_avatar: !!row?.avatar_key,
    twitter: row?.twitter ?? null,
    facebook: row?.facebook ?? null,
    instagram: row?.instagram ?? null,
    youtube: row?.youtube ?? null,
    website: row?.website ?? null,
  };
}

/**
 * Lifetime reach for a contributor: how many posts they have on The Wire, and
 * the total views and likes those posts have drawn.
 *
 * Counts PUBLISHED, non-taken-down work only — on both the public and the
 * private profile. A private view that also counted drafts would show a bigger
 * number than anyone else can see, which makes the figure mean two different
 * things depending on who is looking.
 *
 * Likes come from wire_likes rather than a denormalised column, so unliking
 * is reflected; views are the counters already maintained on each post.
 * Failure returns zeros rather than throwing — a profile still renders.
 */
async function authorStats(pool: Pool, userId: string): Promise<{ posts: number; views: number; likes: number }> {
  const empty = { posts: 0, views: 0, likes: 0 };
  try {
    const r = await pool.query<{ posts: string; views: string; likes: string }>(
      `WITH mine AS (
         SELECT 'article'::text AS t, id, COALESCE(views, 0) AS views FROM articles
           WHERE author_id = $1 AND status = 'published' AND taken_down_at IS NULL
       )
       SELECT COUNT(*)::int AS posts,
              COALESCE(SUM(mine.views), 0)::bigint AS views,
              (SELECT COUNT(*)::int FROM wire_likes l
                 JOIN mine m ON m.t = l.parent_type AND m.id = l.parent_id) AS likes
         FROM mine`,
    [userId]);
    const row = r.rows[0];
    if (!row) return empty;
    return { posts: Number(row.posts) || 0, views: Number(row.views) || 0, likes: Number(row.likes) || 0 };
  } catch (err) {
    log.warn({ err, userId }, 'profiles: author stats failed');
    return empty;
  }
}

profilesRouter.get('/api/profiles/:userId', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const userId = c.req.param('userId');
  try {
    const [r, stats, tags] = await Promise.all([
      pool.query<ProfileRow>('SELECT * FROM user_profiles WHERE user_id = $1', [userId]),
      authorStats(pool, userId),
      tagsFor(pool, userId),
    ]);
    return c.json({ profile: shapeProfile(userId, r.rows[0]), stats, tags });
  } catch (err) {
    log.error({ err, userId }, 'profiles: get failed');
    return c.json({ error: 'failed to load profile' }, 500);
  }
});

profilesRouter.put('/api/profiles', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = c.get('userId') as string;
  try {
    const d = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const displayName = typeof d['display_name'] === 'string' ? d['display_name'].trim().slice(0, 60) || null : null;
    // Short public bio. Plain text, capped; rendered escaped by the frontend.
    const bio = typeof d['bio'] === 'string' ? d['bio'].trim().slice(0, 500) || null : null;
    const twitter = normUrl(d['twitter']);
    const facebook = normUrl(d['facebook']);
    const instagram = normUrl(d['instagram']);
    const youtube = normUrl(d['youtube']);
    const website = normUrl(d['website']);
    // avatar_key is only written when provided (COALESCE keeps the existing one).
    // Must be a key we minted (under wire/avatars/) — never an arbitrary object.
    let avatarKey = typeof d['avatar_key'] === 'string' && d['avatar_key'] ? d['avatar_key'].slice(0, 200) : null;
    if (avatarKey && !avatarKey.startsWith('wire/avatars/')) avatarKey = null;
    // clear_avatar removes the custom picture (COALESCE alone can't: null
    // means "keep"). The old R2 object is deleted best-effort.
    const clearAvatar = d['clear_avatar'] === true;
    if (clearAvatar) {
      avatarKey = null;
      try {
        const prevKey = await pool.query<{ avatar_key: string | null }>(
          'SELECT avatar_key FROM user_profiles WHERE user_id = $1', [uid]);
        const old = prevKey.rows[0]?.avatar_key;
        if (old) deleteR2Object(old).catch(() => {});
      } catch { /* the clear still proceeds */ }
    }
    // Discord avatar: prefer the verified JWT claim; fall back to a client-sent
    // value. Stored as a public fallback pfp. COALESCE keeps any existing one.
    const jwtAvatar = c.get('userAvatar');
    const discordAvatar = normUrl(jwtAvatar) ?? normUrl(d['discord_avatar_url']);
    const r = await pool.query<ProfileRow>(
      `INSERT INTO user_profiles (user_id, display_name, bio, avatar_key, discord_avatar_url, twitter, facebook, instagram, youtube, website, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
       ON CONFLICT (user_id) DO UPDATE SET
         display_name = $2,
         bio          = $3,
         avatar_key   = CASE WHEN $11::boolean THEN NULL ELSE COALESCE($4, user_profiles.avatar_key) END,
         discord_avatar_url = COALESCE($5, user_profiles.discord_avatar_url),
         twitter = $6, facebook = $7, instagram = $8, youtube = $9, website = $10, updated_at = now()
       RETURNING *`,
      [uid, displayName, bio, avatarKey, discordAvatar, twitter, facebook, instagram, youtube, website, clearAvatar],
    );
    return c.json({ success: true, profile: shapeProfile(uid, r.rows[0]) });
  } catch (err) {
    log.error({ err, uid }, 'profiles: update failed');
    return c.json({ error: 'failed to save profile' }, 500);
  }
});

// Capture the caller's Discord avatar + display name into their public profile
// without touching any other field. Called on load so a contributor's picture
// AND name show to others (and so they're findable in the co-author search)
// even if they never open the profile editor. Both come from the verified JWT;
// the display name only fills in when currently empty, so a user's own chosen
// name is never overwritten.
profilesRouter.post('/api/profiles/sync', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = c.get('userId') as string;
  const discordAvatar = normUrl(c.get('userAvatar'));
  const jwtName = (c.get('userName') as string | undefined)?.trim().slice(0, 60) || null;
  try {
    // Base role: every authenticated account carries 'authed' (see migration
    // 059). This runs on every logged-in page load, so it's how existing and
    // brand-new accounts alike acquire it. It's also what separates the staff
    // "Users" tab (authed only) from "Members" (authed + a real role).
    const granted = await pool.query(
      `INSERT INTO user_roles (user_id, role, granted_by)
       VALUES ($1, 'authed', 'sync')
       ON CONFLICT (user_id, role) DO NOTHING`,
      [uid],
    );
    if ((granted.rowCount ?? 0) > 0) invalidateUserRolesCache(uid);

    if (!discordAvatar && !jwtName) return c.json({ success: true, skipped: 'nothing to sync' });
    await pool.query(
      `INSERT INTO user_profiles (user_id, discord_avatar_url, display_name, updated_at)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (user_id) DO UPDATE SET
         discord_avatar_url = COALESCE($2, user_profiles.discord_avatar_url),
         display_name = COALESCE(user_profiles.display_name, $3),
         updated_at = now()`,
      [uid, discordAvatar, jwtName],
    );
    return c.json({ success: true });
  } catch (err) {
    log.error({ err, uid }, 'profiles: sync failed');
    return c.json({ error: 'failed to sync profile' }, 500);
  }
});

profilesRouter.post('/api/profiles/avatar-url', requireSupabaseJwt, async (c) => {
  if (!r2Configured()) return c.json({ error: 'avatar uploads not configured' }, 503);
  const up = await createImageUploadUrl('wire/avatars');
  if (!up) return c.json({ error: 'could not create upload url' }, 503);
  return c.json({ uploadURL: up.uploadURL, key: up.key, publicUrl: up.publicUrl });
});

// ---- custom media watermark (transparent PNG, stamped onto photos) ---------

profilesRouter.post('/api/profiles/watermark-url', requireSupabaseJwt, async (c) => {
  if (!r2Configured()) return c.json({ error: 'watermark uploads not configured' }, 503);
  const up = await createImageUploadUrl('wire/watermarks', 'png');
  if (!up) return c.json({ error: 'could not create upload url' }, 503);
  return c.json({ uploadURL: up.uploadURL, key: up.key });
});

/** Set (or clear, with key:null) the caller's watermark. The old object is
 *  deleted so replaced watermarks don't accrue in R2. */
profilesRouter.put('/api/profiles/watermark', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = c.get('userId') as string;
  try {
    const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    let key = typeof body['key'] === 'string' ? body['key'].slice(0, 300) : null;
    // Only keys we minted -- never an arbitrary object path.
    if (key && !key.startsWith('wire/watermarks/')) key = null;
    const prev = await pool.query<{ watermark_key: string | null }>(
      'SELECT watermark_key FROM user_profiles WHERE user_id = $1', [uid]);
    await pool.query(
      `INSERT INTO user_profiles (user_id, watermark_key, updated_at)
       VALUES ($1, $2, now())
       ON CONFLICT (user_id) DO UPDATE SET watermark_key = $2, updated_at = now()`,
      [uid, key],
    );
    const old = prev.rows[0]?.watermark_key ?? null;
    if (old && old !== key) deleteR2Object(old).catch(() => {});
    return c.json({ success: true, has_watermark: !!key });
  } catch (err) {
    log.error({ err, uid }, 'profiles: watermark save failed');
    return c.json({ error: 'failed to save watermark' }, 500);
  }
});

/** The caller's OWN watermark bytes. Served through the API (not the R2
 *  public host) so the compose pages can draw it onto a canvas without
 *  cross-origin taint. 404 when none is set. */
profilesRouter.get('/api/profiles/watermark', requireSupabaseJwt, async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const uid = c.get('userId') as string;
  try {
    const r = await pool.query<{ watermark_key: string | null }>(
      'SELECT watermark_key FROM user_profiles WHERE user_id = $1', [uid]);
    const key = r.rows[0]?.watermark_key ?? null;
    if (!key) return c.json({ error: 'no watermark' }, 404);
    const bytes = await readR2ObjectBytes(key);
    if (!bytes) return c.json({ error: 'no watermark' }, 404);
    return c.body(new Uint8Array(bytes), 200, {
      'Content-Type': 'image/png',
      'Cache-Control': 'private, max-age=60',
    });
  } catch (err) {
    log.error({ err, uid }, 'profiles: watermark read failed');
    return c.json({ error: 'failed to read watermark' }, 500);
  }
});
