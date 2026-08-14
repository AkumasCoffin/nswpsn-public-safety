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
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { requireSupabaseJwt } from '../services/auth/supabaseJwt.js';
import { invalidateUserRolesCache } from '../services/auth/roles.js';
import { avatarUrl, createImageUploadUrl, r2Configured } from '../services/wire.js';

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
    twitter: row?.twitter ?? null,
    facebook: row?.facebook ?? null,
    instagram: row?.instagram ?? null,
    youtube: row?.youtube ?? null,
    website: row?.website ?? null,
  };
}

profilesRouter.get('/api/profiles/:userId', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json(DB_UNAVAILABLE, 503);
  const userId = c.req.param('userId');
  try {
    const r = await pool.query<ProfileRow>('SELECT * FROM user_profiles WHERE user_id = $1', [userId]);
    return c.json({ profile: shapeProfile(userId, r.rows[0]) });
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
         avatar_key   = COALESCE($4, user_profiles.avatar_key),
         discord_avatar_url = COALESCE($5, user_profiles.discord_avatar_url),
         twitter = $6, facebook = $7, instagram = $8, youtube = $9, website = $10, updated_at = now()
       RETURNING *`,
      [uid, displayName, bio, avatarKey, discordAvatar, twitter, facebook, instagram, youtube, website],
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
