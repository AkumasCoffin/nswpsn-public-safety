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
import { createImageUploadUrl, r2Configured, r2PublicUrl } from '../services/wire.js';

export const profilesRouter = new Hono();

const DB_UNAVAILABLE = { error: 'database unavailable' } as const;

/** Normalise a social/website value to an http(s) URL, or null. */
function normUrl(v: unknown): string | null {
  if (typeof v !== 'string') return null;
  const s = v.trim().slice(0, 300);
  if (!s) return null;
  const url = /^https?:\/\//i.test(s) ? s : `https://${s}`;
  return /^https?:\/\/[^\s.]+\.[^\s]+$/i.test(url) ? url : null;
}

interface ProfileRow {
  user_id: string;
  display_name: string | null;
  avatar_key: string | null;
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
    avatar_url: row?.avatar_key ? r2PublicUrl(row.avatar_key) : null,
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
    const twitter = normUrl(d['twitter']);
    const facebook = normUrl(d['facebook']);
    const instagram = normUrl(d['instagram']);
    const youtube = normUrl(d['youtube']);
    const website = normUrl(d['website']);
    // avatar_key is only written when provided (COALESCE keeps the existing one).
    const avatarKey = typeof d['avatar_key'] === 'string' && d['avatar_key'] ? d['avatar_key'].slice(0, 200) : null;
    const r = await pool.query<ProfileRow>(
      `INSERT INTO user_profiles (user_id, display_name, avatar_key, twitter, facebook, instagram, youtube, website, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now())
       ON CONFLICT (user_id) DO UPDATE SET
         display_name = $2,
         avatar_key   = COALESCE($3, user_profiles.avatar_key),
         twitter = $4, facebook = $5, instagram = $6, youtube = $7, website = $8, updated_at = now()
       RETURNING *`,
      [uid, displayName, avatarKey, twitter, facebook, instagram, youtube, website],
    );
    return c.json({ success: true, profile: shapeProfile(uid, r.rows[0]) });
  } catch (err) {
    log.error({ err, uid }, 'profiles: update failed');
    return c.json({ error: 'failed to save profile' }, 500);
  }
});

profilesRouter.post('/api/profiles/avatar-url', requireSupabaseJwt, async (c) => {
  if (!r2Configured()) return c.json({ error: 'avatar uploads not configured' }, 503);
  const up = await createImageUploadUrl('wire/avatars');
  if (!up) return c.json({ error: 'could not create upload url' }, 503);
  return c.json({ uploadURL: up.uploadURL, key: up.key, publicUrl: up.publicUrl });
});
