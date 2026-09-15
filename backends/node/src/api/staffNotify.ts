/**
 * Owner-only configuration for the staff Discord notifications.
 *
 * The staff page picks a guild and one channel per notification kind;
 * everything is stored in `app_settings` so nothing lives in an env file
 * and the owner can repoint it without a deploy.
 *
 * Gated on `isOwner` rather than the wider staff roles: these channels
 * receive moderation traffic, and the bot dashboard's own authorisation
 * ("Manage Channels in some Discord server") is far too weak a gate to
 * decide where signup and takedown notices land.
 *
 * The guild/channel listings proxy Discord through the bot token the
 * dashboard already uses (`discordApi.botGet`), so the staff page never
 * needs a Discord login of its own.
 */
import { Hono } from 'hono';
import type { Context } from 'hono';

import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { botGet } from '../services/discordApi.js';
import { isBotDbConfigured } from '../services/botDb.js';
import { isOwner, requireRole } from '../services/auth/roles.js';
import { getBotActionSecret } from '../services/botActionSign.js';
import {
  NOTIFY_CHANNEL_KEYS,
  NOTIFY_GUILD_KEY,
  readNotifySettings,
  writeNotifySetting,
  type StaffNotifyKind,
} from '../services/staffNotify.js';

export const staffNotifyRouter = new Hono();

const KINDS = Object.keys(NOTIFY_CHANNEL_KEYS) as StaffNotifyKind[];

function currentUserId(c: { get: (k: string) => unknown }): string | undefined {
  const v = c.get('userId');
  return typeof v === 'string' && v ? v : undefined;
}

/** Snowflakes are 17-20 digits and exceed Number.MAX_SAFE_INTEGER — strings only. */
function cleanSnowflake(v: unknown): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s) return null;
  return /^\d{15,20}$/.test(s) ? s : null;
}

staffNotifyRouter.use('/api/staff/discord-notify', requireRole(isOwner));
staffNotifyRouter.use('/api/staff/discord-notify/*', requireRole(isOwner));

// ---------------------------------------------------------------------------
// GET — current settings + whether the delivery path is actually usable.
// ---------------------------------------------------------------------------
staffNotifyRouter.get('/api/staff/discord-notify', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);

  const s = await readNotifySettings(pool);
  const channels: Record<string, string | null> = {};
  for (const kind of KINDS) channels[kind] = s[NOTIFY_CHANNEL_KEYS[kind]] ?? null;

  return c.json({
    guild_id: s[NOTIFY_GUILD_KEY] ?? null,
    channels,
    // Surfaced so the UI can explain why nothing is arriving rather than
    // looking configured-but-silent.
    delivery: {
      bot_db: isBotDbConfigured(),
      signed: getBotActionSecret() !== null,
    },
  });
});

// ---------------------------------------------------------------------------
// PUT — set the guild and/or any channel. Null/'' clears one.
// ---------------------------------------------------------------------------
staffNotifyRouter.put('/api/staff/discord-notify', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);

  const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
  const uid = currentUserId(c) ?? null;

  if (Object.prototype.hasOwnProperty.call(body, 'guild_id')) {
    const raw = body['guild_id'];
    const gid = raw === null || raw === '' ? null : cleanSnowflake(raw);
    if (raw !== null && raw !== '' && !gid) {
      return c.json({ error: 'guild_id must be a Discord id' }, 400);
    }
    await writeNotifySetting(pool, NOTIFY_GUILD_KEY, gid, uid);
    // Channels belong to a guild; keeping them across a guild change would
    // point notifications at ids that don't exist in the new one.
    if (!gid) {
      for (const kind of KINDS) await writeNotifySetting(pool, NOTIFY_CHANNEL_KEYS[kind], null, uid);
    }
  }

  const channelsIn = body['channels'];
  if (channelsIn && typeof channelsIn === 'object' && !Array.isArray(channelsIn)) {
    for (const kind of KINDS) {
      const obj = channelsIn as Record<string, unknown>;
      if (!Object.prototype.hasOwnProperty.call(obj, kind)) continue;
      const raw = obj[kind];
      const cid = raw === null || raw === '' ? null : cleanSnowflake(raw);
      if (raw !== null && raw !== '' && !cid) {
        return c.json({ error: `channel for ${kind} must be a Discord id` }, 400);
      }
      await writeNotifySetting(pool, NOTIFY_CHANNEL_KEYS[kind], cid, uid);
    }
  }

  const s = await readNotifySettings(pool);
  const channels: Record<string, string | null> = {};
  for (const kind of KINDS) channels[kind] = s[NOTIFY_CHANNEL_KEYS[kind]] ?? null;
  log.info({ by: uid }, 'staff discord notification settings updated');
  return c.json({ guild_id: s[NOTIFY_GUILD_KEY] ?? null, channels, success: true });
});

// ---------------------------------------------------------------------------
// Discord listings, proxied through the bot token.
// ---------------------------------------------------------------------------
interface DiscordGuild { id: string; name?: string }
interface DiscordChannel { id: string; name?: string; type?: number; position?: number; parent_id?: string | null }

function discordErr(c: Context, status: number) {
  if (status === 401 || status === 403) {
    return c.json({ error: 'Discord rejected the bot token (is DISCORD_BOT_TOKEN set?)' }, 502);
  }
  if (status === 429) return c.json({ error: 'Discord rate limit; retry shortly' }, 503);
  return c.json({ error: `Discord responded ${status}` }, status >= 500 ? 502 : 503);
}

staffNotifyRouter.get('/api/staff/discord-notify/guilds', async (c) => {
  const res = await botGet<DiscordGuild[]>('/users/@me/guilds');
  if (res.status !== 200 || !Array.isArray(res.body)) return discordErr(c, res.status);
  const guilds = res.body
    .map((g) => ({ id: String(g.id), name: g.name ?? '' }))
    .sort((a, b) => a.name.localeCompare(b.name));
  return c.json(guilds);
});

staffNotifyRouter.get('/api/staff/discord-notify/channels', async (c) => {
  const guildId = cleanSnowflake(c.req.query('guild_id'));
  if (!guildId) return c.json({ error: 'guild_id required' }, 400);

  const res = await botGet<DiscordChannel[]>(`/guilds/${guildId}/channels`);
  if (res.status !== 200 || !Array.isArray(res.body)) return discordErr(c, res.status);
  // Types 0 (text) and 5 (announcement) are the ones a bot can post into,
  // matching the dashboard's own filter.
  const channels = res.body
    .filter((ch) => ch && (ch.type === 0 || ch.type === 5))
    .map((ch) => ({
      id: String(ch.id),
      name: ch.name ?? '',
      position: ch.position ?? 0,
      parent_id: ch.parent_id ? String(ch.parent_id) : null,
    }));
  channels.sort((a, b) => {
    const ap = a.parent_id ?? '';
    const bp = b.parent_id ?? '';
    if (ap !== bp) return ap < bp ? -1 : 1;
    return a.position - b.position;
  });
  return c.json(channels);
});
