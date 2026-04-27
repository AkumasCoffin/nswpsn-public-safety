/**
 * Bot-token Discord REST helper. Covers the dashboard's need to call
 * /guilds/:id/channels, /guilds/:id/roles, /guilds/:id, /users/@me/guilds
 * (the bot's own membership), and /applications/@me — none of which the
 * user's OAuth access_token has scope for.
 *
 * Mirrors python external_api_proxy.py:16688-16735, plus the inline
 * 429 retry pattern at lines 17129-17140 / 17166-17177. One retry on a
 * 429 (sleeps Retry-After up to 5 s, then a second attempt; 429 again
 * → caller surfaces 503).
 *
 * Bot token is read from BOT_TOKEN or DISCORD_BOT_TOKEN — python checks
 * both (line 16691). We keep that for env parity.
 */
import { log } from '../lib/log.js';
import { DISCORD_API_BASE } from './discordOauth.js';

const TIMEOUT_MS = 10_000;

export function getBotToken(): string {
  return process.env['DISCORD_BOT_TOKEN'] || process.env['BOT_TOKEN'] || '';
}

export interface BotApiResponse<T = unknown> {
  status: number;
  body: T | { error?: string; message?: string } | null;
  retryAfter: string | null;
}

/** Single GET against the Discord REST API with Bot auth. */
async function botGetOnce<T>(path: string): Promise<BotApiResponse<T>> {
  const token = getBotToken();
  if (!token) {
    log.warn({ path }, 'discord bot_api: BOT_TOKEN not set');
    return { status: 503, body: { error: 'no_bot_token' }, retryAfter: null };
  }
  let res: Response;
  try {
    res = await fetch(`${DISCORD_API_BASE}${path}`, {
      headers: {
        Authorization: `Bot ${token}`,
        'User-Agent': 'NSWPSN-Dashboard (https://nswpsn.forcequit.xyz)',
      },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
  } catch (err) {
    log.warn({ err, path }, 'discord bot_api network error');
    return {
      status: 502,
      body: { error: 'discord_unreachable', message: String((err as Error).message) },
      retryAfter: null,
    };
  }
  let body: unknown = null;
  try {
    body = await res.json();
  } catch {
    body = null;
  }
  if (res.status >= 400) {
    const msg =
      body && typeof body === 'object' && 'message' in body
        ? (body as Record<string, unknown>)['message']
        : String(body).slice(0, 120);
    log.warn({ status: res.status, path, msg }, 'discord bot_api error');
  }
  return {
    status: res.status,
    body: body as T,
    retryAfter: res.headers.get('Retry-After'),
  };
}

/**
 * GET with a single 429 retry. Mirrors the inline pattern at python
 * lines 17129-17139 / 17166-17176. Caller still surfaces 503 on a
 * second 429.
 */
export async function botGet<T>(path: string): Promise<BotApiResponse<T>> {
  const first = await botGetOnce<T>(path);
  if (first.status !== 429) return first;
  let waitMs = 1_000;
  if (first.retryAfter) {
    const n = Number(first.retryAfter);
    if (Number.isFinite(n) && n > 0) {
      waitMs = Math.min(5_000, n * 1000);
    }
  }
  await new Promise<void>((r) => setTimeout(r, waitMs));
  return botGetOnce<T>(path);
}

// ---------------------------------------------------------------------------
// CDN URL builders (python lines 16724-16735).
// ---------------------------------------------------------------------------
export function userAvatarUrl(userId: string | undefined, avatarHash: string | null | undefined): string | null {
  if (!userId || !avatarHash) return null;
  const ext = String(avatarHash).startsWith('a_') ? 'gif' : 'png';
  return `https://cdn.discordapp.com/avatars/${userId}/${avatarHash}.${ext}`;
}

export function guildIconUrl(guildId: string, iconHash: string | null | undefined): string | null {
  if (!iconHash) return null;
  const ext = String(iconHash).startsWith('a_') ? 'gif' : 'png';
  return `https://cdn.discordapp.com/icons/${guildId}/${iconHash}.${ext}`;
}

// ---------------------------------------------------------------------------
// Guild metadata cache. Mirrors python's `_dash_guild_meta_lookup`
// (external_api_proxy.py:18266-18370) — 10-minute TTL with eviction at
// 2× TTL so guilds the bot has long since left don't accumulate forever.
//
// Concurrency: Node's event loop serialises map writes, but we still bound
// in-flight fetches per gid so a burst of overview reloads doesn't fan out
// N parallel requests for the same guild.
// ---------------------------------------------------------------------------
export interface GuildMeta {
  name: string;
  icon_url: string | null;
  member_count: number | null;
}

const GUILD_META_TTL_MS = 10 * 60 * 1000;
const GUILD_META_EVICT_AFTER_MS = GUILD_META_TTL_MS * 2;

interface GuildMetaCacheEntry {
  ts: number;
  data: GuildMeta;
}

const guildMetaCache = new Map<string, GuildMetaCacheEntry>();
const guildMetaInflight = new Map<string, Promise<GuildMeta | null>>();

function evictStaleGuildMeta(now: number): void {
  const cutoff = now - GUILD_META_EVICT_AFTER_MS;
  for (const [k, v] of guildMetaCache) {
    if (v.ts < cutoff) guildMetaCache.delete(k);
  }
}

interface DiscordGuildBody {
  id?: string;
  name?: string;
  icon?: string | null;
  approximate_member_count?: number;
  member_count?: number;
}

/**
 * Fetch metadata for a single guild via the bot token. Returns null when
 * BOT_TOKEN isn't configured or Discord errored — admin UI shows blank
 * fields for those rows. Cached for 10 minutes.
 */
export async function getGuildMeta(guildId: string): Promise<GuildMeta | null> {
  const gid = String(guildId);
  if (!gid) return null;
  const now = Date.now();
  const cached = guildMetaCache.get(gid);
  if (cached && now - cached.ts < GUILD_META_TTL_MS) {
    return cached.data;
  }
  if (!getBotToken()) return null;

  const inflight = guildMetaInflight.get(gid);
  if (inflight) return inflight;

  const p = (async (): Promise<GuildMeta | null> => {
    try {
      const res = await botGet<DiscordGuildBody>(`/guilds/${gid}?with_counts=true`);
      if (res.status !== 200 || !res.body || typeof res.body !== 'object') {
        return null;
      }
      const body = res.body as DiscordGuildBody;
      const meta: GuildMeta = {
        name: body.name ?? '',
        icon_url: guildIconUrl(gid, body.icon),
        member_count:
          typeof body.approximate_member_count === 'number'
            ? body.approximate_member_count
            : typeof body.member_count === 'number'
              ? body.member_count
              : null,
      };
      const ts = Date.now();
      guildMetaCache.set(gid, { ts, data: meta });
      evictStaleGuildMeta(ts);
      return meta;
    } catch (err) {
      log.warn({ err, gid }, 'getGuildMeta failed');
      return null;
    } finally {
      guildMetaInflight.delete(gid);
    }
  })();
  guildMetaInflight.set(gid, p);
  return p;
}

/** Bulk variant — returns a map keyed by gid. Fetches missing entries in
 *  parallel (capped) and falls through to per-gid cache. Mirrors the
 *  `_dash_guild_meta_lookup` call site that hits N guilds at a time. */
export async function getGuildMetaBulk(
  guildIds: Iterable<string>,
): Promise<Map<string, GuildMeta>> {
  const out = new Map<string, GuildMeta>();
  const ids = Array.from(new Set(Array.from(guildIds, (g) => String(g)))).filter(Boolean);
  if (ids.length === 0) return out;
  if (!getBotToken()) {
    // Without a bot token we still serve any cached entries we happen to
    // have. Caller treats missing entries as 'name unknown'.
    const now = Date.now();
    for (const gid of ids) {
      const c = guildMetaCache.get(gid);
      if (c && now - c.ts < GUILD_META_TTL_MS) out.set(gid, c.data);
    }
    return out;
  }
  // Cap at 8-way concurrency, same as python's ThreadPoolExecutor(8).
  const MAX_CONCURRENT = 8;
  let i = 0;
  async function worker(): Promise<void> {
    while (i < ids.length) {
      const idx = i++;
      const gid = ids[idx]!;
      const meta = await getGuildMeta(gid);
      if (meta) out.set(gid, meta);
    }
  }
  await Promise.all(Array.from({ length: Math.min(MAX_CONCURRENT, ids.length) }, worker));
  return out;
}

// ---------------------------------------------------------------------------
// Application install counts. Mirrors python's `_dash_app_install_counts`
// (external_api_proxy.py:18290-18312). Cached 5 minutes — these are
// "approximate" counts from Discord and don't churn.
// ---------------------------------------------------------------------------
export interface AppInstallCounts {
  servers_total: number | null;
  user_installs: number | null;
}

const APP_INFO_TTL_MS = 5 * 60 * 1000;
let appInfoCache: { ts: number; data: AppInstallCounts } | null = null;
let appInfoInflight: Promise<AppInstallCounts> | null = null;

interface ApplicationsMeBody {
  approximate_guild_count?: number;
  approximate_user_install_count?: number;
}

export async function getAppInstallCounts(): Promise<AppInstallCounts> {
  const now = Date.now();
  if (appInfoCache && now - appInfoCache.ts < APP_INFO_TTL_MS) {
    return appInfoCache.data;
  }
  if (appInfoInflight) return appInfoInflight;
  const empty: AppInstallCounts = { servers_total: null, user_installs: null };
  if (!getBotToken()) return empty;

  appInfoInflight = (async () => {
    try {
      const res = await botGet<ApplicationsMeBody>('/applications/@me');
      if (res.status === 200 && res.body && typeof res.body === 'object') {
        const body = res.body as ApplicationsMeBody;
        const data: AppInstallCounts = {
          servers_total:
            typeof body.approximate_guild_count === 'number' ? body.approximate_guild_count : null,
          user_installs:
            typeof body.approximate_user_install_count === 'number'
              ? body.approximate_user_install_count
              : null,
        };
        appInfoCache = { ts: Date.now(), data };
        return data;
      }
      return empty;
    } catch (err) {
      log.warn({ err }, 'getAppInstallCounts failed');
      return empty;
    } finally {
      appInfoInflight = null;
    }
  })();
  return appInfoInflight;
}

// ---------------------------------------------------------------------------
// TEST-ONLY helpers — wipe the module-level caches so individual tests can
// observe the fetch behaviour deterministically.
// ---------------------------------------------------------------------------
export function _resetDiscordApiCachesForTests(): void {
  guildMetaCache.clear();
  guildMetaInflight.clear();
  appInfoCache = null;
  appInfoInflight = null;
}
