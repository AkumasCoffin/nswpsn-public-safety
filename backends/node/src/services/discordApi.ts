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
