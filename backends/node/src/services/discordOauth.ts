/**
 * Discord OAuth2 token exchange + identity fetch.
 *
 * Mirrors python external_api_proxy.py:16871-16919 — the slice of the
 * /api/dashboard/auth/callback handler that talks to Discord directly.
 * Kept in its own module so the route file stays focused on HTTP shape
 * and so the OAuth flow is testable in isolation.
 *
 * - exchangeCode(): POST /oauth2/token with client_credentials.
 * - fetchIdentityAndGuilds(): the parallelised pair of /users/@me +
 *   /users/@me/guilds calls (`Promise.all` instead of python's
 *   ThreadPoolExecutor — same effect, no thread).
 * - refreshUserGuilds(): re-fetches guilds with a stored access token
 *   (used by /api/dashboard/me when the cached list is > 10 min old).
 */
import { log } from '../lib/log.js';

export const DISCORD_API_BASE = 'https://discord.com/api/v10';
const TIMEOUT_MS = 15_000;

export interface TokenResponse {
  access_token: string;
  token_type: string;
  expires_in?: number;
  refresh_token?: string;
  scope?: string;
}

export interface DiscordUser {
  id: string;
  username?: string;
  global_name?: string | null;
  avatar?: string | null;
}

export interface DiscordRawGuild {
  id: string;
  name?: string;
  icon?: string | null;
  owner?: boolean;
  permissions?: string | number;
}

/** Token exchange. Returns parsed body or throws on non-200 / network error. */
export async function exchangeCode(args: {
  clientId: string;
  clientSecret: string;
  code: string;
  redirectUri: string;
}): Promise<TokenResponse> {
  const params = new URLSearchParams({
    client_id: args.clientId,
    client_secret: args.clientSecret,
    grant_type: 'authorization_code',
    code: args.code,
    redirect_uri: args.redirectUri,
  });
  const res = await fetch(`${DISCORD_API_BASE}/oauth2/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString(),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  if (res.status !== 200) {
    const text = await res.text().catch(() => '');
    throw new Error(`Token exchange rejected: ${res.status} ${text.slice(0, 300)}`);
  }
  const body = (await res.json()) as TokenResponse;
  if (!body.access_token) {
    throw new Error('No access_token from Discord.');
  }
  return body;
}

async function discordGetWithBearer<T>(path: string, accessToken: string, tokenType = 'Bearer'): Promise<{
  status: number;
  body: T | null;
}> {
  try {
    const res = await fetch(`${DISCORD_API_BASE}${path}`, {
      headers: {
        Authorization: `${tokenType} ${accessToken}`,
        'User-Agent': 'NSWPSN-Dashboard',
      },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    let body: T | null = null;
    try {
      body = (await res.json()) as T;
    } catch {
      body = null;
    }
    return { status: res.status, body };
  } catch (err) {
    log.warn({ err, path }, 'Discord OAuth fetch failed');
    throw err;
  }
}

/**
 * Run /users/@me and /users/@me/guilds in parallel — Promise.all is the
 * Promise-API equivalent of python's ThreadPoolExecutor. Throws if
 * either call returns non-200.
 */
export async function fetchIdentityAndGuilds(accessToken: string): Promise<{
  user: DiscordUser;
  guilds: DiscordRawGuild[];
}> {
  const [meRes, guildRes] = await Promise.all([
    discordGetWithBearer<DiscordUser>('/users/@me', accessToken),
    discordGetWithBearer<DiscordRawGuild[]>('/users/@me/guilds', accessToken),
  ]);
  if (meRes.status !== 200 || guildRes.status !== 200) {
    throw new Error(
      `Discord identity/guild fetch failed (user=${meRes.status}, guilds=${guildRes.status})`,
    );
  }
  if (!meRes.body || !Array.isArray(guildRes.body)) {
    throw new Error('Malformed Discord identity response');
  }
  return { user: meRes.body, guilds: guildRes.body };
}

/**
 * Re-fetch the user's guild list with their stored access_token. Returns
 * null on non-200 (caller should keep the cached list and not bump
 * gfresh — matches python's "leave cached, retry next request" behaviour).
 */
export async function refreshUserGuilds(
  accessToken: string,
  tokenType = 'Bearer',
): Promise<DiscordRawGuild[] | null> {
  try {
    const res = await discordGetWithBearer<DiscordRawGuild[]>(
      '/users/@me/guilds',
      accessToken,
      tokenType,
    );
    if (res.status !== 200 || !Array.isArray(res.body)) return null;
    return res.body;
  } catch {
    return null;
  }
}
