/**
 * Discord OAuth-driven dashboard endpoints.
 *
 * Mirrors python external_api_proxy.py:16797-19046. The router covers
 * the Discord OAuth login/callback/logout flow, /api/dashboard/me, the
 * guild channel/role lookups, preset CRUD, mute-state CRUD, and the
 * admin overview / broadcast / cleanup / bot-action / source-health
 * panels.
 *
 * Cookie crypto + server-side session map live in
 * src/services/dashboardSession.ts (HMAC-signed, mirroring python's
 * home-spun signer). The bot-data Postgres pool is in
 * src/services/botDb.ts (third pool, separate from DATABASE_URL and
 * RDIO_DATABASE_URL).
 *
 * Public-prefix (`/api/dashboard/*` is in PUBLIC_ENDPOINT_PREFIXES) so
 * requireApiKey doesn't run — the dashboard authenticates via the
 * session cookie instead.
 */
import { Hono } from 'hono';
import type { Context } from 'hono';
import { randomBytes } from 'node:crypto';
import { log } from '../lib/log.js';
import { getBotDbPool, isBotDbConfigured } from '../services/botDb.js';
import {
  DASH_OAUTH_COOKIE,
  DASH_OAUTH_STATE_TTL_SECS,
  DASH_SESSION_COOKIE,
  DASH_SESSION_TTL_SECS,
  DASH_GUILD_REFRESH_INTERVAL_SECS,
  buildClearCookie,
  buildSetCookie,
  dropSession,
  getAdminIds,
  getBotGuildIds,
  getCookieDomain,
  getSessionSecret,
  isAdmin,
  isSecureRequest,
  loadSession,
  makeCookie,
  newSid,
  parseCookie,
  parseCookieHeader,
  persistSessionPublic,
  putSession,
  type DashSession,
  type OAuthStatePayload,
  type SessionCookiePayload,
  type SessionGuild,
} from '../services/dashboardSession.js';
import {
  DISCORD_API_BASE,
  exchangeCode,
  fetchIdentityAndGuilds,
  refreshUserGuilds,
  type DiscordRawGuild,
} from '../services/discordOauth.js';
import { botGet, getBotToken, guildIconUrl, userAvatarUrl } from '../services/discordApi.js';

export const dashboardRouter = new Hono();

// -- Discord permission flags. Mirrors python lines 16140-16141.
const MANAGE_CHANNELS = 0x10n;
const ADMINISTRATOR = 0x8n;

// Mirror of discord-bot/bot.py ALERT_TYPES (python line 16156-16167).
const ALERT_TYPES: readonly string[] = [
  'rfs',
  'bom_land', 'bom_marine',
  'traffic_incident', 'traffic_roadwork', 'traffic_flood',
  'traffic_fire', 'traffic_majorevent',
  'endeavour_current', 'endeavour_planned',
  'ausgrid',
  'essential_planned', 'essential_future',
  'waze_hazard', 'waze_jam', 'waze_police', 'waze_roadwork',
  'user_incident',
  'radio_summary',
];

const PG_UNIQUE_VIOLATION = '23505';

// ---------------------------------------------------------------------------
// Tiny helpers.
// ---------------------------------------------------------------------------
function dashErr(c: Context, code: string, message: string, status: number) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return c.json({ error: code, message }, status as any);
}

function getCookieMap(c: Context): Record<string, string> {
  return parseCookieHeader(c.req.header('Cookie'));
}

function isReqSecure(c: Context): boolean {
  return isSecureRequest(c.req.header('X-Forwarded-Proto'), c.req.url);
}

function setSignedCookie(c: Context, name: string, value: string, maxAge: number) {
  const secure = isReqSecure(c);
  const domain = getCookieDomain();
  c.header(
    'Set-Cookie',
    buildSetCookie(name, value, { maxAge, secure, ...(domain ? { domain } : {}) }),
    { append: true },
  );
}

function clearCookie(c: Context, name: string) {
  const secure = isReqSecure(c);
  const domain = getCookieDomain();
  c.header('Set-Cookie', buildClearCookie(name, secure, domain), { append: true });
}

interface NormalisedGuild {
  id: string;
  name: string;
  icon: string | null;
  owner: boolean;
  permissions: string;
  has_bot: boolean;
  manage_channels: boolean;
}

function normaliseGuilds(raw: DiscordRawGuild[], botGuildIds: Set<string>): NormalisedGuild[] {
  const out: NormalisedGuild[] = [];
  for (const g of raw ?? []) {
    let perms = 0n;
    try {
      perms = BigInt(g.permissions ?? 0);
    } catch {
      perms = 0n;
    }
    const gid = String(g.id);
    const owner = Boolean(g.owner);
    const manage = owner || (perms & (MANAGE_CHANNELS | ADMINISTRATOR)) !== 0n;
    out.push({
      id: gid,
      name: g.name ?? '',
      icon: g.icon ?? null,
      owner,
      permissions: String(perms),
      has_bot: botGuildIds.has(gid),
      manage_channels: manage,
    });
  }
  return out;
}

function dashPublicBaseUrl(): string {
  return (process.env['PUBLIC_BASE_URL'] ?? '').replace(/\/+$/, '');
}

function dashFrontendBaseUrl(): string {
  return (
    process.env['DASHBOARD_FRONTEND_URL'] ||
    process.env['DASHBOARD_FRONTEND_BASE'] ||
    dashPublicBaseUrl()
  ).replace(/\/+$/, '');
}

function dashRedirectUri(c: Context): string {
  const env = process.env['DASHBOARD_REDIRECT_URI'];
  if (env) return env;
  const base = dashPublicBaseUrl() || new URL(c.req.url).origin;
  return `${base.replace(/\/+$/, '')}/api/dashboard/auth/callback`;
}

function dashEnabled(): boolean {
  return isBotDbConfigured();
}

// Session-loading helper used inside route handlers. Returns the session
// or sends back the appropriate error response.
async function requireSession(c: Context): Promise<DashSession | Response> {
  if (!dashEnabled()) {
    return dashErr(c, 'dashboard_disabled', 'Dashboard is not configured on this server.', 503);
  }
  if (!getSessionSecret()) {
    return dashErr(c, 'missing_session_secret', 'DASHBOARD_SESSION_SECRET is not configured.', 503);
  }
  const session = await loadSession(c.req.header('Cookie'));
  if (!session) {
    return dashErr(c, 'invalid_session', 'Sign in via /api/dashboard/auth/login.', 401);
  }
  return session;
}

async function requireAdmin(c: Context): Promise<DashSession | Response> {
  const result = await requireSession(c);
  if (result instanceof Response) return result;
  if (!isAdmin(result)) {
    return dashErr(c, 'not_admin', 'Admin access required.', 403);
  }
  return result;
}

interface GuardEntry { id: string; name: string; permissions: string; owner?: boolean; icon?: string | null }

async function guildGuard(
  c: Context,
  session: DashSession,
  guildId: string,
): Promise<{ entry: GuardEntry } | { err: Response }> {
  const entry = (session.guilds ?? []).find((g) => String(g.id) === String(guildId));
  if (!entry) {
    return { err: dashErr(c, 'guild_not_found', 'You are not a member of that guild.', 403) };
  }
  let perms = 0n;
  try {
    perms = BigInt(entry.permissions ?? 0);
  } catch {
    perms = 0n;
  }
  const owner = Boolean(entry.owner);
  if (!owner && (perms & (MANAGE_CHANNELS | ADMINISTRATOR)) === 0n) {
    return {
      err: dashErr(c, 'forbidden', 'Manage Channels permission is required on this guild.', 403),
    };
  }
  const botGuilds = await getBotGuildIds();
  if (!botGuilds.has(String(guildId))) {
    return {
      err: dashErr(c, 'bot_not_in_guild', 'The NSW PSN bot is not configured in this guild yet.', 403),
    };
  }
  return { entry };
}

// ---------------------------------------------------------------------------
// Auth flow.
// ---------------------------------------------------------------------------

dashboardRouter.get('/api/dashboard/auth/login', (c) => {
  if (!dashEnabled()) {
    return dashErr(c, 'dashboard_disabled', 'Dashboard is not configured on this server.', 503);
  }
  const secret = getSessionSecret();
  if (!secret) {
    return dashErr(c, 'missing_session_secret', 'DASHBOARD_SESSION_SECRET is not configured.', 503);
  }
  const clientId = process.env['DISCORD_CLIENT_ID'] ?? '';
  if (!clientId) {
    return dashErr(c, 'dashboard_disabled', 'DISCORD_CLIENT_ID is not configured.', 503);
  }
  const url = new URL(c.req.url);
  const nextPath = url.searchParams.get('next') ?? '/dashboard.html';
  const safeNext = nextPath.startsWith('/') ? nextPath : '/dashboard.html';

  const noncebuf = randomBytes(16);
  const nonce = noncebuf.toString('base64url');
  const statePayload: OAuthStatePayload = {
    nonce,
    next: safeNext,
    exp: Math.floor(Date.now() / 1000) + DASH_OAUTH_STATE_TTL_SECS,
  };
  const stateCookie = makeCookie(statePayload, secret);

  const params = new URLSearchParams({
    client_id: clientId,
    redirect_uri: dashRedirectUri(c),
    response_type: 'code',
    scope: 'identify guilds',
    state: nonce,
    prompt: 'none',
  });
  const target = `https://discord.com/api/oauth2/authorize?${params.toString()}`;
  setSignedCookie(c, DASH_OAUTH_COOKIE, stateCookie, DASH_OAUTH_STATE_TTL_SECS);
  return c.redirect(target, 302);
});

dashboardRouter.get('/api/dashboard/auth/callback', async (c) => {
  if (!dashEnabled()) {
    return dashErr(c, 'dashboard_disabled', 'Dashboard is not configured on this server.', 503);
  }
  const secret = getSessionSecret();
  if (!secret) {
    return dashErr(c, 'missing_session_secret', 'DASHBOARD_SESSION_SECRET is not configured.', 503);
  }
  const url = new URL(c.req.url);
  const code = url.searchParams.get('code');
  const state = url.searchParams.get('state');
  if (!code || !state) {
    return dashErr(c, 'bad_request', 'Missing code or state.', 400);
  }
  const cookieMap = getCookieMap(c);
  const stateCookieVal = cookieMap[DASH_OAUTH_COOKIE];
  const statePayload = parseCookie<OAuthStatePayload>(stateCookieVal, secret);
  if (!statePayload) {
    return dashErr(c, 'bad_request', 'Invalid OAuth state.', 400);
  }
  if (statePayload.nonce !== state) {
    return dashErr(c, 'bad_request', 'OAuth state mismatch.', 400);
  }
  if ((statePayload.exp ?? 0) < Math.floor(Date.now() / 1000)) {
    return dashErr(c, 'bad_request', 'OAuth state expired.', 400);
  }
  const nextPath = statePayload.next?.startsWith('/') ? statePayload.next : '/dashboard.html';

  const clientId = process.env['DISCORD_CLIENT_ID'];
  const clientSecret = process.env['DISCORD_CLIENT_SECRET'];
  if (!clientId || !clientSecret) {
    return dashErr(c, 'dashboard_disabled', 'Discord OAuth is not configured.', 503);
  }

  let token;
  try {
    token = await exchangeCode({
      clientId,
      clientSecret,
      code,
      redirectUri: dashRedirectUri(c),
    });
  } catch (err) {
    return dashErr(c, 'discord_error', `Token exchange failed: ${(err as Error).message}`, 502);
  }

  let user, guildsRaw;
  try {
    const result = await fetchIdentityAndGuilds(token.access_token);
    user = result.user;
    guildsRaw = result.guilds;
  } catch (err) {
    return dashErr(c, 'discord_error', (err as Error).message, 502);
  }

  const botIds = await getBotGuildIds();
  const guildEntries = normaliseGuilds(guildsRaw, botIds);

  const now = Math.floor(Date.now() / 1000);
  const exp = now + DASH_SESSION_TTL_SECS;
  const sid = newSid();

  const sessionGuilds: SessionGuild[] = guildEntries.map((g) => ({
    id: g.id,
    name: g.name,
    icon: g.icon,
    permissions: g.permissions,
    owner: g.owner,
  }));

  await putSession(sid, {
    uid: String(user.id),
    username: user.username || user.global_name || '',
    avatar: user.avatar ?? null,
    access_token: token.access_token,
    token_type: token.token_type ?? 'Bearer',
    refresh_token: token.refresh_token ?? null,
    guilds: sessionGuilds,
    gfresh: now,
    iat: now,
    exp,
  });

  const sessionCookie = makeCookie({ sid, exp } as SessionCookiePayload, secret);
  const base = dashFrontendBaseUrl() || new URL(c.req.url).origin;
  const redirectTarget = base + nextPath;

  setSignedCookie(c, DASH_SESSION_COOKIE, sessionCookie, DASH_SESSION_TTL_SECS);
  clearCookie(c, DASH_OAUTH_COOKIE);
  return c.redirect(redirectTarget, 302);
});

dashboardRouter.post('/api/dashboard/auth/logout', async (c) => {
  const secret = getSessionSecret();
  if (secret) {
    const cookieMap = getCookieMap(c);
    const payload = parseCookie<SessionCookiePayload>(cookieMap[DASH_SESSION_COOKIE], secret);
    if (payload?.sid) {
      await dropSession(payload.sid);
    }
  }
  clearCookie(c, DASH_SESSION_COOKIE);
  clearCookie(c, DASH_OAUTH_COOKIE);
  return c.body(null, 204);
});

// ---------------------------------------------------------------------------
// /me.
// ---------------------------------------------------------------------------
dashboardRouter.get('/api/dashboard/me', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;

  const botGuildIds = await getBotGuildIds();
  const now = Math.floor(Date.now() / 1000);

  // Refresh from Discord when stale.
  if (now - Number(session.gfresh ?? 0) > DASH_GUILD_REFRESH_INTERVAL_SECS && session.access_token) {
    const fresh = await refreshUserGuilds(session.access_token, session.token_type ?? 'Bearer');
    if (fresh) {
      const normed = normaliseGuilds(fresh, botGuildIds);
      session.guilds = normed.map((g) => ({
        id: g.id,
        name: g.name,
        icon: g.icon,
        permissions: g.permissions,
        owner: g.owner,
      }));
      session.gfresh = now;
      if (session._sid) {
        void persistSessionPublic(session._sid, session);
      }
    }
  }

  const guildsOut = (session.guilds ?? []).map((g) => {
    const gid = String(g.id);
    let perms = 0n;
    try {
      perms = BigInt(g.permissions ?? 0);
    } catch {
      perms = 0n;
    }
    const isOwner = Boolean(g.owner);
    return {
      id: gid,
      name: g.name ?? '',
      icon_url: guildIconUrl(gid, g.icon),
      has_bot: botGuildIds.has(gid),
      owner: isOwner,
      manage_channels: isOwner || (perms & (MANAGE_CHANNELS | ADMINISTRATOR)) !== 0n,
    };
  });

  let botInviteUrl: string | null = null;
  const clientId = process.env['DISCORD_CLIENT_ID'] ?? '';
  if (clientId) {
    botInviteUrl =
      `https://discord.com/oauth2/authorize?client_id=${clientId}` +
      '&permissions=378091407360&scope=bot+applications.commands';
  }

  return c.json({
    user: {
      id: session.uid,
      username: session.username,
      avatar_url: userAvatarUrl(session.uid, session.avatar),
      is_admin: isAdmin(session),
    },
    guilds: guildsOut,
    bot_invite_url: botInviteUrl,
  });
});

// ---------------------------------------------------------------------------
// Discord channels/roles (cached).
// ---------------------------------------------------------------------------
const DISCORD_CACHE_TTL_MS = 60_000;
interface CacheEntry<T> { ts: number; data: T }
const discordCache = new Map<string, CacheEntry<unknown>>();

function cacheGet<T>(kind: string, key: string): T | null {
  const e = discordCache.get(`${kind}:${key}`);
  if (!e) return null;
  if (Date.now() - e.ts > DISCORD_CACHE_TTL_MS) return null;
  return e.data as T;
}
function cacheSet<T>(kind: string, key: string, data: T) {
  discordCache.set(`${kind}:${key}`, { ts: Date.now(), data });
  // Sweep stale entries (>10x TTL).
  if (discordCache.size > 256) {
    const cutoff = Date.now() - DISCORD_CACHE_TTL_MS * 10;
    for (const [k, v] of discordCache) {
      if (v.ts < cutoff) discordCache.delete(k);
    }
  }
}

interface DiscordChannel { id: string; name?: string; type?: number; position?: number; parent_id?: string | null }
interface DiscordRole { id: string; name?: string; color?: number; position?: number; managed?: boolean }

dashboardRouter.get('/api/dashboard/guilds/:guildId/channels', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;
  const guildId = c.req.param('guildId');
  const guard = await guildGuard(c, session, guildId);
  if ('err' in guard) return guard.err;

  const cached = cacheGet<unknown[]>('channels', guildId);
  if (cached) return c.json(cached);

  const res = await botGet<DiscordChannel[]>(`/guilds/${guildId}/channels`);
  if (res.status === 429) {
    return dashErr(c, 'rate_limited', 'Discord rate limit hit; please retry shortly.', 503);
  }
  if (res.status !== 200 || !Array.isArray(res.body)) {
    return dashErr(
      c,
      'discord_error',
      `Discord responded ${res.status}.`,
      res.status >= 500 ? 502 : 503,
    );
  }
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
  cacheSet('channels', guildId, channels);
  return c.json(channels);
});

dashboardRouter.get('/api/dashboard/guilds/:guildId/roles', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;
  const guildId = c.req.param('guildId');
  const guard = await guildGuard(c, session, guildId);
  if ('err' in guard) return guard.err;

  const cached = cacheGet<unknown[]>('roles', guildId);
  if (cached) return c.json(cached);

  const res = await botGet<DiscordRole[]>(`/guilds/${guildId}/roles`);
  if (res.status === 429) {
    return dashErr(c, 'rate_limited', 'Discord rate limit hit; please retry shortly.', 503);
  }
  if (res.status !== 200 || !Array.isArray(res.body)) {
    return dashErr(
      c,
      'discord_error',
      `Discord responded ${res.status}.`,
      res.status >= 500 ? 502 : 503,
    );
  }
  const roles: Array<{ id: string; name: string; color: number; position: number }> = [];
  for (const r of res.body) {
    if (String(r.id) === String(guildId)) continue;
    if (r.managed) continue;
    roles.push({
      id: String(r.id),
      name: r.name ?? '',
      color: Number(r.color ?? 0),
      position: Number(r.position ?? 0),
    });
  }
  roles.sort((a, b) => b.position - a.position);
  cacheSet('roles', guildId, roles);
  return c.json(roles);
});

// ---------------------------------------------------------------------------
// Preset CRUD.
// ---------------------------------------------------------------------------
interface PresetRow {
  id: number;
  channel_id: number | string;
  name: string;
  alert_types?: string[];
  pager_enabled?: boolean;
  pager_capcodes?: string | null;
  role_ids?: Array<number | string>;
  enabled?: boolean;
  enabled_ping?: boolean;
  type_overrides?: Record<string, { enabled?: boolean; enabled_ping?: boolean }> | null;
  filters?: Record<string, unknown> | null;
  created_at?: Date | string | null;
  updated_at?: Date | string | null;
}

function isoOrNull(v: Date | string | null | undefined): string | null {
  if (!v) return null;
  if (v instanceof Date) return v.toISOString();
  return String(v);
}

function rowToPreset(row: PresetRow): Record<string, unknown> {
  const roleIds = (row.role_ids ?? []).map((r) => String(BigInt(r)));
  const alertTypes = row.alert_types ?? [];
  const overridesRaw = row.type_overrides ?? {};
  const typeOverrides: Record<string, { enabled: boolean; enabled_ping: boolean }> = {};
  if (overridesRaw && typeof overridesRaw === 'object') {
    for (const [k, v] of Object.entries(overridesRaw)) {
      if (!v || typeof v !== 'object') continue;
      typeOverrides[k] = {
        enabled: Boolean((v as { enabled?: boolean }).enabled ?? true),
        enabled_ping: Boolean((v as { enabled_ping?: boolean }).enabled_ping ?? true),
      };
    }
  }
  return {
    id: Number(row.id),
    channel_id: String(BigInt(row.channel_id)),
    name: row.name ?? '',
    alert_types: alertTypes,
    pager_enabled: Boolean(row.pager_enabled),
    pager_capcodes: row.pager_capcodes ?? null,
    role_ids: roleIds,
    enabled: Boolean(row.enabled ?? true),
    enabled_ping: Boolean(row.enabled_ping ?? true),
    type_overrides: typeOverrides,
    filters:
      row.filters && typeof row.filters === 'object' ? row.filters : {},
    created_at: isoOrNull(row.created_at),
    updated_at: isoOrNull(row.updated_at),
  };
}

const PRESET_COLS =
  'id, guild_id, channel_id, name, alert_types, pager_enabled, pager_capcodes, ' +
  'role_ids, enabled, enabled_ping, type_overrides, filters, created_at, updated_at';

function parseAlertTypes(raw: unknown): string[] | null {
  if (raw == null) return [];
  if (!Array.isArray(raw)) return null;
  const seen = new Set<string>();
  const out: string[] = [];
  for (const a of raw) {
    if (typeof a !== 'string') return null;
    const s = a.trim();
    if (!s) continue;
    if (!ALERT_TYPES.includes(s)) return null;
    if (seen.has(s)) continue;
    seen.add(s);
    out.push(s);
  }
  return out;
}

function parseRoleIds(raw: unknown): bigint[] | null {
  if (raw == null) return [];
  if (!Array.isArray(raw)) return null;
  const seen = new Set<string>();
  const out: bigint[] = [];
  for (const p of raw) {
    if (p == null) continue;
    const s = String(p).trim();
    if (!s) continue;
    let n: bigint;
    try {
      n = BigInt(s);
    } catch {
      return null;
    }
    const key = n.toString();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(n);
  }
  return out;
}

dashboardRouter.get('/api/dashboard/guilds/:guildId/presets', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;
  const guildId = c.req.param('guildId');
  const guard = await guildGuard(c, session, guildId);
  if ('err' in guard) return guard.err;

  let gid: bigint;
  try {
    gid = BigInt(guildId);
  } catch {
    return dashErr(c, 'bad_request', 'guild_id must be numeric.', 400);
  }
  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);

  try {
    const r = await pool.query<PresetRow>(
      `SELECT ${PRESET_COLS} FROM alert_presets WHERE guild_id=$1 ORDER BY channel_id, name`,
      [gid.toString()],
    );
    return c.json({ presets: r.rows.map(rowToPreset) });
  } catch (err) {
    log.error({ err }, 'dashboard presets_get error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

dashboardRouter.post('/api/dashboard/guilds/:guildId/presets', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;
  const guildId = c.req.param('guildId');
  const guard = await guildGuard(c, session, guildId);
  if ('err' in guard) return guard.err;

  const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
  let gid: bigint;
  let cid: bigint;
  try {
    gid = BigInt(guildId);
    cid = BigInt(String(body['channel_id'] ?? '0'));
  } catch {
    return dashErr(c, 'bad_request', 'channel_id must be numeric.', 400);
  }
  if (cid === 0n) return dashErr(c, 'bad_request', 'channel_id is required.', 400);

  const nameRaw = body['name'];
  if (typeof nameRaw !== 'string') return dashErr(c, 'bad_request', 'name is required.', 400);
  const name = nameRaw.trim();
  if (name.length < 1 || name.length > 64) {
    return dashErr(c, 'bad_request', 'name must be 1-64 characters.', 400);
  }

  const alertTypes = parseAlertTypes(body['alert_types']);
  if (alertTypes === null) {
    return dashErr(c, 'bad_request', `alert_types must be strings in ${ALERT_TYPES.join(',')}.`, 400);
  }
  const roleIds = parseRoleIds(body['role_ids']);
  if (roleIds === null) {
    return dashErr(c, 'bad_request', 'role_ids must be an array of numeric ids.', 400);
  }

  const pagerEnabled = Boolean(body['pager_enabled']);
  const pagerCapcodes = body['pager_capcodes'];
  if (pagerCapcodes != null && typeof pagerCapcodes !== 'string') {
    return dashErr(c, 'bad_request', 'pager_capcodes must be a string.', 400);
  }
  if (alertTypes.length === 0 && !pagerEnabled) {
    return dashErr(
      c,
      'empty_preset',
      'Preset must have at least one alert_type or pager_enabled=true.',
      400,
    );
  }

  const enabled = body['enabled'] === undefined ? true : Boolean(body['enabled']);
  const enabledPing = body['enabled_ping'] === undefined ? true : Boolean(body['enabled_ping']);

  // Filters: leave validation light — just accept an object as-is. The
  // python-side bespoke geofilter / severity / subtype validation is
  // recreated as a punted TODO (see final report).
  const filtersRaw = body['filters'];
  if (filtersRaw != null && (typeof filtersRaw !== 'object' || Array.isArray(filtersRaw))) {
    return dashErr(c, 'bad_filter', 'filters must be an object.', 400);
  }
  const filters = filtersRaw && typeof filtersRaw === 'object' ? filtersRaw : {};

  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);

  try {
    const r = await pool.query<PresetRow>(
      `INSERT INTO alert_presets
         (guild_id, channel_id, name, alert_types, pager_enabled, pager_capcodes,
          role_ids, enabled, enabled_ping, type_overrides, filters, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '{}'::jsonb, $10::jsonb, now(), now())
       RETURNING ${PRESET_COLS}`,
      [
        gid.toString(),
        cid.toString(),
        name,
        alertTypes,
        pagerEnabled,
        pagerCapcodes ?? null,
        roleIds.map((b) => b.toString()),
        enabled,
        enabledPing,
        JSON.stringify(filters),
      ],
    );
    if (!r.rows[0]) {
      return dashErr(c, 'db_error', 'INSERT returned no row', 500);
    }
    return c.json({ preset: rowToPreset(r.rows[0]) }, 201);
  } catch (err) {
    if ((err as { code?: string }).code === PG_UNIQUE_VIOLATION) {
      return dashErr(
        c,
        'name_conflict',
        'A preset with this name already exists on this channel.',
        409,
      );
    }
    log.error({ err }, 'dashboard presets_create error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

dashboardRouter.patch('/api/dashboard/guilds/:guildId/presets/:presetId', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;
  const guildId = c.req.param('guildId');
  const presetId = Number(c.req.param('presetId'));
  if (!Number.isFinite(presetId)) {
    return dashErr(c, 'bad_request', 'preset_id must be numeric.', 400);
  }
  const guard = await guildGuard(c, session, guildId);
  if ('err' in guard) return guard.err;

  let gid: bigint;
  try {
    gid = BigInt(guildId);
  } catch {
    return dashErr(c, 'bad_request', 'guild_id must be numeric.', 400);
  }

  const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
  const sets: string[] = [];
  const params: unknown[] = [];

  if ('channel_id' in body) {
    let cid: bigint;
    try {
      cid = BigInt(String(body['channel_id']));
    } catch {
      return dashErr(c, 'bad_request', 'channel_id must be numeric.', 400);
    }
    if (cid === 0n) return dashErr(c, 'bad_request', 'channel_id must be non-zero.', 400);
    sets.push(`channel_id=$${params.length + 1}`);
    params.push(cid.toString());
  }
  if ('name' in body) {
    const nm = body['name'];
    if (typeof nm !== 'string') return dashErr(c, 'bad_request', 'name must be a string.', 400);
    const trimmed = nm.trim();
    if (trimmed.length < 1 || trimmed.length > 64) {
      return dashErr(c, 'bad_request', 'name must be 1-64 characters.', 400);
    }
    sets.push(`name=$${params.length + 1}`);
    params.push(trimmed);
  }
  let newAlertTypes: string[] | null = null;
  if ('alert_types' in body) {
    newAlertTypes = parseAlertTypes(body['alert_types']);
    if (newAlertTypes === null) {
      return dashErr(c, 'bad_request', `alert_types must be strings in ${ALERT_TYPES.join(',')}.`, 400);
    }
    sets.push(`alert_types=$${params.length + 1}`);
    params.push(newAlertTypes);
  }
  let newPagerEnabled: boolean | null = null;
  if ('pager_enabled' in body) {
    newPagerEnabled = Boolean(body['pager_enabled']);
    sets.push(`pager_enabled=$${params.length + 1}`);
    params.push(newPagerEnabled);
  }
  if ('pager_capcodes' in body) {
    const pc = body['pager_capcodes'];
    if (pc != null && typeof pc !== 'string') {
      return dashErr(c, 'bad_request', 'pager_capcodes must be a string.', 400);
    }
    sets.push(`pager_capcodes=$${params.length + 1}`);
    params.push(pc ?? null);
  }
  if ('role_ids' in body) {
    const rids = parseRoleIds(body['role_ids']);
    if (rids === null) {
      return dashErr(c, 'bad_request', 'role_ids must be an array of numeric ids.', 400);
    }
    sets.push(`role_ids=$${params.length + 1}`);
    params.push(rids.map((b) => b.toString()));
  }
  if ('enabled' in body) {
    sets.push(`enabled=$${params.length + 1}`);
    params.push(Boolean(body['enabled']));
  }
  if ('enabled_ping' in body) {
    sets.push(`enabled_ping=$${params.length + 1}`);
    params.push(Boolean(body['enabled_ping']));
  }
  if ('filters' in body) {
    const f = body['filters'];
    if (f != null && (typeof f !== 'object' || Array.isArray(f))) {
      return dashErr(c, 'bad_filter', 'filters must be an object.', 400);
    }
    sets.push(`filters=$${params.length + 1}::jsonb`);
    params.push(JSON.stringify(f && typeof f === 'object' ? f : {}));
  }

  if (sets.length === 0) {
    return dashErr(c, 'bad_request', 'No updatable fields supplied.', 400);
  }

  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);

  try {
    const existing = await pool.query<PresetRow>(
      `SELECT ${PRESET_COLS} FROM alert_presets WHERE id=$1 AND guild_id=$2`,
      [presetId, gid.toString()],
    );
    if (existing.rows.length === 0) {
      return dashErr(c, 'not_found', 'Preset not found.', 404);
    }
    const ex = existing.rows[0]!;
    const finalAlertTypes = newAlertTypes ?? (ex.alert_types ?? []);
    const finalPager = newPagerEnabled ?? Boolean(ex.pager_enabled);
    if (finalAlertTypes.length === 0 && !finalPager) {
      return dashErr(
        c,
        'empty_preset',
        'Preset must have at least one alert_type or pager_enabled=true.',
        400,
      );
    }
    sets.push('updated_at=now()');
    const sql =
      `UPDATE alert_presets SET ${sets.join(', ')} ` +
      `WHERE id=$${params.length + 1} AND guild_id=$${params.length + 2} ` +
      `RETURNING ${PRESET_COLS}`;
    params.push(presetId, gid.toString());
    const r = await pool.query<PresetRow>(sql, params);
    if (!r.rows[0]) return dashErr(c, 'not_found', 'Preset not found.', 404);
    return c.json({ preset: rowToPreset(r.rows[0]) });
  } catch (err) {
    if ((err as { code?: string }).code === PG_UNIQUE_VIOLATION) {
      return dashErr(
        c,
        'name_conflict',
        'A preset with this name already exists on this channel.',
        409,
      );
    }
    log.error({ err }, 'dashboard presets_patch error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

dashboardRouter.delete('/api/dashboard/guilds/:guildId/presets/:presetId', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;
  const guildId = c.req.param('guildId');
  const presetId = Number(c.req.param('presetId'));
  if (!Number.isFinite(presetId)) {
    return dashErr(c, 'bad_request', 'preset_id must be numeric.', 400);
  }
  const guard = await guildGuard(c, session, guildId);
  if ('err' in guard) return guard.err;

  let gid: bigint;
  try {
    gid = BigInt(guildId);
  } catch {
    return dashErr(c, 'bad_request', 'guild_id must be numeric.', 400);
  }
  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);
  try {
    const r = await pool.query(
      'DELETE FROM alert_presets WHERE id=$1 AND guild_id=$2',
      [presetId, gid.toString()],
    );
    if ((r.rowCount ?? 0) === 0) {
      return dashErr(c, 'not_found', 'Preset not found.', 404);
    }
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err }, 'dashboard presets_delete error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

// ---------------------------------------------------------------------------
// Preset stats — simplified compared to python's stale-while-revalidate
// pattern. We just run the query inline with a 60s in-memory cache;
// statement_timeout on the pool keeps it bounded. See punted TODOs.
// ---------------------------------------------------------------------------
interface PresetStatsRow {
  preset_id: string | number;
  fires_7d: number | string | null;
  fires_30d: number | string | null;
  last_fire: Date | string | null;
}
const PRESET_STATS_TTL_MS = 60_000;
const presetStatsCache = new Map<string, { ts: number; data: unknown[] }>();

dashboardRouter.get('/api/dashboard/guilds/:guildId/preset-stats', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;
  const guildId = c.req.param('guildId');
  const guard = await guildGuard(c, session, guildId);
  if ('err' in guard) return guard.err;

  let gid: bigint;
  try {
    gid = BigInt(guildId);
  } catch {
    return dashErr(c, 'bad_request', 'guild_id must be numeric.', 400);
  }

  const cached = presetStatsCache.get(guildId);
  if (cached && Date.now() - cached.ts < PRESET_STATS_TTL_MS) {
    return c.json({ stats: cached.data, cache_age_seconds: Math.floor((Date.now() - cached.ts) / 1000) });
  }

  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);

  try {
    const r = await pool.query<PresetStatsRow>(
      `SELECT p.id AS preset_id,
              COUNT(*) FILTER (WHERE f.fired_at > NOW() - INTERVAL '7 days')  AS fires_7d,
              COUNT(*) FILTER (WHERE f.fired_at > NOW() - INTERVAL '30 days') AS fires_30d,
              MAX(f.fired_at) AS last_fire
         FROM alert_presets p
         LEFT JOIN preset_fire_log f ON f.preset_id = p.id
        WHERE p.guild_id = $1
     GROUP BY p.id`,
      [gid.toString()],
    );
    const out = r.rows.map((row) => ({
      preset_id: String(row.preset_id),
      fires_7d: Number(row.fires_7d ?? 0),
      fires_30d: Number(row.fires_30d ?? 0),
      last_fire: isoOrNull(row.last_fire),
    }));
    presetStatsCache.set(guildId, { ts: Date.now(), data: out });
    return c.json({ stats: out, cache_age_seconds: 0 });
  } catch (err) {
    log.warn({ err, guildId }, 'preset_stats query failed');
    // Empty stub matches python's first-call behaviour.
    return c.json({ stats: [], cache_age_seconds: null, warming: true });
  }
});

// ---------------------------------------------------------------------------
// Mute state.
// ---------------------------------------------------------------------------
interface MuteRow { enabled?: boolean; enabled_ping?: boolean; channel_id?: string | number }

function rowToMute(row: MuteRow, channelMode = false): Record<string, unknown> {
  const base = {
    enabled: Boolean(row.enabled ?? true),
    enabled_ping: Boolean(row.enabled_ping ?? true),
  };
  if (channelMode && row.channel_id != null) {
    return { channel_id: String(BigInt(row.channel_id)), ...base };
  }
  return base;
}

function partialMuteValues(body: Record<string, unknown>):
  | { enabled: boolean | null; enabledPing: boolean | null }
  | { error: string } {
  const hasEnabled = 'enabled' in body && body['enabled'] != null;
  const hasPing = 'enabled_ping' in body && body['enabled_ping'] != null;
  if (!hasEnabled && !hasPing) {
    return { error: 'Supply at least one of enabled/enabled_ping.' };
  }
  return {
    enabled: hasEnabled ? Boolean(body['enabled']) : null,
    enabledPing: hasPing ? Boolean(body['enabled_ping']) : null,
  };
}

dashboardRouter.get('/api/dashboard/guilds/:guildId/mute-state', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;
  const guildId = c.req.param('guildId');
  const guard = await guildGuard(c, session, guildId);
  if ('err' in guard) return guard.err;

  let gid: bigint;
  try {
    gid = BigInt(guildId);
  } catch {
    return dashErr(c, 'bad_request', 'guild_id must be numeric.', 400);
  }

  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);
  try {
    const g = await pool.query<MuteRow>(
      'SELECT enabled, enabled_ping FROM guild_mute_state WHERE guild_id=$1',
      [gid.toString()],
    );
    const guildState = g.rows[0]
      ? rowToMute(g.rows[0])
      : { enabled: true, enabled_ping: true };
    const ch = await pool.query<MuteRow>(
      'SELECT channel_id, enabled, enabled_ping FROM channel_mute_state ' +
        'WHERE guild_id=$1 ORDER BY channel_id',
      [gid.toString()],
    );
    const channels = ch.rows.map((r) => rowToMute(r, true));
    return c.json({ guild: guildState, channels });
  } catch (err) {
    log.error({ err }, 'dashboard mute_state_get error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

dashboardRouter.put('/api/dashboard/guilds/:guildId/mute-state/guild', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;
  const guildId = c.req.param('guildId');
  const guard = await guildGuard(c, session, guildId);
  if ('err' in guard) return guard.err;

  let gid: bigint;
  try {
    gid = BigInt(guildId);
  } catch {
    return dashErr(c, 'bad_request', 'guild_id must be numeric.', 400);
  }
  const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
  const partial = partialMuteValues(body);
  if ('error' in partial) return dashErr(c, 'bad_request', partial.error, 400);

  const insEnabled = partial.enabled ?? true;
  const insPing = partial.enabledPing ?? true;

  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);
  try {
    const r = await pool.query<MuteRow>(
      `INSERT INTO guild_mute_state (guild_id, enabled, enabled_ping, updated_at)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (guild_id) DO UPDATE SET
         enabled = CASE WHEN $4 THEN EXCLUDED.enabled ELSE guild_mute_state.enabled END,
         enabled_ping = CASE WHEN $5 THEN EXCLUDED.enabled_ping ELSE guild_mute_state.enabled_ping END,
         updated_at = now()
       RETURNING enabled, enabled_ping`,
      [gid.toString(), insEnabled, insPing, partial.enabled !== null, partial.enabledPing !== null],
    );
    return c.json({ guild: rowToMute(r.rows[0] ?? {}) });
  } catch (err) {
    log.error({ err }, 'dashboard mute_state_guild_put error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

dashboardRouter.delete('/api/dashboard/guilds/:guildId/mute-state/guild', async (c) => {
  const session = await requireSession(c);
  if (session instanceof Response) return session;
  const guildId = c.req.param('guildId');
  const guard = await guildGuard(c, session, guildId);
  if ('err' in guard) return guard.err;

  let gid: bigint;
  try {
    gid = BigInt(guildId);
  } catch {
    return dashErr(c, 'bad_request', 'guild_id must be numeric.', 400);
  }
  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);
  try {
    await pool.query('DELETE FROM guild_mute_state WHERE guild_id=$1', [gid.toString()]);
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err }, 'dashboard mute_state_guild_delete error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

dashboardRouter.put(
  '/api/dashboard/guilds/:guildId/mute-state/channels/:channelId',
  async (c) => {
    const session = await requireSession(c);
    if (session instanceof Response) return session;
    const guildId = c.req.param('guildId');
    const channelId = c.req.param('channelId');
    const guard = await guildGuard(c, session, guildId);
    if ('err' in guard) return guard.err;

    let gid: bigint, cid: bigint;
    try {
      gid = BigInt(guildId);
      cid = BigInt(channelId);
    } catch {
      return dashErr(c, 'bad_request', 'guild_id/channel_id must be numeric.', 400);
    }
    if (cid === 0n) return dashErr(c, 'bad_request', 'channel_id must be non-zero.', 400);

    const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const partial = partialMuteValues(body);
    if ('error' in partial) return dashErr(c, 'bad_request', partial.error, 400);
    const insEnabled = partial.enabled ?? true;
    const insPing = partial.enabledPing ?? true;

    const pool = await getBotDbPool();
    if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);
    try {
      const r = await pool.query<MuteRow>(
        `INSERT INTO channel_mute_state (guild_id, channel_id, enabled, enabled_ping, updated_at)
         VALUES ($1, $2, $3, $4, now())
         ON CONFLICT (guild_id, channel_id) DO UPDATE SET
           enabled = CASE WHEN $5 THEN EXCLUDED.enabled ELSE channel_mute_state.enabled END,
           enabled_ping = CASE WHEN $6 THEN EXCLUDED.enabled_ping ELSE channel_mute_state.enabled_ping END,
           updated_at = now()
         RETURNING channel_id, enabled, enabled_ping`,
        [
          gid.toString(),
          cid.toString(),
          insEnabled,
          insPing,
          partial.enabled !== null,
          partial.enabledPing !== null,
        ],
      );
      return c.json({ channel: rowToMute(r.rows[0] ?? { channel_id: cid.toString() }, true) });
    } catch (err) {
      log.error({ err }, 'dashboard mute_state_channel_put error');
      return dashErr(c, 'db_error', String((err as Error).message), 500);
    }
  },
);

dashboardRouter.delete(
  '/api/dashboard/guilds/:guildId/mute-state/channels/:channelId',
  async (c) => {
    const session = await requireSession(c);
    if (session instanceof Response) return session;
    const guildId = c.req.param('guildId');
    const channelId = c.req.param('channelId');
    const guard = await guildGuard(c, session, guildId);
    if ('err' in guard) return guard.err;

    let gid: bigint, cid: bigint;
    try {
      gid = BigInt(guildId);
      cid = BigInt(channelId);
    } catch {
      return dashErr(c, 'bad_request', 'guild_id/channel_id must be numeric.', 400);
    }
    const pool = await getBotDbPool();
    if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);
    try {
      await pool.query(
        'DELETE FROM channel_mute_state WHERE guild_id=$1 AND channel_id=$2',
        [gid.toString(), cid.toString()],
      );
      return c.json({ ok: true });
    } catch (err) {
      log.error({ err }, 'dashboard mute_state_channel_delete error');
      return dashErr(c, 'db_error', String((err as Error).message), 500);
    }
  },
);

// ---------------------------------------------------------------------------
// Admin: overview.
// ---------------------------------------------------------------------------
const ADMIN_OVERVIEW_TTL_MS = 30_000;
let adminOverviewCache: { ts: number; data: unknown } | null = null;

dashboardRouter.get('/api/dashboard/admin/overview', async (c) => {
  const session = await requireAdmin(c);
  if (session instanceof Response) return session;

  const now = Math.floor(Date.now() / 1000);
  if (adminOverviewCache && Date.now() - adminOverviewCache.ts < ADMIN_OVERVIEW_TTL_MS) {
    return c.json(adminOverviewCache.data);
  }

  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);

  try {
    // Aggregate alert_presets stats in one round-trip.
    const apRes = await pool.query<{
      total: number | string | null;
      pager: number | string | null;
      muted: number | string | null;
      guilds_with_presets: number | string | null;
      channels_configured: number | string | null;
    }>(`
      SELECT
        COUNT(*)                                AS total,
        COUNT(*) FILTER (WHERE pager_enabled)   AS pager,
        COUNT(*) FILTER (WHERE NOT enabled)     AS muted,
        COUNT(DISTINCT guild_id)                AS guilds_with_presets,
        COUNT(DISTINCT (guild_id, channel_id))  AS channels_configured
      FROM alert_presets
    `);
    const ap = apRes.rows[0] ?? {
      total: 0, pager: 0, muted: 0, guilds_with_presets: 0, channels_configured: 0,
    };

    const msRes = await pool.query<{
      guilds_muted: number | string | null;
      channels_muted: number | string | null;
    }>(`
      SELECT
        (SELECT COUNT(*) FROM guild_mute_state   WHERE NOT enabled) AS guilds_muted,
        (SELECT COUNT(*) FROM channel_mute_state WHERE NOT enabled) AS channels_muted
    `);
    const ms = msRes.rows[0] ?? { guilds_muted: 0, channels_muted: 0 };

    const typeCounts: Record<string, number> = {};
    for (const t of ALERT_TYPES) typeCounts[t] = 0;
    const tcRes = await pool.query<{ t: string; n: number | string | null }>(
      'SELECT unnest(alert_types) AS t, COUNT(*) AS n FROM alert_presets GROUP BY t',
    );
    for (const r of tcRes.rows) {
      if (r.t in typeCounts) typeCounts[r.t] = Number(r.n ?? 0);
    }

    const guildsRes = await pool.query<{
      guild_id: string | number;
      preset_count: number | string | null;
      channel_count: number | string | null;
      pager_presets: number | string | null;
      muted_presets: number | string | null;
      last_change: Date | string | null;
    }>(`
      SELECT guild_id,
             COUNT(*) AS preset_count,
             COUNT(DISTINCT channel_id) AS channel_count,
             SUM(CASE WHEN pager_enabled THEN 1 ELSE 0 END) AS pager_presets,
             SUM(CASE WHEN NOT enabled THEN 1 ELSE 0 END) AS muted_presets,
             MAX(updated_at) AS last_change
        FROM alert_presets
    GROUP BY guild_id
    ORDER BY preset_count DESC
    `);
    const guildsOut = guildsRes.rows.map((r) => ({
      guild_id: String(r.guild_id),
      name: '',
      icon_url: '',
      preset_count: Number(r.preset_count ?? 0),
      channel_count: Number(r.channel_count ?? 0),
      pager_presets: Number(r.pager_presets ?? 0),
      muted_presets: Number(r.muted_presets ?? 0),
      last_change: isoOrNull(r.last_change),
    }));

    let usersLifetime = 0;
    let historicalUsers: unknown[] = [];
    try {
      const huRes = await pool.query<{
        uid: string;
        username: string | null;
        avatar: string | null;
        last_seen: number | string | null;
        login_count: number | string | null;
      }>(
        'SELECT uid, username, avatar, ' +
          'EXTRACT(EPOCH FROM last_seen)::bigint AS last_seen, ' +
          'login_count FROM dashboard_users ORDER BY last_seen DESC',
      );
      historicalUsers = huRes.rows.map((r) => ({
        uid: String(r.uid),
        username: r.username ?? '',
        avatar_url: userAvatarUrl(String(r.uid), r.avatar),
        guild_count: 0,
        age_seconds: Math.max(0, now - Number(r.last_seen ?? 0)),
        session_age_seconds: Math.max(0, now - Number(r.last_seen ?? 0)),
        is_admin: getAdminIds().has(String(r.uid)),
        is_active: false,
        login_count: Number(r.login_count ?? 0),
      }));
      const cntRes = await pool.query<{ n: number | string | null }>(
        'SELECT COUNT(*) AS n FROM dashboard_users',
      );
      usersLifetime = Number(cntRes.rows[0]?.n ?? 0);
    } catch (err) {
      log.warn({ err }, 'dashboard_users query failed');
    }

    const payload = {
      stats: {
        sessions_active: 0,
        dashboard_users: usersLifetime,
        users_active: 0,
        users_total: usersLifetime,
        servers_total: null,
        user_installs: null,
        guilds_with_presets: Number(ap.guilds_with_presets ?? 0),
        channels_configured: Number(ap.channels_configured ?? 0),
        presets_total: Number(ap.total ?? 0),
        presets_pager: Number(ap.pager ?? 0),
        presets_muted: Number(ap.muted ?? 0),
        guilds_muted: Number(ms.guilds_muted ?? 0),
        channels_muted: Number(ms.channels_muted ?? 0),
        alert_type_counts: typeCounts,
      },
      guilds: guildsOut,
      sessions: historicalUsers,
      admin_ids: Array.from(getAdminIds()).sort(),
      server_time: now,
    };
    adminOverviewCache = { ts: Date.now(), data: payload };
    return c.json(payload);
  } catch (err) {
    log.error({ err }, 'dashboard admin_overview error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

// ---------------------------------------------------------------------------
// Admin: broadcast targets + broadcast enqueue.
// ---------------------------------------------------------------------------
const BCAST_TARGETS_TTL_MS = 60_000;
let bcastTargetsCache: { ts: number; data: unknown } | null = null;

dashboardRouter.get('/api/dashboard/admin/broadcast/targets', async (c) => {
  const session = await requireAdmin(c);
  if (session instanceof Response) return session;
  if (bcastTargetsCache && Date.now() - bcastTargetsCache.ts < BCAST_TARGETS_TTL_MS) {
    return c.json({ targets: bcastTargetsCache.data });
  }

  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);

  try {
    const r = await pool.query<{ guild_id: string | number }>(
      'SELECT DISTINCT guild_id FROM alert_presets ORDER BY guild_id',
    );
    const guildIds = r.rows.map((row) => String(row.guild_id));
    // Skip the heavy parallel Discord-fetch dance from python — we just
    // return the guild ids so the admin UI can drive lookups separately.
    // See punted TODOs in the final report for full parity.
    const out = guildIds.map((gid) => ({
      guild_id: gid,
      guild_name: '',
      guild_icon_url: '',
      detected_channel_id: null,
      detected_channel_name: '',
      channels: [] as unknown[],
      channels_error: 'channels_not_fetched',
    }));
    bcastTargetsCache = { ts: Date.now(), data: out };
    return c.json({ targets: out });
  } catch (err) {
    log.error({ err }, 'dashboard broadcast_targets error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

dashboardRouter.post('/api/dashboard/admin/broadcast', async (c) => {
  const session = await requireAdmin(c);
  if (session instanceof Response) return session;

  const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
  const title = typeof body['title'] === 'string' ? body['title'].trim() : '';
  const description = typeof body['description'] === 'string' ? body['description'].trim() : '';
  const colorHex = typeof body['color'] === 'string' ? body['color'].trim() : '';
  const footer = typeof body['footer'] === 'string' ? body['footer'].trim() : '';
  const url = typeof body['url'] === 'string' ? body['url'].trim() : '';
  const targetsRaw = body['targets'];

  if (!title && !description) {
    return dashErr(c, 'bad_request', 'title or description is required.', 400);
  }
  if (!Array.isArray(targetsRaw) || targetsRaw.length === 0) {
    return dashErr(c, 'bad_request', 'targets must be a non-empty list.', 400);
  }

  const seen = new Set<string>();
  const cleanTargets: Array<{ guild_id: string; channel_id: string }> = [];
  for (const t of targetsRaw) {
    if (!t || typeof t !== 'object') continue;
    const tt = t as Record<string, unknown>;
    const gid = String(tt['guild_id'] ?? '').trim();
    const cid = String(tt['channel_id'] ?? '').trim();
    if (!/^\d+$/.test(gid) || !/^\d+$/.test(cid)) continue;
    if (seen.has(cid)) continue;
    seen.add(cid);
    cleanTargets.push({ guild_id: gid, channel_id: cid });
  }
  if (cleanTargets.length === 0) {
    return dashErr(c, 'bad_request', 'no valid targets after normalisation.', 400);
  }

  const params = {
    title: title.slice(0, 256),
    description: description.slice(0, 4000),
    color: colorHex,
    footer: footer.slice(0, 2048),
    url,
    targets: cleanTargets,
  };
  const requestedBy = String(session.uid ?? '');

  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);

  try {
    const r = await pool.query<{ id: number | string }>(
      'INSERT INTO pending_bot_actions (action, params, requested_by) ' +
        "VALUES ('broadcast', $1::jsonb, $2) RETURNING id",
      [JSON.stringify(params), requestedBy],
    );
    const newId = Number(r.rows[0]?.id ?? 0);
    log.info(
      { id: newId, targets: cleanTargets.length, by: requestedBy },
      'dashboard broadcast queued',
    );
    return c.json({ id: newId, queued: true, total: cleanTargets.length }, 202);
  } catch (err) {
    log.error({ err }, 'dashboard broadcast enqueue error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

// ---------------------------------------------------------------------------
// Admin: cleanup candidates.
// ---------------------------------------------------------------------------
dashboardRouter.get('/api/dashboard/admin/cleanup/candidates', async (c) => {
  const session = await requireAdmin(c);
  if (session instanceof Response) return session;

  // Authoritative live bot guilds via discord.com.
  const liveRes = await botGet<DiscordRawGuild[]>('/users/@me/guilds');
  if (liveRes.status !== 200 || !Array.isArray(liveRes.body)) {
    return dashErr(c, 'discord_error', `discord ${liveRes.status}`, 502);
  }
  const liveIds = new Set<string>();
  for (const g of liveRes.body) {
    if (g?.id) liveIds.add(String(g.id));
  }

  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);

  try {
    const r = await pool.query<{
      guild_id: string | number;
      preset_count: number | string | null;
      channel_count: number | string | null;
      pager_presets: number | string | null;
      last_change: Date | string | null;
    }>(`
      SELECT guild_id,
             COUNT(*) AS preset_count,
             COUNT(DISTINCT channel_id) AS channel_count,
             SUM(CASE WHEN pager_enabled THEN 1 ELSE 0 END) AS pager_presets,
             MAX(updated_at) AS last_change
        FROM alert_presets
    GROUP BY guild_id
    `);
    const candidates = r.rows
      .filter((row) => !liveIds.has(String(row.guild_id)))
      .map((row) => ({
        guild_id: String(row.guild_id),
        name: '',
        icon_url: '',
        preset_count: Number(row.preset_count ?? 0),
        channel_count: Number(row.channel_count ?? 0),
        pager_presets: Number(row.pager_presets ?? 0),
        last_change: isoOrNull(row.last_change),
      }));
    return c.json({ candidates, bot_guild_count: liveIds.size });
  } catch (err) {
    log.error({ err }, 'dashboard cleanup_candidates error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

// ---------------------------------------------------------------------------
// Admin: bot-actions list + enqueue.
// ---------------------------------------------------------------------------
const ALLOWED_BOT_ACTIONS = new Set(['sync', 'test', 'cleanup', 'broadcast']);

dashboardRouter.get('/api/dashboard/admin/bot-actions', async (c) => {
  const session = await requireAdmin(c);
  if (session instanceof Response) return session;
  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);

  try {
    const r = await pool.query<{
      id: number | string;
      action: string;
      params: Record<string, unknown> | null;
      status: string;
      requested_by: string | null;
      requested_at: Date | string | null;
      claimed_at: Date | string | null;
      completed_at: Date | string | null;
      result: string | null;
      error: string | null;
    }>(
      'SELECT id, action, params, status, requested_by, ' +
        'requested_at, claimed_at, completed_at, result, error ' +
        'FROM pending_bot_actions ORDER BY requested_at DESC LIMIT 30',
    );
    const out = r.rows.map((row) => ({
      id: Number(row.id),
      action: row.action,
      params: row.params ?? {},
      status: row.status,
      requested_by: row.requested_by,
      requested_at: isoOrNull(row.requested_at),
      claimed_at: isoOrNull(row.claimed_at),
      completed_at: isoOrNull(row.completed_at),
      result: row.result,
      error: row.error,
    }));
    return c.json({ actions: out });
  } catch (err) {
    log.error({ err }, 'dashboard bot_actions_list error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

dashboardRouter.post('/api/dashboard/admin/bot-actions', async (c) => {
  const session = await requireAdmin(c);
  if (session instanceof Response) return session;

  const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
  const action = String(body['action'] ?? '').trim().toLowerCase();
  if (!ALLOWED_BOT_ACTIONS.has(action)) {
    return dashErr(c, 'bad_request', `action must be one of ${[...ALLOWED_BOT_ACTIONS].sort()}.`, 400);
  }
  const paramsIn = body['params'];
  if (paramsIn != null && (typeof paramsIn !== 'object' || Array.isArray(paramsIn))) {
    return dashErr(c, 'bad_request', 'params must be an object.', 400);
  }
  let params: Record<string, unknown> = (paramsIn as Record<string, unknown>) ?? {};

  if (action === 'test') {
    const gid = params['guild_id'];
    const cid = params['channel_id'];
    if (!gid || !cid) {
      return dashErr(c, 'bad_request', 'test action needs params.guild_id and params.channel_id.', 400);
    }
    const at = params['alert_type'] || 'all';
    params = { guild_id: String(gid), channel_id: String(cid), alert_type: String(at) };
  } else if (action === 'cleanup') {
    const gids = params['guild_ids'];
    if (!Array.isArray(gids) || gids.length === 0) {
      return dashErr(
        c,
        'bad_request',
        'cleanup action needs params.guild_ids (non-empty array).',
        400,
      );
    }
    params = { guild_ids: gids.map((x) => String(x)) };
  } else {
    params = {};
  }

  const requestedBy = String(session.uid ?? '');
  const pool = await getBotDbPool();
  if (!pool) return dashErr(c, 'dashboard_disabled', 'BOT_DATA_DATABASE_URL is not configured.', 503);

  try {
    const r = await pool.query<{ id: number | string }>(
      'INSERT INTO pending_bot_actions (action, params, requested_by) ' +
        'VALUES ($1, $2::jsonb, $3) RETURNING id',
      [action, JSON.stringify(params), requestedBy],
    );
    const newId = Number(r.rows[0]?.id ?? 0);
    log.info({ action, id: newId, by: requestedBy }, 'dashboard bot-action queued');
    return c.json({ id: newId, action, params }, 201);
  } catch (err) {
    log.error({ err }, 'dashboard bot_action_enqueue error');
    return dashErr(c, 'db_error', String((err as Error).message), 500);
  }
});

// ---------------------------------------------------------------------------
// Admin: source health.
//
// Python reads from _SOURCE_HEALTH / _SOURCE_THRESHOLDS module-level
// state. The Node backend doesn't (yet) have a single canonical source-
// health record; the closest analogue is /api/status. We surface a
// minimal payload here so the admin panel doesn't error out, and punt
// the full parity to a follow-up.
// ---------------------------------------------------------------------------
dashboardRouter.get('/api/dashboard/admin/sources', async (c) => {
  const session = await requireAdmin(c);
  if (session instanceof Response) return session;
  return c.json({
    sources: [],
    server_time: Math.floor(Date.now() / 1000),
    message:
      'Source-health snapshot not yet wired in node backend; see /api/status for live checks.',
  });
});

dashboardRouter.delete('/api/dashboard/admin/sources', async (c) => {
  const session = await requireAdmin(c);
  if (session instanceof Response) return session;
  // No-op until source-health is wired up.
  log.info('dashboard admin/sources DELETE — no-op (source-health not yet ported)');
  return c.json({ ok: true });
});

// ---------------------------------------------------------------------------
// Test-only utilities.
// ---------------------------------------------------------------------------
export function _resetDashboardCachesForTests(): void {
  discordCache.clear();
  presetStatsCache.clear();
  adminOverviewCache = null;
  bcastTargetsCache = null;
}

// Ensure unused-import lint doesn't strip these — they're used inside
// helper functions but TypeScript's verbatimModuleSyntax doesn't flag
// type-only imports.
export type { DashSession };
// Reference DISCORD_API_BASE / getBotToken so dead-code elimination is
// stable across consolidator builds — both are reachable through the
// services we import but not directly used in the router file.
void DISCORD_API_BASE;
void getBotToken;
