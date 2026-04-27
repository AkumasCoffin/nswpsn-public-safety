/**
 * Dashboard router smoke tests.
 *
 * Strategy:
 *   - Mock botDb so getBotDbPool() returns a fake pool we drive per-test.
 *   - Mock discordOauth so the OAuth callback can complete without
 *     hitting discord.com.
 *   - Mock discordApi.botGet so /channels and /roles can return canned
 *     data and so /admin/cleanup can answer with a fake live-guilds set.
 *   - Set DASHBOARD_SESSION_SECRET, BOT_DATA_DATABASE_URL, etc. before
 *     module import (config + helpers read process.env at call time).
 *
 * The tests don't fire any real HTTP and don't hit any real DB.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

// Critical: set env BEFORE module imports — services read process.env.
process.env['DASHBOARD_SESSION_SECRET'] = 'test-dashboard-secret';
process.env['DASHBOARD_ADMIN_IDS'] = 'admin-uid-1';
process.env['DASHBOARD_COOKIE_DOMAIN'] = '';
process.env['BOT_DATA_DATABASE_URL'] = 'postgres://test/test'; // placeholder; pool is mocked
process.env['DISCORD_CLIENT_ID'] = 'test-client-id';
process.env['DISCORD_CLIENT_SECRET'] = 'test-client-secret';
process.env['DISCORD_BOT_TOKEN'] = 'test-bot-token';
process.env['PUBLIC_BASE_URL'] = 'http://api.test';
process.env['DASHBOARD_FRONTEND_URL'] = 'http://dash.test';

interface QueryCall { sql: string; params?: unknown[] }
const queryCalls: QueryCall[] = [];
let queryQueue: Array<{ rows: unknown[]; rowCount?: number } | Error> = [];

const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    queryCalls.push({ sql, ...(params !== undefined ? { params } : {}) });
    const next = queryQueue.shift();
    if (next instanceof Error) throw next;
    return next ?? { rows: [], rowCount: 0 };
  }),
};

vi.mock('../../../src/services/botDb.js', () => ({
  isBotDbConfigured: () => true,
  getBotDbPool: vi.fn(async () => fakePool),
  closeBotDbPool: vi.fn(async () => undefined),
  _setBotDbPoolForTests: vi.fn(),
}));

const exchangeCodeMock = vi.fn();
const fetchIdentityAndGuildsMock = vi.fn();
const refreshUserGuildsMock = vi.fn();
vi.mock('../../../src/services/discordOauth.js', () => ({
  DISCORD_API_BASE: 'https://discord.com/api/v10',
  exchangeCode: (...a: unknown[]) => exchangeCodeMock(...a),
  fetchIdentityAndGuilds: (...a: unknown[]) => fetchIdentityAndGuildsMock(...a),
  refreshUserGuilds: (...a: unknown[]) => refreshUserGuildsMock(...a),
}));

const botGetMock = vi.fn();
const getGuildMetaBulkMock = vi.fn(async () => new Map());
const getAppInstallCountsMock = vi.fn(async () => ({ servers_total: null, user_installs: null }));
vi.mock('../../../src/services/discordApi.js', () => ({
  getBotToken: () => 'test-bot-token',
  botGet: (...a: unknown[]) => botGetMock(...a),
  guildIconUrl: (gid: string, hash: string | null) =>
    hash ? `https://cdn.discordapp.com/icons/${gid}/${hash}.png` : null,
  userAvatarUrl: (uid: string | undefined, hash: string | null) =>
    uid && hash ? `https://cdn.discordapp.com/avatars/${uid}/${hash}.png` : null,
  getGuildMetaBulk: (...a: unknown[]) => getGuildMetaBulkMock(...a),
  getAppInstallCounts: (...a: unknown[]) => getAppInstallCountsMock(...a),
  _resetDiscordApiCachesForTests: vi.fn(),
}));

const { dashboardRouter, _resetDashboardCachesForTests } = await import('../../../src/api/dashboard.js');
const sessionMod = await import('../../../src/services/dashboardSession.js');

function makeApp() {
  const app = new Hono();
  app.route('/', dashboardRouter);
  return app;
}

function makeSessionCookie(uid: string, opts: { admin?: boolean; guilds?: unknown[] } = {}) {
  const sid = sessionMod.newSid();
  const exp = Math.floor(Date.now() / 1000) + 600;
  sessionMod._getSessionsForTests().set(sid, {
    uid: opts.admin ? 'admin-uid-1' : uid,
    username: 'tester',
    avatar: null,
    access_token: 'fake-at',
    token_type: 'Bearer',
    refresh_token: null,
    // Default: one guild where the user has MANAGE_CHANNELS (0x10) and the
    // bot is present (we drive bot guild ids via the alert_presets query
    // mock in each test).
    guilds:
      (opts.guilds as Parameters<typeof sessionMod._getSessionsForTests>[0] extends never
        ? never
        : never) ??
      ([
        { id: '111', name: 'Test Guild', icon: null, permissions: '8', owner: false },
      ] as unknown[]),
    gfresh: Math.floor(Date.now() / 1000),
    iat: Math.floor(Date.now() / 1000),
    exp,
  } as never);
  const secret = sessionMod.getSessionSecret()!;
  const cookie = sessionMod.makeCookie({ sid, exp }, secret);
  return `nswpsn_dash_sess=${cookie}`;
}

beforeEach(() => {
  queryCalls.length = 0;
  queryQueue = [];
  fakePool.query.mockClear();
  exchangeCodeMock.mockReset();
  fetchIdentityAndGuildsMock.mockReset();
  refreshUserGuildsMock.mockReset();
  botGetMock.mockReset();
  sessionMod._resetSessionsForTests();
  sessionMod._markDbReadyForTests(); // skip CREATE TABLE during tests
  sessionMod._resetBotGuildIdsCacheForTests();
  _resetDashboardCachesForTests();
});

// ---------------------------------------------------------------------------
// 503 surface when env unset.
// ---------------------------------------------------------------------------
describe('config / 503 paths', () => {
  it('GET /me 401s without session cookie (env present)', async () => {
    const app = makeApp();
    const res = await app.request('/api/dashboard/me');
    expect(res.status).toBe(401);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe('invalid_session');
  });
});

// ---------------------------------------------------------------------------
// Auth flow.
// ---------------------------------------------------------------------------
describe('OAuth login + callback + logout', () => {
  it('login: 302s to discord.com with the right query string and sets state cookie', async () => {
    const app = makeApp();
    const res = await app.request('/api/dashboard/auth/login?next=/foo');
    expect(res.status).toBe(302);
    const loc = res.headers.get('Location');
    expect(loc).toContain('https://discord.com/api/oauth2/authorize');
    expect(loc).toContain('client_id=test-client-id');
    expect(loc).toContain('scope=identify+guilds');
    const setCookie = res.headers.get('Set-Cookie');
    expect(setCookie).toContain('nswpsn_dash_oauth=');
  });

  it('callback: bad state returns 400', async () => {
    const app = makeApp();
    const res = await app.request(
      '/api/dashboard/auth/callback?code=abc&state=mismatch',
      { headers: { Cookie: 'nswpsn_dash_oauth=garbage' } },
    );
    expect(res.status).toBe(400);
  });

  it('callback: happy path mints a session cookie + redirects to frontend', async () => {
    // Prepare a valid state cookie (would normally be set by /login).
    const secret = sessionMod.getSessionSecret()!;
    const nonce = 'test-nonce';
    const stateCookie = sessionMod.makeCookie(
      { nonce, next: '/dashboard.html', exp: Math.floor(Date.now() / 1000) + 600 },
      secret,
    );

    exchangeCodeMock.mockResolvedValueOnce({
      access_token: 'fake-at',
      token_type: 'Bearer',
      refresh_token: 'fake-rt',
    });
    fetchIdentityAndGuildsMock.mockResolvedValueOnce({
      user: { id: '42', username: 'wendy', avatar: 'a_hash' },
      guilds: [{ id: '111', name: 'Test', icon: null, permissions: '8', owner: false }],
    });
    // For getBotGuildIds (inside callback). Returns a row with guild_id 111.
    queryQueue.push({ rows: [{ guild_id: '111' }] });
    // putSession -> ensureSessionTables runs CREATE TABLE x2 + CREATE
    // INDEX x1 + DELETE + SELECT — five queries before the INSERT. Then
    // INSERT for persistSession + UPDATE for upsertDashboardUser (fire-
    // and-forget). Queue empties safely on rows: [].
    for (let i = 0; i < 10; i += 1) queryQueue.push({ rows: [] });

    const app = makeApp();
    const res = await app.request(
      `/api/dashboard/auth/callback?code=thecode&state=${nonce}`,
      { headers: { Cookie: `nswpsn_dash_oauth=${stateCookie}` } },
    );
    expect(res.status).toBe(302);
    expect(res.headers.get('Location')).toBe('http://dash.test/dashboard.html');
    const setCookies = res.headers.getSetCookie();
    expect(setCookies.some((c) => c.startsWith('nswpsn_dash_sess='))).toBe(true);
  });

  it('logout: clears the session cookie and returns 204', async () => {
    const cookie = makeSessionCookie('user-x');
    const app = makeApp();
    const res = await app.request('/api/dashboard/auth/logout', {
      method: 'POST',
      headers: { Cookie: cookie },
    });
    expect(res.status).toBe(204);
    const sc = res.headers.getSetCookie();
    expect(sc.some((s) => s.includes('nswpsn_dash_sess='))).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// /me and channels/roles.
// ---------------------------------------------------------------------------
describe('/api/dashboard/me', () => {
  it('returns user + guilds shape with the bot-invite URL', async () => {
    const cookie = makeSessionCookie('42');
    // getBotGuildIds query.
    queryQueue.push({ rows: [{ guild_id: '111' }] });

    const app = makeApp();
    const res = await app.request('/api/dashboard/me', {
      headers: { Cookie: cookie },
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      user: { id: string; is_admin: boolean };
      guilds: Array<{ id: string; has_bot: boolean; manage_channels: boolean }>;
      bot_invite_url: string;
    };
    expect(body.user.id).toBe('42');
    expect(body.user.is_admin).toBe(false);
    expect(body.guilds[0]?.id).toBe('111');
    expect(body.guilds[0]?.has_bot).toBe(true);
    expect(body.guilds[0]?.manage_channels).toBe(true); // perms=8 = ADMIN
    expect(body.bot_invite_url).toContain('discord.com/oauth2/authorize');
  });
});

describe('GET /guilds/:id/channels', () => {
  it('proxies through botGet and filters to text channel types', async () => {
    const cookie = makeSessionCookie('42');
    queryQueue.push({ rows: [{ guild_id: '111' }] }); // bot guild ids
    botGetMock.mockResolvedValueOnce({
      status: 200,
      body: [
        { id: '1', name: 'general', type: 0, position: 1, parent_id: null },
        { id: '2', name: 'voice', type: 2, position: 0, parent_id: null }, // dropped
        { id: '3', name: 'announce', type: 5, position: 2, parent_id: '99' },
      ],
      retryAfter: null,
    });
    const app = makeApp();
    const res = await app.request('/api/dashboard/guilds/111/channels', {
      headers: { Cookie: cookie },
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as Array<{ id: string; name: string }>;
    expect(body.map((c) => c.id)).toEqual(['1', '3']);
  });
});

// ---------------------------------------------------------------------------
// Presets CRUD.
// ---------------------------------------------------------------------------
describe('Preset CRUD', () => {
  it('GET /presets returns the list', async () => {
    const cookie = makeSessionCookie('42');
    queryQueue.push({ rows: [{ guild_id: '111' }] });
    queryQueue.push({
      rows: [
        {
          id: 1,
          guild_id: '111',
          channel_id: '999',
          name: 'p1',
          alert_types: ['rfs'],
          pager_enabled: false,
          pager_capcodes: null,
          role_ids: [123],
          enabled: true,
          enabled_ping: true,
          type_overrides: {},
          filters: {},
          created_at: new Date('2026-01-01T00:00:00Z'),
          updated_at: new Date('2026-01-02T00:00:00Z'),
        },
      ],
    });
    const app = makeApp();
    const res = await app.request('/api/dashboard/guilds/111/presets', {
      headers: { Cookie: cookie },
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { presets: Array<{ id: number; name: string }> };
    expect(body.presets.length).toBe(1);
    expect(body.presets[0]?.name).toBe('p1');
  });

  it('POST /presets validates empty preset', async () => {
    const cookie = makeSessionCookie('42');
    queryQueue.push({ rows: [{ guild_id: '111' }] });
    const app = makeApp();
    const res = await app.request('/api/dashboard/guilds/111/presets', {
      method: 'POST',
      headers: { Cookie: cookie, 'Content-Type': 'application/json' },
      body: JSON.stringify({ channel_id: '999', name: 'x' }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe('empty_preset');
  });

  it('DELETE /presets/:id 404s when row missing', async () => {
    const cookie = makeSessionCookie('42');
    queryQueue.push({ rows: [{ guild_id: '111' }] });
    queryQueue.push({ rows: [], rowCount: 0 });
    const app = makeApp();
    const res = await app.request('/api/dashboard/guilds/111/presets/12345', {
      method: 'DELETE',
      headers: { Cookie: cookie },
    });
    expect(res.status).toBe(404);
  });
});

// ---------------------------------------------------------------------------
// Mute state.
// ---------------------------------------------------------------------------
describe('mute state', () => {
  it('GET returns guild + channels rows', async () => {
    const cookie = makeSessionCookie('42');
    queryQueue.push({ rows: [{ guild_id: '111' }] }); // bot guild ids
    queryQueue.push({ rows: [{ enabled: false, enabled_ping: true }] }); // guild
    queryQueue.push({
      rows: [{ channel_id: '777', enabled: true, enabled_ping: false }],
    }); // channels
    const app = makeApp();
    const res = await app.request('/api/dashboard/guilds/111/mute-state', {
      headers: { Cookie: cookie },
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      guild: { enabled: boolean };
      channels: Array<{ channel_id: string; enabled_ping: boolean }>;
    };
    expect(body.guild.enabled).toBe(false);
    expect(body.channels[0]?.channel_id).toBe('777');
    expect(body.channels[0]?.enabled_ping).toBe(false);
  });

  it('PUT /mute-state/guild rejects an empty body', async () => {
    const cookie = makeSessionCookie('42');
    queryQueue.push({ rows: [{ guild_id: '111' }] });
    const app = makeApp();
    const res = await app.request('/api/dashboard/guilds/111/mute-state/guild', {
      method: 'PUT',
      headers: { Cookie: cookie, 'Content-Type': 'application/json' },
      body: '{}',
    });
    expect(res.status).toBe(400);
  });
});

// ---------------------------------------------------------------------------
// Admin gate.
// ---------------------------------------------------------------------------
describe('admin gate', () => {
  it('GET /admin/overview 403s for a non-admin session', async () => {
    const cookie = makeSessionCookie('999'); // not in DASHBOARD_ADMIN_IDS
    const app = makeApp();
    const res = await app.request('/api/dashboard/admin/overview', {
      headers: { Cookie: cookie },
    });
    expect(res.status).toBe(403);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe('not_admin');
  });

  it('GET /admin/overview returns payload for admin uid', async () => {
    const cookie = makeSessionCookie('admin-uid-1', { admin: true });
    // 1: alert_presets aggregates
    queryQueue.push({
      rows: [
        {
          total: 0,
          pager: 0,
          muted: 0,
          guilds_with_presets: 0,
          channels_configured: 0,
        },
      ],
    });
    // 2: mute-state COUNTs
    queryQueue.push({ rows: [{ guilds_muted: 0, channels_muted: 0 }] });
    // 3: type counts
    queryQueue.push({ rows: [] });
    // 4: per-guild aggregates
    queryQueue.push({ rows: [] });
    // 5: dashboard_users SELECT
    queryQueue.push({ rows: [] });
    // 6: dashboard_users count
    queryQueue.push({ rows: [{ n: 0 }] });

    const app = makeApp();
    const res = await app.request('/api/dashboard/admin/overview', {
      headers: { Cookie: cookie },
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      stats: { presets_total: number };
      admin_ids: string[];
    };
    expect(body.stats.presets_total).toBe(0);
    expect(body.admin_ids).toContain('admin-uid-1');
  });

  it('POST /admin/bot-actions rejects unknown action', async () => {
    const cookie = makeSessionCookie('admin-uid-1', { admin: true });
    const app = makeApp();
    const res = await app.request('/api/dashboard/admin/bot-actions', {
      method: 'POST',
      headers: { Cookie: cookie, 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'launch-the-missiles' }),
    });
    expect(res.status).toBe(400);
  });

  it('POST /admin/bot-actions enqueues a sync', async () => {
    const cookie = makeSessionCookie('admin-uid-1', { admin: true });
    queryQueue.push({ rows: [{ id: 99 }] });
    const app = makeApp();
    const res = await app.request('/api/dashboard/admin/bot-actions', {
      method: 'POST',
      headers: { Cookie: cookie, 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'sync' }),
    });
    expect(res.status).toBe(201);
    const body = (await res.json()) as { id: number; action: string };
    expect(body.id).toBe(99);
    expect(body.action).toBe('sync');
  });
});
