/**
 * Manual notifications API — the auth gate, the input limits, the per-sender
 * ceiling, and the link allowlist.
 *
 * The link matters more than its size suggests: the bell renders it straight
 * into an href and escapes HTML entities only, so a `javascript:` URL would be
 * stored XSS delivered to every recipient of a notice.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
let calls: Call[] = [];
let resultQueue: Array<{ rows: unknown[] }> = [];

const fakeClient = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    if (/^(BEGIN|COMMIT|ROLLBACK)/.test(sql)) return { rows: [] };
    return resultQueue.shift() ?? { rows: [] };
  }),
  release: vi.fn(),
};
const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return resultQueue.shift() ?? { rows: [] };
  }),
  connect: vi.fn(async () => fakeClient),
};
vi.mock('../../../src/db/pool.js', () => ({ getPool: vi.fn(async () => fakePool) }));

let sender = true;
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canSendNotices: vi.fn(async () => sender) };
});

// The people picker reads the account directory, which needs these two set to
// do anything at all. Everything else about the config stays real.
vi.mock('../../../src/config.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/config.js')>();
  return {
    ...actual,
    config: {
      ...actual.config,
      SUPABASE_URL: 'https://supa.test',
      SUPABASE_SERVICE_ROLE_KEY: 'svc-key',
    },
  };
});

const { staffNoticesRouter, safeNoticeLink, _resetNoticeRateLimit } =
  await import('../../../src/api/staffNotices.js');

type DirectoryUser = { id: string; email?: string; user_metadata?: Record<string, unknown> };

/** Answer the next account-directory call with these accounts. */
function withDirectory(users: DirectoryUser[]) {
  vi.stubGlobal('fetch', vi.fn(async () => new Response(
    JSON.stringify({ users }),
    { status: 200, headers: { 'Content-Type': 'application/json' } },
  )));
}

function app(userId: string | null = 'staff-1') {
  const a = new Hono();
  a.use('*', async (c, next) => {
    if (userId) {
      c.set('userId', userId);
      c.set('userName', 'Staff One');
    }
    await next();
  });
  a.route('/', staffNoticesRouter);
  return a;
}

function post(body: unknown, userId: string | null = 'staff-1') {
  return app(userId).request('/api/staff/notices', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

/** A send that resolves to two recipients and returns notice id 1. */
function armSend() {
  resultQueue = [{ rows: [{ user_id: 'a' }, { user_id: 'b' }] }, { rows: [{ id: '1' }] }];
}

const valid = { audience: 'all' as const, title: 'Maintenance', body: 'Sunday 2am.' };

beforeEach(() => {
  calls = [];
  resultQueue = [];
  sender = true;
  _resetNoticeRateLimit();
  fakePool.query.mockClear();
  fakeClient.query.mockClear();
  vi.unstubAllGlobals();
});

describe('safeNoticeLink', () => {
  it('accepts a path on this site and an https address', () => {
    expect(safeNoticeLink('/feeder')).toBe('/feeder');
    expect(safeNoticeLink('  /contact?t=3  ')).toBe('/contact?t=3');
    expect(safeNoticeLink('https://nswpsn.forcequit.xyz/wire')).toBe('https://nswpsn.forcequit.xyz/wire');
  });

  it('refuses every scheme that could run or mislead', () => {
    // The bell puts this in an href; entity-escaping does nothing to these.
    for (const bad of [
      'javascript:alert(1)',
      'JavaScript:alert(1)',
      ' javascript:alert(1)',
      'data:text/html,<script>alert(1)</script>',
      'vbscript:msgbox(1)',
      'file:///etc/passwd',
      'http://insecure.example',          // downgrade
      '//evil.example/looks-relative',    // protocol-relative, looks like a path
      'not a url',
    ]) {
      expect(safeNoticeLink(bad), bad).toBeNull();
    }
  });

  it('treats blank as no link, and refuses a huge one', () => {
    expect(safeNoticeLink('')).toBeNull();
    expect(safeNoticeLink('   ')).toBeNull();
    expect(safeNoticeLink('/' + 'x'.repeat(600))).toBeNull();
  });
});

describe('POST /api/staff/notices', () => {
  it('sends to everyone and reports the count', async () => {
    armSend();
    const res = await post(valid);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true, noticeId: '1', recipients: 2 });
    const notice = calls.find((c) => c.sql.includes('INSERT INTO staff_notices'));
    expect(notice?.params?.[0]).toBe('staff-1');
    expect(notice?.params?.[1]).toBe('Staff One');
  });

  it('refuses anyone without the role', async () => {
    sender = false;
    expect((await post(valid)).status).toBe(403);
    expect(calls).toHaveLength(0);
  });

  it('refuses an unauthenticated caller', async () => {
    expect((await post(valid, null)).status).toBe(401);
  });

  it('refuses an audience that resolves to nobody', async () => {
    // Every time, this is a mistake in the picking — not a send of nothing.
    resultQueue = [{ rows: [] }];
    const res = await post(valid);
    expect(res.status).toBe(400);
    expect((await res.json()).error).toMatch(/nobody in it/);
    expect(calls.some((c) => c.sql.includes('INSERT INTO staff_notices'))).toBe(false);
  });

  it('holds the line on title and message', async () => {
    expect((await post({ ...valid, title: '' })).status).toBe(400);
    expect((await post({ ...valid, title: 'x'.repeat(121) })).status).toBe(400);
    expect((await post({ ...valid, body: '' })).status).toBe(400);
    expect((await post({ ...valid, body: 'x'.repeat(2001) })).status).toBe(400);
    expect((await post({ ...valid, audience: 'everyone' })).status).toBe(400);
  });

  it('refuses a dangerous link before anything is written', async () => {
    const res = await post({ ...valid, link: 'javascript:alert(1)' });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toMatch(/https/);
    expect(calls).toHaveLength(0);
  });

  it('keeps a good link', async () => {
    armSend();
    await post({ ...valid, link: '/feeder' });
    expect(calls.find((c) => c.sql.includes('INSERT INTO notifications'))?.params?.[4]).toBe('/feeder');
  });

  it('refuses an unknown role, and accepts a known one', async () => {
    expect((await post({ ...valid, audience: 'role', role: 'wizard' })).status).toBe(400);
    armSend();
    expect((await post({ ...valid, audience: 'role', role: 'feeder:radio' })).status).toBe(200);
  });

  it('refuses a people-pick with nobody in it, and a pick that is too big', async () => {
    expect((await post({ ...valid, audience: 'users', userIds: [] })).status).toBe(400);
    const tooMany = Array.from({ length: 201 }, (_, i) => `u${i}`);
    expect((await post({ ...valid, audience: 'users', userIds: tooMany })).status).toBe(400);
  });

  it('stops a sender who is sending in a loop', async () => {
    // There is no unsend, so the ceiling is worth more than the flexibility.
    for (let i = 0; i < 10; i++) {
      armSend();
      expect((await post(valid)).status).toBe(200);
    }
    armSend();
    const res = await post(valid);
    expect(res.status).toBe(429);
    // A different sender is unaffected.
    armSend();
    expect((await post(valid, 'staff-2')).status).toBe(200);
  });
});

describe('GET /api/staff/notices', () => {
  it('returns what was sent, newest first', async () => {
    resultQueue = [{ rows: [{
      id: '2', sent_by: 'staff-1', sent_by_name: 'Staff One', audience: 'role',
      target_role: 'feeder:radio', recipients: 5, title: 'Hi', body: 'There',
      link: null, created_at: new Date('2026-10-05T01:00:00Z'),
    }] }];
    const res = await app().request('/api/staff/notices');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.notices).toHaveLength(1);
    expect(body.notices[0]).toMatchObject({ audience: 'role', targetRole: 'feeder:radio', recipients: 5 });
  });

  it('is gated too', async () => {
    sender = false;
    expect((await app().request('/api/staff/notices')).status).toBe(403);
  });
});

describe('GET /api/staff/notices/targets', () => {
  it('reports how many each audience reaches', async () => {
    resultQueue = [{ rows: [
      { role: 'authed', user_id: 'a' }, { role: 'authed', user_id: 'b' },
      { role: 'feeder:radio', user_id: 'a' },
    ] }];
    const body = await (await app().request('/api/staff/notices/targets')).json();
    expect(body).toEqual({ all: 2, byRole: [{ role: 'feeder:radio', count: 1 }] });
  });
});

describe('GET /api/staff/notices/user-search', () => {
  it('says nothing for a query too short to mean anything', async () => {
    const body = await (await app().request('/api/staff/notices/user-search?q=a')).json();
    expect(body).toEqual({ users: [] });
  });

  it('is gated', async () => {
    sender = false;
    expect((await app().request('/api/staff/notices/user-search?q=abc')).status).toBe(403);
  });

  it('carries each match a face, and null for whoever has none', async () => {
    // Picking out of a list of near-identical usernames is slow; a photo is
    // what makes the dropdown scannable, so the search has to resolve it.
    withDirectory([
      { id: 'u-1', email: 'alice@example.com', user_metadata: { username: 'alice' } },
      { id: 'u-2', email: 'alicia@example.com', user_metadata: { username: 'alicia' } },
    ]);
    resultQueue = [{ rows: [
      { user_id: 'u-1', avatar_key: null, discord_avatar_url: 'https://cdn.discordapp.com/a.png' },
    ] }];

    const body = await (await app().request('/api/staff/notices/user-search?q=ali')).json() as
      { users: Array<{ id: string; username: string; avatar: string | null }> };

    expect(body.users.map((u) => [u.id, u.avatar])).toEqual([
      ['u-1', 'https://cdn.discordapp.com/a.png'],
      ['u-2', null],
    ]);
    expect(calls.some((x) => x.sql.includes('FROM user_profiles'))).toBe(true);
  });

  it('still returns the matches when the avatar lookup falls over', async () => {
    // A face is decoration. Losing it must not cost the staff member the
    // search they are mid-way through typing.
    withDirectory([{ id: 'u-1', email: 'alice@example.com', user_metadata: { username: 'alice' } }]);
    fakePool.query.mockImplementationOnce(async () => { throw new Error('no profiles table'); });

    const res = await app().request('/api/staff/notices/user-search?q=ali');
    expect(res.status).toBe(200);
    const body = await res.json() as { users: Array<{ id: string; avatar: string | null }> };
    expect(body.users).toEqual([
      { id: 'u-1', username: 'alice', email: 'alice@example.com', avatar: null },
    ]);
  });
});
