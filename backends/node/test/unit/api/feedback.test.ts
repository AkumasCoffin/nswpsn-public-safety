/**
 * Bug reports and suggestions — who may file and from where, the per-reporter
 * cap, status changes and what each one tells (the reporter's bell, the staff
 * alert's event, the public board), linking to tickets, and the bot's signed
 * write routes: static key AND a valid signature, or nothing.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

interface Call { sql: string; params?: unknown[] }
const calls: Call[] = [];
let resultQueue: Array<{ rows: unknown[]; rowCount?: number }> = [];

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
    const r = resultQueue.shift() ?? { rows: [] };
    return { rowCount: r.rows.length, ...r };
  }),
  connect: vi.fn(async () => fakeClient),
};
vi.mock('../../../src/db/pool.js', () => ({ getPool: vi.fn(async () => fakePool) }));

vi.mock('../../../src/services/auth/supabaseJwt.js', () => ({
  requireSupabaseJwt: async (c: { get: (k: string) => unknown; json: (b: unknown, s: number) => unknown }, next: () => Promise<void>) => {
    if (!c.get('userId')) return c.json({ error: 'auth required' }, 401);
    await next();
  },
}));

let handler = false;
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canHandleTickets: vi.fn(async () => handler) };
});

const bells: Array<Record<string, unknown>> = [];
vi.mock('../../../src/services/wireComments.js', () => ({
  notify: vi.fn(async (_pool: unknown, n: Record<string, unknown>) => { bells.push(n); }),
  displayNameMap: vi.fn(async () => new Map<string, string>([['user-1', 'Current Name']])),
}));

const alerts: Array<Record<string, unknown>> = [];
const board: Array<Record<string, unknown>> = [];
vi.mock('../../../src/services/staffNotify.js', () => ({
  notifyStaff: (_pool: unknown, input: Record<string, unknown>) => { alerts.push(input); },
  feedbackBoard: (_pool: unknown, input: Record<string, unknown>) => { board.push(input); },
}));

let linked: { id: string; name: string } | null = null;
vi.mock('../../../src/api/users.js', () => ({
  findUserByDiscordId: vi.fn(async () => linked),
}));

let signingSecret: string | null = 'test-secret';
vi.mock('../../../src/services/botActionSign.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/botActionSign.js')>();
  return { ...actual, getBotActionSecret: vi.fn(() => signingSecret) };
});

const { feedbackRouter } = await import('../../../src/api/feedback.js');
const { signBotAction } = await import('../../../src/services/botActionSign.js');

/** A user app (JWT-shaped caller) or the bot (static key, authClass 'static'). */
function makeApp(opts: { userId?: string | null; userName?: string | null; authClass?: string } = {}) {
  const app = new Hono();
  app.use('*', async (c, next) => {
    if (opts.authClass) (c.set as (k: string, v: unknown) => void)('authClass', opts.authClass);
    if (opts.userId !== null && !opts.authClass) {
      c.set('userId', opts.userId ?? 'user-1');
      if (opts.userName !== null) c.set('userName', opts.userName ?? 'Test User');
    }
    await next();
  });
  app.route('/', feedbackRouter);
  return app;
}

const json = (body: unknown, method = 'POST') => ({
  method,
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(body),
});

/** A signed bot request: the body's signature over (action, 'bot', body). */
function signed(action: string, body: Record<string, unknown>, sig?: string) {
  return {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'X-Bot-Signature': sig ?? signBotAction('test-secret', action, 'bot', body),
    },
    body: JSON.stringify(body),
  };
}

const ROW = (over: Record<string, unknown> = {}) => ({
  id: 12, kind: 'bug', source: 'site', user_id: 'user-1', user_name: 'Test User',
  discord_user_id: null, discord_user_name: null, subject: 'Map tiles blank on Firefox',
  body: 'Zoomed in past 14 and the tiles stopped loading.', status: 'open', status_note: null,
  status_by: null, status_by_name: null, status_at: null, ticket_id: null,
  board_guild_id: null, board_channel_id: null, board_message_id: null,
  created_at: 't', updated_at: 't', ...over,
});

const DISCORD_ROW = (over: Record<string, unknown> = {}) =>
  ROW({ id: 13, source: 'discord', user_id: null, user_name: null, discord_user_id: '123456789012345678', discord_user_name: 'jordan', ...over });

beforeEach(() => {
  calls.length = 0;
  resultQueue = [];
  bells.length = 0;
  alerts.length = 0;
  board.length = 0;
  handler = false;
  linked = null;
  signingSecret = 'test-secret';
});

describe('filing from the site', () => {
  it('401 without a session', async () => {
    expect((await makeApp({ userId: null }).request('/api/feedback', json({ kind: 'bug', subject: 'abc', body: 'x' }))).status).toBe(401);
    expect((await makeApp({ userId: null }).request('/api/feedback/mine')).status).toBe(401);
  });

  it('validates kind, subject and description', async () => {
    const app = makeApp();
    expect((await app.request('/api/feedback', json({ kind: 'ticket', subject: 'abc', body: 'x' }))).status).toBe(400);
    expect((await app.request('/api/feedback', json({ kind: 'bug', subject: 'ab', body: 'x' }))).status).toBe(400);
    expect((await app.request('/api/feedback', json({ kind: 'bug', subject: 'abc', body: '' }))).status).toBe(400);
    expect((await app.request('/api/feedback', json({ kind: 'bug', subject: 'abc', body: 'y'.repeat(5001) }))).status).toBe(400);
    expect(calls).toHaveLength(0);
  });

  it('files under the account, alerts staff privately and posts to the public board', async () => {
    resultQueue = [{ rows: [{ n: '0' }] }, { rows: [ROW()] }];
    const res = await makeApp().request('/api/feedback', json({ kind: 'bug', subject: 'Map tiles blank on Firefox', body: 'Zoomed in past 14 and the tiles stopped loading.' }));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { feedback: Record<string, unknown> };
    expect(body.feedback).toMatchObject({ id: 12, kind: 'bug', kindLabel: 'Bug report', status: 'open', statusLabel: 'Open' });
    // The reporter's view carries no one else's details.
    expect(body.feedback).not.toHaveProperty('userId');
    expect(body.feedback).not.toHaveProperty('boardUrl');

    const ins = calls.find((q) => q.sql.includes('INSERT INTO feedback'));
    expect(ins?.params).toEqual(['bug', 'site', 'user-1', 'Test User', null, null, 'Map tiles blank on Firefox', 'Zoomed in past 14 and the tiles stopped loading.']);
    expect(alerts[0]).toMatchObject({ kind: 'bug_report', event: 'new', ref: '12', title: 'Bug report #12', subtitle: 'Map tiles blank on Firefox' });
    // No status on a new alert: that is what made new tickets render as resolved.
    expect(alerts[0]).not.toHaveProperty('status');
    expect(board[0]).toMatchObject({ event: 'new', kind: 'bug', id: 12, reporter: 'Test User', source: 'site', status: 'open', statusLabel: 'Open' });
    expect(bells).toHaveLength(0);
  });

  it('a suggestion gets its own alert kind and labels', async () => {
    resultQueue = [{ rows: [{ n: '0' }] }, { rows: [ROW({ kind: 'suggestion', subject: 'Dark mode for the map' })] }];
    const res = await makeApp().request('/api/feedback', json({ kind: 'suggestion', subject: 'Dark mode for the map', body: 'Night shifts.' }));
    const body = (await res.json()) as { feedback: Record<string, unknown> };
    expect(body.feedback).toMatchObject({ kindLabel: 'Suggestion', statusLabel: 'Open' });
    expect(alerts[0]).toMatchObject({ kind: 'suggestion', title: 'Suggestion #12' });
  });

  it('429 at five open reports, counted for this account', async () => {
    resultQueue = [{ rows: [{ n: '5' }] }];
    const res = await makeApp().request('/api/feedback', json({ kind: 'bug', subject: 'abc', body: 'x' }));
    expect(res.status).toBe(429);
    const cnt = calls.find((q) => q.sql.includes('COUNT(*)'));
    expect(cnt?.sql).toContain("status IN ('open', 'in_progress')");
    expect(cnt?.params).toEqual(['user-1']);
    expect(calls.some((q) => q.sql.includes('INSERT'))).toBe(false);
  });

  it('GET /mine lists the caller\'s reports with their status labels', async () => {
    resultQueue = [{ rows: [ROW({ status: 'done', status_note: 'Fixed in last night\'s deploy' }), ROW({ id: 9, kind: 'suggestion', status: 'declined' })] }];
    const res = await makeApp().request('/api/feedback/mine');
    const body = (await res.json()) as { feedback: Array<Record<string, unknown>> };
    expect(body.feedback[0]).toMatchObject({ status: 'done', statusLabel: 'Fixed', statusNote: 'Fixed in last night\'s deploy' });
    expect(body.feedback[1]).toMatchObject({ statusLabel: 'Not added' });
    expect(calls[0]?.params).toEqual(['user-1']);
  });
});

describe('staff routes', () => {
  it('403 for a non-handler on every staff route', async () => {
    const app = makeApp();
    expect((await app.request('/api/staff/feedback')).status).toBe(403);
    expect((await app.request('/api/staff/feedback/12')).status).toBe(403);
    expect((await app.request('/api/staff/feedback/12/status', json({ status: 'done' }))).status).toBe(403);
    expect((await app.request('/api/staff/feedback/12/ticket', json({ ticket_id: 1 }))).status).toBe(403);
    expect((await app.request('/api/staff/feedback/12', { method: 'DELETE' })).status).toBe(403);
    expect(calls).toHaveLength(0);
  });

  it('lists with kind and status filters, counts by kind, names overlaid', async () => {
    handler = true;
    resultQueue = [
      { rows: [ROW(), DISCORD_ROW()] },
      { rows: [{ kind: 'bug', status: 'open', n: '2' }, { kind: 'bug', status: 'done', n: '4' }, { kind: 'suggestion', status: 'in_progress', n: '1' }] },
    ];
    const res = await makeApp().request('/api/staff/feedback?kind=bug&status=open');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { feedback: Array<Record<string, unknown>>; counts: Record<string, Record<string, number>> };
    expect(calls[0]?.sql).toContain('kind = $1');
    expect(calls[0]?.sql).toContain('status = $2');
    expect(calls[0]?.params).toEqual(['bug', 'open']);
    expect(body.counts).toEqual({ bug: { open: 2, in_progress: 0, closed: 4 }, suggestion: { open: 0, in_progress: 1, closed: 0 } });
    // Staff see who: the current display name for an account, the Discord name otherwise.
    expect(body.feedback[0]).toMatchObject({ userId: 'user-1', userName: 'Current Name', source: 'site' });
    expect(body.feedback[1]).toMatchObject({ userId: null, userName: 'jordan', discordUserName: 'jordan', source: 'discord' });

    calls.length = 0;
    resultQueue = [{ rows: [] }, { rows: [] }];
    await makeApp().request('/api/staff/feedback?status=closed');
    expect(calls[0]?.sql).toContain("status IN ('done', 'declined', 'duplicate')");
    expect(calls[0]?.params).toEqual([]);

    expect((await makeApp().request('/api/staff/feedback?kind=ticket')).status).toBe(400);
    expect((await makeApp().request('/api/staff/feedback?status=nope')).status).toBe(400);
  });

  it('detail carries the board link and the linked ticket', async () => {
    handler = true;
    resultQueue = [
      { rows: [ROW({ ticket_id: 40, board_guild_id: '1'.repeat(18), board_channel_id: '2'.repeat(18), board_message_id: '3'.repeat(18) })] },
      { rows: [{ id: 40, subject: 'Re: your bug report #12', status: 'awaiting_user' }] },
    ];
    const res = await makeApp().request('/api/staff/feedback/12');
    const body = (await res.json()) as { feedback: Record<string, unknown>; ticket: Record<string, unknown> };
    expect(body.feedback).toMatchObject({ ticketId: 40, boardUrl: `https://discord.com/channels/${'1'.repeat(18)}/${'2'.repeat(18)}/${'3'.repeat(18)}` });
    expect(body.ticket).toMatchObject({ id: 40, status: 'awaiting_user' });

    resultQueue = [{ rows: [] }];
    expect((await makeApp().request('/api/staff/feedback/99')).status).toBe(404);
    expect((await makeApp().request('/api/staff/feedback/x')).status).toBe(400);
  });

  describe('status', () => {
    it('validates the status and the note', async () => {
      handler = true;
      const app = makeApp();
      expect((await app.request('/api/staff/feedback/12/status', json({ status: 'fixed' }))).status).toBe(400);
      expect((await app.request('/api/staff/feedback/12/status', json({ status: 'done', note: 'n'.repeat(1001) }))).status).toBe(400);
      expect(calls).toHaveLength(0);
    });

    it('a terminal status: bell to the site reporter, alert resolved, board replied to', async () => {
      handler = true;
      resultQueue = [{ rows: [ROW({ status: 'done', status_note: 'Deployed tonight', status_by_name: 'Staff Person' })] }];
      const res = await makeApp({ userId: 'staff-1', userName: 'Staff Person' }).request('/api/staff/feedback/12/status', json({ status: 'done', note: 'Deployed tonight' }));
      expect(res.status).toBe(200);
      const upd = calls.find((q) => q.sql.includes('UPDATE feedback'));
      expect(upd?.params).toEqual([12, 'done', 'Deployed tonight', 'staff-1', 'Staff Person']);
      expect(bells[0]).toMatchObject({ userId: 'user-1', type: 'feedback.status', title: 'Your bug report "Map tiles blank on Firefox" was marked Fixed', body: 'Deployed tonight', link: '/contact', exceptUserId: 'staff-1' });
      expect(alerts[0]).toMatchObject({ kind: 'bug_report', event: 'resolved', ref: '12', title: 'Bug report #12 · Fixed', status: 'Fixed', actor: 'Staff Person' });
      expect(board[0]).toMatchObject({ event: 'status', kind: 'bug', id: 12, status: 'done', statusLabel: 'Fixed', note: 'Deployed tonight', actor: 'Staff Person' });
    });

    it('"looking into it" is an update, not a resolution; a Discord-only reporter gets no bell', async () => {
      handler = true;
      resultQueue = [{ rows: [DISCORD_ROW({ status: 'in_progress' })] }];
      await makeApp({ userId: 'staff-1' }).request('/api/staff/feedback/13/status', json({ status: 'in_progress' }));
      expect(bells).toHaveLength(0);
      expect(alerts[0]).toMatchObject({ event: 'update', status: 'Looking into it', title: 'Bug report #13 · Looking into it' });
      expect(board[0]).toMatchObject({ event: 'status', statusLabel: 'Looking into it', note: null });
    });

    it('suggestion labels: Planned / Added / Not added', async () => {
      handler = true;
      for (const [status, label] of [['in_progress', 'Planned'], ['done', 'Added'], ['declined', 'Not added']] as const) {
        alerts.length = 0;
        resultQueue = [{ rows: [ROW({ kind: 'suggestion', status })] }];
        await makeApp({ userId: 'staff-1' }).request('/api/staff/feedback/12/status', json({ status }));
        expect(alerts[0]).toMatchObject({ kind: 'suggestion', status: label });
      }
    });

    it('404 for an unknown report', async () => {
      handler = true;
      resultQueue = [{ rows: [] }];
      expect((await makeApp().request('/api/staff/feedback/77/status', json({ status: 'done' }))).status).toBe(404);
      expect(alerts).toHaveLength(0);
      expect(board).toHaveLength(0);
    });
  });

  describe('tickets', () => {
    it('links an existing ticket, refuses a missing one, unlinks with null', async () => {
      handler = true;
      resultQueue = [{ rows: [{ id: 40 }] }, { rows: [ROW({ ticket_id: 40 })] }];
      const res = await makeApp().request('/api/staff/feedback/12/ticket', json({ ticket_id: 40 }));
      expect(res.status).toBe(200);
      expect(((await res.json()) as { feedback: { ticketId: number } }).feedback.ticketId).toBe(40);
      expect(calls.find((q) => q.sql.includes('SET ticket_id'))?.params).toEqual([12, 40]);

      resultQueue = [{ rows: [] }];
      expect((await makeApp().request('/api/staff/feedback/12/ticket', json({ ticket_id: 41 }))).status).toBe(404);

      calls.length = 0;
      resultQueue = [{ rows: [ROW()] }];
      expect((await makeApp().request('/api/staff/feedback/12/ticket', json({ ticket_id: null }))).status).toBe(200);
      expect(calls[0]?.params).toEqual([12, null]);

      expect((await makeApp().request('/api/staff/feedback/12/ticket', json({ ticket_id: 'abc' }))).status).toBe(400);
    });

    it('opens a ticket at a site reporter with the report quoted, links it, bells them', async () => {
      handler = true;
      resultQueue = [
        { rows: [ROW()] },        // fetch
        { rows: [{ id: 41 }] },   // INSERT support_tickets
        { rows: [] },             // INSERT message
        { rows: [] },             // UPDATE feedback
      ];
      const res = await makeApp({ userId: 'staff-1', userName: 'Staff Person' }).request('/api/staff/feedback/12/ticket/new', json({ body: 'Which Firefox version?' }));
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({ ok: true, ticketId: 41 });
      const t = calls.find((q) => q.sql.includes('INSERT INTO support_tickets'));
      expect(t?.params?.[0]).toBe('user-1');
      expect(t?.params?.[2]).toBe('Re: your bug report #12 — Map tiles blank on Firefox');
      expect(t?.params?.[3]).toBe('bug');
      expect(t?.sql).toContain("'awaiting_user', true");
      const m = calls.find((q) => q.sql.includes('INSERT INTO support_ticket_messages'));
      expect(String(m?.params?.[3])).toContain('> **Bug report #12 · Map tiles blank on Firefox**');
      expect(String(m?.params?.[3])).toContain('> Zoomed in past 14');
      expect(String(m?.params?.[3])).toContain('Which Firefox version?');
      expect(calls.find((q) => q.sql.includes('SET ticket_id'))?.params).toEqual([12, 41]);
      const order = calls.map((q) => q.sql.trim().split(/\s+/)[0]);
      expect(order).toContain('BEGIN');
      expect(order[order.length - 1]).toBe('COMMIT');
      expect(bells[0]).toMatchObject({ userId: 'user-1', type: 'ticket.opened', link: '/contact?t=41' });
    });

    it('409 for a Discord-only reporter, and when already linked', async () => {
      handler = true;
      resultQueue = [{ rows: [DISCORD_ROW()] }];
      const res = await makeApp().request('/api/staff/feedback/13/ticket/new', json({ body: 'hi there' }));
      expect(res.status).toBe(409);
      expect(((await res.json()) as { error: string }).error).toContain('Discord');
      resultQueue = [{ rows: [ROW({ ticket_id: 40 })] }];
      expect((await makeApp().request('/api/staff/feedback/12/ticket/new', json({ body: 'hi there' }))).status).toBe(409);
      expect(calls.some((q) => q.sql.includes('INSERT'))).toBe(false);
    });
  });

  it('delete removes the row and takes the board post down', async () => {
    handler = true;
    resultQueue = [{ rows: [ROW()] }];
    expect((await makeApp().request('/api/staff/feedback/12', { method: 'DELETE' })).status).toBe(200);
    expect(calls[0]?.sql).toContain('DELETE FROM feedback');
    expect(board[0]).toMatchObject({ event: 'delete', kind: 'bug', id: 12 });
    resultQueue = [{ rows: [] }];
    expect((await makeApp().request('/api/staff/feedback/12', { method: 'DELETE' })).status).toBe(404);
  });
});

describe('bot routes (signed)', () => {
  const FILE = { kind: 'bug', discord_user_id: '123456789012345678', discord_user_name: 'jordan', subject: 'Pager page 500s', body: 'Opening /pager gives a 500 since this morning.' };

  it('401 for a logged-in person or a page: the route is the bot\'s', async () => {
    expect((await makeApp().request('/api/discord/feedback', signed('feedback', FILE))).status).toBe(401);
    expect((await makeApp({ authClass: 'browser' }).request('/api/discord/feedback', signed('feedback', FILE))).status).toBe(401);
    expect(calls).toHaveLength(0);
  });

  it('503 when the signing secret is unset — fails closed, unlike the outbox', async () => {
    signingSecret = null;
    expect((await makeApp({ authClass: 'static' }).request('/api/discord/feedback', signed('feedback', FILE))).status).toBe(503);
  });

  it('403 on a missing, mis-shaped or wrong signature, and on a body that was altered', async () => {
    const app = makeApp({ authClass: 'static' });
    expect((await app.request('/api/discord/feedback', { ...signed('feedback', FILE), headers: { 'content-type': 'application/json' } })).status).toBe(403);
    expect((await app.request('/api/discord/feedback', signed('feedback', FILE, 'nope'))).status).toBe(403);
    expect((await app.request('/api/discord/feedback', signed('feedback', FILE, 'f'.repeat(64)))).status).toBe(403);
    // Signed as a different action, or the body changed after signing.
    expect((await app.request('/api/discord/feedback', signed('feedback_board_post', FILE))).status).toBe(403);
    const tampered = signed('feedback', FILE);
    tampered.body = JSON.stringify({ ...FILE, subject: 'Something else' });
    expect((await app.request('/api/discord/feedback', tampered)).status).toBe(403);
    expect(calls).toHaveLength(0);
  });

  it('files under the Discord identity, alerts staff, leaves the board to the bot', async () => {
    resultQueue = [{ rows: [{ n: '0' }] }, { rows: [DISCORD_ROW()] }];
    const res = await makeApp({ authClass: 'static' }).request('/api/discord/feedback', signed('feedback', FILE));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true, feedback: { id: 13, kind: 'bug', statusLabel: 'Open' }, linked: false });
    const cnt = calls.find((q) => q.sql.includes('COUNT(*)'));
    expect(cnt?.sql).toContain('discord_user_id = $1');
    const ins = calls.find((q) => q.sql.includes('INSERT INTO feedback'));
    expect(ins?.params).toEqual(['bug', 'discord', null, null, '123456789012345678', 'jordan', 'Pager page 500s', 'Opening /pager gives a 500 since this morning.']);
    expect(alerts[0]).toMatchObject({ kind: 'bug_report', event: 'new', ref: '13' });
    expect(JSON.stringify(alerts[0])).toContain('jordan · via Discord');
    // The bot posts a Discord report to the board itself (it has the files).
    expect(board).toHaveLength(0);
  });

  it('files under the site account when the Discord is linked, and counts the cap for that account', async () => {
    linked = { id: 'user-1', name: 'Test User' };
    resultQueue = [{ rows: [{ n: '0' }] }, { rows: [DISCORD_ROW({ user_id: 'user-1', user_name: 'Test User' })] }];
    const res = await makeApp({ authClass: 'static' }).request('/api/discord/feedback', signed('feedback', FILE));
    expect(((await res.json()) as { linked: boolean }).linked).toBe(true);
    expect(calls.find((q) => q.sql.includes('COUNT(*)'))?.params).toEqual(['user-1']);
    expect(calls.find((q) => q.sql.includes('INSERT INTO feedback'))?.params?.slice(2, 6)).toEqual(['user-1', 'Test User', '123456789012345678', 'jordan']);
    expect(JSON.stringify(alerts[0])).toContain('linked account');
  });

  it('validates the payload and passes the cap through as 429', async () => {
    const app = makeApp({ authClass: 'static' });
    expect((await app.request('/api/discord/feedback', signed('feedback', { ...FILE, discord_user_id: 'jordan' }))).status).toBe(400);
    expect((await app.request('/api/discord/feedback', signed('feedback', { ...FILE, subject: 'ab' }))).status).toBe(400);
    resultQueue = [{ rows: [{ n: '5' }] }];
    const res = await app.request('/api/discord/feedback', signed('feedback', FILE));
    expect(res.status).toBe(429);
    expect(((await res.json()) as { error: string }).error).toContain('5 open reports');
  });

  it('records the board post; the signed id must match the path', async () => {
    const ids = { id: '13', guild_id: '1'.repeat(18), channel_id: '2'.repeat(18), message_id: '3'.repeat(18) };
    resultQueue = [{ rows: [{}], rowCount: 1 }];
    const res = await makeApp({ authClass: 'static' }).request('/api/discord/feedback/13/board', signed('feedback_board_post', ids));
    expect(res.status).toBe(200);
    expect(calls[0]?.sql).toContain('SET board_guild_id');
    expect(calls[0]?.params).toEqual([13, ids.guild_id, ids.channel_id, ids.message_id]);

    expect((await makeApp({ authClass: 'static' }).request('/api/discord/feedback/14/board', signed('feedback_board_post', ids))).status).toBe(400);
    expect((await makeApp({ authClass: 'static' }).request('/api/discord/feedback/13/board', signed('feedback_board_post', { ...ids, message_id: 'x' }))).status).toBe(400);
    resultQueue = [{ rows: [], rowCount: 0 }];
    expect((await makeApp({ authClass: 'static' }).request('/api/discord/feedback/13/board', signed('feedback_board_post', ids))).status).toBe(404);
  });
});
