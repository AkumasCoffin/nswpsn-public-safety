/**
 * Contact tickets API — auth matrix, clamps, status transitions (incl. the
 * reply-reopens rule), the close claim race, staff-initiated outreach, and the
 * notification matrix (bell vs Discord per action).
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

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => fakePool),
}));

// requireSupabaseJwt → passthrough that 401s when the test injected no user.
vi.mock('../../../src/services/auth/supabaseJwt.js', () => ({
  requireSupabaseJwt: async (c: { get: (k: string) => unknown; json: (b: unknown, s: number) => unknown }, next: () => Promise<void>) => {
    if (!c.get('userId')) return c.json({ error: 'auth required' }, 401);
    await next();
  },
}));

// canHandleTickets is DB-backed — make it switchable per test.
let handler = false;
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canHandleTickets: vi.fn(async () => handler) };
});

const bells: Array<Record<string, unknown>> = [];
vi.mock('../../../src/services/wireComments.js', () => ({
  notify: vi.fn(async (_pool: unknown, n: Record<string, unknown>) => { bells.push(n); }),
  displayNameMap: vi.fn(async () => new Map<string, string>()),
}));

const discord: Array<Record<string, unknown>> = [];
vi.mock('../../../src/services/staffNotify.js', () => ({
  notifyStaff: (_pool: unknown, input: Record<string, unknown>) => { discord.push(input); },
}));

let knownUser: string | null = 'Target User';
vi.mock('../../../src/api/users.js', () => ({
  getUsername: vi.fn(async () => knownUser),
}));

const { ticketsRouter } = await import('../../../src/api/tickets.js');

function makeApp(opts: { userId?: string | null } = {}) {
  const app = new Hono();
  app.use('*', async (c, next) => {
    if (opts.userId !== null) {
      c.set('userId', opts.userId ?? 'user-1');
      c.set('userName', 'Test User');
    }
    await next();
  });
  app.route('/', ticketsRouter);
  return app;
}

const TICKET = (over: Record<string, unknown> = {}) => ({
  id: 7, user_id: 'user-1', user_name: 'Test User', subject: 'Radio keeps dropping',
  category: 'bug', status: 'open', created_by_staff: false,
  closed_by: null, closed_by_name: null, closed_at: null,
  last_message_at: 't', created_at: 't', updated_at: 't', ...over,
});

beforeEach(() => {
  calls.length = 0;
  resultQueue = [];
  bells.length = 0;
  discord.length = 0;
  handler = false;
  knownUser = 'Target User';
});

describe('user routes', () => {
  it('401s without a session', async () => {
    const res = await makeApp({ userId: null }).request('/api/tickets', { method: 'POST', body: '{}' });
    expect(res.status).toBe(401);
  });

  it('rejects bad subject / category / body', async () => {
    const app = makeApp();
    const post = (body: unknown) =>
      app.request('/api/tickets', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
    expect((await post({ subject: 'ab', category: 'bug', body: 'x' })).status).toBe(400);
    expect((await post({ subject: 'x'.repeat(141), category: 'bug', body: 'x' })).status).toBe(400);
    expect((await post({ subject: 'valid subject', category: 'nope', body: 'x' })).status).toBe(400);
    expect((await post({ subject: 'valid subject', category: 'bug', body: '' })).status).toBe(400);
    expect((await post({ subject: 'valid subject', category: 'bug', body: 'y'.repeat(5001) })).status).toBe(400);
  });

  it('429s at the open-ticket cap', async () => {
    resultQueue = [{ rows: [{ n: '5' }] }];
    const res = await makeApp().request('/api/tickets', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ subject: 'valid subject', category: 'bug', body: 'help' }),
    });
    expect(res.status).toBe(429);
    expect(discord).toHaveLength(0);
  });

  it('creates a ticket: txn insert + Discord alert, no bell', async () => {
    resultQueue = [
      { rows: [{ n: '0' }] },      // open count
      { rows: [TICKET()] },        // insert ticket
      { rows: [] },                // insert message
    ];
    const res = await makeApp().request('/api/tickets', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ subject: 'Radio keeps dropping', category: 'bug', body: 'since yesterday' }),
    });
    expect(res.status).toBe(200);
    expect(discord).toHaveLength(1);
    expect(discord[0]).toMatchObject({ kind: 'ticket', event: 'new', ref: '7' });
    expect(bells).toHaveLength(0);
  });

  it('GET /api/tickets/:id is a 404 for strangers, even handlers=false', async () => {
    resultQueue = [{ rows: [TICKET({ user_id: 'someone-else' })] }];
    const res = await makeApp().request('/api/tickets/7');
    expect(res.status).toBe(404);
  });

  it('a LOCKED ticket (closed >72h) refuses user replies with 409', async () => {
    const old = new Date(Date.now() - 73 * 3600 * 1000).toISOString();
    resultQueue = [
      { rows: [TICKET({ status: 'closed', closed_at: old, closed_by_name: 'Owner' })] },
    ];
    const res = await makeApp().request('/api/tickets/7/reply', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ body: 'too late' }),
    });
    expect(res.status).toBe(409);
    expect(discord).toHaveLength(0);
  });

  it('a freshly closed ticket (<72h) still reopens on reply', async () => {
    const recent = new Date(Date.now() - 1 * 3600 * 1000).toISOString();
    resultQueue = [
      { rows: [TICKET({ status: 'closed', closed_at: recent })] },
      { rows: [] }, { rows: [] },
    ];
    const res = await makeApp().request('/api/tickets/7/reply', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ body: 'quick follow-up' }),
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ status: 'open', reopened: true });
  });

  it('a user reply reopens a closed ticket to OPEN and alerts Discord', async () => {
    resultQueue = [
      { rows: [TICKET({ status: 'closed', closed_by: 'o', closed_by_name: 'Owner' })] },
      { rows: [] }, // insert message (client)
      { rows: [] }, // update status (client)
    ];
    const res = await makeApp().request('/api/tickets/7/reply', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ body: 'still broken' }),
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ status: 'open', reopened: true });
    const upd = calls.find((q) => q.sql.includes('UPDATE support_tickets') && q.sql.includes('closed_by = NULL'));
    expect(upd?.params).toContain('open');
    expect(discord[0]).toMatchObject({ kind: 'ticket', event: 'new', ref: '7' });
    expect(String(discord[0]!.title)).toContain('reopened');
  });
});

describe('staff routes', () => {
  it('403s a non-handler', async () => {
    const res = await makeApp().request('/api/staff/tickets');
    expect(res.status).toBe(403);
  });

  it('staff-create validates the target and sets awaiting_user + created_by_staff; bell yes, Discord no', async () => {
    handler = true;
    knownUser = null;
    const app = makeApp({ userId: 'staff-1' });
    const bad = await app.request('/api/staff/tickets', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ user_id: 'ghost', subject: 'About your node', category: 'feeder_node', body: 'hello' }),
    });
    expect(bad.status).toBe(400);

    knownUser = 'Target User';
    resultQueue = [
      { rows: [TICKET({ user_id: 'target-1', status: 'awaiting_user', created_by_staff: true })] },
      { rows: [] },
    ];
    const res = await app.request('/api/staff/tickets', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ user_id: 'target-1', subject: 'About your node', category: 'feeder_node', body: 'hello' }),
    });
    expect(res.status).toBe(200);
    const ins = calls.find((q) => q.sql.includes('INSERT INTO support_tickets'));
    expect(ins?.sql).toContain("'awaiting_user'");
    expect(ins?.sql).toContain('true');
    expect(bells).toHaveLength(1);
    expect(bells[0]).toMatchObject({ userId: 'target-1', type: 'ticket.opened', link: '/contact?t=7' });
    expect(discord).toHaveLength(0);
  });

  it('staff reply sets awaiting_user and bell-notifies the owner with exceptUserId', async () => {
    handler = true;
    resultQueue = [
      { rows: [TICKET()] },
      { rows: [] }, { rows: [] }, // client insert+update
    ];
    const res = await makeApp({ userId: 'staff-1' }).request('/api/staff/tickets/7/reply', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ body: 'on it' }),
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ status: 'awaiting_user' });
    expect(bells[0]).toMatchObject({
      userId: 'user-1', type: 'ticket.reply', link: '/contact?t=7', exceptUserId: 'staff-1',
    });
    expect(discord).toHaveLength(0);
  });

  it('close is an atomic claim: winner notifies both ways, loser gets 409', async () => {
    handler = true;
    const app = makeApp({ userId: 'staff-1' });
    resultQueue = [{ rows: [TICKET({ status: 'closed', closed_by_name: 'Test User' })] }];
    const win = await app.request('/api/staff/tickets/7/close', { method: 'POST' });
    expect(win.status).toBe(200);
    expect(bells[0]).toMatchObject({ userId: 'user-1', type: 'ticket.status' });
    expect(discord[0]).toMatchObject({ kind: 'ticket', event: 'resolved', ref: '7', status: 'closed' });

    resultQueue = [
      { rows: [] },                               // claim misses
      { rows: [TICKET({ status: 'closed' })] },   // ...but the ticket exists
    ];
    const lose = await app.request('/api/staff/tickets/7/close', { method: 'POST' });
    expect(lose.status).toBe(409);
  });

  it('reopen works only from closed', async () => {
    handler = true;
    const app = makeApp({ userId: 'staff-1' });
    resultQueue = [
      { rows: [] },              // claim misses (not closed)
      { rows: [TICKET()] },      // exists
    ];
    expect((await app.request('/api/staff/tickets/7/reopen', { method: 'POST' })).status).toBe(409);

    resultQueue = [{ rows: [TICKET({ status: 'open' })] }];
    const ok = await app.request('/api/staff/tickets/7/reopen', { method: 'POST' });
    expect(ok.status).toBe(200);
    expect(bells[0]).toMatchObject({ type: 'ticket.status' });
    expect(discord).toHaveLength(0);
  });

  it('list returns counts for the badge', async () => {
    handler = true;
    resultQueue = [
      { rows: [TICKET()] },
      { rows: [{ status: 'open', n: '3' }, { status: 'closed', n: '9' }] },
    ];
    const res = await makeApp({ userId: 'staff-1' }).request('/api/staff/tickets?status=open');
    expect(res.status).toBe(200);
    const j = (await res.json()) as { counts: Record<string, number> };
    expect(j.counts).toEqual({ open: 3, awaiting_user: 0, closed: 9 });
  });
});
