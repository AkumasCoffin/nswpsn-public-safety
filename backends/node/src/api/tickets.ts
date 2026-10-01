// Contact tickets — the site's support inbox.
//
// Any logged-in account (requireSupabaseJwt — the base `authed` tier, same as
// wire comments) can open a ticket and converse with staff; the owner and the
// `support` role (canHandleTickets) work the queue from the staff page, and
// can also open a ticket AT a user (outreach). A ticket is subject + category
// + a flat message thread; status tracks whose court the ball is in:
//
//   open          — staff need to act (new ticket, or ANY user reply — a user
//                   reply to a closed ticket lands here, which IS the reopen)
//   awaiting_user — staff replied, or staff opened the ticket at the user
//   closed        — resolved
//
// Notification matrix (the two systems serve different audiences):
//   user creates / replies      -> Discord staff alert (kind 'ticket', its own
//                                  channel) — staff hear about work to do
//   staff creates / replies /
//   closes / reopens            -> in-app bell notify() to the ticket owner,
//                                  linking to /contact?t=<id>
//   staff closes                -> ALSO notifyStaff event 'resolved', which
//                                  EDITS the ticket's Discord message in place
// Staff-initiated creation deliberately sends no Discord alert — staff did it.
//
// No queue, on purpose: single-row Postgres writes in the request, identical
// in shape to agency_data_change and wire_takedowns (see the design note in
// services/wireComments.ts:12 — the Discord outbox pending_bot_actions already
// is the durable queue for the only genuinely async part).
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { config } from '../config.js';
import { requireSupabaseJwt } from '../services/auth/supabaseJwt.js';
import { requireRole, canHandleTickets } from '../services/auth/roles.js';
import { notify, displayNameMap } from '../services/wireComments.js';
import { notifyStaff } from '../services/staffNotify.js';
import { getUsername } from './users.js';

export const ticketsRouter = new Hono();

// The dataset's category vocabulary — slugs in the DB CHECK, labels in the UI.
export const TICKET_CATEGORIES: Record<string, string> = {
  general: 'General question',
  account: 'Account help',
  data_correction: 'Data correction',
  feeder_node: 'Feeder node help',
  bug: 'Bug report',
  other: 'Other',
};

const SUBJECT_MIN = 3;
const SUBJECT_MAX = 140;
const BODY_MAX = 5000;
/** A user may have at most this many non-closed tickets — spam belt. */
const MAX_OPEN_PER_USER = 5;

interface TicketRow {
  id: number;
  user_id: string;
  user_name: string | null;
  subject: string;
  category: string;
  status: string;
  created_by_staff: boolean;
  closed_by: string | null;
  closed_by_name: string | null;
  closed_at: string | null;
  last_message_at: string;
  created_at: string;
  updated_at: string;
  message_count?: string;
}

function currentUserId(c: { get: (k: 'userId') => unknown }): string | undefined {
  const v = c.get('userId');
  return typeof v === 'string' && v.length > 0 ? v : undefined;
}

function currentUserName(c: { get: (k: 'userName') => unknown }): string | null {
  const v = c.get('userName');
  return typeof v === 'string' && v ? v : null;
}

/** Validates a create/reply body. Returns the clean values or an error string. */
function cleanSubject(raw: unknown): string | null {
  const s = typeof raw === 'string' ? raw.trim() : '';
  return s.length >= SUBJECT_MIN && s.length <= SUBJECT_MAX ? s : null;
}
function cleanBody(raw: unknown): string | null {
  const s = typeof raw === 'string' ? raw.trim() : '';
  return s.length >= 1 && s.length <= BODY_MAX ? s : null;
}
function cleanCategory(raw: unknown): string | null {
  const s = typeof raw === 'string' ? raw.trim() : '';
  return s in TICKET_CATEGORIES ? s : null;
}

/** The owner-facing view of a ticket. `closed_by` (the id) stays staff-side. */
function mapTicket(t: TicketRow, nameOverlay?: Map<string, string>) {
  return {
    id: t.id,
    subject: t.subject,
    category: t.category,
    status: t.status,
    createdByStaff: t.created_by_staff,
    closedByName: t.closed_by_name,
    closedAt: t.closed_at,
    lastMessageAt: t.last_message_at,
    createdAt: t.created_at,
    updatedAt: t.updated_at,
    ...(t.message_count !== undefined ? { messageCount: Number(t.message_count) } : {}),
    ...(nameOverlay ? { userId: t.user_id, userName: nameOverlay.get(t.user_id) ?? t.user_name } : {}),
  };
}

type Pool = NonNullable<Awaited<ReturnType<typeof getPool>>>;

async function fetchTicket(pool: Pool, id: number): Promise<TicketRow | null> {
  const r = await pool.query<TicketRow>('SELECT * FROM support_tickets WHERE id = $1', [id]);
  return r.rows[0] ?? null;
}

async function fetchMessages(pool: Pool, ticketId: number) {
  const r = await pool.query<{
    id: number; author_id: string; author_name: string | null; is_staff: boolean;
    body: string; created_at: string;
  }>(
    'SELECT id, author_id, author_name, is_staff, body, created_at FROM support_ticket_messages WHERE ticket_id = $1 ORDER BY created_at, id',
    [ticketId],
  );
  // Current-name overlay: write-time author_name snapshots go stale.
  const names = await displayNameMap(pool, r.rows.map((m) => m.author_id));
  return r.rows.map((m) => ({
    id: m.id,
    authorName: names.get(m.author_id) ?? m.author_name ?? 'User',
    isStaff: m.is_staff,
    body: m.body,
    createdAt: m.created_at,
  }));
}

/** INSERT a message + flip the ticket's status, in one transaction. */
async function addMessage(
  pool: Pool,
  ticketId: number,
  author: { id: string; name: string | null; isStaff: boolean },
  body: string,
  newStatus: 'open' | 'awaiting_user',
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      'INSERT INTO support_ticket_messages (ticket_id, author_id, author_name, is_staff, body) VALUES ($1, $2, $3, $4, $5)',
      [ticketId, author.id, author.name, author.isStaff, body],
    );
    // A reply also un-closes the ticket: user reply -> open (staff's court),
    // staff reply -> awaiting_user. Clearing closed_* keeps the row honest.
    await client.query(
      `UPDATE support_tickets
          SET status = $2, closed_by = NULL, closed_by_name = NULL, closed_at = NULL,
              last_message_at = now(), updated_at = now()
        WHERE id = $1`,
      [ticketId, newStatus],
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

/** Short body excerpt for the Discord embed field. */
function excerpt(body: string): string {
  return body.length > 300 ? body.slice(0, 297) + '…' : body;
}

// ---------------------------------------------------------------------------
// User endpoints — any logged-in account
// ---------------------------------------------------------------------------

ticketsRouter.post('/api/tickets', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const userName = currentUserName(c);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const subject = cleanSubject(raw?.subject);
  const category = cleanCategory(raw?.category);
  const body = cleanBody(raw?.body);
  if (!subject) return c.json({ error: `subject must be ${SUBJECT_MIN}-${SUBJECT_MAX} characters` }, 400);
  if (!category) return c.json({ error: 'unknown category' }, 400);
  if (!body) return c.json({ error: `message must be 1-${BODY_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const open = await pool.query<{ n: string }>(
      "SELECT COUNT(*)::text AS n FROM support_tickets WHERE user_id = $1 AND status <> 'closed'",
      [userId],
    );
    if (Number(open.rows[0]?.n ?? 0) >= MAX_OPEN_PER_USER) {
      return c.json({ error: `you already have ${MAX_OPEN_PER_USER} open tickets — please wait for a reply` }, 429);
    }

    const client = await pool.connect();
    let ticket: TicketRow;
    try {
      await client.query('BEGIN');
      const ins = await client.query<TicketRow>(
        `INSERT INTO support_tickets (user_id, user_name, subject, category) VALUES ($1, $2, $3, $4) RETURNING *`,
        [userId, userName, subject, category],
      );
      ticket = ins.rows[0]!;
      await client.query(
        'INSERT INTO support_ticket_messages (ticket_id, author_id, author_name, is_staff, body) VALUES ($1, $2, $3, false, $4)',
        [ticket.id, userId, userName, body],
      );
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }

    notifyStaff(pool, {
      kind: 'ticket',
      event: 'new',
      ref: String(ticket.id),
      title: `Ticket #${ticket.id} · ${TICKET_CATEGORIES[category]}`,
      subtitle: subject,
      status: 'open',
      fields: [
        { name: 'From', value: userName ?? userId.slice(0, 8), inline: true },
        { name: 'Message', value: excerpt(body) },
      ],
    });
    log.info({ ticketId: ticket.id, userId, category }, 'support ticket opened');
    return c.json({ ok: true, ticket: mapTicket(ticket) });
  } catch (err) {
    log.error({ err, userId }, 'ticket create failed');
    return c.json({ error: 'Failed to create ticket' }, 500);
  }
});

ticketsRouter.get('/api/tickets', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const r = await pool.query<TicketRow>(
      `SELECT t.*, (SELECT COUNT(*)::text FROM support_ticket_messages m WHERE m.ticket_id = t.id) AS message_count
         FROM support_tickets t
        WHERE t.user_id = $1
        ORDER BY t.updated_at DESC
        LIMIT 100`,
      [userId],
    );
    return c.json({ tickets: r.rows.map((t) => mapTicket(t)) });
  } catch (err) {
    log.error({ err, userId }, 'ticket list failed');
    return c.json({ error: 'Failed to list tickets' }, 500);
  }
});

ticketsRouter.get('/api/tickets/:id', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id) || id <= 0) return c.json({ error: 'invalid id' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const ticket = await fetchTicket(pool, id);
    // 404 for strangers (not 403): the ticket's existence is itself private.
    if (!ticket) return c.json({ error: 'ticket not found' }, 404);
    const isHandler = ticket.user_id === userId ? false : await canHandleTickets(userId);
    if (ticket.user_id !== userId && !isHandler) return c.json({ error: 'ticket not found' }, 404);
    const messages = await fetchMessages(pool, id);
    const names = isHandler ? await displayNameMap(pool, [ticket.user_id]) : undefined;
    return c.json({ ticket: mapTicket(ticket, names), messages });
  } catch (err) {
    log.error({ err, id }, 'ticket fetch failed');
    return c.json({ error: 'Failed to fetch ticket' }, 500);
  }
});

ticketsRouter.post('/api/tickets/:id/reply', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const userName = currentUserName(c);
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id) || id <= 0) return c.json({ error: 'invalid id' }, 400);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const body = cleanBody(raw?.body);
  if (!body) return c.json({ error: `message must be 1-${BODY_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const ticket = await fetchTicket(pool, id);
    if (!ticket || ticket.user_id !== userId) return c.json({ error: 'ticket not found' }, 404);

    const reopened = ticket.status === 'closed';
    await addMessage(pool, id, { id: userId, name: userName, isStaff: false }, body, 'open');

    notifyStaff(pool, {
      kind: 'ticket',
      event: 'new',
      ref: String(ticket.id),
      title: `Ticket #${ticket.id} · user replied${reopened ? ' (reopened)' : ''}`,
      subtitle: ticket.subject,
      status: 'open',
      fields: [
        { name: 'From', value: userName ?? userId.slice(0, 8), inline: true },
        { name: 'Message', value: excerpt(body) },
      ],
    });
    return c.json({ ok: true, status: 'open', reopened });
  } catch (err) {
    log.error({ err, id }, 'ticket reply failed');
    return c.json({ error: 'Failed to reply' }, 500);
  }
});

// ---------------------------------------------------------------------------
// Staff endpoints — owner + support
// ---------------------------------------------------------------------------

ticketsRouter.get('/api/staff/tickets', requireRole(canHandleTickets), async (c) => {
  const status = c.req.query('status') ?? 'open';
  if (!['open', 'awaiting_user', 'closed', 'all'].includes(status)) {
    return c.json({ error: 'invalid status filter' }, 400);
  }
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const where = status === 'all' ? '' : 'WHERE t.status = $1';
    const params = status === 'all' ? [] : [status];
    const r = await pool.query<TicketRow>(
      `SELECT t.*, (SELECT COUNT(*)::text FROM support_ticket_messages m WHERE m.ticket_id = t.id) AS message_count
         FROM support_tickets t ${where}
        ORDER BY t.updated_at DESC
        LIMIT 200`,
      params,
    );
    const counts = await pool.query<{ status: string; n: string }>(
      'SELECT status, COUNT(*)::text AS n FROM support_tickets GROUP BY status',
    );
    const countMap: Record<string, number> = { open: 0, awaiting_user: 0, closed: 0 };
    for (const row of counts.rows) countMap[row.status] = Number(row.n);
    const names = await displayNameMap(pool, r.rows.map((t) => t.user_id));
    return c.json({ tickets: r.rows.map((t) => mapTicket(t, names)), counts: countMap });
  } catch (err) {
    log.error({ err }, 'staff ticket list failed');
    return c.json({ error: 'Failed to list tickets' }, 500);
  }
});

// Staff-initiated outreach: open a ticket AT a user. The target becomes the
// ticket owner (it appears on their /contact exactly like one they opened),
// the first message is staff's, and the ball starts in the USER's court.
// No Discord alert — staff themselves did this.
ticketsRouter.post('/api/staff/tickets', requireRole(canHandleTickets), async (c) => {
  const actorId = currentUserId(c)!;
  const actorName = currentUserName(c);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const targetId = typeof raw?.user_id === 'string' ? raw.user_id.trim() : '';
  const subject = cleanSubject(raw?.subject);
  const category = cleanCategory(raw?.category);
  const body = cleanBody(raw?.body);
  if (!targetId) return c.json({ error: 'user_id required' }, 400);
  if (!subject) return c.json({ error: `subject must be ${SUBJECT_MIN}-${SUBJECT_MAX} characters` }, 400);
  if (!category) return c.json({ error: 'unknown category' }, 400);
  if (!body) return c.json({ error: `message must be 1-${BODY_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const targetName = await getUsername(targetId);
    if (targetName === null) return c.json({ error: 'unknown user' }, 400);

    const client = await pool.connect();
    let ticket: TicketRow;
    try {
      await client.query('BEGIN');
      const ins = await client.query<TicketRow>(
        `INSERT INTO support_tickets (user_id, user_name, subject, category, status, created_by_staff)
         VALUES ($1, $2, $3, $4, 'awaiting_user', true) RETURNING *`,
        [targetId, targetName, subject, category],
      );
      ticket = ins.rows[0]!;
      await client.query(
        'INSERT INTO support_ticket_messages (ticket_id, author_id, author_name, is_staff, body) VALUES ($1, $2, $3, true, $4)',
        [ticket.id, actorId, actorName, body],
      );
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }

    notify(pool, {
      userId: targetId,
      type: 'ticket.opened',
      title: `Staff opened a ticket for you: "${subject}"`,
      body: excerpt(body),
      link: `/contact?t=${ticket.id}`,
    }).catch(() => undefined);
    log.info({ ticketId: ticket.id, targetId, by: actorId }, 'staff-initiated ticket opened');
    return c.json({ ok: true, ticket: mapTicket(ticket, new Map([[targetId, targetName]])) });
  } catch (err) {
    log.error({ err, targetId }, 'staff ticket create failed');
    return c.json({ error: 'Failed to create ticket' }, 500);
  }
});

ticketsRouter.post('/api/staff/tickets/:id/reply', requireRole(canHandleTickets), async (c) => {
  const actorId = currentUserId(c)!;
  const actorName = currentUserName(c);
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id) || id <= 0) return c.json({ error: 'invalid id' }, 400);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const body = cleanBody(raw?.body);
  if (!body) return c.json({ error: `message must be 1-${BODY_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const ticket = await fetchTicket(pool, id);
    if (!ticket) return c.json({ error: 'ticket not found' }, 404);

    await addMessage(pool, id, { id: actorId, name: actorName, isStaff: true }, body, 'awaiting_user');

    notify(pool, {
      userId: ticket.user_id,
      type: 'ticket.reply',
      title: `Reply to your ticket: "${ticket.subject}"`,
      body: excerpt(body),
      link: `/contact?t=${ticket.id}`,
      exceptUserId: actorId, // an owner replying to their own ticket shouldn't self-ring
    }).catch(() => undefined);
    return c.json({ ok: true, status: 'awaiting_user' });
  } catch (err) {
    log.error({ err, id }, 'staff ticket reply failed');
    return c.json({ error: 'Failed to reply' }, 500);
  }
});

ticketsRouter.post('/api/staff/tickets/:id/close', requireRole(canHandleTickets), async (c) => {
  const actorId = currentUserId(c)!;
  const actorName = currentUserName(c) ?? actorId.slice(0, 8);
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id) || id <= 0) return c.json({ error: 'invalid id' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    // Atomic claim (the agency.ts idiom): a lost race means someone else
    // already closed it — 409, not a silent double-close.
    const r = await pool.query<TicketRow>(
      `UPDATE support_tickets
          SET status = 'closed', closed_by = $2, closed_by_name = $3, closed_at = now(), updated_at = now()
        WHERE id = $1 AND status <> 'closed'
        RETURNING *`,
      [id, actorId, actorName],
    );
    if (r.rowCount === 0) {
      const exists = await fetchTicket(pool, id);
      return exists ? c.json({ error: 'already closed' }, 409) : c.json({ error: 'ticket not found' }, 404);
    }
    const ticket = r.rows[0]!;

    notify(pool, {
      userId: ticket.user_id,
      type: 'ticket.status',
      title: `Your ticket "${ticket.subject}" was closed`,
      link: `/contact?t=${ticket.id}`,
      exceptUserId: actorId,
    }).catch(() => undefined);
    // 'resolved' EDITS the ticket's Discord message in place rather than
    // posting a new one (the bot keeps a kind+ref -> message map).
    notifyStaff(pool, {
      kind: 'ticket',
      event: 'resolved',
      ref: String(ticket.id),
      title: `Ticket #${ticket.id} · closed`,
      subtitle: ticket.subject,
      status: 'closed',
      actor: actorName,
    });
    return c.json({ ok: true, status: 'closed' });
  } catch (err) {
    log.error({ err, id }, 'ticket close failed');
    return c.json({ error: 'Failed to close ticket' }, 500);
  }
});

ticketsRouter.post('/api/staff/tickets/:id/reopen', requireRole(canHandleTickets), async (c) => {
  const actorId = currentUserId(c)!;
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id) || id <= 0) return c.json({ error: 'invalid id' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const r = await pool.query<TicketRow>(
      `UPDATE support_tickets
          SET status = 'open', closed_by = NULL, closed_by_name = NULL, closed_at = NULL, updated_at = now()
        WHERE id = $1 AND status = 'closed'
        RETURNING *`,
      [id],
    );
    if (r.rowCount === 0) {
      const exists = await fetchTicket(pool, id);
      return exists ? c.json({ error: 'not closed' }, 409) : c.json({ error: 'ticket not found' }, 404);
    }
    const ticket = r.rows[0]!;
    notify(pool, {
      userId: ticket.user_id,
      type: 'ticket.status',
      title: `Your ticket "${ticket.subject}" was reopened`,
      link: `/contact?t=${ticket.id}`,
      exceptUserId: actorId,
    }).catch(() => undefined);
    return c.json({ ok: true, status: 'open' });
  } catch (err) {
    log.error({ err, id }, 'ticket reopen failed');
    return c.json({ error: 'Failed to reopen ticket' }, 500);
  }
});

// User picker for staff-initiated tickets. Its own lean endpoint because
// /api/users is owner|staff-gated (canManageUsers) — a support-only member
// cannot call it, and the picker needs only id/name/email of matches.
ticketsRouter.get('/api/staff/tickets/user-search', requireRole(canHandleTickets), async (c) => {
  const q = (c.req.query('q') ?? '').trim().toLowerCase();
  if (q.length < 2) return c.json({ users: [] });
  if (!config.SUPABASE_URL || !config.SUPABASE_SERVICE_ROLE_KEY) {
    return c.json({ error: 'user lookup not configured' }, 503);
  }
  try {
    const res = await fetch(`${config.SUPABASE_URL}/auth/v1/admin/users?per_page=1000`, {
      headers: {
        apikey: config.SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${config.SUPABASE_SERVICE_ROLE_KEY}`,
      },
      signal: AbortSignal.timeout(15_000),
    });
    if (!res.ok) return c.json({ error: 'user lookup failed' }, 502);
    const data = (await res.json()) as {
      users?: Array<{ id?: string; email?: string; user_metadata?: Record<string, unknown> }>;
    };
    const str = (v: unknown): string | null => (typeof v === 'string' && v.trim() !== '' ? v.trim() : null);
    const users = (data.users ?? [])
      .map((u) => {
        const md = u.user_metadata ?? {};
        const username =
          str(md['username']) ?? str(md['display_name']) ?? str(md['full_name']) ?? str(md['name']) ??
          (u.email ? u.email.split('@')[0]! : null);
        return { id: u.id ?? '', username: username ?? '', email: u.email ?? '' };
      })
      .filter((u) => u.id && (u.username.toLowerCase().includes(q) || u.email.toLowerCase().includes(q)))
      .slice(0, 20);
    return c.json({ users });
  } catch (err) {
    log.error({ err }, 'ticket user-search failed');
    return c.json({ error: 'user lookup failed' }, 502);
  }
});
