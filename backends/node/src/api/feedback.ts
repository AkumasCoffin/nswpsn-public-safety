/**
 * Bug reports and suggestions — the contact page's other two types.
 *
 * Not tickets. A ticket (tickets.ts) is a private conversation; a report is
 * the report, its description and a STATUS staff set, and it is public:
 * every one is posted to a Discord board channel with its status, and each
 * status change is relayed under that post (services/staffNotify.ts,
 * feedbackBoard). When a report needs a back-and-forth, staff link it to a
 * ticket — open one at the reporter, or link one that exists — and the talk
 * happens there, privately.
 *
 * Two ways in:
 *   site     a logged-in account on /contact (POST /api/feedback)
 *   discord  a message in an intake channel, filed by the bot
 *            (POST /api/discord/feedback, signed — see auth/botSigned.ts).
 *            Any member of the server; if their Discord is linked to a site
 *            account the report is filed under it too, so it shows on their
 *            contact page and they get the bell on a status change.
 *
 *   User (requireSupabaseJwt):
 *     POST /api/feedback                      {kind, subject, body}
 *     GET  /api/feedback/mine
 *   Staff (owner | support — canHandleTickets, the same people as tickets):
 *     GET  /api/staff/feedback?kind=&status=
 *     GET  /api/staff/feedback/:id
 *     POST /api/staff/feedback/:id/status     {status, note?}
 *     POST /api/staff/feedback/:id/ticket     {ticket_id | null}   link / unlink
 *     POST /api/staff/feedback/:id/ticket/new {body}               open a ticket at the reporter
 *     DELETE /api/staff/feedback/:id                               spam
 *   Bot (static key + X-Bot-Signature):
 *     POST /api/discord/feedback              {kind, discord_user_id, discord_user_name, subject, body}
 *     POST /api/discord/feedback/:id/board    {id, guild_id, channel_id, message_id}
 *
 * Notifications: a new report -> Discord staff alert (kind bug_report /
 * suggestion, its own private channel) and, for site reports, the board post
 * (Discord reports are posted to the board by the bot itself, which holds
 * the attachments at that moment). A status change -> bell to the site
 * owner, the staff alert edited in place, the board replied to and edited.
 */
import { Hono } from 'hono';
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { requireSupabaseJwt } from '../services/auth/supabaseJwt.js';
import { requireRole, canHandleTickets } from '../services/auth/roles.js';
import { requireSignedBot } from '../services/auth/botSigned.js';
import { notify, displayNameMap } from '../services/wireComments.js';
import { notifyStaff, feedbackBoard, type FeedbackKind, type StaffNotifyKind } from '../services/staffNotify.js';
import { findUserByDiscordId } from './users.js';

export const feedbackRouter = new Hono();

export const FEEDBACK_KIND_LABELS: Record<FeedbackKind, string> = {
  bug: 'Bug report',
  suggestion: 'Suggestion',
};

export type FeedbackStatus = 'open' | 'in_progress' | 'done' | 'declined' | 'duplicate';
export const FEEDBACK_STATUSES: readonly FeedbackStatus[] = ['open', 'in_progress', 'done', 'declined', 'duplicate'];

/** What each status is called, per kind — a bug is fixed, a suggestion is added. */
export const FEEDBACK_STATUS_LABELS: Record<FeedbackKind, Record<FeedbackStatus, string>> = {
  bug: { open: 'Open', in_progress: 'Looking into it', done: 'Fixed', declined: "Won't fix", duplicate: 'Duplicate' },
  suggestion: { open: 'Open', in_progress: 'Planned', done: 'Added', declined: 'Not added', duplicate: 'Duplicate' },
};

/** Statuses that end the matter: the staff alert is edited as resolved. */
const TERMINAL: ReadonlySet<FeedbackStatus> = new Set(['done', 'declined', 'duplicate']);

const STAFF_NOTIFY_KIND: Record<FeedbackKind, StaffNotifyKind> = { bug: 'bug_report', suggestion: 'suggestion' };

const SUBJECT_MIN = 3;
const SUBJECT_MAX = 140;
const BODY_MAX = 5000;
const NOTE_MAX = 1000;
/** Open or in-progress reports one reporter may have — spam belt, per identity. */
const MAX_OPEN_PER_REPORTER = 5;

export interface FeedbackRow {
  id: number;
  kind: FeedbackKind;
  source: 'site' | 'discord';
  user_id: string | null;
  user_name: string | null;
  discord_user_id: string | null;
  discord_user_name: string | null;
  subject: string;
  body: string;
  status: FeedbackStatus;
  status_note: string | null;
  status_by: string | null;
  status_by_name: string | null;
  status_at: string | null;
  ticket_id: number | null;
  board_guild_id: string | null;
  board_channel_id: string | null;
  board_message_id: string | null;
  created_at: string;
  updated_at: string;
}

type Queryable = Pick<Pool, 'query'>;

function currentUserId(c: { get: (k: 'userId') => unknown }): string | undefined {
  const v = c.get('userId');
  return typeof v === 'string' && v.length > 0 ? v : undefined;
}

function currentUserName(c: { get: (k: 'userName') => unknown }): string | null {
  const v = c.get('userName');
  return typeof v === 'string' && v ? v : null;
}

function cleanKind(raw: unknown): FeedbackKind | null {
  return raw === 'bug' || raw === 'suggestion' ? raw : null;
}
function cleanSubject(raw: unknown): string | null {
  const s = typeof raw === 'string' ? raw.trim() : '';
  return s.length >= SUBJECT_MIN && s.length <= SUBJECT_MAX ? s : null;
}
function cleanBody(raw: unknown): string | null {
  const s = typeof raw === 'string' ? raw.trim() : '';
  return s.length >= 1 && s.length <= BODY_MAX ? s : null;
}
function cleanStatus(raw: unknown): FeedbackStatus | null {
  return typeof raw === 'string' && (FEEDBACK_STATUSES as readonly string[]).includes(raw) ? (raw as FeedbackStatus) : null;
}
/** Optional note; '' and absent both mean none. Too long is an error, not a truncation. */
function cleanNote(raw: unknown): { ok: true; note: string | null } | { ok: false } {
  if (raw == null) return { ok: true, note: null };
  const s = typeof raw === 'string' ? raw.trim() : '';
  if (!s) return { ok: true, note: null };
  return s.length <= NOTE_MAX ? { ok: true, note: s } : { ok: false };
}
function cleanId(raw: string): number | null {
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : null;
}
/** Snowflakes exceed Number.MAX_SAFE_INTEGER — strings only. */
function cleanSnowflake(v: unknown): string | null {
  const s = typeof v === 'string' ? v.trim() : '';
  return /^\d{15,20}$/.test(s) ? s : null;
}

function excerpt(body: string, max = 300): string {
  return body.length > max ? body.slice(0, max - 1) + '…' : body;
}

/** The name on the board and in the staff alert. */
function reporterName(r: Pick<FeedbackRow, 'user_name' | 'discord_user_name' | 'source'>): string {
  return r.user_name ?? r.discord_user_name ?? (r.source === 'discord' ? 'a Discord member' : 'a user');
}

function boardUrl(r: Pick<FeedbackRow, 'board_guild_id' | 'board_channel_id' | 'board_message_id'>): string | null {
  return r.board_guild_id && r.board_channel_id && r.board_message_id
    ? `https://discord.com/channels/${r.board_guild_id}/${r.board_channel_id}/${r.board_message_id}`
    : null;
}

/** The reporter's view; staff get who and where as well. */
export function mapFeedback(r: FeedbackRow, staff?: { names: Map<string, string> }) {
  return {
    id: r.id,
    kind: r.kind,
    kindLabel: FEEDBACK_KIND_LABELS[r.kind],
    subject: r.subject,
    body: r.body,
    status: r.status,
    statusLabel: FEEDBACK_STATUS_LABELS[r.kind][r.status],
    statusNote: r.status_note,
    statusAt: r.status_at,
    source: r.source,
    ticketId: r.ticket_id,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    ...(staff
      ? {
          userId: r.user_id,
          // The current display name for an account; a Discord-only reporter's Discord name.
          userName: (r.user_id ? staff.names.get(r.user_id) : undefined) ?? r.user_name ?? r.discord_user_name,
          discordUserId: r.discord_user_id,
          discordUserName: r.discord_user_name,
          statusByName: r.status_by_name,
          boardUrl: boardUrl(r),
        }
      : {}),
  };
}

async function fetchFeedback(q: Queryable, id: number): Promise<FeedbackRow | null> {
  const r = await q.query<FeedbackRow>('SELECT * FROM feedback WHERE id = $1', [id]);
  return r.rows[0] ?? null;
}

/** Open + in-progress reports for one identity: a site account, else a Discord user. */
async function countOpen(q: Queryable, who: { userId: string | null; discordUserId: string | null }): Promise<number> {
  const r = who.userId
    ? await q.query<{ n: string }>(
        "SELECT COUNT(*)::text AS n FROM feedback WHERE user_id = $1 AND status IN ('open', 'in_progress')",
        [who.userId],
      )
    : await q.query<{ n: string }>(
        "SELECT COUNT(*)::text AS n FROM feedback WHERE discord_user_id = $1 AND status IN ('open', 'in_progress')",
        [who.discordUserId],
      );
  return Number(r.rows[0]?.n ?? 0);
}

interface CreateInput {
  kind: FeedbackKind;
  source: 'site' | 'discord';
  userId: string | null;
  userName: string | null;
  discordUserId?: string | null;
  discordUserName?: string | null;
  subject: string;
  body: string;
}

/**
 * One INSERT, then the staff alert and, for a site report, the board post.
 * A Discord report's board post is made by the bot (it holds the files).
 */
async function createFeedback(pool: Pool, input: CreateInput): Promise<FeedbackRow> {
  const ins = await pool.query<FeedbackRow>(
    `INSERT INTO feedback (kind, source, user_id, user_name, discord_user_id, discord_user_name, subject, body)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *`,
    [input.kind, input.source, input.userId, input.userName, input.discordUserId ?? null, input.discordUserName ?? null, input.subject, input.body],
  );
  const row = ins.rows[0]!;
  const from = row.source === 'discord'
    ? `${reporterName(row)} · via Discord${row.user_id ? ' (linked account)' : ''}`
    : reporterName(row);
  notifyStaff(pool, {
    kind: STAFF_NOTIFY_KIND[row.kind],
    event: 'new',
    ref: String(row.id),
    title: `${FEEDBACK_KIND_LABELS[row.kind]} #${row.id}`,
    subtitle: row.subject,
    fields: [
      { name: 'From', value: from, inline: true },
      { name: 'Description', value: excerpt(row.body) },
    ],
  });
  if (row.source === 'site') {
    feedbackBoard(pool, {
      event: 'new',
      kind: row.kind,
      id: row.id,
      subject: row.subject,
      body: row.body,
      reporter: reporterName(row),
      source: 'site',
      status: row.status,
      statusLabel: FEEDBACK_STATUS_LABELS[row.kind][row.status],
    });
  }
  log.info({ id: row.id, kind: row.kind, source: row.source }, 'feedback filed');
  return row;
}

// ---------------------------------------------------------------------------
// User endpoints — any logged-in account
// ---------------------------------------------------------------------------

feedbackRouter.post('/api/feedback', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const userName = currentUserName(c);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const kind = cleanKind(raw?.kind);
  const subject = cleanSubject(raw?.subject);
  const body = cleanBody(raw?.body);
  if (!kind) return c.json({ error: 'kind must be bug or suggestion' }, 400);
  if (!subject) return c.json({ error: `subject must be ${SUBJECT_MIN}-${SUBJECT_MAX} characters` }, 400);
  if (!body) return c.json({ error: `description must be 1-${BODY_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    if ((await countOpen(pool, { userId, discordUserId: null })) >= MAX_OPEN_PER_REPORTER) {
      return c.json({ error: `you already have ${MAX_OPEN_PER_REPORTER} open reports — please wait for staff to look at them` }, 429);
    }
    const row = await createFeedback(pool, { kind, source: 'site', userId, userName, subject, body });
    return c.json({ ok: true, feedback: mapFeedback(row) });
  } catch (err) {
    log.error({ err, userId }, 'feedback create failed');
    return c.json({ error: 'Failed to send' }, 500);
  }
});

feedbackRouter.get('/api/feedback/mine', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const r = await pool.query<FeedbackRow>(
      'SELECT * FROM feedback WHERE user_id = $1 ORDER BY created_at DESC LIMIT 100',
      [userId],
    );
    return c.json({ feedback: r.rows.map((row) => mapFeedback(row)) });
  } catch (err) {
    log.error({ err, userId }, 'feedback list failed');
    return c.json({ error: 'Failed to list' }, 500);
  }
});

// ---------------------------------------------------------------------------
// Staff endpoints — owner + support
// ---------------------------------------------------------------------------

feedbackRouter.get('/api/staff/feedback', requireRole(canHandleTickets), async (c) => {
  const kind = c.req.query('kind') ?? 'all';
  const status = c.req.query('status') ?? 'open';
  if (!['all', 'bug', 'suggestion'].includes(kind)) return c.json({ error: 'invalid kind filter' }, 400);
  if (!['open', 'in_progress', 'closed', 'all'].includes(status)) return c.json({ error: 'invalid status filter' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const conds: string[] = [];
    const params: unknown[] = [];
    if (kind !== 'all') {
      params.push(kind);
      conds.push(`kind = $${params.length}`);
    }
    if (status === 'closed') conds.push("status IN ('done', 'declined', 'duplicate')");
    else if (status !== 'all') {
      params.push(status);
      conds.push(`status = $${params.length}`);
    }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    const r = await pool.query<FeedbackRow>(
      `SELECT * FROM feedback ${where} ORDER BY updated_at DESC LIMIT 200`,
      params,
    );
    // Counts over everything, by kind and bucket, so the pills can show
    // their numbers whichever filter is on.
    const cr = await pool.query<{ kind: FeedbackKind; status: FeedbackStatus; n: string }>(
      'SELECT kind, status, COUNT(*)::text AS n FROM feedback GROUP BY kind, status',
    );
    const counts: Record<FeedbackKind, { open: number; in_progress: number; closed: number }> = {
      bug: { open: 0, in_progress: 0, closed: 0 },
      suggestion: { open: 0, in_progress: 0, closed: 0 },
    };
    for (const row of cr.rows) {
      const bucket = row.status === 'open' ? 'open' : row.status === 'in_progress' ? 'in_progress' : 'closed';
      if (counts[row.kind]) counts[row.kind][bucket] += Number(row.n);
    }
    const names = await displayNameMap(pool, r.rows.map((row) => row.user_id).filter((v): v is string => !!v));
    return c.json({ feedback: r.rows.map((row) => mapFeedback(row, { names })), counts });
  } catch (err) {
    log.error({ err }, 'staff feedback list failed');
    return c.json({ error: 'Failed to list' }, 500);
  }
});

feedbackRouter.get('/api/staff/feedback/:id', requireRole(canHandleTickets), async (c) => {
  const id = cleanId(c.req.param('id'));
  if (!id) return c.json({ error: 'invalid id' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const row = await fetchFeedback(pool, id);
    if (!row) return c.json({ error: 'not found' }, 404);
    const names = await displayNameMap(pool, row.user_id ? [row.user_id] : []);
    let ticket: { id: number; subject: string; status: string } | null = null;
    if (row.ticket_id) {
      const t = await pool.query<{ id: number; subject: string; status: string }>(
        'SELECT id, subject, status FROM support_tickets WHERE id = $1',
        [row.ticket_id],
      );
      ticket = t.rows[0] ?? null;
    }
    return c.json({ feedback: mapFeedback(row, { names }), ticket });
  } catch (err) {
    log.error({ err, id }, 'staff feedback fetch failed');
    return c.json({ error: 'Failed to fetch' }, 500);
  }
});

feedbackRouter.post('/api/staff/feedback/:id/status', requireRole(canHandleTickets), async (c) => {
  const actorId = currentUserId(c)!;
  const actorName = currentUserName(c) ?? actorId.slice(0, 8);
  const id = cleanId(c.req.param('id'));
  if (!id) return c.json({ error: 'invalid id' }, 400);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const status = cleanStatus(raw?.status);
  if (!status) return c.json({ error: `status must be one of ${FEEDBACK_STATUSES.join(', ')}` }, 400);
  const note = cleanNote(raw?.note);
  if (!note.ok) return c.json({ error: `note must be at most ${NOTE_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const r = await pool.query<FeedbackRow>(
      `UPDATE feedback
          SET status = $2, status_note = $3, status_by = $4, status_by_name = $5, status_at = now(), updated_at = now()
        WHERE id = $1
        RETURNING *`,
      [id, status, note.note, actorId, actorName],
    );
    const row = r.rows[0];
    if (!row) return c.json({ error: 'not found' }, 404);
    const label = FEEDBACK_STATUS_LABELS[row.kind][row.status];
    const kindLabel = FEEDBACK_KIND_LABELS[row.kind];

    if (row.user_id) {
      notify(pool, {
        userId: row.user_id,
        type: 'feedback.status',
        title: `Your ${kindLabel.toLowerCase()} "${row.subject}" was marked ${label}`,
        body: note.note ?? undefined,
        link: '/contact',
        exceptUserId: actorId,
      }).catch(() => undefined);
    }
    // The private alert: resolved styling only when the matter is over.
    notifyStaff(pool, {
      kind: STAFF_NOTIFY_KIND[row.kind],
      event: TERMINAL.has(row.status) ? 'resolved' : 'update',
      ref: String(row.id),
      title: `${kindLabel} #${row.id} · ${label}`,
      subtitle: row.subject,
      status: row.status === 'open' ? '' : label,
      actor: actorName,
      fields: note.note ? [{ name: 'Note', value: excerpt(note.note), inline: false }] : [],
    });
    feedbackBoard(pool, {
      event: 'status',
      kind: row.kind,
      id: row.id,
      subject: row.subject,
      status: row.status,
      statusLabel: label,
      note: note.note,
      actor: actorName,
    });
    log.info({ id, status, by: actorId }, 'feedback status set');
    return c.json({ ok: true, feedback: mapFeedback(row, { names: new Map() }) });
  } catch (err) {
    log.error({ err, id }, 'feedback status failed');
    return c.json({ error: 'Failed to set status' }, 500);
  }
});

// Link a report to an existing ticket (or unlink with null). The ticket is
// where the conversation happens; the report keeps its public status.
feedbackRouter.post('/api/staff/feedback/:id/ticket', requireRole(canHandleTickets), async (c) => {
  const id = cleanId(c.req.param('id'));
  if (!id) return c.json({ error: 'invalid id' }, 400);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const tRaw = raw?.ticket_id;
  let ticketId: number | null = null;
  if (tRaw !== null && tRaw !== undefined && tRaw !== '') {
    const n = Number(tRaw);
    if (!Number.isInteger(n) || n <= 0) return c.json({ error: 'ticket_id must be a ticket number' }, 400);
    ticketId = n;
  }
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    if (ticketId) {
      const t = await pool.query<{ id: number }>('SELECT id FROM support_tickets WHERE id = $1', [ticketId]);
      if (!t.rows[0]) return c.json({ error: `ticket #${ticketId} does not exist` }, 404);
    }
    const r = await pool.query<FeedbackRow>(
      'UPDATE feedback SET ticket_id = $2, updated_at = now() WHERE id = $1 RETURNING *',
      [id, ticketId],
    );
    const row = r.rows[0];
    if (!row) return c.json({ error: 'not found' }, 404);
    return c.json({ ok: true, feedback: mapFeedback(row, { names: new Map() }) });
  } catch (err) {
    log.error({ err, id }, 'feedback ticket link failed');
    return c.json({ error: 'Failed to link' }, 500);
  }
});

// Open a ticket AT the reporter about this report — the staff-initiated
// outreach of tickets.ts, with the report quoted above staff's message. Only
// possible when the reporter has a site account: a Discord-only reporter
// has no contact page for the ticket to appear on.
feedbackRouter.post('/api/staff/feedback/:id/ticket/new', requireRole(canHandleTickets), async (c) => {
  const actorId = currentUserId(c)!;
  const actorName = currentUserName(c);
  const id = cleanId(c.req.param('id'));
  if (!id) return c.json({ error: 'invalid id' }, 400);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const body = cleanBody(raw?.body);
  if (!body) return c.json({ error: `message must be 1-${BODY_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const row = await fetchFeedback(pool, id);
    if (!row) return c.json({ error: 'not found' }, 404);
    if (!row.user_id) {
      return c.json({ error: 'this was reported from Discord by someone with no site account — link an existing ticket instead' }, 409);
    }
    if (row.ticket_id) return c.json({ error: `already linked to ticket #${row.ticket_id}` }, 409);

    const kindLabel = FEEDBACK_KIND_LABELS[row.kind];
    const subject = `Re: your ${kindLabel.toLowerCase()} #${row.id} — ${row.subject}`.slice(0, SUBJECT_MAX);
    const quoted = excerpt(row.body, 1000).split('\n').map((l) => `> ${l}`).join('\n');
    const first = `> **${kindLabel} #${row.id} · ${row.subject}**\n${quoted}\n\n${body}`.slice(0, BODY_MAX);

    const client = await pool.connect();
    let ticket: { id: number };
    try {
      await client.query('BEGIN');
      const ins = await client.query<{ id: number }>(
        `INSERT INTO support_tickets (user_id, user_name, subject, category, status, created_by_staff)
         VALUES ($1, $2, $3, $4, 'awaiting_user', true) RETURNING id`,
        [row.user_id, row.user_name, subject, row.kind === 'bug' ? 'bug' : 'general'],
      );
      ticket = ins.rows[0]!;
      await client.query(
        'INSERT INTO support_ticket_messages (ticket_id, author_id, author_name, is_staff, body) VALUES ($1, $2, $3, true, $4)',
        [ticket.id, actorId, actorName, first],
      );
      await client.query('UPDATE feedback SET ticket_id = $2, updated_at = now() WHERE id = $1', [row.id, ticket.id]);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }

    notify(pool, {
      userId: row.user_id,
      type: 'ticket.opened',
      title: `Staff opened a ticket about your ${kindLabel.toLowerCase()}: "${row.subject}"`,
      body: excerpt(body),
      link: `/contact?t=${ticket.id}`,
    }).catch(() => undefined);
    log.info({ id, ticketId: ticket.id, by: actorId }, 'ticket opened from feedback');
    return c.json({ ok: true, ticketId: ticket.id });
  } catch (err) {
    log.error({ err, id }, 'feedback ticket create failed');
    return c.json({ error: 'Failed to open a ticket' }, 500);
  }
});

feedbackRouter.delete('/api/staff/feedback/:id', requireRole(canHandleTickets), async (c) => {
  const actorId = currentUserId(c)!;
  const id = cleanId(c.req.param('id'));
  if (!id) return c.json({ error: 'invalid id' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const r = await pool.query<FeedbackRow>('DELETE FROM feedback WHERE id = $1 RETURNING *', [id]);
    const row = r.rows[0];
    if (!row) return c.json({ error: 'not found' }, 404);
    feedbackBoard(pool, {
      event: 'delete',
      kind: row.kind,
      id: row.id,
      status: row.status,
      statusLabel: FEEDBACK_STATUS_LABELS[row.kind][row.status],
    });
    log.info({ id, by: actorId }, 'feedback removed');
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err, id }, 'feedback delete failed');
    return c.json({ error: 'Failed to remove' }, 500);
  }
});

// ---------------------------------------------------------------------------
// Bot endpoints — the Discord intake channels (auth/botSigned.ts)
// ---------------------------------------------------------------------------

feedbackRouter.post('/api/discord/feedback', requireSignedBot('feedback'), async (c) => {
  const b = c.get('botBody')!;
  const kind = cleanKind(b['kind']);
  const discordUserId = cleanSnowflake(b['discord_user_id']);
  const discordUserName = typeof b['discord_user_name'] === 'string' ? b['discord_user_name'].trim().slice(0, 100) : '';
  const subject = cleanSubject(b['subject']);
  const body = cleanBody(b['body']);
  if (!kind) return c.json({ error: 'kind must be bug or suggestion' }, 400);
  if (!discordUserId) return c.json({ error: 'discord_user_id must be a Discord id' }, 400);
  if (!subject) return c.json({ error: `subject must be ${SUBJECT_MIN}-${SUBJECT_MAX} characters` }, 400);
  if (!body) return c.json({ error: `description must be 1-${BODY_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const linked = await findUserByDiscordId(discordUserId);
    if ((await countOpen(pool, { userId: linked?.id ?? null, discordUserId })) >= MAX_OPEN_PER_REPORTER) {
      return c.json({ error: `you already have ${MAX_OPEN_PER_REPORTER} open reports — please wait for staff to look at them` }, 429);
    }
    const row = await createFeedback(pool, {
      kind,
      source: 'discord',
      userId: linked?.id ?? null,
      userName: linked?.name ?? null,
      discordUserId,
      discordUserName: discordUserName || null,
      subject,
      body,
    });
    return c.json({
      ok: true,
      feedback: { id: row.id, kind: row.kind, statusLabel: FEEDBACK_STATUS_LABELS[row.kind][row.status] },
      linked: !!linked,
    });
  } catch (err) {
    log.error({ err, discordUserId }, 'discord feedback create failed');
    return c.json({ error: 'Failed to file' }, 500);
  }
});

// Where the bot put the report on the public board — the ticket's "Open in
// Discord" link. `id` is in the signed body as well as the path, so a signed
// body cannot be replayed against another report.
feedbackRouter.post('/api/discord/feedback/:id/board', requireSignedBot('feedback_board_post'), async (c) => {
  const b = c.get('botBody')!;
  const id = cleanId(c.req.param('id'));
  if (!id || String(b['id']) !== String(id)) return c.json({ error: 'id mismatch' }, 400);
  const guildId = cleanSnowflake(b['guild_id']);
  const channelId = cleanSnowflake(b['channel_id']);
  const messageId = cleanSnowflake(b['message_id']);
  if (!guildId || !channelId || !messageId) return c.json({ error: 'guild_id, channel_id and message_id must be Discord ids' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const r = await pool.query(
      'UPDATE feedback SET board_guild_id = $2, board_channel_id = $3, board_message_id = $4 WHERE id = $1',
      [id, guildId, channelId, messageId],
    );
    if (!r.rowCount) return c.json({ error: 'not found' }, 404);
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err, id }, 'discord feedback board link failed');
    return c.json({ error: 'Failed to record' }, 500);
  }
});
