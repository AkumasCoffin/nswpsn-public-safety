/**
 * Staff → user notifications: compose, send, and the record of what was sent.
 *
 * Gated on canSendNotices (owner | staff). Everything here writes into the
 * same `notifications` table the rest of the site uses, so a notice arrives in
 * the bell looking like any other message — which is the point: the recipient
 * should not have to learn a new surface to be told something.
 */
import { Hono } from 'hono';
import { z } from 'zod';

import { config } from '../config.js';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { canSendNotices, isKnownRole, requireRole } from '../services/auth/roles.js';
import { avatarMap } from '../services/wireComments.js';
import {
  MAX_NAMED_RECIPIENTS,
  listNotices,
  resolveRecipients,
  sendNotice,
  targetCounts,
  type NoticeTarget,
} from '../services/staffNotices.js';

export const staffNoticesRouter = new Hono();

/**
 * How many notices one sender may post an hour.
 *
 * Not about abuse — everyone through this gate is trusted. It is about the
 * accident: a hung request retried, a double submit, a loop. There is no
 * unsend, so the cheap ceiling is worth more than the flexibility.
 */
const SEND_WINDOW_MS = 60 * 60 * 1000;
const MAX_SENDS_PER_WINDOW = 10;
const sendHits = new Map<string, number[]>();

function sendRateOk(userId: string): boolean {
  const now = Date.now();
  const cutoff = now - SEND_WINDOW_MS;
  const recent = (sendHits.get(userId) ?? []).filter((t) => t >= cutoff);
  if (recent.length >= MAX_SENDS_PER_WINDOW) {
    sendHits.set(userId, recent);
    return false;
  }
  recent.push(now);
  sendHits.set(userId, recent);
  return true;
}

/** Test seam. */
export function _resetNoticeRateLimit(): void {
  sendHits.clear();
}

/**
 * A link that is safe to put in an href.
 *
 * The bell renders `<a href="${escNotif(n.link)}">` (auth-common.js), and
 * escNotif escapes HTML entities — which does nothing to a `javascript:` URL,
 * since it needs none of the characters being escaped. A notice reaches every
 * recipient, so an unchecked link here is stored XSS with a staff-sized blast
 * radius. Allowlist, never blocklist: one leading slash (site-relative) or an
 * absolute https URL. Protocol-relative `//host` is refused precisely because
 * it LOOKS like the relative form.
 */
export function safeNoticeLink(raw: string): string | null {
  const link = raw.trim();
  if (link === '') return null;
  if (link.length > 500) return null;
  if (link.startsWith('//')) return null;
  if (link.startsWith('/')) return link;
  let url: URL;
  try {
    url = new URL(link);
  } catch {
    return null;
  }
  return url.protocol === 'https:' ? url.toString() : null;
}

const SendSchema = z.object({
  audience: z.enum(['user', 'users', 'role', 'all']),
  userIds: z.array(z.string().trim().min(1).max(64)).max(MAX_NAMED_RECIPIENTS).optional(),
  role: z.string().trim().min(1).max(64).optional(),
  title: z.string().trim().min(1).max(120),
  body: z.string().trim().min(1).max(2000),
  link: z.string().trim().max(500).optional(),
});

// ---------------------------------------------------------------------------
// POST /api/staff/notices — send one.
// ---------------------------------------------------------------------------
staffNoticesRouter.post('/api/staff/notices', requireRole(canSendNotices), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  const userId = c.get('userId') as string;

  const parsed = SendSchema.safeParse(await c.req.json().catch(() => ({})));
  if (!parsed.success) {
    return c.json({ error: 'invalid notice', details: parsed.error.issues.slice(0, 5) }, 400);
  }
  const { audience, title, body } = parsed.data;

  let link: string | null = null;
  if (parsed.data.link) {
    link = safeNoticeLink(parsed.data.link);
    if (link === null) {
      return c.json(
        { error: 'a link must be a path on this site (/somewhere) or an https:// address' },
        400,
      );
    }
  }

  if (audience === 'role') {
    const role = parsed.data.role ?? '';
    if (!isKnownRole(role)) return c.json({ error: 'unknown role' }, 400);
  }
  if ((audience === 'user' || audience === 'users') && !(parsed.data.userIds ?? []).length) {
    return c.json({ error: 'pick at least one person' }, 400);
  }

  if (!sendRateOk(userId)) {
    return c.json({ error: 'too many notices sent in the last hour' }, 429);
  }

  const target: NoticeTarget = {
    audience,
    userIds: parsed.data.userIds,
    role: parsed.data.role ?? null,
  };

  try {
    const recipients = await resolveRecipients(pool, target);
    if (recipients.length === 0) {
      // Said plainly rather than recorded as a send of nothing: an audience
      // that resolves to nobody is a mistake in the picking, every time.
      return c.json({ error: 'that audience has nobody in it' }, 400);
    }
    const sent = await sendNotice(
      pool,
      {
        sentBy: userId,
        sentByName: (c.get('userName') as string | undefined) ?? null,
        target,
        title,
        body,
        link,
      },
      recipients,
    );
    log.info(
      { noticeId: sent.noticeId, audience, role: target.role, recipients: sent.recipients, by: userId },
      'staff notice sent',
    );
    return c.json({ ok: true, ...sent });
  } catch (err) {
    log.error({ err, by: userId, audience }, 'staff notice send failed');
    return c.json({ error: 'could not send the notice' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/staff/notices — what has been sent.
// ---------------------------------------------------------------------------
staffNoticesRouter.get('/api/staff/notices', requireRole(canSendNotices), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const limit = Number(c.req.query('limit') ?? 20);
    return c.json({ notices: await listNotices(pool, limit) });
  } catch (err) {
    log.error({ err }, 'listing staff notices failed');
    return c.json({ error: 'could not load sent notices' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/staff/notices/targets — how many people each audience reaches.
// ---------------------------------------------------------------------------
staffNoticesRouter.get('/api/staff/notices/targets', requireRole(canSendNotices), async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    return c.json(await targetCounts(pool));
  } catch (err) {
    log.error({ err }, 'notice target counts failed');
    return c.json({ error: 'could not load audiences' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/staff/notices/user-search?q= — the people picker.
//
// Its own endpoint rather than /api/users for the same reason tickets has one:
// that route is canManageUsers-gated and returns the whole directory, and a
// picker needs an id, a name and an email for the handful that match.
//
// It also carries each match's avatar, because a list of similar usernames is
// slow to pick from and a face is not. Avatars come from user_profiles via the
// shared resolver, so the picker shows the same image the rest of the site
// does, and are decoration: a failure there never fails the search.
// ---------------------------------------------------------------------------
staffNoticesRouter.get('/api/staff/notices/user-search', requireRole(canSendNotices), async (c) => {
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
    const str = (v: unknown): string | null =>
      typeof v === 'string' && v.trim() !== '' ? v.trim() : null;
    const matched = (data.users ?? [])
      .map((u) => {
        const md = u.user_metadata ?? {};
        const username =
          str(md['username']) ?? str(md['display_name']) ?? str(md['full_name']) ?? str(md['name']) ??
          (u.email ? u.email.split('@')[0]! : null);
        return { id: u.id ?? '', username: username ?? '', email: u.email ?? '' };
      })
      .filter((u) => u.id && (u.username.toLowerCase().includes(q) || u.email.toLowerCase().includes(q)))
      .slice(0, 20);

    const pool = await getPool();
    const avatars = pool ? await avatarMap(pool, matched.map((u) => u.id)) : new Map<string, string>();
    const users = matched.map((u) => ({ ...u, avatar: avatars.get(u.id) ?? null }));
    return c.json({ users });
  } catch (err) {
    log.error({ err }, 'notice user-search failed');
    return c.json({ error: 'user lookup failed' }, 502);
  }
});
