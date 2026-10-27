/**
 * Named API keys — request, review, claim, revoke.
 *
 * Any logged-in account can ASK for a key (name + what it is for). Owner|staff
 * approve or reject from the staff page (Moderation → API keys), optionally
 * setting the per-minute limit and an expiry. Approval does NOT mint anything:
 * the user then generates the key from their profile (POST …/claim), the
 * backend mints it in that request, and the plaintext goes back once to the
 * person who owns it. Staff never see a plaintext and nothing stores one —
 * the same one-time-display rule as scripts/issue-api-key.ts, just with the
 * user holding the terminal.
 *
 *   User (requireSupabaseJwt):
 *     GET  /api/api-keys/me                       own requests + undeleted keys + caps
 *     POST /api/api-keys/requests                 {name, reason} → pending
 *     POST /api/api-keys/requests/:id/claim       approved → mints, returns plaintext ONCE
 *     POST /api/api-keys/:prefix/reissue          own live key → new secret, plaintext ONCE
 *     POST /api/api-keys/:prefix/delete           own key → revoked + gone from their list
 *   Staff (owner|staff — canManageUsers):
 *     GET  /api/staff/api-keys/requests?status=   pending|approved|rejected|all
 *     POST /api/staff/api-keys/requests/:id/approve   {rateLimitPerMin?, expiresAt?, note?}
 *     POST /api/staff/api-keys/requests/:id/reject    {note?}
 *     POST /api/staff/api-keys/requests/:id/withdraw  {note?}  approved-but-unclaimed → rejected
 *     GET  /api/staff/api-keys                    every key (deleted too), owner names, 24h/7d usage
 *     GET  /api/staff/api-keys/:id/usage?window=  24h|7d|30d — series + paths for one key
 *     POST /api/staff/api-keys/:prefix/revoke     any key; the owner is told
 *
 * Usage figures come from api_key_usage_hourly (migration 127,
 * services/auth/apiKeyUsage.ts): every request made with a key's real secret,
 * the gate's refusals included, by hour, path and status. A window is a whole
 * number of hourly buckets ending with the current one, so a key's "24h" in
 * the list, its tiles and the sum of its chart are the same number.
 *
 * Caps: one OPEN request per user (pending, or approved and not yet
 * generated and not lapsed) and MAX_ACTIVE_KEYS_PER_USER live keys. Both are
 * checked under a per-user advisory lock, so two concurrent requests or
 * claims by the same person queue rather than both slipping under the cap.
 * Notifications: a new request → Discord staff alert (kind api_key_request);
 * approve/reject/withdraw → the requester's bell (link /#api-keys opens their
 * profile) and the Discord message is edited in place.
 */
import { Hono } from 'hono';
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { requireSupabaseJwt } from '../services/auth/supabaseJwt.js';
import { requireRole, canManageUsers } from '../services/auth/roles.js';
import {
  createApiKey,
  revokeApiKeyById,
  reissueApiKey,
  deleteApiKey,
  forgetApiKeyPrefix,
  DEFAULT_RATE_LIMIT_PER_MIN,
  type ApiKeyRow,
} from '../services/auth/apiKeys.js';
import { notify, displayNameMap } from '../services/wireComments.js';
import { notifyStaff } from '../services/staffNotify.js';
import { getUsername } from './users.js';

export const apiKeysRouter = new Hono();

const NAME_MIN = 2;
const NAME_MAX = 60;
const REASON_MIN = 10;
const REASON_MAX = 1000;
const NOTE_MAX = 500;
/** Highest per-minute limit a reviewer may grant from the UI. */
const RPM_MAX = 6000;
/** Furthest expiry a reviewer may set. */
const EXPIRES_MAX_MS = 5 * 365 * 24 * 60 * 60 * 1000;
/** A user may hold this many live (not revoked, not expired) keys. */
export const MAX_ACTIVE_KEYS_PER_USER = 3;
/** Where a bell notification sends the user: auth-common.js opens the profile modal on this hash. */
const KEYS_LINK = '/#api-keys';

const PREFIX_RE = /^[A-Za-z0-9]{10}$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * A request that still binds the user: waiting on staff, or approved and
 * still claimable. A grant whose expiry has already passed is not — it can
 * never be minted into a usable key, so it stops blocking a fresh request.
 */
const OPEN_REQUEST_SQL =
  "(status = 'pending' OR (status = 'approved' AND key_id IS NULL AND (expires_at IS NULL OR expires_at > now())))";

interface RequestRow {
  id: number;
  user_id: string;
  user_name: string | null;
  name: string;
  reason: string;
  status: 'pending' | 'approved' | 'rejected';
  rate_limit_per_min: number | null;
  expires_at: string | null;
  review_note: string | null;
  reviewed_by: string | null;
  reviewed_by_name: string | null;
  reviewed_at: string | null;
  key_id: string | null;
  claimed_at: string | null;
  created_at: string;
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

function cleanText(raw: unknown, min: number, max: number): string | null {
  const s = typeof raw === 'string' ? raw.trim() : '';
  return s.length >= min && s.length <= max ? s : null;
}

/** Optional note; '' and absent both mean none. Too long is an error, not a truncation. */
function cleanNote(raw: unknown): { ok: true; note: string | null } | { ok: false } {
  if (raw == null) return { ok: true, note: null };
  const s = typeof raw === 'string' ? raw.trim() : '';
  if (!s) return { ok: true, note: null };
  return s.length <= NOTE_MAX ? { ok: true, note: s } : { ok: false };
}

function cleanRpm(raw: unknown): { ok: true; rpm: number | null } | { ok: false } {
  if (raw == null || raw === '') return { ok: true, rpm: null };
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 && n <= RPM_MAX ? { ok: true, rpm: n } : { ok: false };
}

function cleanExpires(raw: unknown, now: number): { ok: true; at: Date | null } | { ok: false } {
  if (raw == null || raw === '') return { ok: true, at: null };
  if (typeof raw !== 'string') return { ok: false };
  const d = new Date(raw);
  const t = d.getTime();
  if (!Number.isFinite(t) || t <= now || t - now > EXPIRES_MAX_MS) return { ok: false };
  return { ok: true, at: d };
}

function isLiveKey(k: ApiKeyRow, now: number): boolean {
  if (k.revoked_at) return false;
  if (k.expires_at && new Date(k.expires_at).getTime() <= now) return false;
  return true;
}

/** Approved, never generated, and the granted expiry has already passed. */
function isLapsed(r: RequestRow, now: number): boolean {
  return r.status === 'approved' && !r.key_id && !!r.expires_at && new Date(r.expires_at).getTime() <= now;
}

/** JS twin of OPEN_REQUEST_SQL. */
function isOpenRequest(r: RequestRow, now: number): boolean {
  if (r.status === 'pending') return true;
  return r.status === 'approved' && !r.key_id && !isLapsed(r, now);
}

/** The request as either side sees it. Staff get the owner's id + current name. */
function mapRequest(r: RequestRow, now: number, nameOverlay?: Map<string, string>) {
  return {
    id: r.id,
    name: r.name,
    reason: r.reason,
    status: r.status,
    rateLimitPerMin: r.rate_limit_per_min,
    expiresAt: r.expires_at,
    reviewNote: r.review_note,
    reviewedByName: r.reviewed_by_name,
    reviewedAt: r.reviewed_at,
    claimed: !!r.key_id,
    claimedAt: r.claimed_at,
    lapsed: isLapsed(r, now),
    createdAt: r.created_at,
    ...(nameOverlay ? { userId: r.user_id, userName: nameOverlay.get(r.user_id) ?? r.user_name } : {}),
  };
}

/** The key without its hash. */
function mapKey(k: ApiKeyRow, now: number, nameOverlay?: Map<string, string>) {
  return {
    id: k.id,
    prefix: k.prefix,
    name: k.name,
    scopes: k.scopes,
    rateLimitPerMin: k.rate_limit_per_min,
    createdAt: k.created_at,
    lastUsedAt: k.last_used_at,
    expiresAt: k.expires_at,
    revokedAt: k.revoked_at,
    reissuedAt: k.reissued_at ?? null,
    deletedAt: k.deleted_at ?? null,
    requestsTotal: Number(k.requests_total ?? 0),
    active: isLiveKey(k, now),
    ...(nameOverlay ? { userId: k.user_id, userName: nameOverlay.get(k.user_id) ?? null } : {}),
  };
}

// ---------------------------------------------------------------------------
// Usage (api_key_usage_hourly)
// ---------------------------------------------------------------------------

/**
 * How a status is reported. ok is anything the key's caller got data or a
 * not-modified from; refused is the gate (or a route) saying no.
 */
const USAGE_CLASSES = ['ok', 'notFound', 'refused', 'rateLimited', 'otherClient', 'serverError'] as const;
type UsageClass = (typeof USAGE_CLASSES)[number];
type UsageCounts = { requests: number } & Record<UsageClass, number>;

const USAGE_CLASS_SQL = `CASE WHEN status < 400 THEN 'ok'
            WHEN status = 404 THEN 'notFound'
            WHEN status IN (401, 403) THEN 'refused'
            WHEN status = 429 THEN 'rateLimited'
            WHEN status < 500 THEN 'otherClient'
            ELSE 'serverError' END`;

/**
 * The first hourly bucket of a window `$n` hours long that ends with the
 * current hour. Bracketed: AT TIME ZONE binds tighter than the minus, so an
 * unbracketed start would apply it to the interval alone.
 */
const windowStartSql = (param: string) => `(date_trunc('hour', now()) - (${param}::int - 1) * interval '1 hour')`;

const USAGE_WINDOWS: Record<string, { hours: number; bucket: 'hour' | 'day' }> = {
  '24h': { hours: 24, bucket: 'hour' },
  '7d': { hours: 7 * 24, bucket: 'day' },
  '30d': { hours: 30 * 24, bucket: 'day' },
};

function emptyCounts(): UsageCounts {
  return { requests: 0, ok: 0, notFound: 0, refused: 0, rateLimited: 0, otherClient: 0, serverError: 0 };
}

function addTo(c: UsageCounts, cls: string, n: number): void {
  if (!(USAGE_CLASSES as readonly string[]).includes(cls)) return;
  c[cls as UsageClass] += n;
  c.requests += n;
}

/** Every key's last 24 hours and last 7 days, by class. Keys with no use are absent. */
async function usageByKey(pool: Pool): Promise<Map<string, { h24: UsageCounts; d7: UsageCounts }>> {
  const { rows } = await pool.query<{ key_id: string; in_day: boolean; cls: string; n: number }>(
    `SELECT key_id::text AS key_id,
            hour >= ${windowStartSql('$1')} AS in_day,
            ${USAGE_CLASS_SQL} AS cls,
            SUM(requests)::int AS n
       FROM api_key_usage_hourly
      WHERE hour >= ${windowStartSql('$2')}
      GROUP BY 1, 2, 3`,
    [USAGE_WINDOWS['24h']!.hours, USAGE_WINDOWS['7d']!.hours],
  );
  const out = new Map<string, { h24: UsageCounts; d7: UsageCounts }>();
  for (const r of rows) {
    let u = out.get(r.key_id);
    if (!u) {
      u = { h24: emptyCounts(), d7: emptyCounts() };
      out.set(r.key_id, u);
    }
    addTo(u.d7, r.cls, Number(r.n));
    if (r.in_day) addTo(u.h24, r.cls, Number(r.n));
  }
  return out;
}

/**
 * Serialise every cap check for one user inside the current transaction. The
 * row locks alone are not enough: two claims of two different requests, or
 * two simultaneous submissions, lock nothing in common and would both read
 * the same count.
 */
async function lockUser(q: Queryable, userId: string): Promise<void> {
  await q.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`api-keys:${userId}`]);
}

async function countLiveKeys(q: Queryable, userId: string): Promise<number> {
  const r = await q.query<{ n: string }>(
    `SELECT COUNT(*)::text AS n FROM api_keys
      WHERE user_id = $1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > now())`,
    [userId],
  );
  return Number(r.rows[0]?.n ?? 0);
}

async function countOpenRequests(q: Queryable, userId: string): Promise<number> {
  const r = await q.query<{ n: string }>(
    `SELECT COUNT(*)::text AS n FROM api_key_requests WHERE user_id = $1 AND ${OPEN_REQUEST_SQL}`,
    [userId],
  );
  return Number(r.rows[0]?.n ?? 0);
}

function excerpt(s: string): string {
  return s.length > 300 ? s.slice(0, 297) + '…' : s;
}

// ---------------------------------------------------------------------------
// User endpoints — any logged-in account
// ---------------------------------------------------------------------------

apiKeysRouter.get('/api/api-keys/me', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const now = Date.now();
    const [reqs, keys] = await Promise.all([
      pool.query<RequestRow>(
        'SELECT * FROM api_key_requests WHERE user_id = $1 ORDER BY created_at DESC LIMIT 50',
        [userId],
      ),
      // A key the owner deleted is gone from their view; staff still see it.
      pool.query<ApiKeyRow>(
        'SELECT * FROM api_keys WHERE user_id = $1 AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 50',
        [userId],
      ),
    ]);
    const live = keys.rows.filter((k) => isLiveKey(k, now)).length;
    const hasOpen = reqs.rows.some((r) => isOpenRequest(r, now));
    return c.json({
      requests: reqs.rows.map((r) => mapRequest(r, now)),
      keys: keys.rows.map((k) => mapKey(k, now)),
      limits: { maxActiveKeys: MAX_ACTIVE_KEYS_PER_USER },
      canRequest: !hasOpen && live < MAX_ACTIVE_KEYS_PER_USER,
    });
  } catch (err) {
    log.error({ err, userId }, 'api-keys/me failed');
    return c.json({ error: 'Failed to load API keys' }, 500);
  }
});

apiKeysRouter.post('/api/api-keys/requests', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const userName = currentUserName(c);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const name = cleanText(raw?.name, NAME_MIN, NAME_MAX);
  const reason = cleanText(raw?.reason, REASON_MIN, REASON_MAX);
  if (!name) return c.json({ error: `name must be ${NAME_MIN}-${NAME_MAX} characters` }, 400);
  if (!reason) return c.json({ error: `tell us what the key is for (${REASON_MIN}-${REASON_MAX} characters)` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);

  const client = await pool.connect();
  let row: RequestRow;
  try {
    await client.query('BEGIN');
    await lockUser(client, userId);
    if ((await countOpenRequests(client, userId)) > 0) {
      await client.query('ROLLBACK');
      return c.json({ error: 'you already have a request waiting for review, or an approved key you have not generated yet' }, 409);
    }
    if ((await countLiveKeys(client, userId)) >= MAX_ACTIVE_KEYS_PER_USER) {
      await client.query('ROLLBACK');
      return c.json({ error: `you already hold ${MAX_ACTIVE_KEYS_PER_USER} active keys — revoke one first` }, 409);
    }
    const ins = await client.query<RequestRow>(
      'INSERT INTO api_key_requests (user_id, user_name, name, reason) VALUES ($1, $2, $3, $4) RETURNING *',
      [userId, userName, name, reason],
    );
    row = ins.rows[0]!;
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    // The partial unique index (migration 121) is the backstop for the
    // one-pending rule; hitting it means the lock above was bypassed somehow.
    if ((err as { code?: string }).code === '23505') {
      return c.json({ error: 'you already have a request waiting for review' }, 409);
    }
    log.error({ err, userId }, 'api key request failed');
    return c.json({ error: 'Failed to submit request' }, 500);
  } finally {
    client.release();
  }

  notifyStaff(pool, {
    kind: 'api_key_request',
    event: 'new',
    ref: String(row.id),
    title: `API key request #${row.id} · ${name}`,
    subtitle: null,
    fields: [
      { name: 'From', value: userName ?? userId.slice(0, 8), inline: true },
      { name: 'For', value: excerpt(reason) },
    ],
  });
  log.info({ requestId: row.id, userId }, 'api key requested');
  return c.json({ ok: true, request: mapRequest(row, Date.now()) });
});

// The user mints their approved key. One transaction: lock the user, lock
// the request, check it is still claimable and the cap holds, insert the key,
// link it. The plaintext exists only in this response.
apiKeysRouter.post('/api/api-keys/requests/:id/claim', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id) || id <= 0) return c.json({ error: 'invalid id' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await lockUser(client, userId);
    const sel = await client.query<RequestRow>(
      'SELECT * FROM api_key_requests WHERE id = $1 AND user_id = $2 FOR UPDATE',
      [id, userId],
    );
    const req = sel.rows[0];
    // 404 for strangers: another user's request is not theirs to see.
    if (!req) {
      await client.query('ROLLBACK');
      return c.json({ error: 'request not found' }, 404);
    }
    if (req.status !== 'approved') {
      await client.query('ROLLBACK');
      return c.json({ error: req.status === 'pending' ? 'this request is still waiting for review' : 'this request was not approved' }, 409);
    }
    if (req.key_id) {
      await client.query('ROLLBACK');
      return c.json({ error: 'this key was already generated — it is shown only once' }, 409);
    }
    const now = Date.now();
    if (isLapsed(req, now)) {
      // Minting would produce a key that is already expired and burn the
      // one-shot grant doing it. The grant simply stops counting (see
      // OPEN_REQUEST_SQL), so the user can ask again.
      await client.query('ROLLBACK');
      return c.json({ error: 'this approval has lapsed — its expiry date has passed. Please submit a new request' }, 409);
    }
    if ((await countLiveKeys(client, userId)) >= MAX_ACTIVE_KEYS_PER_USER) {
      await client.query('ROLLBACK');
      return c.json({ error: `you already hold ${MAX_ACTIVE_KEYS_PER_USER} active keys — revoke one first` }, 409);
    }

    const { plaintext, row: key } = await createApiKey(
      {
        userId,
        name: req.name,
        rateLimitPerMin: req.rate_limit_per_min ?? DEFAULT_RATE_LIMIT_PER_MIN,
        expiresAt: req.expires_at ? new Date(req.expires_at) : null,
      },
      client,
    );
    await client.query(
      'UPDATE api_key_requests SET key_id = $2, claimed_at = now() WHERE id = $1',
      [id, key.id],
    );
    await client.query('COMMIT');
    log.info({ requestId: id, userId, prefix: key.prefix }, 'api key claimed');
    return c.json({ ok: true, plaintext, key: mapKey(key, now) });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    log.error({ err, id, userId }, 'api key claim failed');
    return c.json({ error: 'Failed to generate the key' }, 500);
  } finally {
    client.release();
  }
});

// A lost or leaked key, replaced without another review: the grant is the
// same, only the secret changes. The plaintext exists only in this response,
// exactly as at claim. Same per-user lock as claim, so a re-issue cannot
// interleave with a claim or a second re-issue in another tab.
apiKeysRouter.post('/api/api-keys/:prefix/reissue', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const prefix = c.req.param('prefix');
  if (!PREFIX_RE.test(prefix)) return c.json({ error: 'invalid key' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await lockUser(client, userId);
    const sel = await client.query<ApiKeyRow>(
      'SELECT * FROM api_keys WHERE prefix = $1 AND user_id = $2 AND deleted_at IS NULL FOR UPDATE',
      [prefix, userId],
    );
    const key = sel.rows[0];
    if (!key) {
      await client.query('ROLLBACK');
      return c.json({ error: 'key not found' }, 404);
    }
    const now = Date.now();
    if (!isLiveKey(key, now)) {
      await client.query('ROLLBACK');
      return c.json(
        { error: key.revoked_at ? 'this key is revoked — it cannot be re-issued' : 'this key has expired — request a new one' },
        409,
      );
    }
    const out = await reissueApiKey(key.id, client);
    if (!out) {
      await client.query('ROLLBACK');
      return c.json({ error: 'this key is no longer live' }, 409);
    }
    await client.query('COMMIT');
    // Only now: before the commit, a request could re-cache the old row.
    forgetApiKeyPrefix(out.oldPrefix);
    log.info({ userId, oldPrefix: out.oldPrefix, prefix: out.row.prefix }, 'api key re-issued');
    return c.json({ ok: true, plaintext: out.plaintext, key: mapKey(out.row, now) });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    log.error({ err, prefix, userId }, 'api key re-issue failed');
    return c.json({ error: 'Failed to re-issue the key' }, 500);
  } finally {
    client.release();
  }
});

// Delete is the owner's one way to end a key: a live one is revoked as it
// goes. The row stays — it is the staff record, and its usage history hangs
// off it — but the owner never sees it again.
apiKeysRouter.post('/api/api-keys/:prefix/delete', requireSupabaseJwt, async (c) => {
  const userId = currentUserId(c)!;
  const prefix = c.req.param('prefix');
  if (!PREFIX_RE.test(prefix)) return c.json({ error: 'invalid key' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    // One UPDATE scoped to the owner: someone else's prefix, a deleted key and
    // a second click all match nothing.
    if (!(await deleteApiKey(prefix, userId))) return c.json({ error: 'key not found' }, 404);
    log.info({ prefix, userId }, 'api key deleted by owner');
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err, prefix, userId }, 'api key delete failed');
    return c.json({ error: 'Failed to delete' }, 500);
  }
});

// ---------------------------------------------------------------------------
// Staff endpoints — owner | staff
// ---------------------------------------------------------------------------

apiKeysRouter.use('/api/staff/api-keys', requireRole(canManageUsers));
apiKeysRouter.use('/api/staff/api-keys/*', requireRole(canManageUsers));

apiKeysRouter.get('/api/staff/api-keys/requests', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  const raw = (new URL(c.req.url).searchParams.get('status') ?? 'pending').toLowerCase();
  const status = ['pending', 'approved', 'rejected', 'all'].includes(raw) ? raw : 'pending';
  try {
    const now = Date.now();
    const res =
      status === 'all'
        ? await pool.query<RequestRow>('SELECT * FROM api_key_requests ORDER BY created_at DESC LIMIT 200')
        : await pool.query<RequestRow>(
            'SELECT * FROM api_key_requests WHERE status = $1 ORDER BY created_at DESC LIMIT 200',
            [status],
          );
    const pendingCount = (
      await pool.query<{ n: string }>("SELECT COUNT(*)::int AS n FROM api_key_requests WHERE status = 'pending'")
    ).rows[0]?.n;
    const names = await displayNameMap(pool, res.rows.map((r) => r.user_id));
    return c.json({
      requests: res.rows.map((r) => mapRequest(r, now, names)),
      pendingCount: Number(pendingCount ?? 0),
    });
  } catch (err) {
    log.error({ err }, 'api key requests list failed');
    return c.json({ error: 'failed to load requests' }, 500);
  }
});

apiKeysRouter.post('/api/staff/api-keys/requests/:id/approve', async (c) => {
  const userId = c.get('userId') as string;
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id) || id <= 0) return c.json({ error: 'invalid id' }, 400);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const rpm = cleanRpm(raw?.rateLimitPerMin);
  if (!rpm.ok) return c.json({ error: `rateLimitPerMin must be a whole number from 1 to ${RPM_MAX}` }, 400);
  const exp = cleanExpires(raw?.expiresAt, Date.now());
  if (!exp.ok) return c.json({ error: 'expiresAt must be a future date within five years' }, 400);
  const note = cleanNote(raw?.note);
  if (!note.ok) return c.json({ error: `note must be at most ${NOTE_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const name = await getUsername(userId).catch(() => null);
    // Claim pending→approved atomically so two reviewers cannot both decide.
    const { rows } = await pool.query<RequestRow>(
      `UPDATE api_key_requests
          SET status = 'approved', rate_limit_per_min = $2, expires_at = $3, review_note = $4,
              reviewed_by = $5, reviewed_by_name = $6, reviewed_at = now()
        WHERE id = $1 AND status = 'pending'
        RETURNING *`,
      [id, rpm.rpm, exp.at, note.note, userId, name],
    );
    const req = rows[0];
    if (!req) {
      const cur = await pool.query<{ status: string }>('SELECT status FROM api_key_requests WHERE id = $1', [id]);
      if (!cur.rows[0]) return c.json({ error: 'not found' }, 404);
      return c.json({ error: `already ${cur.rows[0].status}` }, 409);
    }

    await notify(pool, {
      userId: req.user_id,
      type: 'api_key_approved',
      title: `API key "${req.name}" approved`,
      body: 'Open your profile to generate it — it is shown only once.' + (note.note ? ` Note from staff: ${note.note}` : ''),
      link: KEYS_LINK,
      meta: { requestId: req.id },
    });
    notifyStaff(pool, {
      kind: 'api_key_request',
      event: 'resolved',
      ref: String(req.id),
      title: `API key request #${req.id} · ${req.name}`,
      status: 'approved',
      actor: name,
    });
    log.info({ requestId: id, by: userId, rpm: rpm.rpm, expiresAt: exp.at }, 'api key request approved');
    return c.json({ ok: true, request: mapRequest(req, Date.now()) });
  } catch (err) {
    log.error({ err, id }, 'api key request approve failed');
    return c.json({ error: 'failed to approve' }, 500);
  }
});

apiKeysRouter.post('/api/staff/api-keys/requests/:id/reject', async (c) => {
  const userId = c.get('userId') as string;
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id) || id <= 0) return c.json({ error: 'invalid id' }, 400);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const note = cleanNote(raw?.note);
  if (!note.ok) return c.json({ error: `note must be at most ${NOTE_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const name = await getUsername(userId).catch(() => null);
    const { rows } = await pool.query<RequestRow>(
      `UPDATE api_key_requests
          SET status = 'rejected', review_note = $2, reviewed_by = $3, reviewed_by_name = $4, reviewed_at = now()
        WHERE id = $1 AND status = 'pending'
        RETURNING *`,
      [id, note.note, userId, name],
    );
    const req = rows[0];
    if (!req) return c.json({ error: 'not found or already reviewed' }, 409);

    await notify(pool, {
      userId: req.user_id,
      type: 'api_key_rejected',
      title: `API key "${req.name}" not approved`,
      body: note.note ?? 'Your request was declined. You can ask again with more detail.',
      link: KEYS_LINK,
      meta: { requestId: req.id },
    });
    notifyStaff(pool, {
      kind: 'api_key_request',
      event: 'resolved',
      ref: String(req.id),
      title: `API key request #${req.id} · ${req.name}`,
      status: 'rejected',
      actor: name,
    });
    log.info({ requestId: id, by: userId }, 'api key request rejected');
    return c.json({ ok: true, request: mapRequest(req, Date.now()) });
  } catch (err) {
    log.error({ err, id }, 'api key request reject failed');
    return c.json({ error: 'failed to reject' }, 500);
  }
});

// Take back an approval the user has not acted on. A grant is a standing
// permission to mint; without this, the only way to end one was to wait for
// the user to generate the key and then revoke it. Serialises against the
// claim through the row lock: whichever runs first wins, the other sees it.
apiKeysRouter.post('/api/staff/api-keys/requests/:id/withdraw', async (c) => {
  const userId = c.get('userId') as string;
  const id = Number(c.req.param('id'));
  if (!Number.isInteger(id) || id <= 0) return c.json({ error: 'invalid id' }, 400);
  const raw = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
  const note = cleanNote(raw?.note);
  if (!note.ok) return c.json({ error: `note must be at most ${NOTE_MAX} characters` }, 400);

  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const name = await getUsername(userId).catch(() => null);
    const { rows } = await pool.query<RequestRow>(
      `UPDATE api_key_requests
          SET status = 'rejected', review_note = $2, reviewed_by = $3, reviewed_by_name = $4, reviewed_at = now()
        WHERE id = $1 AND status = 'approved' AND key_id IS NULL
        RETURNING *`,
      [id, note.note, userId, name],
    );
    const req = rows[0];
    if (!req) return c.json({ error: 'not found, not approved, or already generated' }, 409);

    await notify(pool, {
      userId: req.user_id,
      type: 'api_key_rejected',
      title: `API key "${req.name}" approval withdrawn`,
      body: note.note ?? 'Staff withdrew the approval before the key was generated.',
      link: KEYS_LINK,
      meta: { requestId: req.id },
    });
    notifyStaff(pool, {
      kind: 'api_key_request',
      event: 'resolved',
      ref: String(req.id),
      title: `API key request #${req.id} · ${req.name}`,
      status: 'withdrawn',
      actor: name,
    });
    log.info({ requestId: id, by: userId }, 'api key approval withdrawn');
    return c.json({ ok: true, request: mapRequest(req, Date.now()) });
  } catch (err) {
    log.error({ err, id }, 'api key request withdraw failed');
    return c.json({ error: 'failed to withdraw' }, 500);
  }
});

apiKeysRouter.get('/api/staff/api-keys', async (c) => {
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const now = Date.now();
    const res = await pool.query<ApiKeyRow>('SELECT * FROM api_keys ORDER BY created_at DESC LIMIT 500');
    const names = await displayNameMap(pool, res.rows.map((k) => k.user_id));
    // The keys are the point of the page; their counts are a column. A usage
    // read that fails leaves the list up with usage null ("—" on the page).
    let usage: Map<string, { h24: UsageCounts; d7: UsageCounts }> | null = null;
    try {
      usage = await usageByKey(pool);
    } catch (err) {
      log.warn({ err }, 'api key usage totals failed');
    }
    return c.json({
      keys: res.rows.map((k) => ({
        ...mapKey(k, now, names),
        usage: usage ? (usage.get(k.id) ?? { h24: emptyCounts(), d7: emptyCounts() }) : null,
      })),
    });
  } catch (err) {
    log.error({ err }, 'api keys list failed');
    return c.json({ error: 'failed to load keys' }, 500);
  }
});

// One key's usage over a window: totals, a zero-filled series (hourly for
// 24h, by Sydney day for 7d and 30d), and the paths it hit most. Addressed by
// id, not prefix: a re-issue changes the prefix but not the key.
apiKeysRouter.get('/api/staff/api-keys/:id/usage', async (c) => {
  const id = c.req.param('id');
  if (!UUID_RE.test(id)) return c.json({ error: 'invalid key id' }, 400);
  const windowRaw = new URL(c.req.url).searchParams.get('window') ?? '24h';
  const window = USAGE_WINDOWS[windowRaw] ? windowRaw : '24h';
  const spec = USAGE_WINDOWS[window]!;
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const now = Date.now();
    const keyRes = await pool.query<ApiKeyRow>('SELECT * FROM api_keys WHERE id = $1', [id]);
    const key = keyRes.rows[0];
    if (!key) return c.json({ error: 'key not found' }, 404);

    const start = windowStartSql('$2');
    // Day buckets are Sydney days: the first is partial (the window starts on
    // the hour, not at midnight), so the bars still sum to the totals.
    const bucketExpr = spec.bucket === 'hour' ? 'hour' : `(hour AT TIME ZONE 'Australia/Sydney')::date`;
    const bucketSeries = spec.bucket === 'hour'
      ? `(SELECT generate_series(${start}, date_trunc('hour', now()), interval '1 hour') AS at)`
      : `(SELECT generate_series((${start} AT TIME ZONE 'Australia/Sydney')::date,
                                 (now() AT TIME ZONE 'Australia/Sydney')::date, interval '1 day')::date AS at)`;
    const [seriesRes, pathsRes, pathCountRes] = await Promise.all([
      pool.query<{ at: Date | string; cls: string | null; n: number }>(
        `WITH u AS (
           SELECT ${bucketExpr} AS at, ${USAGE_CLASS_SQL} AS cls, requests
             FROM api_key_usage_hourly
            WHERE key_id = $1 AND hour >= ${start}
         )
         SELECT b.at${spec.bucket === 'day' ? '::text' : ''} AS at, u.cls, COALESCE(SUM(u.requests), 0)::int AS n
           FROM ${bucketSeries} AS b
           LEFT JOIN u ON u.at = b.at
          GROUP BY b.at, u.cls
          ORDER BY b.at`,
        [id, spec.hours],
      ),
      pool.query<{ path: string; cls: string; n: number; last_hour: Date }>(
        `WITH top AS (
           SELECT path FROM api_key_usage_hourly
            WHERE key_id = $1 AND hour >= ${start}
            GROUP BY path ORDER BY SUM(requests) DESC, path LIMIT 50
         )
         SELECT u.path, ${USAGE_CLASS_SQL} AS cls, SUM(u.requests)::int AS n, MAX(u.hour) AS last_hour
           FROM api_key_usage_hourly u JOIN top USING (path)
          WHERE u.key_id = $1 AND u.hour >= ${start}
          GROUP BY 1, 2`,
        [id, spec.hours],
      ),
      pool.query<{ n: number }>(
        `SELECT COUNT(DISTINCT path)::int AS n FROM api_key_usage_hourly WHERE key_id = $1 AND hour >= ${start}`,
        [id, spec.hours],
      ),
    ]);

    const totals = emptyCounts();
    const series: Array<{ at: string } & UsageCounts> = [];
    for (const r of seriesRes.rows) {
      const at = r.at instanceof Date ? r.at.toISOString() : String(r.at);
      let point = series[series.length - 1];
      if (!point || point.at !== at) {
        point = { at, ...emptyCounts() };
        series.push(point);
      }
      if (r.cls) {
        addTo(point, r.cls, Number(r.n));
        addTo(totals, r.cls, Number(r.n));
      }
    }

    const byPath = new Map<string, { path: string; lastSeen: string | null } & UsageCounts>();
    for (const r of pathsRes.rows) {
      let p = byPath.get(r.path);
      if (!p) {
        p = { path: r.path, lastSeen: null, ...emptyCounts() };
        byPath.set(r.path, p);
      }
      addTo(p, r.cls, Number(r.n));
      const last = r.last_hour ? new Date(r.last_hour).toISOString() : null;
      if (last && (!p.lastSeen || last > p.lastSeen)) p.lastSeen = last;
    }
    const paths = Array.from(byPath.values()).sort((a, b) => b.requests - a.requests || a.path.localeCompare(b.path));

    return c.json({
      key: mapKey(key, now),
      window,
      bucket: spec.bucket,
      totals,
      series,
      paths,
      pathCount: Number(pathCountRes.rows[0]?.n ?? 0),
    });
  } catch (err) {
    log.error({ err, id }, 'api key usage failed');
    return c.json({ error: 'failed to load usage' }, 500);
  }
});

// By id, like the usage route: the owner may re-issue while the staff page
// is open, and the prefix on that page is then one no key has.
apiKeysRouter.post('/api/staff/api-keys/:id/revoke', async (c) => {
  const userId = c.get('userId') as string;
  const id = c.req.param('id');
  if (!UUID_RE.test(id)) return c.json({ error: 'invalid key id' }, 400);
  const pool = await getPool();
  if (!pool) return c.json({ error: 'database unavailable' }, 503);
  try {
    const cur = await pool.query<{ user_id: string; name: string; revoked_at: string | null }>(
      'SELECT user_id, name, revoked_at FROM api_keys WHERE id = $1',
      [id],
    );
    const key = cur.rows[0];
    if (!key) return c.json({ error: 'key not found' }, 404);
    // Only the request that actually flipped the key tells the owner — a
    // lost race must not produce a second "revoked by staff" bell.
    const prefix = key.revoked_at ? null : await revokeApiKeyById(id);
    if (!prefix) return c.json({ error: 'already revoked' }, 409);
    await notify(pool, {
      userId: key.user_id,
      type: 'api_key_revoked',
      title: `API key "${key.name}" was revoked by staff`,
      body: 'Requests using it will be refused from now on. Open a ticket if you think this is a mistake.',
      link: KEYS_LINK,
      exceptUserId: userId,
    });
    log.info({ id, prefix, by: userId, owner: key.user_id }, 'api key revoked by staff');
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err, id }, 'api key staff revoke failed');
    return c.json({ error: 'Failed to revoke' }, 500);
  }
});
