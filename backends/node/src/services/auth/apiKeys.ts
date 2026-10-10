/**
 * Named API keys — credentials for scripts and the command line.
 *
 * A key is
 *     ak_<10-char lookup prefix>_<secret>
 * and the row stores only sha256(plaintext) plus the prefix, the same shape as
 * per-node tokens (nodeToken.ts): the plaintext is shown once at creation and
 * is never derivable again. Keys belong to a user, carry scopes (only `read`
 * exists today), a per-minute rate limit, an optional expiry, and can be
 * revoked. Issuing and revoking is done by the owner from the server
 * (scripts/issue-api-key.ts, scripts/revoke-api-key.ts) until there is a UI.
 *
 * These keys work from curl and from servers and are REFUSED from web pages:
 * the gate (apiKey.ts) rejects any request that presents one together with a
 * browser `Origin` header. A page on another site cannot borrow a key, and a
 * key pasted into a page is caught the first time it is used.
 */
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { getPool } from '../../db/pool.js';

export const API_KEY_PREFIX = 'ak_';
const LOOKUP_LEN = 10;
const SECRET_BYTES = 24; // 32 base64url chars

export const DEFAULT_RATE_LIMIT_PER_MIN = 120;

export interface ApiKeyRow {
  id: string;
  user_id: string;
  name: string;
  prefix: string;
  key_hash: string;
  scopes: string[];
  rate_limit_per_min: number;
  created_at: string;
  last_used_at: string | null;
  expires_at: string | null;
  revoked_at: string | null;
}

function sha256hex(s: string): string {
  return createHash('sha256').update(s).digest('hex');
}

function safeEqual(a: string, b: string): boolean {
  const aBuf = Buffer.from(a, 'utf8');
  const bBuf = Buffer.from(b, 'utf8');
  if (aBuf.length !== bBuf.length) return false;
  return timingSafeEqual(aBuf, bBuf);
}

/** `ak_<prefix>_<secret>` → the prefix, or null for anything else. */
export function lookupPrefix(plaintext: string): string | null {
  if (!plaintext.startsWith(API_KEY_PREFIX)) return null;
  const rest = plaintext.slice(API_KEY_PREFIX.length);
  const sep = rest.indexOf('_');
  if (sep !== LOOKUP_LEN) return null;
  return rest.slice(0, sep);
}

export function looksLikeApiKey(s: string | undefined | null): boolean {
  return !!s && s.startsWith(API_KEY_PREFIX);
}

/**
 * Mint a key. The plaintext is returned ONCE for the caller to hand over; only
 * the hash lands in the table.
 */
export async function createApiKey(opts: {
  userId: string;
  name: string;
  scopes?: string[];
  rateLimitPerMin?: number;
  expiresAt?: Date | null;
}): Promise<{ plaintext: string; row: ApiKeyRow }> {
  const pool = await getPool();
  if (!pool) throw new Error('no database');
  const prefix = randomBytes(8).toString('base64url').replace(/[-_]/g, 'x').slice(0, LOOKUP_LEN);
  const plaintext = `${API_KEY_PREFIX}${prefix}_${randomBytes(SECRET_BYTES).toString('base64url')}`;
  const res = await pool.query<ApiKeyRow>(
    `INSERT INTO api_keys (user_id, name, prefix, key_hash, scopes, rate_limit_per_min, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     RETURNING *`,
    [
      opts.userId,
      opts.name,
      prefix,
      sha256hex(plaintext),
      opts.scopes ?? ['read'],
      opts.rateLimitPerMin ?? DEFAULT_RATE_LIMIT_PER_MIN,
      opts.expiresAt ?? null,
    ],
  );
  const row = res.rows[0];
  if (!row) throw new Error('insert returned no row');
  return { plaintext, row };
}

/** Revoke by prefix. Returns false when no live key had that prefix. */
export async function revokeApiKey(prefix: string): Promise<boolean> {
  const pool = await getPool();
  if (!pool) throw new Error('no database');
  const res = await pool.query(
    `UPDATE api_keys SET revoked_at = now() WHERE prefix = $1 AND revoked_at IS NULL`,
    [prefix],
  );
  resolveCache.delete(prefix);
  return (res.rowCount ?? 0) > 0;
}

export async function listApiKeys(userId?: string): Promise<ApiKeyRow[]> {
  const pool = await getPool();
  if (!pool) throw new Error('no database');
  const res = userId
    ? await pool.query<ApiKeyRow>(`SELECT * FROM api_keys WHERE user_id = $1 ORDER BY created_at`, [userId])
    : await pool.query<ApiKeyRow>(`SELECT * FROM api_keys ORDER BY created_at`);
  return res.rows;
}

// Resolution cache, keyed by prefix. Caches misses too: a key that is being
// guessed or was just revoked should not cost a query per attempt.
const RESOLVE_CACHE_TTL_MS = 60_000;
const resolveCache = new Map<string, { ts: number; row: ApiKeyRow | null }>();

export function _clearApiKeyCache(): void {
  resolveCache.clear();
  lastUsedTouch.clear();
  keyCounts.clear();
}

export type ApiKeyResolve =
  | { ok: true; row: ApiKeyRow }
  | { ok: false; reason: 'bad_key' | 'revoked' | 'expired' | 'unavailable' };

/** Presented plaintext → its row, or why not. Constant-time on the hash. */
export async function resolveApiKey(plaintext: string, now = Date.now()): Promise<ApiKeyResolve> {
  const prefix = lookupPrefix(plaintext);
  if (!prefix) return { ok: false, reason: 'bad_key' };
  const supplied = sha256hex(plaintext);

  let row: ApiKeyRow | null;
  const cached = resolveCache.get(prefix);
  if (cached && now - cached.ts < RESOLVE_CACHE_TTL_MS) {
    row = cached.row;
  } else {
    const pool = await getPool();
    if (!pool) return { ok: false, reason: 'unavailable' };
    const res = await pool.query<ApiKeyRow>(`SELECT * FROM api_keys WHERE prefix = $1`, [prefix]);
    row = res.rows[0] ?? null;
    resolveCache.set(prefix, { ts: now, row });
  }

  if (!row || !safeEqual(supplied, row.key_hash)) return { ok: false, reason: 'bad_key' };
  if (row.revoked_at) return { ok: false, reason: 'revoked' };
  if (row.expires_at && new Date(row.expires_at).getTime() <= now) return { ok: false, reason: 'expired' };
  return { ok: true, row };
}

// Per-key request limiter. One fixed window per key id; the limit is the row's.
const keyCounts = new Map<string, { count: number; resetAt: number }>();

/** Count one request against the key; false when it is over its own limit. */
export function apiKeyRequestOk(row: ApiKeyRow, now = Date.now()): boolean {
  const cur = keyCounts.get(row.id);
  if (!cur || now > cur.resetAt) {
    keyCounts.set(row.id, { count: 1, resetAt: now + 60_000 });
    return true;
  }
  cur.count += 1;
  return cur.count <= row.rate_limit_per_min;
}

// last_used_at is written at most once a minute per key — it is a "when was
// this last seen" hint, not an access log, and must not cost a write per call.
const lastUsedTouch = new Map<string, number>();

export function touchLastUsed(row: ApiKeyRow, now = Date.now()): void {
  const prev = lastUsedTouch.get(row.id) ?? 0;
  if (now - prev < 60_000) return;
  lastUsedTouch.set(row.id, now);
  void getPool().then((pool) => {
    if (!pool) return;
    return pool.query(`UPDATE api_keys SET last_used_at = now() WHERE id = $1`, [row.id]);
  }).catch(() => undefined);
}

/** Test seam. */
export function _resetApiKeyLimits(): void {
  keyCounts.clear();
  lastUsedTouch.clear();
}
