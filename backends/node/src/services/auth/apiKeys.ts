/**
 * Named API keys — credentials for scripts and the command line.
 *
 * A key is
 *     ak_<10-char lookup prefix>_<secret>
 * and the row stores only sha256(plaintext) plus the prefix, the same shape as
 * per-node tokens (nodeToken.ts): the plaintext is shown once at creation and
 * is never derivable again. Keys belong to a user, carry scopes (only `read`
 * exists today), a per-minute rate limit, an optional expiry, and can be
 * revoked. Keys are minted two ways: a user claims an approved request from
 * their profile (api/apiKeys.ts — the user mints, so only they ever see the
 * plaintext), or the owner issues one from the server
 * (scripts/issue-api-key.ts, scripts/revoke-api-key.ts).
 *
 * These keys work from curl and from servers and are REFUSED from web pages:
 * the gate (apiKey.ts) rejects any request that presents one together with a
 * browser `Origin` header. A page on another site cannot borrow a key, and a
 * key pasted into a page is caught the first time it is used.
 */
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import type { Pool } from 'pg';
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
  /** Lifetime request count, bumped by the usage flush (apiKeyUsage.ts). pg returns BIGINT as text. */
  requests_total: string | number;
  reissued_at: string | null;
  /** The owner removed it from their list. Always revoked as well. */
  deleted_at: string | null;
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

/** Anything that can run a parameterised query — the pool, or a client mid-transaction. */
export type Queryable = Pick<Pool, 'query'>;

/** A fresh prefix + plaintext pair, and the hash that is all the table keeps. */
function mintKeyMaterial(): { prefix: string; plaintext: string; hash: string } {
  const prefix = randomBytes(8).toString('base64url').replace(/[-_]/g, 'x').slice(0, LOOKUP_LEN);
  const plaintext = `${API_KEY_PREFIX}${prefix}_${randomBytes(SECRET_BYTES).toString('base64url')}`;
  return { prefix, plaintext, hash: sha256hex(plaintext) };
}

/**
 * Mint a key. The plaintext is returned ONCE for the caller to hand over; only
 * the hash lands in the table. Pass `q` to mint inside a caller's transaction
 * (the claim flow links the key to its request atomically).
 */
export async function createApiKey(opts: {
  userId: string;
  name: string;
  scopes?: string[];
  rateLimitPerMin?: number;
  expiresAt?: Date | null;
}, q?: Queryable): Promise<{ plaintext: string; row: ApiKeyRow }> {
  const runner: Queryable | null = q ?? (await getPool());
  if (!runner) throw new Error('no database');
  const { prefix, plaintext, hash } = mintKeyMaterial();
  const res = await runner.query<ApiKeyRow>(
    `INSERT INTO api_keys (user_id, name, prefix, key_hash, scopes, rate_limit_per_min, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     RETURNING *`,
    [
      opts.userId,
      opts.name,
      prefix,
      hash,
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

/**
 * Give a live key a new secret, and with it a new prefix. Everything else
 * about the key carries on: its id, name, limit, expiry and usage history,
 * and its per-minute window, which is keyed by id, so re-issuing cannot reset
 * the limit. The row no longer has the old prefix to be found by, so the old
 * plaintext stops resolving — once the caller has COMMITTED and then dropped
 * `oldPrefix` from the cache (forgetApiKeyPrefix). Dropping it any earlier
 * lets a request in that gap re-cache the old row from the uncommitted state.
 *
 * Returns null when the key is not live (revoked, expired or deleted) — the
 * UPDATE's own WHERE decides, so a revoke racing the re-issue wins cleanly.
 */
export async function reissueApiKey(id: string, q: Queryable): Promise<{ plaintext: string; row: ApiKeyRow; oldPrefix: string } | null> {
  const old = await q.query<{ prefix: string }>('SELECT prefix FROM api_keys WHERE id = $1', [id]);
  const oldPrefix = old.rows[0]?.prefix;
  if (!oldPrefix) return null;
  const { prefix, plaintext, hash } = mintKeyMaterial();
  const res = await q.query<ApiKeyRow>(
    `UPDATE api_keys SET prefix = $2, key_hash = $3, reissued_at = now()
      WHERE id = $1 AND revoked_at IS NULL AND deleted_at IS NULL
        AND (expires_at IS NULL OR expires_at > now())
      RETURNING *`,
    [id, prefix, hash],
  );
  const row = res.rows[0];
  if (!row) return null;
  return { plaintext, row, oldPrefix };
}

/** Drop a prefix's cached resolution, so its next use reads the table. */
export function forgetApiKeyPrefix(prefix: string): void {
  resolveCache.delete(prefix);
}

/**
 * The owner removes a key from their list. It is revoked if it was still live
 * (a deleted key never works), and stays in the table for staff and for its
 * usage history. False when the owner has no undeleted key with that prefix.
 */
export async function deleteApiKey(prefix: string, userId: string): Promise<boolean> {
  const pool = await getPool();
  if (!pool) throw new Error('no database');
  const res = await pool.query(
    `UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()), deleted_at = now()
      WHERE prefix = $1 AND user_id = $2 AND deleted_at IS NULL`,
    [prefix, userId],
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
/**
 * Hard ceiling on cached prefixes.
 *
 * The prefix is caller-supplied and the gate resolves before any rate limit
 * runs, so an unauthenticated client sending `ak_<10 random>_x` with a new
 * prefix each time added a permanent entry per request. Every one of those
 * requests is a 403, so the attack showed up nowhere but in the heap, and it
 * grew until the process was OOM-killed. Live keys number in the dozens, so
 * anything above that is misses — the cap holds them to a fixed cost without
 * giving up the "a guessed key must not cost a query per attempt" property.
 */
const RESOLVE_CACHE_MAX = 1_000;
const resolveCache = new Map<string, { ts: number; row: ApiKeyRow | null }>();

/** Expire on write (as roles.ts does), then cap. Map is in insertion order, so
 *  the cap drops the least-recently-written prefix. */
function rememberResolve(prefix: string, ts: number, row: ApiKeyRow | null): void {
  for (const [p, entry] of resolveCache) {
    if (ts - entry.ts >= RESOLVE_CACHE_TTL_MS) resolveCache.delete(p);
  }
  // Re-insert rather than overwrite so a refreshed entry counts as the newest.
  resolveCache.delete(prefix);
  resolveCache.set(prefix, { ts, row });
  while (resolveCache.size > RESOLVE_CACHE_MAX) {
    const oldest = resolveCache.keys().next().value;
    if (oldest === undefined) break;
    resolveCache.delete(oldest);
  }
}

export function _clearApiKeyCache(): void {
  resolveCache.clear();
  lastUsedTouch.clear();
  keyCounts.clear();
}

/** Test seam — assert the cache stays bounded under a flood of new prefixes. */
export function _apiKeyCacheSize(): number {
  return resolveCache.size;
}

/**
 * A revoked or expired key still names its row: the secret matched, so the
 * request is that key's holder, and the gate counts it against the key
 * (apiKeyUsage.ts) — a dead key that is still being used is worth seeing.
 */
export type ApiKeyResolve =
  | { ok: true; row: ApiKeyRow }
  | { ok: false; reason: 'revoked' | 'expired'; row: ApiKeyRow }
  | { ok: false; reason: 'bad_key' | 'unavailable' };

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
    rememberResolve(prefix, now, row);
  }

  if (!row || !safeEqual(supplied, row.key_hash)) return { ok: false, reason: 'bad_key' };
  if (row.revoked_at) return { ok: false, reason: 'revoked', row };
  if (row.expires_at && new Date(row.expires_at).getTime() <= now) return { ok: false, reason: 'expired', row };
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
