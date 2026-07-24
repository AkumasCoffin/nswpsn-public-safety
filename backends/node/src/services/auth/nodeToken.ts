/**
 * Feeder-node credentials.
 *
 * Provisioning is role-driven (see 034_feeder_nodes.sql). Every user with
 * the `radio_contributor` role gets ONE long-lived token, minted lazily the
 * first time they hit feeder.html / a download endpoint.
 *
 * The token is NOT stored. It is
 *     npsn_<first 40 hex of HMAC-SHA256(FEEDER_TOKEN_SECRET, "userId:version")>
 * so /api/feeder/download can regenerate it on demand, while feeder_tokens
 * holds only its sha256 (for a constant-time auth compare that needs no
 * secret on the hot path) and a prefix (for O(1) lookup + logging). Rotation
 * bumps `token_version` → the HMAC input changes → a brand-new token/hash/
 * prefix, instantly killing the old one.
 *
 * Validity is additionally gated at resolve time on the user STILL holding
 * `radio_contributor` (via the cached hasRole), so removing the role
 * unlinks their nodes with no extra bookkeeping.
 */
import { createHmac, createHash, timingSafeEqual } from 'node:crypto';
import { config } from '../../config.js';
import { getPool } from '../../db/pool.js';
import { hasRole } from './roles.js';

const TOKEN_PREFIX = 'npsn_';
// Hex chars of HMAC kept after the 'npsn_' prefix. 40 hex = 160 bits.
const TOKEN_HEX_LEN = 40;
// Chars used as the DB lookup key: 'npsn_' + 16 hex = 64 bits, collision-safe.
const LOOKUP_PREFIX_LEN = TOKEN_PREFIX.length + 16;
// Chars shown in logs — enough to tell contributors apart, not the whole key.
const LOG_PREFIX_LEN = TOKEN_PREFIX.length + 7;

export function feederTokensConfigured(): boolean {
  return !!config.FEEDER_TOKEN_SECRET;
}

function computeToken(userId: string, version: number): string {
  const secret = config.FEEDER_TOKEN_SECRET;
  if (!secret) throw new Error('FEEDER_TOKEN_SECRET not configured');
  const mac = createHmac('sha256', secret)
    .update(`${userId}:${version}`)
    .digest('hex');
  return TOKEN_PREFIX + mac.slice(0, TOKEN_HEX_LEN);
}

function sha256hex(s: string): string {
  return createHash('sha256').update(s).digest('hex');
}

function lookupPrefix(token: string): string {
  return token.slice(0, LOOKUP_PREFIX_LEN);
}

/** First few chars of a token, safe to log. */
export function logPrefix(token: string): string {
  return token.slice(0, LOG_PREFIX_LEN);
}

function safeEqualHex(a: string, b: string): boolean {
  const aBuf = Buffer.from(a, 'utf8');
  const bBuf = Buffer.from(b, 'utf8');
  if (aBuf.length !== bBuf.length) return false;
  return timingSafeEqual(aBuf, bBuf);
}

// Short-lived cache so a burst of call uploads from one node doesn't hit the
// DB per request. Keyed by lookup-prefix. Cleared wholesale on rotation.
interface CacheEntry {
  ts: number;
  userId: string;
  tokenHash: string;
}
const RESOLVE_CACHE_TTL_MS = 15_000;
const resolveCache = new Map<string, CacheEntry>();

export function _clearFeederTokenCache(): void {
  resolveCache.clear();
}

export type ResolveResult =
  | { ok: true; userId: string }
  | { ok: false; reason: 'unconfigured' | 'bad_token' | 'no_role' };

/**
 * Verify a presented feeder token and confirm the owner still holds
 * radio_contributor. Returns the owning user id, or a reason for rejection.
 * Does NOT require FEEDER_TOKEN_SECRET (compares against the stored hash).
 */
export async function resolveFeederToken(token: string): Promise<ResolveResult> {
  if (!token || !token.startsWith(TOKEN_PREFIX)) {
    return { ok: false, reason: 'bad_token' };
  }
  const prefix = lookupPrefix(token);
  const suppliedHash = sha256hex(token);

  let userId: string | null = null;
  const cached = resolveCache.get(prefix);
  if (cached && Date.now() - cached.ts < RESOLVE_CACHE_TTL_MS) {
    if (safeEqualHex(suppliedHash, cached.tokenHash)) userId = cached.userId;
  } else {
    const pool = await getPool();
    if (!pool) return { ok: false, reason: 'bad_token' };
    const res = await pool.query<{ user_id: string; token_hash: string }>(
      'SELECT user_id, token_hash FROM feeder_tokens WHERE token_prefix = $1',
      [prefix],
    );
    const row = res.rows[0];
    if (row) {
      resolveCache.set(prefix, {
        ts: Date.now(),
        userId: row.user_id,
        tokenHash: row.token_hash,
      });
      if (safeEqualHex(suppliedHash, row.token_hash)) userId = row.user_id;
    }
  }

  if (!userId) return { ok: false, reason: 'bad_token' };
  if (!(await hasRole(userId, ['radio_contributor']))) {
    return { ok: false, reason: 'no_role' };
  }
  return { ok: true, userId };
}

/**
 * Idempotently ensure a feeder token exists for a contributor and return the
 * plaintext (regenerated from the stored version). Callers MUST have already
 * verified the caller owns this userId / is staff. Throws if the secret is
 * unset — callers should gate on feederTokensConfigured() and 503 first.
 */
export async function mintFeederToken(
  userId: string,
): Promise<{ token: string; prefix: string }> {
  const pool = await getPool();
  if (!pool) throw new Error('DATABASE_URL not configured');

  const existing = await pool.query<{ token_version: number }>(
    'SELECT token_version FROM feeder_tokens WHERE user_id = $1',
    [userId],
  );
  let version = existing.rows[0]?.token_version;
  if (version === undefined) {
    version = 1;
    const token = computeToken(userId, version);
    // ON CONFLICT handles a race where two requests mint at once — the row
    // is identical either way (deterministic token), so DO NOTHING is safe.
    await pool.query(
      `INSERT INTO feeder_tokens (user_id, token_hash, token_prefix, token_version)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (user_id) DO NOTHING`,
      [userId, sha256hex(token), lookupPrefix(token), version],
    );
    return { token, prefix: logPrefix(token) };
  }
  const token = computeToken(userId, version);
  return { token, prefix: logPrefix(token) };
}

/**
 * Rotate a contributor's token: bump the version, recompute, persist the new
 * hash/prefix, and return the new plaintext. The old token stops resolving
 * immediately (its prefix is no longer in the table).
 */
export async function rotateFeederToken(userId: string): Promise<{ token: string }> {
  const pool = await getPool();
  if (!pool) throw new Error('DATABASE_URL not configured');

  const cur = await pool.query<{ token_version: number }>(
    'SELECT token_version FROM feeder_tokens WHERE user_id = $1',
    [userId],
  );
  const version = (cur.rows[0]?.token_version ?? 0) + 1;
  const token = computeToken(userId, version);
  await pool.query(
    `INSERT INTO feeder_tokens (user_id, token_hash, token_prefix, token_version, rotated_at)
     VALUES ($1, $2, $3, $4, now())
     ON CONFLICT (user_id) DO UPDATE
       SET token_hash = EXCLUDED.token_hash,
           token_prefix = EXCLUDED.token_prefix,
           token_version = EXCLUDED.token_version,
           rotated_at = now()`,
    [userId, sha256hex(token), lookupPrefix(token), version],
  );
  resolveCache.clear();
  return { token };
}
