/**
 * Per-node feeder credentials.
 *
 * Each node has its OWN token, minted when the node is created in the panel and
 * baked into that node's installer. The token is a random
 *     npsn_<40 hex>            (160 bits; format unchanged so the installer
 *                               parsers keep working)
 * of which only the sha256 (token_hash) + a lookup prefix (token_prefix) are
 * stored, on the node row itself. The plaintext is returned ONCE at create /
 * rotate time and is never re-derivable — re-downloading an installer requires a
 * rotate. Deleting a node hard-revokes its token (no auto-link recreates it);
 * rotating replaces the hash. This is the per-node revocation the old per-user
 * HMAC token could not give.
 *
 * Validity is additionally gated at resolve time on the owner STILL holding
 * `radio_contributor`, so losing the role stops all of that user's nodes.
 */
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { getPool } from '../../db/pool.js';
import { hasRole } from './roles.js';

const TOKEN_PREFIX = 'npsn_';
// Random hex after the 'npsn_' prefix. 40 hex = 160 bits (20 random bytes).
const TOKEN_BYTES = 20;
// Chars used as the DB lookup key: 'npsn_' + 16 hex = 64 bits, collision-safe.
const LOOKUP_PREFIX_LEN = TOKEN_PREFIX.length + 16;
// Chars shown in logs — enough to tell nodes apart, not the whole key.
const LOG_PREFIX_LEN = TOKEN_PREFIX.length + 7;

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

/**
 * Mint a fresh random per-node token. Returns the plaintext (to hand to the
 * operator / bake into the installer, ONCE) plus the hash + prefix to store on
 * the node row. Nothing here is derivable from the token later.
 */
export function mintNodeToken(): { token: string; tokenHash: string; tokenPrefix: string } {
  const token = TOKEN_PREFIX + randomBytes(TOKEN_BYTES).toString('hex');
  return { token, tokenHash: sha256hex(token), tokenPrefix: lookupPrefix(token) };
}

// Short-lived cache so a burst of call uploads from one node doesn't hit the
// DB per request. Keyed by lookup-prefix. Cleared wholesale on rotate/delete.
interface CacheEntry {
  ts: number;
  nodeId: string;
  userId: string;
  kind: string;
  enabled: boolean;
  feedEnabled: boolean;
  installId: string | null;
  tokenHash: string;
}
const RESOLVE_CACHE_TTL_MS = 15_000;
const resolveCache = new Map<string, CacheEntry>();

export function _clearNodeTokenCache(): void {
  resolveCache.clear();
}

export type ResolveResult =
  | {
      ok: true;
      nodeId: string;
      userId: string;
      kind: string;
      enabled: boolean;
      feedEnabled: boolean;
      installId: string | null;
    }
  | { ok: false; reason: 'bad_token' | 'no_role' };

/**
 * Verify a presented per-node token → the node it belongs to, gated on the
 * owner still holding radio_contributor. Compares against the stored hash
 * (constant-time); no secret needed on the hot path.
 */
export async function resolveNodeToken(token: string): Promise<ResolveResult> {
  if (!token || !token.startsWith(TOKEN_PREFIX)) {
    return { ok: false, reason: 'bad_token' };
  }
  const prefix = lookupPrefix(token);
  const suppliedHash = sha256hex(token);

  let entry: CacheEntry | null = null;
  const cached = resolveCache.get(prefix);
  if (cached && Date.now() - cached.ts < RESOLVE_CACHE_TTL_MS) {
    if (safeEqualHex(suppliedHash, cached.tokenHash)) entry = cached;
  } else {
    const pool = await getPool();
    if (!pool) return { ok: false, reason: 'bad_token' };
    const res = await pool.query<{
      id: string;
      user_id: string;
      kind: string;
      enabled: boolean;
      feed_enabled: boolean;
      install_id: string | null;
      token_hash: string | null;
    }>(
      `SELECT id, user_id, kind, enabled, feed_enabled, install_id, token_hash
         FROM nodes WHERE token_prefix = $1`,
      [prefix],
    );
    const row = res.rows[0];
    if (row && row.token_hash) {
      const e: CacheEntry = {
        ts: Date.now(),
        nodeId: row.id,
        userId: row.user_id,
        kind: row.kind,
        enabled: row.enabled,
        feedEnabled: row.feed_enabled,
        installId: row.install_id,
        tokenHash: row.token_hash,
      };
      resolveCache.set(prefix, e);
      if (safeEqualHex(suppliedHash, row.token_hash)) entry = e;
    }
  }

  if (!entry) return { ok: false, reason: 'bad_token' };
  // Ongoing role gate, matched to the node's KIND: a pager node needs
  // feeder:pager, radio (and adsb) needs feeder:radio. Losing the relevant role
  // stops that user's nodes of that kind.
  const neededRole = entry.kind === 'pager' ? 'feeder:pager' : 'feeder:radio';
  if (!(await hasRole(entry.userId, [neededRole]))) {
    return { ok: false, reason: 'no_role' };
  }
  return {
    ok: true,
    nodeId: entry.nodeId,
    userId: entry.userId,
    kind: entry.kind,
    enabled: entry.enabled,
    feedEnabled: entry.feedEnabled,
    installId: entry.installId,
  };
}
