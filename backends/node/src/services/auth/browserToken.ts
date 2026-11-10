/**
 * Browser session tokens — what a page holds instead of the site API key.
 *
 * A static site cannot keep a secret: anything the browser sends can be read
 * out of the network tab. So the browser is given something that is worthless
 * anywhere else — a short-lived token bound to the client that asked for it:
 *
 *     bt1.<expiry unix secs>.<nonce>.<signature>
 *     signature = HMAC-SHA256(secret, exp | nonce | H(ip scope) | H(user agent))
 *
 * Nothing is stored server-side; verification recomputes the signature from
 * the presenting request's own address and User-Agent. Copy the token to
 * another machine and it fails; wait TTL and it fails; alter a byte and it
 * fails. The secret never leaves the server.
 *
 * The address is bound at /24 (v4) or /48 (v6), not exactly — see
 * clientIp.ipScope for why. The gate ALSO requires a first-party Origin on
 * every request that presents one of these (firstParty.ts), so a token is not
 * enough on its own even inside its window.
 */
import { createHmac, createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { config } from '../../config.js';
import { log } from '../../lib/log.js';
import { ipScope } from './clientIp.js';

export const BROWSER_TOKEN_PREFIX = 'bt1.';

let warnedDerived = false;

/**
 * The signing secret. BROWSER_TOKEN_SECRET when set; otherwise derived from
 * NSWPSN_API_KEY so a deploy that has not yet added the new variable cannot
 * lock every page out. Derived is weaker only in that rotating the site key
 * also rotates every token — it is not predictable without the key.
 */
function secret(): Buffer {
  if (config.BROWSER_TOKEN_SECRET) return Buffer.from(config.BROWSER_TOKEN_SECRET, 'utf8');
  if (!warnedDerived) {
    warnedDerived = true;
    log.warn('BROWSER_TOKEN_SECRET is not set — deriving the browser-token secret from NSWPSN_API_KEY; set it in .env');
  }
  return createHash('sha256').update('browser-token:' + config.NSWPSN_API_KEY).digest();
}

function h(s: string): string {
  return createHash('sha256').update(s).digest('base64url');
}

export interface Binding {
  ip: string;
  ua: string;
}

function sign(exp: number, nonce: string, b: Binding): string {
  return createHmac('sha256', secret())
    .update(`${exp}|${nonce}|${h(ipScope(b.ip))}|${h(b.ua)}`)
    .digest('base64url');
}

export function tokenTtlSecs(): number {
  return config.BROWSER_TOKEN_TTL_SECS;
}

/** Mint a token for this caller. Returns the token and its lifetime in seconds. */
export function mintBrowserToken(b: Binding, now = Date.now()): { token: string; expiresIn: number } {
  const ttl = tokenTtlSecs();
  const exp = Math.floor(now / 1000) + ttl;
  const nonce = randomBytes(9).toString('base64url');
  return { token: `${BROWSER_TOKEN_PREFIX}${exp}.${nonce}.${sign(exp, nonce, b)}`, expiresIn: ttl };
}

export type VerifyResult = 'ok' | 'expired' | 'mismatch' | 'bad';

/**
 * Check a presented token against the presenting request. 'mismatch' covers a
 * token minted for a different client or a tampered one — the two are not
 * distinguished, deliberately: neither deserves a more specific answer.
 */
export function verifyBrowserToken(token: string, b: Binding, now = Date.now()): VerifyResult {
  if (!token.startsWith(BROWSER_TOKEN_PREFIX)) return 'bad';
  const parts = token.slice(BROWSER_TOKEN_PREFIX.length).split('.');
  if (parts.length !== 3) return 'bad';
  const [expStr, nonce, sig] = parts as [string, string, string];
  if (!/^\d{1,12}$/.test(expStr) || !nonce || !sig) return 'bad';
  const exp = Number(expStr);
  const expected = sign(exp, nonce, b);
  const a = Buffer.from(sig, 'utf8');
  const e = Buffer.from(expected, 'utf8');
  if (a.length !== e.length || !timingSafeEqual(a, e)) return 'mismatch';
  if (exp * 1000 <= now) return 'expired';
  return 'ok';
}

/** Test seam: forget the one-time derived-secret warning. */
export function _resetBrowserTokenWarn(): void {
  warnedDerived = false;
}
