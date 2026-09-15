/**
 * Personal referral codes for contributors.
 *
 *   GET /api/referral-code — the caller's own stable referral code plus
 *                            simple stats (signups that used it / approved).
 *                            Created on first call. Gated on canRefer
 *                            (feeder:radio, feeder:pager, wire:contributor,
 *                            map:editor, owner) — the code identifies its
 *                            owner as a voucher, so only contributor roles
 *                            get one.
 *
 * The code lands on signup requests as editor_requests.referred_by(_name)
 * via POST /api/editor-requests (see editor.ts) — attribution only, the
 * review flow is unchanged.
 */
import { Hono } from 'hono';
import { randomBytes } from 'node:crypto';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { requireRole, canRefer } from '../services/auth/roles.js';

export const referralsRouter = new Hono();

// Human-typeable, unambiguous alphabet: no 0/O, 1/I/L. Uppercase-only —
// signup lookups uppercase the input, so codes are case-insensitive to use.
const REFERRAL_CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const REFERRAL_CODE_LENGTH = 8;

/** CSPRNG code with rejection sampling (no modulo bias — 256 % 31 != 0). */
export function generateReferralCode(): string {
  const n = REFERRAL_CODE_ALPHABET.length; // 31
  const cutoff = 256 - (256 % n); // largest unbiased multiple of n
  let code = '';
  while (code.length < REFERRAL_CODE_LENGTH) {
    const b = randomBytes(1)[0]!;
    if (b >= cutoff) continue; // reject the biased tail, draw again
    code += REFERRAL_CODE_ALPHABET[b % n];
  }
  return code;
}

referralsRouter.get('/api/referral-code', requireRole(canRefer), async (c) => {
  const userId = c.get('userId') as string; // requireRole guarantees presence
  try {
    const pool = await getPool();
    if (!pool) return c.json({ error: 'database unavailable' }, 503);

    let code: string | null = null;
    const existing = await pool.query<{ code: string }>(
      'SELECT code FROM referral_codes WHERE user_id = $1',
      [userId],
    );
    code = existing.rows[0]?.code ?? null;

    // First call mints the code. ON CONFLICT (user_id) DO UPDATE is a no-op
    // that makes RETURNING yield the winner's code if a concurrent request
    // raced us; a collision on the code's own UNIQUE constraint throws, so
    // retry with a fresh draw (31^8 codes — collisions are lottery-grade).
    for (let attempt = 0; code === null && attempt < 3; attempt++) {
      try {
        const inserted = await pool.query<{ code: string }>(
          `INSERT INTO referral_codes (user_id, code) VALUES ($1, $2)
           ON CONFLICT (user_id) DO UPDATE SET user_id = referral_codes.user_id
           RETURNING code`,
          [userId, generateReferralCode()],
        );
        code = inserted.rows[0]?.code ?? null;
      } catch (err) {
        if (attempt === 2) throw err;
      }
    }
    if (!code) return c.json({ error: 'failed to allocate code' }, 500);

    const stats = await pool.query<{ uses: number; approved: number }>(
      `SELECT COUNT(*)::int AS uses,
              COUNT(*) FILTER (WHERE status = 'approved')::int AS approved
         FROM editor_requests WHERE referred_by = $1`,
      [userId],
    );
    const row = stats.rows[0];
    return c.json({
      code,
      uses: row?.uses ?? 0,
      approved: row?.approved ?? 0,
    });
  } catch (err) {
    log.error({ err, userId }, 'referral-code lookup failed');
    return c.json({ error: 'failed to load referral code' }, 500);
  }
});
