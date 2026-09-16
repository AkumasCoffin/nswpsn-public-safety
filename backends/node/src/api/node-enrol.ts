/**
 * POST /api/node-enrol — an agent trades its single-use enrolment code for a
 * node token.
 *
 * Called once, on an agent's first run, before it has any credential. So this
 * route is authenticated BY the code itself and by nothing else, which makes
 * its guards the only thing standing between a guessed code and a node:
 *
 *   - the code is 128 bits of randomness, so guessing is not a threat model;
 *   - it is single-use and expires, so a leaked installer stops working;
 *   - attempts are rate-limited per IP, so an unauthenticated endpoint cannot
 *     be used to hammer the database;
 *   - failures say only which of the three things went wrong (unknown,
 *     expired, or could-not-check), never anything about a node.
 *
 * Deliberately NOT behind the site API key: the agent is a fresh install and
 * has no keys of any kind yet. The '/api/node-enrol' prefix is exempted
 * alongside '/api/node-ingest/' for the same reason.
 */
import { Hono } from 'hono';
import { z } from 'zod';
import { log } from '../lib/log.js';
import { consumeEnrolCode } from '../services/auth/nodeEnrol.js';

export const nodeEnrolRouter = new Hono();

const EnrolSchema = z.object({
  code: z.string().min(8).max(128),
  // The machine id the agent will use from now on. Same shape the WS upgrade
  // demands, validated here too so a malformed one is refused before it can be
  // written to the row.
  installId: z.string().regex(/^[A-Za-z0-9._-]{8,64}$/),
  // Informational: lets a kind mismatch be logged at the moment it happens,
  // rather than surfacing later as a node that behaves oddly.
  kind: z.string().max(32).optional(),
});

/**
 * Per-IP rate limit. Enrolment is a once-per-install event, so anything beyond
 * a handful an hour from one address is either a broken retry loop or someone
 * working through guesses; both deserve the same answer.
 */
const ATTEMPT_WINDOW_MS = 60 * 60 * 1000;
const MAX_ATTEMPTS = 20;
const attempts = new Map<string, { count: number; resetAt: number }>();

function attemptOk(ip: string): boolean {
  const now = Date.now();
  const cur = attempts.get(ip);
  if (!cur || now > cur.resetAt) {
    attempts.set(ip, { count: 1, resetAt: now + ATTEMPT_WINDOW_MS });
    return true;
  }
  cur.count += 1;
  return cur.count <= MAX_ATTEMPTS;
}

/** Test seam. */
export function _resetEnrolAttempts(): void {
  attempts.clear();
}

function clientIp(c: import('hono').Context): string {
  return (
    c.req.header('cf-connecting-ip') ??
    c.req.header('x-forwarded-for')?.split(',')[0]?.trim() ??
    'unknown'
  );
}

nodeEnrolRouter.post('/api/node-enrol', async (c) => {
  const ip = clientIp(c);
  if (!attemptOk(ip)) {
    log.warn({ ip }, 'node enrol: rate limited');
    return c.json({ error: 'too many enrolment attempts' }, 429);
  }

  const parsed = EnrolSchema.safeParse(await c.req.json().catch(() => ({})));
  if (!parsed.success) {
    return c.json({ error: 'code and installId are required' }, 400);
  }
  const { code, installId, kind } = parsed.data;

  try {
    const r = await consumeEnrolCode(code, installId);
    if (!r.ok) {
      if (r.reason === 'unavailable') {
        // 503, not 401/400: the code could not be checked, so the agent should
        // retry rather than treat its one credential as dead.
        return c.json({ error: 'node registry unavailable' }, 503);
      }
      log.warn({ ip, installId, reason: r.reason }, 'node enrol: refused');
      return c.json(
        {
          error:
            r.reason === 'expired'
              ? 'this enrolment code has expired — download the installer again'
              : 'unknown enrolment code — download the installer again',
        },
        401,
      );
    }

    if (kind && kind !== r.kind) {
      // Warn, never reject: the node row is authoritative about what it is, and
      // refusing here would strand an install over a cosmetic disagreement.
      log.warn({ nodeId: r.nodeId, nodeKind: r.kind, agentKind: kind }, 'node enrol: kind mismatch');
    }
    log.info({ nodeId: r.nodeId, kind: r.kind, installId }, 'node enrol: issued a token');
    c.header('Cache-Control', 'no-store');
    return c.json({ token: r.token, nodeId: r.nodeId, kind: r.kind });
  } catch (err) {
    log.error({ err }, 'node enrol failed');
    return c.json({ error: 'enrolment failed' }, 500);
  }
});
