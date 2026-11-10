/**
 * POST /api/session/token — a page asks for its browser session token.
 *
 * Public (no credential to present yet), and the only route that mints. Three
 * things stand between a caller and a token: the request must look like it
 * came from one of our pages (first-party Origin/Referer, not cross-site), the
 * address must be under its mint budget, and the token it gets is bound to
 * that address and User-Agent for a short while. See browserToken.ts.
 *
 * POST, not GET: a typed URL cannot hit it, a cached response cannot leak it,
 * and a link to it is not a credential.
 */
import { Hono } from 'hono';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { clientIp } from '../services/auth/clientIp.js';
import { requestIsFirstParty } from '../services/auth/firstParty.js';
import { mintBrowserToken } from '../services/auth/browserToken.js';
import { makeLimiter } from '../services/auth/ipRateLimit.js';

export const sessionRouter = new Hono();

// A page load mints once and a token lasts 15 minutes, so a real browser
// needs a handful of mints an hour. The budget is per address per ten
// minutes; a script re-minting on every request runs into it at once.
const mintLimiter = makeLimiter(config.SESSION_MINT_PER_10MIN, 10 * 60 * 1000);

/** Test seam. */
export function _resetSessionMintLimit(): void {
  mintLimiter.reset();
}

sessionRouter.post('/api/session/token', (c) => {
  c.header('Cache-Control', 'no-store');
  if (!requestIsFirstParty(c)) {
    return c.json(
      { error: 'first_party_only', message: 'Session tokens are issued to this site’s own pages only' },
      403,
    );
  }
  const ip = clientIp(c);
  if (!mintLimiter.ok(ip || 'unknown')) {
    log.warn({ ip }, 'session token: mint rate limited');
    return c.json({ error: 'rate_limited', message: 'Too many session requests' }, 429);
  }
  const { token, expiresIn } = mintBrowserToken({ ip, ua: c.req.header('user-agent') ?? '' });
  return c.json({ token, expiresIn });
});
