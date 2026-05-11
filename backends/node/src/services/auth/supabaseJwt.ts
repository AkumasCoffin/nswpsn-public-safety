/**
 * Optional Supabase JWT verifier middleware.
 *
 * Reads `Authorization: Bearer <jwt>` and, if a valid Supabase-issued
 * HS256 JWT is present, sets `c.set('userId', sub)` so downstream
 * handlers can do ownership checks. If no Authorization header is
 * present at all the middleware does nothing — it's purely additive,
 * which preserves byte-for-byte compatibility with the python backend
 * that authenticates these endpoints with NSWPSN_API_KEY only.
 *
 * The python backend doesn't currently verify the JWT itself; user_id
 * is taken from the URL path on /api/check-admin/<user_id> etc. This
 * middleware exists so future routes (or a tightening pass post-cutover)
 * have a ready-made hook without re-plumbing JWT decoding everywhere.
 *
 * Why "optional": the Authorization header is overloaded between
 * "Bearer <NSWPSN_API_KEY>" (legacy) and "Bearer <supabase_jwt>" (new).
 * Distinguishing requires trying to verify as a JWT and falling back
 * cleanly. We only call jose's verifier when the token has the JWT
 * three-segment shape (a.b.c) AND SUPABASE_JWT_SECRET is configured;
 * anything else is left untouched for the API-key middleware.
 */
import type { MiddlewareHandler } from 'hono';
import { jwtVerify } from 'jose';
import { config } from '../../config.js';
import { log } from '../../lib/log.js';

declare module 'hono' {
  interface ContextVariableMap {
    /** Supabase user id (sub claim) when a valid JWT was presented. */
    userId?: string;
  }
}

const JWT_SHAPE = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/;

export const optionalSupabaseJwt: MiddlewareHandler = async (c, next) => {
  const auth = c.req.header('Authorization');
  if (!auth || !auth.startsWith('Bearer ')) {
    await next();
    return;
  }

  const token = auth.slice(7);
  if (!JWT_SHAPE.test(token)) {
    // Almost certainly an opaque NSWPSN_API_KEY — leave it for the
    // requireApiKey middleware to handle.
    await next();
    return;
  }

  const secret = config.SUPABASE_JWT_SECRET;
  if (!secret) {
    // No secret configured server-side; we can't verify so we don't
    // pretend to. Continue without setting userId.
    await next();
    return;
  }

  try {
    const { payload } = await jwtVerify(token, new TextEncoder().encode(secret));
    if (typeof payload.sub === 'string' && payload.sub.length > 0) {
      c.set('userId', payload.sub);
    }
  } catch (err) {
    // Verification failed — log at debug because clients legitimately
    // send expired/garbage tokens and we don't want to spam warnings.
    log.debug({ err }, 'supabase jwt verify failed');
  }

  await next();
};

/**
 * Strict variant — rejects the request with 401 if no valid JWT is
 * present. Use on routes that absolutely require a real Supabase user
 * (none of W5's python-compat routes do today, but it's the obvious
 * tightening once cutover happens).
 */
export const requireSupabaseJwt: MiddlewareHandler = async (c, next) => {
  const secret = config.SUPABASE_JWT_SECRET;
  if (!secret) {
    return c.json({ error: 'jwt verification unavailable' }, 503);
  }
  const auth = c.req.header('Authorization');
  if (!auth || !auth.startsWith('Bearer ')) {
    return c.json({ error: 'authorization required' }, 401);
  }
  const token = auth.slice(7);
  if (!JWT_SHAPE.test(token)) {
    return c.json({ error: 'invalid token' }, 401);
  }
  try {
    const { payload } = await jwtVerify(token, new TextEncoder().encode(secret));
    if (typeof payload.sub !== 'string' || payload.sub.length === 0) {
      return c.json({ error: 'invalid token' }, 401);
    }
    c.set('userId', payload.sub);
  } catch {
    return c.json({ error: 'invalid token' }, 401);
  }
  await next();
};
