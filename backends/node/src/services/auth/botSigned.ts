/**
 * The gate for what the Discord bot POSTS to this backend.
 *
 * Until now the bot only ever read from the backend, and the only
 * bot-backend write ran the other way (the signed outbox the bot drains,
 * services/staffNotify.ts). Filing a bug report from a Discord channel
 * needs the bot to write, and the static site key alone is not enough for
 * that: rdio's transcripts plugin holds the same key, and anything that
 * can call a write route with it could file reports in other people's names.
 *
 * So a bot write needs two things: the static key (authClass 'static' from
 * the credential gate — a logged-in person or a page token is refused here)
 * AND an HMAC over the JSON body in `X-Bot-Signature`, computed with the
 * same secret and canonical form as the outbox rows
 * (BOT_ACTION_SIGNING_SECRET; sign_bot_action in discord-bot/database.py,
 * action = the route's name, requested_by = 'bot'). Unlike the outbox,
 * which fails open while the secret is being rolled out, this fails
 * CLOSED: with no secret there is no way to tell the bot from anyone else
 * holding the key, so the route answers 503 until it is set.
 *
 * The verified body is left on the context as `botBody` so the handler
 * does not parse it twice (and cannot read a different body from the one
 * that was signed).
 */
import type { MiddlewareHandler } from 'hono';
import { getBotActionSecret, verifyBotActionSig } from '../botActionSign.js';

declare module 'hono' {
  interface ContextVariableMap {
    /** The signed JSON body of a bot write, once requireSignedBot has verified it. */
    botBody?: Record<string, unknown>;
  }
}

export const BOT_REQUESTED_BY = 'bot';

export function requireSignedBot(action: string): MiddlewareHandler {
  return async (c, next) => {
    if (c.get('authClass') !== 'static') {
      return c.json({ error: 'bot_only', message: 'This route is for the Discord bot' }, 401);
    }
    const secret = getBotActionSecret();
    if (!secret) {
      return c.json({ error: 'bot_signing_unset', message: 'BOT_ACTION_SIGNING_SECRET is not set' }, 503);
    }
    const body = (await c.req.json().catch(() => null)) as unknown;
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      return c.json({ error: 'invalid body' }, 400);
    }
    if (!verifyBotActionSig(secret, action, BOT_REQUESTED_BY, body, c.req.header('X-Bot-Signature'))) {
      return c.json({ error: 'bad_signature', message: 'Signature does not match the body' }, 403);
    }
    c.set('botBody', body as Record<string, unknown>);
    await next();
  };
}
