/**
 * The credential gate for /api/* routes that aren't public.
 *
 * Four kinds of caller get through, each with its own rule:
 *
 *   Supabase JWT      a logged-in person. Verified upstream by
 *                     optionalSupabaseJwt; passes here, role checks on top.
 *   Browser token     `bt1.…` — what a page holds (browserToken.ts). Must
 *                     verify against THIS request's address + User-Agent, and
 *                     the request must carry a first-party Origin/Referer.
 *                     A copied token fails; a curl with no Origin fails.
 *   Named API key     `ak_…` — a person's key from the api_keys table
 *                     (apiKeys.ts). Works from scripts and the command line.
 *                     REFUSED when a browser `Origin` header is present: keys
 *                     are not for web pages, ours or anyone else's.
 *   Static site key   NSWPSN_API_KEY, for the server-side callers that
 *                     predate the above (Discord bot, rdio's transcripts
 *                     plugin). Accepted via Bearer, X-API-Key or ?api_key= as
 *                     it always was. No longer handed to browsers.
 *
 * The error bodies for "nothing presented" / "wrong key" are unchanged from
 * the original Python backend — clients key off them.
 */
import { timingSafeEqual } from 'node:crypto';
import type { MiddlewareHandler } from 'hono';
import { config } from '../../config.js';
import { clientIp } from './clientIp.js';
import { requestIsFirstParty } from './firstParty.js';
import { BROWSER_TOKEN_PREFIX, verifyBrowserToken } from './browserToken.js';
import {
  apiKeyRequestOk,
  looksLikeApiKey,
  lookupPrefix,
  resolveApiKey,
  touchLastUsed,
} from './apiKeys.js';

/** Which credential class satisfied the gate. Read by the request log. */
export type AuthClass = 'jwt' | 'browser' | 'apikey' | 'static';

declare module 'hono' {
  interface ContextVariableMap {
    /** Set by requireApiKey on every request that passed a credential. */
    authClass?: AuthClass;
    /** Row id of the named key that authenticated this request. */
    apiKeyId?: string;
    /** Lookup prefix of that key — safe to log, identifies the key. */
    apiKeyPrefix?: string;
    /** Owner of that key. Not `userId`: a key is not a login. */
    apiKeyUserId?: string;
    /** Scopes on that key. Only 'read' exists today; nothing enforces it yet. */
    apiKeyScopes?: string[];
  }
}

/**
 * Constant-time string comparison. Guards against length mismatch first
 * (timingSafeEqual throws on unequal-length buffers); leaking the length
 * of the secret is not a meaningful side-channel here.
 */
function safeEqual(a: string, b: string): boolean {
  const aBuf = Buffer.from(a, 'utf8');
  const bBuf = Buffer.from(b, 'utf8');
  if (aBuf.length !== bBuf.length) return false;
  return timingSafeEqual(aBuf, bBuf);
}

// Exact-match public endpoints (no credential required).
export const PUBLIC_ENDPOINTS = new Set<string>([
  '/api/health',
  '/',
  '/api/config',
  // Where a page gets its browser token. POST-only; the route itself checks
  // the caller is first-party and rate-limits the address.
  '/api/session/token',
  '/api/heartbeat',
  '/api/editor-requests',
  '/api/data/history/filters',
  '/api/status',
  // The cameras collection endpoint itself is public; sub-paths are
  // covered by the '/api/centralwatch/cameras/' prefix. Listed here as
  // an exact match so the prefix doesn't over-match sibling routes like
  // '/api/centralwatch/cameras-admin'.
  '/api/centralwatch/cameras',
  // Agency reference tables — replaces the public static agency-extended.json,
  // so the read stays public (the agency page needs no login). Write/edit
  // endpoints under /api/agency/ are gated per-handler, NOT listed here.
  '/api/agency/extended',
]);

// Prefix-match public endpoints.
export const PUBLIC_ENDPOINT_PREFIXES: readonly string[] = [
  '/api/check-editor/',
  '/api/centralwatch/image/',
  '/api/centralwatch/cameras/',
  '/api/dashboard/',
  // Vessel image proxy is loaded via <img src> which can't add a header, so
  // it has to be public (the upstream MT photo is itself public anyway).
  '/api/marinetraffic/vessel-image/',
  // Feeder-node call relay. The node agent authenticates with its own
  // feeder token (X-Node-Token/X-Node-Install), verified inside the
  // handler — NOT the site key — so it must skip this gate.
  '/api/node-ingest/',
  // A freshly installed agent has no keys of any kind — trading its enrolment
  // code for a token is how it gets one. The code is the credential here.
  '/api/node-enrol',
  // Scanner feed: a third-party rdio DOWNSTREAM cannot send custom headers, so
  // it authenticates with its key as a form field, verified in the handler.
  '/api/scanner-ingest/',
  // Node self-update manifest. Same story: the node authenticates with its
  // feeder token (X-Node-Token), verified inside the handler.
  '/api/node-updates/',
  // Per-agency reference tables (/api/agency/extended/:slug) — public read,
  // matching the exact /api/agency/extended above.
  '/api/agency/extended/',
  // Wire link-unfurl metadata. Consumed by the Cloudflare Worker on the /wire
  // route to inject per-post Open Graph tags for social crawlers, which can't
  // send a credential. Only exposes OG fields of already-published posts.
  '/api/wire/og/',
  // Whisper status + drain. The PC's idle watcher is a headless script with
  // no user session and no reason to hold the site key; both routes verify
  // WHISPER_ADMIN_TOKEN (or, for status, a staff role) inside the handler.
  //
  // NOT /api/whisper/v1/ — the transcription path deliberately KEEPS this
  // gate. rdio has an API key field of its own to put NSWPSN_API_KEY in, and
  // an endpoint that spends GPU time should not be the one open route.
  '/api/whisper/status',
  '/api/whisper/history',
  '/api/whisper/drain',
  // LGA names for the signup form's State -> LGA flow. Signup runs before any
  // auth exists, and the list is ABS public data.
  '/api/boundaries/lga-names',
];

function isPublic(path: string): boolean {
  if (PUBLIC_ENDPOINTS.has(path)) return true;
  for (const prefix of PUBLIC_ENDPOINT_PREFIXES) {
    if (path.startsWith(prefix)) return true;
  }
  // Non-/api paths skip auth entirely, so the middleware is safe to mount
  // globally.
  if (!path.startsWith('/api/')) return true;
  return false;
}

function bearerOf(authHeader: string | undefined): string {
  return authHeader && authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
}

const NEED_KEY = {
  error: 'API key required',
  message: 'Provide API key via Authorization: Bearer <key> header or X-API-Key header',
};
const BAD_KEY = { error: 'Invalid API key', message: 'The provided API key is not valid' };

/**
 * Hono middleware: the gate described at the top of this file. OPTIONS
 * preflights, public endpoints, and non-/api paths pass through untouched.
 */
export const requireApiKey: MiddlewareHandler = async (c, next) => {
  if (c.req.method === 'OPTIONS') {
    await next();
    return;
  }

  const url = new URL(c.req.url);
  if (isPublic(url.pathname)) {
    await next();
    return;
  }

  // A request already authenticated as a Supabase user (optionalSupabaseJwt
  // verified the JWT and set userId) passes the gate — a logged-in person is
  // at least as trusted as a page. Privileged routes still enforce their own
  // role check (requireRole) on top of this.
  if (c.get('userId')) {
    c.set('authClass', 'jwt');
    await next();
    return;
  }

  const bearer = bearerOf(c.req.header('Authorization'));
  const xApiKey = c.req.header('X-API-Key') ?? '';

  // --- Browser session token --------------------------------------------------
  if (bearer.startsWith(BROWSER_TOKEN_PREFIX)) {
    // A token is only ever presented by one of our pages; without a
    // first-party Origin the holder is a script, whatever it copied.
    if (!requestIsFirstParty(c)) {
      return c.json({ error: 'token_origin', message: 'Session tokens are valid from this site’s pages only' }, 401);
    }
    const v = verifyBrowserToken(bearer, { ip: clientIp(c), ua: c.req.header('user-agent') ?? '' });
    if (v === 'expired') {
      // The page re-mints on this — the one failure it is meant to recover from.
      return c.json({ error: 'token_expired', message: 'Session token expired; request a new one' }, 401);
    }
    if (v !== 'ok') {
      return c.json({ error: 'token_invalid', message: 'Session token is not valid for this client' }, 401);
    }
    c.set('authClass', 'browser');
    await next();
    return;
  }

  // --- Named API key ------------------------------------------------------------
  const presentedKey = looksLikeApiKey(bearer) ? bearer : looksLikeApiKey(xApiKey) ? xApiKey : '';
  if (presentedKey) {
    // Keys are for scripts and servers. A browser always sends Origin on a
    // cross-origin call, so its presence means a web page — ours or another
    // site's — is trying to use a key, and that is exactly what keys are not for.
    if (c.req.header('Origin')) {
      return c.json(
        { error: 'api_key_browser_use', message: 'API keys are for scripts and the command line, not web pages' },
        403,
      );
    }
    const r = await resolveApiKey(presentedKey);
    if (!r.ok) {
      if (r.reason === 'unavailable') {
        return c.json({ error: 'auth_unavailable', message: 'Could not verify the API key; try again shortly' }, 503);
      }
      return c.json(BAD_KEY, 403);
    }
    if (!apiKeyRequestOk(r.row)) {
      return c.json(
        { error: 'rate_limited', message: `This key is limited to ${r.row.rate_limit_per_min} requests per minute` },
        429,
      );
    }
    touchLastUsed(r.row);
    c.set('authClass', 'apikey');
    c.set('apiKeyId', r.row.id);
    c.set('apiKeyPrefix', lookupPrefix(presentedKey) ?? '');
    c.set('apiKeyUserId', r.row.user_id);
    c.set('apiKeyScopes', r.row.scopes);
    await next();
    return;
  }

  // --- Static site key (server-side callers) ------------------------------------
  const provided = bearer || xApiKey || url.searchParams.get('api_key') || '';
  if (!provided) {
    return c.json(NEED_KEY, 401);
  }
  if (!safeEqual(provided, config.NSWPSN_API_KEY)) {
    return c.json(BAD_KEY, 403);
  }
  c.set('authClass', 'static');
  await next();
};
