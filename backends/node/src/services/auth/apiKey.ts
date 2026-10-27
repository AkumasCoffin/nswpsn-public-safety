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
 *                     are not for web pages, ours or anyone else's. Reads
 *                     only what its scopes cover (apiKeyScopes.ts).
 *   Static site key   NSWPSN_API_KEY, for the server-side callers that
 *                     predate the above (Discord bot, rdio's transcripts
 *                     plugin). Accepted via Bearer, X-API-Key or ?api_key= as
 *                     it always was. No longer handed to browsers.
 *
 * The error bodies for "nothing presented" / "wrong key" are unchanged from
 * the original Python backend — clients key off them. On /api/v1/ paths every
 * refusal is the versioned API's one error shape instead (api/v1/respond.ts),
 * with the same status.
 */
import { timingSafeEqual } from 'node:crypto';
import type { Context, MiddlewareHandler } from 'hono';
import type { ContentfulStatusCode } from 'hono/utils/http-status';
import { config } from '../../config.js';
import { isV1Path, unversionedPath } from '../../lib/apiVersion.js';
import { v1ErrorBody, type ErrorCode } from '../../api/v1/respond.js';
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
import { keyMayRequest, routeNeed } from './apiKeyScopes.js';

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
    /** Scopes on that key (apiKeyScopes.ts) — what the gate let it read. */
    apiKeyScopes?: string[];
  }
}

// What a named key may reach is decided by its scopes, in apiKeyScopes.ts:
// a key in the new form reads the routes whose declared source, type and
// window it holds; a key holding `read` (every key made before scopes) reads
// the fixed list of upstream feeds it always could. Either way, GET or HEAD
// only. Public endpoints (PUBLIC_ENDPOINTS below) never reach that check —
// they take no credential from anyone — so nothing there is about hiding them.

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
  // The API documentation page and the viewer it loads (api/v1/docs.ts). The
  // page asks for a key itself and sends it to /api/v1/openapi.json, which is
  // NOT public — what it shows depends on the key.
  '/api/docs',
];

/**
 * Does this path take no credential? A /api/v1/ path is judged as its
 * unversioned twin: versioning a route does not change who may call it.
 */
export function isPublicPath(path: string): boolean {
  const p = unversionedPath(path);
  if (PUBLIC_ENDPOINTS.has(p)) return true;
  for (const prefix of PUBLIC_ENDPOINT_PREFIXES) {
    if (p.startsWith(prefix)) return true;
  }
  // Non-/api paths skip auth entirely, so the middleware is safe to mount
  // globally.
  if (!p.startsWith('/api/')) return true;
  return false;
}

/** A refusal: the versioned error shape on /api/v1/, the original body elsewhere. */
function refuse(
  c: Context,
  status: ContentfulStatusCode,
  code: ErrorCode,
  body: { error: string; message: string },
): Response {
  if (isV1Path(c.req.path)) return c.json(v1ErrorBody(code, body.message), status);
  return c.json(body, status);
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
  if (isPublicPath(url.pathname)) {
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
      return refuse(c, 401, 'token_origin', { error: 'token_origin', message: 'Session tokens are valid from this site’s pages only' });
    }
    const v = verifyBrowserToken(bearer, { ip: clientIp(c), ua: c.req.header('user-agent') ?? '' });
    if (v === 'expired') {
      // The page re-mints on this — the one failure it is meant to recover from.
      return refuse(c, 401, 'token_expired', { error: 'token_expired', message: 'Session token expired; request a new one' });
    }
    if (v !== 'ok') {
      return refuse(c, 401, 'token_invalid', { error: 'token_invalid', message: 'Session token is not valid for this client' });
    }
    c.set('authClass', 'browser');
    await next();
    return;
  }

  // --- Named API key ------------------------------------------------------------
  const presentedKey = looksLikeApiKey(bearer) ? bearer : looksLikeApiKey(xApiKey) ? xApiKey : '';
  if (presentedKey) {
    // Resolved before any refusal so that every refusal below can be counted
    // against the key it was for (apiKeyUsage.ts) — a key used from a web
    // page, after revocation, past its limit or on the wrong path is what
    // staff most need to see. Only a matching secret is attributed: a wrong
    // one against a real prefix is not the key's holder.
    const r = await resolveApiKey(presentedKey);
    if ('row' in r) c.set('apiKeyUsageId', r.row.id);
    // Keys are for scripts and servers. A browser always sends Origin on a
    // cross-origin call, so its presence means a web page — ours or another
    // site's — is trying to use a key, and that is exactly what keys are not
    // for. Answered before the key's own state, as it always was.
    //
    // The one page that may: the API's own documentation (/api/docs), served
    // from this origin, where the holder pastes their key to read the
    // reference and try it. Chrome and Firefox send no Origin on a same-origin
    // GET; Safari does. Sec-Fetch-Site is set by the browser and a page cannot
    // forge it, so `same-origin` there is a page on this API's own origin —
    // never nswpsn.forcequit.xyz (same-site) and never anyone else's.
    if (c.req.header('Origin') && c.req.header('Sec-Fetch-Site') !== 'same-origin') {
      return refuse(c, 403, 'api_key_browser_use', {
        error: 'api_key_browser_use',
        message: 'API keys are for scripts and the command line, not web pages',
      });
    }
    if (!r.ok) {
      if (r.reason === 'unavailable') {
        return refuse(c, 503, 'auth_unavailable', {
          error: 'auth_unavailable',
          message: 'Could not verify the API key; try again shortly',
        });
      }
      return refuse(c, 403, 'invalid_api_key', BAD_KEY);
    }
    if (!apiKeyRequestOk(r.row)) {
      return refuse(c, 429, 'rate_limited', {
        error: 'rate_limited',
        message: `This key is limited to ${r.row.rate_limit_per_min} requests per minute`,
      });
    }
    // Checked after the rate limit on purpose: hammering a refused path still
    // spends the key's budget, so a scan cannot be free.
    const may = keyMayRequest(r.row.scopes, c.req.method, url.pathname, routeNeed(c));
    if (!may.ok) return refuse(c, 403, may.error, { error: may.error, message: may.message });
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
    return refuse(c, 401, 'unauthorized', NEED_KEY);
  }
  if (!safeEqual(provided, config.NSWPSN_API_KEY)) {
    return refuse(c, 403, 'invalid_api_key', BAD_KEY);
  }
  c.set('authClass', 'static');
  await next();
};
