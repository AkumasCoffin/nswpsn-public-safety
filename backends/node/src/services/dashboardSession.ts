/**
 * Dashboard cookie crypto + server-side session map + DB persistence.
 *
 * Mirrors python external_api_proxy.py:16188-16548 (the _dash_* family
 * around cookie signing, session storage, hydration on startup).
 *
 * Cookie shape (both flavours):
 *     `<base64url(payload_json)>.<base64url(hmac_sha256(payload_json))>`
 *
 * - DASH_OAUTH_COOKIE: short-lived (10 min). Carries `{ nonce, next, exp }`.
 * - DASH_SESSION_COOKIE: 24 h. Carries only `{ sid, exp }` — the heavy
 *   data (guild list, access_token) lives in `_DASH_SESSIONS` server-side.
 *
 * The HMAC is a vanilla Node crypto.createHmac('sha256', secret) — no
 * JWT library needed. timingSafeEqual prevents leaky comparisons.
 *
 * Server-side session storage is two-tier:
 *  - in-memory Map<sid, sessionData> for fast lookup.
 *  - write-through to dash_sessions table so a restart doesn't force
 *    every user to re-login.
 *
 * On startup, hydrateSessions() restores any non-expired rows from the
 * table back into the map (python does this at line 16363-16375).
 */
import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
import type { Pool } from 'pg';
import { log } from '../lib/log.js';
import { getBotDbPool } from './botDb.js';

// --- Constants (mirror python lines 16131-16138) -----------------------------
export const DASH_SESSION_COOKIE = 'nswpsn_dash_sess';
export const DASH_OAUTH_COOKIE = 'nswpsn_dash_oauth';
export const DASH_SESSION_TTL_SECS = 24 * 60 * 60; // 24h
export const DASH_OAUTH_STATE_TTL_SECS = 10 * 60; // 10m
export const DASH_GUILD_REFRESH_INTERVAL_SECS = 10 * 60;
// Cookie domain — the dashboard UI is at nswpsn.forcequit.xyz but the API is
// at api.forcequit.xyz; share the cookie via the parent domain.
export const DASH_COOKIE_DOMAIN_DEFAULT = '.forcequit.xyz';

// --- Types -------------------------------------------------------------------
export interface SessionGuild {
  id: string;
  name: string;
  icon: string | null;
  permissions: string;
  owner?: boolean;
}

export interface DashSession {
  uid: string;
  username: string;
  avatar: string | null;
  access_token: string;
  token_type: string;
  refresh_token: string | null;
  guilds: SessionGuild[];
  gfresh: number;
  iat: number;
  exp: number;
  last_seen?: number;
  // Runtime-only; never persisted. Set by loadSession so downstream code
  // can refer back to the sid.
  _sid?: string;
}

export interface OAuthStatePayload {
  nonce: string;
  next: string;
  exp: number;
}

export interface SessionCookiePayload {
  sid: string;
  exp: number;
}

// ---------------------------------------------------------------------------
// Base64url + HMAC helpers (python lines 16179-16228).
// ---------------------------------------------------------------------------
function b64url(data: Buffer): string {
  return data
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

function b64urlDecode(s: string): Buffer {
  const pad = (4 - (s.length % 4)) % 4;
  return Buffer.from(s.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat(pad), 'base64');
}

function sign(payload: Buffer, secret: Buffer): Buffer {
  return createHmac('sha256', secret).update(payload).digest();
}

/** Get DASHBOARD_SESSION_SECRET as a Buffer or null if unset. */
export function getSessionSecret(): Buffer | null {
  const raw = process.env['DASHBOARD_SESSION_SECRET'];
  if (!raw) return null;
  return Buffer.from(raw, 'utf8');
}

/** `<b64payload>.<b64sig>` for the JSON-encoded payload. */
export function makeCookie(payload: unknown, secret: Buffer): string {
  // sort_keys=True equivalent: stringify with keys ordered. JSON.stringify
  // doesn't sort by default; we replicate python's json.dumps(sort_keys=True)
  // by pre-sorting object keys.
  const raw = Buffer.from(stableStringify(payload), 'utf8');
  const sig = sign(raw, secret);
  return `${b64url(raw)}.${b64url(sig)}`;
}

function stableStringify(value: unknown): string {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(value).sort()) {
      out[k] = sortKeys((value as Record<string, unknown>)[k]);
    }
    return out;
  }
  return value;
}

/**
 * Verify and parse a signed cookie. Returns the payload or null on
 * tampering / malformed value. Uses timingSafeEqual.
 */
export function parseCookie<T = unknown>(
  cookieValue: string | undefined | null,
  secret: Buffer,
): T | null {
  if (!cookieValue || !cookieValue.includes('.')) return null;
  const parts = cookieValue.split('.', 2);
  const pB64 = parts[0];
  const sB64 = parts[1];
  if (!pB64 || !sB64) return null;
  let raw: Buffer;
  let sig: Buffer;
  try {
    raw = b64urlDecode(pB64);
    sig = b64urlDecode(sB64);
  } catch {
    return null;
  }
  const expected = sign(raw, secret);
  if (expected.length !== sig.length) return null;
  if (!timingSafeEqual(expected, sig)) return null;
  try {
    return JSON.parse(raw.toString('utf8')) as T;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Cookie header helpers — Hono uses the standard Set-Cookie/cookie strings.
// ---------------------------------------------------------------------------
interface CookieOpts {
  maxAge: number;
  secure: boolean;
  domain?: string | null;
}

export function buildSetCookie(name: string, value: string, opts: CookieOpts): string {
  const sameSite = opts.secure ? 'None' : 'Lax';
  const parts = [
    `${name}=${value}`,
    'Path=/',
    `Max-Age=${opts.maxAge}`,
    'HttpOnly',
    `SameSite=${sameSite}`,
  ];
  if (opts.secure) parts.push('Secure');
  if (opts.domain) parts.push(`Domain=${opts.domain}`);
  return parts.join('; ');
}

export function buildClearCookie(name: string, secure: boolean, domain?: string | null): string {
  return buildSetCookie(name, '', { maxAge: 0, secure, ...(domain ? { domain } : {}) });
}

/** Parse the Cookie request header into a name->value map. */
export function parseCookieHeader(header: string | undefined | null): Record<string, string> {
  const out: Record<string, string> = {};
  if (!header) return out;
  for (const part of header.split(';')) {
    const idx = part.indexOf('=');
    if (idx < 0) continue;
    const k = part.slice(0, idx).trim();
    const v = part.slice(idx + 1).trim();
    if (k) out[k] = decodeURIComponent(v);
  }
  return out;
}

/** Whether to send Secure on cookies. Honours X-Forwarded-Proto. */
export function isSecureRequest(
  forwardedProto: string | undefined | null,
  url: string,
): boolean {
  const xf = (forwardedProto ?? '').toLowerCase();
  if (xf) return xf === 'https';
  return url.startsWith('https://');
}

export function getCookieDomain(): string | null {
  const v = process.env['DASHBOARD_COOKIE_DOMAIN'];
  if (v === undefined) return DASH_COOKIE_DOMAIN_DEFAULT;
  return v || null;
}

// ---------------------------------------------------------------------------
// Server-side session storage (python lines 16282-16511).
// ---------------------------------------------------------------------------

const SESSIONS = new Map<string, DashSession>();
let dbReady = false;

/** TEST-ONLY: clear in-memory state. */
export function _resetSessionsForTests(): void {
  SESSIONS.clear();
  dbReady = false;
}

/** TEST-ONLY: peek at the map. */
export function _getSessionsForTests(): Map<string, DashSession> {
  return SESSIONS;
}

/** TEST-ONLY: mark the DB-init flag without running CREATE TABLE etc. */
export function _markDbReadyForTests(): void {
  dbReady = true;
}

/**
 * One-time CREATE TABLE + hydrate. Mirrors python's
 * _dash_sessions_db_ensure() at line 16329 — runs at most once per process.
 * Subsequent put/drop just write through.
 */
export async function ensureSessionTables(): Promise<void> {
  if (dbReady) return;
  const pool = await getBotDbPool();
  if (!pool) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS dash_sessions (
        sid        TEXT PRIMARY KEY,
        data       JSONB NOT NULL,
        exp        INTEGER NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);
    await pool.query(
      'CREATE INDEX IF NOT EXISTS idx_dash_sessions_exp ON dash_sessions(exp)',
    );
    await pool.query(`
      CREATE TABLE IF NOT EXISTS dashboard_users (
        uid          TEXT PRIMARY KEY,
        username     TEXT,
        avatar       TEXT,
        first_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
        login_count  INTEGER NOT NULL DEFAULT 1
      )
    `);
    const now = Math.floor(Date.now() / 1000);
    await pool.query('DELETE FROM dash_sessions WHERE exp < $1', [now]);
    const res = await pool.query<{ sid: string; data: DashSession }>(
      'SELECT sid, data FROM dash_sessions',
    );
    let loaded = 0;
    for (const row of res.rows) {
      if (!SESSIONS.has(row.sid)) {
        SESSIONS.set(row.sid, row.data);
        loaded += 1;
      }
    }
    dbReady = true;
    if (loaded > 0) {
      log.info({ loaded }, 'dashboard: restored sessions from DB');
    }
  } catch (err) {
    log.warn({ err }, 'dashboard session DB init failed');
  }
}

async function persistSession(sid: string, data: DashSession): Promise<void> {
  const pool = await getBotDbPool();
  if (!pool) return;
  // Drop runtime-only keys (those prefixed with _) per python line 16395.
  const persisted: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(data)) {
    if (!k.startsWith('_')) persisted[k] = v;
  }
  try {
    await pool.query(
      `INSERT INTO dash_sessions (sid, data, exp) VALUES ($1, $2::jsonb, $3)
       ON CONFLICT (sid) DO UPDATE SET
         data = EXCLUDED.data, exp = EXCLUDED.exp, updated_at = now()`,
      [sid, JSON.stringify(persisted), Number(persisted['exp'] ?? 0)],
    );
  } catch (err) {
    log.warn({ err }, 'dashboard session persist failed');
  }
}

async function deleteSessionDb(sid: string): Promise<void> {
  const pool = await getBotDbPool();
  if (!pool) return;
  try {
    await pool.query('DELETE FROM dash_sessions WHERE sid = $1', [sid]);
  } catch (err) {
    log.warn({ err }, 'dashboard session delete failed');
  }
}

/** Record / refresh the persistent dashboard_users row. */
export async function upsertDashboardUser(
  uid: string,
  username: string | null,
  avatar: string | null,
  incrementLogin: boolean,
): Promise<void> {
  if (!uid) return;
  const pool = await getBotDbPool();
  if (!pool) return;
  try {
    if (incrementLogin) {
      await pool.query(
        `INSERT INTO dashboard_users (uid, username, avatar)
         VALUES ($1, $2, $3)
         ON CONFLICT (uid) DO UPDATE SET
           username = COALESCE(EXCLUDED.username, dashboard_users.username),
           avatar   = COALESCE(EXCLUDED.avatar,   dashboard_users.avatar),
           last_seen = now(),
           login_count = dashboard_users.login_count + 1`,
        [uid, username ?? '', avatar ?? ''],
      );
    } else {
      await pool.query(
        'UPDATE dashboard_users SET last_seen = now() WHERE uid = $1',
        [uid],
      );
    }
  } catch (err) {
    log.warn({ err }, 'dashboard_users upsert failed');
  }
}

/** Mint a new session id (192 bits of entropy, b64url). */
export function newSid(): string {
  return b64url(randomBytes(24));
}

export async function putSession(sid: string, data: DashSession): Promise<void> {
  await ensureSessionTables();
  SESSIONS.set(sid, data);
  // Persist write-through. Don't await dashboard_users so the OAuth
  // callback returns quickly — match python's threaded behaviour.
  await persistSession(sid, data);
  if (data.uid) {
    void upsertDashboardUser(data.uid, data.username, data.avatar, true);
  }
  // Opportunistic GC at 512 entries.
  if (SESSIONS.size > 512) {
    const now = Math.floor(Date.now() / 1000);
    for (const [k, v] of SESSIONS) {
      if ((v.exp ?? 0) < now) {
        SESSIONS.delete(k);
        void deleteSessionDb(k);
      }
    }
  }
}

export async function getSession(sid: string): Promise<DashSession | null> {
  await ensureSessionTables();
  const sess = SESSIONS.get(sid);
  if (!sess) return null;
  if ((sess.exp ?? 0) < Math.floor(Date.now() / 1000)) {
    SESSIONS.delete(sid);
    void deleteSessionDb(sid);
    return null;
  }
  return sess;
}

export async function dropSession(sid: string): Promise<void> {
  SESSIONS.delete(sid);
  await deleteSessionDb(sid);
}

/** Update session in place + write-through. Used by /me guild refresh. */
export async function persistSessionPublic(sid: string, data: DashSession): Promise<void> {
  await persistSession(sid, data);
}

/**
 * Resolve the session for the current request from the cookie header.
 * Returns null on missing/invalid/expired cookie or absent server-side
 * session. Mirrors python's _dash_load_session() at line 16514.
 */
export async function loadSession(
  cookieHeader: string | undefined,
): Promise<DashSession | null> {
  const secret = getSessionSecret();
  if (!secret) return null;
  const cookies = parseCookieHeader(cookieHeader);
  const cookieVal = cookies[DASH_SESSION_COOKIE];
  if (!cookieVal) return null;
  const payload = parseCookie<SessionCookiePayload>(cookieVal, secret);
  if (!payload) return null;
  const exp = payload.exp;
  if (typeof exp !== 'number' || exp < Date.now() / 1000) return null;
  const sid = payload.sid;
  if (!sid) return null;
  const session = await getSession(sid);
  if (!session) return null;
  // Surface sid + bump last_seen (in-memory only, like python).
  session._sid = sid;
  session.last_seen = Math.floor(Date.now() / 1000);
  return session;
}

// ---------------------------------------------------------------------------
// Admin helpers (python lines 16143-16152).
// ---------------------------------------------------------------------------

export function getAdminIds(): Set<string> {
  const raw = process.env['DASHBOARD_ADMIN_IDS'] ?? '';
  return new Set(
    raw
      .split(',')
      .map((p) => p.trim())
      .filter(Boolean),
  );
}

export function isAdmin(session: DashSession | null | undefined): boolean {
  const uid = String(session?.uid ?? '');
  return Boolean(uid) && getAdminIds().has(uid);
}

// ---------------------------------------------------------------------------
// Bot guild ids (python lines 16633-16663) — 30 s cache around
// `SELECT DISTINCT guild_id FROM alert_presets`.
// ---------------------------------------------------------------------------

const BOT_GUILD_IDS_TTL_MS = 30_000;
let botGuildIdsCache: { ts: number; data: Set<string> } = { ts: 0, data: new Set() };

export async function getBotGuildIds(): Promise<Set<string>> {
  const now = Date.now();
  if (now - botGuildIdsCache.ts < BOT_GUILD_IDS_TTL_MS) {
    return new Set(botGuildIdsCache.data);
  }
  const pool: Pool | null = await getBotDbPool();
  if (!pool) {
    log.warn('dashboard getBotGuildIds: BOT_DATA_DATABASE_URL not set');
    return new Set();
  }
  try {
    const res = await pool.query<{ guild_id: string | number }>(
      'SELECT DISTINCT guild_id FROM alert_presets',
    );
    const ids = new Set(res.rows.map((r) => String(r.guild_id)));
    botGuildIdsCache = { ts: Date.now(), data: ids };
    return ids;
  } catch (err) {
    log.error({ err }, 'dashboard getBotGuildIds error');
    return new Set();
  }
}

export function _resetBotGuildIdsCacheForTests(): void {
  botGuildIdsCache = { ts: 0, data: new Set() };
}
