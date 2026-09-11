/**
 * Staff notifications → Discord.
 *
 * Signup requests, Wire approvals and Wire takedowns are pushed to a staff
 * Discord channel the moment they happen.
 *
 * WHY PUSH AND NOT POLL. The bot already polls this backend for public
 * feeds, but these three are role-gated moderation queues, so a pollable
 * endpoint would mean exposing them to anything holding NSWPSN_API_KEY.
 * Instead the backend enqueues a signed row into `pending_bot_actions`
 * (the existing outbox the dashboard uses for broadcast/sync/test) and the
 * bot drains it within ~10s. That also sidesteps the poller's bootstrap,
 * which marks everything present at bot start as "seen" — a pending signup
 * request would otherwise be silently swallowed by a restart.
 *
 * WHY THESE MESSAGES CARRY NO PERSONAL DATA. Discord history is
 * searchable, screenshottable and outside our control, and the channel id
 * is operator-configured so it can be pointed at the wrong place. The
 * payloads below are deliberately limited to non-identifying context plus
 * a link back to the staff page. In particular `editor_requests.notes`
 * holds a generated temp password and must never be sent anywhere.
 *
 * Every function here is best-effort: a notification must never fail, slow
 * or roll back the moderation action that triggered it.
 */
import type { Pool } from 'pg';

import { getBotDbPool } from './botDb.js';
import { getBotActionSecret, signBotAction } from './botActionSign.js';
import { log } from '../lib/log.js';

export type StaffNotifyKind = 'signup_request' | 'wire_approval' | 'wire_takedown';

/** app_settings keys. One channel per kind, plus the guild they live in. */
export const NOTIFY_GUILD_KEY = 'discord_notify_guild_id';
export const NOTIFY_CHANNEL_KEYS: Record<StaffNotifyKind, string> = {
  signup_request: 'discord_notify_channel_signup',
  wire_approval: 'discord_notify_channel_wire_approval',
  wire_takedown: 'discord_notify_channel_wire_takedown',
};

/** Which staff view each kind deep-links to. */
const STAFF_VIEW: Record<StaffNotifyKind, string> = {
  signup_request: 'signup',
  wire_approval: 'approvals',
  wire_takedown: 'takedowns',
};

function publicBase(): string {
  return (process.env['PUBLIC_BASE_URL'] || 'https://nswpsn.forcequit.xyz').replace(/\/+$/, '');
}

// ---------------------------------------------------------------------------
// Settings (app_settings — the same table the Wire toggles use).
// ---------------------------------------------------------------------------

/**
 * Short read-through cache. These are read on every moderation action, and
 * they change about once ever. On a DB error we serve the previous value
 * rather than silently dropping notifications.
 */
const CACHE_MS = 10_000;
let _cache: { at: number; values: Record<string, string | null> } | null = null;

const ALL_KEYS = [NOTIFY_GUILD_KEY, ...Object.values(NOTIFY_CHANNEL_KEYS)];

export function invalidateStaffNotifySettings(): void {
  _cache = null;
}

export async function readNotifySettings(pool: Pool): Promise<Record<string, string | null>> {
  const now = Date.now();
  if (_cache && now - _cache.at < CACHE_MS) return _cache.values;
  try {
    const r = await pool.query<{ key: string; value: string }>(
      'SELECT key, value FROM app_settings WHERE key = ANY($1::text[])',
      [ALL_KEYS],
    );
    const values: Record<string, string | null> = {};
    for (const k of ALL_KEYS) values[k] = null;
    for (const row of r.rows) values[row.key] = row.value || null;
    _cache = { at: now, values };
    return values;
  } catch (err) {
    if (_cache) return _cache.values;
    log.warn({ err: (err as Error).message }, 'staffNotify: settings read failed');
    const empty: Record<string, string | null> = {};
    for (const k of ALL_KEYS) empty[k] = null;
    return empty;
  }
}

/**
 * Upsert one setting. `value` of null/'' clears it (turning that
 * notification off) rather than storing an empty string.
 */
export async function writeNotifySetting(
  pool: Pool,
  key: string,
  value: string | null,
  updatedBy: string | null,
): Promise<void> {
  if (!ALL_KEYS.includes(key)) throw new Error(`unknown staff-notify key: ${key}`);
  if (value) {
    await pool.query(
      `INSERT INTO app_settings (key, value, updated_by, updated_at)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value,
         updated_by = EXCLUDED.updated_by, updated_at = now()`,
      [key, value, updatedBy],
    );
  } else {
    await pool.query('DELETE FROM app_settings WHERE key = $1', [key]);
  }
  invalidateStaffNotifySettings();
}

// ---------------------------------------------------------------------------
// Enqueue.
// ---------------------------------------------------------------------------

let _signWarned = false;
function sign(action: string, requestedBy: string, params: unknown): string | null {
  const secret = getBotActionSecret();
  if (!secret) {
    if (!_signWarned) {
      _signWarned = true;
      log.warn(
        'BOT_ACTION_SIGNING_SECRET is unset — staff notifications are enqueued ' +
          'UNSIGNED and the bot fails open. Set the same value in the backend and ' +
          'discord-bot .env before relying on this for moderation traffic.',
      );
    }
    return null;
  }
  return signBotAction(secret, action, requestedBy, params);
}

export interface StaffNotifyInput {
  kind: StaffNotifyKind;
  /** 'new' posts a message; 'resolved' edits the one posted for the same ref. */
  event: 'new' | 'resolved';
  /** Stable id of the underlying row — how 'resolved' finds its message. */
  ref: string;
  /** Short non-identifying headline, e.g. "Radio Feeder · NSW". */
  title: string;
  /** Optional second line. Must not contain personal data. */
  subtitle?: string | null;
  /** For 'resolved': 'approved' | 'rejected' | 'upheld' | … */
  status?: string | null;
  /** For 'resolved': display name of the staff member who actioned it. */
  actor?: string | null;
}

/**
 * Fire-and-forget. Deliberately returns void rather than a promise the
 * caller might await — a Discord hiccup must not affect the HTTP response
 * for the moderation action itself.
 */
export function notifyStaff(pool: Pool | null, input: StaffNotifyInput): void {
  void notifyStaffAsync(pool, input).catch((err) => {
    log.warn({ err: (err as Error).message, kind: input.kind }, 'staffNotify: enqueue failed');
  });
}

/** The awaitable form — exported for tests. */
export async function notifyStaffAsync(pool: Pool | null, input: StaffNotifyInput): Promise<boolean> {
  if (!pool) return false;

  const settings = await readNotifySettings(pool);
  const channelId = settings[NOTIFY_CHANNEL_KEYS[input.kind]];
  const guildId = settings[NOTIFY_GUILD_KEY];
  // Unconfigured is the normal state until an owner sets it up — not an error.
  if (!channelId || !guildId) return false;

  const botPool = await getBotDbPool();
  if (!botPool) return false;

  // All-strings: the signing canonicaliser is shared with Python, and
  // keeping every value a string removes any number-formatting drift.
  const params: Record<string, string> = {
    kind: input.kind,
    event: input.event,
    ref: String(input.ref),
    guild_id: String(guildId),
    channel_id: String(channelId),
    title: input.title,
    subtitle: input.subtitle ?? '',
    status: input.status ?? '',
    actor: input.actor ?? '',
    url: `${publicBase()}/staff?view=${STAFF_VIEW[input.kind]}`,
  };

  const requestedBy = 'backend';
  const sig = sign('staff_notify', requestedBy, params);
  await botPool.query(
    'INSERT INTO pending_bot_actions (action, params, requested_by, sig) VALUES ($1, $2::jsonb, $3, $4)',
    ['staff_notify', JSON.stringify(params), requestedBy, sig],
  );
  return true;
}
