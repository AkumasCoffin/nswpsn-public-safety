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
 * WHAT THESE MESSAGES MAY CARRY. They go to a private staff channel, so
 * they carry the detail needed to triage a request without opening the
 * site — an applicant's email, a node's name, a new account's handle. The
 * summary-only form they started as was worse than useless: it announced
 * that something had happened without saying enough to act on, so every
 * notification became a prompt to go and look the thing up.
 *
 * WHAT THEY MUST NEVER CARRY IS SECRETS. Discord history is searchable,
 * screenshottable and outside our control, and the channel id is
 * operator-configured so it can be pointed at the wrong place. In
 * particular `editor_requests.notes` holds the generated temp password for
 * an approved signup, and sending that whole string is exactly how the
 * password reached a staff channel once. Send the fact that a password was
 * issued, never the password; the same goes for node tokens (the prefix is
 * fine, the token is not) and for anything read out of app_settings.
 *
 * Every function here is best-effort: a notification must never fail, slow
 * or roll back the moderation action that triggered it.
 */
import type { Pool } from 'pg';

import { getBotDbPool } from './botDb.js';
import { getBotActionSecret, signBotAction } from './botActionSign.js';
import { log } from '../lib/log.js';

export type StaffNotifyKind =
  | 'signup_request'
  | 'wire_approval'
  | 'wire_takedown'
  | 'new_user'
  | 'new_node'
  | 'ticket'
  | 'siteSurvey'
  | 'api_key_request'
  | 'bug_report'
  | 'suggestion';

/** app_settings keys. One channel per kind, plus the guild they live in. */
export const NOTIFY_GUILD_KEY = 'discord_notify_guild_id';
export const NOTIFY_CHANNEL_KEYS: Record<StaffNotifyKind, string> = {
  signup_request: 'discord_notify_channel_signup',
  wire_approval: 'discord_notify_channel_wire_approval',
  wire_takedown: 'discord_notify_channel_wire_takedown',
  new_user: 'discord_notify_channel_new_user',
  new_node: 'discord_notify_channel_new_node',
  // Contact tickets get their OWN channel, distinct from signups/nodes.
  ticket: 'discord_notify_channel_ticket',
  siteSurvey: 'discord_notify_channel_site_survey',
  // A user asks for a named API key; approve/reject edits the message in place.
  api_key_request: 'discord_notify_channel_api_key',
  // Bug reports and suggestions (api/feedback.ts): the private staff alert.
  // Their PUBLIC channels are FEEDBACK_CHANNEL_KEYS below.
  bug_report: 'discord_notify_channel_bug_report',
  suggestion: 'discord_notify_channel_suggestion',
};

/** Which staff view each kind deep-links to — the query after `/staff?view=`. */
const STAFF_VIEW: Record<StaffNotifyKind, string> = {
  signup_request: 'signup',
  wire_approval: 'approvals',
  wire_takedown: 'takedowns',
  new_user: 'users',
  new_node: 'nodes',
  ticket: 'tickets',
  siteSurvey: 'nodes',
  api_key_request: 'apikeys',
  bug_report: 'tickets&tab=reports&kind=bug',
  suggestion: 'tickets&tab=reports&kind=suggestion',
};

/**
 * The public side of bug reports and suggestions, per kind: the INTAKE
 * channel the bot files messages from, and the BOARD channel it posts every
 * report to with its status. Both are app_settings keys like the alert
 * channels; a change is pushed to the bot as a `feedback_channels` action
 * (enqueueFeedbackChannels), because the bot never reads app_settings.
 */
export type FeedbackKind = 'bug' | 'suggestion';
export const FEEDBACK_KINDS: readonly FeedbackKind[] = ['bug', 'suggestion'];
export const FEEDBACK_CHANNEL_KEYS: Record<FeedbackKind, { intake: string; board: string }> = {
  bug: { intake: 'discord_intake_channel_bug', board: 'discord_board_channel_bug' },
  suggestion: { intake: 'discord_intake_channel_suggestion', board: 'discord_board_channel_suggestion' },
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

const ALL_KEYS = [
  NOTIFY_GUILD_KEY,
  ...Object.values(NOTIFY_CHANNEL_KEYS),
  ...Object.values(FEEDBACK_CHANNEL_KEYS).flatMap((k) => [k.intake, k.board]),
];

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

/**
 * Sign and insert one outbox row. Every value in `params` must be a string:
 * the canonicaliser is shared with Python, and keeping to strings removes
 * any number-formatting drift between the two. Returns false, never throws,
 * when the bot database is not configured.
 */
export async function enqueueBotAction(action: string, params: Record<string, string>): Promise<boolean> {
  const botPool = await getBotDbPool();
  if (!botPool) return false;
  const requestedBy = 'backend';
  const sig = sign(action, requestedBy, params);
  await botPool.query(
    'INSERT INTO pending_bot_actions (action, params, requested_by, sig) VALUES ($1, $2::jsonb, $3, $4)',
    [action, JSON.stringify(params), requestedBy, sig],
  );
  return true;
}

export interface StaffNotifyInput {
  kind: StaffNotifyKind;
  /**
   * 'new' posts a message; 'resolved' and 'update' edit the one posted for
   * the same ref. 'update' is a status that is not the end of the matter
   * (a bug report marked "Looking into it"): the message shows the status
   * but keeps its live styling.
   */
  event: 'new' | 'resolved' | 'update';
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
  /**
   * Detail rows for the embed. These notifications go to a PRIVATE staff
   * channel, so they carry what staff actually need to triage without
   * opening the site — the earlier summary-only form made almost every
   * notification a prompt to go and look the request up.
   *
   * Still bounded: empty values are dropped, each value is truncated, and
   * the whole set is capped, so one long free-text answer cannot blow the
   * embed limit or bury the rest.
   */
  fields?: Array<{ name: string; value: unknown; inline?: boolean }>;
}

/** Discord allows 25 fields / 1024 chars each; stay well inside both. */
const MAX_FIELDS = 12;
const MAX_FIELD_VALUE = 400;

/**
 * Drop empties, coerce, truncate, cap. Returns [] when nothing survives, so
 * a caller can pass everything it has and let this decide what is worth
 * sending.
 */
function packFields(fields: StaffNotifyInput['fields']): Array<{
  name: string;
  value: string;
  inline: boolean;
}> {
  if (!Array.isArray(fields)) return [];
  const out: Array<{ name: string; value: string; inline: boolean }> = [];
  for (const f of fields) {
    if (!f || !f.name) continue;
    let v: string;
    if (typeof f.value === 'boolean') v = f.value ? 'Yes' : 'No';
    else if (f.value == null) continue;
    else v = String(f.value).trim();
    if (!v) continue;
    if (v.length > MAX_FIELD_VALUE) v = `${v.slice(0, MAX_FIELD_VALUE - 1)}…`;
    out.push({ name: String(f.name).slice(0, 256), value: v, inline: f.inline !== false });
    if (out.length >= MAX_FIELDS) break;
  }
  return out;
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

  return enqueueBotAction('staff_notify', {
    kind: input.kind,
    event: input.event,
    ref: String(input.ref),
    guild_id: String(guildId),
    channel_id: String(channelId),
    title: input.title,
    subtitle: input.subtitle ?? '',
    status: input.status ?? '',
    actor: input.actor ?? '',
    // JSON in a string: every param stays a string so the canonical signing
    // form is identical either side of the language boundary.
    fields: JSON.stringify(packFields(input.fields)),
    url: `${publicBase()}/staff?view=${STAFF_VIEW[input.kind]}`,
  });
}

// ---------------------------------------------------------------------------
// Bug reports and suggestions: the public side (api/feedback.ts).
// ---------------------------------------------------------------------------

/**
 * Tell the bot which channels one kind uses now. Sent whenever the owner
 * changes either picker, with BOTH current values, so the bot's row for the
 * kind is always whole. An empty id means "none": the bot takes down its
 * pinned how-to from an intake channel that is no longer one.
 */
export async function enqueueFeedbackChannels(pool: Pool, kind: FeedbackKind): Promise<boolean> {
  const s = await readNotifySettings(pool);
  return enqueueBotAction('feedback_channels', {
    kind,
    guild_id: s[NOTIFY_GUILD_KEY] ?? '',
    intake_channel_id: s[FEEDBACK_CHANNEL_KEYS[kind].intake] ?? '',
    board_channel_id: s[FEEDBACK_CHANNEL_KEYS[kind].board] ?? '',
    site_url: publicBase(),
  });
}

export interface FeedbackBoardInput {
  /** 'new' posts the report; 'status' replies under it and edits its status; 'delete' removes it. */
  event: 'new' | 'status' | 'delete';
  kind: FeedbackKind;
  id: number;
  subject?: string;
  body?: string;
  /** The name shown on the board. */
  reporter?: string;
  source?: 'site' | 'discord';
  status: string;
  statusLabel: string;
  note?: string | null;
  actor?: string | null;
}

/**
 * The public board. Skipped (false) when the kind has no board channel —
 * the owner may well run intake without a board, or neither.
 * Discord-sourced reports are posted by the bot itself at filing time (it
 * holds the attachments then), so callers send 'new' for site reports only.
 */
export async function enqueueFeedbackBoard(pool: Pool, input: FeedbackBoardInput): Promise<boolean> {
  const s = await readNotifySettings(pool);
  const channelId = s[FEEDBACK_CHANNEL_KEYS[input.kind].board];
  const guildId = s[NOTIFY_GUILD_KEY];
  if (!channelId || !guildId) return false;
  return enqueueBotAction('feedback_board', {
    event: input.event,
    kind: input.kind,
    id: String(input.id),
    guild_id: String(guildId),
    channel_id: String(channelId),
    subject: input.subject ?? '',
    body: (input.body ?? '').slice(0, 4000),
    reporter: input.reporter ?? '',
    source: input.source ?? '',
    status: input.status,
    status_label: input.statusLabel,
    note: (input.note ?? '').slice(0, 1000),
    actor: input.actor ?? '',
    site_url: publicBase(),
  });
}

/** Fire-and-forget twin of enqueueFeedbackBoard, for request handlers. */
export function feedbackBoard(pool: Pool | null, input: FeedbackBoardInput): void {
  if (!pool) return;
  void enqueueFeedbackBoard(pool, input).catch((err) => {
    log.warn({ err: (err as Error).message, id: input.id, event: input.event }, 'feedback board: enqueue failed');
  });
}
