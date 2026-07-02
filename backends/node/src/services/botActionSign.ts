/**
 * HMAC signing for bot-action queue rows.
 *
 * The web dashboard enqueues rows into `pending_bot_actions` (broadcast /
 * sync / test / cleanup) that the Discord bot later drains and executes
 * WITH NO further authentication — anyone who can INSERT a row would get
 * unattended Discord actions. To close that hole the backend signs each
 * row with a shared secret (BOT_ACTION_SIGNING_SECRET) and the bot
 * verifies the signature before dispatching.
 *
 * The canonical string + HMAC below MUST stay byte-for-byte identical to
 * the Python implementation in discord-bot/database.py
 * (canonical_bot_action / verify_bot_action_sig) or cross-language
 * verification silently fails. See botActionSign.test.ts for a parity
 * fixture (with the equivalent Python output in a comment).
 *
 *   canonical = action + "\n" + requested_by + "\n" + stableJson(params)
 *   signature = hex( HMAC-SHA256(secret_utf8, canonical_utf8) )
 *
 * stableJson is a deterministic serializer: object keys are sorted so the
 * canonical form round-trips through Postgres JSONB (which reorders keys).
 * All params values in this system are strings / arrays-of-strings /
 * arrays-of-objects-of-strings, so no floats can cause number-formatting
 * drift between the two languages.
 */
import { createHmac } from 'node:crypto';
import { config } from '../config.js';

/**
 * The shared signing secret, or null when unset/empty. When null the
 * backend enqueues rows WITHOUT a signature and the bot fails open (for a
 * staged rollout) — both sides log a warning.
 */
export function getBotActionSecret(): string | null {
  const s = config.BOT_ACTION_SIGNING_SECRET;
  return s && s.length > 0 ? s : null;
}

/**
 * Deterministic JSON serializer. Matches Python's json.dumps for scalar
 * quoting but sorts object keys and never inserts whitespace, so both
 * languages (and a JSONB round-trip) produce the identical string.
 *
 * - null      -> "null"
 * - boolean   -> "true" / "false"   (checked BEFORE number)
 * - number    -> JSON number form   (none expected here, but handled)
 * - string    -> JSON.stringify(s)  (JSON-quoted)
 * - array     -> "[" + items.map(stableJson).join(",") + "]"
 * - object    -> "{" + sortedKeys.map(k => JSON.stringify(k)+":"+stableJson(v)).join(",") + "}"
 */
function stableJson(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') return JSON.stringify(value);
  if (typeof value === 'string') return JSON.stringify(value);
  if (Array.isArray(value)) {
    return '[' + value.map((v) => stableJson(v)).join(',') + ']';
  }
  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    return (
      '{' +
      keys.map((k) => JSON.stringify(k) + ':' + stableJson(obj[k])).join(',') +
      '}'
    );
  }
  // Any other type (function/symbol/bigint) is unexpected here; serialise
  // as null so the signature is still deterministic rather than throwing.
  return 'null';
}

/**
 * Build the canonical signing string for a bot-action row. Kept exported
 * for the parity test.
 */
export function canonicalBotAction(
  action: string,
  requestedBy: string,
  params: unknown,
): string {
  return action + '\n' + requestedBy + '\n' + stableJson(params);
}

/**
 * Compute the hex HMAC-SHA256 signature for a bot-action row.
 */
export function signBotAction(
  secret: string,
  action: string,
  requestedBy: string,
  params: unknown,
): string {
  const canonical = canonicalBotAction(action, requestedBy, params);
  return createHmac('sha256', Buffer.from(secret, 'utf8'))
    .update(Buffer.from(canonical, 'utf8'))
    .digest('hex');
}
