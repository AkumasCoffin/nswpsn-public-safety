/**
 * botActionSign unit tests.
 *
 * These assert the EXACT canonical string and the stability of the HMAC,
 * which is what guarantees cross-language parity with the Python bot in
 * discord-bot/database.py (canonical_bot_action / verify_bot_action_sig).
 *
 * Cross-language parity fixture — the Python side MUST produce the same
 * canonical string for the same inputs. With:
 *   action        = "broadcast"
 *   requested_by  = "42"
 *   params        = {"targets":[{"guild_id":"2","channel_id":"1"}],
 *                    "title":"hi","color":""}
 *
 * the canonical string (object keys sorted; inner object keys sorted;
 * no whitespace) is, byte-for-byte:
 *
 *   broadcast\n42\n{"color":"","targets":[{"channel_id":"1","guild_id":"2"}],"title":"hi"}
 *
 * Equivalent Python (discord-bot/database.py):
 *   >>> from database import canonical_bot_action, sign_bot_action
 *   >>> canonical_bot_action("broadcast", "42",
 *   ...   {"targets":[{"guild_id":"2","channel_id":"1"}],"title":"hi","color":""})
 *   'broadcast\n42\n{"color":"","targets":[{"channel_id":"1","guild_id":"2"}],"title":"hi"}'
 *   >>> sign_bot_action("shared-secret", "broadcast", "42", {...})   # same hex as signBotAction
 *
 * A human can eyeball parity by running the two one-liners and diffing.
 */
import { describe, it, expect } from 'vitest';
import {
  canonicalBotAction,
  signBotAction,
} from '../../../src/services/botActionSign.js';

const PARAMS = {
  targets: [{ guild_id: '2', channel_id: '1' }],
  title: 'hi',
  color: '',
};

// The exact canonical string the Python side must also produce.
const EXPECTED_CANONICAL =
  'broadcast\n42\n{"color":"","targets":[{"channel_id":"1","guild_id":"2"}],"title":"hi"}';

describe('canonicalBotAction', () => {
  it('produces the known exact canonical string with sorted keys', () => {
    expect(canonicalBotAction('broadcast', '42', PARAMS)).toBe(EXPECTED_CANONICAL);
  });

  it('sorts object keys regardless of insertion order (JSONB round-trip safe)', () => {
    // Same logical params, keys inserted in a different order — must yield
    // the identical canonical string.
    const reordered = {
      color: '',
      title: 'hi',
      targets: [{ channel_id: '1', guild_id: '2' }],
    };
    expect(canonicalBotAction('broadcast', '42', reordered)).toBe(EXPECTED_CANONICAL);
  });

  it('serialises scalars: null, boolean before number, and quoted strings', () => {
    expect(canonicalBotAction('a', 'b', null)).toBe('a\nb\nnull');
    expect(canonicalBotAction('a', 'b', { x: true, y: false })).toBe(
      'a\nb\n{"x":true,"y":false}',
    );
    expect(canonicalBotAction('a', 'b', { s: 'he"llo' })).toBe(
      'a\nb\n{"s":"he\\"llo"}',
    );
  });
});

describe('signBotAction', () => {
  const secret = 'shared-secret';

  it('is deterministic / stable for identical inputs', () => {
    const a = signBotAction(secret, 'broadcast', '42', PARAMS);
    const b = signBotAction(secret, 'broadcast', '42', PARAMS);
    expect(a).toBe(b);
    // hex sha256 -> 64 chars
    expect(a).toMatch(/^[0-9a-f]{64}$/);
  });

  it('changes when params change', () => {
    const base = signBotAction(secret, 'broadcast', '42', PARAMS);
    const changed = signBotAction(secret, 'broadcast', '42', {
      ...PARAMS,
      title: 'bye',
    });
    expect(changed).not.toBe(base);
  });

  it('changes when the action or requested_by changes', () => {
    const base = signBotAction(secret, 'broadcast', '42', PARAMS);
    expect(signBotAction(secret, 'cleanup', '42', PARAMS)).not.toBe(base);
    expect(signBotAction(secret, 'broadcast', '99', PARAMS)).not.toBe(base);
  });

  it('changes when the secret changes', () => {
    const a = signBotAction('secret-a', 'broadcast', '42', PARAMS);
    const b = signBotAction('secret-b', 'broadcast', '42', PARAMS);
    expect(a).not.toBe(b);
  });
});
