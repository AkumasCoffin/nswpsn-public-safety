/**
 * Staff notifications → Discord.
 *
 * The behaviour worth pinning here is mostly about what must NOT happen:
 * a moderation action must never fail because Discord is unreachable, and
 * the payload must never carry personal data. Both are invisible until the
 * day they matter — the first as a 500 on an approval, the second as an
 * applicant's email sitting permanently in a Discord channel.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

/** The bot-DB pool the outbox row is inserted into. */
const botQuery = vi.fn(async () => ({ rows: [], rowCount: 1 }));
let botPoolAvailable = true;
vi.mock('../../../src/services/botDb.js', () => ({
  getBotDbPool: vi.fn(async () => (botPoolAvailable ? { query: botQuery } : null)),
  isBotDbConfigured: vi.fn(() => botPoolAvailable),
}));

let signingSecret: string | null = 'test-secret';
vi.mock('../../../src/services/botActionSign.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/botActionSign.js')>();
  return { ...actual, getBotActionSecret: vi.fn(() => signingSecret) };
});

const {
  notifyStaffAsync,
  writeNotifySetting,
  invalidateStaffNotifySettings,
  NOTIFY_GUILD_KEY,
  NOTIFY_CHANNEL_KEYS,
} = await import('../../../src/services/staffNotify.js');

/** Main-DB pool holding app_settings. */
let settingsRows: Array<{ key: string; value: string }> = [];
const mainQuery = vi.fn(async (sql: string) => {
  if (sql.includes('SELECT key, value FROM app_settings')) return { rows: settingsRows, rowCount: settingsRows.length };
  return { rows: [], rowCount: 0 };
});
const mainPool = { query: mainQuery } as never;

/** The INSERT's bound params: [action, paramsJson, requestedBy, sig]. */
function insertArgs(call = 0): unknown[] {
  return (botQuery.mock.calls[call] as unknown as [string, unknown[]])[1];
}
/** The enqueued params object, parsed. */
function sentParams(call = 0): Record<string, string> {
  return JSON.parse(insertArgs(call)[1] as string) as Record<string, string>;
}

function configureAll() {
  settingsRows = [
    { key: NOTIFY_GUILD_KEY, value: '111111111111111111' },
    { key: NOTIFY_CHANNEL_KEYS.signup_request, value: '222222222222222222' },
    { key: NOTIFY_CHANNEL_KEYS.wire_approval, value: '333333333333333333' },
    { key: NOTIFY_CHANNEL_KEYS.wire_takedown, value: '444444444444444444' },
  ];
}

beforeEach(() => {
  vi.clearAllMocks();
  invalidateStaffNotifySettings();
  botPoolAvailable = true;
  signingSecret = 'test-secret';
  settingsRows = [];
});

describe('notifyStaffAsync', () => {
  it('enqueues a signed staff_notify row when a channel is configured', async () => {
    configureAll();
    const ok = await notifyStaffAsync(mainPool, {
      kind: 'signup_request',
      event: 'new',
      ref: '42',
      title: 'Radio Feeder',
      subtitle: 'NSW',
    });
    expect(ok).toBe(true);
    expect(botQuery).toHaveBeenCalledTimes(1);
    const sql = (botQuery.mock.calls[0] as unknown as [string])[0];
    expect(sql).toContain('INSERT INTO pending_bot_actions');
    const params = insertArgs();
    expect(params[0]).toBe('staff_notify');
    const sent = sentParams();
    expect(sent.kind).toBe('signup_request');
    expect(sent.ref).toBe('42');
    expect(sent.channel_id).toBe('222222222222222222');
    expect(sent.guild_id).toBe('111111111111111111');
    // A signature must be present, or the bot's gate is pointless.
    expect(typeof params[3]).toBe('string');
    expect((params[3] as string).length).toBeGreaterThan(0);
  });

  it('routes each kind to its own channel', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, { kind: 'wire_takedown', event: 'new', ref: 't1', title: 'X' });
    const sent = sentParams();
    expect(sent.channel_id).toBe('444444444444444444');
  });

  it('carries no personal data — only the fields the caller passed', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, {
      kind: 'signup_request',
      event: 'new',
      ref: '7',
      title: 'Pager Feeder',
      subtitle: 'QLD',
    });
    const sent = sentParams();
    // The payload is a fixed, closed set of keys. If someone adds a field
    // that could carry an email or a complaint body, this fails.
    expect(Object.keys(sent).sort()).toEqual(
      ['actor', 'channel_id', 'event', 'guild_id', 'kind', 'ref', 'status', 'subtitle', 'title', 'url'],
    );
    const blob = JSON.stringify(sent).toLowerCase();
    expect(blob).not.toContain('@');           // no email address
    expect(blob).not.toContain('password');    // editor_requests.notes holds one
  });

  it('does nothing when the kind has no channel configured', async () => {
    settingsRows = [{ key: NOTIFY_GUILD_KEY, value: '111111111111111111' }];
    const ok = await notifyStaffAsync(mainPool, { kind: 'wire_approval', event: 'new', ref: 'a', title: 'X' });
    expect(ok).toBe(false);
    expect(botQuery).not.toHaveBeenCalled();
  });

  it('does nothing when no guild is set, even if a channel id lingers', async () => {
    settingsRows = [{ key: NOTIFY_CHANNEL_KEYS.signup_request, value: '222222222222222222' }];
    const ok = await notifyStaffAsync(mainPool, { kind: 'signup_request', event: 'new', ref: 'a', title: 'X' });
    expect(ok).toBe(false);
    expect(botQuery).not.toHaveBeenCalled();
  });

  it('does nothing when the bot database is not configured', async () => {
    configureAll();
    botPoolAvailable = false;
    const ok = await notifyStaffAsync(mainPool, { kind: 'signup_request', event: 'new', ref: 'a', title: 'X' });
    expect(ok).toBe(false);
  });

  it('still enqueues (unsigned) when the signing secret is unset', async () => {
    // Fails open deliberately, matching the bot — but the row must still go.
    configureAll();
    signingSecret = null;
    const ok = await notifyStaffAsync(mainPool, { kind: 'signup_request', event: 'new', ref: 'a', title: 'X' });
    expect(ok).toBe(true);
    expect(insertArgs()[3]).toBeNull();
  });

  it('returns false rather than throwing when there is no pool', async () => {
    await expect(
      notifyStaffAsync(null, { kind: 'signup_request', event: 'new', ref: 'a', title: 'X' }),
    ).resolves.toBe(false);
  });

  it('sends a resolved event with status and actor so the bot can edit in place', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, {
      kind: 'wire_approval',
      event: 'resolved',
      ref: 'article:abc',
      title: 'Article',
      status: 'approved',
      actor: 'Jordan',
    });
    const sent = sentParams();
    expect(sent.event).toBe('resolved');
    // Same ref as the 'new' event — that's how the bot finds the message.
    expect(sent.ref).toBe('article:abc');
    expect(sent.status).toBe('approved');
    expect(sent.actor).toBe('Jordan');
  });

  it('links back to the matching staff view', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, { kind: 'wire_takedown', event: 'new', ref: 'x', title: 'T' });
    const sent = sentParams();
    expect(sent.url).toContain('/staff?view=takedowns');
  });
});

describe('writeNotifySetting', () => {
  it('refuses a key outside the known set', async () => {
    await expect(writeNotifySetting(mainPool, 'wire_public', 'true', 'u1')).rejects.toThrow(/unknown/);
  });

  it('deletes rather than storing an empty string when cleared', async () => {
    await writeNotifySetting(mainPool, NOTIFY_GUILD_KEY, null, 'u1');
    const sql = String((mainQuery.mock.calls.at(-1) as unknown as [string])[0]);
    expect(sql).toContain('DELETE FROM app_settings');
  });

  it('upserts when given a value', async () => {
    await writeNotifySetting(mainPool, NOTIFY_GUILD_KEY, '123456789012345678', 'u1');
    const sql = String((mainQuery.mock.calls.at(-1) as unknown as [string])[0]);
    expect(sql).toContain('INSERT INTO app_settings');
    expect(sql).toContain('ON CONFLICT');
  });
});
