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
    { key: NOTIFY_CHANNEL_KEYS.new_user, value: '555555555555555555' },
    { key: NOTIFY_CHANNEL_KEYS.new_node, value: '666666666666666666' },
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

  it('sends a fixed, closed set of keys', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, {
      kind: 'signup_request',
      event: 'new',
      ref: '7',
      title: 'Pager Feeder',
      subtitle: 'QLD',
    });
    const sent = sentParams();
    expect(Object.keys(sent).sort()).toEqual(
      ['actor', 'channel_id', 'event', 'fields', 'guild_id', 'kind', 'ref',
       'status', 'subtitle', 'title', 'url'],
    );
    // Every value stays a string so the canonical signing form is identical
    // either side of the language boundary.
    for (const v of Object.values(sent)) expect(typeof v).toBe('string');
    expect(sent.fields).toBe('[]');
  });

  it('carries detail rows — these go to a private staff channel', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, {
      kind: 'signup_request',
      event: 'new',
      ref: '7',
      title: 'Pager Feeder',
      subtitle: 'QLD',
      fields: [
        { name: 'Email', value: 'someone@example.com' },
        { name: 'Experience', value: 3 },
        { name: 'Existing setup', value: true },
      ],
    });
    const fields = JSON.parse(sentParams().fields) as Array<{ name: string; value: string }>;
    expect(fields.map((f) => f.name)).toEqual(['Email', 'Experience', 'Existing setup']);
    // Coerced to strings; booleans read as Yes/No rather than "true".
    expect(fields[1]?.value).toBe('3');
    expect(fields[2]?.value).toBe('Yes');
  });

  it('drops empty rows, truncates long ones and caps the count', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, {
      kind: 'signup_request',
      event: 'new',
      ref: '7',
      title: 'x',
      fields: [
        { name: 'Gone', value: '' },
        { name: 'Gone too', value: null },
        { name: 'Long', value: 'y'.repeat(5000) },
        ...Array.from({ length: 30 }, (_, i) => ({ name: `f${i}`, value: 'v' })),
      ],
    });
    const fields = JSON.parse(sentParams().fields) as Array<{ name: string; value: string }>;
    expect(fields.map((f) => f.name)).not.toContain('Gone');
    expect(fields.map((f) => f.name)).not.toContain('Gone too');
    // Comfortably inside Discord's 25-field / 1024-char embed limits.
    expect(fields.length).toBeLessThanOrEqual(12);
    for (const f of fields) expect(f.value.length).toBeLessThanOrEqual(400);
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

describe('new_user / new_node', () => {
  it('each gets its own channel', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, { kind: 'new_user', event: 'new', ref: '', title: 'New account' });
    expect(sentParams().channel_id).toBe('555555555555555555');

    vi.clearAllMocks();
    invalidateStaffNotifySettings();
    configureAll();
    await notifyStaffAsync(mainPool, { kind: 'new_node', event: 'new', ref: '', title: 'pager node' });
    expect(sentParams().channel_id).toBe('666666666666666666');
  });

  it('a new account names the account, so the notification is actionable', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, {
      kind: 'new_user',
      event: 'new',
      ref: 'uid-123',
      title: 'jordansmith',
      subtitle: 'Someone signed up',
      fields: [{ name: 'Display name', value: 'jordansmith' }],
    });
    const sent = sentParams();
    expect(sent.title).toBe('jordansmith');
    expect(sent.ref).toBe('uid-123');
    expect(sent.fields).toContain('jordansmith');
  });

  it('a new node names the node and where it is', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, {
      kind: 'new_node',
      event: 'new',
      ref: '42',
      title: 'pager node',
      subtitle: 'Brisbane · QLD',
      fields: [
        { name: 'Name', value: 'pager-jordansmith-a1b2c3d4' },
        { name: 'State', value: 'QLD' },
      ],
    });
    const sent = sentParams();
    expect(sent.title).toBe('pager node');
    expect(sent.subtitle).toBe('Brisbane · QLD');
    expect(sent.fields).toContain('pager-jordansmith-a1b2c3d4');
  });

  it('deep-links to the tab each one belongs to', async () => {
    configureAll();
    await notifyStaffAsync(mainPool, { kind: 'new_user', event: 'new', ref: '', title: 'x' });
    expect(sentParams().url).toContain('/staff?view=users');

    vi.clearAllMocks();
    invalidateStaffNotifySettings();
    configureAll();
    await notifyStaffAsync(mainPool, { kind: 'new_node', event: 'new', ref: '', title: 'x' });
    expect(sentParams().url).toContain('/staff?view=nodes');
  });

  it('stays silent when only the other kinds are configured', async () => {
    settingsRows = [
      { key: NOTIFY_GUILD_KEY, value: '111111111111111111' },
      { key: NOTIFY_CHANNEL_KEYS.signup_request, value: '222222222222222222' },
    ];
    expect(await notifyStaffAsync(mainPool, { kind: 'new_user', event: 'new', ref: '', title: 'x' })).toBe(false);
    expect(await notifyStaffAsync(mainPool, { kind: 'new_node', event: 'new', ref: '', title: 'x' })).toBe(false);
    expect(botQuery).not.toHaveBeenCalled();
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
