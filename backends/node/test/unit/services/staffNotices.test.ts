/**
 * Manual notifications — who a target resolves to, and the all-or-nothing
 * send. The roster is user_roles, so these tests are mostly about agreeing
 * with that table rather than about the message.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

interface Call { sql: string; params?: unknown[] }
let calls: Call[] = [];
let resultQueue: Array<{ rows: unknown[] }> = [];
let failOn: RegExp | null = null;

const fakeClient = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    if (failOn && failOn.test(sql)) throw new Error('boom');
    if (/^(BEGIN|COMMIT|ROLLBACK)/.test(sql)) return { rows: [] };
    return resultQueue.shift() ?? { rows: [] };
  }),
  release: vi.fn(),
};
const fakePool = {
  query: vi.fn(async (sql: string, params?: unknown[]) => {
    calls.push({ sql, ...(params ? { params } : {}) });
    return resultQueue.shift() ?? { rows: [] };
  }),
  connect: vi.fn(async () => fakeClient),
};

const {
  resolveRecipients, sendNotice, listNotices, targetCounts, NOTICE_TYPE,
} = await import('../../../src/services/staffNotices.js');

const rows = (ids: string[]) => ({ rows: ids.map((user_id) => ({ user_id })) });
const sqlOf = (fragment: string) => calls.find((c) => c.sql.includes(fragment));

beforeEach(() => {
  calls = [];
  resultQueue = [];
  failOn = null;
  fakeClient.query.mockClear();
  fakePool.query.mockClear();
});

describe('resolveRecipients', () => {
  it('everyone is the authed roster, not an admin API listing', async () => {
    resultQueue = [rows(['a', 'b', 'c'])];
    const out = await resolveRecipients(fakePool as never, { audience: 'all' });
    expect(out).toEqual(['a', 'b', 'c']);
    expect(sqlOf("role = 'authed'")).toBeDefined();
  });

  it('a role target asks for the legacy name too', async () => {
    // Migration 059 renamed the roles and left existing rows alone, so asking
    // for the canonical name alone silently misses everyone not rewritten.
    resultQueue = [rows(['a'])];
    await resolveRecipients(fakePool as never, { audience: 'role', role: 'wire:contributor' });
    const q = sqlOf('role = ANY');
    expect(q?.params?.[0]).toEqual(['wire:contributor', 'media_feeder']);
  });

  it('a role with no legacy name asks for just itself', async () => {
    resultQueue = [rows([])];
    await resolveRecipients(fakePool as never, { audience: 'role', role: 'support' });
    expect(sqlOf('role = ANY')?.params?.[0]).toEqual(['support']);
  });

  it('accepts a legacy role name and still finds both', async () => {
    resultQueue = [rows(['a'])];
    await resolveRecipients(fakePool as never, { audience: 'role', role: 'media_feeder' });
    expect(sqlOf('role = ANY')?.params?.[0]).toEqual(['wire:contributor', 'media_feeder']);
  });

  it('named people are checked against the roster, not trusted', async () => {
    // 'ghost' is not an account this site knows about; a notification row for
    // it would sit in nobody's inbox forever.
    resultQueue = [rows(['real-1', 'real-2'])];
    const out = await resolveRecipients(fakePool as never, {
      audience: 'users', userIds: ['real-1', 'real-2', 'ghost'],
    });
    expect(out).toEqual(['real-1', 'real-2']);
    expect(sqlOf('user_id = ANY')?.params?.[0]).toEqual(['real-1', 'real-2', 'ghost']);
  });

  it('dedupes and drops blanks before asking', async () => {
    resultQueue = [rows(['a'])];
    await resolveRecipients(fakePool as never, { audience: 'users', userIds: ['a', 'a', ' ', ''] });
    expect(sqlOf('user_id = ANY')?.params?.[0]).toEqual(['a']);
  });

  it('an empty pick asks nothing', async () => {
    expect(await resolveRecipients(fakePool as never, { audience: 'users', userIds: [] })).toEqual([]);
    expect(await resolveRecipients(fakePool as never, { audience: 'role', role: '  ' })).toEqual([]);
    expect(calls).toHaveLength(0);
  });
});

describe('sendNotice', () => {
  const input = {
    sentBy: 'staff-1', sentByName: 'Staff One',
    target: { audience: 'role' as const, role: 'feeder:radio' },
    title: 'Maintenance Sunday', body: 'The site will be down 0200-0300.', link: '/status',
  };

  it('records the send and delivers it in one transaction', async () => {
    resultQueue = [{ rows: [{ id: '7' }] }];
    const out = await sendNotice(fakePool as never, input, ['a', 'b', 'c']);
    expect(out).toEqual({ noticeId: '7', recipients: 3 });

    const order = calls.map((c) => c.sql.trim().split(/\s+/).slice(0, 2).join(' '));
    expect(order[0]).toBe('BEGIN');
    expect(order[order.length - 1]).toBe('COMMIT');

    const notice = sqlOf('INSERT INTO staff_notices');
    expect(notice?.params?.slice(0, 6)).toEqual(['staff-1', 'Staff One', 'role', 'feeder:radio', null, 3]);

    const fanout = sqlOf('INSERT INTO notifications');
    expect(fanout?.params?.[0]).toEqual(['a', 'b', 'c']);
    expect(fanout?.params?.[1]).toBe(NOTICE_TYPE);
    expect(fanout?.params?.[2]).toBe('Maintenance Sunday');
    expect(fanout?.params?.[4]).toBe('/status');
    expect(JSON.parse(String(fanout?.params?.[5]))).toEqual({ noticeId: 7 });
  });

  it('keeps the named recipients on the record, but not for a role send', async () => {
    resultQueue = [{ rows: [{ id: '8' }] }];
    await sendNotice(
      fakePool as never,
      { ...input, target: { audience: 'users', userIds: ['a', 'b'] } },
      ['a', 'b'],
    );
    const notice = sqlOf('INSERT INTO staff_notices');
    expect(notice?.params?.[3]).toBeNull();          // no role
    expect(notice?.params?.[4]).toEqual(['a', 'b']); // who exactly
  });

  it('a failed fan-out writes neither the record nor the rows', async () => {
    // The alternative reports "sent to 340 people" when nobody got it.
    resultQueue = [{ rows: [{ id: '9' }] }];
    failOn = /INSERT INTO notifications/;
    await expect(sendNotice(fakePool as never, input, ['a', 'b'])).rejects.toThrow('boom');
    expect(calls.some((c) => c.sql.startsWith('ROLLBACK'))).toBe(true);
    expect(calls.some((c) => c.sql.startsWith('COMMIT'))).toBe(false);
    expect(fakeClient.release).toHaveBeenCalled();
  });

  it('refuses to send to nobody', async () => {
    await expect(sendNotice(fakePool as never, input, [])).rejects.toThrow(/at least one recipient/);
    expect(calls).toHaveLength(0);
  });

  it('dedupes recipients so one person cannot be told twice', async () => {
    resultQueue = [{ rows: [{ id: '10' }] }];
    const out = await sendNotice(fakePool as never, input, ['a', 'a', 'b']);
    expect(out.recipients).toBe(2);
    expect(sqlOf('INSERT INTO notifications')?.params?.[0]).toEqual(['a', 'b']);
  });
});

describe('targetCounts', () => {
  it('counts each audience the way the send will resolve it', async () => {
    resultQueue = [{ rows: [
      { role: 'authed', user_id: 'a' }, { role: 'authed', user_id: 'b' }, { role: 'authed', user_id: 'c' },
      { role: 'feeder:radio', user_id: 'a' },
      // The same person under both names of one role counts once.
      { role: 'media_feeder', user_id: 'b' }, { role: 'wire:contributor', user_id: 'b' },
    ] }];
    const out = await targetCounts(fakePool as never);
    expect(out.all).toBe(3);
    expect(out.byRole).toEqual([
      { role: 'feeder:radio', count: 1 },
      { role: 'wire:contributor', count: 1 },
    ]);
  });

  it('is empty rather than broken with no roles at all', async () => {
    resultQueue = [{ rows: [] }];
    expect(await targetCounts(fakePool as never)).toEqual({ all: 0, byRole: [] });
  });
});

describe('listNotices', () => {
  it('clamps the limit and maps the row', async () => {
    resultQueue = [{ rows: [{
      id: '3', sent_by: 'staff-1', sent_by_name: 'Staff One', audience: 'all',
      target_role: null, recipients: 42, title: 'Hi', body: 'There', link: null,
      created_at: new Date('2026-10-05T00:00:00Z'),
    }] }];
    const out = await listNotices(fakePool as never, 9999);
    expect(sqlOf('FROM staff_notices')?.params?.[0]).toBe(100);
    expect(out[0]).toMatchObject({ id: '3', audience: 'all', recipients: 42, sentByName: 'Staff One' });
    expect(out[0]!.createdAt).toBe('2026-10-05T00:00:00.000Z');
  });
});
