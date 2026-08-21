/**
 * services/nodeEvents.ts — activity-event capture + logical grouping unit
 * tests (mocked writer pool; verifies the SQL sequencing + parameters, not a
 * live Postgres).
 *
 * Covers (migration 044 model — radio rows come from vce activity events):
 *   - recordActivityEvents: new group / joined group logical_calls 1/0
 *   - dedupe: ON CONFLICT DO NOTHING re-send → accepted 0, no bucket bumps
 *   - grouping across two nodes' events with the same systemId+target ±4s
 *   - atMs clock sanity clamp (now±48h)
 *   - fire-safe contract: DB failure never throws, ROLLBACK is attempted
 *   - markRecorded: closest-row match stamps recorded+bytes (+hourly bytes),
 *     no-match is a clean no-op
 *   - pager path unchanged: sha256(trimmed message) grouping, logical_pages
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createHash } from 'node:crypto';

const clientQuery = vi.fn();
const clientRelease = vi.fn();
const connectMock = vi.fn(async () => ({ query: clientQuery, release: clientRelease }));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => null),
  getWriterPool: vi.fn(async () => ({ connect: connectMock })),
  closePool: vi.fn(async () => undefined),
}));

import {
  recordActivityEvents,
  markRecorded,
  recordPagerEvent,
  safeInt,
  type ActivityEventInput,
} from '../../../src/services/nodeEvents.js';

/** Route mocked queries by SQL substring. `insertIds` is consumed per
 *  activity-event INSERT; an entry of null simulates the ON CONFLICT dedupe
 *  (no row returned). `foundRadio` may be a function to vary per lookup. */
function armQueries(opts: {
  foundRadio?: string | null | (() => string | null);
  foundPager?: string | null;
  insertIds?: Array<string | null>;
  updatedRow?: { received_at: string; system: number | null; talkgroup: number | null } | null;
}) {
  const insertQueue = [...(opts.insertIds ?? ['101'])];
  clientQuery.mockImplementation(async (sql: string) => {
    if (sql.includes('SELECT logical_call_id FROM node_radio_events')) {
      const v = typeof opts.foundRadio === 'function' ? opts.foundRadio() : opts.foundRadio;
      return { rows: v ? [{ logical_call_id: v }] : [] };
    }
    if (sql.includes('SELECT logical_id FROM node_pager_events')) {
      return { rows: opts.foundPager ? [{ logical_id: opts.foundPager }] : [] };
    }
    if (sql.includes('UPDATE node_radio_events SET recorded = true')) {
      const row = opts.updatedRow;
      return {
        rows: row ? [{ received_at: new Date(row.received_at), system: row.system, talkgroup: row.talkgroup }] : [],
        rowCount: row ? 1 : 0,
      };
    }
    if (sql.includes('INSERT INTO node_radio_events')) {
      const id = insertQueue.length > 0 ? insertQueue.shift() : '101';
      return { rows: id === null || id === undefined ? [] : [{ id }] };
    }
    if (sql.includes('RETURNING id')) {
      // pager insert
      return { rows: [{ id: insertQueue.shift() ?? '201' }] };
    }
    return { rows: [] };
  });
}

/** Params of the Nth (default first) call whose SQL contains the fragment. */
function callWith(sqlFragment: string, nth = 0): unknown[] | undefined {
  const calls = clientQuery.mock.calls.filter(
    (args) => typeof args[0] === 'string' && (args[0] as string).includes(sqlFragment),
  );
  return calls[nth]?.[1] as unknown[] | undefined;
}

function countCalls(sqlFragment: string): number {
  return clientQuery.mock.calls.filter(
    (args) => typeof args[0] === 'string' && (args[0] as string).includes(sqlFragment),
  ).length;
}

const baseEvent: ActivityEventInput = {
  id: 9001,
  atMs: Date.now(),
  action: 'call',
  eventType: 'GROUP_VOICE_CHANNEL_GRANT',
  source: 777,
  target: 12345,
  frequencyHz: 420_662_500,
  timeslot: null,
  encrypted: false,
  rfss: 1,
  site: 12,
  nac: 0x2f4,
  wacn: 0xbee00,
  systemId: 0x4a2,
  channelName: 'PolAir Sydney',
};

describe('recordActivityEvents', () => {
  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
  });

  it('starts a new group: logical_call_id = own id, logical_calls +1, accepted 1', async () => {
    armQueries({ foundRadio: null, insertIds: ['101'] });
    const accepted = await recordActivityEvents('node-aaaa', 'stream-1234', [{ ...baseEvent }]);
    expect(accepted).toBe(1);

    // Advisory lock key uses systemId + target with -1 fallbacks.
    expect(callWith('pg_advisory_xact_lock')).toEqual([`nrc:${0x4a2}:${12345}`]);

    // Group find matched on system = systemId, talkgroup = target.
    const find = callWith('SELECT logical_call_id FROM node_radio_events');
    expect(find?.[0]).toBe(0x4a2);
    expect(find?.[1]).toBe(12345);
    expect(find?.[3]).toBe(777);

    expect(callWith('SET logical_call_id')).toEqual(['101', '101']);

    // INSERT carries the dedupe triple + the new columns.
    const ins = callWith('INSERT INTO node_radio_events');
    // [nodeId, receivedAt, streamId, sourceEventId, action, eventType,
    //  system, talkgroup, sourceUnit, frequency, timeslot, encrypted,
    //  rfss, site, nac, wacn]
    expect(ins?.[0]).toBe('node-aaaa');
    expect(ins?.[2]).toBe('stream-1234');
    expect(ins?.[3]).toBe(9001);
    expect(ins?.[4]).toBe('call');
    expect(ins?.[5]).toBe('GROUP_VOICE_CHANNEL_GRANT');
    expect(ins?.[6]).toBe(0x4a2);
    expect(ins?.[7]).toBe(12345);
    expect(ins?.[11]).toBe(false);
    expect(ins?.[15]).toBe(0xbee00);
    // New columns [16]=system_label (systemName), [17]=source_alias — null
    // when the event carries neither (older agents / unresolved joins).
    expect(ins?.[16]).toBeNull();
    expect(ins?.[17]).toBeNull();

    const sys = callWith('node_radio_hourly_sys');
    // [receivedAt, system, talkgroup, site_rfss, site_id, logicalIncrement]
    expect(sys?.[1]).toBe(0x4a2);
    expect(sys?.[2]).toBe(12345);
    expect(sys?.[3]).toBe(1);
    expect(sys?.[4]).toBe(12);
    expect(sys?.[5]).toBe(1); // new group → logical_calls +1

    expect(clientQuery.mock.calls.some((a) => a[0] === 'COMMIT')).toBe(true);
    expect(clientRelease).toHaveBeenCalled();
  });

  it('writes the friendly systemName and OTA sourceAlias into system_label/source_alias', async () => {
    armQueries({ foundRadio: null, insertIds: ['101'] });
    await recordActivityEvents('node-aaaa', 'stream-1234', [
      { ...baseEvent, systemName: 'NSWPSN', sourceAlias: 'NEWRAD08' },
    ]);
    const ins = callWith('INSERT INTO node_radio_events');
    expect(ins?.[16]).toBe('NSWPSN');
    expect(ins?.[17]).toBe('NEWRAD08');
  });

  it("groups two nodes' events with the same systemId+target within ±4s", async () => {
    // Node A starts the group…
    armQueries({ foundRadio: null, insertIds: ['101'] });
    const a = await recordActivityEvents('node-aaaa', 'stream-aaaa', [{ ...baseEvent }]);
    expect(a).toBe(1);
    expect(callWith('SET logical_call_id')).toEqual(['101', '101']);
    expect(callWith('node_radio_hourly_sys')?.[5]).toBe(1);

    // …node B's reception 2s later finds it and JOINS (no logical increment).
    clientQuery.mockReset();
    clientRelease.mockReset();
    armQueries({ foundRadio: '101', insertIds: ['102'] });
    const b = await recordActivityEvents('node-bbbb', 'stream-bbbb', [
      { ...baseEvent, id: 4, atMs: baseEvent.atMs + 2_000, rfss: 1, site: 12 },
    ]);
    expect(b).toBe(1);
    expect(callWith('SET logical_call_id')).toEqual(['101', '102']);
    expect(callWith('node_radio_hourly_sys')?.[5]).toBe(0);
  });

  it('dedupes a re-sent event: accepted 0, no stamps, no bucket bumps', async () => {
    armQueries({ foundRadio: null, insertIds: [null] }); // ON CONFLICT → no row
    const accepted = await recordActivityEvents('node-aaaa', 'stream-1234', [{ ...baseEvent }]);
    expect(accepted).toBe(0);
    expect(countCalls('SET logical_call_id')).toBe(0);
    expect(countCalls('node_radio_hourly')).toBe(0); // covers _sys too (substring)
    // The tx still completes cleanly (releases the advisory lock).
    expect(clientQuery.mock.calls.some((a) => a[0] === 'COMMIT')).toBe(true);
  });

  it('counts only newly-inserted events in a mixed batch', async () => {
    armQueries({ foundRadio: null, insertIds: ['101', null, '103'] });
    const accepted = await recordActivityEvents('node-aaaa', 'stream-1234', [
      { ...baseEvent, id: 1 },
      { ...baseEvent, id: 2 }, // duplicate re-send
      { ...baseEvent, id: 3 },
    ]);
    expect(accepted).toBe(2);
    expect(countCalls('SET logical_call_id')).toBe(2);
    expect(countCalls('node_radio_hourly_sys')).toBe(2);
  });

  it('encodes unknown systemId/target as -1 in the lock key and unknown site as -1 in the sys bucket', async () => {
    armQueries({ foundRadio: null, insertIds: ['101'] });
    await recordActivityEvents('node-aaaa', 'stream-1234', [
      { ...baseEvent, systemId: null, target: null, rfss: null, site: null },
    ]);
    expect(callWith('pg_advisory_xact_lock')).toEqual(['nrc:-1:-1']);
    const sys = callWith('node_radio_hourly_sys');
    expect(sys?.[1]).toBe(0);
    expect(sys?.[2]).toBe(0);
    expect(sys?.[3]).toBe(-1);
    expect(sys?.[4]).toBe(-1);
  });

  it('clamps a wildly wrong node clock to now', async () => {
    armQueries({ foundRadio: null, insertIds: ['101'] });
    await recordActivityEvents('node-aaaa', 'stream-1234', [
      { ...baseEvent, atMs: new Date('1999-01-01T00:00:00Z').getTime() },
    ]);
    const ins = callWith('INSERT INTO node_radio_events');
    const receivedAt = new Date(ins?.[1] as string).getTime();
    expect(Math.abs(receivedAt - Date.now())).toBeLessThan(60_000);
  });

  it('never throws when the DB fails, rolls back, and keeps processing the batch', async () => {
    clientQuery.mockImplementation(async (sql: string) => {
      if (sql === 'ROLLBACK' || sql === 'BEGIN') return { rows: [] };
      throw new Error('boom');
    });
    const accepted = await recordActivityEvents('node-aaaa', 'stream-1234', [
      { ...baseEvent, id: 1 },
      { ...baseEvent, id: 2 },
    ]);
    expect(accepted).toBe(0);
    // Both events attempted (per-event tx), both rolled back.
    expect(clientQuery.mock.calls.filter((a) => a[0] === 'ROLLBACK').length).toBe(2);
    expect(clientRelease).toHaveBeenCalled();
  });
});

describe('markRecorded', () => {
  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
  });

  const at = new Date();

  it('stamps the closest unrecorded row and folds bytes into the hourly bucket', async () => {
    armQueries({
      updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 12345 },
    });
    await markRecorded('node-aaaa', 12345, at, 90_000);

    const upd = callWith('UPDATE node_radio_events SET recorded = true');
    // [nodeId, talkgroup, at, audioBytes]
    expect(upd?.[0]).toBe('node-aaaa');
    expect(upd?.[1]).toBe(12345);
    expect(upd?.[3]).toBe(90_000);

    const hourly = callWith('INSERT INTO node_radio_hourly ');
    // [hour source ts, nodeId, system, talkgroup, bytes]
    expect(hourly?.[1]).toBe('node-aaaa');
    expect(hourly?.[2]).toBe(0x4a2);
    expect(hourly?.[3]).toBe(12345);
    expect(hourly?.[4]).toBe(90_000);
    expect(clientQuery.mock.calls.some((a) => a[0] === 'COMMIT')).toBe(true);
  });

  // The window has to cover the PHYSICAL gap between the call-grant event
  // (which stamps the row) and the recorder's first audio sample (which stamps
  // the upload) — measured at ~1.7-5s live, with a tail. A 2s window matched
  // almost nothing. Being generous is only safe because unit/frequency now
  // pin the call, so this guards the pair together.
  it('uses a window wide enough for the real grant→audio skew', async () => {
    armQueries({ updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 12345 } });
    await markRecorded('node-aaaa', 12345, at, 1);

    const sql = clientQuery.mock.calls
      .map((a) => String(a[0]))
      .find((s) => s.includes('UPDATE node_radio_events SET recorded = true'));
    expect(sql).toBeDefined();
    const windows = [...String(sql).matchAll(/interval '(\d+) seconds'/g)].map((m) => Number(m[1]));
    expect(windows.length).toBe(2); // lower + upper bound
    for (const w of windows) expect(w).toBeGreaterThanOrEqual(6);
  });

  it('binds source unit and frequency, and prefers them over time proximity', async () => {
    armQueries({ updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 12345 } });
    await markRecorded('node-aaaa', 12345, at, 90_000, 2019985, 851_062_500);

    const upd = callWith('UPDATE node_radio_events SET recorded = true');
    // [nodeId, talkgroup, at, audioBytes, frequency, sourceUnit]
    expect(upd?.[4]).toBe(851_062_500);
    expect(upd?.[5]).toBe(2019985);
    const sql = clientQuery.mock.calls
      .map((a) => String(a[0]))
      .find((s) => s.includes('UPDATE node_radio_events SET recorded = true'));
    // Exact-match ranking comes BEFORE the time-distance tiebreak, or a
    // neighbouring call that happens to be closer in time would still win.
    const order = String(sql).indexOf('ORDER BY');
    expect(String(sql).indexOf('frequency = $5', order)).toBeGreaterThan(-1);
    expect(String(sql).indexOf('source_unit = $6', order)).toBeGreaterThan(-1);
    expect(String(sql).indexOf('abs(extract(epoch', order))
      .toBeGreaterThan(String(sql).indexOf('source_unit = $6', order));
  });

  it('degrades to time-only matching when the upload carries neither', async () => {
    // An older agent sends no source/frequency. The predicates must be
    // tolerant of NULL on either side or every such upload would miss.
    armQueries({ updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 12345 } });
    await markRecorded('node-aaaa', 12345, at, 1);
    const upd = callWith('UPDATE node_radio_events SET recorded = true');
    expect(upd?.[4]).toBeNull();
    expect(upd?.[5]).toBeNull();
    const sql = String(clientQuery.mock.calls
      .map((a) => String(a[0]))
      .find((s) => s.includes('UPDATE node_radio_events SET recorded = true')));
    expect(sql).toContain('$5::bigint IS NULL');
    expect(sql).toContain('$6::integer IS NULL');
  });

  it('is a clean no-op when no row matches', async () => {
    armQueries({ updatedRow: null });
    await markRecorded('node-aaaa', 12345, at, 90_000);
    expect(countCalls('UPDATE node_radio_events SET recorded = true')).toBe(1);
    expect(countCalls('INSERT INTO node_radio_hourly ')).toBe(0);
    expect(clientQuery.mock.calls.some((a) => a[0] === 'COMMIT')).toBe(true);
  });

  it('never throws when the DB fails', async () => {
    clientQuery.mockImplementation(async (sql: string) => {
      if (sql === 'ROLLBACK' || sql === 'BEGIN') return { rows: [] };
      throw new Error('boom');
    });
    await expect(markRecorded('node-aaaa', 12345, at, 1)).resolves.toBeUndefined();
    expect(clientQuery.mock.calls.some((a) => a[0] === 'ROLLBACK')).toBe(true);
    expect(clientRelease).toHaveBeenCalled();
  });
});

describe('recordPagerEvent grouping', () => {
  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
  });

  const basePager = {
    nodeId: 'node-bbbb',
    receivedAt: new Date(),
    capcode: '123456',
    function: 3,
    freqMhz: 148.825,
    message: '  MFS: RESPOND structure fire  ',
  };

  it('hashes the trimmed message and starts a new group', async () => {
    armQueries({ foundPager: null, insertIds: ['201'] });
    await recordPagerEvent({ ...basePager });

    const expectedHash = createHash('sha256')
      .update('MFS: RESPOND structure fire')
      .digest('hex');
    const find = callWith('SELECT logical_id FROM node_pager_events');
    expect(find?.[0]).toBe('123456');
    expect(find?.[1]).toBe(expectedHash);

    expect(callWith('SET logical_id')).toEqual(['201', '201']);
    const hourly = callWith('node_pager_hourly');
    // [receivedAt, nodeId, capcode, logicalIncrement]
    expect(hourly?.[2]).toBe('123456');
    expect(hourly?.[3]).toBe(1);
  });

  it('joins an existing group without a logical_pages increment', async () => {
    armQueries({ foundPager: '77', insertIds: ['202'] });
    await recordPagerEvent({ ...basePager });
    expect(callWith('SET logical_id')).toEqual(['77', '202']);
    expect(callWith('node_pager_hourly')?.[3]).toBe(0);
  });
});

describe('safeInt', () => {
  it('parses ints and rejects garbage', () => {
    expect(safeInt('42')).toBe(42);
    expect(safeInt(7.9)).toBe(7);
    expect(safeInt('')).toBeNull();
    expect(safeInt('abc')).toBeNull();
    expect(safeInt(undefined)).toBeNull();
    expect(safeInt(null)).toBeNull();
    expect(safeInt(Infinity)).toBeNull();
  });
});
