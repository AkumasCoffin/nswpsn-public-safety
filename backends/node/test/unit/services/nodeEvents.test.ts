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

/** Operator-declared patches. Empty by default — the state every existing test
 *  here already assumed, since an unconfigured central rdio yields no patches
 *  and every talkgroup then groups as itself. `groupingTalkgroup` stays real:
 *  it is a pure function and mocking it would test the mock. */
const patchLookup: { byTalkgroup: Map<number, { talkgroups: number[] }>; all: unknown[] } = {
  byTalkgroup: new Map(),
  all: [],
};
vi.mock('../../../src/services/rdioPatches.js', async (importOriginal) => {
  const actual = (await importOriginal()) as Record<string, unknown>;
  return { ...actual, rdioPatches: vi.fn(async () => patchLookup) };
});

import {
  recordActivityEvents,
  markRecorded,
  recordPagerEvent,
  recordScannerCall,
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

    // Group find matched on system = systemId, talkgroup = target. The
    // talkgroup is passed as a LIST because a patch member also matches its
    // sibling channels; an unpatched talkgroup is simply a list of one.
    const find = callWith('SELECT logical_call_id FROM node_radio_events');
    expect(find?.[0]).toBe(0x4a2);
    expect(find?.[1]).toEqual([12345]);
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

describe('recordActivityEvents claiming a parked upload', () => {
  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
  });

  const ev = (over: Partial<ActivityEventInput> = {}): ActivityEventInput => ({
    id: 1,
    atMs: Date.now(),
    action: 'CALL',
    eventType: 'CALL_GROUP',
    source: 2019985,
    target: 12345,
    frequencyHz: 851_062_500,
    timeslot: null,
    encrypted: false,
    rfss: 4,
    site: 85,
    nac: null,
    wacn: null,
    systemId: 0x4a2,
    channelName: null,
    ...over,
  });

  /** Like armQueries but lets the pending-claim DELETE return a parked row. */
  function armWithPending(pendingBytes: number | null) {
    clientQuery.mockImplementation(async (sql: string) => {
      if (sql.includes('SELECT logical_call_id FROM node_radio_events')) return { rows: [] };
      if (sql.includes('DELETE FROM node_pending_recordings')) {
        return { rows: pendingBytes === null ? [] : [{ audio_bytes: String(pendingBytes) }] };
      }
      if (sql.includes('INSERT INTO node_radio_events')) return { rows: [{ id: '101' }] };
      return { rows: [] };
    });
  }

  it('flags the new row and folds the bytes in when an upload was waiting', async () => {
    armWithPending(90_000);
    const n = await recordActivityEvents('node-aaaa', 's1', [ev()]);
    expect(n).toBe(1);

    const flag = callWith('SET recorded = true');
    expect(flag?.[0]).toBe(90_000);
    expect(flag?.[1]).toBe('101');
    // The pending row is consumed, so nothing will add these bytes later —
    // the hourly bucket must count them HERE or they are lost.
    const hourly = callWith('INSERT INTO node_radio_hourly ');
    expect(hourly?.[4]).toBe(90_000);
  });

  it('leaves the row unflagged and the bucket at zero bytes when none was waiting', async () => {
    armWithPending(null);
    await recordActivityEvents('node-aaaa', 's1', [ev()]);
    expect(countCalls('SET recorded = true')).toBe(0);
    const hourly = callWith('INSERT INTO node_radio_hourly ');
    expect(hourly?.[4]).toBe(0);
  });

  it('claims within a transaction, so a rollback returns the upload to the queue', async () => {
    armWithPending(1);
    await recordActivityEvents('node-aaaa', 's1', [ev()]);
    const sqls = clientQuery.mock.calls.map((a) => String(a[0]));
    const begin = sqls.indexOf('BEGIN');
    const del = sqls.findIndex((s) => s.includes('DELETE FROM node_pending_recordings'));
    const commit = sqls.indexOf('COMMIT');
    expect(begin).toBeGreaterThanOrEqual(0);
    expect(del).toBeGreaterThan(begin);
    expect(commit).toBeGreaterThan(del);
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

  // Every radio showed as a bare id because the OTA alias was being thrown
  // away. It rides on the audio upload next to `source` and `talkgroup` — the
  // only path it reaches us on, since the activity feed's own sourceAlias
  // arrives null — and this is the point where that form is read.
  it('records the radio\'s over-the-air alias from the upload', async () => {
    armQueries({ updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 12345 } });
    await markRecorded('node-aaaa', 12345, at, 90_000, 2019977, 851062500, '  FIRE ENG 12  ');

    const upd = callWith('UPDATE node_radio_events SET recorded = true');
    expect(upd?.[6]).toBe('FIRE ENG 12'); // trimmed
    expect(String(clientQuery.mock.calls.find(
      (a) => String(a[0]).includes('UPDATE node_radio_events SET recorded = true'))?.[0]))
      .toContain('COALESCE(source_alias');  // never overwrite an alias we know
  });

  it('treats a blank alias as no alias', async () => {
    // Most radios never transmit one; an empty form field must not write ''
    // over a name captured from an earlier call.
    armQueries({ updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 12345 } });
    await markRecorded('node-aaaa', 12345, at, 90_000, 2019977, 851062500, '   ');
    expect(callWith('UPDATE node_radio_events SET recorded = true')?.[6]).toBeNull();
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
    // `frequency = $5` yields NULL when the ROW's frequency is null, and DESC
    // sorts NULLS FIRST in Postgres — an unguarded expression would rank a row
    // that knows nothing ABOVE the exact match. Both keys must be coalesced.
    const orderClause = String(sql).slice(order);
    expect(orderClause).toContain('COALESCE($5::bigint IS NOT NULL AND frequency = $5::bigint, false) DESC');
    expect(orderClause).toContain('COALESCE($6::integer IS NOT NULL AND source_unit = $6::integer, false) DESC');
  });

  it('degrades to time-only matching when the upload carries neither', async () => {
    // An older agent sends no source/frequency; those uploads must still match.
    armQueries({ updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 12345 } });
    await markRecorded('node-aaaa', 12345, at, 1);
    const upd = callWith('UPDATE node_radio_events SET recorded = true');
    expect(upd?.[4]).toBeNull();
    expect(upd?.[5]).toBeNull();
  });

  it('accepts a far-in-time row when unit AND frequency both identify the call', async () => {
    // The two sides do not share a clock: the upload is stamped with the real
    // call start (getStartTime()), the event with observed_at_ms — when the
    // activity logger wrote the row. There is no call-start column to align
    // to, so identity has to carry matches the clock would reject.
    armQueries({ updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 12345 } });
    await markRecorded('node-aaaa', 12345, at, 1, 2019985, 851_062_500);
    const sql = String(clientQuery.mock.calls
      .map((a) => String(a[0]))
      .find((s) => s.includes('UPDATE node_radio_events SET recorded = true')));
    const where = sql.slice(sql.indexOf('WHERE'), sql.indexOf('ORDER BY'));
    // Time-only acceptance stays, OR'd with a full identity match.
    expect(where).toContain('frequency = $5::bigint');
    expect(where).toContain('source_unit = $6::integer');
    // The identity escape requires BOTH — either alone is not enough to
    // override the clock (traffic channels are reused; units repeat).
    expect(where).toMatch(/frequency = \$5::bigint\s*\n?\s*AND \$6::integer IS NOT NULL AND source_unit = \$6::integer/);
    // …and it is still bounded, or a call from an hour ago could match.
    const bounds = [...sql.matchAll(/interval '(\d+) seconds'/g)].map((m) => Number(m[1]));
    for (const b of bounds) expect(b).toBeLessThanOrEqual(300);
  });

  it('frequency and unit only ever WIDEN the candidate set, never narrow it', async () => {
    // As narrowing predicates these cost more matches than they saved (measured
    // 76 misses to 71 hits): one call heard at several sites has a different
    // traffic frequency per site, while the upload carries only the recording
    // site's, so an AND discards the rows it should be choosing between. They
    // may appear in WHERE solely inside the OR that admits a far-in-time
    // identity match, and otherwise only rank.
    armQueries({ updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 12345 } });
    await markRecorded('node-aaaa', 12345, at, 1, 2019985, 851_062_500);
    const sql = String(clientQuery.mock.calls
      .map((a) => String(a[0]))
      .find((s) => s.includes('UPDATE node_radio_events SET recorded = true')));
    const where = sql.slice(sql.indexOf('WHERE'), sql.indexOf('ORDER BY'));
    // Every mention of either column sits after the OR that widens.
    const orIdx = where.indexOf('OR ($5::bigint IS NOT NULL');
    expect(orIdx).toBeGreaterThan(-1);
    expect(where.indexOf('frequency')).toBeGreaterThan(orIdx);
    expect(where.indexOf('source_unit')).toBeGreaterThan(orIdx);
    // …and both still rank.
    expect(sql.slice(sql.indexOf('ORDER BY'))).toContain('frequency = $5');
    expect(sql.slice(sql.indexOf('ORDER BY'))).toContain('source_unit = $6');
  });

  it('parks the upload when no event row exists yet', async () => {
    // The feeds race: audio closes in ~1-2s on a short over, activity events
    // ship on a 3-5s tick. This used to be a one-shot give-up, permanently
    // losing the flag for exactly the calls quickest to upload — which is why
    // busy talkgroups scored worst (30017 at 67% vs 30003 at 95%).
    armQueries({ updatedRow: null });
    await markRecorded('node-aaaa', 12345, at, 90_000, 2019985, 851_062_500);

    const parked = callWith('INSERT INTO node_pending_recordings');
    expect(parked).toBeDefined();
    // [nodeId, talkgroup, sourceUnit, frequency, startedAt, bytes] — the
    // identity has to be parked too, or the event cannot tell which call it is.
    expect(parked?.[0]).toBe('node-aaaa');
    expect(parked?.[1]).toBe(12345);
    expect(parked?.[2]).toBe(2019985);
    expect(parked?.[3]).toBe(851_062_500);
    expect(parked?.[5]).toBe(90_000);
    expect(clientQuery.mock.calls.some((a) => a[0] === 'COMMIT')).toBe(true);
  });

  it('does NOT park when a row was flagged', async () => {
    armQueries({ updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 12345 } });
    await markRecorded('node-aaaa', 12345, at, 90_000);
    expect(countCalls('INSERT INTO node_pending_recordings')).toBe(0);
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

describe('isRealTalkerAlias', () => {
  it('rejects an alias that is only the radio id echoed back', async () => {
    const { isRealTalkerAlias } = await import('../../../src/services/nodeEvents.js');
    expect(isRealTalkerAlias('2072676', 2072676)).toBe(false);
    expect(isRealTalkerAlias('02072676', 2072676)).toBe(false); // zero-padded echo
    expect(isRealTalkerAlias('  2072676  ', 2072676)).toBe(false);
    expect(isRealTalkerAlias('', 2072676)).toBe(false);
  });

  it('keeps a real name, including a numeric one that is not the id', async () => {
    const { isRealTalkerAlias } = await import('../../../src/services/nodeEvents.js');
    expect(isRealTalkerAlias('NEWRAD03', 2000103)).toBe(true);
    expect(isRealTalkerAlias('260', 2073548)).toBe(true);
    expect(isRealTalkerAlias('P359', 2073252)).toBe(true);
    // No unit id to compare against — keep whatever was transmitted.
    expect(isRealTalkerAlias('anything', null)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// recordScannerCall — patch grouping.
//
// A patch is several talkgroups carrying ONE conversation. A scanner reports
// it on whichever member it happened to be scanning, which is frequently not
// the member a node reported it on, so without patch awareness the two
// recordings of one conversation open rival logical calls — and the feed
// inflates the very call count it exists to complete.
// ---------------------------------------------------------------------------
describe('recordScannerCall patch grouping', () => {
  const call = {
    nodeId: 'scanner-abc',
    receivedAt: new Date('2026-08-23T04:26:12.000Z'),
    talkgroup: 30003,
    sourceUnit: 2319851,
    frequency: 419362500,
    talkerAlias: null,
    audioBytes: 4096,
  };

  beforeEach(() => {
    // callWith reads clientQuery.mock.calls, which accumulate across tests
    // unless reset — without this every assertion here reads the FIRST test's
    // queries and the later cases silently verify nothing.
    clientQuery.mockReset();
    clientRelease.mockReset();
    patchLookup.byTalkgroup = new Map();
    patchLookup.all = [];
  });

  it('searches EVERY member of the patch, so a sibling channel joins', async () => {
    // Illawarra A Patch, ranked. 30003 is a member but not the highest.
    const members = [10079, 30003, 20004, 30013, 10075];
    patchLookup.byTalkgroup = new Map(members.map((t) => [t, { talkgroups: members }]));
    armQueries({ foundRadio: null, insertIds: ['501'] });

    await recordScannerCall(call);

    const params = callWith('SELECT logical_call_id FROM node_radio_events');
    expect(params?.[1]).toEqual(members);
  });

  it('locks on the patch home, not the reception talkgroup', async () => {
    // Two members taking different locks would both find no group and fork.
    const members = [10079, 30003, 20004, 30013, 10075];
    patchLookup.byTalkgroup = new Map(members.map((t) => [t, { talkgroups: members }]));
    armQueries({ foundRadio: null, insertIds: ['502'] });

    await recordScannerCall(call);

    const lock = callWith('pg_advisory_xact_lock');
    expect(lock?.[0]).toBe('nrc:-1:10079');   // highest-ranked member, not 30003
  });

  it('joins the logical call a node already opened on a sibling talkgroup', async () => {
    const members = [10079, 30003, 20004, 30013, 10075];
    patchLookup.byTalkgroup = new Map(members.map((t) => [t, { talkgroups: members }]));
    armQueries({ foundRadio: '900', insertIds: ['503'] });

    await recordScannerCall(call);

    // The new row is stamped with the EXISTING group, not its own id.
    const upd = callWith('SET logical_call_id');
    expect(upd?.[0]).toBe('900');
    expect(upd?.[1]).toBe('503');
  });

  it('an unpatched talkgroup is a list of one and locks on itself', async () => {
    armQueries({ foundRadio: null, insertIds: ['504'] });

    await recordScannerCall({ ...call, talkgroup: 20458 });

    expect(callWith('SELECT logical_call_id FROM node_radio_events')?.[1]).toEqual([20458]);
    expect(callWith('pg_advisory_xact_lock')?.[0]).toBe('nrc:-1:20458');
  });
});
