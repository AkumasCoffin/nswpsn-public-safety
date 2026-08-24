/**
 * services/nodeEvents.ts — activity-event capture + logical grouping unit
 * tests (mocked writer pool; verifies the SQL sequencing + parameters, not a
 * live Postgres).
 *
 * Covers (migration 044 model — radio rows come from vce activity events):
 *   - recordActivityEvents: new group / joined group logical_calls 1/0
 *   - dedupe: ON CONFLICT DO NOTHING re-send → accepted 0, no bucket bumps
 *   - grouping across two nodes' events with the same systemId+target ±5s
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

/** The READ pool. Null by default — every existing test here predates
 *  resolveScannerSystem needing one, and a null pool simply leaves a scanner
 *  call with no system attributed. The scanner-identity tests below set it. */
const poolQuery = vi.fn(async () => ({ rows: [] as unknown[] }));
let readPool: unknown = null;

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => readPool),
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
  patchMembersOf,
  recordActivityEvents,
  markRecorded,
  recordPagerEvent,
  recordScannerCall,
  mergeAutomaticPatch,
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
    if (sql.includes('SELECT c.logical_call_id')) {
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
    expect(accepted).toEqual({ accepted: 1, failed: 0 });

    // Advisory lock key uses systemId + target with -1 fallbacks.
    expect(callWith('pg_advisory_xact_lock')).toEqual([`nrc:${0x4a2}:${12345}`]);

    // Group find matched on system = systemId, talkgroup = target. The
    // talkgroup is passed as a LIST because a patch member also matches its
    // sibling channels; an unpatched talkgroup is simply a list of one.
    const find = callWith('SELECT c.logical_call_id');
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
    // Normalised on the way in. vce sends Java enum names, uppercase by
    // convention rather than contract, and the read path matches them
    // directly rather than wrapping every row in upper().
    expect(ins?.[4]).toBe('CALL');
    expect(ins?.[5]).toBe('GROUP_VOICE_CHANNEL_GRANT');
    expect(ins?.[6]).toBe(0x4a2);
    expect(ins?.[7]).toBe(12345);
    expect(ins?.[11]).toBe(false);
    expect(ins?.[15]).toBe(0xbee00);
    // New columns [16]=system_label (systemName), [17]=source_alias — null
    // when the event carries neither (older agents / unresolved joins).
    expect(ins?.[16]).toBeNull();
    expect(ins?.[17]).toBeNull();

    // The hourly rollups are DERIVED now (services/nodeHourlyRollup.ts), so
    // ingest must not write them: a per-event counter cannot survive
    // mergeAutomaticPatch folding calls together after the fact.
    expect(countCalls('node_radio_hourly')).toBe(0);

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

  it("groups two nodes' events with the same systemId+target within ±5s", async () => {
    // Node A starts the group…
    armQueries({ foundRadio: null, insertIds: ['101'] });
    const a = await recordActivityEvents('node-aaaa', 'stream-aaaa', [{ ...baseEvent }]);
    expect(a).toEqual({ accepted: 1, failed: 0 });
    expect(callWith('SET logical_call_id')).toEqual(['101', '101']);

    // …node B's reception 2s later finds it and JOINS (no logical increment).
    clientQuery.mockReset();
    clientRelease.mockReset();
    armQueries({ foundRadio: '101', insertIds: ['102'] });
    const b = await recordActivityEvents('node-bbbb', 'stream-bbbb', [
      { ...baseEvent, id: 4, atMs: baseEvent.atMs + 2_000, rfss: 1, site: 12 },
    ]);
    expect(b).toEqual({ accepted: 1, failed: 0 });
    expect(callWith('SET logical_call_id')).toEqual(['101', '102']);
    // Whether this reception started or joined a group is no longer recorded
    // at ingest time — the rollup derives it from logical_call_id later.
    expect(countCalls('node_radio_hourly')).toBe(0);
  });

  it('dedupes a re-sent event: accepted 0, no stamps', async () => {
    armQueries({ foundRadio: null, insertIds: [null] }); // ON CONFLICT → no row
    const accepted = await recordActivityEvents('node-aaaa', 'stream-1234', [{ ...baseEvent }]);
    expect(accepted).toEqual({ accepted: 0, failed: 0 });
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
    expect(accepted).toEqual({ accepted: 2, failed: 0 });
    expect(countCalls('SET logical_call_id')).toBe(2);
  });

  it('encodes unknown systemId/target as -1 in the lock key', async () => {
    armQueries({ foundRadio: null, insertIds: ['101'] });
    await recordActivityEvents('node-aaaa', 'stream-1234', [
      { ...baseEvent, systemId: null, target: null, rfss: null, site: null },
    ]);
    expect(callWith('pg_advisory_xact_lock')).toEqual(['nrc:-1:-1']);
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
    // Every event failed, and the count is what the route needs to refuse
    // the ack: acking a batch the agent then discards is silent data loss.
    expect(accepted).toEqual({ accepted: 0, failed: 2 });
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
      if (sql.includes('SELECT c.logical_call_id')) return { rows: [] };
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
    expect(n).toEqual({ accepted: 1, failed: 0 });

    const flag = callWith('SET recorded = true');
    expect(flag?.[0]).toBe(90_000);
    expect(flag?.[1]).toBe('101');
    // The bytes live on the detail row; the rollup sums them from there, so
    // nothing needs to fold them into a bucket at claim time.
    expect(countCalls('INSERT INTO node_radio_hourly ')).toBe(0);
  });

  it('leaves the row unflagged when no upload was waiting', async () => {
    armWithPending(null);
    await recordActivityEvents('node-aaaa', 's1', [ev()]);
    expect(countCalls('SET recorded = true')).toBe(0);
    expect(countCalls('INSERT INTO node_radio_hourly ')).toBe(0);
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
    // [nodeId, talkgroupCandidates, at, audioBytes]
    expect(upd?.[0]).toBe('node-aaaa');
    // An unpatched talkgroup resolves to exactly itself — the widening below
    // must not change ordinary traffic.
    expect(upd?.[1]).toEqual([12345]);
    expect(upd?.[3]).toBe(90_000);

    // The bytes stay on the detail row; the hourly rollup sums them from
    // there, so markRecorded no longer folds them into a bucket.
    expect(countCalls('INSERT INTO node_radio_hourly ')).toBe(0);
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

  it('lets a patched upload claim a reception announced on another member', async () => {
    // rdio files a patched call under the highest-ranked member that received
    // a copy (Patch.homeRank); our reception rows keep whichever talkgroup the
    // site announced it on. Those disagree constantly — measured over 24h, rdio
    // held 202 calls on TG 20201 against 7 we had flagged, and 1,240 uploads
    // network-wide matched no row at all. The upload may claim any member.
    patchLookup.byTalkgroup.set(20201, { talkgroups: [20201, 10250, 20202] });
    try {
      armQueries({ updatedRow: { received_at: at.toISOString(), system: 0x4a2, talkgroup: 10250 } });
      await markRecorded('node-aaaa', 20201, at, 90_000);

      const upd = callWith('UPDATE node_radio_events SET recorded = true');
      expect(upd?.[1]).toEqual([20201, 10250, 20202]);
      const sql = clientQuery.mock.calls
        .map((a) => String(a[0]))
        .find((x) => x.includes('UPDATE node_radio_events SET recorded = true'));
      // Set membership, not equality — and still bounded to the patch, never
      // widened to the whole system.
      expect(String(sql)).toContain('talkgroup = ANY($2::int[])');
      expect(String(sql)).not.toContain('talkgroup IS NOT DISTINCT FROM $2');
    } finally {
      patchLookup.byTalkgroup.clear();
    }
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
// recordScannerCall — the P25 identity a scanner call is filed under.
//
// Every rollup on the Data page groups by (wacn, system). A scanner row that
// carried the right system number but a null wacn was therefore a DIFFERENT
// group to a node row on the same system, and one P25 network rendered as two:
// "NSWPSN 721" plus a nameless 721 beside it, with 53 of 402 talkgroups
// doubled the same way.
//
// Dynamic import per test: resolveScannerSystem caches its answer for five
// minutes, so a fresh module is the only way to observe a different one.
// ---------------------------------------------------------------------------
describe('recordScannerCall system identity', () => {
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
    clientQuery.mockReset();
    clientRelease.mockReset();
    poolQuery.mockReset();
    readPool = null;
    vi.resetModules();
  });

  it('files under the same (wacn, system, label) the nodes are on', async () => {
    poolQuery.mockResolvedValue({ rows: [{ system: 721, wacn: 781824, label: 'NSWPSN' }] });
    readPool = { query: poolQuery };
    armQueries({ foundRadio: null, insertIds: ['501'] });

    const mod = await import('../../../src/services/nodeEvents.js');
    await mod.recordScannerCall(call);

    const ins = callWith('INSERT INTO node_radio_events');
    // [nodeId, receivedAt, sourceEventId, system, wacn, systemLabel,
    //  talkgroup, sourceUnit, frequency, audioBytes, alias]
    expect(ins?.[3]).toBe(721);
    expect(ins?.[4]).toBe(781824);
    expect(ins?.[5]).toBe('NSWPSN');
  });

  it('resolves from NODE rows only, never from scanner rows', async () => {
    poolQuery.mockResolvedValue({ rows: [{ system: 721, wacn: 781824, label: 'NSWPSN' }] });
    readPool = { query: poolQuery };
    armQueries({ foundRadio: null, insertIds: ['502'] });

    const mod = await import('../../../src/services/nodeEvents.js');
    await mod.recordScannerCall(call);

    // Letting a scanner row teach the resolver what a system looks like would
    // let the first wrong answer keep re-electing itself.
    const sql = String(poolQuery.mock.calls[0]?.[0] ?? '');
    expect(sql).toContain("stream_id <> 'scanner'");
    expect(sql).toContain('GROUP BY wacn, system');
  });

  it('stores the call with nulls when no node has been heard from yet', async () => {
    readPool = null;
    armQueries({ foundRadio: null, insertIds: ['503'] });

    const mod = await import('../../../src/services/nodeEvents.js');
    await mod.recordScannerCall(call);

    const ins = callWith('INSERT INTO node_radio_events');
    expect(ins?.[3]).toBeNull();
    expect(ins?.[4]).toBeNull();
    expect(ins?.[5]).toBeNull();
    // The reception is still recorded — an unattributed call beats no call.
    expect(ins?.[6]).toBe(30003);
  });
});

// ---------------------------------------------------------------------------
// patchMembersOf — the talkgroups an over-the-air patch put on a call.
//
// A patched transmission carries the PATCH GROUP as its target, so the member
// talkgroups are the only record of what the conversation actually went out on.
// They come from vce (activity_event_talkgroup_member) because a node hears the
// patch on its supergroup alone.
// ---------------------------------------------------------------------------
describe('patchMembersOf', () => {
  it('keeps the reported members, sorted and deduped', () => {
    expect(patchMembersOf([10125, 10120, 10125], 10128)).toEqual([10120, 10125]);
  });

  it('never stores the call own talkgroup as one of its patch members', () => {
    // The call is already filed there; listing it would read as the
    // transmission going out on that channel twice.
    expect(patchMembersOf([10128, 10120], 10128)).toEqual([10120]);
  });

  it('distinguishes "not reported" from "none"', () => {
    // Null is an older control server saying nothing, which the column keeps
    // as null. An empty list is a call that simply is not patched — a patch of
    // nothing is not a patch, so it stores as null too rather than claiming
    // the question was answered with an empty array.
    expect(patchMembersOf(null, 10128)).toBeNull();
    expect(patchMembersOf(undefined, 10128)).toBeNull();
    expect(patchMembersOf([], 10128)).toBeNull();
    expect(patchMembersOf([10128], 10128)).toBeNull();
  });

  it('drops junk without losing the rest', () => {
    expect(patchMembersOf([0, -5, 10120, Number.NaN], 10128)).toEqual([10120]);
  });

  it('keeps every member when the talkgroup is unknown', () => {
    expect(patchMembersOf([10128, 10120], null)).toEqual([10120, 10128]);
  });
});

describe('recordActivityEvents patch members', () => {
  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
  });

  it('stores what vce reported, minus the supergroup itself', async () => {
    armQueries({ foundRadio: null, insertIds: ['101'] });
    await recordActivityEvents('node-aaaa', 'stream-1', [
      { ...baseEvent, target: 10128, eventType: 'CALL_PATCH_GROUP', patchMembers: [10128, 10120, 10125] },
    ]);

    const ins = callWith('INSERT INTO node_radio_events');
    expect(ins?.[18]).toEqual([10120, 10125]);
  });

  it('leaves the column null for an ordinary call', async () => {
    armQueries({ foundRadio: null, insertIds: ['102'] });
    await recordActivityEvents('node-aaaa', 'stream-1', [baseEvent]);

    expect(callWith('INSERT INTO node_radio_events')?.[18]).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Logical grouping: the two rules that decide whether a reception JOINS an
// existing call or starts a new one.
//
// Both exist because of measured damage in production, and both live in the
// one SQL builder shared by the activity and scanner paths.
// ---------------------------------------------------------------------------
describe('logical grouping rules', () => {
  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
  });

  function lookupSql(): string {
    return String(
      clientQuery.mock.calls.find(
        (a) => typeof a[0] === 'string' && (a[0] as string).includes('SELECT c.logical_call_id'),
      )?.[0] ?? '',
    );
  }

  it('anchors the window at the group FIRST row, so groups cannot chain', async () => {
    armQueries({ foundRadio: null, insertIds: ['101'] });
    await recordActivityEvents('node-aaaa', 'stream-1', [baseEvent]);

    const sql = lookupSql();
    // logical_call_id IS the id of the group's first row, so the anchor is a
    // join — and the anchor, not merely some member, must be inside the window.
    expect(sql).toContain('JOIN node_radio_events anchor ON anchor.id = c.logical_call_id');
    expect(sql).toContain('anchor.received_at BETWEEN');
  });

  it('refuses a group already holding THIS site on another frequency', async () => {
    armQueries({ foundRadio: null, insertIds: ['101'] });
    await recordActivityEvents('node-aaaa', 'stream-1', [baseEvent]);

    const sql = lookupSql();
    // One site grants one traffic channel per transmission, so a group already
    // carrying this site on a different frequency is a different transmission.
    expect(sql).toContain('x.site_rfss = $5::integer');
    expect(sql).toContain('x.site_id = $6::integer');
    expect(sql).toContain('x.frequency <> $7::bigint');

    // Site and frequency reach the lookup as $5/$6/$7.
    const find = callWith('SELECT c.logical_call_id');
    expect(find?.[4]).toBe(1);
    expect(find?.[5]).toBe(12);
    expect(find?.[6]).toBe(420_662_500);
  });

  it('never compares frequencies across sites — a simulcast call stays whole', async () => {
    armQueries({ foundRadio: null, insertIds: ['101'] });
    await recordActivityEvents('node-aaaa', 'stream-1', [baseEvent]);

    // The frequency test is reached ONLY through an equal (rfss, site). Two
    // sites carrying one transmission use two different traffic channels, so an
    // unscoped comparison would tear every multi-site call in half.
    const sql = lookupSql();
    const guard = sql.slice(sql.indexOf('NOT EXISTS'));
    expect(guard.indexOf('x.site_rfss = $5::integer')).toBeLessThan(
      guard.indexOf('x.frequency <> $7::bigint'),
    );
  });

  it('a scanner sends no site, which switches the frequency split off', async () => {
    armQueries({ foundRadio: null, insertIds: ['501'] });
    await recordScannerCall({
      nodeId: 'scanner-abc',
      receivedAt: new Date('2026-08-23T04:26:12.000Z'),
      talkgroup: 30003,
      sourceUnit: 2319851,
      frequency: 419362500,
      talkerAlias: null,
      audioBytes: 4096,
    });

    // A scanner has no control-channel view, so it cannot say WHICH site it
    // heard. Null site means the NOT EXISTS never fires — the honest answer.
    const find = callWith('SELECT c.logical_call_id');
    expect(find?.[4]).toBeNull();
    expect(find?.[5]).toBeNull();
    expect(find?.[6]).toBe(419362500);
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

    const params = callWith('SELECT c.logical_call_id');
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

    expect(callWith('SELECT c.logical_call_id')?.[1]).toEqual([20458]);
    expect(callWith('pg_advisory_xact_lock')?.[0]).toBe('nrc:-1:20458');
  });
});

// ---------------------------------------------------------------------------
// mergeAutomaticPatch — the correction step for patches nobody configured.
//
// An automatic patch is detected over the air PER TRANSMISSION and reaches us
// only on the audio upload, a second or two after the activity events have
// already been grouped. So each member has usually opened its own logical call
// by then, and this folds them back into one.
// ---------------------------------------------------------------------------
describe('mergeAutomaticPatch', () => {
  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
  });

  /** Arm the DISTINCT logical_call_id lookup with the ids it should find. */
  function armMerge(ids: string[]) {
    clientQuery.mockImplementation(async (sql: string) => {
      if (sql.includes('SELECT DISTINCT logical_call_id')) {
        return { rows: ids.map((logical_call_id) => ({ logical_call_id })) };
      }
      return { rows: [], rowCount: ids.length };
    });
  }

  const at = new Date('2026-08-23T10:49:29.000Z');

  it('a patch of one is not a patch — the decoder reports these constantly', async () => {
    armMerge(['1']);
    await mergeAutomaticPatch('node-a', [30003], at);
    expect(clientQuery).not.toHaveBeenCalled();
  });

  it('ignores an empty or junk member list without touching the database', async () => {
    armMerge([]);
    await mergeAutomaticPatch('node-a', [], at);
    await mergeAutomaticPatch('node-a', [0, -5], at);
    expect(clientQuery).not.toHaveBeenCalled();
  });

  it('folds rival logical calls into the EARLIEST id', async () => {
    // Ids are compared numerically, not lexically: '9' < '10' as bigints but
    // not as strings, and picking the wrong one splits the call the other way.
    armMerge(['10', '9', '11']);
    await mergeAutomaticPatch('node-a', [30003, 30013], at);

    const upd = callWith('SET logical_call_id');
    expect(upd?.[0]).toBe('9');
    expect(upd?.[1]).toEqual(['10', '11']);
    expect(clientQuery.mock.calls.some((a) => a[0] === 'COMMIT')).toBe(true);
  });

  it('does nothing when the members already share one call', async () => {
    armMerge(['7']);
    await mergeAutomaticPatch('node-a', [30003, 30013], at);
    expect(callWith('SET logical_call_id')).toBeUndefined();
    expect(clientQuery.mock.calls.some((a) => a[0] === 'COMMIT')).toBe(true);
  });

  it('locks on the patch itself, so member ORDER cannot change the key', async () => {
    armMerge(['4', '5']);
    await mergeAutomaticPatch('node-a', [30013, 30003], at);
    expect(callWith('pg_advisory_xact_lock')?.[0]).toBe('nrc:auto:30003,30013');
  });

  it('gives two nodes reporting DIFFERENT subsets different locks', async () => {
    // The reason the key is the member set and not its minimum: keyed on the
    // lowest member these two would collide on 10120 and be treated as one
    // patch, while keyed on the set they are correctly distinct — and two
    // nodes reporting the SAME set still serialise, which is the case that
    // matters.
    armMerge(['4', '5']);
    await mergeAutomaticPatch('node-a', [10125, 10120], at);
    const a = callWith('pg_advisory_xact_lock')?.[0];
    clientQuery.mockReset();
    armMerge(['6', '7']);
    await mergeAutomaticPatch('node-b', [10120, 10130], at);
    const b = callWith('pg_advisory_xact_lock')?.[0];
    expect(a).toBe('nrc:auto:10120,10125');
    expect(b).toBe('nrc:auto:10120,10130');
    expect(a).not.toBe(b);
  });

  it('scopes the merge to one system and to voice calls', async () => {
    armMerge(['4', '5']);
    await mergeAutomaticPatch('node-a', [30013, 30003], at, null, 721);
    const sql = String(
      clientQuery.mock.calls.find(
        (c) => typeof c[0] === 'string' && (c[0] as string).includes('SELECT DISTINCT logical_call_id'),
      )?.[0] ?? '',
    );
    // Without the system, two networks sharing a talkgroup number merge;
    // without the call-group filter, signalling rows join, whose `talkgroup`
    // column holds a RADIO id.
    expect(sql).toContain('system IS NOT DISTINCT FROM $4::integer');
    expect(sql).toContain("event_type LIKE 'CALL_GROUP%'");
    expect(callWith('SELECT DISTINCT logical_call_id')?.[3]).toBe(721);
  });

  it('never throws and rolls back when the database fails', async () => {
    clientQuery.mockImplementation(async (sql: string) => {
      if (sql.includes('SELECT DISTINCT logical_call_id')) throw new Error('boom');
      return { rows: [] };
    });
    await expect(mergeAutomaticPatch('node-a', [30003, 30013], at)).resolves.toBeUndefined();
    expect(clientQuery.mock.calls.some((a) => a[0] === 'ROLLBACK')).toBe(true);
    expect(clientRelease).toHaveBeenCalled();
  });
});
