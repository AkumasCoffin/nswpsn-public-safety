/**
 * Hourly radio rollups, derived from the detail table (migration 080).
 *
 * These pin the properties the OLD per-event counters could not have: that a
 * re-run cannot double-count, that the hour in progress is never summarised,
 * and that the pruner cannot delete detail the rollup has not read yet — the
 * detail table being the only thing the rollups can be derived from.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const clientQuery = vi.fn();
const clientRelease = vi.fn();
const poolQuery = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => ({ query: poolQuery })),
  getWriterPool: vi.fn(async () => ({
    query: poolQuery,
    connect: async () => ({ query: clientQuery, release: clientRelease }),
  })),
  closePool: vi.fn(async () => undefined),
}));
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

import { rollupNodeHourlyOnce, rollupHighWater } from '../../../src/services/nodeHourlyRollup.js';

/** SQL text of every statement the transaction ran. */
const sqls = (): string[] => clientQuery.mock.calls.map((c) => String(c[0]));
const ran = (fragment: string): string[] => sqls().filter((s) => s.includes(fragment));
/** Bind params of the nth statement matching a fragment. */
const paramsOf = (fragment: string, nth = 0): unknown[] =>
  (clientQuery.mock.calls.filter((c) => String(c[0]).includes(fragment))[nth]?.[1] ??
    []) as unknown[];

/** State the cursor, then answer everything else emptily. */
function armCursor(rolledUpTo: string | null) {
  poolQuery.mockImplementation(async () => ({ rows: [{ rolled_up_to: rolledUpTo ? new Date(rolledUpTo) : null }] }));
  clientQuery.mockImplementation(async () => ({ rows: [], rowCount: 0 }));
}

beforeEach(() => {
  clientQuery.mockReset();
  clientRelease.mockReset();
  poolQuery.mockReset();
});

describe('rollupNodeHourlyOnce', () => {
  it('never summarises the hour in progress', async () => {
    // Cursor at 10:00, "now" is 11:30 — 11:00→11:30 is still running.
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    // The upper bound is the top of the current hour, never 11:30: an hour
    // summarised while still running would need summarising again. Asserted
    // across every batch, because the re-derive window is now covered in
    // several of them and it is the LAST one that reaches the boundary.
    const uppers = clientQuery.mock.calls
      .filter((c) => String(c[0]).includes('INSERT INTO node_radio_hourly\n'))
      .map((c) => String((c[1] as unknown[])[1]));
    expect(uppers.length).toBeGreaterThan(0);
    expect(uppers.at(-1)).toBe('2026-08-24T11:00:00.000Z');
    for (const u of uppers) expect(u <= '2026-08-24T11:00:00.000Z').toBe(true);
  });

  it('re-derives the recent past, not just the hours it has never seen', async () => {
    // `recorded` is not final when an hour ends. markRecorded sets it when the
    // audio arrives, which lands after the call and so, often enough, after
    // the hour it belongs to was summarised. A cursor that only moved forward
    // left that hour permanently short — invisible while nothing read these
    // tables, and wrong numbers on the page the moment a tile is served from
    // one.
    armCursor('2026-08-24T10:00:00.000Z');
    const hours = await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    // Six hours back, plus the one new one.
    expect(hours).toBe(7);
    const p = paramsOf('INSERT INTO node_radio_hourly\n');
    expect(p[0]).toBe('2026-08-24T04:00:00.000Z');
  });

  it('does not walk back past the backfill window on a cold start', async () => {
    // A null cursor already reaches back as far as the detail table goes;
    // subtracting another six hours would only run off the end of it.
    armCursor(null);
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));
    const p = paramsOf('INSERT INTO node_radio_hourly\n');
    const upTo = Date.UTC(2026, 7, 24, 11, 0, 0);
    expect(p[0]).toBe(new Date(upTo - 24 * 31 * 3_600_000).toISOString());
  });

  it('does nothing when the cursor is already at the current hour', async () => {
    armCursor('2026-08-24T11:00:00.000Z');
    const hours = await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));
    expect(hours).toBe(0);
    expect(ran('INSERT INTO')).toHaveLength(0);
  });

  it('DELETEs the range before inserting it, so a re-run cannot double-count', async () => {
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    const order = sqls();
    const delNode = order.findIndex((s) => s.includes('DELETE FROM node_radio_hourly '));
    const insNode = order.findIndex((s) => s.includes('INSERT INTO node_radio_hourly\n'));
    const delSys = order.findIndex((s) => s.includes('DELETE FROM node_radio_hourly_sys'));
    const insSys = order.findIndex((s) => s.includes('INSERT INTO node_radio_hourly_sys'));
    const delUnit = order.findIndex((s) => s.includes('DELETE FROM node_radio_hourly_unit'));
    const insUnit = order.findIndex((s) => s.includes('INSERT INTO node_radio_hourly_unit'));
    expect(delNode).toBeGreaterThan(-1);
    expect(insNode).toBeGreaterThan(delNode);
    expect(delSys).toBeGreaterThan(-1);
    expect(insSys).toBeGreaterThan(delSys);
    expect(delUnit).toBeGreaterThan(-1);
    expect(insUnit).toBeGreaterThan(delUnit);
  });

  it('buckets a call by the hour it STARTED, in every table', async () => {
    // The per-node and per-site tables used to bucket each EVENT into its own
    // hour, so a call spanning a boundary was counted in both. About one call
    // per boundary — nothing, while nothing read these tables, and a permanent
    // floor on how closely a window sum could match the detail query the
    // overview is moving off. All four now agree with node_radio_hourly_rx.
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    for (const table of [
      'INSERT INTO node_radio_hourly\n',
      'INSERT INTO node_radio_hourly_sys',
      'INSERT INTO node_radio_hourly_rx',
      'INSERT INTO node_radio_hourly_unit',
      'INSERT INTO node_radio_hourly_heard',
    ]) {
      const sql = ran(table)[0] ?? '';
      // Ordered by id rather than received_at, matching radioReceptionsSql:
      // the rule that picks a call's home talkgroup should pick its hour too.
      expect(sql).toContain("array_agg(date_trunc('hour', received_at) ORDER BY id)");
    }
  });

  it('reads detail from before the range, so a call straddling its lower edge is filed once', async () => {
    // Bucketing by the call's first event is only half the answer. Each hour
    // gets its FINAL derivation in the pass where it is the lowest hour in the
    // range, and a scan starting at that boundary cannot see the receptions a
    // call had in the hour before it. Its earliest VISIBLE event then sits in
    // the boundary hour and the call is filed there too — while the previous
    // pass, which could see the whole call, had already filed it in the hour
    // before. Both rows survive, because each pass only DELETEs its own range.
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    for (const table of [
      'INSERT INTO node_radio_hourly\n',
      'INSERT INTO node_radio_hourly_sys',
      'INSERT INTO node_radio_hourly_rx',
      'INSERT INTO node_radio_hourly_unit',
      'INSERT INTO node_radio_hourly_heard',
    ]) {
      const sql = ran(table)[0] ?? '';
      // The scan reaches an hour behind the range, so the call's true first
      // event is always in it.
      expect(sql).toContain("received_at >= $1::timestamptz - interval '1 hour'");
      // And stops at $3, which the forward pass sets to the range's own end.
      expect(sql).toContain('received_at < $3::timestamptz');
      expect(paramsOf(table)[2]).toBe(paramsOf(table)[1]);
      // And the CALL's hour, not the scan window, decides which pass files it.
      expect(sql).toMatch(/(\w+)\.hour >= \$1::timestamptz AND \1\.hour < \$2::timestamptz/);
      // Inner joins, so the filter really does drop a call this pass does not
      // own rather than leaving it with a null hour.
      expect(sql).toMatch(/\bJOIN (ch|tx t) USING \(logical_call_id\)/);
      expect(sql).not.toContain('LEFT JOIN ch');
    }

    // The DELETE range is untouched: widening it would erase an hour this pass
    // no longer rebuilds.
    for (const del of [
      'DELETE FROM node_radio_hourly ',
      'DELETE FROM node_radio_hourly_sys',
      'DELETE FROM node_radio_hourly_rx',
      'DELETE FROM node_radio_hourly_unit',
      'DELETE FROM node_radio_hourly_heard',
    ]) {
      expect(ran(del)[0] ?? '').toContain(
        'hour >= $1::timestamptz AND hour < $2::timestamptz',
      );
    }
  });

  it('counts a radio the way the card does, and keeps its alias', async () => {
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    const sql = ran('INSERT INTO node_radio_hourly_unit')[0] ?? '';
    // The same tuple the detail query counts. The talkgroup is IN it: a radio
    // heard on a patched call was heard on each of its talkgroups.
    expect(sql).toContain('SELECT DISTINCT ch.hour, e.source_unit, e.logical_call_id, e.node_id');
    expect(sql).toContain('e.rfss, e.site, e.talkgroup');
    // Aliases that merely echo the RID back as text are not aliases.
    expect(sql).toContain('e.source_alias <> e.source_unit::text');
    // The band predicate is NOT applied here — it is a read-time question.
    expect(sql).not.toContain('BETWEEN 100000');
    // The exact time it was last heard in the hour (migration 125), which the
    // Aliases list's "Last heard" reads instead of rounding to the hour.
    expect(sql).toContain('MAX(e.received_at) AS last_heard');
    expect(sql).toContain('(hour, source_unit, receptions, alias, last_heard)');
  });

  it('attributes calls twice on the per-site table, once per call and once per talkgroup', async () => {
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    const sys = ran('INSERT INTO node_radio_hourly_sys')[0] ?? '';
    // logical_calls: once per call, fleet-wide. Summing rows gives the network
    // total, which is the only reason this table can be summed at all.
    expect(sys).toContain('PARTITION BY r.hour, r.logical_call_id\n');
    // logical_calls_tg: once per (call, system, talkgroup). The talkgroup card
    // asks COUNT(DISTINCT call) PER TALKGROUP, and a patched call belongs to
    // each of its talkgroups — under the fleet-wide attribution alone every
    // talkgroup but the lowest would read zero.
    expect(sys).toContain('PARTITION BY r.hour, r.logical_call_id, r.system, r.talkgroup');
    expect(sys).toContain('(COUNT(*) FILTER (WHERE a.rn_tg = 1))::int');
  });

  it('attributes a THIRD time per site, so the systems tree can count one', async () => {
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    const sys = ran('INSERT INTO node_radio_hourly_sys')[0] ?? '';
    // "How many calls did THIS SITE hear" is a question neither of the other
    // two answers: the fleet-wide attribution credits a simulcast call to the
    // lowest site only, and the talkgroup one counts a patched call once per
    // talkgroup. Summed over a site's rows this equals COUNT(DISTINCT call)
    // per site in the detail table, which is what the drill-down shows.
    expect(sys).toContain(
      'PARTITION BY r.hour, r.logical_call_id, r.system, r.rfss, r.site',
    );
    expect(sys).toContain('(COUNT(*) FILTER (WHERE a.rn_site = 1))::int');
    expect(sys).toContain('logical_calls_tg, logical_calls_site');
    // The site drill-down's encrypted tile needs the same per-site attribution:
    // under rn = 1 a simulcast call is encrypted at its lowest site only.
    expect(sys).toContain('encrypted_calls_site, recorded_calls_site');
    expect(sys).toContain('(COUNT(*) FILTER (WHERE a.rn_site = 1 AND a.call_encrypted))::int');
    expect(sys).toContain('(COUNT(*) FILTER (WHERE a.rn_site = 1 AND a.call_recorded))::int');
  });

  it('files each reception under ONE radio, so a node or site sum is its receptions', async () => {
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    const sql = ran('INSERT INTO node_radio_hourly_heard')[0] ?? '';
    // One row per reception first — node_radio_hourly_sys's grain exactly…
    expect(sql).toContain(
      'GROUP BY ch.hour, e.logical_call_id, e.node_id, e.system, e.rfss, e.site, e.talkgroup',
    );
    // …then the radio that keyed it, or 0. Counted once per radio present, as
    // the per-radio table does, a grant and call naming different radios would
    // make the node's sum exceed its receptions.
    expect(sql).toContain('FILTER (WHERE e.source_unit IS NOT NULL))[1], 0) AS source_unit');
    // NULL radios are kept (as 0) so nothing drops out of the node totals.
    expect(sql).not.toContain('source_unit IS NOT NULL\n');
    expect(sql).toContain('MAX(e.received_at) AS last_heard');
    // Plausibility is a read-time question, as for the per-radio table.
    expect(sql).not.toContain('BETWEEN 100000');
  });

  it('attributes each call to ONE row, so the site rows still sum correctly', async () => {
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    const sys = ran('INSERT INTO node_radio_hourly_sys')[0] ?? '';
    // The table is keyed per SITE but a call is not a per-site thing, so
    // counting distinct calls inside each site row and summing double-counts
    // every simulcast call — 406 read as 822 on a live hour. Each call is
    // attributed to exactly one row instead (rn = 1) and only that row counts
    // it, which makes any sum of rows exact.
    expect(sys).toContain('(COUNT(*) FILTER (WHERE a.rn = 1))::int');
    expect(sys).not.toContain('COUNT(DISTINCT r.logical_call_id)');
    // Partitioned by the CALL alone. Adding talkgroup counts a patched call
    // once per member talkgroup — 407 against a true 406 on the same hour.
    expect(sys).toContain('PARTITION BY r.hour, r.logical_call_id');
    // encrypted/recorded belong to the CALL, so they are resolved across all
    // of its receptions before the attribution filter picks the counting row.
    expect(sys).toContain('bool_or(r.encrypted) OVER w');
    expect(sys).toContain('WHERE a.rn = 1 AND a.call_encrypted');
    // Data and signalling are not calls.
    expect(sys).toContain("event_type LIKE 'CALL_GROUP%'");
  });

  it('advances the cursor only inside the same transaction as the rebuild', async () => {
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    const order = sqls();
    const begin = order.indexOf('BEGIN');
    const state = order.findIndex((s) => s.includes('UPDATE node_rollup_state'));
    const commit = order.indexOf('COMMIT');
    // If the cursor could advance outside the rebuild's transaction, a crash
    // between them would skip an hour permanently.
    expect(begin).toBeGreaterThanOrEqual(0);
    expect(state).toBeGreaterThan(begin);
    expect(commit).toBeGreaterThan(state);
  });

  it('batches a cold start, and stops before it monopolises the database', async () => {
    armCursor(null); // never run
    const hours = await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    // A month of catch-up is covered a few batches per tick, each its own
    // transaction — 8 batches of 6 hours. The old loop ran until it caught up,
    // which against a slow database meant one tick holding a writer connection
    // for hours; and at 24 hours a batch not one of them could finish inside
    // the writer pool's statement timeout, so the first failed and the cursor
    // never moved at all.
    expect(hours).toBe(48);
    expect(sqls().filter((s) => s === 'COMMIT')).toHaveLength(8);
    // Each batch raises the timeout for its own transaction only.
    expect(ran('SET LOCAL statement_timeout')).toHaveLength(8);
  });

  it('resumes from where the previous tick stopped', async () => {
    // The cursor the first tick committed is where the next one picks up, so
    // capping the work per tick delays the rebuild without losing any of it.
    armCursor('2026-08-24T00:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));
    // The FIRST commit can sit at or behind the armed cursor — a pass re-derives
    // the recent past before it moves on — so the one that matters is the last.
    const committed = clientQuery.mock.calls
      .filter((c) => String(c[0]).includes('UPDATE node_rollup_state'))
      .map((c) => String((c[1] as unknown[])[0]));
    expect(committed.length).toBeGreaterThan(1);
    expect(committed.at(-1)).toBe('2026-08-24T11:00:00.000Z');
  });

  it('never throws, and releases the connection, when the rebuild fails', async () => {
    poolQuery.mockResolvedValue({ rows: [{ rolled_up_to: new Date('2026-08-24T10:00:00.000Z') }] });
    clientQuery.mockImplementation(async (sql: string) => {
      if (String(sql).includes('INSERT INTO')) throw new Error('boom');
      return { rows: [], rowCount: 0 };
    });
    await expect(rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'))).resolves.toBe(0);
    expect(sqls()).toContain('ROLLBACK');
    expect(clientRelease).toHaveBeenCalled();
  });
});

describe('re-deriving history without moving the seam (migration 126)', () => {
  /** Both cursors, plus the oldest detail row the floor is computed from. */
  function armState(rolledUpTo: string, rederiveBelow: string | null, floor: string | null) {
    poolQuery.mockImplementation(async (sql: string) => {
      if (String(sql).includes('SELECT rolled_up_to, rederive_below')) {
        return {
          rows: [{
            rolled_up_to: new Date(rolledUpTo),
            rederive_below: rederiveBelow ? new Date(rederiveBelow) : null,
          }],
        };
      }
      if (String(sql).includes('AS floor')) return { rows: [{ floor: floor ? new Date(floor) : null }] };
      return { rows: [], rowCount: 0 };
    });
    clientQuery.mockImplementation(async () => ({ rows: [], rowCount: 0 }));
  }
  const stateWrites = () =>
    clientQuery.mock.calls.filter((c) => String(c[0]).includes('UPDATE node_rollup_state'));
  const NOW = new Date('2026-08-24T11:30:00.000Z');

  it('walks DOWN from rederive_below, newest first, and never touches rolled_up_to', async () => {
    // rolled_up_to is the read path's seam. Rewinding it to rebuild history —
    // what 081/084/085/123 did — put every staff read back on raw detail for
    // the whole catch-up.
    armState('2026-08-24T11:00:00.000Z', '2026-08-24T11:00:00.000Z', '2026-08-01T03:00:00.000Z');
    const hours = await rollupNodeHourlyOnce(NOW);

    // The forward pass had nothing to do, so the walk got all eight batches.
    expect(hours).toBe(48);
    const writes = stateWrites();
    expect(writes).toHaveLength(8);
    for (const w of writes) {
      expect(String(w[0])).toContain('SET rederive_below = $1');
      expect(String(w[0])).not.toContain('rolled_up_to');
    }
    // Each batch lowers the cursor to the bottom of the range it just rebuilt.
    expect(writes.map((w) => (w[1] as unknown[])[0])).toEqual([
      '2026-08-24T05:00:00.000Z',
      '2026-08-23T23:00:00.000Z',
      '2026-08-23T17:00:00.000Z',
      '2026-08-23T11:00:00.000Z',
      '2026-08-23T05:00:00.000Z',
      '2026-08-22T23:00:00.000Z',
      '2026-08-22T17:00:00.000Z',
      '2026-08-22T11:00:00.000Z',
    ]);
  });

  it('reads an hour past the top of each range, since no later pass will finish its calls', async () => {
    armState('2026-08-24T11:00:00.000Z', '2026-08-24T11:00:00.000Z', '2026-08-01T03:00:00.000Z');
    await rollupNodeHourlyOnce(NOW);

    const p = paramsOf('INSERT INTO node_radio_hourly_heard');
    expect(p).toEqual([
      '2026-08-24T05:00:00.000Z',
      '2026-08-24T11:00:00.000Z',
      // A call that starts at 10:59:58 belongs to this range; its last
      // receptions land after 11:00.
      '2026-08-24T12:00:00.000Z',
    ]);
    // The DELETE is still the owned range only.
    expect(paramsOf('DELETE FROM node_radio_hourly_heard')).toEqual([
      '2026-08-24T05:00:00.000Z',
      '2026-08-24T11:00:00.000Z',
    ]);
  });

  it('stops at the oldest complete hour of detail and clears the cursor', async () => {
    // Re-deriving an hour the pruner has half-deleted would replace its
    // correct rows with a fraction of them, permanently.
    armState('2026-08-24T11:00:00.000Z', '2026-08-24T11:00:00.000Z', '2026-08-24T02:00:00.000Z');
    const hours = await rollupNodeHourlyOnce(NOW);

    expect(hours).toBe(9);
    const writes = stateWrites().map((w) => (w[1] as unknown[])[0]);
    // [05:00, 11:00) and then [02:00, 05:00) — clipped at the floor, which
    // carries the cursor's clear in the same transaction.
    expect(writes).toEqual(['2026-08-24T05:00:00.000Z', null]);
    expect(paramsOf('INSERT INTO node_radio_hourly_heard', 1)[0]).toBe('2026-08-24T02:00:00.000Z');
  });

  it('clears a cursor already at the floor without re-deriving anything', async () => {
    armState('2026-08-24T11:00:00.000Z', '2026-08-01T00:00:00.000Z', '2026-08-01T03:00:00.000Z');
    const hours = await rollupNodeHourlyOnce(NOW);
    expect(hours).toBe(0);
    expect(ran('INSERT INTO')).toHaveLength(0);
    expect(
      poolQuery.mock.calls.some((c) => String(c[0]).includes('SET rederive_below = NULL')),
    ).toBe(true);
  });

  it('gives the forward pass first call on the batches', async () => {
    // A seam that falls behind slows every staff read; history can wait.
    armState('2026-08-22T00:00:00.000Z', '2026-08-22T00:00:00.000Z', '2026-08-01T03:00:00.000Z');
    await rollupNodeHourlyOnce(NOW);
    const writes = stateWrites().map((w) => String(w[0]));
    expect(writes).toHaveLength(8);
    expect(writes.every((s) => s.includes('SET rolled_up_to'))).toBe(true);
  });

  it('does nothing when there is no history to re-derive', async () => {
    armState('2026-08-24T11:00:00.000Z', null, '2026-08-01T03:00:00.000Z');
    expect(await rollupNodeHourlyOnce(NOW)).toBe(0);
    expect(ran('INSERT INTO')).toHaveLength(0);
    expect(poolQuery.mock.calls.some((c) => String(c[0]).includes('AS floor'))).toBe(false);
  });

  it('prunes the drill-down table at 32 days, every tick', async () => {
    armState('2026-08-24T11:00:00.000Z', null, null);
    await rollupNodeHourlyOnce(NOW);
    const prune = poolQuery.mock.calls.find((c) =>
      String(c[0]).includes('DELETE FROM node_radio_hourly_heard WHERE hour < $1'));
    expect(prune).toBeDefined();
    expect((prune?.[1] as unknown[])[0]).toBe('2026-07-23T11:00:00.000Z');
  });
});

describe('rollupHighWater', () => {
  it('reports the cursor the pruner must not delete past', async () => {
    poolQuery.mockResolvedValue({ rows: [{ rolled_up_to: new Date('2026-08-24T10:00:00.000Z') }] });
    await expect(rollupHighWater()).resolves.toEqual(new Date('2026-08-24T10:00:00.000Z'));
  });

  it('reports null when the rollup has never run, so nothing may be pruned', async () => {
    poolQuery.mockResolvedValue({ rows: [{ rolled_up_to: null }] });
    await expect(rollupHighWater()).resolves.toBeNull();
  });

  it('reports null rather than throwing when the query fails', async () => {
    poolQuery.mockRejectedValue(new Error('boom'));
    await expect(rollupHighWater()).resolves.toBeNull();
  });
});
