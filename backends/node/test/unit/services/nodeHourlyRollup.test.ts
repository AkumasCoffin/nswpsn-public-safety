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
    // Cursor at 10:00, "now" is 11:30 — only 10:00→11:00 is complete.
    armCursor('2026-08-24T10:00:00.000Z');
    const hours = await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    expect(hours).toBe(1);
    // The upper bound is the top of the current hour, never 11:30: an hour
    // summarised while still running would need summarising again.
    const p = paramsOf('INSERT INTO node_radio_hourly\n');
    expect(p[0]).toBe('2026-08-24T10:00:00.000Z');
    expect(p[1]).toBe('2026-08-24T11:00:00.000Z');
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
    expect(delNode).toBeGreaterThan(-1);
    expect(insNode).toBeGreaterThan(delNode);
    expect(delSys).toBeGreaterThan(-1);
    expect(insSys).toBeGreaterThan(delSys);
  });

  it('counts a call once however many nodes or sites heard it', async () => {
    armCursor('2026-08-24T10:00:00.000Z');
    await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    const sys = ran('INSERT INTO node_radio_hourly_sys')[0] ?? '';
    // The inner query collapses to one row per (call, node, site) — a
    // reception — and logical_calls is DISTINCT over the call id on top of
    // that, so simulcast and multi-node reception cannot inflate it.
    expect(sys).toContain('COUNT(DISTINCT r.logical_call_id)::int');
    expect(sys).toContain('logical_call_id,\n                  node_id,');
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

  it('batches a cold start rather than rebuilding a month in one transaction', async () => {
    armCursor(null); // never run
    const hours = await rollupNodeHourlyOnce(new Date('2026-08-24T11:30:00.000Z'));

    expect(hours).toBe(24 * 31);
    // 31 days at 24h per batch — one COMMIT each, not one giant transaction.
    expect(sqls().filter((s) => s === 'COMMIT')).toHaveLength(31);
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
