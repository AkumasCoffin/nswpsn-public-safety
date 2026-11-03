/**
 * The pruner's rollup guard.
 *
 * node_radio_events is the ONLY source the hourly rollups can be derived from
 * (services/nodeHourlyRollup.ts), so pruning it past what the rollup has read
 * destroys that history permanently. These pin that the radio prune is bounded
 * by the rollup's high-water mark, and that the other detail tables — which
 * nothing derives from — are unaffected.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const poolQuery = vi.fn();
const highWater = vi.fn();
/** The checked-out client the partition jobs use for their transactions. */
const clientQuery = vi.fn();
const clientRelease = vi.fn();

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
// The factory is hoisted above the const declarations, so it cannot close
// over `highWater` directly — reach it through the hoisted vi.fn instead.
vi.mock('../../../src/services/nodeHourlyRollup.js', () => ({
  rollupHighWater: (...args: unknown[]) => highWater(...args),
}));

import {
  pruneNodeEventsOnce,
  ensureNodeEventPartitions,
} from '../../../src/services/nodeEventsPruner.js';

/** Statements issued against a given table. */
const forTable = (table: string): Array<{ sql: string; params: unknown[] }> =>
  poolQuery.mock.calls
    .map((c) => ({ sql: String(c[0]), params: (c[1] ?? []) as unknown[] }))
    .filter((c) => c.sql.includes(`DELETE FROM ${table} `));

beforeEach(() => {
  poolQuery.mockReset();
  highWater.mockReset();
  // One pass per table: delete nothing, so the batching loop exits at once.
  poolQuery.mockResolvedValue({ rows: [], rowCount: 0 });
});

describe('pruneNodeEventsOnce rollup guard', () => {
  it('bounds the radio prune by the rollup high-water mark', async () => {
    const water = new Date('2026-08-24T10:00:00.000Z');
    highWater.mockResolvedValue(water);
    await pruneNodeEventsOnce();

    const radio = forTable('node_radio_events');
    expect(radio).toHaveLength(1);
    // Both bounds present: the 30-day retention AND the rollup ceiling.
    expect(radio[0]!.sql).toContain("received_at < now() - interval '30 days'");
    expect(radio[0]!.sql).toContain('received_at < $1::timestamptz');
    expect(radio[0]!.params[0]).toBe(water.toISOString());
  });

  it('prunes NO radio detail when the rollup has never run', async () => {
    highWater.mockResolvedValue(null);
    await pruneNodeEventsOnce();
    // Deleting here would erase hours nothing had summarised, irrecoverably.
    expect(forTable('node_radio_events')).toHaveLength(0);
  });

  it('leaves the other detail tables unbounded — nothing derives from them', async () => {
    highWater.mockResolvedValue(null);
    await pruneNodeEventsOnce();

    for (const table of ['node_pager_events', 'node_site_decode_samples']) {
      const calls = forTable(table);
      expect(calls).toHaveLength(1);
      expect(calls[0]!.sql).not.toContain('$1::timestamptz');
    }
  });

  it('never throws when the high-water lookup fails', async () => {
    highWater.mockRejectedValue(new Error('boom'));
    await expect(pruneNodeEventsOnce()).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// After migration 135: node_radio_events is partitioned by UTC day, and its
// retention is DROP TABLE rather than DELETE.
// ---------------------------------------------------------------------------
describe('radio retention once partitioned', () => {
  const DAY = 86_400_000;
  let legacy: boolean;
  /** Partitions the bound probe reports as past the cutoff it was given. */
  let expired: Array<{ partition: string; range_to: string }>;

  const clientSql = () => clientQuery.mock.calls.map((c) => String(c[0]));

  beforeEach(() => {
    clientQuery.mockReset();
    clientRelease.mockReset();
    legacy = true;
    expired = [];
    poolQuery.mockImplementation(async (sql: string) => {
      if (sql.includes('relkind')) return { rows: [{ partitioned: true, legacy }], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    });
    clientQuery.mockImplementation(async (sql: string, params?: unknown[]) => {
      if (sql.includes('pg_get_expr(child.relpartbound')) {
        const cutoff = new Date(String(params?.[2]));
        return {
          rows: expired
            .filter((p) => new Date(p.range_to) <= cutoff)
            .map((p) => ({ parent: 'node_radio_events', row_count: '0', ...p })),
        };
      }
      if (sql.includes('ensure_node_radio_events_partition')) return { rows: [{ made: '0' }] };
      if (sql.startsWith('DROP TABLE') && sql.includes('legacy')) legacy = false;
      return { rows: [] };
    });
  });

  it('drops a day only once it is past BOTH retention and the rollup high-water', async () => {
    const now = Date.now();
    // The rollup is behind: it has read up to 32 days ago.
    highWater.mockResolvedValue(new Date(now - 32 * DAY));
    expired = [
      { partition: 'node_radio_events_20260101', range_to: new Date(now - 33 * DAY).toISOString() },
      // Past retention, but the rollup has not read it yet: must stay.
      { partition: 'node_radio_events_20260102', range_to: new Date(now - 31 * DAY).toISOString() },
    ];
    await pruneNodeEventsOnce();

    const drops = clientSql().filter((s) => s.startsWith('DROP TABLE'));
    expect(drops).toEqual(['DROP TABLE IF EXISTS node_radio_events_20260101']);
    // The probe was given the EARLIER of the two lines.
    const probe = clientQuery.mock.calls.find((c) => String(c[0]).includes('relpartbound'));
    expect(probe?.[1]?.[0]).toEqual(['node_radio_events']);
    expect(new Date(String(probe?.[1]?.[2])).getTime()).toBe(now - 32 * DAY);
  });

  it('runs each drop in its own transaction under a lock_timeout', async () => {
    highWater.mockResolvedValue(new Date());
    expired = [{ partition: 'node_radio_events_20260101', range_to: '2026-01-02T00:00:00Z' }];
    await pruneNodeEventsOnce();

    const s = clientSql();
    const drop = s.indexOf('DROP TABLE IF EXISTS node_radio_events_20260101');
    // BEGIN, SET LOCAL lock_timeout, DROP, COMMIT — the timeout scoped to
    // this transaction, so it cannot leak onto the pooled connection.
    expect(s.slice(drop - 2, drop + 2)).toEqual([
      'BEGIN',
      "SET LOCAL lock_timeout = '3s'",
      'DROP TABLE IF EXISTS node_radio_events_20260101',
      'COMMIT',
    ]);
  });

  it('a drop that times out is rolled back, logged, and does not stop the pass', async () => {
    highWater.mockResolvedValue(new Date());
    expired = [
      { partition: 'node_radio_events_20260101', range_to: '2026-01-02T00:00:00Z' },
      { partition: 'node_radio_events_20260102', range_to: '2026-01-03T00:00:00Z' },
    ];
    const base = clientQuery.getMockImplementation()!;
    clientQuery.mockImplementation(async (sql: string, params?: unknown[]) => {
      if (sql === 'DROP TABLE IF EXISTS node_radio_events_20260101') {
        throw Object.assign(new Error('canceling statement due to lock timeout'), { code: '55P03' });
      }
      return base(sql, params);
    });
    await expect(pruneNodeEventsOnce()).resolves.toBeUndefined();

    const s = clientSql();
    expect(s).toContain('ROLLBACK');
    expect(s).toContain('DROP TABLE IF EXISTS node_radio_events_20260102');
    // The pager/decode prunes still ran.
    expect(forTable('node_pager_events')).toHaveLength(1);
  });

  it('drops nothing when the rollup has never run', async () => {
    highWater.mockResolvedValue(null);
    expired = [{ partition: 'node_radio_events_20260101', range_to: '2026-01-02T00:00:00Z' }];
    await pruneNodeEventsOnce();
    expect(clientSql().filter((s) => s.startsWith('DROP TABLE'))).toHaveLength(0);
    expect(forTable('node_radio_events_legacy')).toHaveLength(0);
  });

  it('never drops a table it did not create, whatever the probe returns', async () => {
    highWater.mockResolvedValue(new Date());
    expired = [{ partition: 'node_radio_events_hourly', range_to: '2026-01-02T00:00:00Z' }];
    await pruneNodeEventsOnce();
    expect(clientSql().filter((s) => s.startsWith('DROP TABLE'))).toHaveLength(0);
  });

  it('trims the legacy partition by DELETE until its bound passes, then drops it', async () => {
    const water = new Date();
    highWater.mockResolvedValue(water);
    await pruneNodeEventsOnce();
    // Still within its life: trimmed with the same bounds as before.
    const del = forTable('node_radio_events_legacy');
    expect(del).toHaveLength(1);
    expect(del[0]!.sql).toContain("received_at < now() - interval '30 days'");
    expect(del[0]!.params[0]).toBe(water.toISOString());
    // The parent itself is never DELETEd from once partitioned.
    expect(forTable('node_radio_events')).toHaveLength(0);

    poolQuery.mockClear();
    clientQuery.mockClear();
    expired = [{ partition: 'node_radio_events_legacy', range_to: '2026-01-02T00:00:00Z' }];
    await pruneNodeEventsOnce();
    expect(clientSql()).toContain('DROP TABLE IF EXISTS node_radio_events_legacy');
    // Gone, so nothing left to trim.
    expect(forTable('node_radio_events_legacy')).toHaveLength(0);
  });

  it('analyzes the parent after a drop (autovacuum never does)', async () => {
    highWater.mockResolvedValue(new Date());
    expired = [{ partition: 'node_radio_events_20260101', range_to: '2026-01-02T00:00:00Z' }];
    await pruneNodeEventsOnce();
    expect(poolQuery.mock.calls.map((c) => String(c[0]))).toContain('ANALYZE node_radio_events');
  });

  it('keeps a runway from yesterday to a week ahead, in one transaction', async () => {
    await ensureNodeEventPartitions();
    const call = clientQuery.mock.calls.find((c) => String(c[0]).includes('ensure_node_radio_events_partition'));
    const day = (offset: number) => new Date(Date.now() + offset * DAY).toISOString().slice(0, 10);
    expect(call?.[1]).toEqual([day(-1), day(7)]);
    const s = clientSql();
    expect(s[0]).toBe('BEGIN');
    expect(s[1]).toBe("SET LOCAL lock_timeout = '3s'");
    expect(s.at(-1)).toBe('COMMIT');
    expect(clientRelease).toHaveBeenCalled();
  });

  it('the runway is a no-op before migration 135, and never throws', async () => {
    poolQuery.mockImplementation(async () => ({ rows: [{ partitioned: false, legacy: false }] }));
    await expect(ensureNodeEventPartitions()).resolves.toBe(0);
    expect(clientQuery).not.toHaveBeenCalled();

    poolQuery.mockImplementation(async (sql: string) =>
      sql.includes('relkind') ? { rows: [{ partitioned: true, legacy: true }] } : { rows: [] });
    clientQuery.mockImplementation(async (sql: string) => {
      if (sql.includes('ensure_node_radio_events_partition')) throw new Error('lock timeout');
      return { rows: [] };
    });
    await expect(ensureNodeEventPartitions()).resolves.toBe(0);
    expect(clientSql()).toContain('ROLLBACK');
  });
});
