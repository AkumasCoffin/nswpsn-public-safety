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

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => ({ query: poolQuery })),
  getWriterPool: vi.fn(async () => ({ query: poolQuery })),
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

import { pruneNodeEventsOnce } from '../../../src/services/nodeEventsPruner.js';

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

    for (const table of ['node_pager_events', 'node_site_decode_samples', 'node_pending_recordings']) {
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
