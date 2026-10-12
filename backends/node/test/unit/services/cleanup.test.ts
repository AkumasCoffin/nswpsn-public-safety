/**
 * Partition-level data cleanup.
 *
 * These pin the one thing a partition drop can get wrong that a row DELETE
 * could not: the archive_*_latest sidecar outlives the parent rows it points
 * at. Write-time dedup (migration 018) stores a parent row only when the data
 * changes, so a record still in the feed but unchanged for longer than the
 * retention window has its single parent row inside the partition being
 * dropped — and once that row is gone nothing ever writes it again.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const clientQuery = vi.fn();
const clientRelease = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => ({
    connect: async () => ({ query: clientQuery, release: clientRelease }),
  })),
}));
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));
vi.mock('../../../src/services/statsArchiver.js', () => ({
  pruneOldSnapshots: vi.fn(async () => 0),
}));
vi.mock('../../../src/services/incidentImages.js', () => ({
  removeIncidentImageDir: vi.fn(async () => undefined),
  sweepStaleUploadParts: vi.fn(async () => 0),
  reconcileOrphanImageDirs: vi.fn(async () => undefined),
}));

import { runCleanupOnce } from '../../../src/services/cleanup.js';

/** The one expired partition every test is handed. */
const EXPIRED = {
  parent: 'archive_traffic',
  partition: 'archive_traffic_2026_07',
  range_to: '2026-08-01T00:00:00.000Z',
  row_count: '0',
};

const sqls = (): string[] => clientQuery.mock.calls.map((c) => String(c[0]));
const ran = (fragment: string): string[] => sqls().filter((s) => s.includes(fragment));
const paramsOf = (fragment: string): unknown[] =>
  (clientQuery.mock.calls.find((c) => String(c[0]).includes(fragment))?.[1] ?? []) as unknown[];

/** Answer the partition probe with one expired partition, everything else
 *  emptily. `fail` lets a test make one statement throw. */
function armOneExpiredPartition(fail?: (sql: string) => boolean) {
  clientQuery.mockImplementation(async (sql: string) => {
    const s = String(sql);
    if (fail?.(s)) throw new Error('boom');
    if (s.includes('pg_get_expr(child.relpartbound')) return { rows: [EXPIRED], rowCount: 1 };
    return { rows: [], rowCount: 0 };
  });
}

beforeEach(() => {
  clientQuery.mockReset();
  clientRelease.mockReset();
});

describe('runCleanupOnce', () => {
  it('re-arms live sidecar rows before dropping the partition holding their stored row', async () => {
    armOneExpiredPartition();
    const r = await runCleanupOnce(31);
    expect(r.partitions_dropped).toBe(1);

    // Clearing data_hash is the repair: lookupExistingHashes reads a NULL hash
    // as "no comparison basis", so the next poll stores a parent row again and
    // latest_fetched_at moves onto it. Without this the record silently leaves
    // the unique=1 history list while /filters keeps counting it.
    const rearm = ran('UPDATE archive_traffic_latest');
    expect(rearm).toHaveLength(1);
    expect(rearm[0]).toContain('SET data_hash = NULL');

    // Scoped to rows whose stored row is inside an expired range, and to
    // records still being seen in polls — a sidecar row nobody has seen since
    // the cutoff is deleted outright later in the same pass.
    const p = paramsOf('UPDATE archive_traffic_latest');
    expect(p[0]).toBe(EXPIRED.range_to);
    expect(rearm[0]).toContain('latest_fetched_at < $1::timestamptz');
    expect(rearm[0]).toContain('last_seen_at >= $2::timestamptz');

    // Before the DROP, not after: the sidecar row is only findable while the
    // partition still exists to be bounded against.
    const order = sqls();
    const upd = order.findIndex((s) => s.includes('UPDATE archive_traffic_latest'));
    const drop = order.findIndex((s) => s.includes('DROP TABLE'));
    expect(upd).toBeGreaterThan(-1);
    expect(drop).toBeGreaterThan(upd);
  });

  it('leaves the partition in place when the re-arm fails, so the next pass retries', async () => {
    // Drop-first would orphan the record permanently — the partition is gone
    // next pass, so nothing would ever notice. Re-arm-first makes a failure
    // cost an hour of retention instead.
    armOneExpiredPartition((s) => s.includes('UPDATE archive_traffic_latest'));
    const r = await runCleanupOnce(31);

    expect(ran('DROP TABLE')).toHaveLength(0);
    expect(r.partitions_dropped).toBe(0);
  });

  it('resets the statement timeout it raised, whatever happened', async () => {
    // The session-level SET survives release(), so a connection handed back
    // with no timeout at all would defeat the pool's default for whichever
    // API query drew it next.
    armOneExpiredPartition();
    await runCleanupOnce(31);
    expect(ran('SET statement_timeout = 0')).toHaveLength(1);
    expect(ran('RESET statement_timeout')).toHaveLength(1);
  });
});
