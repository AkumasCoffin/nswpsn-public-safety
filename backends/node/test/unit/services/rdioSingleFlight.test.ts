/**
 * rdio-pool hygiene: every cache that reads rdio's Postgres must
 *   (a) carry a statement_timeout on its pool — a hung query must never hold
 *       a connection open indefinitely against rdio's own database, and
 *   (b) single-flight its refresh — at TTL expiry every in-flight ingest
 *       reaches the stale check together, and ONE query must serve them all
 *       (the label-cache stampede once turned a 50-row response into ~100
 *       queries against the max-5 pool).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const queryMock = vi.fn(async (_sql?: string) => ({ rows: [] as unknown[] }));
const poolConfigs: Array<Record<string, unknown>> = [];

vi.mock('pg', () => {
  class Pool {
    query = queryMock;
    on = vi.fn();
    constructor(cfg: Record<string, unknown>) {
      poolConfigs.push(cfg);
    }
  }
  // rdio.ts default-imports pg for types.setTypeParser; the caches import
  // the named Pool. Serve both shapes.
  return { Pool, default: { Pool, types: { setTypeParser: vi.fn() } } };
});
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

async function fresh<T>(mod: string): Promise<T> {
  vi.resetModules();
  vi.doMock('../../../src/config.js', () => ({
    config: { RDIO_DATABASE_URL: 'postgres://rdio.local/rdio' },
  }));
  return (await import(mod)) as T;
}

beforeEach(() => {
  queryMock.mockClear();
  queryMock.mockImplementation(async () => ({ rows: [] }));
  poolConfigs.length = 0;
});

describe('rdioPatches', () => {
  it('pool carries a statement_timeout; concurrent stale reads share one query', async () => {
    const { rdioPatches } = await fresh<typeof import('../../../src/services/rdioPatches.js')>(
      '../../../src/services/rdioPatches.js',
    );
    const results = await Promise.all(Array.from({ length: 5 }, () => rdioPatches()));
    expect(queryMock).toHaveBeenCalledTimes(1);
    // All callers got the SAME lookup object — one refresh served everyone.
    expect(new Set(results).size).toBe(1);
    expect(poolConfigs[0]).toMatchObject({ max: 2, statement_timeout: 10_000 });
  });
});

describe('programmedTalkgroupIds', () => {
  it('pool carries a statement_timeout; concurrent stale reads share one query', async () => {
    const { programmedTalkgroupIds } = await fresh<
      typeof import('../../../src/services/rdioTalkgroups.js')
    >('../../../src/services/rdioTalkgroups.js');
    queryMock.mockImplementation(async () => ({ rows: [{ id: 10079 }] }));
    const results = await Promise.all(Array.from({ length: 5 }, () => programmedTalkgroupIds()));
    expect(queryMock).toHaveBeenCalledTimes(1);
    expect(results.every((s) => s.has(10079))).toBe(true);
    expect(poolConfigs[0]).toMatchObject({ max: 2, statement_timeout: 10_000 });
  });
});

describe('resolveLabels', () => {
  it('a concurrent batch at TTL expiry triggers ONE refresh (two scans), not one pair per row', async () => {
    const rdio = await fresh<typeof import('../../../src/services/rdio.js')>(
      '../../../src/services/rdio.js',
    );
    queryMock.mockImplementation(async (sql?: string) => {
      if (String(sql).includes('rdioScannerSystems')) return { rows: [{ id: 1, label: 'PSN' }] };
      return { rows: [{ systemId: 1, id: 10079, label: 'TG', name: 'Illawarra' }] };
    });
    const results = await Promise.all(
      Array.from({ length: 10 }, () => rdio.resolveLabels(1, 10079)),
    );
    // refreshLabelCache runs two queries (systems + talkgroups) — exactly once.
    expect(queryMock).toHaveBeenCalledTimes(2);
    expect(results.every((r) => r.systemLabel === 'PSN' && r.talkgroupLabel === 'Illawarra')).toBe(true);
  });
});
