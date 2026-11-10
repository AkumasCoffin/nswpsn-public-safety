/**
 * The sidecar read path of the filter facet aggregator.
 *
 * filterCache.test.ts covers the LiveStore-only variant, which needs no
 * database. This one mocks the pool so the archive_*_latest path answers —
 * the normal case in production once the sidecars have rows — and pins that
 * every dimension it counts survives the merge into the response.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const clientQuery = vi.fn();
const clientRelease = vi.fn();
const poolQuery = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => ({
    query: poolQuery,
    connect: async () => ({ query: clientQuery, release: clientRelease }),
  })),
}));
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

import { getFilterFacets, _resetFilterCacheForTests } from '../../../src/store/filterCache.js';

/** One sidecar row, as the per-table GROUP BY returns it. */
function row(over: Partial<Record<string, string>> = {}): Record<string, string> {
  return {
    source: 'rfs',
    category: 'Bushfire',
    subcategory: '',
    status: 'Advice',
    severity: '',
    state: 'NSW',
    cnt: '4',
    oldest: '1760000000',
    newest: '1760003600',
    ...over,
  };
}

/** Answer the archive_rfs_latest aggregate with `rows`, everything else
 *  emptily — the other three tables and the fallback paths included. */
function armSidecar(rows: Array<Record<string, string>>) {
  clientQuery.mockImplementation(async (sql: string) => {
    if (String(sql).includes('FROM archive_rfs_latest')) return { rows, rowCount: rows.length };
    return { rows: [], rowCount: 0 };
  });
  poolQuery.mockImplementation(async () => ({ rows: [], rowCount: 0 }));
}

beforeEach(() => {
  clientQuery.mockReset();
  clientRelease.mockReset();
  poolQuery.mockReset();
  _resetFilterCacheForTests();
});

describe('getFilterFacets (sidecar path)', () => {
  it('carries the state dimension through to the response', async () => {
    // The per-table merge used to list four dimension names and stop at
    // severity, so `state` was counted per table and then thrown away: every
    // type came back with states: [], logs.html hid its State dropdown and
    // cleared any state already selected, and the filter migration 082 added
    // could not be used at all — while archive_*_latest.state was filled the
    // whole time and dataHistoryQuery already honoured ?state=.
    armSidecar([row(), row({ state: 'ACT', cnt: '1' })]);
    const facets = await getFilterFacets(null, 24);

    const rfs = facets.providers.find((p) => p.key === 'rfs');
    const type = rfs?.types.find((t) => t.alert_type === 'rfs');
    expect(type?.count).toBe(5);
    expect(type?.states).toEqual([
      { value: 'NSW', count: 4 },
      { value: 'ACT', count: 1 },
    ]);
  });

  it('still carries the dimensions it always did', async () => {
    // Two values per dim: a dim with a single value covering the whole type is
    // dropped as a useless filter, which is its own behaviour and not the
    // thing under test here.
    armSidecar([row(), row({ category: 'Hazard Reduction', status: 'Watch and Act', cnt: '3' })]);
    const facets = await getFilterFacets(null, 24);

    const type = facets.providers
      .find((p) => p.key === 'rfs')
      ?.types.find((t) => t.alert_type === 'rfs');
    expect(type?.categories.map((c) => c.value)).toContain('Bushfire');
    expect(type?.statuses.map((s) => s.value)).toContain('Advice');
  });
});
