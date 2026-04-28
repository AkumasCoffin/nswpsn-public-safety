/**
 * /api/data/history route tests.
 *
 * Mocks the pg pool so we drive the route end-to-end without a real DB.
 * Each test feeds a canned `query()` response, exercises the Hono
 * handler, and asserts the JSON shape, cursor encoding, and error paths.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const queryMock = vi.fn();
const releaseMock = vi.fn();
const connectMock = vi.fn(() => Promise.resolve({ query: queryMock, release: releaseMock }));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ connect: connectMock, query: queryMock })),
  closePool: vi.fn(),
}));

interface PoolMockRow {
  id: number | string;
  source: string;
  source_id: string | null;
  fetched_at_epoch: number | string;
  fetched_at: Date | string;
  lat: number | null;
  lng: number | null;
  category: string | null;
  subcategory: string | null;
  data: Record<string, unknown> | string;
}

function row(over: Partial<PoolMockRow>): PoolMockRow {
  return {
    id: 1,
    source: 'rfs',
    source_id: 'inc-1',
    fetched_at_epoch: 1_700_000_000,
    fetched_at: new Date(1_700_000_000_000),
    lat: -33.86,
    lng: 151.21,
    category: 'Bushfire',
    subcategory: null,
    data: { title: 'Test', is_live: true },
    ...over,
  };
}

async function setupApp() {
  const { dataHistoryRouter } = await import('../../../src/api/data-history.js');
  const app = new Hono();
  app.route('/', dataHistoryRouter);
  return app;
}

beforeEach(() => {
  queryMock.mockReset();
  releaseMock.mockReset();
  connectMock.mockClear();
  // Default: SET LOCAL statement_timeout calls + query returning rows.
  // Tests override per-case as needed.
});

describe('GET /api/data/history', () => {
  it('returns the records, cursor, and query info', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('statement_timeout')) return { rows: [] };
      return {
        rows: [
          row({ id: 5, source: 'rfs', fetched_at_epoch: 1_700_000_500 }),
          row({ id: 4, source: 'rfs', fetched_at_epoch: 1_700_000_400 }),
        ],
      };
    });

    const app = await setupApp();
    const res = await app.request('/api/data/history?source=rfs&limit=2');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;

    expect(body['count']).toBe(2);
    const records = body['records'] as Array<Record<string, unknown>>;
    expect(records).toHaveLength(2);
    expect(records[0]?.['source']).toBe('rfs');
    expect(records[0]?.['title']).toBe('Test');
    // is_live derived from data->>is_live; should default true.
    expect(records[0]?.['is_live']).toBe(true);
    // latitude/longitude legacy keys, mapped from lat/lng columns.
    expect(records[0]?.['latitude']).toBe(-33.86);
    expect(records[0]?.['longitude']).toBe(151.21);

    // next_cursor should encode the last row's (fetched_at, id).
    expect(typeof body['next_cursor']).toBe('string');

    // query.filters_applied echoes the source filter only.
    const q = body['query'] as Record<string, unknown>;
    const filters = q['filters_applied'] as Record<string, unknown>;
    expect(filters['source']).toEqual(['rfs']);
  });

  it('omits next_cursor when fewer rows than limit are returned', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('statement_timeout')) return { rows: [] };
      return { rows: [row({ id: 1 })] };
    });

    const app = await setupApp();
    const res = await app.request('/api/data/history?source=rfs&limit=10');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['next_cursor']).toBeNull();
  });

  it('returns 400 for offset > MAX_OFFSET', async () => {
    const app = await setupApp();
    const res = await app.request('/api/data/history?offset=20000');
    expect(res.status).toBe(400);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['error']).toBe('offset_too_large');
    expect(body['max_offset']).toBe(10_000);
  });

  it('returns 400 for malformed cursor', async () => {
    const app = await setupApp();
    const res = await app.request('/api/data/history?cursor=!!!nope!!!');
    expect(res.status).toBe(400);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['error']).toBe('invalid_cursor');
  });

  it('routes a waze_police query to archive_waze only', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('statement_timeout')) return { rows: [] };
      if (sql.includes('SELECT COUNT(*)')) return { rows: [{ n: 0 }] };
      return { rows: [] };
    });

    const app = await setupApp();
    await app.request('/api/data/history?source=waze_police');
    const sqls = queryMock.mock.calls
      .map((call) => call[0] as string)
      .filter(
        (s) =>
          !s.includes('statement_timeout') &&
          !s.includes('SELECT COUNT(*)') &&
          s !== 'BEGIN' &&
          s !== 'COMMIT' &&
          s !== 'ROLLBACK',
      );
    expect(sqls).toHaveLength(1);
    expect(sqls[0]).toContain('FROM archive_waze');
  });

  it('fans out across 5 family tables when no source filter is set', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('statement_timeout')) return { rows: [] };
      if (sql.includes('SELECT COUNT(*)')) return { rows: [{ n: 0 }] };
      return { rows: [] };
    });

    const app = await setupApp();
    await app.request('/api/data/history');
    const sqls = queryMock.mock.calls
      .map((call) => call[0] as string)
      .filter(
        (s) =>
          !s.includes('statement_timeout') &&
          !s.includes('SELECT COUNT(*)') &&
          s !== 'BEGIN' &&
          s !== 'COMMIT' &&
          s !== 'ROLLBACK',
      );
    expect(sqls).toHaveLength(5);
    const targets = sqls.map((s) => {
      // Skip the `FROM fetched_at` inside `extract(epoch FROM fetched_at)`
      // by matching only the archive_* tables we actually care about.
      const m = s.match(/FROM\s+(archive_\w+)/);
      return m ? m[1] : null;
    });
    expect(new Set(targets)).toEqual(
      new Set([
        'archive_waze',
        'archive_traffic',
        'archive_rfs',
        'archive_power',
        'archive_misc',
      ]),
    );
  });

  it('threads unique=1 through to an is_latest filter', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('statement_timeout')) return { rows: [] };
      return { rows: [] };
    });

    const app = await setupApp();
    await app.request('/api/data/history?source=waze_police&unique=1');
    const sqls = queryMock.mock.calls
      .map((call) => call[0] as string)
      .filter((s) => !s.includes('statement_timeout'));
    expect(sqls.some((s) => s.includes('is_latest = true'))).toBe(true);
  });

  it('always sets statement_timeout = 60s before each query', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('statement_timeout')) return { rows: [] };
      return { rows: [] };
    });

    const app = await setupApp();
    await app.request('/api/data/history?source=rfs');
    const timeoutCalls = queryMock.mock.calls.filter((c) =>
      String(c[0]).includes('statement_timeout'),
    );
    expect(timeoutCalls.length).toBeGreaterThan(0);
    expect(String(timeoutCalls[0]?.[0])).toContain("'60s'");
  });

  it('echoes since/until as ISO strings in filters_applied', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('statement_timeout')) return { rows: [] };
      return { rows: [] };
    });

    const app = await setupApp();
    const res = await app.request(
      '/api/data/history?source=rfs&since=1700000000&until=1700001000',
    );
    const body = (await res.json()) as Record<string, unknown>;
    const filters = (body['query'] as Record<string, unknown>)['filters_applied'] as Record<
      string,
      unknown
    >;
    expect(typeof filters['since']).toBe('string');
    expect(typeof filters['until']).toBe('string');
  });
});

describe('GET /api/data/history/sources', () => {
  it('aggregates source counts across all 5 tables', async () => {
    let selectCalls = 0;
    queryMock.mockImplementation((sql: string) => {
      // Skip transaction control + SET LOCAL — they're plumbing, not
      // the per-table SELECT we're counting.
      if (
        sql === 'BEGIN' ||
        sql === 'COMMIT' ||
        sql === 'ROLLBACK' ||
        sql.includes('statement_timeout')
      ) {
        return { rows: [] };
      }
      selectCalls += 1;
      // Return different counts for waze and rfs; empty for the rest.
      if (sql.includes('archive_waze')) {
        return {
          rows: [
            { source: 'waze_police', count: '10', oldest: new Date(1_700_000_000_000), newest: new Date(1_700_001_000_000) },
          ],
        };
      }
      if (sql.includes('archive_rfs')) {
        return {
          rows: [
            { source: 'rfs', count: '5', oldest: new Date(1_700_000_000_000), newest: new Date(1_700_001_000_000) },
          ],
        };
      }
      return { rows: [] };
    });

    const app = await setupApp();
    const res = await app.request('/api/data/history/sources');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    const sources = body['sources'] as Array<Record<string, unknown>>;
    expect(sources).toHaveLength(2);
    // Sorted by count DESC.
    expect(sources[0]?.['source']).toBe('waze_police');
    expect(sources[0]?.['count']).toBe(10);
    expect(selectCalls).toBe(5); // one SELECT per table
  });
});

describe('GET /api/data/history/stats', () => {
  it('returns total + tables breakdown', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('statement_timeout')) return { rows: [] };
      if (sql.includes('archive_waze')) {
        return {
          rows: [
            {
              total: '100',
              oldest: new Date(1_700_000_000_000),
              newest: new Date(1_700_001_000_000),
            },
          ],
        };
      }
      return { rows: [{ total: '0', oldest: null, newest: null }] };
    });

    const app = await setupApp();
    const res = await app.request('/api/data/history/stats');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['total_records']).toBe(100);
    const tables = body['tables'] as Record<string, unknown>;
    expect(tables['archive_waze']).toBeDefined();
  });
});

describe('GET /api/data/history/incident/:source/:source_id', () => {
  it('returns the snapshot history for a single incident', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('statement_timeout')) return { rows: [] };
      return {
        rows: [
          row({ id: 1, fetched_at_epoch: 1_700_000_000, data: { title: 'a', is_live: false } }),
          row({ id: 2, fetched_at_epoch: 1_700_000_500, data: { title: 'b', is_live: true } }),
        ],
      };
    });

    const app = await setupApp();
    const res = await app.request('/api/data/history/incident/rfs/inc-1');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['source']).toBe('rfs');
    expect(body['source_id']).toBe('inc-1');
    expect(body['snapshots']).toBe(2);
    // Last record has is_live=true so the rollup should be true.
    expect(body['is_live']).toBe(true);
    const history = body['history'] as Array<Record<string, unknown>>;
    expect(history).toHaveLength(2);
    // Full data blob is included on incident endpoint.
    const lastData = history[1]?.['data'] as Record<string, unknown>;
    expect(lastData['title']).toBe('b');
  });
});

describe('GET /api/data/history/filters', () => {
  it('returns the provider/type-nested shape', async () => {
    const app = await setupApp();
    const res = await app.request('/api/data/history/filters');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(Array.isArray(body['providers'])).toBe(true);
    expect(body['date_range']).toBeDefined();
  });
});
