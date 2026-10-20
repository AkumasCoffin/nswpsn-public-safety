/**
 * Pager Data-tab endpoints: pager-overview, pager-pages, pager-filtered,
 * capcodes, capcode, pager-nodes, pager-node.
 *
 * Mocks the pg pool so we drive the Hono handlers end-to-end without a DB, and
 * points PAGER_CAPCODE_CSV at a temp file so capcode→alias resolution runs on
 * the NORMALISED capcode (a zero-padded CSV address resolves the unpadded
 * stored capcode). Asserts the JSON shapes, the logical_id grouping the
 * message browser depends on, and that every traffic figure leaves the
 * filtered pages out.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

const queryMock = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));

// requireRole stays real; the node-data reads are gated on canViewNodeData —
// stub it true so the routes run.
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canViewNodeData: vi.fn(() => Promise.resolve(true)) };
});

beforeAll(() => {
  const dir = mkdtempSync(path.join(tmpdir(), 'pager-caps-'));
  const csv = path.join(dir, 'Capcode-Aliases.csv');
  writeFileSync(
    csv,
    'id,address,alias,agency\n1,0010627,Gresford Brigade,RFS\n',
    'utf8',
  );
  process.env['PAGER_CAPCODE_CSV'] = csv;
});

async function setupApp() {
  const { nodeDataRouter } = await import('../../../src/api/node-data.js');
  const app = new Hono();
  // Inject an authenticated user so requireRole passes.
  app.use('*', async (c, next) => {
    c.set('userId', 'u1');
    await next();
  });
  app.route('/', nodeDataRouter);
  return app;
}

/** The SQL text of every node_pager_events query the last request ran. */
function pagerQueries(): Array<{ sql: string; params: unknown[] }> {
  return queryMock.mock.calls
    .map(([sql, params]) => ({ sql: String(sql), params: (params ?? []) as unknown[] }))
    .filter((q) => q.sql.includes('node_pager_events'));
}

beforeEach(() => {
  queryMock.mockReset();
});

describe('GET /api/node-data/pager-overview', () => {
  it('returns traffic totals, the filtered split, top capcodes, series and latest pages', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('WITH k AS')) {
        return {
          rows: [{
            at: new Date(1_700_000_600_000), message: 'STRUCTURE FIRE', freq_mhz: 148.5875,
            receptions: 2, capcodes: ['10627', '999'], nodes: [{ id: 'n1', name: 'Newcastle' }], total: 1,
          }],
        };
      }
      if (sql.includes('AS capcodes')) {
        return { rows: [{ pages: 42, logical: 30, nodes: 3, capcodes: 7 }] };
      }
      if (sql.includes('GROUP BY reason')) {
        return { rows: [{ reason: 'blocked capcode', n: 1300 }, { reason: 'time code', n: 51 }] };
      }
      if (sql.includes('GROUP BY capcode') && sql.includes('LIMIT 10')) {
        return {
          rows: [
            { capcode: '10627', pages: 20, last_seen: new Date(1_700_000_000_000) },
            { capcode: '999', pages: 5, last_seen: new Date(1_700_000_500_000) },
          ],
        };
      }
      if (sql.includes('unnest($3::text[]) AS k(capcode)')) {
        return {
          rows: [
            { capcode: '10627', node_id: 'n1', name: 'Newcastle', pages: 12 },
            { capcode: '999', node_id: null, name: null, pages: null },
          ],
        };
      }
      if (sql.includes('generate_series')) {
        return { rows: [{ hour: new Date(1_700_000_000_000), pages: 4 }] };
      }
      return { rows: [] };
    });

    const app = await setupApp();
    const res = await app.request('/api/node-data/pager-overview?window=7d');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;

    expect(body['window']).toBe('7d');
    expect(body['totals']).toEqual({ pages: 42, pagesLogical: 30, activeNodes: 3, capcodes: 7 });
    expect(body['filtered']).toEqual({ pages: 1351, byReason: { 'blocked capcode': 1300, 'time code': 51 } });

    const caps = body['topCapcodes'] as Array<Record<string, unknown>>;
    expect(caps).toHaveLength(2);
    // Zero-padded CSV address (0010627) resolves the unpadded stored capcode.
    expect(caps[0]?.['capcode']).toBe('10627');
    expect(caps[0]?.['alias']).toBe('Gresford Brigade');
    expect(caps[0]?.['agency']).toBe('RFS');
    expect(caps[0]?.['topNode']).toEqual({ id: 'n1', name: 'Newcastle', pages: 12 });
    // Unmatched capcode → null alias, null topNode.
    expect(caps[1]?.['alias']).toBeNull();
    expect(caps[1]?.['topNode']).toBeNull();

    // Per-node figures belong to /pager-nodes, not the overview.
    expect(body).not.toHaveProperty('topNodes');

    const series = body['series'] as Array<Record<string, unknown>>;
    expect(series[0]?.['pages']).toBe(4);
    expect(typeof series[0]?.['hour']).toBe('string');

    // One row per page as sent, every capcode it went to labelled.
    const recent = body['recent'] as Array<Record<string, unknown>>;
    expect(recent[0]?.['message']).toBe('STRUCTURE FIRE');
    expect(recent[0]?.['capcodes']).toEqual([
      { capcode: '10627', alias: 'Gresford Brigade', agency: 'RFS' },
      { capcode: '999', alias: null, agency: null },
    ]);
  });

  it('binds the blocked capcodes to every query and counts traffic only', async () => {
    queryMock.mockResolvedValue({ rows: [] });
    const app = await setupApp();
    await app.request('/api/node-data/pager-overview?window=24h');
    const qs = pagerQueries();
    expect(qs.length).toBeGreaterThanOrEqual(5);
    for (const q of qs) {
      expect(q.params[1]).toEqual(expect.arrayContaining(['1850000', '521839']));
      expect(q.sql).toContain("THEN 'blocked capcode'");
    }
  });
});

describe('GET /api/node-data/pager-pages', () => {
  it('searches the message case-insensitively and pages the groups', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('WITH k AS')) {
        return {
          rows: [{
            at: new Date(1_700_000_600_000), message: 'MVA persons trapped', freq_mhz: 148.5875,
            receptions: 3, capcodes: ['10627'], nodes: [{ id: 'n1', name: 'Newcastle' }], total: 57,
          }],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/pager-pages?window=7d&q=MVA&limit=20&offset=20');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['total']).toBe(57);
    expect(body['limit']).toBe(20);
    expect(body['offset']).toBe(20);
    const q = pagerQueries().find((x) => x.sql.includes('WITH k AS'))!;
    expect(q.sql).toContain('strpos(lower(e.message), $3) > 0');
    expect(q.params).toEqual(['7 days', expect.any(Array), 'mva', 20, 20]);
    // Copies of one message chain into a page on the gap to the previous one.
    expect(q.sql).toContain("<= interval '60 seconds'");
  });
});

describe('GET /api/node-data/pager-filtered', () => {
  it('lists filtered pages with their reason, and counts every reason', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('GROUP BY reason')) {
        return { rows: [{ reason: 'blocked capcode', n: 1300 }, { reason: 'time code', n: 51 }] };
      }
      if (sql.includes('GROUP BY capcode, reason')) {
        return { rows: [{ capcode: '521839', reason: 'blocked capcode', pages: 1300, last_seen: new Date(1_700_000_000_000) }] };
      }
      if (sql.includes('GROUP BY COALESCE(f.logical_id, f.id)')) {
        return {
          rows: [{
            at: new Date(1_700_000_000_000), capcode: '180111', reason: 'time code', message: '0157',
            freq_mhz: 148.9625, receptions: 1, nodes: [{ id: 'n1', name: 'Newcastle' }],
          }],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/pager-filtered?window=24h&reason=time%20code');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['byReason']).toEqual({ 'blocked capcode': 1300, 'time code': 51 });
    // The total is the chosen reason's, not the whole window's.
    expect(body['total']).toBe(51);
    expect((body['messages'] as Array<Record<string, unknown>>)[0]).toMatchObject({
      capcode: '180111', reason: 'time code', message: '0157',
    });
    const list = pagerQueries().find((x) => x.sql.includes('GROUP BY COALESCE(f.logical_id, f.id)'))!;
    expect(list.sql).toContain('AND f.reason = $3');
    expect(list.params[2]).toBe('time code');
  });

  it('rejects an unknown reason with 400', async () => {
    const app = await setupApp();
    const res = await app.request('/api/node-data/pager-filtered?reason=spam');
    expect(res.status).toBe(400);
  });
});

describe('GET /api/node-data/capcodes', () => {
  it('paginates the per-capcode rollup with alias + topNode, traffic only', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('COUNT(DISTINCT capcode)::int AS n')) {
        return { rows: [{ n: 3 }] };
      }
      if (sql.includes('GROUP BY capcode') && sql.includes('LIMIT $')) {
        return { rows: [{ capcode: '10627', pages: 9, last_seen: new Date(1_700_000_000_000) }] };
      }
      if (sql.includes('unnest($3::text[]) AS k(capcode)')) {
        return { rows: [{ capcode: '10627', node_id: 'n2', name: 'Hunter', pages: 6 }] };
      }
      return { rows: [] };
    });

    const app = await setupApp();
    const res = await app.request('/api/node-data/capcodes?window=24h&sort=pages&limit=50');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['total']).toBe(3);
    const list = body['capcodes'] as Array<Record<string, unknown>>;
    expect(list[0]?.['alias']).toBe('Gresford Brigade');
    expect(list[0]?.['pages']).toBe(9);
    expect(list[0]?.['topNode']).toEqual({ id: 'n2', name: 'Hunter', pages: 6 });
    for (const q of pagerQueries()) expect(q.sql).toMatch(/END\) IS NULL/);
  });

  it('rejects a non-numeric q prefix with 400', async () => {
    const app = await setupApp();
    const res = await app.request('/api/node-data/capcodes?q=abc');
    expect(res.status).toBe(400);
  });
});

describe('GET /api/node-data/capcode', () => {
  it('groups messages by logical_id and resolves the capcode alias', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('COUNT(DISTINCT COALESCE(logical_id, id))')) {
        return { rows: [{ n: 2 }] };
      }
      if (sql.includes('GROUP BY COALESCE(e.logical_id, e.id)')) {
        return {
          rows: [
            {
              at: new Date(1_700_000_500_000),
              message: 'STRUCTURE FIRE',
              freq_mhz: 148.7,
              receptions: 3,
              nodes: [
                { id: 'n1', name: 'Newcastle' },
                { id: 'n2', name: 'Hunter' },
              ],
            },
          ],
        };
      }
      return { rows: [] };
    });

    const app = await setupApp();
    const res = await app.request('/api/node-data/capcode?capcode=10627&window=7d');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['total']).toBe(2);
    expect(body['capcode']).toBe('10627');
    expect(body['alias']).toBe('Gresford Brigade');
    expect(body['agency']).toBe('RFS');
    const msgs = body['messages'] as Array<Record<string, unknown>>;
    expect(msgs[0]?.['message']).toBe('STRUCTURE FIRE');
    expect(msgs[0]?.['freqMhz']).toBe(148.7);
    expect(msgs[0]?.['receptions']).toBe(3);
    expect((msgs[0]?.['nodes'] as unknown[]).length).toBe(2);
  });

  it('returns 400 when capcode is missing', async () => {
    const app = await setupApp();
    const res = await app.request('/api/node-data/capcode?window=7d');
    expect(res.status).toBe(400);
  });
});

describe('GET /api/node-data/pager-nodes', () => {
  it('lists every pager node, including one that has heard nothing', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes("WHERE n.kind = 'pager'")) {
        return {
          rows: [
            { id: 'n1', name: 'Newcastle', state: 'NSW', lga: 'Newcastle', enabled: true, feed_enabled: true,
              pages: 120, receptions: 121, capcodes: 30, filtered: 900, unique_pages: 80,
              last_heard: new Date(1_700_000_000_000) },
            { id: 'n2', name: 'Quiet', state: 'NSW', lga: 'Bega', enabled: true, feed_enabled: false,
              pages: 0, receptions: 0, capcodes: 0, filtered: 0, unique_pages: 0, last_heard: null },
          ],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/pager-nodes?window=7d');
    expect(res.status).toBe(200);
    const nodes = ((await res.json()) as { nodes: Array<Record<string, unknown>> }).nodes;
    expect(nodes[0]).toMatchObject({ id: 'n1', pages: 120, unique: 80, filtered: 900, online: false, readers: [] });
    expect(nodes[1]).toMatchObject({ id: 'n2', pages: 0, lastHeard: null, feedEnabled: false });
  });
});

describe('GET /api/node-data/pager-node', () => {
  it('splits traffic from filtered and pages its recent pages', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.startsWith('SELECT id, name, kind')) {
        return { rows: [{ id: 'n1', name: 'Newcastle', kind: 'pager', state: 'NSW', lga: 'Newcastle' }] };
      }
      if (sql.includes('AS last_heard')) {
        return { rows: [{ pages: 10, receptions: 11, capcodes: 4, filtered: 300, last_heard: new Date(1_700_000_000_000) }] };
      }
      if (sql.includes('HAVING COUNT(DISTINCT node_id) = 1')) return { rows: [{ n: 7 }] };
      if (sql.includes('WITH k AS')) return { rows: [] };
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/pager-node?nodeId=n1&window=24h');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['totals']).toMatchObject({ pages: 10, receptions: 11, capcodes: 4, unique: 7, filtered: 300 });
    expect(body['recent']).toEqual({ total: 0, limit: 25, offset: 0, pages: [] });
    const recent = pagerQueries().find((x) => x.sql.includes('WITH k AS'))!;
    expect(recent.sql).toContain('e.node_id = $3');
  });
});
