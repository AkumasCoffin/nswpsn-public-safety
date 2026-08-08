/**
 * Pager Data-tab endpoints (owner|dev only): pager-overview, capcodes, capcode.
 *
 * Mocks the pg pool so we drive the Hono handlers end-to-end without a DB, and
 * points PAGER_CAPCODE_CSV at a temp file so capcode→alias resolution runs on
 * the NORMALISED capcode (a zero-padded CSV address resolves the unpadded
 * stored capcode). Asserts the aggregation JSON shapes and the logical_id
 * grouping the message browser depends on.
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

beforeEach(() => {
  queryMock.mockReset();
});

describe('GET /api/node-data/pager-overview', () => {
  it('returns totals, alias-labelled top capcodes with topNode, top nodes, and series', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('AS capcodes')) {
        return { rows: [{ pages: 42, logical: 30, nodes: 3, capcodes: 7 }] };
      }
      if (sql.includes('GROUP BY capcode') && sql.includes('LIMIT 15')) {
        return {
          rows: [
            { capcode: '10627', pages: 20, last_seen: new Date(1_700_000_000_000) },
            { capcode: '999', pages: 5, last_seen: new Date(1_700_000_500_000) },
          ],
        };
      }
      if (sql.includes('unnest($2::text[]) AS k(capcode)')) {
        return {
          rows: [
            { capcode: '10627', node_id: 'n1', name: 'Newcastle', pages: 12 },
            { capcode: '999', node_id: null, name: null, pages: null },
          ],
        };
      }
      if (sql.includes('GROUP BY e.node_id, n.name')) {
        return { rows: [{ node_id: 'n1', name: 'Newcastle', pages: 18 }] };
      }
      if (sql.includes("date_trunc('hour'")) {
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

    const nodes = body['topNodes'] as Array<Record<string, unknown>>;
    expect(nodes[0]).toEqual({ nodeId: 'n1', name: 'Newcastle', pages: 18 });

    const series = body['series'] as Array<Record<string, unknown>>;
    expect(series[0]?.['pages']).toBe(4);
    expect(typeof series[0]?.['hour']).toBe('string');
  });
});

describe('GET /api/node-data/capcodes', () => {
  it('paginates the per-capcode rollup with alias + topNode', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('COUNT(DISTINCT capcode)::int AS n')) {
        return { rows: [{ n: 3 }] };
      }
      if (sql.includes('GROUP BY capcode') && sql.includes('LIMIT $')) {
        return { rows: [{ capcode: '10627', pages: 9, last_seen: new Date(1_700_000_000_000) }] };
      }
      if (sql.includes('unnest($2::text[]) AS k(capcode)')) {
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
