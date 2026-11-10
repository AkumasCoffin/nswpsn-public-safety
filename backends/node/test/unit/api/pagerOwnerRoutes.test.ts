/**
 * The owner's view of their own pager node: /pager-overview and
 * /pager-messages on the feeder router.
 *
 * As with the ADS-B owner routes, the GATE is ownership rather than a staff
 * role, so somebody else's node must 404 and a node of another kind must 400.
 * Past the gate, what matters is that every query is scoped to the one node,
 * that traffic and filtered come apart by the same rule the staff views use,
 * and that nothing staff-only (which other nodes heard a page) reaches the
 * owner.
 */
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

const OWNER = 'user-owner';
const OTHER = 'user-other';

const PAGER_NODE = { id: 'node-pager-1', name: 'pager-01', kind: 'pager', user_id: OWNER, state: 'NSW' };
const RADIO_NODE = { id: 'node-radio-1', name: 'radio-01', kind: 'radio', user_id: OWNER, state: 'NSW' };
const FOREIGN_NODE = { ...PAGER_NODE, id: 'node-pager-2', user_id: OTHER };

const NODES: Record<string, Record<string, unknown>> = {
  [PAGER_NODE.id]: PAGER_NODE,
  [RADIO_NODE.id]: RADIO_NODE,
  [FOREIGN_NODE.id]: FOREIGN_NODE,
};

const queryMock = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));

vi.mock('../../../src/services/nodes/registry.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/nodes/registry.js')>();
  return {
    ...actual,
    getNode: vi.fn((id: string) => Promise.resolve(NODES[id] ?? null)),
  };
});

// The contributor role gate open, so the ownership check (ownedNodeById),
// which stays real, is the gate under test.
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, hasRole: vi.fn(() => Promise.resolve(true)) };
});

vi.mock('../../../src/services/auth/supabaseJwt.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/supabaseJwt.js')>();
  return {
    ...actual,
    requireSupabaseJwt: async (_c: unknown, next: () => Promise<void>) => { await next(); },
  };
});

vi.mock('../../../src/services/nodes/hub.js', () => ({
  hub: {
    liveStatus: () => null,
    isOnline: () => true,
    recordUpload: () => {},
    clearNode: () => {},
    forceDisconnectAgent: () => {},
  },
}));

beforeAll(() => {
  const dir = mkdtempSync(path.join(tmpdir(), 'pager-owner-caps-'));
  const csv = path.join(dir, 'Capcode-Aliases.csv');
  writeFileSync(csv, 'id,address,alias,agency\n1,0010627,Gresford Brigade,RFS\n', 'utf8');
  process.env['PAGER_CAPCODE_CSV'] = csv;
});

async function ownerApp(userId: string) {
  const { feederRouter } = await import('../../../src/api/feeder.js');
  const app = new Hono();
  app.use('*', async (c, next) => { c.set('userId', userId); await next(); });
  app.route('/', feederRouter);
  return app;
}

/** Every node_pager_events query the last request ran. */
function pagerQueries(): Array<{ sql: string; params: unknown[] }> {
  return queryMock.mock.calls
    .map(([sql, params]) => ({ sql: String(sql), params: (params ?? []) as unknown[] }))
    .filter((q) => q.sql.includes('node_pager_events'));
}

const isTrafficRows = (sql: string) =>
  sql.includes('GROUP BY COALESCE(e.logical_id, e.id)') && sql.includes('COUNT(*) OVER ()');
const isFilteredRows = (sql: string) =>
  sql.includes('GROUP BY COALESCE(f.logical_id, f.id)') && sql.includes('COUNT(*) OVER ()');

beforeEach(() => {
  queryMock.mockReset();
  queryMock.mockResolvedValue({ rows: [] });
});

describe('owner pager routes — the gate', () => {
  it('404s on somebody else’s node', async () => {
    const app = await ownerApp(OWNER);
    for (const p of ['pager-overview', 'pager-messages']) {
      const res = await app.request(`/api/feeder/nodes/${FOREIGN_NODE.id}/${p}`);
      expect(res.status).toBe(404);
      expect((await res.json()).error).toBe('not your node');
    }
    expect(pagerQueries()).toHaveLength(0);
  });

  it('400s on the owner’s own radio node', async () => {
    const app = await ownerApp(OWNER);
    for (const p of ['pager-overview', 'pager-messages']) {
      const res = await app.request(`/api/feeder/nodes/${RADIO_NODE.id}/${p}`);
      expect(res.status).toBe(400);
      expect((await res.json()).error).toBe('not a pager node');
    }
  });

  it('404s on a node id that does not exist', async () => {
    const app = await ownerApp(OWNER);
    const res = await app.request('/api/feeder/nodes/no-such-node/pager-overview');
    expect(res.status).toBe(404);
  });

  it('no longer serves the old ring-buffer route', async () => {
    const app = await ownerApp(OWNER);
    const res = await app.request(`/api/feeder/nodes/${PAGER_NODE.id}/messages`);
    expect(res.status).toBe(404);
  });
});

describe('GET /api/feeder/nodes/:id/pager-overview', () => {
  it('returns 24-hour and 7-day figures for this node only', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('AS messages_24h')) {
        return {
          rows: [{
            messages_24h: 150, messages_7d: 1100, filtered_24h: 1300, filtered_7d: 9400,
            capcodes_24h: 40, capcodes_7d: 120,
            last_message: new Date(1_700_000_600_000), last_heard: new Date(1_700_000_700_000),
          }],
        };
      }
      if (sql.includes('GROUP BY capcode')) {
        return { rows: [{ capcode: '10627', messages: 12, last_seen: new Date(1_700_000_500_000) }] };
      }
      if (sql.includes('GROUP BY reason')) {
        return { rows: [{ reason: 'blocked capcode', n: 9000 }, { reason: 'time code', n: 400 }] };
      }
      return { rows: [] };
    });
    const app = await ownerApp(OWNER);
    const res = await app.request(`/api/feeder/nodes/${PAGER_NODE.id}/pager-overview`);
    expect(res.status).toBe(200);
    expect(res.headers.get('cache-control')).toBe('no-store');
    const body = await res.json();
    expect(body.totals).toEqual({
      messages24h: 150, messages7d: 1100, filtered24h: 1300, filtered7d: 9400,
      capcodes24h: 40, capcodes7d: 120,
      lastMessage: new Date(1_700_000_600_000).toISOString(),
      lastHeard: new Date(1_700_000_700_000).toISOString(),
    });
    expect(body.topCapcodes).toEqual([{
      capcode: '10627', alias: 'Gresford Brigade', agency: 'RFS', messages: 12,
      lastSeen: new Date(1_700_000_500_000).toISOString(),
    }]);
    expect(body.filteredByReason).toEqual({ 'blocked capcode': 9000, 'time code': 400 });

    const qs = pagerQueries();
    expect(qs).toHaveLength(3);
    for (const q of qs) {
      expect(q.params).toEqual(['7 days', expect.any(Array), PAGER_NODE.id]);
      expect(q.sql).toContain('node_id = $3');
      expect(q.sql).toContain("THEN 'blocked capcode'");
    }
  });
});

describe('GET /api/feeder/nodes/:id/pager-messages', () => {
  it('lists traffic for this node, labelled by plan frequency, with no staff detail', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (isTrafficRows(sql)) {
        return {
          rows: [
            {
              at: new Date(1_700_000_600_000), message: 'STRUCTURE FIRE', freq_mhz: 148.9625, capcode: '10627',
              receptions: 2, nodes: [{ id: 'node-pager-1', name: 'pager-01' }, { id: 'n9', name: 'Somebody else' }],
              total: 57,
            },
            {
              at: new Date(1_700_000_590_000), message: 'MVA', freq_mhz: 148.7, capcode: '999',
              receptions: 1, nodes: [{ id: 'node-pager-1', name: 'pager-01' }], total: 57,
            },
          ],
        };
      }
      return { rows: [] };
    });
    const app = await ownerApp(OWNER);
    const res = await app.request(`/api/feeder/nodes/${PAGER_NODE.id}/pager-messages`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toMatchObject({ window: '24h', total: 57, limit: 50, offset: 0 });
    expect(body.messages[0]).toEqual({
      at: new Date(1_700_000_600_000).toISOString(), capcode: '10627', alias: 'Gresford Brigade', agency: 'RFS',
      message: 'STRUCTURE FIRE', source: 'FRNSW',
    });
    // Off the state's plan: the frequency itself.
    expect(body.messages[1].source).toBe('148.7000 MHz');
    expect(JSON.stringify(body)).not.toContain('Somebody else');

    const q = pagerQueries().find((x) => isTrafficRows(x.sql))!;
    expect(q.params).toEqual(['24 hours', expect.any(Array), PAGER_NODE.id, 50, 0]);
    expect(q.sql).toContain('e.node_id = $3');
    expect(q.sql).toMatch(/END\) IS NULL/);
  });

  it('lists filtered messages apart, with their reason', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (isFilteredRows(sql)) {
        return {
          rows: [{
            at: new Date(1_700_000_000_000), capcode: '521839', reason: 'blocked capcode', message: 'zUgXGMGA+1Tg97',
            freq_mhz: 148.9625, receptions: 1, nodes: [{ id: 'node-pager-1', name: 'pager-01' }], total: 9400,
          }],
        };
      }
      return { rows: [] };
    });
    const app = await ownerApp(OWNER);
    const res = await app.request(`/api/feeder/nodes/${PAGER_NODE.id}/pager-messages?filtered=1&window=7d&offset=50`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toMatchObject({ window: '7d', total: 9400, offset: 50 });
    expect(body.messages[0]).toEqual({
      at: new Date(1_700_000_000_000).toISOString(), capcode: '521839', alias: null, agency: null,
      message: 'zUgXGMGA+1Tg97', source: 'FRNSW', reason: 'blocked capcode',
    });

    const q = pagerQueries().find((x) => isFilteredRows(x.sql))!;
    // The node narrows the scan inside the CTE, not after it.
    expect(q.sql).toMatch(/WITH f AS \([\s\S]*AND e\.node_id = \$3[\s\S]*\)\s+SELECT/);
    expect(q.sql).toContain('f.reason IS NOT NULL');
    expect(q.params).toEqual(['7 days', expect.any(Array), PAGER_NODE.id, 50, 50]);
    expect(pagerQueries().some((x) => isTrafficRows(x.sql))).toBe(false);
  });

  it('falls back to 24 hours on a window it does not know', async () => {
    const app = await ownerApp(OWNER);
    const res = await app.request(`/api/feeder/nodes/${PAGER_NODE.id}/pager-messages?window=forever`);
    expect((await res.json()).window).toBe('24h');
  });
});

describe('pagerPlanLabelFor', () => {
  it('names plan frequencies by state and nothing else', async () => {
    const { pagerPlanLabelFor } = await import('../../../src/services/nodes/configMerge.js');
    expect(pagerPlanLabelFor('NSW', 148.5875)).toBe('NSWRFS');
    expect(pagerPlanLabelFor('NSW', 148.96250000001)).toBe('FRNSW');
    expect(pagerPlanLabelFor('NSW', 148.6375)).toBeNull();
    expect(pagerPlanLabelFor('QLD', 148.6375)).toBe('QFES');
    expect(pagerPlanLabelFor(null, 148.5875)).toBe('NSWRFS');
    expect(pagerPlanLabelFor('NSW', null)).toBeNull();
  });
});
