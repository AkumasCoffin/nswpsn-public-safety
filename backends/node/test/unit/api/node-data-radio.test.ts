/**
 * Radio Data-tab rollups (owner|dev only): talkgroups + radios paging.
 *
 * The staff Data tab now mounts Talkgroups/Radios INSIDE a system drill-down,
 * so the UI always passes ?system=<p25 systemId>. These tests mock the pg pool
 * and assert the endpoints thread that filter into the grouped WHERE (and stay
 * fleet-wide when it is omitted, for backward safety).
 *
 * They also cover the read-time display enrichment: talkgroup labels resolved
 * from the global sdrtrunk alias config, site names resolved from the latest
 * node_site_snapshots channel_name (siteNames()), and talker aliases.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const queryMock = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));

// requireRole stays real; canManageNodes is stubbed true so the routes run.
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canManageNodes: vi.fn(() => Promise.resolve(true)) };
});

// Global config: one talkgroup alias so talkgroupLabels() resolves 10101.
vi.mock('../../../src/services/nodes/globalConfig.js', () => ({
  getGlobalConfig: vi.fn(() =>
    Promise.resolve({
      sdrtrunkConfig: {
        aliases: [
          { name: 'Sydney Metro 01', ids: [{ type: 'talkgroup', attrs: { value: '10101' } }] },
        ],
      },
    }),
  ),
}));

async function setupApp() {
  const { nodeDataRouter } = await import('../../../src/api/node-data.js');
  const app = new Hono();
  app.use('*', async (c, next) => {
    c.set('userId', 'u1');
    await next();
  });
  app.route('/', nodeDataRouter);
  return app;
}

// Capture every (sql, params) pair the handler runs. Empty page rows keep the
// enrich lateral from firing, so we only see the count + page queries.
type Call = { sql: string; params: unknown[] };
function captureCalls(): Call[] {
  const calls: Call[] = [];
  queryMock.mockImplementation((sql: string, params: unknown[] = []) => {
    calls.push({ sql, params });
    if (sql.includes('AS n')) return { rows: [{ n: 0 }] };
    return { rows: [] };
  });
  return calls;
}

beforeEach(() => {
  queryMock.mockReset();
  // Fresh module per test so the label/site-name caches (~60s TTL) never leak
  // one test's mocked rows into the next.
  vi.resetModules();
});

// The DISTINCT ON snapshot row siteNames() builds its map from.
const SNAPSHOT_ROW = { system_id: 721, rfss: 4, site_id: 85, channel_name: 'Cambewarra MT' };
const LAST_SEEN = new Date('2026-08-01T00:00:00Z');

describe('GET /api/node-data/talkgroups', () => {
  it('scopes the rollup to ?system=<id>', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, talkgroup'));
    expect(grouped).toBeDefined();
    // $1 is the window interval, so the system predicate binds $2 = 721.
    expect(grouped?.sql).toContain('system = $2');
    expect(grouped?.params).toContain(721);
  });

  it('stays fleet-wide with no system param', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, talkgroup'));
    expect(grouped?.sql).not.toContain('system = $');
  });

  it('rejects a non-numeric system with 400', async () => {
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?system=abc');
    expect(res.status).toBe(400);
  });
});

describe('GET /api/node-data/radios', () => {
  it('scopes the rollup to ?system=<id>', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/radios?window=7d&system=721');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, source_unit'));
    expect(grouped).toBeDefined();
    expect(grouped?.sql).toContain('system = $2');
    expect(grouped?.params).toContain(721);
  });

  it('stays fleet-wide with no system param', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/radios?window=7d');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, source_unit'));
    expect(grouped?.sql).not.toContain('system = $');
  });
});

describe('display enrichment (labels, site names, aliases)', () => {
  it('/system attaches topTalkgroup labels, radio aliases and per-site tg labels', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      if (sql.includes('COUNT(DISTINCT talkgroup)::int AS talkgroups')) {
        return { rows: [{ calls: 5, logical: 3, enc: 0, talkgroups: 1, radios: 1, sites: 1 }] };
      }
      if (sql.includes('GROUP BY talkgroup')) {
        return { rows: [{ talkgroup: 10101, calls: 5, logical: 3, enc: 0, last_seen: LAST_SEEN }] };
      }
      if (sql.includes('GROUP BY source_unit')) {
        return { rows: [{ radio: 999, alias: 'CAR 1', calls: 5, last_seen: LAST_SEEN }] };
      }
      if (sql.includes('FROM node_site_snapshots m')) {
        return {
          rows: [{
            rfss: 4, site: 85, nac: 1, calls: 5, logical: 3, last_seen: LAST_SEEN,
            top_tg: 10101, top_tg_calls: 5, channel_name: 'Cambewarra MT',
            control_frequency_mhz: 413.9125, channel_count: 2, neighbor_count: 1,
          }],
        };
      }
      if (sql.includes('AS name')) return { rows: [{ name: 'NSWPSN' }] };
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/system?window=7d&system=721');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.topTalkgroups[0].label).toBe('Sydney Metro 01');
    expect(body.topRadios[0].alias).toBe('CAR 1');
    expect(body.sites[0].name).toBe('Cambewarra MT');
    expect(body.sites[0].topTalkgroup).toEqual({ talkgroup: 10101, label: 'Sydney Metro 01', calls: 5 });
  });

  it('/talkgroups attaches label and lastSite/topSite names from snapshots', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      if (sql.includes('AS n')) return { rows: [{ n: 1 }] };
      if (sql.includes('GROUP BY wacn, system, talkgroup')) {
        return { rows: [{ wacn: null, system: 721, talkgroup: 10101, calls: 5, logical: 3, enc: 0, last_seen: LAST_SEEN }] };
      }
      if (sql.includes('WITH ORDINALITY')) {
        return {
          rows: [{
            ord: 1, last_rfss: 4, last_site: 85,
            top_rfss: 4, top_site: 85, top_site_calls: 5,
            top_node_id: 'n1', top_node_name: 'Node 1', top_node_calls: 5,
          }],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.talkgroups[0].label).toBe('Sydney Metro 01');
    expect(body.talkgroups[0].lastSite).toEqual({ rfss: 4, site: 85, name: 'Cambewarra MT' });
    expect(body.talkgroups[0].topSite).toEqual({ rfss: 4, site: 85, calls: 5, name: 'Cambewarra MT' });
  });

  it('/radios attaches site names and per-radio topTalkgroups labels', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      if (sql.includes('AS n')) return { rows: [{ n: 1 }] };
      if (sql.includes('GROUP BY wacn, system, source_unit')) {
        return { rows: [{ wacn: null, system: 721, radio: 999, alias: 'CAR 1', calls: 5, last_seen: LAST_SEEN }] };
      }
      if (sql.includes('WITH ORDINALITY')) {
        return {
          rows: [{
            ord: 1, last_rfss: 4, last_site: 85,
            top_rfss: 4, top_site: 85, top_site_calls: 5,
            top_node_id: 'n1', top_node_name: 'Node 1', top_node_calls: 5,
            top_tgs: [{ talkgroup: 10101, calls: 3 }],
          }],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/radios?window=7d&system=721');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.radios[0].lastSite).toEqual({ rfss: 4, site: 85, name: 'Cambewarra MT' });
    expect(body.radios[0].topSite).toEqual({ rfss: 4, site: 85, calls: 5, name: 'Cambewarra MT' });
    expect(body.radios[0].topTalkgroups).toEqual([{ talkgroup: 10101, calls: 3, label: 'Sydney Metro 01' }]);
  });

  it('/overview attaches topSites name via the rfss:site fallback and topUnits alias', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      if (sql.includes('GROUP BY source_unit')) {
        return { rows: [{ unit: 999, alias: 'CAR 1', calls: 9 }] };
      }
      if (sql.includes('GROUP BY site_rfss, site_id')) {
        return { rows: [{ site_rfss: 4, site_id: 85, calls: 9 }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/overview?window=7d&scope=radio');
    expect(res.status).toBe(200);
    const body = await res.json();
    // Overview topSites rows carry no system id — resolved via "rfss:site".
    expect(body.topSites[0]).toEqual({ siteRfss: 4, siteId: 85, name: 'Cambewarra MT', calls: 9 });
    expect(body.topUnits[0]).toEqual({ unit: 999, alias: 'CAR 1', calls: 9 });
  });
});
