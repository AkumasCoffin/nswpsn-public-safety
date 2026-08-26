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

// requireRole stays real; the node-data reads are gated on canViewNodeData —
// stub it true so the routes run.
vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canViewNodeData: vi.fn(() => Promise.resolve(true)) };
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

  it('adds the node_id filter when ?node= is set', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721&node=n1');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, talkgroup'));
    expect(grouped?.sql).toContain('node_id = $');
    expect(grouped?.params).toContain('n1');
  });

  // The site drill-down reuses this endpoint one rung down, so a site scope has
  // to narrow BOTH the rollup and the per-key laterals — an unscoped lateral
  // would report each talkgroup's fleet-wide top site/node on a page that says
  // it is showing one site.
  it('scopes the rollup AND the laterals to ?rfss=&site=', async () => {
    const calls: Call[] = [];
    queryMock.mockImplementation((sql: string, params: unknown[] = []) => {
      calls.push({ sql, params });
      if (sql.includes('AS n')) return { rows: [{ n: 1 }] };
      if (sql.includes('GROUP BY wacn, system, talkgroup')) {
        return { rows: [{ wacn: 1, system: 721, talkgroup: 10101, calls: 5, logical: 3, enc: 0, last_seen: LAST_SEEN }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721&rfss=4&site=85');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, talkgroup'));
    expect(grouped?.sql).toContain('site_rfss = $');
    expect(grouped?.sql).toContain('site_id = $');
    expect(grouped?.params).toEqual(expect.arrayContaining([4, 85]));
    const lateral = calls.find((c) => c.sql.includes('WITH ORDINALITY'));
    expect(lateral).toBeDefined();
    expect(lateral?.sql).toContain('e.site_rfss = $5');
    expect(lateral?.sql).toContain('e.site_id = $6');
    expect(lateral?.params).toEqual(expect.arrayContaining([4, 85]));
  });

  it('rejects rfss without site (a lone rfss would silently widen the scope)', async () => {
    const app = await setupApp();
    expect((await app.request('/api/node-data/talkgroups?rfss=4')).status).toBe(400);
    expect((await app.request('/api/node-data/talkgroups?site=85')).status).toBe(400);
  });

  // enc=hide drops ALWAYS-encrypted talkgroups. A mixed talkgroup stays, so the
  // predicate has to be "< COUNT(*)", not "= 0".
  it('enc=hide adds the HAVING filter to both the page and the count', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&enc=hide');
    expect(res.status).toBe(200);
    // The page rolls up in two levels now, so its HAVING sums the inner
    // group's counts rather than re-counting rows.
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, talkgroup') && !c.sql.includes('AS n'));
    expect(grouped?.sql).toContain('HAVING SUM(g.enc) < SUM(g.calls)');
    // The count groups per talkgroup only, so it keeps the flat predicate —
    // and either way it must group first and count the survivors, because the
    // predicate is per-group.
    const count = calls.find((c) => c.sql.includes('AS n'));
    expect(count?.sql).toContain('HAVING COUNT(*) FILTER (WHERE encrypted) < COUNT(*)');
    expect(count?.sql).not.toContain('COUNT(DISTINCT (wacn, system, talkgroup))');
  });

  it('omits the HAVING filter without enc=hide', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/talkgroups?window=7d');
    expect(calls.every((c) => !c.sql.includes('HAVING'))).toBe(true);
  });
});

// A talkgroup on this network sits in the range the agencies allocate, not the
// full 16-bit P25 space: above 30500 is decode noise (and 65535 is the all-ones
// null talkgroup). The ingest also drops 7-digit RADIO
// IDs into the same column, so every talkgroup list/count must gate on
// `talkgroup BETWEEN 10000 AND 30500`. The pg pool is mocked, so we emulate the DB
// filter in the mock: it only drops the out-of-range row when the executed SQL
// actually carries the predicate — proving BOTH the list query and the
// distinct-talkgroup COUNT wire it in (a missing predicate keeps the bogus row
// and fails the test).
describe('talkgroup range filter (radio ids excluded)', () => {
  // 10101 = a valid 5-digit TG; 2315291 = a 7-digit radio id masquerading.
  const CANDIDATE_TGS = [
    { wacn: null, system: 721, talkgroup: 10101, calls: 5, logical: 3, enc: 0, last_seen: LAST_SEEN },
    { wacn: null, system: 721, talkgroup: 2315291, calls: 2, logical: 1, enc: 0, last_seen: LAST_SEEN },
  ];
  const TG_PREDICATE = 'talkgroup BETWEEN 10000 AND 30500';
  const applyRange = (sql: string, rows: typeof CANDIDATE_TGS) =>
    sql.includes(TG_PREDICATE)
      ? rows.filter((r) => r.talkgroup >= 10000 && r.talkgroup <= 30500)
      : rows;

  it('/talkgroups drops out-of-range tgs from the list AND the distinct count, keeps a 5-digit tg', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      // Distinct-talkgroup COUNT for pagination total.
      if (sql.includes('AS n')) return { rows: [{ n: applyRange(sql, CANDIDATE_TGS).length }] };
      // Grouped per-talkgroup page (the list).
      if (sql.includes('GROUP BY wacn, system, talkgroup')) {
        return { rows: applyRange(sql, CANDIDATE_TGS) };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721');
    expect(res.status).toBe(200);
    const body = await res.json();
    // List: only the valid 5-digit TG survives.
    expect(body.talkgroups.map((t: { talkgroup: number }) => t.talkgroup)).toEqual([10101]);
    expect(body.talkgroups.some((t: { talkgroup: number }) => t.talkgroup === 2315291)).toBe(false);
    // ...and so does the sub-5-digit decode noise.
    expect(body.talkgroups.some((t: { talkgroup: number }) => t.talkgroup === 798)).toBe(false);
    // Distinct-talkgroup count: the radio id is excluded from the tally too.
    expect(body.total).toBe(1);
  });

  it('/system totals gate the distinct-talkgroup tile on the same range predicate', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/system?window=7d&system=721');
    expect(res.status).toBe(200);
    // The TALKGROUPS tile count must exclude out-of-range ids via FILTER.
    const totals = calls.find((c) => c.sql.includes('AS talkgroups'));
    expect(totals?.sql).toContain('FILTER (WHERE talkgroup BETWEEN 10000 AND 30500)');
    // The scoped top-talkgroups list is gated too.
    const tgList = calls.find(
      (c) => c.sql.includes('GROUP BY talkgroup') && c.sql.includes('ORDER BY calls DESC, talkgroup ASC'),
    );
    expect(tgList?.sql).toContain('talkgroup BETWEEN 10000 AND 30500');
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

describe('GET /api/node-data/systems (folder tree + node filter)', () => {
  it('eager-loads each system\'s sites[] with resolved names', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      // System rollup row (the folder).
      if (sql.includes('MIN(received_at) AS first_seen')) {
        return {
          rows: [{
            wacn: null, system: 721, name: 'NSWPSN', calls: 10, logical: 6, enc: 0,
            sites: 1, talkgroups: 2, radios: 3, first_seen: LAST_SEEN, last_seen: LAST_SEEN,
          }],
        };
      }
      // Per-site rollup (the folder's children).
      if (sql.includes('GROUP BY system, site_rfss, site_id')) {
        return { rows: [{ system: 721, rfss: 4, site: 85, calls: 5, logical: 3, last_seen: LAST_SEEN }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/systems?window=7d');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.systems[0].siteCount).toBe(1);
    expect(Array.isArray(body.systems[0].sites)).toBe(true);
    expect(body.systems[0].sites[0]).toEqual({
      rfss: 4, site: 85, name: 'Cambewarra MT', calls: 5, logicalCalls: 3,
      lastSeen: LAST_SEEN.toISOString(),
    });
  });

  it('adds the node_id filter to both rollups when ?node= is set', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/systems?window=7d&node=n1');
    expect(res.status).toBe(200);
    const sys = calls.find((c) => c.sql.includes('MIN(received_at) AS first_seen'));
    const sites = calls.find((c) => c.sql.includes('GROUP BY system, site_rfss, site_id'));
    expect(sys?.sql).toContain('node_id = $2');
    expect(sys?.params).toContain('n1');
    expect(sites?.sql).toContain('node_id = $2');
    expect(sites?.params).toContain('n1');
  });

  it('stays fleet-wide with no node param', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/systems?window=7d');
    expect(res.status).toBe(200);
    const sys = calls.find((c) => c.sql.includes('MIN(received_at) AS first_seen'));
    expect(sys?.sql).not.toContain('node_id = $');
  });
});

describe('display enrichment (labels, site names, aliases)', () => {
  it('/system attaches topTalkgroup labels, radio aliases and per-site tg labels', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      if (sql.includes('AS talkgroups')) {
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
    // 'CAR 1' is the OTA the radio transmitted; `alias` is the separate
    // configured unit label, absent in this stubbed config.
    expect(body.topRadios[0].ota).toBe('CAR 1');
    expect(body.topRadios[0].alias).toBeNull();
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
        return { rows: [{ unit: 999, alias: 'CAR 1', receptions: 9 }] };
      }
      if (sql.includes('GROUP BY site_rfss, site_id')) {
        return { rows: [{ site_rfss: 4, site_id: 85, receptions: 9 }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/overview?window=7d&scope=radio');
    expect(res.status).toBe(200);
    const body = await res.json();
    // Overview topSites rows carry no system id — resolved via "rfss:site".
    expect(body.topSites[0]).toEqual({ siteRfss: 4, siteId: 85, name: 'Cambewarra MT', receptions: 9 });
    // A radio can carry BOTH aliases: `ota` is what it transmitted over the
    // air, `alias` is its configured unit label. agency/color come from the
    // agencies' unit lists — all null here, since this stubbed config has no
    // agencies to own unit 999.
    expect(body.topUnits[0]).toEqual({
      unit: 999,
      ota: 'CAR 1',
      alias: null,
      agency: null,
      color: null,
      receptions: 9,
    });
  });
});

// ---------------------------------------------------------------------------
// CALL_GROUP filter: the Data page's radio side must reflect ONLY talkgroup
// VOICE calls (event_type CALL_GROUP / CALL_GROUP_ENCRYPTED), never P25 data/
// signaling (DATA_CALL / RESPONSE / QUERY / PAGE, whose `target` is a RADIO id
// stored in the talkgroup column). The pg pool is mocked, so — mirroring the
// TG-range tests above — the mock EMULATES the DB predicate: it drops the
// DATA_CALL row only when the executed SQL actually carries
// `upper(event_type) LIKE 'CALL_GROUP%'`. A missing predicate keeps the bogus
// row and fails the test, proving each query wires the filter in.
// ---------------------------------------------------------------------------
describe('CALL_GROUP filter (talkgroup voice calls only)', () => {
  const CG_PREDICATE = "LIKE 'CALL_GROUP%'";
  // event_type carried on each candidate so the mock can emulate the WHERE.
  // The DATA_CALL row uses an IN-RANGE talkgroup (5000) so ONLY the CALL_GROUP
  // predicate — not the TG range guard — can exclude it.
  const CANDIDATES = [
    { logical: '100', event_type: 'CALL_GROUP', talkgroup: 10101, enc: false },
    { logical: '200', event_type: 'CALL_GROUP_ENCRYPTED', talkgroup: 20202, enc: true },
    { logical: '300', event_type: 'DATA_CALL', talkgroup: 5000, enc: false },
  ];
  const onlyCalls = (sql: string) =>
    sql.includes(CG_PREDICATE)
      ? CANDIDATES.filter((r) => r.event_type.toUpperCase().startsWith('CALL_GROUP'))
      : CANDIDATES;

  it('/talkgroups drops a DATA_CALL row (in-range tg) from the list AND the count, keeps CALL_GROUP + CALL_GROUP_ENCRYPTED', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      const kept = onlyCalls(sql);
      if (sql.includes('AS n')) return { rows: [{ n: kept.length }] };
      if (sql.includes('GROUP BY wacn, system, talkgroup')) {
        return {
          rows: kept.map((r) => ({
            wacn: null, system: 721, talkgroup: r.talkgroup,
            calls: 5, logical: 3, enc: r.enc ? 5 : 0, last_seen: LAST_SEEN,
          })),
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721');
    expect(res.status).toBe(200);
    const body = await res.json();
    // List: both call-group talkgroups survive; the DATA_CALL id (5000) is gone
    // even though it is a valid 16-bit value.
    expect(body.talkgroups.map((t: { talkgroup: number }) => t.talkgroup)).toEqual([10101, 20202]);
    expect(body.talkgroups.some((t: { talkgroup: number }) => t.talkgroup === 5000)).toBe(false);
    // Distinct-talkgroup pagination count excludes the DATA_CALL too.
    expect(body.total).toBe(2);
  });

  it('/events shows one row per call for CALL_GROUP% only, excludes DATA_CALL from the list AND the count', async () => {
    queryMock.mockImplementation((sql: string, params: unknown[] = []) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      const kept = onlyCalls(sql);
      // Combined logical-stream count over the WITH union.
      if (sql.includes('AS n') && sql.includes('FROM u')) {
        return { rows: [{ n: kept.length }] };
      }
      // Page of logical-call ids.
      if (sql.includes('FROM u') && sql.includes('ORDER BY at DESC')) {
        return { rows: kept.map((r) => ({ type: 'radio', id: r.logical, at: LAST_SEEN })) };
      }
      // Hydration: full group aggregates for the page's ids.
      if (sql.includes('e.logical_call_id = ANY')) {
        const ids = (params[0] as unknown[]) ?? [];
        return {
          rows: kept
            .filter((r) => ids.map(String).includes(r.logical))
            .map((r) => ({
              id: r.logical, at: LAST_SEEN, system: 721, talkgroup: r.talkgroup,
              talkgroup_label: null, system_label: null, source_unit: 555, source_alias: null,
              frequency: null, action: 'CALL', event_type: r.event_type,
              encrypted: r.enc, recorded: false, receptions: 1,
              sites: [], nodes: [{ id: 'n1', name: 'Node 1' }],
            })),
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/events?type=radio');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.total).toBe(2);
    // Exactly the two call-group calls appear, once each; the DATA_CALL (tg
    // 5000, a radio-id target) never shows up.
    expect(body.events.map((e: { talkgroup: number }) => e.talkgroup).sort()).toEqual([10101, 20202]);
    expect(body.events.some((e: { talkgroup: number }) => e.talkgroup === 5000)).toBe(false);
    // The CALL_GROUP_ENCRYPTED call is flagged encrypted.
    const encEvt = body.events.find((e: { talkgroup: number }) => e.talkgroup === 20202);
    expect(encEvt.encrypted).toBe(true);
  });

  it('/overview reception totals count CALL_GROUP% only (DATA_CALL excluded)', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('FROM nodes')) return { rows: [] };
      if (sql.includes('AS received') && sql.includes('node_radio_events')) {
        const n = onlyCalls(sql).length;
        return { rows: [{ received: n, transmissions: n, ingested: n }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/overview?window=7d&scope=radio');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.totals.receptionsReceived).toBe(2);
    expect(body.totals.transmissions).toBe(2);
  });

  it('/overview classifies an outcome ONCE, with encrypted beating unprogrammed', async () => {
    let totalsSql = '';
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('FROM nodes')) return { rows: [] };
      if (sql.includes('AS received') && sql.includes('node_radio_events')) {
        totalsSql = sql;
        return { rows: [{ received: 0 }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    await app.request('/api/node-data/overview?window=7d&scope=radio');
    // Every encrypted talkgroup is ALSO unprogrammed in rdio, so testing
    // "programmed" first would swallow the whole encrypted population and
    // leave that tile reading zero. The enc arm must come first.
    const encAt = totalsSql.indexOf("THEN 'enc'");
    const noTgAt = totalsSql.indexOf("THEN 'no_tgid'");
    expect(encAt).toBeGreaterThan(-1);
    expect(noTgAt).toBeGreaterThan(encAt);
    // An unreachable rdio yields an EMPTY programmed list, which must not
    // condemn the whole network to no_tgid.
    expect(totalsSql).toContain('cardinality(');
    // The buckets partition receptions, so a drop is measured, never derived
    // as a residual — the old shape subtracted patch drops from site drops.
    expect(totalsSql).toContain('IS DISTINCT FROM t.home');
    expect(totalsSql).toContain('IS NOT DISTINCT FROM t.home');
  });

  it('/overview enc=hide drops encrypted transmissions whole, not just their tile', async () => {
    let totalsSql = '';
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('FROM nodes')) return { rows: [] };
      if (sql.includes('AS received') && sql.includes('node_radio_events')) {
        totalsSql = sql;
        return { rows: [{ received: 0 }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    await app.request('/api/node-data/overview?window=7d&scope=radio&enc=hide');
    // Filtered on the OUTCOME: dropping the rows instead would strand each
    // encrypted transmission's patch and site drops in the totals and break
    // the partition.
    expect(totalsSql).toContain("WHERE outcome <> 'enc'");
  });

  it('/overview window=all re-sources radio from detail (CALL_GROUP) and flags radioWindowCapped', async () => {
    const seen: string[] = [];
    queryMock.mockImplementation((sql: string) => {
      seen.push(sql);
      if (sql.includes('FROM nodes')) return { rows: [] };
      if (sql.includes('AS received') && sql.includes('node_radio_events')) {
        return { rows: [{ received: 2 }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/overview?window=all&scope=radio');
    expect(res.status).toBe(200);
    const body = await res.json();
    // all-window radio no longer reads the event_type-less hourly rollups.
    expect(seen.some((s) => s.includes('node_radio_hourly'))).toBe(false);
    // Every radio all-window query carries the CALL_GROUP predicate.
    const radioReads = seen.filter((s) => s.includes('node_radio_events'));
    expect(radioReads.length).toBeGreaterThan(0);
    expect(radioReads.every((s) => s.includes("LIKE 'CALL_GROUP%'"))).toBe(true);
    expect(body.radioWindowCapped).toBe(true);
    expect(body.totals.receptionsReceived).toBe(2);
  });
});
