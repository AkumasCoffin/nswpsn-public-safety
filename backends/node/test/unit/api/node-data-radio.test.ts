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

// The rollup seam (resolveRollupEdge) is one infrastructure read that every
// fleet-wide rollup build makes before its real queries. Postgres always
// answers `SELECT <expr>` with exactly one row; the per-test mocks below route
// by SQL and answer anything unrecognised with NO rows, which would make every
// handler throw on the seam. So the pool answers the seam itself, as the
// database would, and hands everything else to the test's mock — which never
// sees it, so the call counts and orderings asserted below are unchanged.
const SEAM = new Date('2026-10-14T06:00:00.000Z');
/** Counts seam reads, which the test's own queryMock never sees. */
const seamReads = vi.fn();

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() =>
    Promise.resolve({
      query: (sql: string, params?: unknown[]) => {
        if (String(sql).includes('FROM node_rollup_state')) {
          seamReads();
          return Promise.resolve({ rows: [{ edge: SEAM }] });
        }
        // Always a promise, as pg's is: a handler may hold a query and await
        // it later.
        return Promise.resolve(queryMock(sql, params));
      },
    }),
  ),
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

beforeEach(async () => {
  queryMock.mockReset();
  seamReads.mockReset();
  // The talkgroup page is cached per query string, and these tests reuse
  // query strings with different mocks behind them.
  (await import('../../../src/api/node-data.js'))._resetTalkgroupsCache();
  // Fresh module per test so the label/site-name caches (~60s TTL) never leak
  // one test's mocked rows into the next.
  vi.resetModules();
});

// The DISTINCT ON snapshot row siteNames() builds its map from.
const SNAPSHOT_ROW = { system_id: 721, rfss: 4, site_id: 85, channel_name: 'Cambewarra MT' };
const isPageSql = (sql: string) =>
  (sql.includes('GROUP BY wacn, system, talkgroup') || sql.includes('GROUP BY system, talkgroup'))
  && !sql.includes('AS n');
const LAST_SEEN = new Date('2026-08-01T00:00:00Z');

describe('GET /api/node-data/talkgroups', () => {
  it('scopes the rollup to ?system=<id>', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => isPageSql(c.sql));
    expect(grouped).toBeDefined();
    // Fleet-wide: the hourly rollups plus the hour in progress, not a scan of
    // every reception in the window.
    expect(grouped?.sql).toContain('FROM node_radio_hourly_sys');
    expect(grouped?.sql).toContain('FROM node_radio_events');
    // $1 is the window interval, so the system predicate binds $2 = 721.
    expect(grouped?.sql).toContain('system = $2');
    expect(grouped?.params).toContain(721);
  });

  it('stays fleet-wide with no system param', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => isPageSql(c.sql));
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
    const grouped = calls.find((c) => isPageSql(c.sql));
    expect(grouped?.sql).toContain('node_id = $');
    expect(grouped?.params).toContain('n1');
    // Node-scoped stays on raw events: the rollups carry no node.
    expect(grouped?.sql).not.toContain('node_radio_hourly_sys');
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
      if (isPageSql(sql)) {
        return { rows: [{ wacn: 1, system: 721, talkgroup: 10101, calls: 5, logical: 3, enc: 0, last_seen: LAST_SEEN }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721&rfss=4&site=85');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => isPageSql(c.sql));
    expect(grouped?.sql).toContain('site_rfss = $');
    expect(grouped?.sql).toContain('site_id = $');
    expect(grouped?.params).toEqual(expect.arrayContaining([4, 85]));
    const lateral = calls.find((c) => c.sql.includes('WITH ORDINALITY'));
    expect(lateral).toBeDefined();
    // Fleet-wide the enrichment reads node_radio_hourly_heard on the list's
    // seam — a window of detail here was 3.3 s of a 3.4 s request — scoped to
    // the page's talkgroups and the site.
    expect(lateral?.sql).toContain('FROM node_radio_hourly_heard');
    expect(lateral?.sql).toContain('talkgroup = ANY($4::int[]) AND site_rfss = $5 AND site_id = $6');
    expect(lateral?.sql).not.toContain('received_at >= now()');
    expect(lateral?.params).toEqual(expect.arrayContaining([4, 85]));
    expect(seamReads).toHaveBeenCalledTimes(1);
  });

  it('keeps the raw enrichment when the list is scoped to a node', async () => {
    const calls: Call[] = [];
    queryMock.mockImplementation((sql: string, params: unknown[] = []) => {
      calls.push({ sql, params });
      if (sql.includes('AS n')) return { rows: [{ n: 1 }] };
      if (isPageSql(sql)) {
        return { rows: [{ wacn: 1, system: 721, talkgroup: 10101, calls: 5, logical: 3, enc: 0, last_seen: LAST_SEEN }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    await app.request('/api/node-data/talkgroups?window=7d&system=721&rfss=4&site=85&node=n1');
    const lateral = calls.find((c) => c.sql.includes('WITH ORDINALITY'));
    expect(lateral?.sql).toContain('e.node_id = $5');
    expect(lateral?.sql).toContain('e.site_rfss = $6');
    expect(lateral?.sql).toContain('e.site_id = $7');
    expect(lateral?.sql).not.toContain('node_radio_hourly_heard');
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
    // Fleet-wide reads the rollups, whose encrypted count is in LOGICAL
    // calls — so "always encrypted" is enc = logical, and the filter is the
    // same predicate on the page and the count. Either way the count must
    // group first and count the survivors, because the predicate is
    // per-group.
    const grouped = calls.find((c) => isPageSql(c.sql));
    expect(grouped?.sql).toContain('HAVING SUM(enc) < SUM(logical)');
    const count = calls.find((c) => c.sql.includes('AS n'));
    expect(count?.sql).toContain('HAVING SUM(enc) < SUM(logical)');
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
      if (isPageSql(sql)) {
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
  const isRadioPage = (sql: string) => sql.includes('g.radio ASC');

  it('scopes the rollup to ?system=<id>', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/radios?window=7d&system=721');
    expect(res.status).toBe(200);
    const page = calls.find((c) => isRadioPage(c.sql));
    expect(page).toBeDefined();
    // Fleet-wide reads node_radio_hourly_heard plus the hour in progress, not
    // every reception of the system in the window.
    expect(page?.sql).toContain('FROM node_radio_hourly_heard');
    expect(page?.sql).toContain(`received_at >= '${SEAM.toISOString()}'::timestamptz`);
    expect(page?.sql).not.toContain('received_at >= now()');
    expect(page?.sql).toContain('system = $2');
    expect(page?.params).toContain(721);
  });

  it('stays fleet-wide with no system param', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/radios?window=7d');
    expect(res.status).toBe(200);
    const page = calls.find((c) => isRadioPage(c.sql));
    expect(page?.sql).not.toContain('system = $');
  });

  it('filters named radios on their lifetime OTA, and sorts unnamed last', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/radios?window=7d&system=721&has=ota&sort=ota&dir=asc');
    const page = calls.find((c) => isRadioPage(c.sql));
    expect(page?.sql).toContain('FROM node_radio_aliases');
    expect(page?.sql).toContain('WHERE a.alias IS NOT NULL');
    expect(page?.sql).toContain('ORDER BY a.alias ASC NULLS LAST, g.radio ASC');
    const count = calls.find((c) => c.sql.includes('AS n'));
    expect(count?.sql).toContain('WHERE a.alias IS NOT NULL');
  });

  it('keeps the raw path when scoped to a node', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/radios?window=7d&system=721&node=n1');
    expect(res.status).toBe(200);
    const grouped = calls.find((c) => c.sql.includes('GROUP BY wacn, system, source_unit'));
    expect(grouped?.sql).toContain('node_id = $3');
    expect(calls.some((c) => c.sql.includes('node_radio_hourly_heard'))).toBe(false);
    expect(seamReads).not.toHaveBeenCalled();
  });
});

describe('GET /api/node-data/systems (folder tree + node filter)', () => {
  it('eager-loads each system\'s sites[] with resolved names', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      // Fleet-wide: the system row comes from the hourly rollups (no wacn,
      // name or radios there), the folder's children likewise.
      if (sql.includes('MIN(first_seen) AS first_seen')) {
        return {
          rows: [{
            system: 721, calls: 10, logical: 6, enc: 0,
            sites: 1, talkgroups: 2, hours: 9, first_seen: LAST_SEEN, last_seen: LAST_SEEN,
          }],
        };
      }
      if (sql.includes('GROUP BY system, rfss, site')) {
        return { rows: [{ system: 721, rfss: 4, site: 85, calls: 5, logical: 3, hours: 9, last_seen: LAST_SEEN }] };
      }
      // WACN and label resolve from the system's newest reception.
      if (sql.includes('AS k(system)')) return { rows: [{ system: 721, wacn: 781824, system_label: 'NSWPSN' }] };
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/systems?window=7d');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.systems[0].name).toBe('NSWPSN');
    expect(body.systems[0].wacn).toBe(781824);
    // No rollup carries radios: fleet-wide reports null, never a fake zero.
    expect(body.systems[0].radios).toBeNull();
    expect(body.systems[0].siteCount).toBe(1);
    expect(Array.isArray(body.systems[0].sites)).toBe(true);
    expect(body.systems[0].sites[0]).toEqual({
      rfss: 4, site: 85, name: 'Cambewarra MT', calls: 5, logicalCalls: 3,
      lastSeen: LAST_SEEN.toISOString(),
    });
  });

  it('counts a site\'s calls with the per-SITE attribution, not the fleet-wide one', async () => {
    // node_radio_hourly_sys.logical_calls attributes a call once FLEET-WIDE,
    // so summing it per site credits a simulcast call to the lowest
    // (system, talkgroup, rfss, site) only. Every other site in the group read
    // far below its real figure — "12 / 5,000" calls against receptions — and
    // contradicted both the node-scoped tree and the /api/node-data/site
    // drill-down, which count every call heard at the site from raw events.
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/systems?window=7d');
    expect(res.status).toBe(200);

    const sites = calls.find((c) => c.sql.includes('GROUP BY system, rfss, site'));
    expect(sites?.sql).toContain('logical_calls_site AS logical');
    expect(sites?.sql).not.toContain('logical_calls AS logical');
    // The live half already counts it per site, which is the figure the stored
    // half now has to match.
    expect(sites?.sql).toContain('COUNT(DISTINCT logical_call_id)::int');
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
    // Fleet-wide goes to the rollups; the raw five-COUNT(DISTINCT) scan over
    // every reception in the window did not finish inside 150 s at 7 days.
    const sys = calls.find((c) => c.sql.includes('MIN(first_seen) AS first_seen'));
    expect(sys).toBeDefined();
    expect(sys?.sql).toContain('FROM node_radio_hourly_sys');
    expect(sys?.sql).not.toContain('node_id = $');
    expect(calls.some((c) => c.sql.includes('MIN(received_at) AS first_seen'))).toBe(false);
  });
});

describe('display enrichment (labels, site names, aliases)', () => {
  it('/system attaches topTalkgroup labels, radio aliases and per-site tg labels', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      if (sql.includes('AS talkgroups')) {
        return { rows: [{ calls: 5, logical: 3, enc: 0, rec: 0, talkgroups: 1, sites: 1 }] };
      }
      if (sql.includes('GROUP BY talkgroup')) {
        return { rows: [{ talkgroup: 10101, calls: 5, logical: 3, enc: 0, last_seen: LAST_SEEN }] };
      }
      if (sql.includes('GROUP BY source_unit')) {
        return { rows: [{ radio: 999, calls: 5, last_seen: LAST_SEEN, radios: 1 }] };
      }
      // The OTA is lifetime identity, from the alias table.
      if (sql.includes('FROM node_radio_aliases')) return { rows: [{ radio_id: 999, alias: 'CAR 1' }] };
      if (sql.includes('FROM st LEFT JOIN tt')) {
        return {
          rows: [{
            rfss: 4, site: 85, nac: null, calls: 5, logical: 3, hours: 9, last_seen: LAST_SEEN,
            top_tg: 10101, top_tg_calls: 5,
          }],
        };
      }
      if (sql.includes('AS in_window')) {
        return {
          rows: [{
            node_id: 'n1', wacn: 781824, rfss: 4, site: 85, nac: 1, channel_name: 'Cambewarra MT',
            control_frequency_mhz: 413.9125, channel_count: 2, neighbor_count: 1,
            received_at: LAST_SEEN, in_window: true,
          }],
        };
      }
      if (sql.includes('AS k(system)')) return { rows: [{ system: 721, wacn: 781824, system_label: 'NSWPSN' }] };
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/system?window=7d&system=721');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.name).toBe('NSWPSN');
    expect(body.topTalkgroups[0].label).toBe('Sydney Metro 01');
    // 'CAR 1' is the OTA the radio transmitted; `alias` is the separate
    // configured unit label, absent in this stubbed config.
    expect(body.topRadios[0].ota).toBe('CAR 1');
    expect(body.topRadios[0].alias).toBeNull();
    expect(body.totals.radios).toBe(1);
    expect(body.sites[0].name).toBe('Cambewarra MT');
    // NAC and the deep metadata come from the site's snapshot.
    expect(body.sites[0]).toMatchObject({ nac: 1, controlFrequencyMhz: 413.9125, channelCount: 2 });
    expect(body.sites[0].topTalkgroup).toEqual({ talkgroup: 10101, label: 'Sydney Metro 01', calls: 5 });
  });

  it('/talkgroups reduces the single grouped pass: top site and node by calls, last site by time', async () => {
    // One grouped row per (site, node). The three laterals this replaced
    // answered "newest site", "busiest site", "busiest node" with three scans;
    // the reduction has to give the same answers from one.
    const OLD = '2026-10-08T01:00:00.000Z';
    const NEW = '2026-10-08T02:00:00.000Z';
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      if (sql.includes('AS n')) return { rows: [{ n: 1 }] };
      if (isPageSql(sql)) {
        return { rows: [{ wacn: null, system: 721, talkgroup: 10101, calls: 9, logical: 4, enc: 0, last_seen: LAST_SEEN }] };
      }
      if (sql.includes('FROM nodes WHERE id = ANY')) {
        return { rows: [{ id: 'n1', name: 'Node 1' }, { id: 'n2', name: 'Node 2' }] };
      }
      if (sql.includes('WITH ORDINALITY')) {
        return {
          rows: [
            // Site 4/85 is busiest (3+3=6) and node n2 is busiest (3+2=5),
            // but the NEWEST row with a site is site 4/86 on n1.
            { ord: 1, site_rfss: 4, site_id: 85, node_id: 'n1', calls: 3, last_at: OLD },
            { ord: 1, site_rfss: 4, site_id: 85, node_id: 'n2', calls: 3, last_at: OLD },
            { ord: 1, site_rfss: 4, site_id: 86, node_id: 'n1', calls: 1, last_at: NEW },
            { ord: 1, site_rfss: null, site_id: null, node_id: 'n2', calls: 2, last_at: NEW },
          ],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721');
    expect(res.status).toBe(200);
    const tg = (await res.json()).talkgroups[0];
    expect(tg.topSite).toMatchObject({ rfss: 4, site: 85, calls: 6 });
    expect(tg.topNode).toMatchObject({ id: 'n2', name: 'Node 2', calls: 5 });
    // A row with no site must not become the last site, however new it is.
    expect(tg.lastSite).toMatchObject({ rfss: 4, site: 86 });
  });

  it('/talkgroups attaches label and lastSite/topSite names from snapshots', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      if (sql.includes('AS n')) return { rows: [{ n: 1 }] };
      if (isPageSql(sql)) {
        return { rows: [{ wacn: null, system: 721, talkgroup: 10101, calls: 5, logical: 3, enc: 0, last_seen: LAST_SEEN }] };
      }
      // The rollups carry no wacn; the page fills it from the system's newest
      // reception.
      if (sql.includes('AS k(system)')) return { rows: [{ system: 721, wacn: 781824, system_label: 'NSWPSN' }] };
      if (sql.includes('FROM nodes WHERE id = ANY')) return { rows: [{ id: 'n1', name: 'Node 1' }] };
      if (sql.includes('WITH ORDINALITY')) {
        return {
          // One grouped row per (key, site, node); the handler reduces it.
          rows: [{ ord: 1, site_rfss: 4, site_id: 85, node_id: 'n1', calls: 5, last_at: LAST_SEEN }],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/talkgroups?window=7d&system=721');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.talkgroups[0].label).toBe('Sydney Metro 01');
    expect(body.talkgroups[0].wacn).toBe(781824);
    expect(body.talkgroups[0].lastSite).toEqual({ rfss: 4, site: 85, name: 'Cambewarra MT' });
    expect(body.talkgroups[0].topSite).toEqual({ rfss: 4, site: 85, calls: 5, name: 'Cambewarra MT' });
  });

  it('/radios attaches site names and per-radio topTalkgroups labels', async () => {
    const OLD = '2026-10-08T01:00:00.000Z';
    const NEW = '2026-10-08T02:00:00.000Z';
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      if (sql.includes('AS n')) return { rows: [{ n: 1 }] };
      if (sql.includes('g.radio ASC')) {
        return { rows: [{ wacn: null, system: 721, radio: 999, alias: 'CAR 1', calls: 9, last_seen: LAST_SEEN }] };
      }
      if (sql.includes('AS k(system)')) return { rows: [{ system: 721, wacn: 781824, system_label: 'NSWPSN' }] };
      if (sql.includes('FROM nodes WHERE id = ANY')) return { rows: [{ id: 'n1', name: 'Node 1' }] };
      // One grouped row per (radio, node, talkgroup, site); the handler
      // reduces them as the four laterals used to answer.
      if (sql.includes('AS tg_ok')) {
        const row = (o: Record<string, unknown>) => ({
          system: 721, radio: 999, node_id: 'n1', talkgroup: 10101, rfss: 4, site: 85,
          calls: 1, last_at: OLD, tg_ok: true, ...o,
        });
        return {
          rows: [
            row({ calls: 5 }),
            row({ talkgroup: 10202, calls: 2 }),
            // Newest, but at no site: never the last site.
            row({ talkgroup: 0, rfss: -1, site: -1, calls: 1, last_at: NEW }),
            // Out of the talkgroup band: never a top talkgroup.
            row({ talkgroup: 65535, calls: 9, tg_ok: false }),
          ],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/radios?window=7d&system=721');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.radios[0].wacn).toBe(781824);
    expect(body.radios[0].ota).toBe('CAR 1');
    expect(body.radios[0].lastSite).toEqual({ rfss: 4, site: 85, name: 'Cambewarra MT' });
    expect(body.radios[0].topSite).toEqual({ rfss: 4, site: 85, calls: 16, name: 'Cambewarra MT' });
    expect(body.radios[0].topNode).toEqual({ id: 'n1', name: 'Node 1', calls: 17 });
    expect(body.radios[0].topTalkgroups).toEqual([
      { talkgroup: 10101, calls: 5, label: 'Sydney Metro 01' },
      { talkgroup: 10202, calls: 2, label: null },
    ]);
  });

  it('/overview attaches topSites name via the rfss:site fallback and topUnits alias', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('DISTINCT ON (system_id, rfss, site_id)')) return { rows: [SNAPSHOT_ROW] };
      if (sql.includes('node_radio_hourly_unit')) {
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
      if (isPageSql(sql)) {
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

  it('/overview window=all reads the rollups, keeps CALL_GROUP, and stays capped at 30 days', async () => {
    const seen: string[] = [];
    queryMock.mockImplementation((sql: string) => {
      seen.push(sql);
      if (sql.includes('FROM nodes')) return { rows: [] };
      // The tiles: completed hours from the rollup, plus the live hour.
      if (sql.includes('node_radio_hourly_rx')) {
        return { rows: [{ received: 2, transmissions: 1, ingested: 1 }] };
      }
      if (sql.includes('AS received') && sql.includes('node_radio_events')) {
        return { rows: [{ received: 1, transmissions: 1, ingested: 1 }] };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/overview?window=all&scope=radio');
    expect(res.status).toBe(200);
    const body = await res.json();

    // This test used to assert the OPPOSITE — that all-window radio never
    // touched the hourly rollups — because they were per-event counters with
    // no event_type column and reading them counted signalling as voice. They
    // are derived from these same rows now, and filter CALL_GROUP doing it.
    expect(seen.some((s) => s.includes('node_radio_hourly_sys'))).toBe(true);
    expect(seen.some((s) => s.includes('node_radio_hourly_unit'))).toBe(true);

    // The tiles come from the rollup too. They used not to: the raw totals
    // thunk was gated on wantRadio alone, so every fleet-wide `all` ran
    // radioReceptionsSql over 30 days of detail — past the statement timeout
    // at the measured ~1.7s a day — while the comment above it said otherwise.
    expect(seen.some((s) => s.includes('node_radio_hourly_rx'))).toBe(true);
    expect(
      seen.some((s) => s.includes('AS received') && s.includes("interval '30 days'")),
    ).toBe(false);
    // Rollup hours (2) plus the live hour (1).
    expect(body.totals.receptionsReceived).toBe(3);

    // What has NOT changed: every read of the detail table is gated on
    // CALL_GROUP, so data and signalling are still never counted as calls.
    const radioReads = seen.filter((s) => s.includes('node_radio_events'));
    expect(radioReads.length).toBeGreaterThan(0);
    expect(radioReads.every((s) => s.includes("LIKE 'CALL_GROUP%'"))).toBe(true);

    // And radio all-time is still 30-day all-time: the rollups are derived
    // from a table pruned at 30 days, so moving to them buys speed, not reach.
    expect(body.radioWindowCapped).toBe(true);
  });

  it('/overview reads the rollup seam ONCE per build, and never inside a statement', async () => {
    // Every fleet-wide build makes five list statements and two totals
    // statements against the rollups. The seam used to be a subquery pasted
    // into each; the planner could not see its value, guessed a third of the
    // table for the live hour, and JIT-compiled the result — ~824ms of the
    // ~900ms the totals took, on every poll. It is now read once and inlined.
    const seen: string[] = [];
    queryMock.mockImplementation((sql: string) => {
      seen.push(sql);
      return { rows: [] };
    });
    const app = await setupApp();
    await app.request('/api/node-data/overview?window=7d&scope=radio');
    expect(seamReads).toHaveBeenCalledTimes(1);
    expect(seen.some((s) => s.includes('node_rollup_state'))).toBe(false);
    const rollupReads = seen.filter((s) => s.includes('node_radio_hourly'));
    expect(rollupReads.length).toBeGreaterThan(0);
    for (const s of rollupReads) {
      expect(s).toContain("'2026-10-14T06:00:00.000Z'::timestamptz");
    }
  });

  it('/overview scoped to a node reads no seam — it reads no rollup', async () => {
    queryMock.mockImplementation(() => ({ rows: [] }));
    const app = await setupApp();
    await app.request('/api/node-data/overview?window=7d&scope=radio&node=n1');
    expect(seamReads).not.toHaveBeenCalled();
  });
});

describe('overview response cache', () => {
  /** Count the executions one /overview request costs. */
  const countingPool = () => {
    const seen: string[] = [];
    queryMock.mockImplementation((sql: string) => {
      seen.push(sql);
      if (sql.includes('FROM nodes')) return { rows: [] };
      return { rows: [] };
    });
    return seen;
  };

  it('serves a repeat of the same request without re-querying', async () => {
    // The staff page polls every 30s and a second tab doubles that; production
    // logs show the 7d view fetched three times inside 30 seconds.
    const seen = countingPool();
    const app = await setupApp();
    await app.request('/api/node-data/overview?window=7d&scope=radio');
    const afterFirst = seen.length;
    expect(afterFirst).toBeGreaterThan(0);

    await app.request('/api/node-data/overview?window=7d&scope=radio');
    expect(seen.length).toBe(afterFirst);
  });

  it('keys on everything that changes the answer', async () => {
    // enc=hide is not a display filter — it removes those transmissions from
    // the totals — so it must not share a cache entry with the plain view.
    const seen = countingPool();
    const app = await setupApp();
    await app.request('/api/node-data/overview?window=7d&scope=radio');
    const afterFirst = seen.length;

    await app.request('/api/node-data/overview?window=7d&scope=radio&enc=hide');
    expect(seen.length).toBeGreaterThan(afterFirst);
    const afterSecond = seen.length;

    await app.request('/api/node-data/overview?window=24h&scope=radio');
    expect(seen.length).toBeGreaterThan(afterSecond);
  });

  it('can be switched off outright', async () => {
    // Number('0') || 20_000 is 20_000, so the obvious off switch used to be
    // the one setting that did nothing.
    process.env['NODE_OVERVIEW_TTL_MS'] = '0';
    try {
      vi.resetModules();
      const seen = countingPool();
      const app = await setupApp();
      await app.request('/api/node-data/overview?window=7d&scope=radio');
      const afterFirst = seen.length;
      await app.request('/api/node-data/overview?window=7d&scope=radio');
      expect(seen.length).toBeGreaterThan(afterFirst);
    } finally {
      delete process.env['NODE_OVERVIEW_TTL_MS'];
    }
  });
});

describe('GET /api/node-data/radio-aliases (the Units tab)', () => {
  // Each request runs a count and a page over the same FROM, so both are
  // asserted.
  const unitQueries = (calls: Call[]) => calls.filter((c) => c.sql.includes('FROM node_radio_aliases'));

  it('answers a fleet-wide window from the hourly rollup plus the live hour', async () => {
    // It used to read the whole window from node_radio_events on every request:
    // 109 s for 24h and 74 s for 7d on production, against a 30 s timeout.
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/radio-aliases?window=7d');
    expect(res.status).toBe(200);
    const qs = unitQueries(calls);
    expect(qs).toHaveLength(2);
    for (const q of qs) {
      expect(q.sql).toContain('FROM node_radio_hourly_unit');
      // Exact last-heard where the rollup has it, the hour where it predates 125.
      expect(q.sql).toContain('COALESCE(last_heard, hour)');
      // The window is bounded by the seam as a literal, never a subquery.
      expect(q.sql).toContain(`hour < '${SEAM.toISOString()}'::timestamptz`);
      expect(q.sql).not.toContain('FROM node_rollup_state');
      // Detail is read for the live hour only — no window scan.
      expect(q.sql).toContain(`received_at >= '${SEAM.toISOString()}'::timestamptz`);
      expect(q.sql).not.toContain('received_at >= now()');
      expect(q.params).toContain('7 days');
    }
    expect(seamReads).toHaveBeenCalledTimes(1);
  });

  it('lists every radio heard, not only the ones that sent an alias', async () => {
    // It listed node_radio_aliases: 570 of the 6,970 radios heard over 7 days.
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/radio-aliases?window=7d');
    for (const q of unitQueries(calls)) {
      // Rows come from traffic; the alias is joined on where there is one.
      expect(q.sql).toMatch(/\) h LEFT JOIN \(/);
      expect(q.sql).toContain('ON a.radio_id = h.radio_id');
      // Membership is "heard in the window", from the same pass.
      expect(q.sql).toContain('HAVING SUM(heard_times) > 0');
    }
  });

  it('counts receptions of voice calls, as the Units card does', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/radio-aliases?window=24h');
    const [count] = unitQueries(calls);
    expect(count?.sql).toContain('SUM(receptions) FILTER (WHERE hour >=');
    expect(count?.sql).toContain("event_type LIKE 'CALL_GROUP%'");
    expect(count?.sql).toContain('COUNT(DISTINCT (logical_call_id, node_id');
    expect(count?.sql).not.toContain('COUNT(*)::int AS heard_times');
    expect(count?.params).toContain('24 hours');
  });

  it('takes First heard from the whole history, not the window', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/radio-aliases?window=24h&sort=firstSeen');
    const page = unitQueries(calls)[1];
    // Unfiltered MIN over the rollup, then the earlier of that and the alias's.
    expect(page?.sql).toContain('MIN(hour) AS first_heard');
    expect(page?.sql).toContain('LEAST(a.first_seen, h.first_heard) AS first_seen');
    expect(page?.sql).toMatch(/ORDER BY LEAST\(a\.first_seen, h\.first_heard\) DESC NULLS LAST/);
  });

  it('reads detail when scoped to a node, which the rollup cannot answer', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/radio-aliases?window=24h&node=node-1');
    expect(res.status).toBe(200);
    for (const q of unitQueries(calls)) {
      expect(q.sql).toContain('FROM node_radio_events');
      expect(q.sql).toMatch(/node_id = \$\d+/);
      // The call predicate matches idx_nre_node_time_calls.
      expect(q.sql).toContain("event_type LIKE 'CALL_GROUP%'");
      expect(q.params).toContain('node-1');
      // The rollup only supplies the lifetime First heard — never the traffic.
      expect(q.sql).not.toContain('FILTER (WHERE hour >=');
      expect(q.sql).not.toContain('SUM(receptions)');
    }
    expect(seamReads).not.toHaveBeenCalled();
  });

  it('lists each radio once, though the alias table keys it per system', async () => {
    // A radio filed under system 0 and its real system was listed twice.
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/radio-aliases?window=7d');
    for (const q of unitQueries(calls)) {
      // Echoed ids are dropped BEFORE the collapse, so one can't win as the alias.
      expect(q.sql).toMatch(/WHERE alias <> radio_id::text\s+GROUP BY radio_id/);
      expect(q.sql).toContain('ORDER BY (system = 0), last_seen DESC');
    }
  });

  it('reads the rollup\'s whole history for window=all', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/radio-aliases?window=all');
    for (const q of unitQueries(calls)) {
      expect(q.sql).toContain('FROM node_radio_hourly_unit');
      expect(q.sql).toContain(`FILTER (WHERE hour >= '-infinity'::timestamptz)`);
      expect(q.params.some((p) => typeof p === 'string' && /hours|days/.test(p))).toBe(false);
    }
    expect(seamReads).toHaveBeenCalledTimes(1);
  });

  it('searches and filters on the heard radio, which may have no alias row', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/radio-aliases?window=7d&q=0200307&sort=ota&dir=asc');
    const page = unitQueries(calls)[1];
    expect(page?.sql).toContain('h.radio_id::text LIKE $');
    expect(page?.sql).not.toContain('a.radio_id::text LIKE');
    // Unnamed radios after named ones, whichever way OTA is sorted.
    expect(page?.sql).toContain('ORDER BY lower(a.alias) ASC NULLS LAST, h.radio_id ASC');
  });

  it('returns a radio with no alias as ota null', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('AS n')) return { rows: [{ n: 1 }] };
      if (sql.includes('FROM node_radio_aliases')) {
        return {
          rows: [{
            system: null, radio_id: 2319723, alias: null,
            first_seen: LAST_SEEN, last_seen: LAST_SEEN, times_seen: 4,
          }],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/radio-aliases?window=24h');
    const body = (await res.json()) as { total: number; rows: Array<Record<string, unknown>> };
    expect(body.total).toBe(1);
    expect(body.rows[0]).toMatchObject({ radio: 2319723, ota: null, timesSeen: 4 });
  });
});

describe('system and site drill-downs (rollups, migration 126)', () => {
  const T = (iso: string) => new Date(iso);

  it('/site reads the rollups plus the live hour, never a window of detail', async () => {
    // The raw version timed out at 30 s on production for a 7-day site.
    const calls = captureCalls();
    const app = await setupApp();
    const res = await app.request('/api/node-data/site?window=7d&system=721&rfss=4&site=85');
    expect(res.status).toBe(200);
    const reads = calls.filter((c) => c.sql.includes('FROM node_radio_events'));
    expect(reads.length).toBeGreaterThan(0);
    for (const q of reads) {
      // Detail only for the hour in progress, bounded by the seam literal.
      expect(q.sql).toContain(`received_at >= '${SEAM.toISOString()}'::timestamptz`);
      expect(q.sql).not.toContain('received_at >= now()');
      expect(q.sql).toContain('site_rfss = $3 AND site_id = $4');
      expect(q.params).toEqual(['7 days', 721, 4, 85]);
    }
    expect(calls.some((c) => c.sql.includes('FROM node_radio_hourly_sys'))).toBe(true);
    expect(calls.some((c) => c.sql.includes('FROM node_radio_hourly_heard'))).toBe(true);
    expect(seamReads).toHaveBeenCalledTimes(1);
  });

  it('/site counts calls with the per-SITE attribution', async () => {
    // The fleet-wide columns credit a simulcast call to its lowest site, so a
    // site total built from them reads near zero everywhere else.
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/site?window=7d&system=721&rfss=4&site=85');
    const totals = calls.find((c) => c.sql.includes('AS talkgroups'));
    expect(totals?.sql).toContain('logical_calls_site AS logical');
    expect(totals?.sql).toContain('encrypted_calls_site AS enc');
    expect(totals?.sql).toContain('recorded_calls_site AS rec');
  });

  it('/system counts calls once per call, the fleet-wide attribution', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/system?window=30d&system=721');
    const totals = calls.find((c) => c.sql.includes('AS talkgroups'));
    expect(totals?.sql).toContain('logical_calls AS logical');
    expect(totals?.sql).toContain('encrypted_calls AS enc');
    expect(totals?.sql).not.toContain('site_rfss = $3');
    expect(calls.every((c) => !c.sql.includes('received_at >= now() - $1::interval'))).toBe(true);
  });

  it('keeps the raw path when scoped to a node, and reads no seam', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    expect((await app.request('/api/node-data/site?window=24h&system=721&rfss=4&site=85&node=n1')).status).toBe(200);
    expect((await app.request('/api/node-data/system?window=24h&system=721&node=n1')).status).toBe(200);
    expect(calls.some((c) => c.sql.includes('node_radio_hourly'))).toBe(false);
    expect(calls.some((c) => c.sql.includes('node_id = $5'))).toBe(true);
    expect(calls.some((c) => c.sql.includes('node_id = $3'))).toBe(true);
    expect(seamReads).not.toHaveBeenCalled();
  });

  it('/site builds totals, top radios, nodes and exact last-seen from the rollups', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('AS talkgroups')) {
        return { rows: [{ calls: 40, logical: 20, enc: 4, rec: 9, talkgroups: 3, sites: 1 }] };
      }
      if (sql.includes('GROUP BY talkgroup')) {
        return { rows: [{ talkgroup: 10101, calls: 30, logical: 15, enc: 0, last_seen: T('2026-10-14T05:00:00Z') }] };
      }
      if (sql.includes('GROUP BY 1 ORDER BY 1')) {
        return { rows: [{ hour: T('2026-10-14T05:00:00Z'), calls: 40 }] };
      }
      if (sql.includes('GROUPING SETS')) {
        return {
          rows: [
            { g: 7, node_id: 'n1', talkgroup: null, rfss: null, site: null, calls: 40, last_seen: T('2026-10-14T05:41:00Z') },
            { g: 3, node_id: 'n1', talkgroup: 10101, rfss: null, site: null, calls: 30, last_seen: T('2026-10-14T05:41:00Z') },
            { g: 3, node_id: 'n1', talkgroup: 0, rfss: null, site: null, calls: 99, last_seen: T('2026-10-14T05:41:00Z') },
            { g: 11, node_id: null, talkgroup: 10101, rfss: null, site: null, calls: 30, last_seen: T('2026-10-14T05:41:00Z') },
          ],
        };
      }
      if (sql.includes('GROUP BY source_unit')) {
        return { rows: [{ radio: 2000106, calls: 12, last_seen: T('2026-10-14T05:40:00Z'), radios: 57 }] };
      }
      if (sql.includes('FROM nodes WHERE id = ANY')) return { rows: [{ id: 'n1', name: 'Node 1', kind: 'radio' }] };
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/site?window=7d&system=721&rfss=4&site=85');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.totals).toEqual({
      calls: 40, logicalCalls: 20, encryptedCalls: 4, recordedCalls: 9,
      talkgroups: 3, radios: 57, sites: 1, nodes: 1,
    });
    // The rollup's hour is overridden by the moment the heard table kept.
    expect(body.topTalkgroups[0].lastSeen).toBe('2026-10-14T05:41:00.000Z');
    expect(body.topRadios[0]).toMatchObject({ radio: 2000106, calls: 12 });
    // Talkgroup 0 is "none decoded", never a node's top talkgroup.
    expect(body.nodes).toEqual([{
      id: 'n1', name: 'Node 1', kind: 'radio', calls: 40,
      lastSeen: '2026-10-14T05:41:00.000Z',
      topTalkgroup: { talkgroup: 10101, label: 'Sydney Metro 01', calls: 30 },
    }]);
    expect(body.series).toEqual([{ hour: '2026-10-14T05:00:00.000Z', calls: 40 }]);
  });

  it('/system lists a site once: no decode error, no phantom from a channel\'s site list', async () => {
    // Braidwood RD's channel reports its own site (S100) and every corrupted id
    // it has ever decoded (S244, S193 …), all refreshed each minute. S244 also
    // carried one reception. The list showed Braidwood RD several times.
    const fresh = T('2026-10-14T05:59:00Z');
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('FROM st LEFT JOIN tt')) {
        return {
          rows: [
            { rfss: 1, site: 100, nac: null, calls: 7000, logical: 6000, hours: 160, last_seen: fresh, top_tg: 10101, top_tg_calls: 900 },
            { rfss: 1, site: 244, nac: null, calls: 1, logical: 1, hours: 1, last_seen: T('2026-10-09T06:00:00Z'), top_tg: 10101, top_tg_calls: 1 },
            { rfss: 3, site: 117, nac: null, calls: 111, logical: 90, hours: 54, last_seen: fresh, top_tg: 10101, top_tg_calls: 20 },
          ],
        };
      }
      if (sql.includes('AS in_window')) {
        const snap = (site: number, channel: string, cf: number | null, node = 'b0af') => ({
          node_id: node, wacn: 781824, rfss: site === 117 || site === 119 ? 3 : 1, site, nac: 723,
          channel_name: channel, control_frequency_mhz: cf, channel_count: 4, neighbor_count: 2,
          received_at: fresh, in_window: true,
        });
        return {
          rows: [
            snap(100, 'Braidwood RD', 420.3125),
            snap(244, 'Braidwood RD', null),
            snap(193, 'Braidwood RD', null),
            snap(117, 'Gibberagee', null, '0d5a'),
            // A channel carrying no traffic at all: quiet, listed once, by the
            // site whose control channel it has decoded.
            snap(119, 'Moonimba', 419.7, '0d5a'),
            snap(120, 'Moonimba', null, '0d5a'),
          ],
        };
      }
      if (sql.includes('GROUPING SETS')) {
        return {
          rows: [
            { g: 7, node_id: 'b0af', talkgroup: null, rfss: null, site: null, calls: 7001, last_seen: fresh },
            { g: 4, node_id: 'b0af', talkgroup: null, rfss: 1, site: 100, calls: 7000, last_seen: fresh },
            { g: 4, node_id: 'b0af', talkgroup: null, rfss: 1, site: 244, calls: 1, last_seen: fresh },
          ],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/system?window=30d&system=721&wacn=781824');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.sites.map((s: { rfss: number; site: number }) => `R${s.rfss} S${s.site}`))
      .toEqual(['R1 S100', 'R3 S117', 'R3 S119']);
    expect(body.sites[2]).toMatchObject({ name: 'Moonimba', calls: 0, controlFrequencyMhz: 419.7 });
    // The node is credited with the sites the list shows, not the phantom.
    expect(body.nodes[0]).toMatchObject({ id: 'b0af', sites: 1, calls: 7001 });
  });

  it('/systems drops a site or system heard in a single hour, and does not count it', async () => {
    queryMock.mockImplementation((sql: string) => {
      if (sql.includes('MIN(first_seen) AS first_seen')) {
        return {
          rows: [
            { system: 721, calls: 9000, logical: 6000, enc: 0, sites: 3, talkgroups: 40, hours: 168, first_seen: LAST_SEEN, last_seen: LAST_SEEN },
            // One corrupted reception made a whole second system.
            { system: 2201, calls: 1, logical: 1, enc: 0, sites: 1, talkgroups: 1, hours: 1, first_seen: LAST_SEEN, last_seen: LAST_SEEN },
          ],
        };
      }
      if (sql.includes('GROUP BY system, rfss, site')) {
        return {
          rows: [
            { system: 721, rfss: 1, site: 100, calls: 7000, logical: 6000, hours: 160, last_seen: LAST_SEEN },
            { system: 721, rfss: 3, site: 117, calls: 111, logical: 90, hours: 54, last_seen: LAST_SEEN },
            { system: 721, rfss: 1, site: 244, calls: 1, logical: 1, hours: 1, last_seen: LAST_SEEN },
            { system: 2201, rfss: 4, site: 32, calls: 1, logical: 1, hours: 1, last_seen: LAST_SEEN },
          ],
        };
      }
      return { rows: [] };
    });
    const app = await setupApp();
    const res = await app.request('/api/node-data/systems?window=7d');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.systems.map((s: { system: number }) => s.system)).toEqual([721]);
    expect(body.systems[0].siteCount).toBe(2);
    expect(body.systems[0].sites.map((s: { site: number }) => s.site)).toEqual([100, 117]);
  });

  it('/systems counts the hours each row was heard in, on both paths', async () => {
    const calls = captureCalls();
    const app = await setupApp();
    await app.request('/api/node-data/systems?window=7d');
    await app.request('/api/node-data/systems?window=7d&node=n1');
    const withHours = calls.filter((c) => c.sql.includes("COUNT(DISTINCT date_trunc('hour',"));
    // Rollup: system + sites. Raw: system + sites.
    expect(withHours).toHaveLength(4);
  });
});
