/**
 * The pure primitives of the node-event query builder.
 *
 * These emitters are the single definition of a Reception, a Logical call, the
 * predicate gates, the Window map and the Rollup-edge seam that the staff Data
 * tab's radio/pager routes all build on. They are pure strings, so — exactly as
 * services/dataHistoryQuery.test.ts does for the history builder — they are tested
 * by asserting the emitted SQL with no database. When a route is migrated onto
 * these emitters, its own golden test asserts the route reproduces today's SQL;
 * this suite pins the emitters themselves.
 */
import { describe, it, expect } from 'vitest';
import {
  WINDOW_INTERVAL,
  TG_MIN,
  TG_MAX,
  tgValid,
  TG_VALID,
  ridValid,
  RID_VALID,
  callGroup,
  CALL_GROUP,
  rollupHours,
  liveHour,
  rollupEdgeSql,
  outcomeSql,
  receptionCount,
  logicalCount,
  LOGICAL_COUNT,
  newAcc,
  pushEq,
  pushIn,
  whereClause,
  TALKGROUPS_ORDER_COL,
  buildTalkgroupsRaw,
  buildTalkgroupsRollup,
  type TalkgroupsQuery,
  heardWindowSql,
  buildRadiosRaw,
  buildRadiosRollup,
  type RadiosQuery,
  buildOverviewTotals,
  buildOverviewPerNode,
  buildOverviewTopTalkgroups,
  buildOverviewTopUnits,
  buildOverviewTopSites,
  buildOverviewSeries,
  overviewTotalsRollupSql,
  overviewTopTalkgroupsRollupSql,
  overviewTopSitesRollupSql,
  overviewTopUnitsRollupSql,
  overviewSeriesRollupSql,
  overviewPerNodeRollupSql,
} from '../../../src/services/nodeEventsQuery.js';

describe('WINDOW_INTERVAL', () => {
  it('maps each non-all window to its Postgres interval', () => {
    expect(WINDOW_INTERVAL).toEqual({
      '24h': '24 hours',
      '7d': '7 days',
      '30d': '30 days',
    });
  });
});

describe('tgValid', () => {
  it('bare form uses the allocated band', () => {
    expect(tgValid()).toBe('talkgroup BETWEEN 10000 AND 30500');
    expect(TG_VALID).toBe('talkgroup BETWEEN 10000 AND 30500');
    expect([TG_MIN, TG_MAX]).toEqual([10000, 30500]);
  });
  it('prefixes the column for aliased tables', () => {
    expect(tgValid('e.')).toBe('e.talkgroup BETWEEN 10000 AND 30500');
  });
});

describe('ridValid', () => {
  it('bare and prefixed forms', () => {
    expect(ridValid()).toBe('source_unit BETWEEN 100000 AND 9999999');
    expect(RID_VALID).toBe('source_unit BETWEEN 100000 AND 9999999');
    expect(ridValid('e.')).toBe('e.source_unit BETWEEN 100000 AND 9999999');
  });
});

describe('callGroup', () => {
  it('matches CALL_GROUP% and CALL_PATCH_GROUP% directly', () => {
    expect(callGroup()).toBe(
      "(event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%')",
    );
    expect(CALL_GROUP).toBe(callGroup());
  });
  it('prefixes both branches', () => {
    expect(callGroup('e.')).toBe(
      "(e.event_type LIKE 'CALL_GROUP%' OR e.event_type LIKE 'CALL_PATCH_GROUP%')",
    );
  });
});

describe('rollup/live seam', () => {
  const SEAM = "'2026-10-14T06:00:00.000Z'::timestamptz";
  it('rollupHours windows back by $1 and stops at the seam', () => {
    expect(rollupHours(SEAM)).toBe(
      `hour >= date_trunc('hour', now()) - $1::interval AND hour < ${SEAM}`,
    );
  });
  it('liveHour reads detail from the seam forward', () => {
    expect(liveHour(SEAM)).toBe(`received_at >= ${SEAM}`);
  });
  it('rollupEdgeSql renders a Date as an injection-safe timestamptz literal', () => {
    expect(rollupEdgeSql(new Date('2026-10-14T06:00:00.000Z'))).toBe(SEAM);
  });
});

describe('outcomeSql', () => {
  it('builds the enc / no-tgid predicates over a home column', () => {
    const { isEnc, isNoTgid } = outcomeSql('home_talkgroup', '$2', '$3');
    expect(isEnc).toBe('home_talkgroup = ANY($2::int[])');
    expect(isNoTgid).toBe(
      'cardinality($3::int[]) > 0 AND home_talkgroup <> ALL($3::int[])',
    );
  });
});

describe('receptionCount — one definition of a Reception', () => {
  it('counts distinct (logical call, node, site) tuples, bare', () => {
    expect(receptionCount()).toBe(
      'COUNT(DISTINCT (logical_call_id, node_id,' +
        ' COALESCE(site_rfss, -1), COALESCE(site_id, -1)))::int',
    );
  });
  it('prefixes every column for an aliased table', () => {
    expect(receptionCount('e.')).toBe(
      'COUNT(DISTINCT (e.logical_call_id, e.node_id,' +
        ' COALESCE(e.site_rfss, -1), COALESCE(e.site_id, -1)))::int',
    );
  });
});

describe('logicalCount — one definition of a Logical call', () => {
  it('counts distinct transmissions, bare and prefixed', () => {
    expect(logicalCount()).toBe('COUNT(DISTINCT logical_call_id)::int');
    expect(LOGICAL_COUNT).toBe(logicalCount());
    expect(logicalCount('e.')).toBe('COUNT(DISTINCT e.logical_call_id)::int');
  });
});

describe('ConditionAcc', () => {
  it('carries initial params and numbers placeholders 1-based on params.length', () => {
    const acc = newAcc(['24 hours']);
    pushEq(acc, 'system', 42);
    expect(acc.parts).toEqual(['system = $2']);
    expect(acc.params).toEqual(['24 hours', 42]);
  });
  it('pushIn emits equality for one value and IN for many', () => {
    const one = newAcc();
    pushIn(one, 'talkgroup', [100]);
    expect(one.parts).toEqual(['talkgroup = $1']);
    expect(one.params).toEqual([100]);

    const many = newAcc(['x']);
    pushIn(many, 'talkgroup', [100, 200, 300]);
    expect(many.parts).toEqual(['talkgroup IN ($2,$3,$4)']);
    expect(many.params).toEqual(['x', 100, 200, 300]);
  });
  it('whereClause joins with AND, or is empty', () => {
    expect(whereClause(newAcc())).toBe('');
    const acc = newAcc();
    pushEq(acc, 'system', 1);
    pushEq(acc, 'node_id', 'n1');
    expect(whereClause(acc)).toBe('WHERE system = $1 AND node_id = $2');
  });
});

// ---------------------------------------------------------------------------
// Talkgroups builders — the golden parity suite. These assert the exact SQL and
// param arrays the route emitted before the lift, so the refactor provably
// changed behaviour nowhere. Because prod cannot be exercised from dev, this is
// the no-database guarantee.
// ---------------------------------------------------------------------------

/** A query with every filter off; each test overrides only what it exercises. */
const baseTgQuery: TalkgroupsQuery = {
  windowInterval: '7 days',
  system: null,
  rfss: null,
  site: null,
  nodeId: null,
  qRaw: '',
  encHide: false,
  encTgs: [],
  agencyIds: null,
  tgConfiguredSql: 'TRUE',
  orderCol: 'calls',
  dir: 'DESC',
  limit: 50,
  offset: 0,
};

describe('TALKGROUPS_ORDER_COL', () => {
  it('maps each whitelisted sort key to its grouped order expression', () => {
    expect(TALKGROUPS_ORDER_COL).toEqual({
      calls: 'calls',
      lastSeen: 'last_seen',
      talkgroup: 'talkgroup',
      enc: 'enc',
      logical: 'logical',
    });
  });
});

describe('buildTalkgroupsRaw — node-scoped two-level scan', () => {
  it('minimal node query: interval + node, no HAVING, limit/offset trail the params', () => {
    const b = buildTalkgroupsRaw({ ...baseTgQuery, nodeId: 'node-1' });
    const where =
      'received_at >= now() - $1::interval'
      + ` AND ${CALL_GROUP} AND ${TG_VALID}`
      + ' AND node_id = $2 AND TRUE';
    expect(b.countParams).toEqual(['7 days', 'node-1']);
    expect(b.pageParams).toEqual(['7 days', 'node-1', 50, 0]);
    // countHaving is empty here, leaving a trailing space after `talkgroup`.
    expect(b.countSql).toBe(
      'SELECT COUNT(*)::int AS n FROM (\n'
      + `  SELECT 1 FROM node_radio_events WHERE ${where}\n`
      + '   GROUP BY wacn, system, talkgroup \n'
      + ') g',
    );
    expect(b.pageSql).toBe(
      'SELECT wacn, system, talkgroup,\n'
      + '       SUM(g.calls)::int AS calls,\n'
      + '       COUNT(*)::int AS logical,\n'
      + '       SUM(g.enc)::int AS enc,\n'
      + '       MAX(g.last_seen) AS last_seen\n'
      + '  FROM (\n'
      + '    SELECT wacn, system, talkgroup, logical_call_id,\n'
      + '           COUNT(*)::int AS calls,\n'
      + '           (COUNT(*) FILTER (WHERE encrypted))::int AS enc,\n'
      + '           MAX(received_at) AS last_seen\n'
      + '      FROM node_radio_events\n'
      + `     WHERE ${where}\n`
      + '     GROUP BY wacn, system, talkgroup, logical_call_id\n'
      + '  ) g\n'
      + ' GROUP BY wacn, system, talkgroup \n'
      + ' ORDER BY calls DESC, talkgroup ASC\n'
      + ' LIMIT $3 OFFSET $4',
    );
  });

  it('full-filter node query numbers every placeholder in source order', () => {
    const b = buildTalkgroupsRaw({
      ...baseTgQuery,
      system: 10,
      rfss: 1,
      site: 2,
      nodeId: 'node-9',
      qRaw: '123',
      encHide: true,
      encTgs: [999, 1000],
      agencyIds: [20001, 20002],
      tgConfiguredSql: '(talkgroup BETWEEN 10000 AND 30500 OR talkgroup = ANY($0::int[]))',
      orderCol: 'last_seen',
      dir: 'ASC',
      limit: 25,
      offset: 75,
    });
    // $1 interval, $2 system, $3 rfss, $4 site, $5 node, $6 q, $7 encTgs, $8 agency
    expect(b.countParams).toEqual([
      '7 days', 10, 1, 2, 'node-9', '123%', [999, 1000], [20001, 20002],
    ]);
    expect(b.pageParams).toEqual([
      '7 days', 10, 1, 2, 'node-9', '123%', [999, 1000], [20001, 20002], 25, 75,
    ]);
    expect(b.countParams).toHaveLength(8);
    // enc=hide → per-group HAVING on both count and page; limit/offset are $9/$10
    expect(b.countSql).toContain('HAVING COUNT(*) FILTER (WHERE encrypted) < COUNT(*)');
    expect(b.pageSql).toContain('HAVING SUM(g.enc) < SUM(g.calls)');
    expect(b.pageSql).toContain('ORDER BY last_seen ASC, talkgroup ASC');
    expect(b.pageSql).toContain('LIMIT $9 OFFSET $10');
    expect(b.countSql).toContain('system = $2');
    expect(b.countSql).toContain('site_rfss = $3 AND site_id = $4');
    expect(b.countSql).toContain('node_id = $5');
    expect(b.countSql).toContain('talkgroup::text LIKE $6');
    expect(b.countSql).toContain('talkgroup <> ALL($7::int[])');
    expect(b.countSql).toContain('talkgroup = ANY($8::int[])');
  });

  it('agencyIds=[] still emits the ANY gate (matches nothing), null omits it', () => {
    const withEmpty = buildTalkgroupsRaw({ ...baseTgQuery, nodeId: 'n', agencyIds: [] });
    expect(withEmpty.countSql).toContain('talkgroup = ANY($3::int[])');
    expect(withEmpty.countParams).toEqual(['7 days', 'n', []]);
    const withNull = buildTalkgroupsRaw({ ...baseTgQuery, nodeId: 'n', agencyIds: null });
    expect(withNull.countSql).not.toContain('ANY(');
  });

  it('encHide with an empty encTgs set skips the <> ALL gate', () => {
    const b = buildTalkgroupsRaw({ ...baseTgQuery, nodeId: 'n', encHide: true, encTgs: [] });
    expect(b.countSql).not.toContain('<> ALL');
    // but the HAVING still applies — enc=hide is about the aggregate, not the id set
    expect(b.pageSql).toContain('HAVING SUM(g.enc) < SUM(g.calls)');
  });
});

describe('buildTalkgroupsRollup — fleet-wide across the Rollup edge', () => {
  const SEAM = rollupEdgeSql(new Date('2026-10-14T06:00:00.000Z'));

  it('minimal fleet query: both halves share $1 interval, page trails limit/offset', () => {
    const b = buildTalkgroupsRollup({ ...baseTgQuery }, SEAM);
    expect(b.countParams).toEqual(['7 days']);
    expect(b.pageParams).toEqual(['7 days', 50, 0]);
    // completed hours from the rollup, live hour from detail, unioned
    expect(b.countSql).toContain('FROM node_radio_hourly_sys');
    expect(b.countSql).toContain('UNION ALL');
    expect(b.countSql).toContain('FROM node_radio_events');
    expect(b.countSql).toContain(rollupHours(SEAM));
    expect(b.countSql).toContain(liveHour(SEAM));
    // no row filters → the hourly half ends right after the talkgroup<>0 guard
    expect(b.countSql).toContain('WHERE ' + rollupHours(SEAM) + ' AND talkgroup <> 0\n');
    expect(b.pageSql).toContain('LIMIT $2 OFFSET $3');
    expect(b.pageSql).toContain('ORDER BY calls DESC, talkgroup ASC');
  });

  it('row filters apply to both union halves; talkgroup filters apply once on the union', () => {
    const b = buildTalkgroupsRollup({
      ...baseTgQuery,
      system: 10,
      rfss: 1,
      site: 2,
      qRaw: '12',
      encHide: true,
      encTgs: [5],
      agencyIds: [20001],
      orderCol: 'enc',
      dir: 'DESC',
    }, SEAM);
    // rp order: $1 interval, $2 system, $3 rfss, $4 site (rowConds),
    //           then $5 q, $6 encTgs, $7 agency (tgConds)
    expect(b.countParams).toEqual(['7 days', 10, 1, 2, '12%', [5], [20001]]);
    expect(b.pageParams).toEqual(['7 days', 10, 1, 2, '12%', [5], [20001], 50, 0]);
    // row filters land in the hourly half's WHERE and the live half's WHERE
    expect(b.countSql).toContain('talkgroup <> 0 AND system = $2 AND site_rfss = $3 AND site_id = $4');
    expect(b.countSql).toContain('talkgroup IS NOT NULL AND system = $2 AND site_rfss = $3 AND site_id = $4');
    // talkgroup filters apply on r, once
    expect(b.countSql).toContain('talkgroup::text LIKE $5');
    expect(b.countSql).toContain('talkgroup <> ALL($6::int[])');
    expect(b.countSql).toContain('talkgroup = ANY($7::int[])');
    expect(b.countSql).toContain('HAVING SUM(enc) < SUM(logical)');
    expect(b.pageSql).toContain('LIMIT $8 OFFSET $9');
  });

  it('each non-all Window flows straight through as the interval literal', () => {
    for (const w of ['24h', '7d', '30d'] as const) {
      const b = buildTalkgroupsRollup({ ...baseTgQuery, windowInterval: WINDOW_INTERVAL[w] }, SEAM);
      expect(b.countParams[0]).toBe(WINDOW_INTERVAL[w]);
    }
  });
});

describe('heardWindowSql — the fleet-wide rollup+live union emitter', () => {
  const SEAM = rollupEdgeSql(new Date('2026-10-14T06:00:00.000Z'));
  it('unions the hourly rollup below the seam with the folded live hour, where on both', () => {
    const sql = heardWindowSql(SEAM, 'system = $2');
    expect(sql).toContain('FROM node_radio_hourly_heard');
    expect(sql).toContain(`WHERE ${rollupHours(SEAM)} AND system = $2`);
    expect(sql).toContain('UNION ALL');
    // live half folds one reception per (logical call, node, site, talkgroup)
    expect(sql).toContain(`WHERE ${liveHour(SEAM)} AND ${CALL_GROUP}`);
    expect(sql).toContain('GROUP BY logical_call_id, node_id, 3, 4, 5, 6');
    // the where is re-applied after the live fold, then grouped to the grain
    expect(sql).toContain('     WHERE system = $2\n     GROUP BY 1, 2, 3, 4, 5, 6, 7');
  });
});

// ---------------------------------------------------------------------------
// Radios builders — golden parity, the twin of the talkgroups suite.
// ---------------------------------------------------------------------------

const baseRadQuery: RadiosQuery = {
  windowInterval: '7 days',
  system: null,
  rfss: null,
  site: null,
  nodeId: null,
  qRaw: '',
  unitIds: null,
  namedOnly: false,
  sort: 'calls',
  dir: 'DESC',
  limit: 50,
  offset: 0,
};

describe('buildRadiosRaw — node-scoped grouped scan', () => {
  it('minimal node query: COUNT(DISTINCT row) count, alias array_agg page', () => {
    const b = buildRadiosRaw({ ...baseRadQuery, nodeId: 'node-1' });
    const where =
      'received_at >= now() - $1::interval'
      + ` AND ${CALL_GROUP} AND ${RID_VALID}`
      + ' AND node_id = $2';
    expect(b.countParams).toEqual(['7 days', 'node-1']);
    expect(b.pageParams).toEqual(['7 days', 'node-1', 50, 0]);
    expect(b.countSql).toBe(
      'SELECT COUNT(DISTINCT (wacn, system, source_unit))::int AS n\n'
      + `  FROM node_radio_events WHERE ${where}`,
    );
    expect(b.pageSql).toContain('source_unit AS radio');
    expect(b.pageSql).toContain('ORDER BY calls DESC, radio ASC');
    expect(b.pageSql).toContain('LIMIT $3 OFFSET $4');
    // no named-only → no HAVING
    expect(b.pageSql).not.toContain('HAVING');
  });

  it('named-only switches the count to group-then-count and adds the HAVING', () => {
    const b = buildRadiosRaw({ ...baseRadQuery, nodeId: 'n', namedOnly: true });
    expect(b.countSql).toContain('GROUP BY wacn, system, source_unit HAVING COUNT(source_alias) > 0');
    expect(b.pageSql).toContain('HAVING COUNT(source_alias) > 0');
  });

  it('sort=ota orders on the alias aggregate with NULLS LAST', () => {
    const b = buildRadiosRaw({ ...baseRadQuery, nodeId: 'n', sort: 'ota' });
    expect(b.pageSql).toContain('ORDER BY alias DESC NULLS LAST, radio ASC');
  });

  it('full filters number placeholders in source order; unitIds is a bigint[]', () => {
    const b = buildRadiosRaw({
      ...baseRadQuery,
      system: 10,
      rfss: 1,
      site: 2,
      nodeId: 'node-9',
      qRaw: '200',
      unitIds: [500123, 500124],
      sort: 'lastSeen',
      dir: 'ASC',
      limit: 25,
      offset: 50,
    });
    // $1 interval, $2 system, $3 rfss, $4 site, $5 node, $6 q, $7 unitIds
    expect(b.countParams).toEqual(['7 days', 10, 1, 2, 'node-9', '200%', [500123, 500124]]);
    expect(b.pageParams).toEqual(['7 days', 10, 1, 2, 'node-9', '200%', [500123, 500124], 25, 50]);
    expect(b.countSql).toContain('source_unit = ANY($7::bigint[])');
    expect(b.pageSql).toContain('source_unit::text LIKE $6');
    expect(b.pageSql).toContain('ORDER BY last_seen ASC, radio ASC');
    expect(b.pageSql).toContain('LIMIT $8 OFFSET $9');
  });
});

describe('buildRadiosRollup — fleet-wide across the Rollup edge', () => {
  const SEAM = rollupEdgeSql(new Date('2026-10-14T06:00:00.000Z'));

  it('minimal fleet query joins the lifetime OTA alias, orders on g.calls', () => {
    const b = buildRadiosRollup({ ...baseRadQuery }, SEAM);
    expect(b.countParams).toEqual(['7 days']);
    expect(b.pageParams).toEqual(['7 days', 50, 0]);
    expect(b.countSql).toContain('WITH h AS (');
    expect(b.countSql).toContain('SELECT COUNT(*)::int AS n FROM (SELECT system, source_unit AS radio');
    expect(b.pageSql).toContain('FROM node_radio_aliases');
    expect(b.pageSql).toContain('ORDER BY g.calls DESC, g.radio ASC');
    expect(b.pageSql).toContain('LIMIT $2 OFFSET $3');
    // not named-only → the alias LEFT JOIN is not filtered to non-null
    expect(b.pageSql).not.toContain('WHERE a.alias IS NOT NULL');
  });

  it('row/unit filters land inside the heard union; named-only filters the alias join', () => {
    const b = buildRadiosRollup({
      ...baseRadQuery,
      system: 10,
      rfss: 1,
      site: 2,
      qRaw: '200',
      unitIds: [500123],
      namedOnly: true,
      sort: 'ota',
    }, SEAM);
    // rp: $1 interval, $2 system, $3 rfss, $4 site, $5 q, $6 unitIds
    expect(b.countParams).toEqual(['7 days', 10, 1, 2, '200%', [500123]]);
    expect(b.pageParams).toEqual(['7 days', 10, 1, 2, '200%', [500123], 50, 0]);
    expect(b.countSql).toContain('system = $2 AND site_rfss = $3 AND site_id = $4');
    expect(b.countSql).toContain('source_unit::text LIKE $5');
    expect(b.countSql).toContain('source_unit = ANY($6::bigint[])');
    expect(b.pageSql).toContain('a ON a.radio_id = g.radio WHERE a.alias IS NOT NULL');
    expect(b.pageSql).toContain('ORDER BY a.alias DESC NULLS LAST, g.radio ASC');
    expect(b.pageSql).toContain('LIMIT $7 OFFSET $8');
  });
});

// ---------------------------------------------------------------------------
// Overview route (batch 2). The detail builders unify the windowed and
// window=all code paths: 24h/7d/30d bind the interval as $1 and bucket the
// series by hour; `all` inlines the 30-day retention cap as a literal floor and
// buckets by day. The optional node scope threads in as the next $N. The rollup
// emitters split on the Rollup-edge seam literal. These pin the SQL the overview
// route used to inline, so the migration provably changed behaviour nowhere.
// ---------------------------------------------------------------------------

describe('overview detail builders', () => {
  it('buildOverviewPerNode: windowed binds $1; all inlines the 30-day cap + node $1', () => {
    expect(buildOverviewPerNode('7d', null)).toEqual({
      sql: `SELECT node_id, COUNT(*)::int AS calls\n     FROM node_radio_events\n    WHERE received_at >= now() - $1::interval AND ${CALL_GROUP} GROUP BY node_id`,
      params: ['7 days'],
    });
    expect(buildOverviewPerNode('all', 'n1')).toEqual({
      sql: `SELECT node_id, COUNT(*)::int AS calls\n     FROM node_radio_events\n    WHERE received_at >= now() - interval '30 days' AND node_id = $1 AND ${CALL_GROUP} GROUP BY node_id`,
      params: ['n1'],
    });
  });

  it('buildOverviewTopTalkgroups: node scope is $2, TG_VALID gate, LIMIT 15', () => {
    expect(buildOverviewTopTalkgroups('7d', 'n1')).toEqual({
      sql:
        `SELECT system, talkgroup,\n` +
        `          COUNT(DISTINCT (logical_call_id, node_id,\n` +
        `            COALESCE(site_rfss, -1), COALESCE(site_id, -1)))::int AS receptions,\n` +
        `          COUNT(DISTINCT logical_call_id)::int AS logical,\n` +
        `          NULL::text AS label\n` +
        `     FROM node_radio_events\n` +
        `    WHERE received_at >= now() - $1::interval AND node_id = $2 AND ${CALL_GROUP} AND ${TG_VALID}\n` +
        `    GROUP BY system, talkgroup\n` +
        `    ORDER BY receptions DESC LIMIT 15`,
      params: ['7 days', 'n1'],
    });
  });

  it('buildOverviewTopUnits: all path uses the 30-day floor + RID_VALID, no params', () => {
    expect(buildOverviewTopUnits('all', null)).toEqual({
      sql:
        `SELECT source_unit AS unit,\n` +
        `          (array_agg(source_alias ORDER BY received_at DESC)\n` +
        `             FILTER (WHERE source_alias IS NOT NULL AND source_alias <> source_unit::text))[1] AS alias,\n` +
        `          COUNT(DISTINCT (logical_call_id, node_id,\n` +
        `            COALESCE(site_rfss, -1), COALESCE(site_id, -1), talkgroup))::int AS receptions\n` +
        `     FROM node_radio_events\n` +
        `    WHERE received_at >= now() - interval '30 days' AND ${CALL_GROUP} AND ${RID_VALID}\n` +
        `    GROUP BY source_unit ORDER BY receptions DESC LIMIT 15`,
      params: [],
    });
  });

  it('buildOverviewTopSites: site-not-null guard, LIMIT 15', () => {
    expect(buildOverviewTopSites('7d', null)).toEqual({
      sql:
        `SELECT site_rfss, site_id,\n` +
        `          COUNT(DISTINCT (logical_call_id, node_id, talkgroup))::int AS receptions\n` +
        `     FROM node_radio_events\n` +
        `    WHERE received_at >= now() - $1::interval AND ${CALL_GROUP} AND site_rfss IS NOT NULL AND site_id IS NOT NULL\n` +
        `    GROUP BY site_rfss, site_id ORDER BY receptions DESC LIMIT 15`,
      params: ['7 days'],
    });
  });

  it('buildOverviewSeries: hour bucket windowed, day bucket for all', () => {
    expect(buildOverviewSeries('7d', null).sql).toBe(
      `SELECT date_trunc('hour', received_at) AS bucket, COUNT(*)::int AS n\n     FROM node_radio_events\n    WHERE received_at >= now() - $1::interval AND ${CALL_GROUP} GROUP BY 1 ORDER BY 1`,
    );
    expect(buildOverviewSeries('all', null).sql).toBe(
      `SELECT date_trunc('day', received_at) AS bucket, COUNT(*)::int AS n\n     FROM node_radio_events\n    WHERE received_at >= now() - interval '30 days' AND ${CALL_GROUP} GROUP BY 1 ORDER BY 1`,
    );
  });

  it('buildOverviewTotals: param order + enc/prog placeholder numbering per path', () => {
    // windowed fleet: $1 interval, enc $2, prog $3
    const wf = buildOverviewTotals('7d', null, [1, 2], [3], false);
    expect(wf.params).toEqual(['7 days', [1, 2], [3]]);
    expect(wf.sql).toContain('received_at >= now() - $1::interval AND ' + CALL_GROUP);
    expect(wf.sql).toContain('t.home = ANY($2::int[])');
    expect(wf.sql).toContain('cardinality($3::int[]) > 0');
    expect(wf.sql).not.toContain(`WHERE outcome <> 'enc'`);
    // windowed node + hideEnc: $1 interval, $2 node, enc $3, prog $4, enc suffix
    const wn = buildOverviewTotals('7d', 'n1', [1, 2], [3], true);
    expect(wn.params).toEqual(['7 days', 'n1', [1, 2], [3]]);
    expect(wn.sql).toContain('node_id = $2 AND ' + CALL_GROUP);
    expect(wn.sql).toContain('t.home = ANY($3::int[])');
    expect(wn.sql.endsWith(`FROM k WHERE outcome <> 'enc'`)).toBe(true);
    // all fleet: no interval param, enc $1, prog $2
    const af = buildOverviewTotals('all', null, [1, 2], [3], false);
    expect(af.params).toEqual([[1, 2], [3]]);
    expect(af.sql).toContain(`received_at >= now() - interval '30 days' AND ` + CALL_GROUP);
    expect(af.sql).toContain('t.home = ANY($1::int[])');
    // all node: $1 node, enc $2, prog $3
    const an = buildOverviewTotals('all', 'n1', [1, 2], [3], false);
    expect(an.params).toEqual(['n1', [1, 2], [3]]);
    expect(an.sql).toContain(`received_at >= now() - interval '30 days' AND node_id = $1`);
    expect(an.sql).toContain('t.home = ANY($2::int[])');
  });
});

describe('overview rollup emitters', () => {
  const SEAM = rollupEdgeSql(new Date('2026-01-01T00:00:00.000Z'));

  it('overviewTopTalkgroupsRollupSql: completed hours UNION the live hour, split on the seam', () => {
    expect(overviewTopTalkgroupsRollupSql(SEAM)).toBe(
      `WITH r AS (\n` +
        `         SELECT system, talkgroup, receptions, logical_calls_tg AS logical\n` +
        `           FROM node_radio_hourly_sys\n` +
        `          WHERE ${rollupHours(SEAM)} AND ${TG_VALID}\n` +
        `         UNION ALL\n` +
        `         SELECT COALESCE(system, 0), talkgroup,\n` +
        `                COUNT(DISTINCT (logical_call_id, node_id,\n` +
        `                  COALESCE(site_rfss, -1), COALESCE(site_id, -1)))::int,\n` +
        `                COUNT(DISTINCT logical_call_id)::int\n` +
        `           FROM node_radio_events\n` +
        `          WHERE ${liveHour(SEAM)} AND ${CALL_GROUP} AND ${TG_VALID}\n` +
        `          GROUP BY 1, 2\n` +
        `       )\n` +
        `       SELECT NULLIF(system, 0) AS system, talkgroup,\n` +
        `              SUM(receptions)::int AS receptions,\n` +
        `              SUM(logical)::int AS logical,\n` +
        `              NULL::text AS label\n` +
        `         FROM r GROUP BY r.system, r.talkgroup\n` +
        `        ORDER BY receptions DESC LIMIT 15`,
    );
  });

  it('overviewSeriesRollupSql: bucket literal flows into both halves', () => {
    expect(overviewSeriesRollupSql(SEAM, 'day')).toContain(`SELECT date_trunc('day', hour) AS bucket, calls AS n`);
    expect(overviewSeriesRollupSql(SEAM, 'day')).toContain(`SELECT date_trunc('day', now()), COUNT(*)::int`);
    expect(overviewSeriesRollupSql(SEAM, 'hour')).toContain(`date_trunc('hour', hour)`);
    expect(overviewSeriesRollupSql(SEAM, 'day')).toContain(rollupHours(SEAM));
    expect(overviewSeriesRollupSql(SEAM, 'day')).toContain(liveHour(SEAM));
  });

  it('overviewTotalsRollupSql: $1 interval + seam window; hideEnc adds the NOT-enc filter', () => {
    const plain = overviewTotalsRollupSql(SEAM, false);
    expect(plain).toContain('FROM node_radio_hourly_rx');
    expect(plain).toContain(`WHERE hour >= date_trunc('hour', now()) - $1::interval`);
    expect(plain).toContain(`AND hour <  ${SEAM}`);
    expect(plain).not.toContain('AND NOT (home_talkgroup = ANY($2::int[]))');
    expect(overviewTotalsRollupSql(SEAM, true)).toContain('AND NOT (home_talkgroup = ANY($2::int[]))');
  });

  it('overviewTopSitesRollupSql / overviewTopUnitsRollupSql / overviewPerNodeRollupSql: seam-split unions', () => {
    expect(overviewTopSitesRollupSql(SEAM)).toContain('FROM node_radio_hourly_sys');
    expect(overviewTopSitesRollupSql(SEAM)).toContain('site_rfss <> -1 AND site_id <> -1');
    expect(overviewTopUnitsRollupSql(SEAM)).toContain('FROM node_radio_hourly_unit');
    expect(overviewTopUnitsRollupSql(SEAM)).toContain(RID_VALID);
    expect(overviewPerNodeRollupSql(SEAM)).toContain('FROM node_radio_hourly WHERE ' + rollupHours(SEAM));
    expect(overviewPerNodeRollupSql(SEAM)).toContain('SELECT node_id, SUM(calls)::int AS calls');
  });
});
