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
