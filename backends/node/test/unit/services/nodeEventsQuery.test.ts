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
