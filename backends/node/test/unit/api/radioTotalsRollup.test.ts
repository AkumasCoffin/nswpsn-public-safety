/**
 * The overview tiles, sourced from the hourly rollup.
 *
 * /api/node-data/overview read ~1.75M detail rows to produce eight integers,
 * at a cost linear in the window (~1.7s per day measured), so 30d exceeded the
 * 30s statement timeout. The tiles now sum node_radio_hourly_rx — tens of rows
 * an hour — and union the hour still in progress from detail.
 *
 * The SQL equivalence (does the rollup define a reception the same way
 * radioReceptionsSql does?) cannot be tested here: this suite mocks the pool
 * and there is no Postgres to run either statement against. It is verified
 * instead against production, where the 24h window already works from detail
 * and must report the same figures once it comes from the rollup.
 *
 * What IS testable here is everything on the TypeScript side of that boundary,
 * and it is where the quiet mistakes live: stitching two result sets together,
 * the residual that keeps the buckets a partition, and refusing to answer a
 * node-scoped request from a fleet-wide table.
 */
import { describe, it, expect, vi } from 'vitest';
import {
  outcomeSql,
  radioTotalsFromRollup,
  resolveRollupEdge,
  rollupEdgeSql,
} from '../../../src/api/node-data.js';

/** The seam the handler resolved, as resolveRollupEdge() would return it. */
const EDGE = new Date('2026-10-14T06:00:00.000Z');
const EDGE_SQL = "'2026-10-14T06:00:00.000Z'::timestamptz";

/** A pool whose two queries answer with the given rows, in call order. */
function poolOf(rollupRow: unknown, partialRow: unknown) {
  const query = vi.fn();
  query.mockResolvedValueOnce({ rows: rollupRow === null ? [] : [rollupRow] });
  query.mockResolvedValueOnce({ rows: partialRow === null ? [] : [partialRow] });
  return { pool: { query } as never, query };
}

const ZERO = {
  received: 0, transmissions: 0, ingested: 0, no_tgid: 0,
  enc: 0, not_recorded: 0, dropped_patch: 0, dropped_site: 0,
};

describe('outcomeSql', () => {
  it('keeps the cardinality guard, which is what stops rdio being unreachable from condemning the network', () => {
    // An EMPTY programmed list means "we could not ask", not "nothing is
    // programmed". Without the guard every talkgroup fails the <> ALL test and
    // the whole network reads as no_tgid.
    const { isNoTgid } = outcomeSql('home_talkgroup', '$2', '$3');
    expect(isNoTgid).toContain('cardinality($3::int[]) > 0');
    expect(isNoTgid).toContain('<> ALL($3::int[])');
  });

  it('tests encryption against the declared list, not a per-event flag', () => {
    // The decoder flag is not set at the instant a call starts, so an
    // always-encrypted talkgroup still emits rows flagged false — measured, it
    // misses 21.6% of encrypted police traffic.
    const { isEnc } = outcomeSql('home_talkgroup', '$2', '$3');
    expect(isEnc).toBe('home_talkgroup = ANY($2::int[])');
  });
});

describe('radioTotalsFromRollup', () => {
  it('adds the hour in progress to the completed hours', async () => {
    const { pool } = poolOf(
      { received: 100, transmissions: 40, ingested: 30, no_tgid: 4, enc: 6, dropped_patch: 9, dropped_site: 51 },
      { received: 10, transmissions: 5, ingested: 4, no_tgid: 1, enc: 0, dropped_patch: 1, dropped_site: 4 },
    );
    const t = await radioTotalsFromRollup(pool, '7d', [], [], false, EDGE);
    expect(t).not.toBeNull();
    expect(t!.received).toBe(110);
    expect(t!.transmissions).toBe(45);
    expect(t!.ingested).toBe(34);
    expect(t!.dropped_patch).toBe(10);
    expect(t!.dropped_site).toBe(55);
  });

  it('derives not_recorded as the residual, so the outcomes always partition', async () => {
    // The detail path gets this free from a CASE with an ELSE. Summing four
    // independently-counted buckets would not: any disagreement would show as
    // tiles that do not add up to the headline, which is the one thing this
    // page must never do.
    const { pool } = poolOf(
      { received: 90, transmissions: 50, ingested: 20, no_tgid: 5, enc: 7, dropped_patch: 0, dropped_site: 40 },
      ZERO,
    );
    const t = await radioTotalsFromRollup(pool, '24h', [], [], false, EDGE);
    expect(t!.not_recorded).toBe(50 - 20 - 5 - 7);
    expect(t!.ingested + t!.no_tgid + t!.enc + t!.not_recorded).toBe(t!.transmissions);
  });

  it('never returns a negative residual as a silent nonsense number', async () => {
    // If the rollup and the partial hour ever disagree badly enough that the
    // outcomes exceed the transmissions, that is a bug worth seeing rather
    // than a tile reading -3.
    const { pool } = poolOf(
      { received: 10, transmissions: 2, ingested: 5, no_tgid: 0, enc: 0, dropped_patch: 0, dropped_site: 8 },
      ZERO,
    );
    const t = await radioTotalsFromRollup(pool, '24h', [], [], false, EDGE);
    expect(t!.not_recorded).toBeGreaterThanOrEqual(0);
  });

  it('splits both halves at the one seam it was given', async () => {
    // The seam used to be assumed to be the top of the current hour. The
    // rollup job is not aligned to the clock, so between HH:00 and its next
    // tick the hour that just closed was in NEITHER half and about an hour of
    // traffic vanished from the tiles, returning when the tick landed. The
    // handler now reads where the rollup has got to (resolveRollupEdge) and
    // both statements split on that same value.
    const { pool, query } = poolOf(ZERO, ZERO);
    await radioTotalsFromRollup(pool, '30d', [1], [2], false, EDGE);

    const rollupSql = String(query.mock.calls[0]![0]);
    expect(rollupSql).toContain('node_radio_hourly_rx');
    // The window still STARTS on a whole hour — a sliding edge would reshuffle
    // the first bucket on every poll...
    expect(rollupSql).toContain("hour >= date_trunc('hour', now()) - $1::interval");
    // ...and ends at the seam...
    expect(rollupSql).toContain(`hour <  ${EDGE_SQL}`);
    // ...where detail picks up, so the halves meet exactly.
    const partialSql = String(query.mock.calls[1]![0]);
    expect(partialSql).toContain('node_radio_events');
    expect(partialSql).toContain(`received_at >= ${EDGE_SQL}`);
    expect(query.mock.calls[0]![1]).toEqual(['30 days', [1], [2]]);
  });

  it('never reads the seam inside either statement', async () => {
    // As a scalar subquery the seam was invisible to the planner, which guessed
    // a third of the table for the live hour (~1.5M rows vs ~7.5k) and spent
    // ~824ms JIT-compiling the totals query for ~80ms of work — 906ms as a
    // subquery against 19ms with the value visible, measured on prod. That was
    // most of every warm overview poll.
    const { pool, query } = poolOf(ZERO, ZERO);
    await radioTotalsFromRollup(pool, '24h', [], [], false, EDGE);
    for (const call of query.mock.calls) {
      expect(String(call[0])).not.toContain('node_rollup_state');
    }
  });

  it('drops encrypted traffic from every figure when asked, not just its own tile', async () => {
    const { pool, query } = poolOf(ZERO, ZERO);
    await radioTotalsFromRollup(pool, '24h', [123], [], true, EDGE);
    // The filter is on the WHERE, so those calls leave `received` and the drop
    // counts too — otherwise the buckets would stop summing to the headline.
    expect(String(query.mock.calls[0]![0])).toContain('AND NOT (home_talkgroup = ANY($2::int[]))');
  });

  it('returns null rather than zeroes when a query answers with no row', async () => {
    // Zeroes would render as a page reporting no traffic, which is a lie the
    // caller cannot detect. Null falls back to the detail path.
    expect(await radioTotalsFromRollup(poolOf(null, ZERO).pool, '24h', [], [], false, EDGE)).toBeNull();
    expect(await radioTotalsFromRollup(poolOf(ZERO, null).pool, '24h', [], [], false, EDGE)).toBeNull();
  });

  it('handles bigint-shaped strings, which is how pg returns sums', async () => {
    const { pool } = poolOf(
      { received: '100', transmissions: '40', ingested: '30', no_tgid: '0', enc: '0', dropped_patch: '0', dropped_site: '60' },
      ZERO,
    );
    const t = await radioTotalsFromRollup(pool, '7d', [], [], false, EDGE);
    expect(t!.received).toBe(100);
    expect(t!.transmissions).toBe(40);
  });
});

describe('resolveRollupEdge', () => {
  const poolAnswering = (edge: unknown) => {
    const query = vi.fn().mockResolvedValue({ rows: [{ edge }] });
    return { pool: { query } as never, query };
  };

  it('reads where the rollup has got to, capped at the hour in progress', async () => {
    const { pool, query } = poolAnswering(EDGE);
    expect(await resolveRollupEdge(pool)).toEqual(EDGE);
    const sql = String(query.mock.calls[0]![0]);
    // LEAST: the rollup never derives the hour in progress, so detail must
    // still cover it even if the stored value ran ahead. COALESCE: a missing
    // row or a NULL before the first pass falls back to the top of the hour.
    // Both evaluated by the DATABASE, so app/DB clock skew cannot open a gap.
    expect(sql).toContain("LEAST(rolled_up_to, date_trunc('hour', now()))");
    expect(sql).toContain('FROM node_rollup_state');
    expect(sql).toContain('COALESCE(');
  });

  it('accepts the timestamp as a string too', async () => {
    // Belt and braces: pg's parsers are global, and services/rdio.ts already
    // installs one for a neighbouring type.
    const { pool } = poolAnswering('2026-10-14T06:00:00.000Z');
    expect(await resolveRollupEdge(pool)).toEqual(EDGE);
  });

  it('throws rather than handing on a seam it could not read', async () => {
    // An Invalid Date would otherwise surface as a RangeError from
    // toISOString() deep inside a query builder, far from the cause.
    await expect(resolveRollupEdge(poolAnswering(null).pool)).rejects.toThrow(/rollup seam/);
    await expect(resolveRollupEdge(poolAnswering('not a date').pool)).rejects.toThrow(/rollup seam/);
  });
});

describe('rollupEdgeSql', () => {
  it('renders a timestamptz literal the planner can estimate', () => {
    expect(rollupEdgeSql(EDGE)).toBe(EDGE_SQL);
  });

  it('keeps sub-hour precision, so a mid-hour seam is not rounded into a gap', () => {
    const odd = new Date('2026-10-14T06:37:12.345Z');
    expect(rollupEdgeSql(odd)).toBe("'2026-10-14T06:37:12.345Z'::timestamptz");
  });
});
