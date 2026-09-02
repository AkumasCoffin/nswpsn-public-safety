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
import { outcomeSql, radioTotalsFromRollup } from '../../../src/api/node-data.js';

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
    const t = await radioTotalsFromRollup(pool, '7d', [], [], false);
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
    const t = await radioTotalsFromRollup(pool, '24h', [], [], false);
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
    const t = await radioTotalsFromRollup(pool, '24h', [], [], false);
    expect(t!.not_recorded).toBeGreaterThanOrEqual(0);
  });

  it('asks the rollup for whole hours and the detail for the live one', async () => {
    const { pool, query } = poolOf(ZERO, ZERO);
    await radioTotalsFromRollup(pool, '30d', [1], [2], false);

    const rollupSql = String(query.mock.calls[0]![0]);
    expect(rollupSql).toContain('node_radio_hourly_rx');
    // Bucketed, so the edges are whole hours: everything up to the top of the
    // current hour comes from the rollup...
    expect(rollupSql).toContain("hour <  date_trunc('hour', now())");
    // ...and the remainder from detail.
    const partialSql = String(query.mock.calls[1]![0]);
    expect(partialSql).toContain('node_radio_events');
    expect(partialSql).toContain("received_at >= date_trunc('hour', now())");
    expect(query.mock.calls[0]![1]).toEqual(['30 days', [1], [2]]);
  });

  it('drops encrypted traffic from every figure when asked, not just its own tile', async () => {
    const { pool, query } = poolOf(ZERO, ZERO);
    await radioTotalsFromRollup(pool, '24h', [123], [], true);
    // The filter is on the WHERE, so those calls leave `received` and the drop
    // counts too — otherwise the buckets would stop summing to the headline.
    expect(String(query.mock.calls[0]![0])).toContain('AND NOT (home_talkgroup = ANY($2::int[]))');
  });

  it('returns null rather than zeroes when a query answers with no row', async () => {
    // Zeroes would render as a page reporting no traffic, which is a lie the
    // caller cannot detect. Null falls back to the detail path.
    expect(await radioTotalsFromRollup(poolOf(null, ZERO).pool, '24h', [], [], false)).toBeNull();
    expect(await radioTotalsFromRollup(poolOf(ZERO, null).pool, '24h', [], [], false)).toBeNull();
  });

  it('handles bigint-shaped strings, which is how pg returns sums', async () => {
    const { pool } = poolOf(
      { received: '100', transmissions: '40', ingested: '30', no_tgid: '0', enc: '0', dropped_patch: '0', dropped_site: '60' },
      ZERO,
    );
    const t = await radioTotalsFromRollup(pool, '7d', [], [], false);
    expect(t!.received).toBe(100);
    expect(t!.transmissions).toBe(40);
  });
});
