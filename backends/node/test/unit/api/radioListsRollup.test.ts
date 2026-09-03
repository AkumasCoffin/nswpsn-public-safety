/**
 * The overview's five lists, sourced from the hourly rollups.
 *
 * Migration 084 moved the eight tiles off the detail table and the phase log
 * then said the remainder plainly: 30d spent 22,346ms in queries-detail and
 * 52ms in totals-rollup. That remainder is these five — top talkgroups, top
 * sites, top radios, the activity series and the per-node card — each a
 * COUNT(DISTINCT (4-tuple)) over ~1.75M rows to render fifteen table rows.
 *
 * As with the tiles, whether the rollups DEFINE a reception the way the detail
 * queries do cannot be tested here: this suite mocks the pool and there is no
 * Postgres to run either statement against. That is settled against production,
 * where 24h works from detail today and must report the same lists once it
 * comes from the rollups.
 *
 * What IS testable is the seam — that each list merges its two sources before
 * ranking them, that the window edges line up with the buckets, that the
 * result sets do not get crossed on the way out, and that the predicates which
 * must stay at read time did.
 */
import { describe, it, expect, vi } from 'vitest';
import { radioListsFromRollup } from '../../../src/api/node-data.js';

/** Route each of the five statements by the table it reads. */
function poolOf(rows: Record<string, unknown[]> = {}) {
  const seen: string[] = [];
  const query = vi.fn(async (sql: string) => {
    seen.push(sql);
    if (sql.includes('node_radio_hourly_unit')) return { rows: rows['unit'] ?? [] };
    if (sql.includes('node_radio_hourly_sys')) {
      return { rows: (sql.includes('site_rfss <> -1') ? rows['sites'] : rows['tg']) ?? [] };
    }
    if (sql.includes('AS bytes')) return { rows: rows['perNode'] ?? [] };
    return { rows: rows['series'] ?? [] };
  });
  return { pool: { query } as never, query, seen };
}

/** The statement that reads a given rollup table. */
const stmt = (seen: string[], table: string): string =>
  seen.find((s) => s.includes(table)) ?? '';

describe('radioListsFromRollup', () => {
  it('ranks AFTER merging the live hour, not before it', async () => {
    // Each list is one statement over both sources. Limiting each source
    // separately would drop a radio quiet all window but busy right now — or,
    // worse, keep it and drop one that is genuinely top-15.
    const { pool, seen } = poolOf();
    await radioListsFromRollup(pool, '7d');

    for (const table of ['node_radio_hourly_sys', 'node_radio_hourly_unit', 'node_radio_hourly']) {
      const sql = stmt(seen, table);
      expect(sql).toContain('node_radio_events');
      expect(sql).toContain('UNION ALL');
    }
    for (const sql of seen.filter((s) => s.includes('LIMIT 15'))) {
      // The LIMIT belongs to the outer SELECT over the merged CTE.
      expect(sql.indexOf('FROM r')).toBeLessThan(sql.indexOf('LIMIT 15'));
    }
  });

  it('splits the window at the top of the current hour, both ways', async () => {
    const { pool, seen } = poolOf();
    await radioListsFromRollup(pool, '30d');

    for (const sql of seen) {
      // Whole hours from the buckets...
      expect(sql).toContain("hour <  date_trunc('hour', now())");
      // ...and the hour in progress from detail. Neither side may overlap the
      // other or every list would double-count the live hour.
      expect(sql).toContain("received_at >= date_trunc('hour', now())");
    }
  });

  it('asks for the interval the window names', async () => {
    const { pool, query } = poolOf();
    await radioListsFromRollup(pool, '7d');
    for (const call of query.mock.calls) expect(call[1]).toEqual(['7 days']);
  });

  it('caps window=all at 30 days, because that is as far back as detail goes', async () => {
    // Every radio rollup is DERIVED from node_radio_events, which is pruned at
    // 30 days. Asking for more would scan empty range and imply reach the page
    // does not have; radioWindowCapped tells the client the same thing.
    const { pool, query, seen } = poolOf();
    await radioListsFromRollup(pool, 'all');
    for (const call of query.mock.calls) expect(call[1]).toEqual(['30 days']);
    // ...and its chart is per-day, matching the detail path it replaces.
    expect(stmt(seen, 'node_radio_hourly WHERE')).toContain("date_trunc('day'");
  });

  it('keeps the talkgroup and radio id bands at READ time', async () => {
    // These say what looks like a real talkgroup or radio id TODAY. Applying
    // them in the rollup would freeze today's answer into stored history, so a
    // widened band could never reclassify the past.
    const { pool, seen } = poolOf();
    await radioListsFromRollup(pool, '24h');
    expect(stmt(seen, 'node_radio_hourly_sys')).toContain('talkgroup BETWEEN');
    expect(stmt(seen, 'node_radio_hourly_unit')).toContain('source_unit BETWEEN');
  });

  it('takes the talkgroup card logical count from the per-talkgroup attribution', async () => {
    // logical_calls attributes each call once FLEET-WIDE, which is what makes a
    // network total summable. Reading it here would give a patched call to its
    // lowest talkgroup and leave the others reading zero.
    const sql = (() => {
      const { pool, seen } = poolOf();
      return radioListsFromRollup(pool, '24h').then(() => stmt(seen, 'node_radio_hourly_sys'));
    })();
    expect(await sql).toContain('logical_calls_tg AS logical');
  });

  it('hands each result set back on its own key', async () => {
    // Crossing two of these would be invisible in a type check — every list is
    // {something, receptions} — and obvious only as sites in the radios card.
    const { pool } = poolOf({
      tg: [{ system: 721, talkgroup: 10101, receptions: 9, logical: 3, label: null }],
      sites: [{ site_rfss: 4, site_id: 85, receptions: 7 }],
      unit: [{ unit: 999, alias: 'CAR 1', receptions: 5 }],
      series: [{ bucket: new Date('2026-08-24T10:00:00Z'), n: 4 }],
      perNode: [{ node_id: 'n1', calls: 12, bytes: '345' }],
    });
    const r = await radioListsFromRollup(pool, '24h');
    expect(r).not.toBeNull();
    expect(r!.topTg[0]!.talkgroup).toBe(10101);
    expect(r!.topSites[0]!.site_id).toBe(85);
    expect(r!.topUnits[0]!.unit).toBe(999);
    expect(r!.series[0]!.n).toBe(4);
    expect(r!.perNode[0]!.node_id).toBe('n1');
  });

  it('restores the NULL system the rollup stores as 0', async () => {
    // 0 is the sentinel a NOT NULL primary key column needs. Left as 0 the
    // card renders a system called 0 where it used to render an em dash.
    const { pool, seen } = poolOf();
    await radioListsFromRollup(pool, '24h');
    expect(stmt(seen, 'node_radio_hourly_sys')).toContain('NULLIF(system, 0) AS system');
  });
});
