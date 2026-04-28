/**
 * Pure-logic tests for the /api/data/history query builder. We assert
 * against the generated SQL string + params array — no real DB
 * involved. Each test pins a specific behaviour the route depends on:
 * source family routing, DISTINCT ON for unique=1, cursor seek, geo
 * bounding box, JSONB predicate generation.
 */
import { describe, it, expect } from 'vitest';
import {
  ALL_ARCHIVE_TABLES,
  buildPlan,
  buildSqlForTable,
  DEPRECATED_SOURCES,
  mergeAndPaginate,
  sourceFamilies,
  type ArchiveQueryRow,
  type DataHistoryParams,
} from '../../../src/services/dataHistoryQuery.js';

function defaultParams(overrides: Partial<DataHistoryParams> = {}): DataHistoryParams {
  return {
    sources: null,
    sourceId: null,
    category: null,
    subcategory: null,
    status: null,
    severity: null,
    since: null,
    until: null,
    sinceSource: null,
    untilSource: null,
    search: null,
    title: null,
    location: null,
    lat: null,
    lng: null,
    radiusKm: 10,
    unique: false,
    liveOnly: false,
    historicalOnly: false,
    activeOnly: false,
    order: 'DESC',
    limit: 100,
    offset: 0,
    cursor: null,
    includeData: false,
    ...overrides,
  };
}

describe('sourceFamilies', () => {
  it('returns all 5 tables when no source filter is set', () => {
    expect(sourceFamilies(null)).toEqual(ALL_ARCHIVE_TABLES);
    expect(sourceFamilies([])).toEqual(ALL_ARCHIVE_TABLES);
  });

  it('routes a single waze source to archive_waze only', () => {
    expect(sourceFamilies(['waze_police'])).toEqual(['archive_waze']);
  });

  it('routes multiple waze sources to archive_waze only', () => {
    expect(sourceFamilies(['waze_police', 'waze_hazard'])).toEqual(['archive_waze']);
  });

  it('routes mixed families to the union of their tables', () => {
    expect(sourceFamilies(['rfs', 'bom_warning'])).toEqual(['archive_rfs', 'archive_misc']);
  });

  it('routes power sources to archive_power', () => {
    expect(sourceFamilies(['endeavour_current', 'ausgrid'])).toEqual(['archive_power']);
  });

  it('falls back unknown sources to archive_misc', () => {
    expect(sourceFamilies(['nonsense_source'])).toEqual(['archive_misc']);
  });

  it('returns tables in stable archive_waze->misc order', () => {
    const fams = sourceFamilies(['pager', 'waze_police', 'rfs']);
    expect(fams).toEqual(['archive_waze', 'archive_rfs', 'archive_misc']);
  });
});

describe('buildSqlForTable — basics', () => {
  it('produces SELECT FROM <table> with only the implicit deprecated-source filter', () => {
    const q = buildSqlForTable('archive_waze', defaultParams());
    expect(q.sql).toContain('FROM archive_waze');
    // Only the implicit deprecated-source NOT IN filter is present.
    expect(q.sql).toContain('source NOT IN');
    expect(q.sql).toMatch(/ORDER BY fetched_at DESC, id DESC/);
    // Params: deprecated source name + LIMIT placeholder for offset+limit=100.
    expect(q.params[q.params.length - 1]).toBe(100);
  });

  it('honours include_data flag', () => {
    const q1 = buildSqlForTable('archive_waze', defaultParams({ includeData: false }));
    expect(q1.sql).toContain("'{}'::jsonb AS data");
    const q2 = buildSqlForTable('archive_waze', defaultParams({ includeData: true }));
    expect(q2.sql).not.toContain("'{}'::jsonb AS data");
    expect(q2.sql).toMatch(/lat, lng, category, subcategory,\s+data/);
  });

  it('emits NOT IN clause for deprecated sources by default', () => {
    const q = buildSqlForTable('archive_power', defaultParams());
    expect(q.sql).toContain('source NOT IN');
    expect(q.params).toContain('essential_energy_cancelled');
  });

  it('omits NOT IN when caller explicitly asks for a deprecated source', () => {
    const dep = Array.from(DEPRECATED_SOURCES)[0];
    if (!dep) throw new Error('expected at least one deprecated source');
    const q = buildSqlForTable(
      'archive_power',
      defaultParams({ sources: [dep] }),
    );
    expect(q.sql).not.toContain('source NOT IN');
  });

  it('fans an `IN (...)` for a multi-value source filter', () => {
    const q = buildSqlForTable(
      'archive_waze',
      defaultParams({ sources: ['waze_police', 'waze_hazard'] }),
    );
    expect(q.sql).toMatch(/source IN \(\$1,\$2\)/);
    expect(q.params.slice(0, 2)).toEqual(['waze_police', 'waze_hazard']);
  });

  it('uses `=` for a single-value source filter', () => {
    const q = buildSqlForTable(
      'archive_rfs',
      defaultParams({ sources: ['rfs'] }),
    );
    expect(q.sql).toMatch(/source = \$1/);
  });
});

describe('buildSqlForTable — JSONB filters', () => {
  it('routes status through data->>status (no top-level column)', () => {
    const q = buildSqlForTable(
      'archive_misc',
      defaultParams({ status: ['Advice', 'Watch'] }),
    );
    expect(q.sql).toContain("(data->>'status') IN");
  });

  it('routes severity through data->>severity', () => {
    const q = buildSqlForTable(
      'archive_traffic',
      defaultParams({ severity: ['Major'] }),
    );
    expect(q.sql).toContain("(data->>'severity') = ");
  });

  it('search uses ILIKE on title and location_text JSONB fields', () => {
    const q = buildSqlForTable(
      'archive_traffic',
      defaultParams({ search: 'hwy' }),
    );
    expect(q.sql).toMatch(/data->>'title'\) ILIKE/);
    expect(q.sql).toMatch(/data->>'location_text'\) ILIKE/);
    expect(q.params).toContain('%hwy%');
  });

  it('live_only filters on data->>is_live truthy variants', () => {
    const q = buildSqlForTable(
      'archive_traffic',
      defaultParams({ liveOnly: true }),
    );
    expect(q.sql).toContain("data->>'is_live' IN");
    expect(q.sql).toContain("'1'");
    expect(q.sql).toContain("'true'");
  });
});

describe('buildSqlForTable — time, geo, cursor', () => {
  it('translates `since` into fetched_at >= to_timestamp($n)', () => {
    const q = buildSqlForTable(
      'archive_waze',
      defaultParams({ since: 1_700_000_000 }),
    );
    expect(q.sql).toContain('fetched_at >= to_timestamp(');
    expect(q.params).toContain(1_700_000_000);
  });

  it('translates `until` into fetched_at <= to_timestamp($n)', () => {
    const q = buildSqlForTable(
      'archive_waze',
      defaultParams({ until: 1_700_000_999 }),
    );
    expect(q.sql).toContain('fetched_at <= to_timestamp(');
    expect(q.params).toContain(1_700_000_999);
  });

  it('emits a bounding-box clause for lat/lng/radius', () => {
    const q = buildSqlForTable(
      'archive_waze',
      defaultParams({ lat: -33.86, lng: 151.21, radiusKm: 5 }),
    );
    expect(q.sql).toContain('lat BETWEEN');
    expect(q.sql).toContain('lng BETWEEN');
    // Four geo params appended at the tail: latLow, latHigh, lonLow, lonHigh.
    // Plus the LIMIT placeholder.
    expect(q.params.length).toBeGreaterThanOrEqual(5);
  });

  it('cursor seek emits the (fetched_at < t OR (fetched_at = t AND id < id)) shape', () => {
    const q = buildSqlForTable(
      'archive_waze',
      defaultParams({ cursor: { fetchedAt: 1_700_000_000, rowId: 99 } }),
    );
    expect(q.sql).toMatch(
      /\(fetched_at < to_timestamp\(\$\d+\) OR \(fetched_at = to_timestamp\(\$\d+\) AND id < \$\d+\)\)/,
    );
    expect(q.params).toContain(1_700_000_000);
    expect(q.params).toContain(99);
  });

  it('cursor seek flips comparator for ASC order', () => {
    const q = buildSqlForTable(
      'archive_waze',
      defaultParams({
        cursor: { fetchedAt: 1_700_000_000, rowId: 99 },
        order: 'ASC',
      }),
    );
    expect(q.sql).toMatch(/fetched_at > to_timestamp/);
  });
});

describe('buildSqlForTable — unique=1', () => {
  it('uses bounded DISTINCT ON when unique=1', () => {
    // Rolled back from is_latest filter (which created I/O contention
    // killing writes). The unique=1 path now bounds the inner scan
    // by fetched_at DESC LIMIT 50000, then DISTINCT ON in-memory.
    const q = buildSqlForTable(
      'archive_waze',
      defaultParams({ unique: true, sources: ['waze_police'] }),
    );
    expect(q.sql).toContain('DISTINCT ON (source, source_id)');
    expect(q.sql).toContain('LIMIT 50000');
  });

  it('non-unique queries do NOT use DISTINCT ON', () => {
    const q = buildSqlForTable('archive_waze', defaultParams());
    expect(q.sql).not.toContain('DISTINCT ON');
  });
});

describe('buildPlan', () => {
  it('produces 1 query for a single-family request', () => {
    const plan = buildPlan(defaultParams({ sources: ['waze_police'] }));
    expect(plan.queries).toHaveLength(1);
    expect(plan.queries[0]?.table).toBe('archive_waze');
  });

  it('produces 5 queries when no source filter is set', () => {
    const plan = buildPlan(defaultParams());
    expect(plan.queries).toHaveLength(5);
  });

  it('produces 2 queries for a 2-family request', () => {
    const plan = buildPlan(defaultParams({ sources: ['waze_police', 'rfs'] }));
    expect(plan.queries).toHaveLength(2);
    expect(plan.queries.map((q) => q.table)).toEqual(['archive_waze', 'archive_rfs']);
  });

  it('zeroes effectiveOffset when a cursor is set', () => {
    const plan = buildPlan(
      defaultParams({
        offset: 500,
        cursor: { fetchedAt: 1_700_000_000, rowId: 1 },
      }),
    );
    expect(plan.effectiveOffset).toBe(0);
  });

  it('preserves offset when no cursor is set', () => {
    const plan = buildPlan(defaultParams({ offset: 500 }));
    expect(plan.effectiveOffset).toBe(500);
  });
});

describe('mergeAndPaginate', () => {
  function row(id: number, ts: number): ArchiveQueryRow {
    return {
      id,
      source: 'x',
      source_id: String(id),
      fetched_at_epoch: ts,
      fetched_at: new Date(ts * 1000),
      lat: null,
      lng: null,
      category: null,
      subcategory: null,
      data: {},
    };
  }

  it('sorts merged rows DESC by (fetched_at, id)', () => {
    const rows = [row(1, 1000), row(2, 1000), row(3, 999)];
    const plan = buildPlan(defaultParams({ limit: 10 }));
    const out = mergeAndPaginate(rows, plan);
    expect(out.map((r) => r.id)).toEqual([2, 1, 3]);
  });

  it('applies offset+limit after merge', () => {
    const rows = Array.from({ length: 50 }, (_, i) => row(i, 1000 + i));
    const plan = buildPlan(defaultParams({ limit: 5, offset: 10 }));
    const out = mergeAndPaginate(rows, plan);
    expect(out).toHaveLength(5);
    // DESC: top item is id=49, offset 10 lands on id=39.
    expect(out[0]?.id).toBe(39);
  });
});
