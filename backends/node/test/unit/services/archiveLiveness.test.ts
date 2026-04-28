/**
 * archiveLiveness — flush-time tombstone helpers. Mocks pool.query so
 * we can stage the "live before" set deterministically across multiple
 * sources sharing one archive table.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  computeFlushTimeTombstones,
  stampLiveRow,
  sweepStaleAsEnded,
} from '../../../src/services/archiveLiveness.js';
import type { ArchiveRow } from '../../../src/store/archive.js';

interface FakeRow {
  source: string;
  source_id: string;
  lat: number | null;
  lng: number | null;
  category: string | null;
  subcategory: string | null;
  data: Record<string, unknown> | null;
}

const queryMock = vi.fn<(sql: string, params?: unknown[]) => Promise<{ rows: FakeRow[]; rowCount?: number }>>();
const fakePool = { query: queryMock } as unknown as Parameters<typeof computeFlushTimeTombstones>[0]['pool'];

beforeEach(() => {
  queryMock.mockReset();
});

const mkRow = (
  source: string,
  sid: string | null,
  extra: Partial<ArchiveRow> = {},
): ArchiveRow => ({
  source,
  source_id: sid,
  fetched_at: 1700000000,
  data: { title: `Incident ${sid ?? '?'}`, severity: 'major' },
  ...extra,
});

describe('stampLiveRow', () => {
  it('adds is_live=true to rows that don\'t set it', () => {
    const stamped = stampLiveRow(mkRow('rfs', 'a', { data: { title: 'foo' } }));
    expect((stamped.data as Record<string, unknown>)['is_live']).toBe(true);
    expect((stamped.data as Record<string, unknown>)['title']).toBe('foo');
  });

  it('preserves an explicit is_live value', () => {
    const stamped = stampLiveRow(mkRow('rfs', 'a', { data: { title: 'foo', is_live: false } }));
    expect((stamped.data as Record<string, unknown>)['is_live']).toBe(false);
  });
});

describe('computeFlushTimeTombstones', () => {
  it('returns empty list when rows is empty', async () => {
    const out = await computeFlushTimeTombstones({
      pool: fakePool,
      table: 'archive_rfs',
      rows: [],
      fetchedAt: 1700000000,
    });
    expect(out).toEqual([]);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('skips waze_* rows (rotation exemption)', async () => {
    const out = await computeFlushTimeTombstones({
      pool: fakePool,
      table: 'archive_waze',
      rows: [mkRow('waze_police', 'a'), mkRow('waze_hazard', 'b')],
      fetchedAt: 1700000000,
    });
    expect(out).toEqual([]);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('skips endeavour_planned, essential_planned and essential_future (future-dated)', async () => {
    const out = await computeFlushTimeTombstones({
      pool: fakePool,
      table: 'archive_power',
      rows: [
        mkRow('endeavour_planned', 'a'),
        mkRow('essential_planned', 'b'),
        mkRow('essential_future', 'c'),
      ],
      fetchedAt: 1700000000,
    });
    expect(out).toEqual([]);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('still runs diff for non-exempt power outages', async () => {
    queryMock.mockResolvedValue({ rows: [], rowCount: 0 });
    await computeFlushTimeTombstones({
      pool: fakePool,
      table: 'archive_power',
      rows: [mkRow('essential_current', 'a'), mkRow('endeavour_current', 'b')],
      fetchedAt: 1700000000,
    });
    expect(queryMock).toHaveBeenCalledOnce();
    const [, params] = queryMock.mock.calls[0]!;
    // Both sources go into the same query via ANY($1::text[]).
    expect(params?.[0]).toEqual(expect.arrayContaining(['essential_current', 'endeavour_current']));
  });

  it('builds tombstones for source_ids that vanished from upstream', async () => {
    queryMock.mockResolvedValueOnce({
      rows: [
        { source: 'rfs', source_id: 'a', lat: 1, lng: 2, category: 'fire', subcategory: 'minor', data: { title: 'A' } },
        { source: 'rfs', source_id: 'b', lat: 3, lng: 4, category: 'fire', subcategory: null, data: { title: 'B' } },
        { source: 'rfs', source_id: 'c', lat: null, lng: null, category: null, subcategory: null, data: null },
      ],
      rowCount: 3,
    });
    const out = await computeFlushTimeTombstones({
      pool: fakePool,
      table: 'archive_rfs',
      rows: [mkRow('rfs', 'a'), mkRow('rfs', 'd')], // 'a' still live, 'b'+'c' disappeared, 'd' is new
      fetchedAt: 1700001000,
    });
    expect(out.map((r) => r.source_id).sort()).toEqual(['b', 'c']);
    const b = out.find((r) => r.source_id === 'b')!;
    expect(b.fetched_at).toBe(1700001000);
    expect(b.lat).toBe(3);
    expect(b.lng).toBe(4);
    expect(b.category).toBe('fire');
    expect((b.data as Record<string, unknown>)['title']).toBe('B'); // preserved
    expect((b.data as Record<string, unknown>)['is_live']).toBe(false);
    // c had null data — tombstone gets a fresh object with just is_live.
    const c = out.find((r) => r.source_id === 'c')!;
    expect((c.data as Record<string, unknown>)['is_live']).toBe(false);
  });

  it('tombstones disappear-detection per source independently in one batched query', async () => {
    // Two sources sharing archive_traffic. Each has one disappeared id.
    queryMock.mockResolvedValueOnce({
      rows: [
        { source: 'traffic_incident', source_id: 'i1', lat: 1, lng: 1, category: null, subcategory: null, data: { title: 'I1' } },
        { source: 'traffic_incident', source_id: 'i2', lat: 1, lng: 1, category: null, subcategory: null, data: { title: 'I2' } },
        { source: 'traffic_roadwork', source_id: 'r1', lat: 1, lng: 1, category: null, subcategory: null, data: { title: 'R1' } },
        { source: 'traffic_roadwork', source_id: 'r2', lat: 1, lng: 1, category: null, subcategory: null, data: { title: 'R2' } },
      ],
      rowCount: 4,
    });
    const out = await computeFlushTimeTombstones({
      pool: fakePool,
      table: 'archive_traffic',
      rows: [
        mkRow('traffic_incident', 'i1'),
        // i2 disappeared
        mkRow('traffic_roadwork', 'r1'),
        // r2 disappeared
      ],
      fetchedAt: 1700001000,
    });
    expect(out).toHaveLength(2);
    const tombstoneIds = out.map((r) => `${r.source}:${r.source_id}`).sort();
    expect(tombstoneIds).toEqual(['traffic_incident:i2', 'traffic_roadwork:r2']);
    // Single SELECT covered both sources.
    expect(queryMock).toHaveBeenCalledOnce();
  });

  it('skips a source whose batch has no source_ids (empty / id-less snapshot)', async () => {
    queryMock.mockResolvedValueOnce({
      rows: [
        // The DB has live records for 'rfs' and 'traffic_incident'.
        { source: 'rfs', source_id: 'a', lat: null, lng: null, category: null, subcategory: null, data: { title: 'A' } },
        { source: 'traffic_incident', source_id: 'i1', lat: null, lng: null, category: null, subcategory: null, data: null },
      ],
      rowCount: 2,
    });
    // RFS has a real source_id, traffic_incident snapshot is id-less (won't be diffed).
    const out = await computeFlushTimeTombstones({
      pool: fakePool,
      table: 'archive_traffic',
      rows: [
        mkRow('rfs', 'a'),
        mkRow('traffic_incident', null),
      ],
      fetchedAt: 1700001000,
    });
    // RFS 'a' is in the batch so no rfs tombstone. traffic_incident
    // wasn't queried (no source_ids in its batch slice) so no
    // traffic tombstone either.
    expect(out).toEqual([]);
  });

  it('returns empty list when DB query fails — does not block flush', async () => {
    queryMock.mockRejectedValueOnce(new Error('connection refused'));
    const out = await computeFlushTimeTombstones({
      pool: fakePool,
      table: 'archive_rfs',
      rows: [mkRow('rfs', 'a')],
      fetchedAt: 1700001000,
    });
    expect(out).toEqual([]);
  });
});

describe('sweepStaleAsEnded', () => {
  it('runs an INSERT…SELECT against every archive table', async () => {
    queryMock.mockResolvedValue({ rows: [], rowCount: 0 });
    const inserted = await sweepStaleAsEnded(fakePool);
    expect(inserted).toBe(0);
    expect(queryMock).toHaveBeenCalledTimes(5);
    for (const call of queryMock.mock.calls) {
      const sql = String(call[0]);
      expect(sql).toMatch(/INSERT INTO archive_(waze|traffic|rfs|power|misc)/);
      expect(sql).toContain("'{\"is_live\": false}'::jsonb");
    }
  });

  it('totals the rowCount across tables', async () => {
    queryMock
      .mockResolvedValueOnce({ rows: [], rowCount: 3 })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({ rows: [], rowCount: 7 })
      .mockResolvedValueOnce({ rows: [], rowCount: 1 })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 });
    const inserted = await sweepStaleAsEnded(fakePool);
    expect(inserted).toBe(11);
  });

  it('continues sweeping other tables when one fails', async () => {
    queryMock
      .mockResolvedValueOnce({ rows: [], rowCount: 2 })
      .mockRejectedValueOnce(new Error('lock timeout'))
      .mockResolvedValueOnce({ rows: [], rowCount: 4 })
      .mockResolvedValueOnce({ rows: [], rowCount: 1 })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 });
    const inserted = await sweepStaleAsEnded(fakePool);
    expect(inserted).toBe(7);
  });
});
