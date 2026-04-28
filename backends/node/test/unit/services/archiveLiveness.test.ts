/**
 * archiveLiveness — tombstone helpers for "disappeared from upstream"
 * tracking. Mocks pool.query so we can stage the "live before" set
 * deterministically.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  computeDisappearedTombstones,
  stampLiveRow,
  sweepStaleAsEnded,
} from '../../../src/services/archiveLiveness.js';
import type { ArchiveRow } from '../../../src/store/archive.js';

interface FakeRow {
  source_id: string;
  lat: number | null;
  lng: number | null;
  category: string | null;
  subcategory: string | null;
  data: Record<string, unknown> | null;
}

const queryMock = vi.fn<(sql: string, params?: unknown[]) => Promise<{ rows: FakeRow[]; rowCount?: number }>>();
const fakePool = { query: queryMock } as unknown as Parameters<typeof computeDisappearedTombstones>[0]['pool'];

beforeEach(() => {
  queryMock.mockReset();
});

const mkRow = (sid: string, extra: Partial<ArchiveRow> = {}): ArchiveRow => ({
  source: 'rfs',
  source_id: sid,
  fetched_at: 1700000000,
  data: { title: `Incident ${sid}`, severity: 'major' },
  ...extra,
});

describe('stampLiveRow', () => {
  it('adds is_live=true to rows that don\'t set it', () => {
    const stamped = stampLiveRow(mkRow('a', { data: { title: 'foo' } }));
    expect((stamped.data as Record<string, unknown>)['is_live']).toBe(true);
    expect((stamped.data as Record<string, unknown>)['title']).toBe('foo');
  });

  it('preserves an explicit is_live value', () => {
    const stamped = stampLiveRow(mkRow('a', { data: { title: 'foo', is_live: false } }));
    expect((stamped.data as Record<string, unknown>)['is_live']).toBe(false);
  });
});

describe('computeDisappearedTombstones', () => {
  it('returns empty list when newRows is empty (upstream blip protection)', async () => {
    const out = await computeDisappearedTombstones({
      pool: fakePool,
      table: 'archive_rfs',
      source: 'rfs',
      newRows: [],
      fetchedAt: 1700000000,
    });
    expect(out).toEqual([]);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('returns empty list for waze_* sources (rotation exemption)', async () => {
    const out = await computeDisappearedTombstones({
      pool: fakePool,
      table: 'archive_waze',
      source: 'waze_police',
      newRows: [mkRow('a')],
      fetchedAt: 1700000000,
    });
    expect(out).toEqual([]);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('returns empty list when newRows have no source_ids', async () => {
    const out = await computeDisappearedTombstones({
      pool: fakePool,
      table: 'archive_rfs',
      source: 'rfs',
      newRows: [mkRow('', { source_id: null })],
      fetchedAt: 1700000000,
    });
    expect(out).toEqual([]);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('builds tombstones for source_ids that vanished from upstream', async () => {
    queryMock.mockResolvedValueOnce({
      rows: [
        { source_id: 'a', lat: 1, lng: 2, category: 'fire', subcategory: 'minor', data: { title: 'A', severity: 'low' } },
        { source_id: 'b', lat: 3, lng: 4, category: 'fire', subcategory: null, data: { title: 'B' } },
        { source_id: 'c', lat: null, lng: null, category: null, subcategory: null, data: null },
      ],
      rowCount: 3,
    });
    const out = await computeDisappearedTombstones({
      pool: fakePool,
      table: 'archive_rfs',
      source: 'rfs',
      newRows: [mkRow('a'), mkRow('d')], // 'a' still live, 'b' and 'c' disappeared, 'd' is new
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

  it('returns empty list when nothing has disappeared', async () => {
    queryMock.mockResolvedValueOnce({
      rows: [
        { source_id: 'a', lat: 1, lng: 2, category: null, subcategory: null, data: { title: 'A' } },
      ],
      rowCount: 1,
    });
    const out = await computeDisappearedTombstones({
      pool: fakePool,
      table: 'archive_rfs',
      source: 'rfs',
      newRows: [mkRow('a'), mkRow('new')],
      fetchedAt: 1700001000,
    });
    expect(out).toEqual([]);
  });

  it('does not block when DB query fails — logs and returns empty', async () => {
    queryMock.mockRejectedValueOnce(new Error('connection refused'));
    const out = await computeDisappearedTombstones({
      pool: fakePool,
      table: 'archive_rfs',
      source: 'rfs',
      newRows: [mkRow('a')],
      fetchedAt: 1700001000,
    });
    expect(out).toEqual([]);
  });

  it('scopes the live-rows query to the supplied source', async () => {
    queryMock.mockResolvedValueOnce({ rows: [], rowCount: 0 });
    await computeDisappearedTombstones({
      pool: fakePool,
      table: 'archive_traffic',
      source: 'traffic_incident',
      newRows: [mkRow('a')],
      fetchedAt: 1700001000,
    });
    expect(queryMock).toHaveBeenCalledOnce();
    const [sql, params] = queryMock.mock.calls[0]!;
    expect(sql).toContain('archive_traffic');
    expect(params?.[0]).toBe('traffic_incident');
  });
});

describe('sweepStaleAsEnded', () => {
  it('runs an INSERT…SELECT against every archive table', async () => {
    queryMock.mockResolvedValue({ rows: [], rowCount: 0 });
    const inserted = await sweepStaleAsEnded(fakePool);
    expect(inserted).toBe(0);
    // 5 archive_* tables.
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
    expect(inserted).toBe(7); // 2+4+1, the failed table contributes 0
  });
});
