/**
 * Unit tests for ArchiveWriter — exercises the queue + bucketing + cap
 * behaviour without a real DB. The DB-bound flush() path is covered by
 * an integration test (later milestones); here we verify pure logic.
 */
import { describe, it, expect } from 'vitest';
import { ArchiveWriter, type ArchiveRow } from '../../src/store/archive.js';

function row(source: string, source_id?: string): ArchiveRow {
  return {
    source,
    source_id: source_id ?? null,
    fetched_at: 1_700_000_000,
    data: { example: true },
  };
}

describe('ArchiveWriter (pure logic)', () => {
  it('starts with empty metrics', () => {
    const w = new ArchiveWriter();
    const m = w.metrics();
    expect(m.queue_size).toBe(0);
    expect(m.dropped).toBe(0);
    expect(m.last_flush_age_secs).toBeNull();
    expect(m.total_written).toBe(0);
  });

  it('push grows the queue', () => {
    const w = new ArchiveWriter();
    w.push('archive_waze', row('waze_police'));
    w.push('archive_waze', row('waze_hazard'));
    expect(w.metrics().queue_size).toBe(2);
  });

  it('pushMany grows the queue in bulk', () => {
    const w = new ArchiveWriter();
    w.pushMany('archive_rfs', [row('rfs', '1'), row('rfs', '2'), row('rfs', '3')]);
    expect(w.metrics().queue_size).toBe(3);
  });

  it('flush is a no-op without a DATABASE_URL configured', async () => {
    const w = new ArchiveWriter();
    w.push('archive_waze', row('waze_police'));
    const r = await w.flush();
    // No DB → flush returns zero counts, queue stays intact.
    expect(r.rows).toBe(0);
    expect(w.metrics().queue_size).toBe(1);
  });

  it('hard cap drops oldest rows under sustained overload', () => {
    const w = new ArchiveWriter();
    // Push 60k rows — past the 50k hard cap.
    const rows: ArchiveRow[] = [];
    for (let i = 0; i < 60_000; i++) {
      rows.push(row('waze_police', String(i)));
    }
    w.pushMany('archive_waze', rows);
    const m = w.metrics();
    expect(m.queue_size).toBeLessThanOrEqual(50_000);
    expect(m.dropped).toBeGreaterThan(0);
  });
});
