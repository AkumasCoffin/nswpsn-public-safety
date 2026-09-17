/**
 * Per-receiver tracks, kept across a restart.
 *
 * The tracks map was memory-only, so every deploy emptied it. A node that had
 * heard sixty-odd aircraft during the day showed ONE — whatever had crossed
 * since the backend last came up — while the coverage envelope beside it was
 * persisted and looked healthy, which made the map read as broken rather than
 * as young.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const queryMock = vi.fn();
let poolAvailable = true;

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve(poolAvailable ? { query: queryMock } : null)),
  closePool: vi.fn(),
}));

const HOUR = 3_600_000;
const H0 = Date.UTC(2026, 8, 17, 4, 0, 0);

const { flushNodeTracks, storedNodeTracks, _resetNodeTrackArchive } = await import(
  '../../../src/services/nodes/nodeTrackArchive.js'
);

const pt = (ms: number, lat: number, lon: number, alt: number | null = 30000) =>
  [ms, lat, lon, alt] as [number, number, number, number | null];

const track = (
  nodeId: string, hex: string,
  points: Array<[number, number, number, number | null]>,
  callsign: string | null = null,
) => ({ nodeId, hex, callsign, points, reports: 12, lastAltFt: 30000, maxAltFt: 34000 });

beforeEach(() => {
  queryMock.mockReset();
  queryMock.mockResolvedValue({ rows: [] });
  poolAvailable = true;
  // The archive remembers what it has written; without this every test after
  // the first would send an empty delta and assert nothing.
  _resetNodeTrackArchive();
});

describe('writing', () => {
  it('writes one row per aircraft-hour, per node', async () => {
    const n = await flushNodeTracks([
      track('node-a', 'abc123', [pt(H0 + 60_000, -33, 151), pt(H0 + 120_000, -33.1, 151.1)], 'QFA1'),
      track('node-b', 'abc123', [pt(H0 + 60_000, -33, 151)]),
    ], H0 + 180_000);
    expect(n).toBe(2);

    const [sql, values] = queryMock.mock.calls[0]!;
    expect(sql).toContain('INSERT INTO node_adsb_tracks');
    expect(sql).toContain('ON CONFLICT (node_id, hex, hour_bucket) DO UPDATE');
    expect(values[0]).toBe('node-a');
    expect(values[1]).toBe('abc123');
    expect(values[5]).toBe('QFA1');
    expect(values[6]).toBe(12);                     // reports
    expect(values[7]).toBe(30000);                  // last_alt_ft
    expect(values[8]).toBe(34000);                  // max_alt_ft
    // Altitude is a per-hour scalar, so the points carry null rather than
    // stamping the current altitude across the whole track.
    expect(JSON.parse(values[9] as string)).toEqual([
      [60, -33, 151, 30000], [120, -33.1, 151.1, 30000],
    ]);
  });

  it('keeps the same aircraft separate for two receivers', async () => {
    // The whole reason node_id is in the key: two receivers hearing one
    // aircraft heard DIFFERENT portions of its flight.
    await flushNodeTracks([
      track('node-a', 'abc123', [pt(H0 + 60_000, -33, 151)]),
      track('node-b', 'abc123', [pt(H0 + 60_000, -34, 152)]),
    ], H0 + 120_000);
    const values = queryMock.mock.calls[0]![1] as unknown[];
    expect(values[0]).toBe('node-a');
    expect(values[10]).toBe('node-b');   // ten columns per row
  });

  it('splits a flight across the hour boundary', async () => {
    const n = await flushNodeTracks([
      track('node-a', 'abc123', [
        pt(H0 + 55 * 60_000, -33, 151),
        pt(H0 + 62 * 60_000, -33.2, 151.2),
      ]),
    ], H0 + 65 * 60_000);
    expect(n).toBe(2);
  });

  it('only rewrites the current hour and the one just closed', async () => {
    // A long-running node must not rewrite its whole day every minute, and
    // rewriting a closed hour would let the upstream decimation degrade
    // history already stored at full resolution.
    const n = await flushNodeTracks([
      track('node-a', 'abc123', [
        pt(H0 - 3 * HOUR, -31, 149),
        pt(H0 - 2 * HOUR, -32, 150),
        pt(H0 - HOUR + 60_000, -32.5, 150.5),
        pt(H0 + 60_000, -33, 151),
      ]),
    ], H0 + 120_000);
    expect(n).toBe(2);
  });

  it('always appends, because only unsent points are ever sent', async () => {
    await flushNodeTracks([track('node-a', 'a1', [pt(H0 + 60_000, -33, 151)])], H0 + 120_000);
    const sql = queryMock.mock.calls[0]![0] as string;
    expect(sql).toContain('node_adsb_tracks.points || EXCLUDED.points');
    // The old clause replaced the row whenever the incoming slice was not
    // strictly newer than what was stored, which is exactly what the second
    // flush after a restart looks like.
    expect(sql).not.toContain('ELSE EXCLUDED.points');
    // The overflow guard must keep what is stored, never swap it for the
    // incoming slice — otherwise it becomes the same bug at 400 points.
    expect(sql).toContain('ELSE node_adsb_tracks.points');
  });

  it('sends only what it has not already written', async () => {
    const t = (pts: Array<[number, number, number, number | null]>) =>
      track('node-a', 'abc123', pts);
    await flushNodeTracks([t([pt(H0 + 60_000, -33, 151)])], H0 + 90_000);
    await flushNodeTracks(
      [t([pt(H0 + 60_000, -33, 151), pt(H0 + 120_000, -33.1, 151.1)])],
      H0 + 150_000,
    );

    expect(queryMock).toHaveBeenCalledTimes(2);
    const second = JSON.parse(queryMock.mock.calls[1]![1]![9] as string);
    // Just the new point — re-sending the first would double it in the row,
    // since the statement now appends unconditionally.
    expect(second).toEqual([[120, -33.1, 151.1, 30000]]);
  });

  it('writes nothing when nothing is new', async () => {
    const pts = [pt(H0 + 60_000, -33, 151)];
    await flushNodeTracks([track('node-a', 'abc123', pts)], H0 + 90_000);
    expect(await flushNodeTracks([track('node-a', 'abc123', pts)], H0 + 120_000)).toBe(0);
    expect(queryMock).toHaveBeenCalledTimes(1);
  });

  it('re-sends the same points after a failed write', async () => {
    // The mark may only move once the delta is durable. Advancing it on a
    // failure would drop those points permanently — the next pass would
    // consider them already written.
    queryMock.mockRejectedValueOnce(new Error('deadlock detected'));
    const pts = [pt(H0 + 60_000, -33, 151), pt(H0 + 120_000, -33.1, 151.1)];
    await flushNodeTracks([track('node-a', 'abc123', pts)], H0 + 150_000);
    await flushNodeTracks([track('node-a', 'abc123', pts)], H0 + 180_000);

    const resent = JSON.parse(queryMock.mock.calls[1]![1]![9] as string);
    expect(resent).toEqual([[60, -33, 151, 30000], [120, -33.1, 151.1, 30000]]);
  });

  it('keeps a track that straddles the hour in one statement', async () => {
    // Two rows, one mark. Splitting them across statements would let the mark
    // advance past a row that never landed.
    await flushNodeTracks([
      track('node-a', 'abc123', [
        pt(H0 + 55 * 60_000, -33, 151),
        pt(H0 + 62 * 60_000, -33.2, 151.2),
      ]),
    ], H0 + 65 * 60_000);
    expect(queryMock).toHaveBeenCalledTimes(1);
  });

  it('appends the whole window again after a restart', async () => {
    // A restart empties the marks, and memory restarts at boot — strictly
    // after anything already stored — so the first delta is the full live
    // trace and it appends cleanly onto the stored half.
    await flushNodeTracks([track('node-a', 'abc123', [pt(H0 + 60_000, -33, 151)])], H0 + 90_000);
    _resetNodeTrackArchive();
    await flushNodeTracks(
      [track('node-a', 'abc123', [pt(H0 + 120_000, -33.1, 151.1)])],
      H0 + 150_000,
    );
    const after = JSON.parse(queryMock.mock.calls[1]![1]![9] as string);
    expect(after).toEqual([[120, -33.1, 151.1, 30000]]);
  });

  it('is a no-op without a database, or without tracks', async () => {
    poolAvailable = false;
    expect(await flushNodeTracks([track('n', 'a', [pt(H0, -33, 151)])], H0)).toBe(0);
    poolAvailable = true;
    expect(await flushNodeTracks([], H0)).toBe(0);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('survives a failing write', async () => {
    queryMock.mockRejectedValue(new Error('deadlock detected'));
    await expect(
      flushNodeTracks([track('n', 'a', [pt(H0 + 60_000, -33, 151)])], H0 + 120_000),
    ).resolves.toBe(0);
  });
});

describe('reading back', () => {
  const row = (hex: string, hourMs: number,
    points: Array<[number, number, number, number | null]>,
    callsign: string | null = null) => ({
    hex, callsign, hour_bucket: new Date(hourMs), points,
    reports: 5, last_alt_ft: 30000, max_alt_ft: 34000,
    first_seen: new Date(hourMs + (points[0]?.[0] ?? 0) * 1000),
    last_seen: new Date(hourMs + (points[points.length - 1]?.[0] ?? 0) * 1000),
  });

  it('bounds on the hour first, then the window', async () => {
    await storedNodeTracks('node-a', 480, H0 + 30 * 60_000);
    const [sql, values] = queryMock.mock.calls[0]!;
    expect(sql).toContain('hour_bucket >=');
    expect(sql).toContain('last_seen   >=');
    expect(values[0]).toBe('node-a');
  });

  it('stitches an aircraft back together across hours', async () => {
    queryMock.mockResolvedValue({
      rows: [
        row('abc123', H0 - HOUR, [[3500, -33.0, 151.0, 30000]]),
        row('abc123', H0, [[60, -33.2, 151.2, 31000]], 'QFA1'),
      ],
    });
    const out = await storedNodeTracks('node-a', 480, H0 + 5 * 60_000);
    expect(out).toHaveLength(1);
    expect(out[0]!.points).toHaveLength(2);
    expect(out[0]!.points[0]![0]).toBeLessThan(out[0]!.points[1]![0]);
    expect(out[0]!.callsign).toBe('QFA1');
    // reports is the whole-trace running total stamped into every hour row,
    // so the largest row IS the total. Summing tripled a three-hour flight.
    expect(out[0]!.reports).toBe(5);
  });

  it('collapses a point written twice', async () => {
    // A flush that commits but reports failure re-sends its delta, and the
    // statement appends unconditionally, so the row can hold the same second
    // twice.
    queryMock.mockResolvedValue({
      rows: [row('abc123', H0, [
        [60, -33.0, 151.0, 30000],
        [60, -33.0, 151.0, 30000],
        [120, -33.1, 151.1, 30000],
      ])],
    });
    const out = await storedNodeTracks('node-a', 480, H0 + 5 * 60_000);
    expect(out[0]!.points).toHaveLength(2);
  });

  it('clips points outside the window even when the row overlaps it', async () => {
    queryMock.mockResolvedValue({
      rows: [row('abc123', H0, [
        [60, -33.0, 151.0, 30000],     // 04:01 — before a 04:20-04:35 window
        [1_260, -33.2, 151.2, 30000],  // 04:21 — inside
        [3_000, -33.6, 151.6, 30000],  // 04:50 — in the future of `now`
      ])],
    });
    const out = await storedNodeTracks('node-a', 15, H0 + 35 * 60_000);
    expect(out[0]!.points).toHaveLength(1);
    expect(out[0]!.points[0]![0]).toBe(H0 + 21 * 60_000);
  });

  it('drops an aircraft with nothing left inside the window', async () => {
    queryMock.mockResolvedValue({ rows: [row('abc123', H0, [[60, -33, 151, 30000]])] });
    expect(await storedNodeTracks('node-a', 15, H0 + 35 * 60_000)).toHaveLength(0);
  });

  it('returns epoch times, not offsets', async () => {
    queryMock.mockResolvedValue({ rows: [row('abc123', H0, [[600, -33, 151, 12000]])] });
    const out = await storedNodeTracks('node-a', 480, H0 + 12 * 60_000);
    expect(out[0]!.points[0]).toEqual([H0 + 600_000, -33, 151]);
  });

  it('returns nothing rather than throwing without a database', async () => {
    poolAvailable = false;
    expect(await storedNodeTracks('node-a', 480, H0)).toEqual([]);
  });
});
