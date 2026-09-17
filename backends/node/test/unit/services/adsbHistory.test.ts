/**
 * Persisted aircraft tracks: writing them down, and reading them back.
 *
 * Two properties carry this feature and both are asserted directly here:
 *
 *  - ONE ROW PER AIRCRAFT PER HOUR. Only the current hour and the one before
 *    it are ever written, so a closed hour is immutable and the write cost
 *    stays flat however long a flight lasts.
 *  - THE SAME URL FOR EVERY VIEWER. `at` is snapped to a fixed grid, so people
 *    scrubbing independently converge on one cacheable request instead of N
 *    private ones. That is the whole answer to "multiple users on the
 *    frontend", so it is a test, not a comment.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const queryMock = vi.fn();
let poolAvailable = true;

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve(poolAvailable ? { query: queryMock } : null)),
  closePool: vi.fn(),
}));

const HOUR = 3_600_000;
/** 2026-09-17T04:00:00Z — an exact hour boundary, so the arithmetic is legible. */
const H0 = Date.UTC(2026, 8, 17, 4, 0, 0);

const {
  sliceTrailByHour, flushAdsbTracks, noteAdsbIdentities, _resetAdsbTrackArchive,
} = await import('../../../src/services/adsbTrackArchive.js');
const {
  adsbHistoryAt, snapHistoryBucket, clampTrailMinutes,
  HISTORY_BUCKET_MS, HISTORY_TRAIL_MAX_MIN,
} = await import('../../../src/services/adsbHistory.js');

/** A trail point as sources/adsb.ts holds it: [epochMs, lat, lon, altFt]. */
const pt = (ms: number, lat: number, lon: number, alt: number | null = 30000) =>
  [ms, lat, lon, alt] as [number, number, number, number | null];

beforeEach(() => {
  queryMock.mockReset();
  queryMock.mockResolvedValue({ rows: [] });
  poolAvailable = true;
  _resetAdsbTrackArchive();
});

describe('cutting a trail into hours', () => {
  it('splits one flight across the hour boundary', () => {
    const points = [
      pt(H0 + 50 * 60_000, -33.0, 151.0),   // 04:50
      pt(H0 + 58 * 60_000, -33.2, 151.2),   // 04:58
      pt(H0 + 62 * 60_000, -33.4, 151.4),   // 05:02
    ];
    const slices = sliceTrailByHour('abc123', points, H0 + 65 * 60_000);
    expect(slices).toHaveLength(2);

    const first = slices.find((s) => s.hourMs === H0)!;
    const second = slices.find((s) => s.hourMs === H0 + HOUR)!;
    expect(first.points).toHaveLength(2);
    expect(second.points).toHaveLength(1);
    // Offsets are seconds INTO their own hour, not into the flight.
    expect(first.points[0]![0]).toBe(50 * 60);
    expect(second.points[0]![0]).toBe(2 * 60);
  });

  it('leaves hours older than the last one alone', () => {
    // The point of the hour bucketing: a long flight rewrites only the hour it
    // is in (plus the one just closed), not its whole track, every minute.
    const points = [
      pt(H0 - 3 * HOUR, -31.0, 149.0),
      pt(H0 - 2 * HOUR, -32.0, 150.0),
      pt(H0 - HOUR + 60_000, -32.5, 150.5),
      pt(H0 + 60_000, -33.0, 151.0),
    ];
    const slices = sliceTrailByHour('abc123', points, H0 + 120_000);
    expect(slices.map((s) => s.hourMs).sort()).toEqual([H0 - HOUR, H0]);
    // A ten-hour flight costs two rows a flush, not ten.
    expect(slices).toHaveLength(2);
  });

  it('still writes the hour that just closed', () => {
    // A flush landing after the top of the hour would otherwise leave the
    // closed hour permanently missing its final minutes.
    const points = [pt(H0 + 59 * 60_000, -33.0, 151.0)];
    const slices = sliceTrailByHour('abc123', points, H0 + 61 * 60_000);
    expect(slices.map((s) => s.hourMs)).toEqual([H0]);
  });

  it('reports the real span of each slice', () => {
    const points = [pt(H0 + 600_000, -33, 151), pt(H0 + 1_200_000, -33.1, 151.1)];
    const [s] = sliceTrailByHour('abc123', points, H0 + 1_300_000);
    expect(s!.firstMs).toBe(H0 + 600_000);
    expect(s!.lastMs).toBe(H0 + 1_200_000);
  });

  it('keeps a single-point slice', () => {
    // Enough to say the aircraft was there, which is what a coverage picture
    // is for even when it is not enough to draw a line.
    expect(sliceTrailByHour('abc123', [pt(H0 + 60_000, -33, 151)], H0 + 120_000))
      .toHaveLength(1);
  });
});

describe('the flush', () => {
  const track = (hex: string, points: Array<[number, number, number, number | null]>) =>
    ({ hex, points });

  it('writes one row per aircraft-hour and carries identity', async () => {
    noteAdsbIdentities([{
      hex: 'abc123', callsign: 'QFA512', reg: 'VH-VZZ', type: 'B738',
      esTag: null, sources: ['adsb.lol', 'node:adsb-syd-01'],
      lat: -33, lon: 151, altFt: 30000, onGround: false, gsKt: 400,
      trackDeg: 90, category: 'A3', squawk: null, emergencySquawk: false,
      ageSec: 1, sourceCount: 2, estimated: false, estimatedSec: null,
    }]);

    const written = await flushAdsbTracks(
      [track('abc123', [pt(H0 + 60_000, -33, 151), pt(H0 + 120_000, -33.1, 151.1)])],
      H0 + 180_000,
    );
    expect(written).toBe(1);

    const [sql, values] = queryMock.mock.calls[0]!;
    expect(sql).toContain('INSERT INTO adsb_tracks');
    expect(sql).toContain('ON CONFLICT (hex, hour_bucket) DO UPDATE');
    expect(values[0]).toBe('abc123');
    expect(values[4]).toBe('QFA512');
    expect(values[5]).toBe('VH-VZZ');
    // sources are carried so "only my receiver" survives a scrub back.
    expect(values[8]).toEqual(['adsb.lol', 'node:adsb-syd-01']);
    expect(JSON.parse(values[9] as string)).toEqual([[60, -33, 151, 30000], [120, -33.1, 151.1, 30000]]);
  });

  it('never blanks out an identity it already knows', async () => {
    const base = {
      hex: 'abc123', reg: null, type: null, esTag: null, sources: ['adsb.lol'],
      lat: -33, lon: 151, altFt: 30000, onGround: false, gsKt: 400,
      trackDeg: 90, category: null, squawk: null, emergencySquawk: false,
      ageSec: 1, sourceCount: 1, estimated: false, estimatedSec: null,
    };
    noteAdsbIdentities([{ ...base, callsign: 'QFA512' } as never]);
    // A later poll where the aggregator dropped the callsign.
    noteAdsbIdentities([{ ...base, callsign: null } as never]);

    await flushAdsbTracks([track('abc123', [pt(H0 + 60_000, -33, 151)])], H0 + 120_000);
    expect(queryMock.mock.calls[0]![1][4]).toBe('QFA512');
  });

  it('unions sources across polls', async () => {
    const base = {
      hex: 'abc123', callsign: null, reg: null, type: null, esTag: null,
      lat: -33, lon: 151, altFt: 30000, onGround: false, gsKt: 400,
      trackDeg: 90, category: null, squawk: null, emergencySquawk: false,
      ageSec: 1, sourceCount: 1, estimated: false, estimatedSec: null,
    };
    noteAdsbIdentities([{ ...base, sources: ['adsb.lol'] } as never]);
    noteAdsbIdentities([{ ...base, sources: ['node:rx-1'] } as never]);
    await flushAdsbTracks([track('abc123', [pt(H0 + 60_000, -33, 151)])], H0 + 120_000);
    expect(queryMock.mock.calls[0]![1][8]).toEqual(['adsb.lol', 'node:rx-1']);
  });

  it('appends rather than replaces only when the slices are disjoint', async () => {
    // The restart case. The conflict clause has to keep what was recorded
    // before the process died, without letting a normal re-cut of the same
    // hour double itself up.
    await flushAdsbTracks([track('a1', [pt(H0 + 60_000, -33, 151)])], H0 + 120_000);
    const sql = queryMock.mock.calls[0]![0] as string;
    expect(sql).toContain('WHEN EXCLUDED.first_seen > adsb_tracks.last_seen');
    expect(sql).toContain('adsb_tracks.points || EXCLUDED.points');
    expect(sql).toContain('ELSE EXCLUDED.points');
    // ...and it cannot grow without bound if a process restart-loops.
    expect(sql).toMatch(/jsonb_array_length\(adsb_tracks\.points\) < \d+/);
  });

  it('is a no-op without a database', async () => {
    poolAvailable = false;
    expect(await flushAdsbTracks([track('a1', [pt(H0, -33, 151)])], H0)).toBe(0);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('writes nothing when no aircraft are being trailed', async () => {
    expect(await flushAdsbTracks([], H0)).toBe(0);
    expect(queryMock).not.toHaveBeenCalled();
  });

  it('survives a failing write without throwing', async () => {
    queryMock.mockRejectedValue(new Error('deadlock detected'));
    await expect(
      flushAdsbTracks([track('a1', [pt(H0 + 60_000, -33, 151)])], H0 + 120_000),
    ).resolves.toBe(0);
  });
});

describe('the shared bucket', () => {
  it('collapses viewers scrubbing to the same minute onto one request', async () => {
    // THE multi-user property. Two people a few seconds apart must produce an
    // identical `atMs`, or every viewer gets a private cache entry.
    const a = snapHistoryBucket(H0 + 137_000);
    const b = snapHistoryBucket(H0 + 172_000);
    expect(a).toBe(b);
    expect(a).toBe(H0 + 120_000);
    expect(a % HISTORY_BUCKET_MS).toBe(0);
  });

  it('echoes the snapped instant, not the one asked for', async () => {
    const h = await adsbHistoryAt(H0 + 137_000, 15, H0 + 200_000);
    expect(h!.atMs).toBe(H0 + 120_000);
  });

  it('clamps the trail window', () => {
    expect(clampTrailMinutes(999)).toBe(HISTORY_TRAIL_MAX_MIN);
    expect(clampTrailMinutes(-5)).toBe(0);
    expect(clampTrailMinutes(Number.NaN)).toBe(15);
    expect(clampTrailMinutes(15)).toBe(15);
  });
});

describe('reading history back', () => {
  /** One stored row, as node-postgres hands it over. */
  const row = (
    hex: string, hourMs: number,
    points: Array<[number, number, number, number | null]>,
    extra: Record<string, unknown> = {},
  ) => ({
    hex, hour_bucket: new Date(hourMs),
    callsign: null, reg: null, type: null, es_tag: null, sources: [],
    points, ...extra,
  });

  it('bounds on the hour first, then the window', async () => {
    await adsbHistoryAt(H0 + 30 * 60_000, 15, H0 + 31 * 60_000);
    const [sql, values] = queryMock.mock.calls[0]!;
    expect(sql).toContain('hour_bucket >=');
    expect(sql).toContain('hour_bucket <=');
    expect(sql).toContain('last_seen  >=');
    expect(sql).toContain('first_seen <=');
    // 04:30 with a 15-minute trail lives entirely inside the 04:00 hour.
    expect(values[0]).toBe(new Date(H0).toISOString());
    expect(values[1]).toBe(new Date(H0).toISOString());
  });

  it('spans two hour buckets when the window straddles the boundary', async () => {
    await adsbHistoryAt(H0 + 5 * 60_000, 15, H0 + 6 * 60_000);
    const values = queryMock.mock.calls[0]![1];
    expect(values[0]).toBe(new Date(H0 - HOUR).toISOString());
    expect(values[1]).toBe(new Date(H0).toISOString());
  });

  it('stitches an aircraft back together across the hour boundary', async () => {
    // The hour split is a storage detail; nothing downstream should see it.
    queryMock.mockResolvedValue({
      rows: [
        row('abc123', H0 - HOUR, [[3500, -33.0, 151.0, 30000]], { callsign: null }),
        row('abc123', H0, [[60, -33.2, 151.2, 31000]], { callsign: 'QFA512' }),
      ],
    });
    const h = await adsbHistoryAt(H0 + 5 * 60_000, 15, H0 + 6 * 60_000);
    expect(h!.aircraft).toHaveLength(1);
    const t = h!.aircraft[0]!;
    expect(t.points).toHaveLength(2);
    expect(t.points[0]![0]).toBeLessThan(t.points[1]![0]);
    // The later hour knew the callsign the earlier one did not.
    expect(t.callsign).toBe('QFA512');
  });

  it('clips points outside the window even when the row overlaps it', async () => {
    // The row-level bounds are coarse: the points live inside a jsonb array,
    // so an hour that touches the window brings in points that do not.
    queryMock.mockResolvedValue({
      rows: [row('abc123', H0, [
        [60, -33.0, 151.0, 30000],     // 04:01 — before a 04:20–04:35 window
        [1_260, -33.2, 151.2, 30000],  // 04:21 — inside
        [2_100, -33.4, 151.4, 30000],  // 04:35 — inside (the instant itself)
        [3_000, -33.6, 151.6, 30000],  // 04:50 — after
      ])],
    });
    const h = await adsbHistoryAt(H0 + 35 * 60_000, 15, H0 + 40 * 60_000);
    const pts = h!.aircraft[0]!.points;
    expect(pts).toHaveLength(2);
    expect(pts[0]![0]).toBe(H0 + 21 * 60_000);
    expect(pts[1]![0]).toBe(H0 + 35 * 60_000);
  });

  it('drops an aircraft whose every point fell outside the window', async () => {
    queryMock.mockResolvedValue({
      rows: [row('abc123', H0, [[60, -33.0, 151.0, 30000]])],
    });
    const h = await adsbHistoryAt(H0 + 35 * 60_000, 15, H0 + 40 * 60_000);
    expect(h!.aircraft).toHaveLength(0);
    expect(h!.count).toBe(0);
  });

  it('converts stored offsets back to absolute times', async () => {
    queryMock.mockResolvedValue({ rows: [row('abc123', H0, [[600, -33, 151, 12000]])] });
    const h = await adsbHistoryAt(H0 + 12 * 60_000, 15, H0 + 13 * 60_000);
    expect(h!.aircraft[0]!.points[0]).toEqual([H0 + 600_000, -33, 151, 12000]);
  });

  it('carries sources so the "my receiver" filter survives a scrub', async () => {
    queryMock.mockResolvedValue({
      rows: [row('abc123', H0, [[600, -33, 151, 12000]], { sources: ['node:rx-1'] })],
    });
    const h = await adsbHistoryAt(H0 + 12 * 60_000, 15, H0 + 13 * 60_000);
    expect(h!.aircraft[0]!.sources).toEqual(['node:rx-1']);
  });

  it('reports the retention window with every answer', async () => {
    const h = await adsbHistoryAt(H0, 15, H0 + 60_000);
    expect(h!.retentionDays).toBeGreaterThan(0);
    expect(h!.oldestMs).toBeLessThan(H0);
  });

  it('returns null rather than throwing when there is no database', async () => {
    poolAvailable = false;
    expect(await adsbHistoryAt(H0, 15, H0)).toBeNull();
  });

  it('says so when it truncates', async () => {
    const rows = [];
    for (let i = 0; i < 4100; i += 1) {
      rows.push(row(`h${i}`, H0, [[600 + (i % 60), -33, 151, 12000]]));
    }
    queryMock.mockResolvedValue({ rows });
    const h = await adsbHistoryAt(H0 + 12 * 60_000, 15, H0 + 13 * 60_000);
    expect(h!.truncated).toBe(true);
    expect(h!.count).toBe(4000);
    expect(h!.aircraft).toHaveLength(4000);
  });

  it('does not claim truncation when it fits', async () => {
    queryMock.mockResolvedValue({ rows: [row('abc123', H0, [[600, -33, 151, 12000]])] });
    const h = await adsbHistoryAt(H0 + 12 * 60_000, 15, H0 + 13 * 60_000);
    expect(h!.truncated).toBe(false);
  });
});

describe('which receiver heard it, at the instant being viewed', () => {
  /** A stored row, as the archive writes one. */
  const row = (hourMs: number, sources: string[], secs: number[]) => ({
    hex: 'abc123', hour_bucket: new Date(hourMs),
    callsign: null, reg: null, type: null, es_tag: null,
    sources,
    points: secs.map((s) => [s, -33.8, 151.2, 30000]),
  });

  it('reports only the sources from the hour on screen', async () => {
    // The reported fault: the map's receiver filter showed aircraft a node had
    // heard at some OTHER point in the trail. Sources are recorded per hour and
    // the stitch unioned every hour the trail touched, so scrubbing to 04:05
    // inherited attribution from an aircraft the node only caught at 05:55.
    queryMock.mockResolvedValue({
      rows: [
        row(H0, ['agg:one'], [300]),
        row(H0 + HOUR, ['agg:one', 'node:ours'], [3300]),
      ],
    });
    const d = await adsbHistoryAt(H0 + 5 * 60_000, 120, H0 + 2 * HOUR);
    expect(d!.aircraft[0]!.sources).toEqual(['agg:one']);
  });

  it('still reports the node in the hour it actually heard it', async () => {
    queryMock.mockResolvedValue({
      rows: [
        row(H0, ['agg:one'], [300]),
        row(H0 + HOUR, ['agg:one', 'node:ours'], [3300]),
      ],
    });
    const d = await adsbHistoryAt(H0 + HOUR + 55 * 60_000, 120, H0 + 2 * HOUR);
    expect(d!.aircraft[0]!.sources).toContain('node:ours');
  });

  it('keeps the union when the viewed hour has no row of its own', async () => {
    // A trail can reach into the window from an earlier hour. There is no
    // attribution for an hour that recorded none, and the union is the only
    // thing left to say — better than claiming nothing heard it.
    queryMock.mockResolvedValue({
      rows: [row(H0, ['agg:one', 'node:ours'], [3599])],
    });
    const d = await adsbHistoryAt(H0 + HOUR + 60_000, 120, H0 + 2 * HOUR);
    expect(d!.aircraft[0]!.sources).toEqual(['agg:one', 'node:ours']);
  });

  it('de-duplicates a repeated source', async () => {
    queryMock.mockResolvedValue({
      rows: [row(H0, ['node:ours', 'node:ours', 'agg:one'], [300])],
    });
    const d = await adsbHistoryAt(H0 + 5 * 60_000, 120, H0 + HOUR);
    expect(d!.aircraft[0]!.sources).toEqual(['node:ours', 'agg:one']);
  });
});
