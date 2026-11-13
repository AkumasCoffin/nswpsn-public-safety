/**
 * Aircraft history: writing it down a minute at a time, and reading it back.
 *
 * The properties that carry this feature, each asserted directly here:
 *
 *  - ONE ROW PER MINUTE, WRITTEN ONCE. A minute is inserted after it closes and
 *    never updated, so the write cost is one statement a minute however many
 *    aircraft are flying, and a minute on disk is final.
 *  - WHAT IS STORED IS WHAT WAS HEARD. Positions are collected as observed, not
 *    re-cut from the display trail, so stored history never gets coarser.
 *  - THE SAME URL FOR EVERY VIEWER. `at` is snapped to a fixed grid, so people
 *    scrubbing independently converge on one cacheable request.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const queryMock = vi.fn();
let poolAvailable = true;

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve(poolAvailable ? { query: queryMock } : null)),
  closePool: vi.fn(),
}));

const MIN = 60_000;
/** 2026-09-17T04:00:00Z — an exact boundary, so the arithmetic is legible. */
const T0 = Date.UTC(2026, 8, 17, 4, 0, 0);

const {
  noteAdsbPositions, noteAdsbNodePoints, maybeFlushAdsbTracks, reduceMinute,
  adsbArchivedThroughMs, adsbArchiveLateDropped, stopAdsbTrackArchive,
  _resetAdsbTrackArchive, _flushAdsbTracksNow,
} = await import('../../../src/services/adsbTrackArchive.js');
const {
  adsbHistoryAt, snapHistoryBucket, clampTrailMinutes,
  HISTORY_BUCKET_MS, HISTORY_TRAIL_MAX_MIN,
} = await import('../../../src/services/adsbHistory.js');

type P = [number, number, number, number | null];
const pt = (ms: number, lat: number, lon: number, alt: number | null = 30000): P =>
  [ms, lat, lon, alt];

/** An observed aircraft record, as the poll hands it over. */
const ac = (over: Record<string, unknown> = {}) => ({
  hex: 'abc123', callsign: null, reg: null, type: null, esTag: null,
  lat: -33.9, lon: 151.2, altFt: 30000, sources: ['adsb_lol'], estimated: false,
  ...over,
}) as never;

/** The chunk inserts issued so far: [minute iso, parsed json]. */
const chunkInserts = () => queryMock.mock.calls
  .filter((c) => String(c[0]).includes('INSERT INTO adsb_track_chunks'))
  .map((c) => ({ minute: (c[1] as unknown[])[0] as string, json: JSON.parse((c[1] as unknown[])[1] as string) }));

const identityUpserts = () => queryMock.mock.calls
  .filter((c) => String(c[0]).includes('INSERT INTO adsb_identities'));

beforeEach(() => {
  queryMock.mockReset();
  queryMock.mockResolvedValue({ rows: [] });
  poolAvailable = true;
  _resetAdsbTrackArchive();
});

describe('reducing a minute', () => {
  it('keeps a straight line to its ends plus a point every 30 s', () => {
    // 1 Hz along a straight line: Douglas-Peucker alone would keep two points;
    // the 30 s rule keeps the aircraft placeable through the minute.
    const pts: P[] = [];
    for (let s = 0; s < 60; s++) pts.push(pt(T0 + s * 1000, -33 + s * 0.001, 151));
    const out = reduceMinute(pts);
    expect(out[0]![0]).toBe(T0);
    expect(out[out.length - 1]![0]).toBe(T0 + 59_000);
    for (let i = 1; i < out.length; i++) {
      expect(out[i]![0] - out[i - 1]![0]).toBeLessThanOrEqual(30_000);
    }
    expect(out.length).toBeLessThanOrEqual(4);
  });

  it('keeps a turn', () => {
    const out = reduceMinute([
      pt(T0, -33.0, 151.0), pt(T0 + 5_000, -33.0, 151.01), pt(T0 + 10_000, -33.01, 151.01),
    ]);
    expect(out).toHaveLength(3);
  });

  it('counts one fix seen twice once, whatever order it arrived in', () => {
    const out = reduceMinute([
      pt(T0 + 10_000, -33.1, 151), pt(T0, -33, 151), pt(T0 + 10_200, -33.1, 151),
    ]);
    expect(out.map((p) => p[0])).toEqual([T0, T0 + 10_000]);
  });
});

describe('writing minutes', () => {
  it('writes a minute once it has closed, as one row holding every aircraft', async () => {
    noteAdsbPositions([ac(), ac({ hex: 'def456', lat: -34 })], T0 + 10_000);
    noteAdsbPositions([ac({ lat: -33.95 })], T0 + 40_000);
    // Still open: nothing written.
    await _flushAdsbTracksNow(T0 + 50_000);
    expect(chunkInserts()).toHaveLength(0);

    await _flushAdsbTracksNow(T0 + MIN + 20_000);
    const ins = chunkInserts();
    expect(ins).toHaveLength(1);
    expect(ins[0]!.minute).toBe(new Date(T0).toISOString());
    expect(Object.keys(ins[0]!.json).sort()).toEqual(['abc123', 'def456']);
    // Offsets are ms INTO the minute.
    expect(ins[0]!.json.abc123.p.map((p: P) => p[0])).toEqual([10_000, 40_000]);
    expect(adsbArchivedThroughMs()).toBe(T0 + MIN);
  });

  it('stamps a position with when it was heard, not when the poll saw it', async () => {
    // Seen by a poll at +40 s, but the aggregator heard it 25 s before that.
    noteAdsbPositions([ac({ ageSec: 25 })], T0 + 40_000);
    await _flushAdsbTracksNow(T0 + MIN + 20_000);
    expect(chunkInserts()[0]!.json.abc123.p.map((p: P) => p[0])).toEqual([15_000]);
  });

  it('only ever inserts — never an UPDATE of history', async () => {
    noteAdsbPositions([ac()], T0 + 10_000);
    await _flushAdsbTracksNow(T0 + 2 * MIN);
    const sql = queryMock.mock.calls.map((c) => String(c[0])).filter((s) => s.includes('adsb_track_chunks'));
    expect(sql.every((s) => !/UPDATE/i.test(s))).toBe(true);
  });

  it('waits out the grace so node fixes land in their own minute', async () => {
    noteAdsbPositions([ac()], T0 + 10_000);
    await _flushAdsbTracksNow(T0 + MIN + 5_000);
    expect(chunkInserts()).toHaveLength(0);
    noteAdsbNodePoints('abc123', [pt(T0 + 59_000, -33.5, 151.5)]);
    await _flushAdsbTracksNow(T0 + MIN + 16_000);
    expect(chunkInserts()[0]!.json.abc123.p.map((p: P) => p[0])).toEqual([10_000, 59_000]);
  });

  it('archives a 1 Hz receiver at full reduced rate, however long the trail', async () => {
    // The display trail crushes past 150 points; the archive must not care.
    for (let m = 0; m < 5; m++) {
      const pts: P[] = [];
      for (let s = 0; s < 60; s++) {
        pts.push(pt(T0 + m * MIN + s * 1000, -33 + (m * 60 + s) * 0.0005, 151 + ((m * 60 + s) % 7) * 0.001));
      }
      noteAdsbNodePoints('abc123', pts);
    }
    await _flushAdsbTracksNow(T0 + 6 * MIN);
    const ins = chunkInserts();
    expect(ins).toHaveLength(5);
    for (const i of ins) expect(i.json.abc123.p.length).toBeGreaterThanOrEqual(3);
  });

  it('drops a position for a minute already written, and counts it', async () => {
    noteAdsbPositions([ac()], T0 + 10_000);
    await _flushAdsbTracksNow(T0 + 2 * MIN);
    noteAdsbNodePoints('abc123', [pt(T0 + 30_000, -33, 151)]);
    expect(adsbArchiveLateDropped()).toBe(1);
    await _flushAdsbTracksNow(T0 + 3 * MIN);
    expect(chunkInserts()).toHaveLength(1);
  });

  it('records the sources heard in THAT minute, not since start', async () => {
    noteAdsbPositions([ac({ sources: ['adsb_lol', 'node:rx-1'] })], T0 + 10_000);
    noteAdsbPositions([ac({ sources: ['adsb_lol'], lat: -33.8 })], T0 + MIN + 10_000);
    await _flushAdsbTracksNow(T0 + 3 * MIN);
    const [first, second] = chunkInserts();
    expect(first!.json.abc123.s.sort()).toEqual(['adsb_lol', 'node:rx-1']);
    expect(second!.json.abc123.s).toEqual(['adsb_lol']);
  });

  it('stamps the best identity known, which only improves', async () => {
    noteAdsbPositions([ac({ callsign: 'QFA1', reg: 'VH-OQA' })], T0 + 10_000);
    noteAdsbPositions([ac({ callsign: null, lat: -33.8 })], T0 + 20_000);
    await _flushAdsbTracksNow(T0 + 2 * MIN);
    const e = chunkInserts()[0]!.json.abc123;
    expect(e.c).toBe('QFA1');
    expect(e.r).toBe('VH-OQA');
  });

  it('retries a failed minute, and later minutes wait behind it', async () => {
    noteAdsbPositions([ac()], T0 + 10_000);
    noteAdsbPositions([ac({ lat: -33.8 })], T0 + MIN + 10_000);
    queryMock.mockRejectedValueOnce(new Error('db away'));
    await _flushAdsbTracksNow(T0 + 3 * MIN);
    // One failed attempt; the second minute did not jump ahead of it.
    expect(chunkInserts()).toHaveLength(1);
    expect(adsbArchivedThroughMs()).toBeNull();
    await _flushAdsbTracksNow(T0 + 4 * MIN);
    expect(chunkInserts().map((i) => i.minute)).toEqual([
      new Date(T0).toISOString(), new Date(T0).toISOString(), new Date(T0 + MIN).toISOString(),
    ]);
    expect(adsbArchivedThroughMs()).toBe(T0 + 2 * MIN);
  });

  it('gives up on a minute after ten minutes of failures', async () => {
    noteAdsbPositions([ac()], T0 + 10_000);
    queryMock.mockRejectedValue(new Error('db away'));
    await _flushAdsbTracksNow(T0 + 12 * MIN);
    queryMock.mockResolvedValue({ rows: [] });
    queryMock.mockClear();
    await _flushAdsbTracksNow(T0 + 13 * MIN);
    expect(chunkInserts()).toHaveLength(0);
  });

  it('inserts under the next free seq, so two processes can share a minute', async () => {
    noteAdsbPositions([ac()], T0 + 10_000);
    await _flushAdsbTracksNow(T0 + 2 * MIN);
    const sql = String(queryMock.mock.calls.find((c) => String(c[0]).includes('INSERT INTO adsb_track_chunks'))![0]);
    expect(sql).toMatch(/COALESCE\(MAX\(seq\) \+ 1, 0\)/);
    expect(sql).toMatch(/seq >= 0/);
  });

  it('writes the open minute at shutdown and accepts nothing after', async () => {
    noteAdsbPositions([ac()], Date.now());
    await stopAdsbTrackArchive();
    expect(chunkInserts()).toHaveLength(1);
    noteAdsbPositions([ac()], Date.now());
    maybeFlushAdsbTracks(Date.now() + 5 * MIN);
    await stopAdsbTrackArchive();
    expect(chunkInserts()).toHaveLength(1);
  });

  it('is a no-op without a database', async () => {
    poolAvailable = false;
    noteAdsbPositions([ac()], T0 + 10_000);
    await expect(_flushAdsbTracksNow(T0 + 2 * MIN)).resolves.toBeUndefined();
    expect(queryMock).not.toHaveBeenCalled();
  });
});

describe('identities', () => {
  it('upserts registration and type only when they change', async () => {
    noteAdsbPositions([ac({ reg: 'VH-OQA', type: 'A388' })], T0 + 10_000);
    await _flushAdsbTracksNow(T0 + 2 * MIN);
    noteAdsbPositions([ac({ reg: 'VH-OQA', type: 'A388', lat: -33.7 })], T0 + 2 * MIN + 10_000);
    await _flushAdsbTracksNow(T0 + 4 * MIN);
    expect(identityUpserts()).toHaveLength(1);
    expect(identityUpserts()[0]![1]).toEqual([['abc123'], ['VH-OQA'], ['A388']]);
    // And the SQL itself skips a write that would change nothing.
    expect(String(identityUpserts()[0]![0])).toMatch(/IS DISTINCT FROM/);
  });

  it('sends nothing for an aircraft with neither', async () => {
    noteAdsbPositions([ac()], T0 + 10_000);
    await _flushAdsbTracksNow(T0 + 2 * MIN);
    expect(identityUpserts()).toHaveLength(0);
  });
});

describe('the shared bucket', () => {
  it('collapses viewers scrubbing to the same minute onto one request', () => {
    const a = snapHistoryBucket(T0 + 137_000);
    const b = snapHistoryBucket(T0 + 172_000);
    expect(a).toBe(b);
    expect(a).toBe(T0 + 120_000);
    expect(a % HISTORY_BUCKET_MS).toBe(0);
  });

  it('echoes the snapped instant, not the one asked for', async () => {
    const h = await adsbHistoryAt(T0 + 137_000, 15, T0 + 200_000);
    expect(h!.atMs).toBe(T0 + 120_000);
  });

  it('clamps the trail window', () => {
    expect(clampTrailMinutes(999)).toBe(HISTORY_TRAIL_MAX_MIN);
    expect(clampTrailMinutes(-5)).toBe(0);
    expect(clampTrailMinutes(Number.NaN)).toBe(15);
    expect(clampTrailMinutes(15)).toBe(15);
  });
});

describe('reading history back', () => {
  /** One stored minute, as node-postgres hands a json column over. */
  const row = (minuteMs: number, aircraft: Record<string, unknown>) =>
    ({ minute: new Date(minuteMs), aircraft });
  const entry = (p: P[], extra: Record<string, unknown> = {}) =>
    ({ c: null, r: null, t: null, e: null, s: [], p, ...extra });

  it('reads the window\'s minutes by primary key', async () => {
    await adsbHistoryAt(T0 + 30 * MIN, 15, T0 + 40 * MIN);
    const [sql, params] = queryMock.mock.calls[0]!;
    expect(String(sql)).toMatch(/FROM adsb_track_chunks/);
    expect(String(sql)).toMatch(/minute >= \$1::timestamptz/);
    expect(String(sql)).toMatch(/minute <= \$2::timestamptz/);
    expect(params).toEqual([
      new Date(T0 + 15 * MIN).toISOString(), new Date(T0 + 30 * MIN).toISOString(),
    ]);
  });

  it('stitches an aircraft back together across minutes, in time order', async () => {
    queryMock.mockResolvedValueOnce({ rows: [
      row(T0, { abc123: entry([[10_000, -33, 151, 1000], [50_000, -33.1, 151.1, 1100]]) }),
      row(T0 + MIN, { abc123: entry([[20_000, -33.2, 151.2, 1200]]) }),
    ] });
    const h = await adsbHistoryAt(T0 + 2 * MIN, 15, T0 + 10 * MIN);
    expect(h!.aircraft).toHaveLength(1);
    expect(h!.aircraft[0]!.points.map((p) => p[0])).toEqual([T0 + 10_000, T0 + 50_000, T0 + MIN + 20_000]);
  });

  it('includes a point exactly on the instant, and none after it', async () => {
    queryMock.mockResolvedValueOnce({ rows: [
      row(T0 + 4 * MIN, { abc123: entry([[0, -33, 151, null], [10_000, -33.1, 151.1, null]]) }),
    ] });
    const h = await adsbHistoryAt(T0 + 4 * MIN, 15, T0 + 10 * MIN);
    expect(h!.aircraft[0]!.points.map((p) => p[0])).toEqual([T0 + 4 * MIN]);
  });

  it('clips points before the window', async () => {
    // The window is [T0, T0 + 15 min]; a row reaching back past it is clipped.
    queryMock.mockResolvedValueOnce({ rows: [
      row(T0 - MIN, { abc123: entry([[50_000, -33, 151, null]]) }),
      row(T0, { abc123: entry([[10_000, -33.1, 151.1, null]]) }),
    ] });
    const h = await adsbHistoryAt(T0 + 15 * MIN, 15, T0 + 20 * MIN);
    expect(h!.aircraft[0]!.points.map((p) => p[0])).toEqual([T0 + 10_000]);
  });

  it('drops an aircraft whose every point fell outside the window', async () => {
    queryMock.mockResolvedValueOnce({ rows: [
      row(T0 + 5 * MIN, { abc123: entry([[30_000, -33, 151, null]]) }),
    ] });
    const h = await adsbHistoryAt(T0 + 5 * MIN, 15, T0 + 10 * MIN);
    expect(h!.aircraft).toHaveLength(0);
  });

  it('merges two rows of one minute and drops the fix they share', async () => {
    queryMock.mockResolvedValueOnce({ rows: [
      row(T0, { abc123: entry([[10_000, -33, 151, null], [30_000, -33.1, 151, null]], { s: ['adsb_lol'] }) }),
      row(T0, { abc123: entry([[30_000, -33.1, 151, null], [50_000, -33.2, 151, null]], { s: ['node:rx-1'] }) }),
    ] });
    const h = await adsbHistoryAt(T0 + MIN, 15, T0 + 10 * MIN);
    const t = h!.aircraft[0]!;
    expect(t.points.map((p) => p[0])).toEqual([T0 + 10_000, T0 + 30_000, T0 + 50_000]);
    expect(t.sources.sort()).toEqual(['adsb_lol', 'node:rx-1']);
  });

  it('reports the sources of the newest minute the aircraft was heard in', async () => {
    // A receiver that heard it ten minutes ago is not credited with it now.
    queryMock.mockResolvedValueOnce({ rows: [
      row(T0, { abc123: entry([[10_000, -33, 151, null]], { s: ['adsb_lol', 'node:rx-1'] }) }),
      row(T0 + 9 * MIN, { abc123: entry([[10_000, -33.5, 151, null]], { s: ['adsb_lol'] }) }),
    ] });
    const h = await adsbHistoryAt(T0 + 10 * MIN, 15, T0 + 20 * MIN);
    expect(h!.aircraft[0]!.sources).toEqual(['adsb_lol']);
  });

  it('takes the newest identity, keeping what a later minute forgot', async () => {
    queryMock.mockResolvedValueOnce({ rows: [
      row(T0, { abc123: entry([[10_000, -33, 151, null]], { c: 'QFA1', r: 'VH-OQA' }) }),
      row(T0 + MIN, { abc123: entry([[10_000, -33.1, 151, null]], { c: 'QFA2', r: null }) }),
    ] });
    const h = await adsbHistoryAt(T0 + 2 * MIN, 15, T0 + 10 * MIN);
    expect(h!.aircraft[0]!.callsign).toBe('QFA2');
    expect(h!.aircraft[0]!.reg).toBe('VH-OQA');
  });

  it('reports the retention window with every answer', async () => {
    const h = await adsbHistoryAt(T0, 15, T0 + MIN);
    expect(h!.retentionDays).toBeGreaterThan(0);
    expect(h!.oldestMs).toBeLessThan(T0);
  });

  it('returns null rather than throwing when there is no database', async () => {
    poolAvailable = false;
    expect(await adsbHistoryAt(T0, 15, T0 + MIN)).toBeNull();
  });

  it('says so when it truncates, keeping the most recently heard', async () => {
    const many: Record<string, unknown> = {};
    for (let i = 0; i < 4001; i++) many[`h${i}`] = entry([[i % 60_000, -33, 151, null]]);
    queryMock.mockResolvedValueOnce({ rows: [row(T0, many)] });
    const h = await adsbHistoryAt(T0 + MIN, 15, T0 + 2 * MIN);
    expect(h!.truncated).toBe(true);
    expect(h!.count).toBe(4000);
  });
});
