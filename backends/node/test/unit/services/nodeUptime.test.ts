/**
 * Node uptime.
 *
 * Presence is a sixty-bit mask per node-hour, one bit per minute, merged with
 * an OR. The OR is the whole design: a counter would have to know which minutes
 * it had already counted, and after a backend restart the in-memory tally
 * starts again at zero — adding it double-counts the overlap, taking the larger
 * loses everything before the restart.
 *
 * The two easy mistakes are counting the unreached part of the current hour as
 * downtime (which pegs every node below 100% forever) and reporting a node that
 * has never been seen as 0% (which accuses a freshly enrolled node of being
 * down).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const queryMock = vi.fn();
let poolAvailable = true;

vi.mock('../../../src/db/pool.js', () => ({
  getPool: async () => (poolAvailable ? { query: queryMock } : null),
}));

const {
  markNodeSeen, flushNodeUptime, foldUptime, nodeUptime, nodeUptimeMany,
  uptimeWindowHours, _resetNodeUptime,
} = await import('../../../src/services/nodes/nodeUptime.js');

const HOUR = 3_600_000;
const MIN = 60_000;
/** 05:30:00 exactly — half an hour into its hour. */
const NOW = Date.UTC(2026, 8, 17, 5, 30, 0);
const H = Math.floor(NOW / HOUR) * HOUR;

/** A mask with every minute of an hour set. */
const FULL = (1n << 60n) - 1n;
/** A mask with the first `n` minutes set. */
const first = (n: number) => (1n << BigInt(n)) - 1n;

beforeEach(() => {
  queryMock.mockReset();
  queryMock.mockResolvedValue({ rows: [] });
  poolAvailable = true;
  _resetNodeUptime();
});

describe('windows', () => {
  it('covers the window the pill names, and defaults safely', () => {
    expect(uptimeWindowHours('24h')).toBe(24);
    expect(uptimeWindowHours('7d')).toBe(24 * 7);
    expect(uptimeWindowHours('30d')).toBe(24 * 30);
    expect(uptimeWindowHours('nonsense')).toBe(24);
  });
});

describe('folding presence into a figure', () => {
  it('does not count the rest of the current hour as downtime', () => {
    // NOW is 05:30, so 31 minutes of this hour have elapsed. Counting the
    // other 29 as missed would cap a perfect node at about 52% and it would
    // never read 100% at any point in any hour.
    const rows = [{ hourMs: H, mask: first(31) }];
    const u = foldUptime(rows, 1, NOW);
    expect(u.minutesWindow).toBe(31);
    expect(u.minutesUp).toBe(31);
    expect(u.pct).toBe(1);
  });

  it('ignores bits claiming minutes that have not happened yet', () => {
    // A node whose clock is ahead could set them; they must not push the
    // figure above 100%.
    const u = foldUptime([{ hourMs: H, mask: FULL }], 1, NOW);
    expect(u.minutesUp).toBe(31);
    expect(u.pct).toBe(1);
  });

  it('reports a half-present hour as a half', () => {
    // Every second minute across a whole, completed hour.
    let mask = 0n;
    for (let i = 0; i < 60; i += 2) mask |= 1n << BigInt(i);
    const u = foldUptime([{ hourMs: H - HOUR, mask }], 2, NOW - 30 * MIN);
    // The previous hour is complete and half-covered; the current one is empty.
    expect(u.minutesUp).toBe(30);
    expect(u.series[0]!.pct).toBe(0.5);
  });

  it('is null, not zero, for a node never seen', () => {
    const u = foldUptime([], 24, NOW);
    expect(u.pct).toBeNull();
    expect(u.minutesUp).toBe(0);
  });

  it('returns one series point per hour of the window, oldest first', () => {
    const u = foldUptime([{ hourMs: H, mask: first(31) }], 24, NOW);
    expect(u.series).toHaveLength(24);
    const times = u.series.map((p) => Date.parse(p.hour));
    expect([...times].sort((a, b) => a - b)).toEqual(times);
    expect(u.series[23]!.pct).toBe(1);
    expect(u.series[0]!.pct).toBe(0);
  });
});

describe('the current run', () => {
  it('measures how long it has been continuously up', () => {
    // Minutes 21..30 of this hour — ten minutes ending at now.
    let mask = 0n;
    for (let i = 21; i <= 30; i += 1) mask |= 1n << BigInt(i);
    const u = foldUptime([{ hourMs: H, mask }], 24, NOW);
    expect(u.currentRunMs).toBe(10 * MIN);
  });

  it('runs back across the hour boundary', () => {
    // The mask is per hour, so a run spanning midnight-of-the-hour has to be
    // stitched from two rows or it reads as having just started.
    let prev = 0n;
    for (let i = 50; i < 60; i += 1) prev |= 1n << BigInt(i);
    let cur = 0n;
    for (let i = 0; i <= 30; i += 1) cur |= 1n << BigInt(i);
    const u = foldUptime([{ hourMs: H - HOUR, mask: prev }, { hourMs: H, mask: cur }], 24, NOW);
    expect(u.currentRunMs).toBe(41 * MIN);
  });

  it('is null when the node is not there now', () => {
    // Up earlier, gone for the last ten minutes.
    let mask = 0n;
    for (let i = 0; i <= 15; i += 1) mask |= 1n << BigInt(i);
    const u = foldUptime([{ hourMs: H, mask }], 24, NOW);
    expect(u.currentRunMs).toBeNull();
    // But the availability still reflects the time it WAS up.
    expect(u.minutesUp).toBe(16);
  });

  it('tolerates the current minute being only seconds old', () => {
    // A heartbeat lands every fifteen seconds, so the current minute can
    // legitimately be empty for a moment without the node having gone.
    let mask = 0n;
    for (let i = 25; i <= 29; i += 1) mask |= 1n << BigInt(i);
    const u = foldUptime([{ hourMs: H, mask }], 24, NOW);
    expect(u.currentRunMs).toBe(5 * MIN);
  });

  it('ends the run at the first missing minute', () => {
    // 21..24 then a gap at 25, then 26..30. Only the latest stretch counts.
    let mask = 0n;
    for (const i of [21, 22, 23, 24, 26, 27, 28, 29, 30]) mask |= 1n << BigInt(i);
    const u = foldUptime([{ hourMs: H, mask }], 24, NOW);
    expect(u.currentRunMs).toBe(5 * MIN);
  });
});

describe('writing', () => {
  it('merges with an OR so a restart cannot lose or double-count', () => {
    markNodeSeen('node-a', H + 3 * MIN);
    return flushNodeUptime().then(() => {
      const [sql, values] = queryMock.mock.calls[0]!;
      expect(String(sql)).toContain('node_uptime_hourly.seen_mask | EXCLUDED.seen_mask');
      expect(values[0]).toBe('node-a');
      // Bit 3 for the fourth minute of the hour.
      expect(values[2]).toBe(String(1n << 3n));
    });
  });

  it('sets one bit per minute, however many times a node reports', () => {
    // Heartbeats are every fifteen seconds; four in a minute is one bit.
    for (let i = 0; i < 4; i += 1) markNodeSeen('node-a', H + 3 * MIN + i * 15_000);
    markNodeSeen('node-a', H + 4 * MIN);
    return flushNodeUptime().then(() => {
      expect(queryMock.mock.calls[0]![1]![2]).toBe(String((1n << 3n) | (1n << 4n)));
    });
  });

  it('sends the mask as text, because sixty bits is not a JS number', () => {
    markNodeSeen('node-a', H + 59 * MIN);
    return flushNodeUptime().then(() => {
      const v = queryMock.mock.calls[0]![1]![2];
      expect(typeof v).toBe('string');
      expect(BigInt(v as string)).toBe(1n << 59n);
    });
  });

  it('keeps each node and each hour apart', async () => {
    markNodeSeen('node-a', H + MIN);
    markNodeSeen('node-b', H + MIN);
    markNodeSeen('node-a', H + HOUR + MIN);
    await flushNodeUptime();
    expect(queryMock).toHaveBeenCalledTimes(3);
  });

  it('is a no-op with nothing pending, and survives a failing write', async () => {
    await flushNodeUptime();
    expect(queryMock).not.toHaveBeenCalled();
    queryMock.mockRejectedValue(new Error('deadlock'));
    markNodeSeen('node-a', H);
    await expect(flushNodeUptime()).resolves.toBeUndefined();
  });

  it('ignores an empty node id rather than writing a row for it', async () => {
    markNodeSeen('', H);
    await flushNodeUptime();
    expect(queryMock).not.toHaveBeenCalled();
  });
});

describe('reading', () => {
  it('counts minutes not yet flushed, so a new node is not reported absent', async () => {
    // The flush is on a minute timer; without this a node that just arrived
    // reads as never seen for up to a minute after it did.
    markNodeSeen('node-a', NOW);
    const u = await nodeUptime('node-a', '24h', NOW);
    expect(u!.pct).not.toBeNull();
    expect(u!.currentRunMs).toBe(MIN);
  });

  it('bounds the query on the node and the window', async () => {
    await nodeUptime('node-a', '7d', NOW);
    const [sql, values] = queryMock.mock.calls[0]!;
    expect(String(sql)).toContain('FROM node_uptime_hourly');
    expect(values[0]).toBe('node-a');
    expect(Date.parse(values[1] as string)).toBe(NOW - 7 * 24 * HOUR);
  });

  it('parses a mask that came back as text', async () => {
    queryMock.mockResolvedValue({
      rows: [{ hour: new Date(H), seen_mask: String(first(31)) }],
    });
    const u = await nodeUptime('node-a', '24h', NOW);
    expect(u!.minutesUp).toBe(31);
  });

  it('answers for several nodes in one query', async () => {
    queryMock.mockResolvedValue({
      rows: [
        { node_id: 'a', hour: new Date(H), seen_mask: String(first(31)) },
        { node_id: 'b', hour: new Date(H), seen_mask: String(first(10)) },
      ],
    });
    const m = await nodeUptimeMany(['a', 'b', 'c'], '24h', NOW);
    expect(queryMock).toHaveBeenCalledTimes(1);
    expect(m.get('a')!.minutesUp).toBe(31);
    expect(m.get('b')!.minutesUp).toBe(10);
    // A node with no rows still gets an answer, and it is "unknown", not zero.
    expect(m.get('c')!.pct).toBeNull();
  });

  it('returns nothing rather than throwing without a database', async () => {
    poolAvailable = false;
    expect(await nodeUptime('node-a', '24h', NOW)).toBeNull();
    expect((await nodeUptimeMany(['a'], '24h', NOW)).size).toBe(0);
  });

  it('survives a failing read', async () => {
    queryMock.mockRejectedValue(new Error('relation does not exist'));
    await expect(nodeUptime('node-a', '24h', NOW)).resolves.toBeNull();
  });
});
