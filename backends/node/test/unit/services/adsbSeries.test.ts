/**
 * The ADS-B charts' data.
 *
 * node_adsb_daily is one row per day, so charting it on a 24-hour window drew a
 * single point. These read node_adsb_hourly instead (migration 107).
 *
 * Two things here are easy to get subtly wrong and expensive to notice: the
 * mean signal, which is an average that must be weighted by how many readings
 * produced it, and the gaps, which must stay gaps rather than being joined into
 * a line that claims steady performance through an outage.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const queryMock = vi.fn();
let poolAvailable = true;

vi.mock('../../../src/db/pool.js', () => ({
  getPool: async () => (poolAvailable ? { query: queryMock } : null),
}));

const { adsbNodeSeries, adsbFleetSeries, adsbSeriesHours } = await import(
  '../../../src/services/nodes/adsbSeries.js'
);

const HOUR = 3_600_000;
const NOW = Date.UTC(2026, 8, 17, 12, 30, 0);
/** The hour `NOW` falls in — the series' last bucket. */
const H = Math.floor(NOW / HOUR) * HOUR;

const row = (hourMs: number, over: Record<string, unknown> = {}) => ({
  hour: new Date(hourMs),
  max_aircraft: 14,
  max_range_km: 212.5,
  msg_rate_max: 96.2,
  signal_sum: null,
  signal_n: null,
  signal_peak: null,
  positions: 4200,
  ...over,
});

beforeEach(() => {
  queryMock.mockReset();
  queryMock.mockResolvedValue({ rows: [] });
  poolAvailable = true;
});

describe('windows', () => {
  it('covers the window the pill names', () => {
    expect(adsbSeriesHours('24h')).toBe(24);
    expect(adsbSeriesHours('7d')).toBe(24 * 7);
    expect(adsbSeriesHours('30d')).toBe(24 * 30);
  });

  it('falls back to a day for anything unrecognised', () => {
    // The window arrives from a query string, so it cannot be trusted to be
    // one of the three.
    expect(adsbSeriesHours('all')).toBe(24);
    expect(adsbSeriesHours('')).toBe(24);
  });
});

describe('one receiver', () => {
  it('returns a full window even from a single hour of data', async () => {
    // The axis must not stretch and shrink as data arrives, and a receiver
    // switched on an hour ago should read as mostly-absent, not as complete.
    queryMock.mockResolvedValue({ rows: [row(H)] });
    const out = await adsbNodeSeries('node-a', '24h', NOW);
    expect(out).toHaveLength(24);
    expect(out[23]!.aircraft).toBe(14);
    expect(out[0]!.aircraft).toBeNull();
  });

  it('leaves an outage as a hole rather than a straight line', async () => {
    // Null, not zero: a line drawn through the gap claims the receiver was
    // working steadily for the hours it was actually dead.
    queryMock.mockResolvedValue({ rows: [row(H - 3 * HOUR), row(H)] });
    const out = await adsbNodeSeries('node-a', '24h', NOW);
    const missing = out.filter((p) => p.aircraft === null);
    expect(missing.length).toBe(22);
    expect(out[out.length - 2]!.aircraft).toBeNull();
  });

  it('is in time order, oldest first', async () => {
    queryMock.mockResolvedValue({ rows: [row(H - 2 * HOUR), row(H)] });
    const out = await adsbNodeSeries('node-a', '24h', NOW);
    const times = out.map((p) => Date.parse(p.hour));
    expect([...times].sort((a, b) => a - b)).toEqual(times);
  });

  it('bounds the query on the node and the window', async () => {
    await adsbNodeSeries('node-a', '7d', NOW);
    const [sql, values] = queryMock.mock.calls[0]!;
    expect(String(sql)).toContain('FROM node_adsb_hourly');
    expect(values[0]).toBe('node-a');
    expect(Date.parse(values[1] as string)).toBe(NOW - 7 * 24 * HOUR);
  });

  it('returns nothing rather than throwing without a database', async () => {
    poolAvailable = false;
    expect(await adsbNodeSeries('node-a', '24h', NOW)).toEqual([]);
  });

  it('survives a failing read', async () => {
    queryMock.mockRejectedValue(new Error('relation does not exist'));
    await expect(adsbNodeSeries('node-a', '24h', NOW)).resolves.toEqual([]);
  });
});

describe('the mean signal', () => {
  it('is weighted by the readings that produced it', async () => {
    // Stored as a running sum and its divisor precisely so this works: an
    // average of averages would weight a quiet minute like a busy one.
    queryMock.mockResolvedValue({
      rows: [row(H, { signal_sum: -1200, signal_n: 100 })],
    });
    const out = await adsbNodeSeries('node-a', '24h', NOW);
    expect(out[23]!.signalDbfs).toBeCloseTo(-12, 6);
  });

  it('is null, never zero, when nothing reported it', async () => {
    // dBFS is negative and zero means full scale, so a missing reading drawn
    // as 0 would be the strongest signal the chart could possibly show —
    // exactly backwards. Every receiver on an agent before 0.1.6 sends none.
    queryMock.mockResolvedValue({ rows: [row(H)] });
    const out = await adsbNodeSeries('node-a', '24h', NOW);
    expect(out[23]!.signalDbfs).toBeNull();
    expect(out[23]!.signalPeakDbfs).toBeNull();
    // The metrics collected all along are unaffected by signal being absent.
    expect(out[23]!.aircraft).toBe(14);
  });

  it('ignores a zero divisor rather than dividing by it', async () => {
    queryMock.mockResolvedValue({ rows: [row(H, { signal_sum: -50, signal_n: 0 })] });
    const out = await adsbNodeSeries('node-a', '24h', NOW);
    expect(out[23]!.signalDbfs).toBeNull();
  });
});

describe('the fleet', () => {
  it('takes the best of the maxima and the sum of the counters', async () => {
    // Two receivers reaching 200 km have not together reached 400, and both
    // seeing the same aircraft have not seen it twice — but their position
    // reports really do add up.
    await adsbFleetSeries('24h', NOW);
    const sql = String(queryMock.mock.calls[0]![0]);
    expect(sql).toContain('MAX(max_range_km)');
    expect(sql).toContain('MAX(max_aircraft)');
    expect(sql).toContain('SUM(positions)');
    expect(sql).toContain('GROUP BY hour');
    // The mean's two halves must both be summed, or the fleet average drifts.
    expect(sql).toContain('SUM(signal_sum)');
    expect(sql).toContain('SUM(signal_n)');
  });

  it('pads its window the same way', async () => {
    queryMock.mockResolvedValue({ rows: [row(H)] });
    const out = await adsbFleetSeries('24h', NOW);
    expect(out).toHaveLength(24);
  });

  it('is not scoped to a node', async () => {
    await adsbFleetSeries('24h', NOW);
    const [sql] = queryMock.mock.calls[0]!;
    expect(String(sql)).not.toContain('node_id = $');
  });
});
