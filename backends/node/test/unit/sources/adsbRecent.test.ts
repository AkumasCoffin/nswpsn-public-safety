/**
 * The `recent` reads behind ?recent= on the live aircraft and trails, and the
 * trails snapshot catching up with receiver points between polls.
 *
 * What matters: a receiver's fresher fix replaces the snapshot's position but
 * keeps what the aggregators knew about the airframe; stale positions stay
 * out; trail points carry their time; and none of it runs the per-poll side
 * effects (missed-aircraft counts), which more frequent reads would skew.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const configMock: Record<string, unknown> = {
  LOG_LEVEL: 'warn',
  NODE_ENV: 'test',
  STATE_DIR: './test/.tmp-state',
  LIVE_PERSIST_INTERVAL_MS: 30_000,
  ARCHIVE_FLUSH_INTERVAL_MS: 30_000,
  NSWPSN_API_KEY: 'test-key',
  PORT: 3001,
  ADSB_DISABLED: false,
};

vi.mock('../../../src/config.js', () => ({
  config: configMock,
  modeLabel: () => 'dev',
}));

const noteNetworkAircraft = vi.fn();
vi.mock('../../../src/services/nodes/adsbLightcone.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/services/nodes/adsbLightcone.js')>()),
  noteNetworkAircraft,
}));

const adsb = await import('../../../src/sources/adsb.js');
const nodeStore = await import('../../../src/services/nodes/adsbNodeStore.js');
const { liveStore } = await import('../../../src/store/live.js');

function upstream(over: Partial<import('../../../src/sources/adsb.js').AdsbAircraft>) {
  return {
    hex: 'x', callsign: null, reg: null, type: null, lat: -33.9, lon: 151.2,
    altFt: 5000, onGround: false, gsKt: 200, trackDeg: 90, category: null, squawk: null,
    emergencySquawk: false, esTag: null, ageSec: 1, sourceCount: 1, sources: ['adsb_lol'],
    estimated: false, estimatedSec: null,
    ...over,
  };
}

function seedSnapshot(nowMs: number, aircraft: ReturnType<typeof upstream>[]) {
  liveStore.set('adsb_aircraft', {
    aircraft,
    count: aircraft.length,
    emergency_count: 0,
    upstreams: [],
    bbox: [112, -44.3, 154.3, -9.7],
    // Two seconds old, so every record has aged by that much.
    fetched_at: new Date(nowMs - 2_000).toISOString(),
  });
}

beforeEach(() => {
  adsb._resetAdsbTrailsForTests();
  nodeStore._resetAdsbNodeStore();
  noteNetworkAircraft.mockClear();
});

describe('adsbRecentAircraft', () => {
  it("takes a receiver's fresher fix, keeps the aggregator's airframe details, and drops stale positions", () => {
    const now = Date.now();
    seedSnapshot(now, [
      upstream({ hex: '7c6db8', lat: -33.9, lon: 151.9, ageSec: 20, reg: 'VH-ABC', type: 'B738' }),
      upstream({ hex: 'old111', ageSec: 20 }),
      upstream({ hex: 'new222', ageSec: 3 }),
      upstream({ hex: 'est333', ageSec: 1, estimated: true, estimatedSec: 50 }),
    ]);
    const recs = adsb.normalizeNodeUpload(
      { at: new Date(now).toISOString(), aircraft: [{ hex: '7c6db8', lat: -33.5, lon: 150.5, seen_pos: 1 }] },
      'node:syd',
    );
    nodeStore.recordNodeAdsbSnapshot('node-a', 'syd', recs);

    const out = adsb.adsbRecentAircraft(15, now);
    const byHex = new Map(out.aircraft.map((a) => [a.hex, a]));
    expect([...byHex.keys()].sort()).toEqual(['7c6db8', 'new222']);
    const merged = byHex.get('7c6db8')!;
    expect(merged.lat).toBe(-33.5);
    expect(merged.reg).toBe('VH-ABC');
    expect(merged.sources).toEqual(expect.arrayContaining(['adsb_lol', 'node:syd']));
    // The aggregator-only record has aged by the snapshot's two seconds.
    expect(byHex.get('new222')!.ageSec).toBeCloseTo(5, 0);
    expect(out.count).toBe(2);
    expect(noteNetworkAircraft).not.toHaveBeenCalled();
  });
});

describe('adsbRecentTrails', () => {
  it('returns only the points inside the window, each with its time', () => {
    const now = 1_800_000_000_000;
    adsb.ingestNodeTrailPoints('7c6db8', [
      [now - 30_000, -33.0, 151.0, 5000],
      [now - 5_000, -33.1, 151.0, 5100],
      [now - 1_000, -33.2, 151.0, null],
    ], now);
    const out = adsb.adsbRecentTrails(15, now);
    expect(out.trails['7c6db8']).toEqual([
      [-33.1, 151.0, 5100, now - 5_000],
      [-33.2, 151.0, null, now - 1_000],
    ]);
    expect(noteNetworkAircraft).not.toHaveBeenCalled();
  });

  it('leaves out aircraft with nothing in the window', () => {
    const now = 1_800_000_000_000;
    adsb.ingestNodeTrailPoints('quiet1', [[now - 40_000, -33.0, 151.0, 5000], [now - 30_000, -33.1, 151.0, 5000]], now);
    expect(adsb.adsbRecentTrails(15, now).trails).toEqual({});
  });
});

describe('trails snapshot between polls', () => {
  it('picks up receiver points without waiting for the next poll, at most every 2 s', () => {
    const now = 1_800_000_000_000;
    adsb.ingestNodeTrailPoints('7c6db8', [[now - 2_000, -33.0, 151.0, 5000], [now - 1_000, -33.1, 151.0, 5000]], now);
    expect(adsb.adsbTrailsSnapshot(now).trails['7c6db8']).toHaveLength(2);

    adsb.ingestNodeTrailPoints('7c6db8', [[now + 500, -33.2, 151.0, 5000]], now + 600);
    // Inside the 2 s rebuild window: the previous build is served.
    expect(adsb.adsbTrailsSnapshot(now + 1_000).trails['7c6db8']).toHaveLength(2);
    expect(adsb.adsbTrailsSnapshot(now + 2_500).trails['7c6db8']).toHaveLength(3);
  });
});
