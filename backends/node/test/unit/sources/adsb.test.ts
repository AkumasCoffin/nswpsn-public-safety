/**
 * Unit tests for the ADS-B aircraft source: emergency-service
 * classifier, raw-record normalization, cross-upstream merge, Australia
 * bbox filter, circle-shard rotation, rotation holdover, and the
 * all-upstreams-down failure path.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

const fetchJsonMock = vi.fn();
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

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

vi.mock('../../../src/config.js', () => ({
  config: configMock,
  modeLabel: () => 'dev',
}));

// Sydney-ish position reused across tests.
const POS = { lat: -33.9, lon: 151.2 };

function raw(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return { hex: 'abc123', ...POS, seen_pos: 1, ...overrides };
}

describe('adsb.classifyEmergencyService', () => {
  it('tags known callsign prefixes', async () => {
    const { classifyEmergencyService } = await import('../../../src/sources/adsb.js');
    expect(classifyEmergencyService('POL30', undefined)).toBe('polair');
    expect(classifyEmergencyService('RSCU201', undefined)).toBe('rescue');
    expect(classifyEmergencyService('LIFS21', undefined)).toBe('rescue'); // Westpac Life Saver
    expect(classifyEmergencyService('WPR1', undefined)).toBe('rescue'); // Westpac Rescue
    expect(classifyEmergencyService('TOL403', undefined)).toBe('rescue'); // Toll
    expect(classifyEmergencyService('BMBR210', undefined)).toBe('firebomber');
    expect(classifyEmergencyService('FIRE07', undefined)).toBe('firebomber');
    expect(classifyEmergencyService('BDOG241', undefined)).toBe('firebomber');
    expect(classifyEmergencyService('AM223', undefined)).toBe('ambulance');
    expect(classifyEmergencyService('MDS405', undefined)).toBe('ambulance');
  });

  it('tags military via dbFlags bit 0', async () => {
    const { classifyEmergencyService } = await import('../../../src/sources/adsb.js');
    expect(classifyEmergencyService('ASY01', 1)).toBe('military');
    expect(classifyEmergencyService(null, 1)).toBe('military');
  });

  it('callsign rules win over the military flag', async () => {
    const { classifyEmergencyService } = await import('../../../src/sources/adsb.js');
    expect(classifyEmergencyService('POL32', 1)).toBe('polair');
  });

  it('returns null for civilians', async () => {
    const { classifyEmergencyService } = await import('../../../src/sources/adsb.js');
    expect(classifyEmergencyService('QFA1', undefined)).toBe(null);
    expect(classifyEmergencyService(null, undefined)).toBe(null);
    expect(classifyEmergencyService(null, 2)).toBe(null); // bit 1 ≠ military
    // POLo (no digit after POL) must not match — guards against
    // POLAR/airline codes.
    expect(classifyEmergencyService('POLAR1', undefined)).toBe(null);
  });
});

describe('adsb.normalizeAircraft', () => {
  it('maps a full record', async () => {
    const { normalizeAircraft } = await import('../../../src/sources/adsb.js');
    const a = normalizeAircraft(
      raw({
        flight: 'RSCU201 ',
        r: 'VH-TJE',
        t: 'AW139',
        alt_baro: 1500,
        gs: 120.5,
        track: 271.3,
        category: 'A7',
        squawk: '3601',
        seen_pos: 2.5,
      }),
      'adsb_lol',
    );
    expect(a).toMatchObject({
      hex: 'abc123',
      callsign: 'RSCU201',
      reg: 'VH-TJE',
      type: 'AW139',
      altFt: 1500,
      onGround: false,
      gsKt: 120.5,
      trackDeg: 271.3,
      squawk: '3601',
      emergencySquawk: false,
      esTag: 'rescue',
      ageSec: 2.5,
      sourceCount: 1,
      sources: ['adsb_lol'],
    });
  });

  it("maps alt_baro 'ground' to onGround with null altFt", async () => {
    const { normalizeAircraft } = await import('../../../src/sources/adsb.js');
    const a = normalizeAircraft(raw({ alt_baro: 'ground' }), 'adsb_fi');
    expect(a?.onGround).toBe(true);
    expect(a?.altFt).toBe(null);
  });

  it('flags emergency squawks', async () => {
    const { normalizeAircraft } = await import('../../../src/sources/adsb.js');
    expect(normalizeAircraft(raw({ squawk: '7700' }), 'x')?.emergencySquawk).toBe(true);
    expect(normalizeAircraft(raw({ emergency: 'general' }), 'x')?.emergencySquawk).toBe(
      true,
    );
    expect(normalizeAircraft(raw({ emergency: 'none' }), 'x')?.emergencySquawk).toBe(
      false,
    );
  });

  it('drops records without hex, position, or with stale seen_pos', async () => {
    const { normalizeAircraft } = await import('../../../src/sources/adsb.js');
    expect(normalizeAircraft({ ...POS, seen_pos: 1 }, 'x')).toBe(null);
    expect(normalizeAircraft({ hex: 'abc123', seen_pos: 1 }, 'x')).toBe(null);
    expect(normalizeAircraft(raw({ seen_pos: 300 }), 'x')).toBe(null);
  });

  it('treats missing optional fields as nulls', async () => {
    const { normalizeAircraft } = await import('../../../src/sources/adsb.js');
    const a = normalizeAircraft(raw(), 'x');
    expect(a).toMatchObject({
      callsign: null,
      reg: null,
      type: null,
      altFt: null,
      gsKt: null,
      trackDeg: null,
      category: null,
      squawk: null,
      esTag: null,
    });
  });
});

describe('adsb.mergeAircraft', () => {
  it('keeps the freshest position and unions sources', async () => {
    const { normalizeAircraft, mergeAircraft } = await import(
      '../../../src/sources/adsb.js'
    );
    const older = normalizeAircraft(raw({ lat: -33.0, seen_pos: 10 }), 'adsb_lol');
    const fresher = normalizeAircraft(raw({ lat: -33.5, seen_pos: 2 }), 'adsb_fi');
    const merged = mergeAircraft([older!, fresher!]);
    expect(merged).toHaveLength(1);
    expect(merged[0]).toMatchObject({
      lat: -33.5,
      ageSec: 2,
      sourceCount: 2,
    });
    expect(merged[0]?.sources.sort()).toEqual(['adsb_fi', 'adsb_lol']);
  });

  it('backfills metadata missing from the freshest record', async () => {
    const { normalizeAircraft, mergeAircraft } = await import(
      '../../../src/sources/adsb.js'
    );
    const withMeta = normalizeAircraft(
      raw({ flight: 'POL30', r: 'VH-PHX', t: 'AW139', seen_pos: 20 }),
      'adsb_lol',
    );
    const fresherNoMeta = normalizeAircraft(raw({ seen_pos: 1 }), 'airplanes_live');
    const merged = mergeAircraft([withMeta!, fresherNoMeta!]);
    expect(merged[0]).toMatchObject({
      ageSec: 1,
      callsign: 'POL30',
      reg: 'VH-PHX',
      type: 'AW139',
      esTag: 'polair',
    });
  });

  it('keeps distinct hexes separate', async () => {
    const { normalizeAircraft, mergeAircraft } = await import(
      '../../../src/sources/adsb.js'
    );
    const a = normalizeAircraft(raw({ hex: 'aaa111' }), 'x');
    const b = normalizeAircraft(raw({ hex: 'bbb222' }), 'x');
    expect(mergeAircraft([a!, b!])).toHaveLength(2);
  });
});

describe('adsb.inAuBbox', () => {
  it('accepts Australia and buffered edges, rejects far away', async () => {
    const { inAuBbox } = await import('../../../src/sources/adsb.js');
    expect(inAuBbox(-33.87, 151.21)).toBe(true); // Sydney
    expect(inAuBbox(-31.95, 115.86)).toBe(true); // Perth
    expect(inAuBbox(-12.46, 130.84)).toBe(true); // Darwin
    expect(inAuBbox(-42.88, 147.32)).toBe(true); // Hobart
    expect(inAuBbox(-9.5, 143.0)).toBe(true); // Torres Strait — inside 0.3° buffer
    expect(inAuBbox(-9.0, 143.0)).toBe(false); // PNG side — past buffer
    expect(inAuBbox(-22.3, 166.4)).toBe(false); // Nouméa
    expect(inAuBbox(-41.3, 174.8)).toBe(false); // Wellington
  });
});

describe('adsb.shardCircles', () => {
  const ids = (shards: { id: string }[][]) => shards.flat().map((c) => c.id).sort();

  it('partitions all circles every tick, however many upstreams are answering', async () => {
    const { shardCircles, CIRCLES } = await import('../../../src/sources/adsb.js');
    const all = [...CIRCLES].map((c) => c.id).sort();
    for (const tick of [0, 1, 2, 3, 7]) {
      for (const [full, probes] of [[4, 0], [3, 0], [2, 0], [1, 0], [2, 1], [1, 2]] as const) {
        const shards = shardCircles(tick, full, probes);
        expect(shards).toHaveLength(full + probes);
        expect(ids(shards)).toEqual(all); // exact partition
        for (let p = 0; p < probes; p++) expect(shards[p]).toHaveLength(1);
      }
      for (const sh of shardCircles(tick, 4)) expect(sh.length).toBe(CIRCLES.length / 4);
    }
  });

  it('rotates each circle through every answering upstream', async () => {
    const { shardCircles, CIRCLES } = await import('../../../src/sources/adsb.js');
    const first = CIRCLES[0]?.id;
    for (const full of [4, 2]) {
      const owners = [0, 1, 2, 3].map((tick) =>
        shardCircles(tick, full).findIndex((sh) => sh.some((c) => c.id === first)),
      );
      expect(new Set(owners).size).toBe(full);
    }
  });

  it('gives a capped upstream no more than its share and the rest to the others', async () => {
    const { shardCircles, CIRCLES } = await import('../../../src/sources/adsb.js');
    const all = [...CIRCLES].map((c) => c.id).sort();
    const shards = shardCircles(0, 2, 1, [3, Infinity]);
    expect(shards.map((s) => s.length)).toEqual([1, 3, CIRCLES.length - 4]);
    expect(ids(shards)).toEqual(all);
  });

  it('with every upstream capped, leaves out the circles dealt most recently', async () => {
    const { shardCircles, CIRCLES } = await import('../../../src/sources/adsb.js');
    // Circles 0-9 were dealt last poll, the rest the poll before.
    const lastDealt = CIRCLES.map((_, i) => (i < 10 ? 5 : 4));
    const shards = shardCircles(6, 2, 0, [5, 4], lastDealt);
    expect(shards.map((s) => s.length)).toEqual([5, 4]);
    const dealt = new Set(shards.flat().map((c) => c.id));
    for (let i = 0; i < 10; i++) expect(dealt.has(CIRCLES[i]!.id)).toBe(false);
  });
});

describe('adsb.trails', () => {
  beforeEach(async () => {
    const { _resetAdsbTrailsForTests, _resetAdsbHoldoverForTests } = await import('../../../src/sources/adsb.js');
    _resetAdsbTrailsForTests();
    _resetAdsbHoldoverForTests();
  });

  it('DP simplification collapses straight lines and keeps corners', async () => {
    const { simplifyTrail } = await import('../../../src/sources/adsb.js');
    // Straight line with a right-angle corner at index 4.
    const pts = [];
    for (let i = 0; i <= 4; i++) pts.push([i, -33 - i * 0.01, 151, 1000]);
    for (let i = 1; i <= 4; i++) pts.push([4 + i, -33.04, 151 + i * 0.01, 1000]);
    const out = simplifyTrail(pts as never, 0.002);
    expect(out.length).toBe(3); // start, corner, end
    expect(out[1]).toEqual([4, -33.04, 151, 1000]);
  });

  it('accumulates per tick, dedupes stationary points, snapshots rounded', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_800_000_000_000);
    const { fetchAdsbAircraft, adsbTrailsSnapshot } = await import('../../../src/sources/adsb.js');
    // tick 1
    fetchJsonMock.mockResolvedValue({ ac: [raw({ lat: -33.9000014, lon: 151.2 })] });
    let p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    await p;
    // tick 2 — moved
    vi.setSystemTime(1_800_000_015_000);
    fetchJsonMock.mockResolvedValue({ ac: [raw({ lat: -33.91, lon: 151.21 })] });
    p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    await p;
    // tick 3 — stationary (same as tick 2)
    vi.setSystemTime(1_800_000_030_000);
    fetchJsonMock.mockResolvedValue({ ac: [raw({ lat: -33.91, lon: 151.21 })] });
    p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    await p;
    const snap = adsbTrailsSnapshot();
    const trail = snap.trails['abc123'];
    expect(trail).toBeDefined();
    expect(trail.length).toBe(2); // stationary tick didn't add a point
    expect(trail[0][0]).toBe(-33.9); // 5dp rounding of -33.9000014
    vi.useRealTimers();
  });

  it('keeps absent hexes for the dropout grace then deletes them', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_800_000_000_000);
    const { fetchAdsbAircraft, adsbTrailsSnapshot } = await import('../../../src/sources/adsb.js');
    fetchJsonMock.mockResolvedValue({ ac: [raw({ hex: 'gone01', lat: -33.9, lon: 151.2 }), raw({ hex: 'stay01', lat: -33.5, lon: 151.0 })] });
    let p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    await p;
    vi.setSystemTime(1_800_000_015_000);
    fetchJsonMock.mockResolvedValue({ ac: [raw({ hex: 'gone01', lat: -33.91, lon: 151.21 }), raw({ hex: 'stay01', lat: -33.51, lon: 151.01 })] });
    p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    await p;
    // gone01 vanishes; within grace it survives.
    vi.setSystemTime(1_800_000_120_000);
    fetchJsonMock.mockResolvedValue({ ac: [raw({ hex: 'stay01', lat: -33.52, lon: 151.02 })] });
    p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    await p;
    expect(adsbTrailsSnapshot().trails['gone01']).toBeDefined();
    // Past the 10 min grace it's deleted.
    vi.setSystemTime(1_800_000_015_000 + 11 * 60_000);
    fetchJsonMock.mockResolvedValue({ ac: [raw({ hex: 'stay01', lat: -33.53, lon: 151.03 })] });
    p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    await p;
    expect(adsbTrailsSnapshot().trails['gone01']).toBeUndefined();
    expect(adsbTrailsSnapshot().trails['stay01']).toBeDefined();
    vi.useRealTimers();
  });
});

describe('adsb.fetchAdsbAircraft', () => {
  beforeEach(async () => {
    fetchJsonMock.mockReset();
    vi.useFakeTimers();
    const { _resetAdsbHoldoverForTests, _resetAdsbTrailsForTests } = await import('../../../src/sources/adsb.js');
    _resetAdsbHoldoverForTests();
    _resetAdsbTrailsForTests();
  });

  it('merges upstreams and reports per-upstream status', async () => {
    const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
    fetchJsonMock.mockImplementation((url: string) => {
      if (url.includes('adsb.lol')) {
        return Promise.resolve({ ac: [raw({ hex: 'aaa111', flight: 'QFA1' })] });
      }
      if (url.includes('adsb.fi')) {
        return Promise.resolve({ ac: [raw({ hex: 'aaa111' }), raw({ hex: 'bbb222' })] });
      }
      return Promise.reject(new Error('HTTP 503'));
    });
    const p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    const snap = await p;
    expect(snap.count).toBe(2);
    expect(snap.aircraft.map((a) => a.hex).sort()).toEqual(['aaa111', 'bbb222']);
    const byId = Object.fromEntries(snap.upstreams.map((u) => [u.id, u]));
    expect(byId['adsb_lol']).toMatchObject({ ok: true, circles_ok: 7, circles_total: 7 });
    expect(byId['airplanes_live']).toMatchObject({ ok: false, circles_ok: 0 });
    expect(byId['airplanes_live']?.error).toContain('503');
  });

  it("accepts adsb.fi's tar1090-style `aircraft` array key", async () => {
    const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
    fetchJsonMock.mockImplementation((url: string) => {
      if (url.includes('adsb.fi')) {
        return Promise.resolve({ aircraft: [raw({ hex: 'aaa111' })] });
      }
      return Promise.resolve({ ac: [] });
    });
    const p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    const snap = await p;
    expect(snap.aircraft.map((a) => a.hex)).toEqual(['aaa111']);
    expect(snap.aircraft[0]?.sources).toEqual(['adsb_fi']);
  });

  it('filters aircraft outside the Australia bbox', async () => {
    const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
    fetchJsonMock.mockResolvedValue({
      ac: [raw({ hex: 'aaa111' }), raw({ hex: 'bbb222', lat: -22.3, lon: 166.4 })],
    });
    const p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    const snap = await p;
    expect(snap.aircraft.map((a) => a.hex)).toEqual(['aaa111']);
  });

  it('sorts emergency services ahead of civilians', async () => {
    const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
    fetchJsonMock.mockResolvedValue({
      ac: [
        raw({ hex: 'ccc333', flight: 'QFA1', alt_baro: 100 }),
        raw({ hex: 'ddd444', flight: 'POL30', alt_baro: 35000 }),
      ],
    });
    const p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    const snap = await p;
    expect(snap.aircraft[0]?.hex).toBe('ddd444');
    expect(snap.emergency_count).toBe(1);
  });

  it('holds an unseen aircraft over the rotation window, then drops it', async () => {
    vi.setSystemTime(1_800_000_000_000);
    const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
    fetchJsonMock.mockResolvedValue({ ac: [raw({ hex: 'aaa111' })] });
    let p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    await p;
    // Next poll: aircraft absent (its aggregator is out of rotation) —
    // holdover keeps it, with its age advanced by the elapsed time.
    vi.setSystemTime(1_800_000_015_000);
    fetchJsonMock.mockResolvedValue({ ac: [] });
    p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    let snap = await p;
    expect(snap.aircraft.map((a) => a.hex)).toEqual(['aaa111']);
    expect(snap.aircraft[0]?.ageSec).toBe(16); // 1 s seen_pos + 15 s elapsed
    // Past 90 s it's gone for real.
    vi.setSystemTime(1_800_000_095_000);
    fetchJsonMock.mockResolvedValue({ ac: [] });
    p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    snap = await p;
    expect(snap.aircraft).toEqual([]);
  });

  it('a re-seen aircraft resets its holdover clock and position', async () => {
    vi.setSystemTime(1_800_000_000_000);
    const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
    fetchJsonMock.mockResolvedValue({ ac: [raw({ hex: 'aaa111' })] });
    let p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    await p;
    vi.setSystemTime(1_800_000_060_000);
    fetchJsonMock.mockResolvedValue({ ac: [raw({ hex: 'aaa111', lat: -34.1 })] });
    p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    await p;
    // 80 s after the refresh (140 s after first sight) it's still held.
    vi.setSystemTime(1_800_000_140_000);
    fetchJsonMock.mockResolvedValue({ ac: [] });
    p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    const snap = await p;
    expect(snap.aircraft.map((a) => a.hex)).toEqual(['aaa111']);
    expect(snap.aircraft[0]?.lat).toBe(-34.1);
  });

  it('throws only when every upstream fails', async () => {
    const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
    fetchJsonMock.mockRejectedValue(new Error('fetch failed: ETIMEDOUT'));
    const p = fetchAdsbAircraft();
    // Attach the rejection expectation before advancing timers so the
    // rejection isn't unhandled.
    const assertion = expect(p).rejects.toThrow(/all upstreams failed/);
    await vi.runAllTimersAsync();
    await assertion;
  });
});

describe('adsb upstream health', () => {
  const T0 = 1_800_000_000_000;
  const status = (code: number, retryAfterMs: number | null = null) =>
    Object.assign(new Error(`HTTP ${code} for https://x/`), { status: code, retryAfterMs });
  const callsTo = (host: string) =>
    fetchJsonMock.mock.calls.filter(([url]) => String(url).includes(host)).length;
  async function poll() {
    const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
    const p = fetchAdsbAircraft();
    p.catch(() => {}); // an all-down poll rejects before the timers finish
    await vi.runAllTimersAsync();
    return p;
  }

  beforeEach(async () => {
    fetchJsonMock.mockReset();
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    const { _resetAdsbHoldoverForTests, _resetAdsbTrailsForTests } = await import('../../../src/sources/adsb.js');
    _resetAdsbHoldoverForTests();
    _resetAdsbTrailsForTests();
  });

  it('a refusing upstream stops its sweep, then is paused and its circles shared out', async () => {
    let refuse = true;
    fetchJsonMock.mockImplementation((url: string) =>
      refuse && url.includes('airplanes.live') ? Promise.reject(status(403)) : Promise.resolve({ ac: [raw()] }),
    );
    let snap = await poll();
    expect(callsTo('airplanes.live')).toBe(1); // first 403 halts the other 6
    expect(snap.upstreams.find((u) => u.id === 'airplanes_live')).toMatchObject({ ok: false, circles_ok: 0 });

    fetchJsonMock.mockClear();
    vi.setSystemTime(T0 + 20_000);
    snap = await poll();
    expect(callsTo('airplanes.live')).toBe(0);
    expect(fetchJsonMock.mock.calls).toHaveLength(28); // every circle still fetched
    const ap = snap.upstreams.find((u) => u.id === 'airplanes_live');
    expect(ap).toMatchObject({ ok: false, circles_total: 0 });
    expect(ap?.error).toMatch(/^paused \(refused\)/);

    // An hour on it gets one probe circle; answering, it rejoins in full.
    refuse = false;
    fetchJsonMock.mockClear();
    vi.setSystemTime(T0 + 61 * 60_000);
    await poll();
    expect(callsTo('airplanes.live')).toBe(1);
    fetchJsonMock.mockClear();
    vi.setSystemTime(T0 + 62 * 60_000);
    await poll();
    expect(callsTo('airplanes.live')).toBe(7);
  });

  it('a 429 pauses for its Retry-After, and a failed probe goes back to sleep', async () => {
    fetchJsonMock.mockImplementation((url: string) =>
      url.includes('adsb.lol') ? Promise.reject(status(429, 5_000)) : Promise.resolve({ ac: [raw()] }),
    );
    await poll();
    expect(callsTo('adsb.lol')).toBe(1);
    const sweepEnd = Date.now();

    fetchJsonMock.mockClear();
    vi.setSystemTime(sweepEnd + 4_000);
    await poll();
    expect(callsTo('adsb.lol')).toBe(0);

    fetchJsonMock.mockClear();
    vi.setSystemTime(Date.now() + 6_000);
    await poll();
    expect(callsTo('adsb.lol')).toBe(1); // the probe, refused again

    fetchJsonMock.mockClear();
    vi.setSystemTime(Date.now() + 1_000);
    await poll();
    expect(callsTo('adsb.lol')).toBe(0);
  });

  it('a 429 halves the circles it is dealt, and clean polls earn them back', async () => {
    let limit = true;
    fetchJsonMock.mockImplementation((url: string) =>
      limit && url.includes('adsb.lol') ? Promise.reject(status(429)) : Promise.resolve({ ac: [raw()] }),
    );
    await poll(); // asked for 7, refused: share 3, paused 30 s
    limit = false;
    const at = (ms: number) => vi.setSystemTime(Date.now() + ms);

    fetchJsonMock.mockClear();
    at(31_000);
    await poll();
    expect(callsTo('adsb.lol')).toBe(1); // the probe

    // Rejoined at its reduced share; the other three take up the rest.
    for (let i = 0; i < 4; i++) {
      fetchJsonMock.mockClear();
      at(20_000);
      await poll();
      expect(callsTo('adsb.lol')).toBe(3);
      expect(fetchJsonMock.mock.calls).toHaveLength(28);
    }
    fetchJsonMock.mockClear();
    at(20_000);
    await poll();
    expect(callsTo('adsb.lol')).toBe(4);
  });

  it('with every upstream paused the probes cover the whole map', async () => {
    fetchJsonMock.mockImplementation(() => Promise.reject(status(403)));
    await expect(poll()).rejects.toThrow(/all upstreams failed/);
    fetchJsonMock.mockReset();
    fetchJsonMock.mockResolvedValue({ ac: [raw()] });
    vi.setSystemTime(T0 + 61 * 60_000);
    const snap = await poll();
    expect(fetchJsonMock.mock.calls).toHaveLength(28);
    expect(snap.count).toBe(1);
  });
});

describe('adsb FlyItalyADSB supplement', () => {
  beforeEach(async () => {
    fetchJsonMock.mockReset();
    vi.useFakeTimers();
    vi.setSystemTime(1_800_000_000_000);
    const { _resetAdsbHoldoverForTests, _resetAdsbTrailsForTests } = await import('../../../src/sources/adsb.js');
    _resetAdsbHoldoverForTests();
    _resetAdsbTrailsForTests();
  });
  afterEach(() => {
    delete configMock.FLYITALY_ADSB_KEY;
    vi.useRealTimers();
  });

  async function poll() {
    const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
    const p = fetchAdsbAircraft();
    await vi.runAllTimersAsync();
    return p;
  }
  const flyCalls = () =>
    fetchJsonMock.mock.calls.filter(([url]) => String(url).includes('flyitalyadsb.com'));

  it('is not polled or reported without a key', async () => {
    fetchJsonMock.mockResolvedValue({ ac: [] });
    const snap = await poll();
    expect(flyCalls()).toHaveLength(0);
    expect(snap.upstreams.map((u) => u.id)).not.toContain('flyitaly');
    // The rotation is untouched: every circle still goes to the four.
    expect(fetchJsonMock.mock.calls).toHaveLength(28);
  });

  it('sends one keyed NSW request beside the rotation and merges what it hears', async () => {
    configMock.FLYITALY_ADSB_KEY = ' fly-key ';
    fetchJsonMock.mockImplementation((url: string) =>
      Promise.resolve(url.includes('flyitalyadsb.com')
        ? { now: 0, count: 1, total: 1, ac: [raw({ hex: 'f1f1f1', flight: 'RSCU201' })] }
        : { ac: [] }));
    const snap = await poll();
    const calls = flyCalls();
    expect(calls).toHaveLength(1);
    expect(calls[0]![0]).toBe('https://api.flyitalyadsb.com/v2/lat/-30.4/lon/144.1/dist/463');
    expect(calls[0]![1]).toMatchObject({ headers: { 'X-Api-Key': 'fly-key' } });
    // The four still cover every circle themselves.
    expect(fetchJsonMock.mock.calls).toHaveLength(29);
    const a = snap.aircraft.find((x) => x.hex === 'f1f1f1');
    expect(a?.sources).toEqual(['flyitaly']);
    expect(snap.upstreams.find((u) => u.id === 'flyitaly')).toMatchObject({ ok: true, circles_ok: 1, count: 1 });
  });

  it('keeps to one request per interval and cycles the NSW quadrants', async () => {
    configMock.FLYITALY_ADSB_KEY = 'fly-key';
    fetchJsonMock.mockResolvedValue({ ac: [] });
    await poll();
    vi.setSystemTime(1_800_000_005_000); // 5 s later: inside the interval
    const quiet = await poll();
    expect(flyCalls()).toHaveLength(1);
    // Reported with the last request's outcome, not as down.
    expect(quiet.upstreams.find((u) => u.id === 'flyitaly')).toMatchObject({ ok: true, circles_total: 0 });
    for (let k = 1; k <= 4; k++) {
      vi.setSystemTime(1_800_000_000_000 + k * 12_000 + 5_000);
      await poll();
    }
    const urls = flyCalls().map(([u]) => String(u));
    expect(urls).toHaveLength(5);
    expect(urls.map((u) => u.split('/v2/')[1])).toEqual([
      'lat/-30.4/lon/144.1/dist/463',
      'lat/-30.4/lon/150.5/dist/463',
      'lat/-35.2/lon/144.1/dist/463',
      'lat/-35.2/lon/150.5/dist/463',
      'lat/-30.4/lon/144.1/dist/463',
    ]);
  });

  it('pauses on a 429 without touching the rotation', async () => {
    configMock.FLYITALY_ADSB_KEY = 'fly-key';
    fetchJsonMock.mockImplementation((url: string) => {
      if (url.includes('flyitalyadsb.com')) {
        return Promise.reject(Object.assign(new Error('HTTP 429'), { status: 429, retryAfterMs: 60_000 }));
      }
      return Promise.resolve({ ac: [raw({ hex: 'aaa111' })] });
    });
    const snap = await poll();
    expect(snap.count).toBe(1);
    expect(snap.upstreams.find((u) => u.id === 'flyitaly')).toMatchObject({ ok: false });
    vi.setSystemTime(1_800_000_030_000); // past the interval, inside Retry-After
    const paused = await poll();
    expect(flyCalls()).toHaveLength(1);
    expect(paused.upstreams.find((u) => u.id === 'flyitaly')?.error).toContain('rate_limited');
  });
});
