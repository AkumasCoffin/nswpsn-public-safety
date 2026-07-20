/**
 * Unit tests for the ADS-B aircraft source: emergency-service
 * classifier, raw-record normalization, cross-upstream merge, NSW bbox
 * filter, and the all-upstreams-down failure path.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

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

describe('adsb.inNswBbox', () => {
  it('accepts NSW and buffered border, rejects far away', async () => {
    const { inNswBbox } = await import('../../../src/sources/adsb.js');
    expect(inNswBbox(-33.87, 151.21)).toBe(true); // Sydney
    expect(inNswBbox(-31.95, 141.45)).toBe(true); // Broken Hill
    expect(inNswBbox(-37.8, 145.0)).toBe(true); // Melbourne fringe — inside 0.3° buffer, like FIRMS
    expect(inNswBbox(-38.5, 145.2)).toBe(false); // Gippsland — past buffer
    expect(inNswBbox(-27.47, 153.03)).toBe(false); // Brisbane
    expect(inNswBbox(-28.9, 154.2)).toBe(false); // off the coast, past buffer
  });
});

describe('adsb.trails', () => {
  beforeEach(async () => {
    const { _resetAdsbTrailsForTests } = await import('../../../src/sources/adsb.js');
    _resetAdsbTrailsForTests();
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
  beforeEach(() => {
    fetchJsonMock.mockReset();
    vi.useFakeTimers();
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
    expect(byId['adsb_lol']).toMatchObject({ ok: true, circles_ok: 4 });
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

  it('filters aircraft outside the NSW bbox', async () => {
    const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
    fetchJsonMock.mockResolvedValue({
      ac: [raw({ hex: 'aaa111' }), raw({ hex: 'bbb222', lat: -42.9, lon: 147.3 })],
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
