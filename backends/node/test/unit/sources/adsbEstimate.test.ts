/**
 * Dead reckoning for aircraft that stop reporting.
 *
 * `applyHoldover` used to re-emit a vanished aircraft frozen at its last
 * position. It now projects it along its last known heading and speed, flagged
 * `estimated`. That makes it the only field on an AdsbAircraft that is not an
 * observation, so the tests that matter are the ones about where the guess is
 * NOT allowed to go:
 *
 *   - it must never be recorded into a trail, and therefore never into the
 *     persisted history built on top of trails;
 *   - it must not be made at all without a heading and a real groundspeed;
 *   - it must not run past the horizon, or off the edge of the region.
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

const T0 = 1_800_000_000_000;
const POS = { lat: -33.9, lon: 151.2 };

/** An airborne, fast-moving, east-bound target — the estimable case. */
function flying(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    hex: 'abc123', ...POS, seen_pos: 0,
    alt_baro: 34000, gs: 450, track: 90,
    ...overrides,
  };
}

/** Run one poll at `atMs` serving `ac`, and return the merged snapshot. */
async function poll(atMs: number, ac: Array<Record<string, unknown>>) {
  vi.setSystemTime(atMs);
  fetchJsonMock.mockResolvedValue({ ac });
  const { fetchAdsbAircraft } = await import('../../../src/sources/adsb.js');
  const p = fetchAdsbAircraft();
  await vi.runAllTimersAsync();
  return p;
}

beforeEach(async () => {
  const { _resetAdsbTrailsForTests, _resetAdsbHoldoverForTests } = await import(
    '../../../src/sources/adsb.js'
  );
  _resetAdsbTrailsForTests();
  _resetAdsbHoldoverForTests();
  fetchJsonMock.mockReset();
  vi.useFakeTimers();
});
afterEach(() => vi.useRealTimers());

describe('estimated positions never reach the trail', () => {
  it('records no trail point while an aircraft is being estimated', async () => {
    // THE guard. A frozen holdover record was harmless because updateTrails
    // skips a point identical to the last one — an estimated record moves, so
    // without the filter the trail would fill with positions nobody observed,
    // and the persisted history built on trails would inherit them.
    const { adsbTrailsSnapshot } = await import('../../../src/sources/adsb.js');

    await poll(T0, [flying()]);
    await poll(T0 + 15_000, [flying({ lat: -33.9, lon: 151.25 })]);
    const observed = adsbTrailsSnapshot().trails['abc123']!;
    expect(observed.length).toBe(2);

    // Now it goes quiet for a minute of polls.
    for (let i = 1; i <= 4; i += 1) {
      await poll(T0 + 15_000 + i * 15_000, []);
    }

    const snap = await poll(T0 + 90_000, []);
    const rec = snap.aircraft.find((a) => a.hex === 'abc123');
    expect(rec?.estimated).toBe(true);
    // ...and the trail has not grown by a single fabricated point.
    expect(adsbTrailsSnapshot().trails['abc123']!.length).toBe(2);
  });
});

describe('the projection itself', () => {
  it('advances along the reported track at the reported speed', async () => {
    await poll(T0, [flying()]);                       // 450 kt, due east
    const snap = await poll(T0 + 60_000, []);          // one minute later
    const rec = snap.aircraft.find((a) => a.hex === 'abc123')!;

    expect(rec.estimated).toBe(true);
    expect(rec.estimatedSec).toBe(60);
    // 450 kt for 60 s is 7.5 nm. Due east: latitude unchanged, longitude
    // advanced by 7.5 / (60 * cos(-33.9°)) degrees.
    expect(rec.lat).toBeCloseTo(POS.lat, 6);
    const expectedDLon = 7.5 / (60 * Math.cos((POS.lat * Math.PI) / 180));
    expect(rec.lon - POS.lon).toBeCloseTo(expectedDLon, 5);
    // ~13.9 km due east.
    const km = (rec.lon - POS.lon) * 111.32 * Math.cos((POS.lat * Math.PI) / 180);
    expect(km).toBeGreaterThan(13.5);
    expect(km).toBeLessThan(14.3);
  });

  it('flies north on a track of 000', async () => {
    await poll(T0, [flying({ track: 0, gs: 600 })]);
    const snap = await poll(T0 + 60_000, []);
    const rec = snap.aircraft.find((a) => a.hex === 'abc123')!;
    // 600 kt for 60 s is 10 nm = 10/60 of a degree of latitude, northward.
    expect(rec.lat - POS.lat).toBeCloseTo(10 / 60, 5);
    expect(rec.lon).toBeCloseTo(POS.lon, 6);
  });

  it('keeps the real record while the aircraft is still reporting', async () => {
    const snap = await poll(T0, [flying()]);
    const rec = snap.aircraft.find((a) => a.hex === 'abc123')!;
    expect(rec.estimated).toBe(false);
    expect(rec.estimatedSec).toBeNull();
    expect(rec.lat).toBe(POS.lat);
    expect(rec.lon).toBe(POS.lon);
  });

  it('drops the estimate once the horizon is passed', async () => {
    await poll(T0, [flying()]);
    const inside = await poll(T0 + 170_000, []);
    expect(inside.aircraft.some((a) => a.hex === 'abc123')).toBe(true);
    const outside = await poll(T0 + 190_000, []);
    expect(outside.aircraft.some((a) => a.hex === 'abc123')).toBe(false);
  });
});

describe('what may not be projected', () => {
  it('freezes an aircraft with no reported track', async () => {
    await poll(T0, [flying({ track: undefined })]);
    const snap = await poll(T0 + 60_000, []);
    const rec = snap.aircraft.find((a) => a.hex === 'abc123')!;
    expect(rec.estimated).toBe(false);
    expect(rec.lat).toBe(POS.lat);
    expect(rec.lon).toBe(POS.lon);
  });

  it('freezes an aircraft with no reported groundspeed', async () => {
    await poll(T0, [flying({ gs: undefined })]);
    const snap = await poll(T0 + 60_000, []);
    expect(snap.aircraft.find((a) => a.hex === 'abc123')!.estimated).toBe(false);
  });

  it('freezes a slow target rather than projecting its noise', async () => {
    // 20 kt is a taxiing aircraft or a drifting fix; its reported heading can
    // swing through 180 degrees between messages.
    await poll(T0, [flying({ gs: 20 })]);
    const snap = await poll(T0 + 60_000, []);
    const rec = snap.aircraft.find((a) => a.hex === 'abc123')!;
    expect(rec.estimated).toBe(false);
    expect(rec.lon).toBe(POS.lon);
  });

  it('freezes an aircraft that was on the ground', async () => {
    await poll(T0, [flying({ alt_baro: 'ground' })]);
    const snap = await poll(T0 + 60_000, []);
    expect(snap.aircraft.find((a) => a.hex === 'abc123')!.estimated).toBe(false);
  });

  it('keeps the shorter 90s window for anything it cannot project', async () => {
    // A frozen dot earns less patience than a projection: it says nothing
    // beyond "it was here a while ago".
    await poll(T0, [flying({ track: undefined })]);
    expect((await poll(T0 + 80_000, [])).aircraft.some((a) => a.hex === 'abc123')).toBe(true);
    expect((await poll(T0 + 100_000, [])).aircraft.some((a) => a.hex === 'abc123')).toBe(false);
  });

  it('forgets an estimate that leaves the region', async () => {
    // The bbox filter runs BEFORE holdover, so nothing downstream would catch
    // a projection that flew off the edge of the map.
    // The east edge is 154.3 plus a 0.3 buffer. Starting on the unbuffered
    // edge at 600 kt due east, 30 s of projection is still inside and 150 s
    // (~0.5 deg of longitude) is well past it.
    await poll(T0, [flying({ lat: -33.9, lon: 154.3, gs: 600, track: 90 })]);
    expect((await poll(T0 + 30_000, [])).aircraft.some((a) => a.hex === 'abc123')).toBe(true);
    const snap = await poll(T0 + 150_000, []);
    expect(snap.aircraft.some((a) => a.hex === 'abc123')).toBe(false);
  });
});

describe('recovery', () => {
  it('a real fix replaces the estimate outright', async () => {
    await poll(T0, [flying()]);
    const guessed = await poll(T0 + 60_000, []);
    expect(guessed.aircraft.find((a) => a.hex === 'abc123')!.estimated).toBe(true);

    // It comes back, somewhere other than where we guessed.
    const real = await poll(T0 + 75_000, [flying({ lat: -34.2, lon: 151.9 })]);
    const rec = real.aircraft.find((a) => a.hex === 'abc123')!;
    expect(rec.estimated).toBe(false);
    expect(rec.estimatedSec).toBeNull();
    expect(rec.lat).toBe(-34.2);
    expect(rec.lon).toBe(151.9);
  });

  it('resumes trailing from the real position, not the guessed one', async () => {
    const { adsbTrailsSnapshot } = await import('../../../src/sources/adsb.js');
    await poll(T0, [flying()]);
    await poll(T0 + 60_000, []);                                   // estimated
    await poll(T0 + 75_000, [flying({ lat: -34.2, lon: 151.9 })]);  // real again

    const trail = adsbTrailsSnapshot().trails['abc123']!;
    expect(trail.length).toBe(2);
    expect(trail[0]).toEqual([POS.lat, POS.lon, 34000]);
    expect(trail[1]).toEqual([-34.2, 151.9, 34000]);
  });
});
