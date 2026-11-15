/**
 * What one receiver upload is allowed to put on the map.
 *
 * Each aircraft is judged on its own: a malformed one, or one further away
 * than any receiver can hear, is dropped and the rest are kept. Rejecting the
 * whole upload over one aircraft cost a receiver every aircraft it had, and
 * an unchecked position let a feeder place any aircraft anywhere.
 */
import { describe, it, expect, vi } from 'vitest';

vi.mock('../../../src/config.js', () => ({
  config: {
    LOG_LEVEL: 'warn', NODE_ENV: 'test', STATE_DIR: './test/.tmp-state',
    LIVE_PERSIST_INTERVAL_MS: 30_000, ARCHIVE_FLUSH_INTERVAL_MS: 30_000,
    NSWPSN_API_KEY: 'test-key', PORT: 3001,
  },
  modeLabel: () => 'dev',
}));

const { acceptAdsbAircraft } = await import('../../../src/api/node-ingest.js');

const PIN = { lat: -33.87, lon: 151.21 }; // Sydney
const ac = (over: Record<string, unknown> = {}) => ({
  hex: '7c6b2d', lat: -33.9, lon: 151.0, seen_pos: 1, ...over,
});

describe('acceptAdsbAircraft', () => {
  it('keeps the good aircraft when one beside them is malformed', () => {
    const r = acceptAdsbAircraft([ac(), ac({ hex: 'not-hex' }), ac({ hex: '7c1234', lat: 'x' })], PIN);
    expect(r.aircraft.map((a) => a.hex)).toEqual(['7c6b2d']);
    expect(r.invalid).toBe(2);
  });

  it('drops an aircraft further from the antenna than a receiver can hear', () => {
    // Perth, about 3,300 km from the pin.
    const r = acceptAdsbAircraft([ac(), ac({ hex: '7c1234', lat: -31.95, lon: 115.86 })], PIN);
    expect(r.aircraft.map((a) => a.hex)).toEqual(['7c6b2d']);
    expect(r.tooFar).toBe(1);
  });

  it('checks no distance for a receiver with no antenna pin', () => {
    const r = acceptAdsbAircraft([ac({ lat: -31.95, lon: 115.86 })], null);
    expect(r.aircraft).toHaveLength(1);
  });

  it('turns an impossible altitude, speed or track into "not reported"', () => {
    const r = acceptAdsbAircraft([ac({ alt_baro: 900_000, gs: -5, track: 720 })], PIN);
    expect(r.aircraft[0]).toMatchObject({ alt_baro: null, gs: null, track: null });
    const ground = acceptAdsbAircraft([ac({ alt_baro: 'ground', gs: 12, track: 90 })], PIN);
    expect(ground.aircraft[0]).toMatchObject({ alt_baro: 'ground', gs: 12, track: 90 });
  });

  it('applies the same checks to the positions carried between uploads', () => {
    const r = acceptAdsbAircraft([ac({
      positions: [[2, -33.91, 151.01, 4000], [1, -31.95, 115.86, 4000], [0.5, -33.9, 151.0, 999_999]],
    })], PIN);
    expect(r.aircraft[0]!.positions).toEqual([[2, -33.91, 151.01, 4000], [0.5, -33.9, 151.0, null]]);
  });

  it('passes the detail fields through for the normaliser to range-check', () => {
    const r = acceptAdsbAircraft([ac({ baro_rate: -640, type: 'adsb_icao', rssi: -11.2, nav_modes: ['autopilot'] })], PIN);
    expect(r.aircraft[0]).toMatchObject({ baro_rate: -640, type: 'adsb_icao', rssi: -11.2, nav_modes: ['autopilot'] });
  });
});
