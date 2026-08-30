/**
 * Per-state resolution tests.
 *
 * The interesting cases are the ones a bounding box would get wrong:
 * towns just the wrong side of a border (Queanbeyan is NSW, not ACT;
 * Eucla is WA, not SA), islands that belong to a state whose mainland is
 * far away, and points offshore that should still attribute to the coast
 * they sit against rather than falling through to null.
 */
import { describe, it, expect } from 'vitest';
import { stateForPoint, stateForRow, AU_STATES } from '../../../src/lib/stateMask.js';

describe('stateForPoint — capitals', () => {
  it.each([
    ['Sydney', 151.209, -33.868, 'NSW'],
    ['Melbourne', 144.963, -37.814, 'VIC'],
    ['Brisbane', 153.026, -27.470, 'QLD'],
    ['Perth', 115.857, -31.953, 'WA'],
    ['Adelaide', 138.600, -34.929, 'SA'],
    ['Hobart', 147.325, -42.882, 'TAS'],
    ['Darwin', 130.845, -12.463, 'NT'],
    ['Canberra', 149.128, -35.283, 'ACT'],
  ] as Array<[string, number, number, string]>)('places %s in %s', (_n, lon, lat, want) => {
    expect(stateForPoint(lon, lat)).toBe(want);
  });
});

describe('stateForPoint — borders', () => {
  it('puts Queanbeyan in NSW, not the ACT it abuts', () => {
    expect(stateForPoint(149.234, -35.354)).toBe('NSW');
  });

  it('keeps Eucla in WA, just short of the SA border', () => {
    expect(stateForPoint(128.883, -31.677)).toBe('WA');
  });

  it('separates Mildura (VIC) from the NSW bank of the Murray', () => {
    expect(stateForPoint(142.163, -34.185)).toBe('VIC');
    expect(stateForPoint(142.163, -34.100)).toBe('NSW');
  });

  it('puts Broken Hill in NSW despite sitting near the SA line', () => {
    expect(stateForPoint(141.467, -31.956)).toBe('NSW');
  });

  it('reads the three-corner country around Mt Gambier as SA', () => {
    expect(stateForPoint(140.780, -37.829)).toBe('SA');
  });
});

describe('stateForPoint — islands', () => {
  it.each([
    ['Kangaroo Island', 137.200, -35.830, 'SA'],
    ['Fraser Island', 153.070, -25.250, 'QLD'],
    ['Melville Island', 130.850, -11.600, 'NT'],
    ['Groote Eylandt', 136.600, -13.970, 'NT'],
    ['King Island', 143.880, -39.870, 'TAS'],
    ['Flinders Island', 148.080, -40.000, 'TAS'],
    ['Lord Howe Island', 159.080, -31.550, 'NSW'],
    ['Thursday Island', 142.219, -10.583, 'QLD'],
  ] as Array<[string, number, number, string]>)('places %s in %s', (_n, lon, lat, want) => {
    expect(stateForPoint(lon, lat)).toBe(want);
  });
});

describe('stateForPoint — outside', () => {
  it('returns null well out to sea', () => {
    expect(stateForPoint(140.0, -25.0 - 20)).toBeNull(); // Southern Ocean
    expect(stateForPoint(160.0, -20.0)).toBeNull(); // Coral Sea
  });

  it('returns null for other countries', () => {
    expect(stateForPoint(147.150, -9.440)).toBeNull(); // Port Moresby
    expect(stateForPoint(125.570, -8.560)).toBeNull(); // Dili
    expect(stateForPoint(174.760, -36.850)).toBeNull(); // Auckland
  });

  it('returns null on non-finite input', () => {
    expect(stateForPoint(NaN, -33)).toBeNull();
    expect(stateForPoint(151, Infinity)).toBeNull();
  });
});

describe('stateForRow', () => {
  it('reads the lat/lng spelling the traffic and RFS feeds use', () => {
    expect(stateForRow({ lat: -33.868, lng: 151.209 })).toBe('NSW');
  });

  it('reads the latitude/longitude spelling the power feeds use', () => {
    expect(stateForRow({ latitude: -31.953, longitude: 115.857 })).toBe('WA');
  });

  it('accepts numeric strings, which some feeds emit', () => {
    expect(stateForRow({ lat: '-12.463', lng: '130.845' })).toBe('NT');
  });

  it('returns null for a row with no coordinate at all', () => {
    // BOM warnings are areas, not points.
    expect(stateForRow({ title: 'Severe weather warning' })).toBeNull();
    expect(stateForRow({ lat: null, lng: null })).toBeNull();
    expect(stateForRow(null)).toBeNull();
    expect(stateForRow('nope')).toBeNull();
  });

  it('ignores an unparseable coordinate rather than treating it as 0,0', () => {
    expect(stateForRow({ lat: 'n/a', lng: 'n/a' })).toBeNull();
  });
});

describe('AU_STATES', () => {
  it('lists the eight jurisdictions the site covers', () => {
    expect([...AU_STATES].sort()).toEqual(
      ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA'].sort(),
    );
  });
});
