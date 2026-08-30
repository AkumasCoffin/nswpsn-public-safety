/**
 * Australia land-mask tests.
 *
 * The mask exists to strip foreign hotspots the FIRMS bounding box
 * unavoidably sweeps in, so the cases that matter are the near misses:
 * PNG across the Torres Strait, Timor and the Indonesian islands off the
 * Top End, and the PNG archipelagos east of Cape York. Equally important
 * is that nothing Australian gets cut — especially the far-north coast
 * and the big offshore islands, which sit closest to the boundary.
 */
import { describe, it, expect } from 'vitest';
import { isInAustralia } from '../../../src/lib/australiaMask.js';

describe('isInAustralia — inside', () => {
  const AUSTRALIAN: Array<[string, number, number]> = [
    ['Sydney', 151.209, -33.868],
    ['Melbourne', 144.963, -37.814],
    ['Brisbane', 153.026, -27.470],
    ['Perth', 115.857, -31.953],
    ['Adelaide', 138.600, -34.929],
    ['Hobart', 147.325, -42.882],
    ['Darwin', 130.845, -12.463],
    ['Canberra (ACT)', 149.128, -35.283],
    ['Alice Springs', 133.881, -23.698],
    ['Broome', 122.236, -17.958],
    // Far-north coast: closest Australian land to the foreign detections.
    ['Cape York tip', 142.531, -10.687],
    ['Thursday Island', 142.219, -10.583],
    ['Cape Leveque', 122.920, -16.400],
    // Big offshore islands — all real fire ground.
    ['Melville Island (Tiwi)', 130.850, -11.600],
    ['Groote Eylandt', 136.600, -13.970],
    ['Kangaroo Island', 137.200, -35.830],
    ['Fraser Island', 153.070, -25.250],
    ['King Island (TAS)', 143.880, -39.870],
    ['Flinders Island (TAS)', 148.080, -40.000],
    ['Lord Howe Island (NSW)', 159.080, -31.550],
  ];

  it.each(AUSTRALIAN)('keeps %s', (_name, lon, lat) => {
    expect(isInAustralia(lon, lat)).toBe(true);
  });
});

describe('isInAustralia — outside', () => {
  const FOREIGN: Array<[string, number, number]> = [
    // Just across the Torres Strait — the closest foreign land there is.
    ['Daru, PNG', 143.210, -9.080],
    ['Port Moresby, PNG', 147.150, -9.440],
    ['Louisiade Archipelago, PNG', 153.300, -11.400],
    ['Dili, Timor-Leste', 125.570, -8.560],
    ['Kupang, Indonesia', 123.580, -10.170],
    ['Rote Island, Indonesia', 123.120, -10.750],
    ['Honiara, Solomon Islands', 159.950, -9.430],
    ['Auckland, NZ', 174.760, -36.850],
    // Open ocean well inside the query box.
    ['Indian Ocean', 118.000, -25.000 - 15],
    ['Coral Sea', 156.000, -20.000],
  ];

  it.each(FOREIGN)('drops %s', (_name, lon, lat) => {
    expect(isInAustralia(lon, lat)).toBe(false);
  });
});

describe('isInAustralia — external territories', () => {
  // The mask is built from the eight states/territories only; the "OT"
  // polygons are excluded on purpose, so these are NOT Australia here.
  it.each([
    ['Christmas Island', 105.630, -10.450],
    ['Cocos (Keeling)', 96.870, -12.190],
    ['Norfolk Island', 167.950, -29.030],
  ] as Array<[string, number, number]>)('excludes %s', (_name, lon, lat) => {
    expect(isInAustralia(lon, lat)).toBe(false);
  });
});

describe('isInAustralia — bad input', () => {
  it('treats non-finite coordinates as outside', () => {
    expect(isInAustralia(NaN, -33)).toBe(false);
    expect(isInAustralia(151, NaN)).toBe(false);
    expect(isInAustralia(Infinity, -Infinity)).toBe(false);
  });

  it('does not confuse a swapped lat/lon pair for a hit', () => {
    // Sydney with the arguments the wrong way round is in the Pacific.
    expect(isInAustralia(-33.868, 151.209)).toBe(false);
  });
});
