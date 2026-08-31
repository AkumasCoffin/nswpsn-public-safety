/**
 * Boundary helper tests.
 *
 * The parts worth pinning are the ones a wrong answer would be silent
 * about: a bbox parsed with the corners the wrong way round returning
 * nothing instead of everything, and a point-in-polygon test that
 * ignores holes — which for these datasets means the ACT reading as
 * NSW, since the ACT is a hole in it.
 */
import { describe, it, expect } from 'vitest';
import { parseBbox, geometryContains } from '../../../src/api/boundaries.js';

describe('parseBbox', () => {
  it('reads west,south,east,north', () => {
    expect(parseBbox('150.5,-34.2,151.5,-33.5')).toEqual([150.5, -34.2, 151.5, -33.5]);
  });

  it('normalises corners given the other way round', () => {
    // A caller that sends north,east first should still get its viewport
    // rather than an empty result.
    expect(parseBbox('151.5,-33.5,150.5,-34.2')).toEqual([150.5, -34.2, 151.5, -33.5]);
  });

  it('rejects anything that is not four finite numbers', () => {
    expect(parseBbox(undefined)).toBeNull();
    expect(parseBbox('')).toBeNull();
    expect(parseBbox('1,2,3')).toBeNull();
    expect(parseBbox('1,2,3,4,5')).toBeNull();
    expect(parseBbox('1,2,three,4')).toBeNull();
    expect(parseBbox('NaN,2,3,4')).toBeNull();
  });

  it('tolerates whitespace, which URLs pick up', () => {
    expect(parseBbox(' 150.5 , -34.2 , 151.5 , -33.5 ')).toEqual([150.5, -34.2, 151.5, -33.5]);
  });
});

describe('geometryContains', () => {
  const square = {
    type: 'Polygon',
    coordinates: [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
  };

  it('places a point inside and outside a simple polygon', () => {
    expect(geometryContains(square, 5, 5)).toBe(true);
    expect(geometryContains(square, 15, 5)).toBe(false);
    expect(geometryContains(square, 5, -1)).toBe(false);
  });

  it('respects a hole — the ACT-in-NSW case', () => {
    const withHole = {
      type: 'Polygon',
      coordinates: [
        [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
        [[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]],
      ],
    };
    expect(geometryContains(withHole, 1, 1)).toBe(true);   // in the ring
    expect(geometryContains(withHole, 5, 5)).toBe(false);  // in the hole
  });

  it('handles a MultiPolygon, which most of these rows are', () => {
    const multi = {
      type: 'MultiPolygon',
      coordinates: [
        [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        [[[5, 5], [6, 5], [6, 6], [5, 6], [5, 5]]],
      ],
    };
    expect(geometryContains(multi, 0.5, 0.5)).toBe(true);
    expect(geometryContains(multi, 5.5, 5.5)).toBe(true);
    expect(geometryContains(multi, 3, 3)).toBe(false);
  });

  it('returns false rather than throwing on junk', () => {
    expect(geometryContains(null, 1, 1)).toBe(false);
    expect(geometryContains({ type: 'Point', coordinates: [1, 1] }, 1, 1)).toBe(false);
    expect(geometryContains({ type: 'Polygon' }, 1, 1)).toBe(false);
    expect(geometryContains({ type: 'Polygon', coordinates: [] }, 1, 1)).toBe(false);
  });
});
