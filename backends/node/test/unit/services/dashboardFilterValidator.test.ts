/**
 * Filter validator unit tests.
 *
 * Mirrors the surface tested by python's _dash_validate_filters /
 * _dash_validate_geofilter. Wire-format compatibility with python is
 * load-bearing — these tests guard the round-trip shape the bot expects
 * to read back out of the alert_presets.filters jsonb column.
 */
import { describe, it, expect } from 'vitest';
import {
  validateFilters,
  validateGeofilter,
  FilterValidationError,
} from '../../../src/services/dashboardFilterValidator.js';

describe('validateFilters: top-level shape', () => {
  it('returns {} for null/undefined', () => {
    expect(validateFilters(null)).toEqual({});
    expect(validateFilters(undefined)).toEqual({});
  });

  it('rejects non-object inputs', () => {
    expect(() => validateFilters([])).toThrow(FilterValidationError);
    expect(() => validateFilters('hi')).toThrow(/must be an object/);
    expect(() => validateFilters(42)).toThrow(/must be an object/);
  });

  it('rejects unknown top-level keys', () => {
    expect(() => validateFilters({ totally_made_up: true })).toThrow(/unknown filter keys/);
  });

  it('returns {} for empty object', () => {
    expect(validateFilters({})).toEqual({});
  });
});

describe('validateFilters: keywords_include / keywords_exclude', () => {
  it('trims, drops empties, preserves order', () => {
    const out = validateFilters({
      keywords_include: ['  fire ', '', 'flood', '   '],
    });
    expect(out).toEqual({ keywords_include: ['fire', 'flood'] });
  });

  it('rejects non-array', () => {
    expect(() => validateFilters({ keywords_include: 'fire' })).toThrow(/must be a list/);
  });

  it('rejects > 20 entries', () => {
    const arr = Array.from({ length: 21 }, (_, i) => `kw${i}`);
    expect(() => validateFilters({ keywords_include: arr })).toThrow(/max 20 entries/);
  });

  it('rejects entry > 100 chars', () => {
    const longKw = 'x'.repeat(101);
    expect(() => validateFilters({ keywords_include: [longKw] })).toThrow(/<= 100 chars/);
  });

  it('rejects non-string entry', () => {
    expect(() => validateFilters({ keywords_include: [1, 2] })).toThrow(/must be a string/);
  });

  it('omits the key when only blanks were supplied', () => {
    expect(validateFilters({ keywords_include: ['', '   '] })).toEqual({});
  });
});

describe('validateFilters: severity_min', () => {
  it('accepts the legacy single-string form', () => {
    expect(validateFilters({ severity_min: 'EMERGENCY' })).toEqual({ severity_min: 'emergency' });
  });

  it('rejects unknown legacy severity strings', () => {
    expect(() => validateFilters({ severity_min: 'apocalypse' })).toThrow(/severity_min must be one of/);
  });

  it('drops blank legacy string', () => {
    expect(validateFilters({ severity_min: '   ' })).toEqual({});
  });

  it('accepts per-alert-type dict form', () => {
    expect(
      validateFilters({
        severity_min: { rfs: 'watch_and_act', bom_land: 'minor' },
      }),
    ).toEqual({ severity_min: { rfs: 'watch_and_act', bom_land: 'minor' } });
  });

  it('rejects unknown alert_type in dict form', () => {
    expect(() =>
      validateFilters({ severity_min: { not_a_type: 'minor' } }),
    ).toThrow(/unknown alert types/);
  });

  it('rejects out-of-scale severity in dict form', () => {
    expect(() =>
      validateFilters({ severity_min: { rfs: 'minor' } }),
    ).toThrow(/severity_min\[rfs\]/);
  });

  it('drops null/empty values from dict form', () => {
    expect(
      validateFilters({ severity_min: { rfs: 'advice', bom_land: '', bom_marine: null } }),
    ).toEqual({ severity_min: { rfs: 'advice' } });
  });

  it('rejects non-string/object', () => {
    expect(() => validateFilters({ severity_min: 7 })).toThrow(/must be a string or object/);
  });
});

describe('validateFilters: subtype_filters', () => {
  it('only accepts subtype-aware alert_types', () => {
    expect(() =>
      validateFilters({ subtype_filters: { ausgrid: ['x'] } }),
    ).toThrow(/unsupported alert types/);
  });

  it('dedupes while preserving insertion order', () => {
    expect(
      validateFilters({ subtype_filters: { rfs: ['Fire', 'Fire', 'Burn'] } }),
    ).toEqual({ subtype_filters: { rfs: ['Fire', 'Burn'] } });
  });

  it('rejects > 50 entries per type', () => {
    const arr = Array.from({ length: 51 }, (_, i) => `s${i}`);
    expect(() =>
      validateFilters({ subtype_filters: { rfs: arr } }),
    ).toThrow(/max 50 entries/);
  });

  it('drops empty strings', () => {
    expect(
      validateFilters({ subtype_filters: { rfs: ['fire', '', '  '] } }),
    ).toEqual({ subtype_filters: { rfs: ['fire'] } });
  });

  it('rejects non-list per-type value', () => {
    expect(() =>
      validateFilters({ subtype_filters: { rfs: 'fire' } }),
    ).toThrow(/must be a list/);
  });
});

describe('validateGeofilter: discriminated union', () => {
  it('rejects unknown type', () => {
    expect(() => validateGeofilter({ type: 'square' })).toThrow(/geofilter\.type must be one of/);
  });

  it('rejects non-object', () => {
    expect(() => validateGeofilter(null)).toThrow(/must be an object/);
  });

  describe('bbox', () => {
    it('round-trips a valid bbox', () => {
      expect(
        validateGeofilter({ type: 'bbox', lat_min: -34, lat_max: -33, lng_min: 150, lng_max: 151 }),
      ).toEqual({ type: 'bbox', lat_min: -34, lat_max: -33, lng_min: 150, lng_max: 151 });
    });

    it('rejects missing fields', () => {
      expect(() => validateGeofilter({ type: 'bbox' })).toThrow(/needs numeric/);
    });

    it('rejects swapped lat min/max', () => {
      expect(() =>
        validateGeofilter({ type: 'bbox', lat_min: -33, lat_max: -34, lng_min: 150, lng_max: 151 }),
      ).toThrow(/lat_min must be <= lat_max/);
    });

    it('rejects out-of-range lat', () => {
      expect(() =>
        validateGeofilter({ type: 'bbox', lat_min: -91, lat_max: 0, lng_min: 0, lng_max: 1 }),
      ).toThrow(/lat must be in/);
    });

    it('coerces numeric strings', () => {
      const out = validateGeofilter({
        type: 'bbox', lat_min: '-34', lat_max: '-33', lng_min: '150', lng_max: '151',
      });
      expect(out).toEqual({ type: 'bbox', lat_min: -34, lat_max: -33, lng_min: 150, lng_max: 151 });
    });
  });

  describe('ring', () => {
    it('round-trips a valid ring', () => {
      expect(
        validateGeofilter({ type: 'ring', lat: -33.85, lng: 151.2, radius_m: 5000 }),
      ).toEqual({ type: 'ring', lat: -33.85, lng: 151.2, radius_m: 5000 });
    });

    it('rejects radius below min', () => {
      expect(() =>
        validateGeofilter({ type: 'ring', lat: 0, lng: 0, radius_m: 0 }),
      ).toThrow(/radius_m must be in/);
    });

    it('rejects radius above max', () => {
      expect(() =>
        validateGeofilter({ type: 'ring', lat: 0, lng: 0, radius_m: 600_000 }),
      ).toThrow(/radius_m must be in/);
    });
  });

  describe('polygon', () => {
    it('round-trips a triangle', () => {
      expect(
        validateGeofilter({
          type: 'polygon',
          points: [[-33, 150], [-34, 151], [-34, 150]],
        }),
      ).toEqual({
        type: 'polygon',
        points: [[-33, 150], [-34, 151], [-34, 150]],
      });
    });

    it('rejects fewer than 3 points', () => {
      expect(() =>
        validateGeofilter({ type: 'polygon', points: [[0, 0], [1, 1]] }),
      ).toThrow(/at least 3 points/);
    });

    it('rejects more than 100 points', () => {
      const pts = Array.from({ length: 101 }, () => [0, 0]);
      expect(() =>
        validateGeofilter({ type: 'polygon', points: pts }),
      ).toThrow(/max 100 points/);
    });

    it('rejects non-pair points', () => {
      expect(() =>
        validateGeofilter({ type: 'polygon', points: [[0], [1, 1], [2, 2]] }),
      ).toThrow(/\[lat, lng\] pairs/);
    });

    it('rejects out-of-range lat in a point', () => {
      expect(() =>
        validateGeofilter({
          type: 'polygon',
          points: [[-91, 0], [0, 0], [0, 1]],
        }),
      ).toThrow(/out of lat\/lng bounds/);
    });
  });
});

describe('validateFilters: legacy bbox alias', () => {
  it('promotes top-level bbox to geofilter', () => {
    const out = validateFilters({
      bbox: { lat_min: -34, lat_max: -33, lng_min: 150, lng_max: 151 },
    });
    expect(out).toEqual({
      geofilter: { type: 'bbox', lat_min: -34, lat_max: -33, lng_min: 150, lng_max: 151 },
    });
  });

  it('rejects bbox + geofilter together', () => {
    expect(() =>
      validateFilters({
        bbox: { lat_min: 0, lat_max: 1, lng_min: 0, lng_max: 1 },
        geofilter: { type: 'ring', lat: 0, lng: 0, radius_m: 100 },
      }),
    ).toThrow(/use only one of bbox \/ geofilter/);
  });

  it('rejects non-object bbox', () => {
    expect(() => validateFilters({ bbox: 'hi' })).toThrow(/bbox must be an object/);
  });
});

describe('validateFilters: composition', () => {
  it('round-trips a maxed-out filter', () => {
    const input = {
      keywords_include: ['storm'],
      keywords_exclude: ['drill'],
      severity_min: { rfs: 'advice' },
      subtype_filters: { rfs: ['Bush Fire'] },
      geofilter: { type: 'ring', lat: -33.85, lng: 151.2, radius_m: 5000 },
    };
    expect(validateFilters(input)).toEqual({
      keywords_include: ['storm'],
      keywords_exclude: ['drill'],
      severity_min: { rfs: 'advice' },
      subtype_filters: { rfs: ['Bush Fire'] },
      geofilter: { type: 'ring', lat: -33.85, lng: 151.2, radius_m: 5000 },
    });
  });
});
