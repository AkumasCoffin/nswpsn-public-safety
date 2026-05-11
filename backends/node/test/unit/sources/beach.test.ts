/**
 * Unit tests for beachwatch + beachsafe fetchers.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchJsonMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

describe('beach.fetchBeachwatch', () => {
  beforeEach(() => fetchJsonMock.mockReset());

  it('returns the upstream payload verbatim', async () => {
    const fixture = {
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [151.2, -33.8] },
          properties: { name: 'Bondi', siteCondition: 'Good' },
        },
      ],
    };
    fetchJsonMock.mockResolvedValueOnce(fixture);
    const { fetchBeachwatch } = await import('../../../src/sources/beach.js');
    const out = await fetchBeachwatch();
    expect(out).toEqual(fixture);
  });

  it('throws on upstream error', async () => {
    fetchJsonMock.mockRejectedValueOnce(new Error('502'));
    const { fetchBeachwatch } = await import('../../../src/sources/beach.js');
    await expect(fetchBeachwatch()).rejects.toThrow('502');
  });
});

describe('beach.fetchBeachsafe', () => {
  beforeEach(() => fetchJsonMock.mockReset());

  it('normalises the upstream beach list shape', async () => {
    fetchJsonMock.mockResolvedValueOnce({
      beaches: [
        {
          id: 7,
          title: 'Bondi Beach',
          latitude: -33.89,
          longitude: 151.27,
          url: '/nsw/waverley/bondi-beach/',
          status: 'Patrolled',
          has_toilet: 1,
          has_parking: true,
          has_dogs_allowed: false,
          image: 'http://example.com/img.jpg',
          weather: { temp: 20 },
          hazards: ['rip'],
          is_patrolled_today: { flag: true, start: '09:00', end: '17:00' },
          patrol: 1,
        },
        {
          id: 8,
          title: 'Tamarama',
          latitude: '-33.9',
          longitude: '151.27',
          url: '/nsw/waverley/tamarama/',
          status: 'Unpatrolled',
        },
      ],
    });
    const { fetchBeachsafe } = await import('../../../src/sources/beach.js');
    const out = await fetchBeachsafe();
    expect(out.length).toBe(2);
    const bondi = out[0];
    expect(bondi).toBeDefined();
    if (!bondi) throw new Error('no bondi');
    expect(bondi.name).toBe('Bondi Beach');
    expect(bondi.lat).toBe(-33.89);
    expect(bondi.lng).toBe(151.27);
    expect(bondi.slug).toBe('bondi-beach');
    expect(bondi.patrolled).toBe(true);
    expect(bondi.hasToilet).toBe(true);
    expect(bondi.dogsAllowed).toBe(false);
    expect(bondi.isPatrolledToday).toBe(true);
    expect(bondi.patrolStart).toBe('09:00');
    expect(bondi.hazards).toEqual(['rip']);

    const tama = out[1];
    expect(tama).toBeDefined();
    if (!tama) throw new Error('no tama');
    expect(tama.lat).toBe(-33.9); // string -> number
    expect(tama.patrolled).toBe(false);
  });

  it('handles missing beaches array gracefully', async () => {
    fetchJsonMock.mockResolvedValueOnce({});
    const { fetchBeachsafe } = await import('../../../src/sources/beach.js');
    const out = await fetchBeachsafe();
    expect(out).toEqual([]);
  });
});
