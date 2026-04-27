/**
 * Unit tests for the weather sources.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchJsonMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

describe('weather.fetchWeatherCurrent', () => {
  beforeEach(() => fetchJsonMock.mockReset());

  it('maps Open-Meteo batch results to features', async () => {
    const { NSW_WEATHER_LOCATIONS, fetchWeatherCurrent } = await import(
      '../../../src/sources/weather.js'
    );
    // Build a parallel array where every location has the same dummy
    // current-conditions payload. Real upstream returns one entry per
    // lat/lon in the same order.
    const list = NSW_WEATHER_LOCATIONS.map(() => ({
      current: {
        temperature_2m: 22.5,
        relative_humidity_2m: 60,
        apparent_temperature: 22,
        precipitation: 0,
        weather_code: 3,
        wind_speed_10m: 12,
        wind_direction_10m: 180,
        wind_gusts_10m: 18,
      },
    }));
    fetchJsonMock.mockResolvedValueOnce(list);

    const out = await fetchWeatherCurrent();
    expect(out.type).toBe('FeatureCollection');
    expect(out.features.length).toBe(NSW_WEATHER_LOCATIONS.length);
    const f0 = out.features[0];
    expect(f0).toBeDefined();
    if (!f0) throw new Error('no f0');
    const loc0 = NSW_WEATHER_LOCATIONS[0];
    if (!loc0) throw new Error('no loc0');
    expect(f0.geometry.coordinates).toEqual([loc0.lon, loc0.lat]);
    expect(f0.properties.name).toBe(loc0.name);
    expect(f0.properties.temperature).toBe(22.5);
    expect(f0.properties.weatherCode).toBe(3);
    expect(f0.properties.weatherDescription).toBe('Overcast');
  });

  it('throws on upstream error', async () => {
    fetchJsonMock.mockRejectedValueOnce(new Error('rate limited'));
    const { fetchWeatherCurrent } = await import('../../../src/sources/weather.js');
    await expect(fetchWeatherCurrent()).rejects.toThrow('rate limited');
  });
});

describe('weather.fetchWeatherRadar', () => {
  beforeEach(() => fetchJsonMock.mockReset());

  it('returns the upstream payload verbatim', async () => {
    const fixture = {
      version: '2',
      generated: 1700000000,
      host: 'tilecache.rainviewer.com',
      radar: { past: [{ time: 1700000000, path: '/v2/radar/abc' }], nowcast: [] },
    };
    fetchJsonMock.mockResolvedValueOnce(fixture);
    const { fetchWeatherRadar } = await import('../../../src/sources/weather.js');
    const out = await fetchWeatherRadar();
    expect(out).toEqual(fixture);
  });
});
