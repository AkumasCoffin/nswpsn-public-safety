/**
 * OPEN_METEO_BASE_URL: one env var covers a self-hosted instance.
 *
 * The public API splits its services across four hostnames, which is the only
 * reason four URL vars exist. A self-hosted instance is one host, and asking
 * the operator to write the same address four times is how one of the four
 * ends up pointing at the wrong place.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

async function freshConfig(env: Record<string, string>) {
  vi.resetModules();
  for (const [k, v] of Object.entries(env)) vi.stubEnv(k, v);
  const { config } = await import('../../src/config.js');
  return config;
}

beforeEach(() => vi.resetModules());
afterEach(() => vi.unstubAllEnvs());

describe('OPEN_METEO_BASE_URL', () => {
  it('derives forecast, marine, air and elevation from one base', async () => {
    const c = await freshConfig({ OPEN_METEO_BASE_URL: 'http://10.1.0.135:8081' });
    expect(c.OPEN_METEO_FORECAST_URL).toBe('http://10.1.0.135:8081/v1/forecast');
    expect(c.OPEN_METEO_MARINE_URL).toBe('http://10.1.0.135:8081/v1/marine');
    expect(c.OPEN_METEO_AIR_URL).toBe('http://10.1.0.135:8081/v1/air-quality');
    expect(c.OPEN_METEO_ELEVATION_URL).toBe('http://10.1.0.135:8081/v1/elevation');
  });

  it('never derives flood — GloFAS is not in the self-host mirror', async () => {
    // Deriving it would point rivers at a dataset that does not exist on the
    // instance: a 404 or an all-null layer, depending on the build.
    const c = await freshConfig({ OPEN_METEO_BASE_URL: 'http://10.1.0.135:8081' });
    expect(c.OPEN_METEO_FLOOD_URL).toBe('https://flood-api.open-meteo.com/v1/flood');
  });

  it('an explicit individual URL beats the base', async () => {
    const c = await freshConfig({
      OPEN_METEO_BASE_URL: 'http://10.1.0.135:8081',
      OPEN_METEO_MARINE_URL: 'http://elsewhere:9999/v1/marine',
    });
    expect(c.OPEN_METEO_MARINE_URL).toBe('http://elsewhere:9999/v1/marine');
    expect(c.OPEN_METEO_FORECAST_URL).toBe('http://10.1.0.135:8081/v1/forecast');
  });

  it('a trailing slash on the base does not double up', async () => {
    const c = await freshConfig({ OPEN_METEO_BASE_URL: 'http://10.1.0.135:8081/' });
    expect(c.OPEN_METEO_FORECAST_URL).toBe('http://10.1.0.135:8081/v1/forecast');
  });

  it('without a base, everything stays on the public hosts', async () => {
    const c = await freshConfig({});
    expect(c.OPEN_METEO_FORECAST_URL).toBe('https://api.open-meteo.com/v1/forecast');
    expect(c.OPEN_METEO_MARINE_URL).toBe('https://marine-api.open-meteo.com/v1/marine');
    expect(c.OPEN_METEO_AIR_URL).toBe('https://air-quality-api.open-meteo.com/v1/air-quality');
    expect(c.OPEN_METEO_FLOOD_URL).toBe('https://flood-api.open-meteo.com/v1/flood');
    expect(c.OPEN_METEO_ELEVATION_URL).toBe('https://api.open-meteo.com/v1/elevation');
  });
});
