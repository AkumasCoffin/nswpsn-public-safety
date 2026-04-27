/**
 * /api/w3w/* smoke tests. Covers input validation + happy-path
 * proxy behaviour + error pass-through with a mocked fetchRaw so the
 * What3Words upstream isn't actually hit.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchRawMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', async () => {
  const actual = await vi.importActual<typeof import('../../../src/sources/shared/http.js')>(
    '../../../src/sources/shared/http.js',
  );
  return {
    ...actual,
    fetchRaw: (...args: unknown[]) => fetchRawMock(...args),
  };
});

const API_KEY = 'test-api-key';
const AUTH = { 'X-API-Key': API_KEY } as const;

describe('/api/w3w/convert-to-coordinates', () => {
  beforeEach(async () => {
    fetchRawMock.mockReset();
    const mod = await import('../../../src/api/w3w.js');
    mod._resetW3wCacheForTests();
  });

  it('400s on malformed words', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/w3w/convert-to-coordinates?words=foo', {
      headers: AUTH,
    });
    expect(res.status).toBe(400);
  });

  it('passes through happy-path response', async () => {
    fetchRawMock.mockResolvedValueOnce({
      status: 200,
      text: JSON.stringify({
        coordinates: { lat: -33.86, lng: 151.21 },
        words: 'lower.elder.truck',
      }),
      headers: new Headers(),
    });
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request(
      '/api/w3w/convert-to-coordinates?words=lower.elder.truck',
      { headers: AUTH },
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['words']).toBe('lower.elder.truck');
  });

  it('502s when upstream throws', async () => {
    fetchRawMock.mockRejectedValueOnce(new Error('boom'));
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request(
      '/api/w3w/convert-to-coordinates?words=lower.elder.truck',
      { headers: AUTH },
    );
    expect(res.status).toBe(502);
  });
});

describe('/api/w3w/convert-to-3wa', () => {
  beforeEach(async () => {
    fetchRawMock.mockReset();
    const mod = await import('../../../src/api/w3w.js');
    mod._resetW3wCacheForTests();
  });

  it('400s without coordinates and without lat/lon', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/w3w/convert-to-3wa', { headers: AUTH });
    expect(res.status).toBe(400);
  });

  it('accepts lat+lon as separate params', async () => {
    fetchRawMock.mockResolvedValueOnce({
      status: 200,
      text: JSON.stringify({ words: 'lower.elder.truck' }),
      headers: new Headers(),
    });
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request(
      '/api/w3w/convert-to-3wa?lat=-33.86&lon=151.21',
      { headers: AUTH },
    );
    expect(res.status).toBe(200);
  });
});

describe('/api/w3w/grid-section', () => {
  beforeEach(async () => {
    fetchRawMock.mockReset();
    const mod = await import('../../../src/api/w3w.js');
    mod._resetW3wCacheForTests();
  });

  it('400s without bounding-box', async () => {
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const res = await app.request('/api/w3w/grid-section', { headers: AUTH });
    expect(res.status).toBe(400);
  });

  it('caches by rounded bbox', async () => {
    fetchRawMock.mockResolvedValueOnce({
      status: 200,
      text: JSON.stringify({ type: 'FeatureCollection', features: [] }),
      headers: new Headers(),
    });
    const { createApp } = await import('../../../src/server.js');
    const app = createApp();
    const url = '/api/w3w/grid-section?bounding-box=-33.86,151.21,-33.85,151.22';
    const r1 = await app.request(url, { headers: AUTH });
    expect(r1.status).toBe(200);
    const r2 = await app.request(url, { headers: AUTH });
    expect(r2.status).toBe(200);
    // Cached: only one upstream fetch even though we made two requests.
    expect(fetchRawMock).toHaveBeenCalledTimes(1);
  });
});
