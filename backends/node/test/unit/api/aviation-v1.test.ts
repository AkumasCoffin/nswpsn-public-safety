/**
 * Airservices weather cameras on /api/v1/ and on their old /api/ paths.
 *
 * One handler serves both (api/v1/mirror.ts). The list is GeoJSON, bare on
 * both paths; one airport's cameras are wrapped in `data` on v1. When
 * Airservices cannot be read, the old path keeps the empty 200 it always
 * sent and v1 answers 503 service_unavailable. The old paths are checked
 * byte for byte.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';

const fetchTextMock = vi.fn();
const fetchJsonMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: fetchTextMock,
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

const NONCE_PAGE = '<script>var config = {"nonce":"abc123def4"};</script>';

// What Airservices' get_airport_modal answers (trimmed).
const MODAL = {
  modal: {
    title: 'Albury Airport',
    state: 'New South Wales',
    name: 'albury-airport',
    north_timestamp: '27/10/2026 18:09:16',
    north_angle: '070',
    north_image: 'https://weathercams.example/072160_070.jpg',
    north_thumbnail: 'https://weathercams.example/072160_070-300x169.jpg',
    east_timestamp: '27/10/2026 18:09:16',
    east_angle: '160',
    east_image: 'https://weathercams.example/072160_160.jpg',
    east_thumbnail: 'https://weathercams.example/072160_160-300x169.jpg',
  },
};

const DETAIL = {
  airport: 'albury-airport',
  title: 'Albury Airport',
  state: 'New South Wales',
  name: 'albury-airport',
  cameras: [
    {
      direction: 'North',
      angle: '070',
      imageUrl: 'https://weathercams.example/072160_070.jpg',
      thumbnail: 'https://weathercams.example/072160_070-300x169.jpg',
      timestamp: '27/10/2026 18:09:16',
    },
    {
      direction: 'East',
      angle: '160',
      imageUrl: 'https://weathercams.example/072160_160.jpg',
      thumbnail: 'https://weathercams.example/072160_160-300x169.jpg',
      timestamp: '27/10/2026 18:09:16',
    },
  ],
  count: 2,
};

async function app() {
  const { aviationRouter } = await import('../../../src/api/aviation.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', aviationRouter);
  mirror(a, aviationRouter, ['/api/aviation/cameras', '/api/aviation/cameras/:airport']);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function get(a: Hono, path: string) {
  const res = await a.request(path);
  return { status: res.status, type: res.headers.get('content-type'), text: await res.text() };
}

beforeEach(async () => {
  fetchTextMock.mockReset();
  fetchJsonMock.mockReset();
  fetchTextMock.mockResolvedValue(NONCE_PAGE);
  liveStore.delete('aviation_cameras');
  const mod = await import('../../../src/sources/aviation.js');
  mod._resetNonceCacheForTests();
  mod._resetDetailCacheForTests();
});

async function resetDetailCache() {
  (await import('../../../src/sources/aviation.js'))._resetDetailCacheForTests();
}

describe('/aviation/cameras', () => {
  it('answers the snapshot bare, on both paths', async () => {
    const snap = {
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [146.951922, -36.06901] },
          properties: {
            id: '567', title: 'Albury Airport', name: 'albury-airport', state: 'NSW', state_full: 'New South Wales',
            link: 'https://weathercams.example/albury-airport/', imageUrl: 'https://weathercams.example/072160_070.jpg',
            source: 'airservices_australia',
          },
        },
      ],
      count: 1,
    };
    liveStore.set('aviation_cameras', snap);
    const a = await app();
    const want = JSON.stringify(snap);
    expect(await get(a, '/api/aviation/cameras')).toEqual({ status: 200, type: 'application/json', text: want });
    expect(await get(a, '/api/v1/aviation/cameras')).toEqual({ status: 200, type: 'application/json', text: want });
  });

  it('answers an empty collection before the first poll', async () => {
    const a = await app();
    const want = '{"type":"FeatureCollection","features":[],"count":0}';
    expect((await get(a, '/api/aviation/cameras')).text).toBe(want);
    expect((await get(a, '/api/v1/aviation/cameras')).text).toBe(want);
  });
});

describe('/aviation/cameras/{airport}', () => {
  it("answers the airport's cameras — wrapped in data on v1", async () => {
    fetchJsonMock.mockResolvedValue(MODAL);
    const a = await app();
    expect(await get(a, '/api/aviation/cameras/albury-airport')).toEqual({
      status: 200,
      type: 'application/json',
      text: JSON.stringify(DETAIL),
    });
    await resetDetailCache();
    expect(await get(a, '/api/v1/aviation/cameras/albury-airport')).toEqual({
      status: 200,
      type: 'application/json',
      text: JSON.stringify({ data: DETAIL }),
    });
    expect(String(fetchJsonMock.mock.calls[0]?.[0])).toContain('airport=albury-airport');
  });

  it('an airport Airservices does not know is an empty list on both paths', async () => {
    fetchJsonMock.mockResolvedValue({ modal: '' });
    const a = await app();
    const want = { airport: 'sydney', title: 'sydney', state: '', name: 'sydney', cameras: [], count: 0 };
    expect((await get(a, '/api/aviation/cameras/sydney')).text).toBe(JSON.stringify(want));
    await resetDetailCache();
    expect((await get(a, '/api/v1/aviation/cameras/sydney')).text).toBe(JSON.stringify({ data: want }));
  });

  it('when Airservices cannot be read: the old path its empty 200, v1 a 503', async () => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 502'));
    const a = await app();
    expect(await get(a, '/api/aviation/cameras/albury-airport')).toEqual({
      status: 200,
      type: 'application/json',
      text: '{"airport":"albury-airport","cameras":[],"count":0}',
    });
    const res = await get(a, '/api/v1/aviation/cameras/albury-airport');
    expect(res.status).toBe(503);
    expect(JSON.parse(res.text)).toEqual({ error: { code: 'service_unavailable', message: expect.any(String) } });
  });

  it('reads the airport from the path as decoded text', async () => {
    fetchJsonMock.mockRejectedValue(new Error('down'));
    const a = await app();
    expect((await get(a, '/api/aviation/cameras/a%20b')).text).toBe('{"airport":"a b","cameras":[],"count":0}');
  });
});
