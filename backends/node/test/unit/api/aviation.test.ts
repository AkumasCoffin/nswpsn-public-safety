/**
 * Airservices weather cameras, for the map. Not on /api/v1/: camera images
 * are not part of the API (v1.test.ts checks they are absent there).
 *
 * The list is GeoJSON; one airport's cameras are a plain object. When
 * Airservices cannot be read, the detail keeps the empty 200 it always sent.
 * Checked byte for byte.
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
  const a = new Hono();
  a.route('/', aviationRouter);
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

describe('/aviation/cameras', () => {
  it('answers the snapshot', async () => {
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
  });

  it('answers an empty collection before the first poll', async () => {
    const a = await app();
    expect((await get(a, '/api/aviation/cameras')).text).toBe('{"type":"FeatureCollection","features":[],"count":0}');
  });
});

describe('/aviation/cameras/{airport}', () => {
  it("answers the airport's cameras", async () => {
    fetchJsonMock.mockResolvedValue(MODAL);
    const a = await app();
    expect(await get(a, '/api/aviation/cameras/albury-airport')).toEqual({
      status: 200,
      type: 'application/json',
      text: JSON.stringify(DETAIL),
    });
    expect(String(fetchJsonMock.mock.calls[0]?.[0])).toContain('airport=albury-airport');
  });

  it('an airport Airservices does not know is an empty list', async () => {
    fetchJsonMock.mockResolvedValue({ modal: '' });
    const a = await app();
    const want = { airport: 'sydney', title: 'sydney', state: '', name: 'sydney', cameras: [], count: 0 };
    expect((await get(a, '/api/aviation/cameras/sydney')).text).toBe(JSON.stringify(want));
  });

  it('when Airservices cannot be read: the empty 200 it always sent', async () => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 502'));
    const a = await app();
    expect(await get(a, '/api/aviation/cameras/albury-airport')).toEqual({
      status: 200,
      type: 'application/json',
      text: '{"airport":"albury-airport","cameras":[],"count":0}',
    });
  });

  it('reads the airport from the path as decoded text', async () => {
    fetchJsonMock.mockRejectedValue(new Error('down'));
    const a = await app();
    expect((await get(a, '/api/aviation/cameras/a%20b')).text).toBe('{"airport":"a b","cameras":[],"count":0}');
  });
});
