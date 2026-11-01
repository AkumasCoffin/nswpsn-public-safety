/**
 * Central Watch, for the map. Not on /api/v1/: camera images are not part of
 * the API (v1.test.ts checks they are absent there).
 *
 * The bodies and headers are checked byte for byte. The image route is public,
 * because the map shows each image with a plain <img src>; the camera lists,
 * the only place an id comes from, need a credential.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const m = vi.hoisted(() => ({
  cameras: [] as unknown[],
  sites: [] as unknown[],
  images: new Map<string, { data: Buffer; contentType: string; ts: number }>(),
}));

vi.mock('../../../src/services/centralwatchImageCache.js', () => ({
  getImage: (id: string) => m.images.get(id),
  hasImage: (id: string) => m.images.has(id),
  cacheSize: () => m.images.size,
  STALE_AFTER_MS_EXPORT: 2 * 60 * 1000,
}));

vi.mock('../../../src/sources/centralwatch.js', () => ({
  getCentralwatchCameras: vi.fn(async () => m.cameras),
  getCentralwatchSites: vi.fn(async () => m.sites),
}));

async function app() {
  const { centralwatchRouter } = await import('../../../src/api/centralwatch.js');
  const a = new Hono();
  a.route('/', centralwatchRouter);
  return a;
}

const NOW = Date.UTC(2026, 9, 28, 4, 0, 0);
const JSON_TYPE = 'application/json';

const camera = (id: string, siteId: string) => ({
  id,
  name: `Camera ${id}`,
  siteName: `Tower ${siteId}`,
  siteId,
  latitude: -33.71,
  longitude: 150.31,
  altitude: 837,
  state: 'NSW',
  imageUrl: `/api/centralwatch/image/${id}`,
  time: '2026-10-28T03:59:00.000Z',
  source: 'centralwatch',
});
const CAMERAS = [camera('cam-1', 'site-a'), camera('cam-2', 'site-a'), camera('cam-3', 'site-b')];
const SITES = [
  {
    siteId: 'site-a', siteName: 'Tower site-a', latitude: -33.71, longitude: 150.31, altitude: 837, state: 'NSW',
    cameras: [
      { id: 'cam-1', name: 'Camera cam-1', imageUrl: '/api/centralwatch/image/cam-1' },
      { id: 'cam-2', name: 'Camera cam-2', imageUrl: '/api/centralwatch/image/cam-2' },
    ],
  },
  {
    siteId: 'site-b', siteName: 'Tower site-b', latitude: null, longitude: null, altitude: null, state: '',
    cameras: [{ id: 'cam-3', name: 'Camera cam-3', imageUrl: '/api/centralwatch/image/cam-3' }],
  },
];

beforeEach(() => {
  m.cameras = CAMERAS;
  m.sites = SITES;
  m.images.clear();
});

describe('GET /api/centralwatch/cameras', () => {
  it('every camera while no image has been fetched', async () => {
    const a = await app();
    const res = await a.request('/api/centralwatch/cameras');
    expect(res.status).toBe(200);
    expect([...res.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await res.text()).toBe(JSON.stringify(CAMERAS));
  });

  it('once images are cached, only cameras with one', async () => {
    m.images.set('cam-2', { data: Buffer.alloc(900, 1), contentType: 'image/jpeg', ts: NOW });
    const a = await app();
    expect(await (await a.request('/api/centralwatch/cameras')).text()).toBe(JSON.stringify([CAMERAS[1]]));
  });

  it('a failed read: []', async () => {
    const { getCentralwatchCameras } = await import('../../../src/sources/centralwatch.js');
    vi.mocked(getCentralwatchCameras).mockRejectedValueOnce(new Error('read failed'));
    const a = await app();
    expect(await (await a.request('/api/centralwatch/cameras')).text()).toBe('[]');
  });
});

describe('GET /api/centralwatch/sites', () => {
  it('towers with their cameras', async () => {
    const a = await app();
    const res = await a.request('/api/centralwatch/sites');
    expect(res.status).toBe(200);
    expect([...res.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await res.text()).toBe(JSON.stringify(SITES));
  });

  it('a tower with no camera left to show is dropped', async () => {
    m.images.set('cam-1', { data: Buffer.alloc(900, 1), contentType: 'image/jpeg', ts: NOW });
    const a = await app();
    const kept = { ...SITES[0]!, cameras: [SITES[0]!.cameras[0]!] };
    expect(await (await a.request('/api/centralwatch/sites')).text()).toBe(JSON.stringify([kept]));
  });
});

describe('GET /api/centralwatch/image/:cameraId', () => {
  it('a cached image: its bytes and headers', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
    try {
      m.images.set('cam-1', { data: Buffer.alloc(1024, 0xab), contentType: 'image/jpeg', ts: NOW - 30_000 });
      m.images.set('cam-2', { data: Buffer.alloc(700, 0xcd), contentType: '', ts: NOW - 4 * 60_000 });
      const a = await app();
      const res = await a.request('/api/centralwatch/image/cam-1');
      expect(res.status).toBe(200);
      expect([...res.headers]).toEqual([
        ['access-control-allow-origin', '*'],
        ['cache-control', 'no-cache, no-store, must-revalidate'],
        ['content-type', 'image/jpeg'],
        ['expires', '0'],
        ['pragma', 'no-cache'],
        ['x-cache', 'HIT'],
        ['x-cache-age', '30'],
      ]);
      expect(Buffer.from(await res.arrayBuffer()).equals(Buffer.alloc(1024, 0xab))).toBe(true);
      const stale = await a.request('/api/centralwatch/image/cam-2');
      expect(stale.headers.get('Content-Type')).toBe('image/jpeg'); // none given: JPEG
      expect(stale.headers.get('X-Cache')).toBe('STALE');
      expect(stale.headers.get('X-Cache-Age')).toBe('240');
    } finally {
      vi.useRealTimers();
    }
  });

  it('no image to hand: the SVG placeholder, with its own headers', async () => {
    const a = await app();
    const res = await a.request('/api/centralwatch/image/cam-9');
    expect(res.status).toBe(200);
    expect([...res.headers]).toEqual([
      ['access-control-allow-origin', '*'],
      ['cache-control', 'no-cache'],
      ['content-type', 'image/svg+xml'],
      ['x-cache', 'PLACEHOLDER'],
    ]);
    expect(await res.text()).toContain('<svg');
  });
});

describe('credentials', () => {
  it('the image is public, for <img src>; the camera lists need a credential', async () => {
    const { isPublicPath } = await import('../../../src/services/auth/apiKey.js');
    expect(isPublicPath('/api/centralwatch/image/x')).toBe(true);
    for (const p of ['/api/centralwatch/cameras', '/api/centralwatch/cameras/x', '/api/centralwatch/sites']) {
      expect(isPublicPath(p), p).toBe(false);
    }
  });
});
