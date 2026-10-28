/**
 * Central Watch on /api/v1/ and on its old /api/ paths.
 *
 * One handler chain serves both (api/v1/mirror.ts). The camera lists are
 * wrapped in `data` on v1 and their imageUrls name the v1 image path; the old
 * paths answer byte for byte as before, imageUrl included. The image route
 * answers the same bytes and headers on both paths. /cameras and /image are
 * public; /sites needs a key.
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

const IN_SCOPE = ['/api/centralwatch/cameras', '/api/centralwatch/sites', '/api/centralwatch/image/:cameraId'];

async function app() {
  const { centralwatchRouter } = await import('../../../src/api/centralwatch.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', centralwatchRouter);
  mirror(a, centralwatchRouter, IN_SCOPE);
  a.notFound(notFound);
  a.onError(onError);
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

const v1Url = (id: string) => `/api/v1/centralwatch/image/${id}`;

beforeEach(() => {
  m.cameras = CAMERAS;
  m.sites = SITES;
  m.images.clear();
});

describe('GET /api/centralwatch/cameras and /api/v1/centralwatch/cameras', () => {
  it('every camera while no image has been fetched; v1 in data with v1 image paths', async () => {
    const a = await app();
    const old = await a.request('/api/centralwatch/cameras');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify(CAMERAS));

    const v1 = await a.request('/api/v1/centralwatch/cameras');
    expect(v1.status).toBe(200);
    expect([...v1.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await v1.text()).toBe(
      JSON.stringify({ data: CAMERAS.map((c) => ({ ...c, imageUrl: v1Url(c.id) })) }),
    );
  });

  it('once images are cached, only cameras with one — on both paths', async () => {
    m.images.set('cam-2', { data: Buffer.alloc(900, 1), contentType: 'image/jpeg', ts: NOW });
    const a = await app();
    expect(await (await a.request('/api/centralwatch/cameras')).text()).toBe(JSON.stringify([CAMERAS[1]]));
    expect(await (await a.request('/api/v1/centralwatch/cameras')).text()).toBe(
      JSON.stringify({ data: [{ ...CAMERAS[1]!, imageUrl: v1Url('cam-2') }] }),
    );
  });

  it('the camera list the source hands over is not changed by a v1 request', async () => {
    const a = await app();
    await a.request('/api/v1/centralwatch/cameras');
    expect((m.cameras as Array<{ imageUrl: string }>).map((c) => c.imageUrl)).toEqual([
      '/api/centralwatch/image/cam-1',
      '/api/centralwatch/image/cam-2',
      '/api/centralwatch/image/cam-3',
    ]);
    expect(await (await a.request('/api/centralwatch/cameras')).text()).toBe(JSON.stringify(CAMERAS));
  });

  it('a failed read: [] on the old path, { data: [] } on v1', async () => {
    const { getCentralwatchCameras } = await import('../../../src/sources/centralwatch.js');
    vi.mocked(getCentralwatchCameras).mockRejectedValueOnce(new Error('read failed')).mockRejectedValueOnce(new Error('read failed'));
    const a = await app();
    expect(await (await a.request('/api/centralwatch/cameras')).text()).toBe('[]');
    expect(await (await a.request('/api/v1/centralwatch/cameras')).text()).toBe('{"data":[]}');
  });
});

describe('GET /api/centralwatch/sites and /api/v1/centralwatch/sites', () => {
  it('towers with their cameras; v1 in data with v1 image paths', async () => {
    const a = await app();
    const old = await a.request('/api/centralwatch/sites');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify(SITES));

    const v1 = await a.request('/api/v1/centralwatch/sites');
    expect(v1.status).toBe(200);
    expect(await v1.text()).toBe(
      JSON.stringify({
        data: SITES.map((s) => ({ ...s, cameras: s.cameras.map((c) => ({ ...c, imageUrl: v1Url(c.id) })) })),
      }),
    );
  });

  it('a tower with no camera left to show is dropped, on both paths', async () => {
    m.images.set('cam-1', { data: Buffer.alloc(900, 1), contentType: 'image/jpeg', ts: NOW });
    const a = await app();
    const kept = { ...SITES[0]!, cameras: [SITES[0]!.cameras[0]!] };
    expect(await (await a.request('/api/centralwatch/sites')).text()).toBe(JSON.stringify([kept]));
    expect(await (await a.request('/api/v1/centralwatch/sites')).text()).toBe(
      JSON.stringify({ data: [{ ...kept, cameras: [{ ...kept.cameras[0]!, imageUrl: v1Url('cam-1') }] }] }),
    );
  });
});

describe('GET /api/centralwatch/image/:cameraId and /api/v1/centralwatch/image/:cameraId', () => {
  it('a cached image: the same bytes and headers on both paths', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
    try {
      m.images.set('cam-1', { data: Buffer.alloc(1024, 0xab), contentType: 'image/jpeg', ts: NOW - 30_000 });
      m.images.set('cam-2', { data: Buffer.alloc(700, 0xcd), contentType: '', ts: NOW - 4 * 60_000 });
      const a = await app();
      for (const p of ['/api/centralwatch/image/cam-1', v1Url('cam-1')]) {
        const res = await a.request(p);
        expect(res.status, p).toBe(200);
        expect([...res.headers], p).toEqual([
          ['access-control-allow-origin', '*'],
          ['cache-control', 'no-cache, no-store, must-revalidate'],
          ['content-type', 'image/jpeg'],
          ['expires', '0'],
          ['pragma', 'no-cache'],
          ['x-cache', 'HIT'],
          ['x-cache-age', '30'],
        ]);
        expect(Buffer.from(await res.arrayBuffer()).equals(Buffer.alloc(1024, 0xab)), p).toBe(true);
      }
      for (const p of ['/api/centralwatch/image/cam-2', v1Url('cam-2')]) {
        const res = await a.request(p);
        expect(res.headers.get('Content-Type'), p).toBe('image/jpeg'); // none given: JPEG
        expect(res.headers.get('X-Cache'), p).toBe('STALE');
        expect(res.headers.get('X-Cache-Age'), p).toBe('240');
      }
    } finally {
      vi.useRealTimers();
    }
  });

  it('no image to hand: the SVG placeholder, with its own headers, on both paths', async () => {
    const a = await app();
    const bodies: string[] = [];
    for (const p of ['/api/centralwatch/image/cam-9', v1Url('cam-9')]) {
      const res = await a.request(p);
      expect(res.status, p).toBe(200);
      expect([...res.headers], p).toEqual([
        ['access-control-allow-origin', '*'],
        ['cache-control', 'no-cache'],
        ['content-type', 'image/svg+xml'],
        ['x-cache', 'PLACEHOLDER'],
      ]);
      bodies.push(await res.text());
    }
    expect(bodies[0]).toContain('<svg');
    expect(bodies[1]).toBe(bodies[0]);
  });
});

describe('credentials and what is mirrored', () => {
  it('cameras and images are public on both paths; sites needs a key', async () => {
    const { isPublicPath } = await import('../../../src/services/auth/apiKey.js');
    for (const p of ['/api/centralwatch/cameras', '/api/v1/centralwatch/cameras', '/api/centralwatch/image/x', '/api/v1/centralwatch/image/x']) {
      expect(isPublicPath(p), p).toBe(true);
    }
    expect(isPublicPath('/api/centralwatch/sites')).toBe(false);
    expect(isPublicPath('/api/v1/centralwatch/sites')).toBe(false);
  });

  it('exactly the three routes', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    const a = await app();
    const cw = mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/centralwatch/'));
    expect(cw.map((r) => `${r.method} ${r.legacyPath}`).sort()).toEqual(IN_SCOPE.map((p) => `GET ${p}`).sort());
    const v1 = await a.request('/api/v1/centralwatch/image/');
    expect(v1.status).toBe(404);
    expect(await v1.json()).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
  });
});
