/**
 * The QLDTraffic camera routes on /api/v1/ and on their old /api/ paths.
 *
 * Both read the poller's snapshot and answer GeoJSON, which is bare on both
 * paths (api/v1/respond.ts), so the two must answer the same bytes. Both are
 * mirrored; nothing in this router is left off v1.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';

const IN_SCOPE = ['/api/qld/cameras', '/api/qld/flood-cameras'];

async function app() {
  const { qldCamerasRouter } = await import('../../../src/api/qld-cameras.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', qldCamerasRouter);
  mirror(a, qldCamerasRouter, IN_SCOPE);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function get(a: Hono, path: string) {
  const res = await a.request(path);
  return { status: res.status, headers: [...res.headers], text: await res.text() };
}

const JSON_TYPE = 'application/json';
const EMPTY = '{"type":"FeatureCollection","features":[],"count":0}';

function snapshot(source: 'qldtraffic_cameras' | 'qldtraffic_floodcameras', id: string) {
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [145.3725, -16.4471] },
        properties: {
          id,
          title: 'Mossman - Mossman Daintree Rd at Foxton bridge - North',
          view: 'Mossman',
          direction: 'North',
          region: 'Far North',
          imageUrl: 'https://example.com/cam.jpg',
          path: '',
          credit: 'Douglas Shire Council',
          note: 'Image updates every 30 minutes',
          source,
        },
      },
    ],
    count: 1,
  };
}

const ROUTES = [
  ['/api/qld/cameras', 'qld_cameras', 'qldtraffic_cameras', 'qld:12'],
  ['/api/qld/flood-cameras', 'qld_flood_cameras', 'qldtraffic_floodcameras', 'qldflood:190'],
] as const;

beforeEach(() => {
  liveStore.delete('qld_cameras');
  liveStore.delete('qld_flood_cameras');
});

describe('the camera snapshots, bare on both paths', () => {
  it.each(ROUTES)('%s answers the snapshot byte for byte', async (path, key, source, id) => {
    const snap = snapshot(source, id);
    liveStore.set(key, snap);
    const a = await app();
    for (const p of [path, path.replace('/api/', '/api/v1/')]) {
      expect(await get(a, p), p).toEqual({ status: 200, headers: [['content-type', JSON_TYPE]], text: JSON.stringify(snap) });
    }
  });

  it.each(ROUTES)('%s answers an empty collection before the first poll', async (path) => {
    const a = await app();
    for (const p of [path, path.replace('/api/', '/api/v1/')]) {
      expect(await get(a, p), p).toEqual({ status: 200, headers: [['content-type', JSON_TYPE]], text: EMPTY });
    }
  });
});

describe('the mirror', () => {
  it('an unknown v1 path is a v1 404; the old 404 is untouched', async () => {
    const a = await app();
    const miss = await get(a, '/api/v1/qld/nope');
    expect(miss.status).toBe(404);
    expect(JSON.parse(miss.text)).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
    expect(await get(a, '/api/qld/nope')).toEqual({
      status: 404,
      headers: [['content-type', 'text/plain; charset=UTF-8']],
      text: '404 Not Found',
    });
  });

  it('mirroring the in-scope paths registers exactly those', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    await app();
    const qld = mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/qld/'));
    expect(qld.map((r) => `${r.method} ${r.legacyPath}`).sort()).toEqual(IN_SCOPE.map((p) => `GET ${p}`).sort());
  });
});
