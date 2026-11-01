/**
 * The QLDTraffic camera routes, for the map. Not on /api/v1/: camera images
 * are not part of the API (v1.test.ts checks they are absent there).
 *
 * Both read the poller's snapshot and answer GeoJSON, byte for byte.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';

async function app() {
  const { qldCamerasRouter } = await import('../../../src/api/qld-cameras.js');
  const a = new Hono();
  a.route('/', qldCamerasRouter);
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

describe('the camera snapshots', () => {
  it.each(ROUTES)('%s answers the snapshot byte for byte', async (path, key, source, id) => {
    const snap = snapshot(source, id);
    liveStore.set(key, snap);
    const a = await app();
    expect(await get(a, path)).toEqual({ status: 200, headers: [['content-type', JSON_TYPE]], text: JSON.stringify(snap) });
  });

  it.each(ROUTES)('%s answers an empty collection before the first poll', async (path) => {
    const a = await app();
    expect(await get(a, path)).toEqual({ status: 200, headers: [['content-type', JSON_TYPE]], text: EMPTY });
  });

  it('an unknown path keeps the plain-text 404', async () => {
    const a = await app();
    expect(await get(a, '/api/qld/nope')).toEqual({
      status: 404,
      headers: [['content-type', 'text/plain; charset=UTF-8']],
      text: '404 Not Found',
    });
  });
});
