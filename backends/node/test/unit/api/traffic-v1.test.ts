/**
 * Live Traffic NSW on /api/v1/ and on its old /api/ paths.
 *
 * One handler serves both (api/v1/mirror.ts), answering through
 * api/v1/respond.ts. GeoJSON is bare on both paths; all-feeds is wrapped in
 * `data` on v1. Where upstream cannot be read, the old path keeps the 200 it
 * always sent and v1 answers 503 service_unavailable. The old paths are
 * checked byte for byte.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';

const fetchJsonMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: vi.fn(),
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

// [route, LiveStore key, upstream file under /traffic/hazards/]
const HAZARDS = [
  ['incidents', 'traffic_incidents', 'incident'],
  ['roadwork', 'traffic_roadwork', 'roadwork'],
  ['flood', 'traffic_flood', 'flood'],
  ['fire', 'traffic_fire', 'fire'],
  ['majorevent', 'traffic_majorevent', 'majorevent'],
  ['alpine', 'traffic_alpine', 'alpine'],
  ['lga', 'traffic_lga', 'regional/lga-incidents'],
] as const;

const PATHS = [
  ...HAZARDS.flatMap(([p]) => [`/api/traffic/${p}`, `/api/traffic/${p}/raw`]),
  '/api/traffic/cameras',
  '/api/traffic/works',
  '/api/traffic/lga-incidents',
  '/api/traffic/all-feeds',
];

async function app() {
  const { trafficRouter } = await import('../../../src/api/traffic.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', trafficRouter);
  mirror(a, trafficRouter, PATHS);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function get(a: Hono, path: string) {
  const res = await a.request(path);
  return { status: res.status, type: res.headers.get('content-type'), text: await res.text() };
}

const v1 = (path: string) => path.replace(/^\/api\//, '/api/v1/');

const EMPTY_SNAPSHOT = '{"type":"FeatureCollection","features":[],"count":0}';
const EMPTY_FC = '{"type":"FeatureCollection","features":[]}';

function snapshot(id: string) {
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [151.1, -33.9] },
        properties: { id, title: 'Crash on West Botany Street', source: 'livetraffic' },
      },
    ],
    count: 1,
  };
}

// The hazard feeds as Transport for NSW publishes them.
function upstreamFeed() {
  return {
    type: 'FeatureCollection',
    lastPublished: 1793124630000,
    layerName: 'Incident',
    rights: { copyright: 'Transport for NSW', licence: 'https://opendata.transport.nsw.gov.au/dataset/live-traffic-hazards' },
    features: [
      {
        type: 'Feature',
        id: 95130,
        geometry: { type: 'Point', coordinates: [148.386062, -35.753159] },
        properties: { headline: '', displayName: 'CHANGED TRAFFIC CONDITIONS Snowy 2.0', created: 1623975269000, ended: false },
      },
    ],
  };
}

async function expectUnavailable(a: Hono, path: string) {
  const res = await get(a, path);
  expect(res.status).toBe(503);
  expect((JSON.parse(res.text) as { error: { code: string } }).error.code).toBe('service_unavailable');
}

beforeEach(() => {
  fetchJsonMock.mockReset();
  for (const [, key] of HAZARDS) liveStore.delete(key);
  liveStore.delete('traffic_works');
  liveStore.delete('traffic_cameras');
});

describe('the parsed hazard feeds and works (LiveStore)', () => {
  const ROUTES = [...HAZARDS.map(([p, key]) => [p, key] as const), ['works', 'traffic_works'] as const];

  it.each(ROUTES)('/traffic/%s answers the snapshot bare, on both paths', async (path, key) => {
    liveStore.set(key, snapshot(key));
    const a = await app();
    const want = JSON.stringify(snapshot(key));
    const old = await get(a, `/api/traffic/${path}`);
    expect(old).toEqual({ status: 200, type: 'application/json', text: want });
    const now = await get(a, `/api/v1/traffic/${path}`);
    expect(now).toEqual({ status: 200, type: 'application/json', text: want });
  });

  it.each(ROUTES)('/traffic/%s answers an empty collection before the first poll', async (path) => {
    const a = await app();
    expect((await get(a, `/api/traffic/${path}`)).text).toBe(EMPTY_SNAPSHOT);
    expect((await get(a, `/api/v1/traffic/${path}`)).text).toBe(EMPTY_SNAPSHOT);
  });
});

describe('/traffic/cameras', () => {
  it('answers the snapshot bare, on both paths', async () => {
    const cams = {
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [151.10533, -34.02977] },
          properties: { id: 'cam-1', title: '5 Ways (Miranda)', view: 'Looking west', direction: 'W', region: 'SYD_SOUTH', imageUrl: 'https://example.com/a.jpeg', path: '5-ways-miranda', source: 'livetraffic_cameras' },
        },
      ],
      count: 1,
    };
    liveStore.set('traffic_cameras', cams);
    const a = await app();
    expect((await get(a, '/api/traffic/cameras')).text).toBe(JSON.stringify(cams));
    expect((await get(a, '/api/v1/traffic/cameras')).text).toBe(JSON.stringify(cams));
  });

  it('answers an empty collection before the first poll', async () => {
    const a = await app();
    expect((await get(a, '/api/traffic/cameras')).text).toBe(EMPTY_SNAPSHOT);
    expect((await get(a, '/api/v1/traffic/cameras')).text).toBe(EMPTY_SNAPSHOT);
  });
});

describe('/traffic/{feed}/raw', () => {
  it.each(HAZARDS)('/traffic/%s/raw passes the feed through, bare on both paths', async (path, _key, endpoint) => {
    fetchJsonMock.mockResolvedValue(upstreamFeed());
    const a = await app();
    const want = JSON.stringify(upstreamFeed());
    expect(await get(a, `/api/traffic/${path}/raw`)).toEqual({ status: 200, type: 'application/json', text: want });
    expect(await get(a, `/api/v1/traffic/${path}/raw`)).toEqual({ status: 200, type: 'application/json', text: want });
    expect(fetchJsonMock).toHaveBeenCalledWith(
      `https://www.livetraffic.com/traffic/hazards/${endpoint}.json`,
      expect.anything(),
    );
  });

  it.each(HAZARDS)('/traffic/%s/raw when upstream fails: the old path its empty 200, v1 a 503', async (path) => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 502'));
    const a = await app();
    expect(await get(a, `/api/traffic/${path}/raw`)).toEqual({ status: 200, type: 'application/json', text: EMPTY_FC });
    await expectUnavailable(a, `/api/v1/traffic/${path}/raw`);
  });

  it('when upstream answers something other than the feed: the old path passes it on, v1 a 503', async () => {
    fetchJsonMock.mockResolvedValue([{ id: 1 }]);
    const a = await app();
    expect(await get(a, '/api/traffic/incidents/raw')).toEqual({ status: 200, type: 'application/json', text: '[{"id":1}]' });
    await expectUnavailable(a, '/api/v1/traffic/incidents/raw');
  });
});

describe('/traffic/lga-incidents', () => {
  it("answers the feed's features without its other top-level fields, bare on both paths", async () => {
    fetchJsonMock.mockResolvedValue(upstreamFeed());
    const a = await app();
    const want = JSON.stringify({ type: 'FeatureCollection', features: upstreamFeed().features });
    expect(await get(a, '/api/traffic/lga-incidents')).toEqual({ status: 200, type: 'application/json', text: want });
    expect(await get(a, '/api/v1/traffic/lga-incidents')).toEqual({ status: 200, type: 'application/json', text: want });
    expect(fetchJsonMock).toHaveBeenCalledWith(
      'https://www.livetraffic.com/traffic/hazards/regional/lga-incidents.json',
      expect.anything(),
    );
  });

  it('takes a bare list of records as the features', async () => {
    fetchJsonMock.mockResolvedValue([{ id: 1 }, { id: 2 }]);
    const a = await app();
    const want = '{"type":"FeatureCollection","features":[{"id":1},{"id":2}]}';
    expect((await get(a, '/api/traffic/lga-incidents')).text).toBe(want);
    expect((await get(a, '/api/v1/traffic/lga-incidents')).text).toBe(want);
  });

  it('when upstream answers something else: the old path its empty 200, v1 a 503', async () => {
    fetchJsonMock.mockResolvedValue({ error: 'maintenance' });
    const a = await app();
    expect(await get(a, '/api/traffic/lga-incidents')).toEqual({ status: 200, type: 'application/json', text: EMPTY_FC });
    await expectUnavailable(a, '/api/v1/traffic/lga-incidents');
  });

  it('when upstream fails: the old path its empty 200, v1 a 503', async () => {
    fetchJsonMock.mockRejectedValue(new Error('timeout'));
    const a = await app();
    expect(await get(a, '/api/traffic/lga-incidents')).toEqual({ status: 200, type: 'application/json', text: EMPTY_FC });
    await expectUnavailable(a, '/api/v1/traffic/lga-incidents');
  });
});

describe('/traffic/all-feeds', () => {
  const FEED = [
    { id: '1', eventType: 'Roadwork', eventCategory: 'Roadwork' },
    { id: '2', eventType: 'liveCams', eventCategory: 'liveCams' },
    { id: '3', eventCategory: 'LGACouncilParticipation' },
    { id: '4' },
    { id: '5', eventType: 'Roadwork', eventCategory: 'Roadwork' },
  ];
  const BODY = {
    raw: FEED,
    grouped: {
      Roadwork: [FEED[0], FEED[4]],
      liveCams: [FEED[1]],
      LGACouncilParticipation: [FEED[2]],
      unknown: [FEED[3]],
    },
    eventTypes: ['Roadwork', 'liveCams', 'LGACouncilParticipation', 'unknown'],
    totalCount: 5,
  };

  it('answers the records and the records grouped — wrapped in data on v1', async () => {
    fetchJsonMock.mockResolvedValue(FEED);
    const a = await app();
    expect(await get(a, '/api/traffic/all-feeds')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(BODY) });
    expect(await get(a, '/api/v1/traffic/all-feeds')).toEqual({
      status: 200,
      type: 'application/json',
      text: JSON.stringify({ data: BODY }),
    });
  });

  it('when upstream answers something other than a list: the old path its empty 200, v1 a 503', async () => {
    fetchJsonMock.mockResolvedValue({ type: 'FeatureCollection', features: [] });
    const a = await app();
    expect(await get(a, '/api/traffic/all-feeds')).toEqual({
      status: 200,
      type: 'application/json',
      text: '{"raw":[],"grouped":{},"eventTypes":[],"totalCount":0}',
    });
    await expectUnavailable(a, '/api/v1/traffic/all-feeds');
  });

  it('when upstream fails: the old path its 200 with the error, v1 a 503', async () => {
    fetchJsonMock.mockRejectedValue(new Error('HTTP 500 for https://www.livetraffic.com/datajson/all-feeds-web.json'));
    const a = await app();
    expect(await get(a, '/api/traffic/all-feeds')).toEqual({
      status: 200,
      type: 'application/json',
      text: '{"error":"HTTP 500 for https://www.livetraffic.com/datajson/all-feeds-web.json","raw":[],"grouped":{},"totalCount":0}',
    });
    const res = await get(a, '/api/v1/traffic/all-feeds');
    expect(res.status).toBe(503);
    // The v1 error does not repeat the upstream failure.
    expect(res.text).not.toContain('HTTP 500');
    expect(JSON.parse(res.text)).toEqual({
      error: { code: 'service_unavailable', message: expect.any(String) },
    });
  });
});

describe('the mirror', () => {
  it('serves every route on /api/v1/; an unknown v1 path is a v1 404', async () => {
    liveStore.set('traffic_incidents', snapshot('x'));
    fetchJsonMock.mockResolvedValue(upstreamFeed());
    const a = await app();
    for (const p of PATHS) expect((await a.request(v1(p))).status, p).not.toBe(404);
    const miss = await get(a, '/api/v1/traffic/nope');
    expect(miss.status).toBe(404);
    expect(JSON.parse(miss.text).error.code).toBe('not_found');
    // The old path's own 404 is untouched.
    expect(await get(a, '/api/traffic/nope')).toEqual({ status: 404, type: 'text/plain; charset=UTF-8', text: '404 Not Found' });
  });
});
