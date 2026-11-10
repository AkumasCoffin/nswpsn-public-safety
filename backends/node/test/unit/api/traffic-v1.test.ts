/**
 * The road endpoints on /api/v1/ and on their old /api/ paths.
 *
 * One handler serves both (api/v1/mirror.ts), answering through
 * api/v1/respond.ts. GeoJSON is bare on both paths; all-feeds is wrapped in
 * `data` on v1. Where upstream cannot be read, the old path keeps the 200 it
 * always sent and v1 answers 503 service_unavailable. The old paths are
 * checked byte for byte. /cameras is the site's alone — camera images are not
 * part of the API — and all-feeds leaves out the camera records it carries.
 * Incidents, roadwork, flood and major events carry the other states'
 * records after NSW's, and every feed takes ?state=; /traffic/works is gone.
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

// What server.ts mirrors: everything but /cameras.
const PATHS = [
  ...HAZARDS.flatMap(([p]) => [`/api/traffic/${p}`, `/api/traffic/${p}/raw`]),
  '/api/traffic/lga-incidents',
  '/api/traffic/all-feeds',
];

const INTERSTATE = ['roads_qld', 'roads_sa', 'roads_vic', 'roads_act'];

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

const LT_CREDIT = 'Live Traffic NSW (Transport for NSW)';

function snapshot(id: string) {
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [151.1, -33.9] },
        properties: { id, title: 'Crash on West Botany Street', state: 'NSW', source: 'livetraffic', credit: LT_CREDIT },
      },
    ],
    count: 1,
  };
}

/** Another state's record on the feed for `type`, as sources/interstateRoads.ts stores it. */
function other(id: string, state: string, type: string) {
  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [153, -27.5] },
    properties: { id, type, title: `${state} ${type}`, state, source: 'qldtraffic', credit: 'QLDTraffic' },
  };
}
const fc = (features: unknown[]) => ({ type: 'FeatureCollection', features, count: features.length });

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
  for (const key of INTERSTATE) liveStore.delete(key);
  liveStore.delete('traffic_cameras');
});

describe('the parsed hazard feeds (LiveStore)', () => {
  const ROUTES = HAZARDS.map(([p, key]) => [p, key] as const);

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

  it('a snapshot saved before records named their state reads as NSW, credited to Live Traffic', async () => {
    liveStore.set('traffic_roadwork', fc([{ type: 'Feature', geometry: { type: 'Point', coordinates: [151, -33] }, properties: { id: 'old', source: 'livetraffic' } }]));
    const a = await app();
    const body = JSON.parse((await get(a, '/api/v1/traffic/roadwork?state=nsw')).text) as { features: Array<{ properties: Record<string, unknown> }> };
    expect(body.features.map((f) => f.properties)).toEqual([{ id: 'old', source: 'livetraffic', state: 'NSW', credit: LT_CREDIT }]);
  });
});

describe("the other states' records", () => {
  beforeEach(() => {
    liveStore.set('traffic_roadwork', snapshot('nsw-rw'));
    liveStore.set('traffic_incidents', snapshot('nsw-inc'));
    liveStore.set('traffic_fire', snapshot('nsw-fire'));
    liveStore.set('roads_qld', fc([other('qld:1', 'QLD', 'Roadwork'), other('qld:2', 'QLD', 'Flood'), other('qld:3', 'QLD', 'Incident')]));
    liveStore.set('roads_sa', fc([other('sa:1', 'SA', 'Roadwork'), other('sa:2', 'SA', 'Incident')]));
    liveStore.set('roads_act', fc([other('act:1', 'ACT', 'Major Event'), other('act:2', 'ACT', 'Roadwork')]));
    liveStore.set('roads_vic', fc([other('vic:1', 'VIC', 'Flood')]));
  });
  const ids = (text: string) => (JSON.parse(text) as { features: Array<{ properties: { id: string } }> }).features.map((f) => f.properties.id);

  it('each feed carries NSW first, then every state\'s records of its type, on both paths', async () => {
    const a = await app();
    for (const p of ['/api/traffic/roadwork', '/api/v1/traffic/roadwork']) {
      const res = await get(a, p);
      expect(ids(res.text), p).toEqual(['nsw-rw', 'qld:1', 'sa:1', 'act:2']);
      expect(JSON.parse(res.text).count, p).toBe(4);
    }
    expect(ids((await get(a, '/api/v1/traffic/incidents')).text)).toEqual(['nsw-inc', 'qld:3', 'sa:2']);
    expect(ids((await get(a, '/api/v1/traffic/flood')).text)).toEqual(['qld:2', 'vic:1']);
    expect(ids((await get(a, '/api/v1/traffic/majorevent')).text)).toEqual(['act:1']);
  });

  it('fire, alpine and the council feed stay NSW-only', async () => {
    const a = await app();
    expect(ids((await get(a, '/api/v1/traffic/fire')).text)).toEqual(['nsw-fire']);
    expect(ids((await get(a, '/api/v1/traffic/alpine')).text)).toEqual([]);
    expect(ids((await get(a, '/api/v1/traffic/lga')).text)).toEqual([]);
  });

  it('?state= keeps the states asked for, in any case, with the count to match', async () => {
    const a = await app();
    const res = await get(a, '/api/v1/traffic/roadwork?state=qld,Act');
    expect(ids(res.text)).toEqual(['qld:1', 'act:2']);
    expect(JSON.parse(res.text).count).toBe(2);
    expect(ids((await get(a, '/api/traffic/roadwork?state=NSW')).text)).toEqual(['nsw-rw']);
    expect(ids((await get(a, '/api/v1/traffic/fire?state=QLD')).text)).toEqual([]);
  });

  it('an unknown state is a 400 naming it', async () => {
    const a = await app();
    const v1res = await get(a, '/api/v1/traffic/roadwork?state=QLD,WA');
    expect(v1res.status).toBe(400);
    const v1body = JSON.parse(v1res.text) as { error: { code: string; message: string } };
    expect(v1body.error.code).toBe('validation_failed');
    expect(v1body.error.message).toBe('state: unknown state: WA (one of NSW, QLD, VIC, SA, ACT)');
    const old = await get(a, '/api/traffic/roadwork?state=WA');
    expect(old.status).toBe(400);
    expect(JSON.parse(old.text)).toEqual({ error: 'state: unknown state: WA (one of NSW, QLD, VIC, SA, ACT)' });
  });

  it('/traffic/works is gone, on both paths', async () => {
    const a = await app();
    expect((await get(a, '/api/traffic/works')).status).toBe(404);
    expect((await get(a, '/api/v1/traffic/works')).status).toBe(404);
  });
});

describe('/traffic/cameras', () => {
  it('answers the snapshot on /api/ only', async () => {
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
    expect((await get(a, '/api/v1/traffic/cameras')).status).toBe(404);
  });

  it('answers an empty collection before the first poll', async () => {
    const a = await app();
    expect((await get(a, '/api/traffic/cameras')).text).toBe(EMPTY_SNAPSHOT);
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
  // The camera record (liveCams) is left out everywhere.
  const BODY = {
    raw: [FEED[0], FEED[2], FEED[3], FEED[4]],
    grouped: {
      Roadwork: [FEED[0], FEED[4]],
      LGACouncilParticipation: [FEED[2]],
      unknown: [FEED[3]],
    },
    eventTypes: ['Roadwork', 'LGACouncilParticipation', 'unknown'],
    totalCount: 4,
  };

  it('answers the records and the records grouped, cameras left out — wrapped in data on v1', async () => {
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
