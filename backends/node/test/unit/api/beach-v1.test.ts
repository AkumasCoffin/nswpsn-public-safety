/**
 * The beach routes on /api/v1/ and on their old /api/ paths.
 *
 * One handler chain serves both (api/v1/mirror.ts). Beachwatch's GeoJSON is
 * bare on both paths; the BeachSafe answers are wrapped in `data` on v1. When
 * BeachSafe cannot be read, /beachsafe/beach/:slug keeps the `{}` (200) it
 * always sent on the old path and answers 404 or 503 in the v1 shape. The old
 * bodies are written out literally here.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';
import { HttpError } from '../../../src/sources/shared/http.js';

const m = vi.hoisted(() => ({ fetchJson: vi.fn() }));

vi.mock('../../../src/sources/shared/http.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/sources/shared/http.js')>()),
  fetchJson: m.fetchJson,
}));

// /api/beachsafe/details stays off v1: nothing fills its store.
const IN_SCOPE = ['/api/beachwatch', '/api/beachsafe', '/api/beachsafe/beach/:slug'];

async function app() {
  const { beachRouter } = await import('../../../src/api/beach.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', beachRouter);
  mirror(a, beachRouter, IN_SCOPE);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

const JSON_TYPE = 'application/json';

const BEACHWATCH = {
  type: 'FeatureCollection',
  features: [
    {
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [152.116518, -32.779284] },
      properties: {
        id: '1c31d57b-d6bd-40a7-90ba-005a452061d6',
        siteName: 'One Mile Beach',
        pollutionForecast: 'Unlikely',
        pollutionForecastTimeStamp: '2026-10-27T19:00:01.3+00:00',
        latestResult: 'Good',
        latestResultRating: 4,
        latestResultObservationDate: '2026-10-24T11:00:00+11:00',
      },
    },
  ],
};

const BEACHSAFE = [
  {
    id: 7, name: 'Bondi Beach', lat: -33.89, lng: 151.27, url: '/nsw/waverley/bondi-beach', slug: 'bondi-beach',
    patrolled: false, status: 'Open', hasToilet: true, hasParking: true, dogsAllowed: false,
    image: 'https://example.org/bondi.jpg', weather: { air_temp: '20.70' }, hazards: [],
    isPatrolledToday: true, patrolStart: '06:00', patrolEnd: '19:00', patrol: 2,
  },
];

const UPSTREAM_DETAIL = {
  beach: {
    weather: { air_temp: '20.70' },
    currentTide: { date_time_local: '2026-10-28 10:26:00', height: '1.87', state: 'high' },
    currentUV: { index: 8, status: 'Very High' },
    attendances: { '2026-10-27': { swimmers: 10 }, '2026-10-28': { swimmers: 40 } },
    todays_marine_warnings: [],
    patrol: 2,
    is_patrolled_today: { start: '06:00', end: '19:00', flag: true },
    status: 'Open',
    hazard: 7,
  },
};

const SHAPED_DETAIL = {
  weather: { air_temp: '20.70' },
  currentTide: { date_time_local: '2026-10-28 10:26:00', height: '1.87', state: 'high' },
  currentUV: { index: 8, status: 'Very High' },
  latestAttendance: { date: '2026-10-28', entries: { swimmers: 40 } },
  todays_marine_warnings: [],
  patrol: 2,
  patrolStart: '06:00',
  patrolEnd: '19:00',
  isPatrolledToday: true,
  status: 'Open',
  hazard: 7,
};

beforeEach(() => {
  m.fetchJson.mockReset();
  for (const k of ['beachwatch', 'beachsafe', 'beachsafe_details']) liveStore.delete(k);
});

describe('GET /api/beachwatch and /api/v1/beachwatch', () => {
  it('the GeoJSON as Beachwatch publishes it, bare on both paths', async () => {
    liveStore.set('beachwatch', BEACHWATCH);
    const a = await app();
    for (const p of ['/api/beachwatch', '/api/v1/beachwatch']) {
      const res = await a.request(p);
      expect(res.status, p).toBe(200);
      expect([...res.headers], p).toEqual([['content-type', JSON_TYPE]]);
      expect(await res.text(), p).toBe(JSON.stringify(BEACHWATCH));
    }
  });

  it('an empty collection before the first refresh', async () => {
    const a = await app();
    for (const p of ['/api/beachwatch', '/api/v1/beachwatch']) {
      expect(await (await a.request(p)).text(), p).toBe('{"type":"FeatureCollection","features":[]}');
    }
  });
});

describe('GET /api/beachsafe and /api/beachsafe/details', () => {
  it('the beach list: bare on the old path, in data on v1', async () => {
    liveStore.set('beachsafe', BEACHSAFE);
    const a = await app();
    const old = await a.request('/api/beachsafe');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify(BEACHSAFE));
    const v1 = await a.request('/api/v1/beachsafe');
    expect(v1.status).toBe(200);
    expect(await v1.text()).toBe(JSON.stringify({ data: BEACHSAFE }));
  });

  it('empty before the first refresh', async () => {
    const a = await app();
    expect(await (await a.request('/api/beachsafe')).text()).toBe('[]');
    expect(await (await a.request('/api/v1/beachsafe')).text()).toBe('{"data":[]}');
  });

  it('the details store: unchanged on the old path, and not on v1', async () => {
    const a = await app();
    expect(await (await a.request('/api/beachsafe/details')).text()).toBe('{}');
    liveStore.set('beachsafe_details', { 'bondi-beach': { status: 'Open' } });
    expect(await (await a.request('/api/beachsafe/details')).text()).toBe('{"bondi-beach":{"status":"Open"}}');
    const v1 = await a.request('/api/v1/beachsafe/details');
    expect(v1.status).toBe(404);
    expect((await v1.json()).error.code).toBe('not_found');
  });
});

describe('GET /api/beachsafe/beach/:slug and /api/v1/beachsafe/beach/:slug', () => {
  it('the shaped detail, read from BeachSafe by the normalised slug', async () => {
    m.fetchJson.mockResolvedValue(UPSTREAM_DETAIL);
    const a = await app();
    const old = await a.request('/api/beachsafe/beach/Bondi%20Beach');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify(SHAPED_DETAIL));
    const v1 = await a.request('/api/v1/beachsafe/beach/bondi-beach');
    expect(v1.status).toBe(200);
    expect(await v1.text()).toBe(JSON.stringify({ data: SHAPED_DETAIL }));
    expect(m.fetchJson.mock.calls.map((c) => c[0])).toEqual([
      'https://beachsafe.org.au/api/v4/beach/bondi-beach',
      'https://beachsafe.org.au/api/v4/beach/bondi-beach',
    ]);
  });

  it('a bad slug: the old 400 body; validation_failed on v1; BeachSafe never asked', async () => {
    const a = await app();
    for (const slug of ['%20', 'a'.repeat(101)]) {
      const old = await a.request(`/api/beachsafe/beach/${slug}`);
      expect(old.status, slug).toBe(400);
      expect([...old.headers], slug).toEqual([['content-type', JSON_TYPE]]);
      expect(await old.text(), slug).toBe('{"error":"Invalid slug"}');
      const v1 = await a.request(`/api/v1/beachsafe/beach/${slug}`);
      expect(v1.status, slug).toBe(400);
      expect(await v1.json(), slug).toEqual({
        error: {
          code: 'validation_failed',
          message: 'slug: must be 1 to 100 characters',
          details: { issues: [{ path: 'slug', message: 'must be 1 to 100 characters' }] },
        },
      });
    }
    expect(m.fetchJson).not.toHaveBeenCalled();
  });

  it('a beach BeachSafe does not know: {} on the old path, 404 not_found on v1', async () => {
    m.fetchJson.mockRejectedValue(new HttpError('HTTP 404', 404, 'https://beachsafe.org.au/api/v4/beach/nowhere'));
    const a = await app();
    const old = await a.request('/api/beachsafe/beach/nowhere');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe('{}');
    const v1 = await a.request('/api/v1/beachsafe/beach/nowhere');
    expect(v1.status).toBe(404);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('not_found');
  });

  it('BeachSafe failing or unreachable: {} on the old path, 503 service_unavailable on v1', async () => {
    const failures = [
      new HttpError('HTTP 500', 500, 'https://beachsafe.org.au/api/v4/beach/bondi-beach'),
      new HttpError('timeout after 10000ms', null, 'https://beachsafe.org.au/api/v4/beach/bondi-beach'),
      new Error('boom'),
    ];
    const a = await app();
    for (const err of failures) {
      m.fetchJson.mockRejectedValue(err);
      const old = await a.request('/api/beachsafe/beach/bondi-beach');
      expect(old.status, err.message).toBe(200);
      expect(await old.text(), err.message).toBe('{}');
      const v1 = await a.request('/api/v1/beachsafe/beach/bondi-beach');
      expect(v1.status, err.message).toBe(503);
      expect(((await v1.json()) as { error: { code: string } }).error.code, err.message).toBe('service_unavailable');
    }
  });

  it('an answer with no beach in it: shaped defaults on the old path, 503 on v1', async () => {
    m.fetchJson.mockResolvedValue({ error: 'something else' });
    const a = await app();
    const old = await a.request('/api/beachsafe/beach/bondi-beach');
    expect(old.status).toBe(200);
    expect(await old.text()).toBe(
      '{"weather":{},"currentTide":null,"currentUV":null,"latestAttendance":null,"todays_marine_warnings":[],"patrol":0,"patrolStart":"","patrolEnd":"","isPatrolledToday":false,"status":"Unknown","hazard":0}',
    );
    const v1 = await a.request('/api/v1/beachsafe/beach/bondi-beach');
    expect(v1.status).toBe(503);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('service_unavailable');
  });
});

describe('what is mirrored', () => {
  it('exactly the four beach routes, and nothing else under them', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    const a = await app();
    const beach = mirroredRoutes().filter((r) => /^\/api\/v1\/beach/.test(r.path));
    expect(beach.map((r) => `${r.method} ${r.legacyPath}`).sort()).toEqual(IN_SCOPE.map((p) => `GET ${p}`).sort());
    const v1 = await a.request('/api/v1/beachsafe/beach');
    expect(v1.status).toBe(404);
    expect(await v1.json()).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
  });
});
