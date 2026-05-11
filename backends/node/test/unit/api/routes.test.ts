/**
 * HTTP route handler tests for every W3 source.
 *
 * Each handler is mounted on its own Hono app so we don't depend on
 * src/server.ts being wired up yet. The tests focus on:
 *   1. Empty-LiveStore behaviour: handlers must not 500
 *   2. Populated-LiveStore behaviour: handlers return the snapshot
 *
 * No real HTTP. The two routes that hit upstream live in /raw and
 * /beachsafe/beach/:slug — we mock fetchJson to keep them offline.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';

const fetchJsonMock = vi.fn();
const fetchTextMock = vi.fn();

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchText: fetchTextMock,
  fetchJson: fetchJsonMock,
  fetchRaw: vi.fn(),
  HttpError: class HttpError extends Error {
    status: number | null;
    constructor(message: string, status: number | null) {
      super(message);
      this.status = status;
    }
  },
}));

const SOURCE_KEYS = [
  'rfs_incidents',
  'bom_warnings',
  'traffic_incidents',
  'traffic_roadwork',
  'traffic_flood',
  'traffic_fire',
  'traffic_majorevent',
  'traffic_cameras',
  'beachwatch',
  'beachsafe',
  'beachsafe_details',
  'weather_current',
  'weather_radar',
  'pager',
];

function clearLiveStore(): void {
  for (const k of SOURCE_KEYS) liveStore.delete(k);
}

describe('GET /api/rfs/incidents', () => {
  beforeEach(() => {
    clearLiveStore();
    fetchTextMock.mockReset();
  });

  it('returns empty FeatureCollection when LiveStore has no data', async () => {
    const { rfsRouter } = await import('../../../src/api/rfs.js');
    const app = new Hono().route('/', rfsRouter);
    const res = await app.request('/api/rfs/incidents');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { type: string; count: number; features: unknown[] };
    expect(body.type).toBe('FeatureCollection');
    expect(body.count).toBe(0);
    expect(body.features).toEqual([]);
  });

  it('returns the LiveStore snapshot when set', async () => {
    liveStore.set('rfs_incidents', {
      type: 'FeatureCollection',
      features: [{ id: 'a' }],
      count: 1,
    });
    const { rfsRouter } = await import('../../../src/api/rfs.js');
    const app = new Hono().route('/', rfsRouter);
    const res = await app.request('/api/rfs/incidents');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { count: number };
    expect(body.count).toBe(1);
  });
});

describe('GET /api/bom/warnings', () => {
  beforeEach(() => clearLiveStore());

  it('returns empty shape when no data', async () => {
    const { bomRouter } = await import('../../../src/api/bom.js');
    const app = new Hono().route('/', bomRouter);
    const res = await app.request('/api/bom/warnings');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['count']).toBe(0);
    expect(body['warnings']).toEqual([]);
    expect(body['counts']).toEqual({ land: 0, marine: 0 });
  });

  it('returns the LiveStore snapshot', async () => {
    liveStore.set('bom_warnings', {
      warnings: [{ title: 'Severe Wind' }],
      count: 1,
      counts: { land: 1, marine: 0 },
    });
    const { bomRouter } = await import('../../../src/api/bom.js');
    const app = new Hono().route('/', bomRouter);
    const res = await app.request('/api/bom/warnings');
    const body = (await res.json()) as { count: number };
    expect(body.count).toBe(1);
  });
});

describe('GET /api/traffic/*', () => {
  beforeEach(() => {
    clearLiveStore();
    fetchJsonMock.mockReset();
  });

  it.each([
    'incidents',
    'roadwork',
    'flood',
    'fire',
    'majorevent',
  ])('returns empty FeatureCollection for /api/traffic/%s', async (kind) => {
    const { trafficRouter } = await import('../../../src/api/traffic.js');
    const app = new Hono().route('/', trafficRouter);
    const res = await app.request(`/api/traffic/${kind}`);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { count: number };
    expect(body.count).toBe(0);
  });

  it('returns cameras snapshot when set', async () => {
    liveStore.set('traffic_cameras', {
      type: 'FeatureCollection',
      features: [{ id: 'cam-1' }],
      count: 1,
    });
    const { trafficRouter } = await import('../../../src/api/traffic.js');
    const app = new Hono().route('/', trafficRouter);
    const res = await app.request('/api/traffic/cameras');
    const body = (await res.json()) as { count: number };
    expect(body.count).toBe(1);
  });

  it('/raw endpoint hits upstream and falls back on error', async () => {
    fetchJsonMock.mockRejectedValueOnce(new Error('502'));
    const { trafficRouter } = await import('../../../src/api/traffic.js');
    const app = new Hono().route('/', trafficRouter);
    const res = await app.request('/api/traffic/incidents/raw');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['type']).toBe('FeatureCollection');
    expect(body['features']).toEqual([]);
  });
});

describe('GET /api/beach*', () => {
  beforeEach(() => {
    clearLiveStore();
    fetchJsonMock.mockReset();
  });

  it('beachwatch returns empty FeatureCollection by default', async () => {
    const { beachRouter } = await import('../../../src/api/beach.js');
    const app = new Hono().route('/', beachRouter);
    const res = await app.request('/api/beachwatch');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['type']).toBe('FeatureCollection');
  });

  it('beachsafe returns empty array by default', async () => {
    const { beachRouter } = await import('../../../src/api/beach.js');
    const app = new Hono().route('/', beachRouter);
    const res = await app.request('/api/beachsafe');
    const body = (await res.json()) as unknown[];
    expect(body).toEqual([]);
  });

  it('beachsafe/details returns empty dict by default', async () => {
    const { beachRouter } = await import('../../../src/api/beach.js');
    const app = new Hono().route('/', beachRouter);
    const res = await app.request('/api/beachsafe/details');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body).toEqual({});
  });

  it('beachsafe/beach/:slug rejects malformed slugs', async () => {
    const { beachRouter } = await import('../../../src/api/beach.js');
    const app = new Hono().route('/', beachRouter);
    const longSlug = 'a'.repeat(150);
    const res = await app.request(`/api/beachsafe/beach/${longSlug}`);
    expect(res.status).toBe(400);
  });

  it('beachsafe/beach/:slug shapes the upstream response', async () => {
    fetchJsonMock.mockResolvedValueOnce({
      beach: {
        weather: { temp: 20 },
        currentTide: 'rising',
        currentUV: 5,
        attendances: { '2026-04-26': [{ count: 1 }] },
        is_patrolled_today: { flag: true, start: '09:00', end: '17:00' },
        patrol: 1,
        status: 'Patrolled',
        hazard: 3,
        todays_marine_warnings: [],
      },
    });
    const { beachRouter } = await import('../../../src/api/beach.js');
    const app = new Hono().route('/', beachRouter);
    const res = await app.request('/api/beachsafe/beach/bondi-beach');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['status']).toBe('Patrolled');
    expect(body['isPatrolledToday']).toBe(true);
    expect((body['latestAttendance'] as { date: string } | null)?.date).toBe(
      '2026-04-26',
    );
  });
});

describe('GET /api/weather/*', () => {
  beforeEach(() => clearLiveStore());

  it('current defaults to empty FeatureCollection', async () => {
    const { weatherRouter } = await import('../../../src/api/weather.js');
    const app = new Hono().route('/', weatherRouter);
    const res = await app.request('/api/weather/current');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['type']).toBe('FeatureCollection');
    expect(body['features']).toEqual([]);
  });

  it('radar defaults to empty radar object', async () => {
    const { weatherRouter } = await import('../../../src/api/weather.js');
    const app = new Hono().route('/', weatherRouter);
    const res = await app.request('/api/weather/radar');
    const body = (await res.json()) as { radar: { past: unknown[]; nowcast: unknown[] } };
    expect(body.radar.past).toEqual([]);
    expect(body.radar.nowcast).toEqual([]);
  });
});

describe('GET /api/pager/hits', () => {
  beforeEach(() => clearLiveStore());

  it('returns empty FeatureCollection when no snapshot', async () => {
    const { pagerRouter } = await import('../../../src/api/pager.js');
    const app = new Hono().route('/', pagerRouter);
    const res = await app.request('/api/pager/hits');
    expect(res.status).toBe(200);
    const body = (await res.json()) as { count: number; hours: number };
    expect(body.count).toBe(0);
    expect(body.hours).toBe(24);
  });

  it('clamps the hours param to 168', async () => {
    const { pagerRouter } = await import('../../../src/api/pager.js');
    const app = new Hono().route('/', pagerRouter);
    const res = await app.request('/api/pager/hits?hours=999');
    const body = (await res.json()) as { hours: number };
    expect(body.hours).toBe(168);
  });

  it('respects the limit param', async () => {
    const now = Math.floor(Date.now() / 1000);
    liveStore.set('pager', {
      messages: Array.from({ length: 5 }, (_, i) => ({
        id: i,
        incident_id: `25-00000${i}`,
        capcode: 'C',
        alias: 'A',
        agency: 'F',
        source: 's',
        message: 'm',
        lat: -33,
        lon: 151,
        incident_time: null,
        timestamp: now,
      })),
      count: 5,
    });
    const { pagerRouter } = await import('../../../src/api/pager.js');
    const app = new Hono().route('/', pagerRouter);
    const res = await app.request('/api/pager/hits?limit=2');
    const body = (await res.json()) as { count: number };
    expect(body.count).toBe(2);
  });

  it('drops messages older than the hours window', async () => {
    const old = Math.floor(Date.now() / 1000) - 30 * 3600; // 30 hours ago
    liveStore.set('pager', {
      messages: [
        {
          id: 1,
          incident_id: '25-111111',
          capcode: 'C',
          alias: '',
          agency: '',
          source: '',
          message: '',
          lat: -33,
          lon: 151,
          incident_time: null,
          timestamp: old,
        },
      ],
      count: 1,
    });
    const { pagerRouter } = await import('../../../src/api/pager.js');
    const app = new Hono().route('/', pagerRouter);
    const res = await app.request('/api/pager/hits?hours=12');
    const body = (await res.json()) as { count: number };
    expect(body.count).toBe(0);
  });
});
