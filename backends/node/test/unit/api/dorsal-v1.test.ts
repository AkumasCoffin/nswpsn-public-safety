/**
 * Dorsal shark reports on /api/v1/ and on the old /api/ path.
 *
 * One handler serves both (api/v1/mirror.ts). The answer is GeoJSON, so it is
 * bare on both paths and byte for byte the same. The query string stays as
 * lenient as it always was: nothing a caller sends turns into a 400.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';

const IN_SCOPE = ['/api/dorsal/reports'];

async function app() {
  const { dorsalRouter } = await import('../../../src/api/dorsal.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', dorsalRouter);
  mirror(a, dorsalRouter, IN_SCOPE);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

const NOW = Date.UTC(2026, 9, 28, 4, 0, 0);
const NOW_S = NOW / 1000;
const JSON_TYPE = 'application/json';

function report(id: string, ageHours: number, category: string, state: string) {
  const unix = NOW_S - ageHours * 3600;
  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [151.3, -33.8] },
    properties: {
      report_id: id, title: `Bull Shark — Beach ${id}`, category, encounter: 'Sighting', species: 'Bull Shark',
      shark_length_m: 2.5, distance_from_shore_m: null, location: `Beach ${id}`, zone: 'Sydney', state,
      reported_at: new Date(unix * 1000).toISOString(), reported_at_unix: unix, official: false,
      provenance: 'Community report', comment: '', photo_url: null, url: 'https://www.dorsalwatch.com/',
    },
  };
}

const R1 = report('1', 1, 'sighting', 'NSW');
const R30 = report('30', 30, 'drone', 'WA');
const R60 = report('60', 60, 'incident', 'NSW');
const R200 = report('200', 200, 'closure', 'QLD');
const FETCHED_AT = '2026-10-28T03:58:00.000Z';

/** What the route has always sent: the matching reports, newest first, plus the window. */
const body = (features: unknown[], hours: number) =>
  JSON.stringify({ type: 'FeatureCollection', features, count: features.length, hours, fetched_at: FETCHED_AT });

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
  liveStore.set('dorsal_reports', {
    type: 'FeatureCollection',
    features: [R1, R30, R60, R200],
    count: 4,
    fetched_at: FETCHED_AT,
  });
});
afterEach(() => {
  vi.useRealTimers();
  liveStore.delete('dorsal_reports');
});

describe('GET /api/dorsal/reports and /api/v1/dorsal/reports', () => {
  it('the last 48 hours by default, bare GeoJSON on both paths', async () => {
    const a = await app();
    for (const p of ['/api/dorsal/reports', '/api/v1/dorsal/reports']) {
      const res = await a.request(p);
      expect(res.status, p).toBe(200);
      expect([...res.headers], p).toEqual([['content-type', JSON_TYPE]]);
      expect(await res.text(), p).toBe(body([R1, R30], 48));
    }
  });

  it('hours, category and state filter as they always did', async () => {
    const a = await app();
    const cases: Array<[string, unknown[], number]> = [
      ['hours=168', [R1, R30, R60], 168],
      ['hours=336', [R1, R30, R60, R200], 336],
      ['hours=9999', [R1, R30, R60, R200], 336], // held to 14 days
      ['hours=2.9', [R1], 2], // rounded down
      ['hours=168&category=DRONE,incident', [R30, R60], 168],
      ['hours=336&state=nsw', [R1, R60], 336],
      ['hours=336&state=wa,%20qld%20', [R30, R200], 336],
      ['category=,,', [R1, R30], 48], // empty list: no filter
      ['state=', [R1, R30], 48],
      ['hours=24&hours=168', [R1], 24], // a repeat: the first value
    ];
    for (const [qs, features, hours] of cases) {
      for (const p of ['/api/dorsal/reports', '/api/v1/dorsal/reports']) {
        const res = await a.request(`${p}?${qs}`);
        expect(res.status, `${p}?${qs}`).toBe(200);
        expect(await res.text(), `${p}?${qs}`).toBe(body(features, hours));
      }
    }
  });

  it('junk never becomes a 400: the default window, on both paths', async () => {
    const a = await app();
    for (const qs of ['hours=', 'hours=abc', 'hours=-5', 'hours=0', 'hours=Infinity', 'hours=%zz', 'x=1', 'category=nope&hours=']) {
      for (const p of ['/api/dorsal/reports', '/api/v1/dorsal/reports']) {
        const res = await a.request(`${p}?${qs}`);
        expect(res.status, `${p}?${qs}`).toBe(200);
        const parsed = (await res.json()) as { hours: number; type: string };
        expect(parsed.type, `${p}?${qs}`).toBe('FeatureCollection');
        expect(parsed.hours, `${p}?${qs}`).toBe(48);
      }
    }
  });

  it('nothing from Dorsal yet: an empty collection, not an error', async () => {
    liveStore.delete('dorsal_reports');
    const a = await app();
    const empty = '{"type":"FeatureCollection","features":[],"count":0,"hours":48,"fetched_at":"1970-01-01T00:00:00.000Z"}';
    for (const p of ['/api/dorsal/reports', '/api/v1/dorsal/reports']) {
      const res = await a.request(p);
      expect(res.status, p).toBe(200);
      expect(await res.text(), p).toBe(empty);
    }
  });
});

describe('what is mirrored', () => {
  it('only /api/dorsal/reports', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    const a = await app();
    const dorsal = mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/dorsal/'));
    expect(dorsal.map((r) => `${r.method} ${r.legacyPath}`)).toEqual(['GET /api/dorsal/reports']);
    const v1 = await a.request('/api/v1/dorsal/report');
    expect(v1.status).toBe(404);
    expect(await v1.json()).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
  });
});
