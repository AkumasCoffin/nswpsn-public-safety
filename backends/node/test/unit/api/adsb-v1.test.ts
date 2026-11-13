/**
 * The ADS-B routes on /api/v1/ and on their old /api/ paths.
 *
 * One handler chain serves both (api/v1/mirror.ts), so the thing to prove is
 * that the old path answers byte for byte as it did — status, body and the
 * headers the routes set — while v1 wraps the body in `data` and answers
 * errors in the shared shape. The old bodies are written out literally here,
 * as the handlers built them before they answered through api/v1/respond.ts.
 *
 * /api/adsb/receivers is node status for the site's own map: not mirrored.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';
import { DATA_RETENTION_DAYS } from '../../../src/lib/retention.js';

const m = vi.hoisted(() => ({
  fetchJson: vi.fn(),
  query: vi.fn(),
  pool: true,
  snapshot: null as unknown,
  trails: null as unknown,
  /** How far the track archive has written (null: nothing yet this run). */
  through: null as number | null,
  /** The `recent` values the handlers asked the source for. */
  recentAsked: [] as Array<[string, number]>,
}));

vi.mock('../../../src/sources/shared/http.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/sources/shared/http.js')>()),
  fetchJson: m.fetchJson,
}));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve(m.pool ? { query: m.query } : null)),
  getWriterPool: vi.fn(() => Promise.resolve(m.pool ? { query: m.query } : null)),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

vi.mock('../../../src/services/adsbTrackArchive.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/services/adsbTrackArchive.js')>()),
  adsbArchivedThroughMs: () => m.through,
}));

vi.mock('../../../src/sources/adsb.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/sources/adsb.js')>()),
  adsbSnapshot: () => m.snapshot,
  adsbTrailsSnapshot: () => m.trails,
  adsbRecentAircraft: (sec: number) => {
    m.recentAsked.push(['aircraft', sec]);
    return { ...(m.snapshot as object), count: 1 };
  },
  adsbRecentTrails: (sec: number) => {
    m.recentAsked.push(['trails', sec]);
    return RECENT_TRAILS;
  },
}));

const RECENT_TRAILS = {
  trails: { '7c6b2d': [[-33.85, 151.2, 1400, 1_800_000_000_000], [-33.86, 151.21, null, 1_800_000_001_000]] },
  fetched_at: '2026-10-28T04:00:00.000Z',
};

const IN_SCOPE = ['/api/adsb/aircraft', '/api/adsb/history', '/api/adsb/trails', '/api/adsb/photo/:hex', '/api/adsb/info/:hex'];

async function app() {
  const { adsbRouter } = await import('../../../src/api/adsb.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', adsbRouter);
  mirror(a, adsbRouter, IN_SCOPE);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

/** 2026-10-28T04:00:00Z — on the minute and on the hour. */
const NOW = Date.UTC(2026, 9, 28, 4, 0, 0);
const OLDEST = NOW - DATA_RETENTION_DAYS * 86_400_000;

const SNAPSHOT = {
  aircraft: [
    {
      hex: '7c6b2d', callsign: 'POL30', reg: 'VH-PVA', type: 'B429', lat: -33.86, lon: 151.21,
      altFt: 1500, onGround: false, gsKt: 110.4, trackDeg: 271.2, category: 'A7', squawk: '1200',
      emergencySquawk: false, esTag: 'polair', ageSec: 0.4, sourceCount: 2, sources: ['adsb_lol', 'node:adsb-syd-01'],
      estimated: false, estimatedSec: null,
    },
    {
      hex: '~2a01f0', callsign: null, reg: null, type: null, lat: -34.1, lon: 150.9,
      altFt: null, onGround: true, gsKt: null, trackDeg: null, category: null, squawk: null,
      emergencySquawk: false, esTag: null, ageSec: 12, sourceCount: 1, sources: ['adsb_fi'],
      estimated: false, estimatedSec: null,
    },
  ],
  count: 2,
  emergency_count: 1,
  upstreams: [
    { id: 'adsb_lol', ok: true, circles_ok: 7, circles_total: 7, count: 412 },
    { id: 'adsb_one', ok: false, circles_ok: 0, circles_total: 7, count: 0, error: 'HTTP 403' },
    { id: 'nodes', ok: true, circles_ok: 1, circles_total: 1, count: 37 },
  ],
  bbox: [112, -44.3, 154.3, -9.7],
  fetched_at: '2026-10-28T03:59:58.000Z',
};

const TRAILS = {
  trails: { '7c6b2d': [[-33.85, 151.2, 1400], [-33.86, 151.21, null]] },
  fetched_at: '2026-10-28T03:59:58.000Z',
};

const JSON_TYPE = 'application/json';

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
  m.fetchJson.mockReset();
  m.query.mockReset();
  m.query.mockResolvedValue({ rows: [] });
  m.pool = true;
  m.snapshot = SNAPSHOT;
  m.trails = TRAILS;
  m.through = null;
  m.recentAsked = [];
});
afterEach(() => {
  vi.useRealTimers();
});

/** What the handler serves for SNAPSHOT: each aircraft gains seenAt, the
 *  moment it was heard (fetched_at less its age). */
const SERVED = {
  ...SNAPSHOT,
  aircraft: SNAPSHOT.aircraft.map((x) => ({
    ...x,
    seenAt: new Date(Date.parse(SNAPSHOT.fetched_at) - x.ageSec * 1000).toISOString(),
  })),
};

describe('GET /api/adsb/aircraft and /api/v1/adsb/aircraft', () => {
  it('stamps each aircraft with when it was heard', async () => {
    const a = await app();
    const res = (await (await a.request('/api/v1/adsb/aircraft')).json()) as {
      data: { aircraft: Array<{ hex: string; seenAt: string }> };
    };
    expect(res.data.aircraft.map((x) => [x.hex, x.seenAt])).toEqual([
      ['7c6b2d', '2026-10-28T03:59:57.600Z'],
      ['~2a01f0', '2026-10-28T03:59:46.000Z'],
    ]);
  });

  it('the snapshot plus the history window — bare on the old path, in data on v1', async () => {
    const a = await app();
    const body = { ...SERVED, history: { retentionDays: DATA_RETENTION_DAYS, oldestMs: OLDEST, newestMs: NOW } };

    const old = await a.request('/api/adsb/aircraft');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify(body));

    const v1 = await a.request('/api/v1/adsb/aircraft');
    expect(v1.status).toBe(200);
    expect(v1.headers.get('Content-Type')).toBe(JSON_TYPE);
    expect(await v1.text()).toBe(JSON.stringify({ data: body }));
  });
});

describe('GET /api/adsb/trails and /api/v1/adsb/trails', () => {
  it('the prebuilt trails, unchanged on the old path', async () => {
    const a = await app();
    const old = await a.request('/api/adsb/trails');
    expect(old.status).toBe(200);
    expect(await old.text()).toBe(JSON.stringify(TRAILS));
    const v1 = await a.request('/api/v1/adsb/trails');
    expect(v1.status).toBe(200);
    expect(await v1.text()).toBe(JSON.stringify({ data: TRAILS }));
  });
});

describe('?recent= on the live aircraft and trails', () => {
  it('serves the recent aircraft, cached for 2 s, on both paths', async () => {
    const a = await app();
    const body = { ...SERVED, count: 1, history: { retentionDays: DATA_RETENTION_DAYS, oldestMs: OLDEST, newestMs: NOW } };
    const v1 = await a.request('/api/v1/adsb/aircraft?recent=15');
    expect(v1.status).toBe(200);
    expect(v1.headers.get('Cache-Control')).toBe('public, max-age=2');
    expect(await v1.text()).toBe(JSON.stringify({ data: body }));
    const old = await a.request('/api/adsb/aircraft?recent=15');
    expect(await old.text()).toBe(JSON.stringify(body));
    expect(m.recentAsked).toEqual([['aircraft', 15], ['aircraft', 15]]);
  });

  it('serves the recent trail points with their times, cached for 2 s', async () => {
    const a = await app();
    const v1 = await a.request('/api/v1/adsb/trails?recent=15');
    expect(v1.status).toBe(200);
    expect(v1.headers.get('Cache-Control')).toBe('public, max-age=2');
    expect(await v1.text()).toBe(JSON.stringify({ data: RECENT_TRAILS }));
    expect(m.recentAsked).toEqual([['trails', 15]]);
  });

  it('ignores a recent that is not a whole number in range, and serves the full answer', async () => {
    const a = await app();
    for (const bad of ['abc', '0', '61', '2.5', '']) {
      const res = await a.request(`/api/v1/adsb/aircraft?recent=${bad}`);
      expect(res.status).toBe(200);
      expect(res.headers.get('Cache-Control')).toBeNull();
      const t = await a.request(`/api/v1/adsb/trails?recent=${bad}`);
      expect(await t.text()).toBe(JSON.stringify({ data: TRAILS }));
    }
    expect(m.recentAsked).toEqual([]);
  });
});

describe('GET /api/adsb/history and /api/v1/adsb/history', () => {
  /** One stored minute (migration 139) holding one aircraft. */
  const row = (minuteMs: number, offsetMs: number) => ({
    minute: new Date(minuteMs),
    aircraft: {
      '7c6b2d': {
        c: 'POL30', r: 'VH-PVA', t: 'B429', e: 'polair', s: ['adsb_lol'],
        p: [[offsetMs, -33.86, 151.21, 1500]],
      },
    },
  });

  /** What the route answers for this instant: adsbHistoryAt's result, as it always was. */
  async function served(at: number, trail: number) {
    const { adsbHistoryAt } = await import('../../../src/services/adsbHistory.js');
    return JSON.stringify(await adsbHistoryAt(at, trail, NOW));
  }

  it('a minute on disk: the history and the long cache, on both paths', async () => {
    m.through = NOW - 2 * 60_000; // written through 03:58
    m.query.mockResolvedValue({ rows: [row(NOW - 10 * 60_000, 0)] }); // 03:50
    const a = await app();
    const at = NOW - 9 * 60_000 - 30_000; // 03:50:30 → snapped to 03:50
    const expected = await served(at, 15);
    expect(JSON.parse(expected).count).toBe(1);

    const old = await a.request(`/api/adsb/history?at=${at}`);
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([
      ['cache-control', 'public, max-age=3600, stale-while-revalidate=86400'],
      ['content-type', JSON_TYPE],
    ]);
    expect(await old.text()).toBe(expected);

    const v1 = await a.request(`/api/v1/adsb/history?at=${at}`);
    expect(v1.status).toBe(200);
    expect(v1.headers.get('Cache-Control')).toBe('public, max-age=3600, stale-while-revalidate=86400');
    expect(await v1.text()).toBe(`{"data":${expected}}`);
  });

  it('a minute not yet on disk gets the short cache', async () => {
    m.through = NOW - 2 * 60_000;
    const a = await app();
    for (const p of ['/api/adsb/history', '/api/v1/adsb/history']) {
      const res = await a.request(`${p}?at=${NOW - 30_000}`);
      expect(res.status, p).toBe(200);
      expect(res.headers.get('Cache-Control'), p).toBe('public, max-age=30, stale-while-revalidate=120');
    }
  });

  it('nothing is long-cached before this process has written a minute', async () => {
    // After a restart only a write proves where the archive has reached.
    const a = await app();
    const res = await a.request(`/api/v1/adsb/history?at=${NOW - 60 * 60_000}`);
    expect(res.headers.get('Cache-Control')).toBe('public, max-age=30, stale-while-revalidate=120');
  });

  it('tolerant parameters stay tolerant: junk never becomes a 400', async () => {
    const a = await app();
    const cases: Array<[string, number]> = [
      [`at=${NOW}&trail=abc`, 15], // junk trail: the default
      [`at=${NOW}&trail=`, 0], // empty trail reads as 0
      [`at=${NOW}&trail=99`, 30], // held to the maximum
      [`at=${NOW}&trail=5&trail=abc`, 5], // a repeat: the first value
      ['at=soon&trail=7', 7], // junk at: now
    ];
    for (const [qs, trail] of cases) {
      for (const p of ['/api/adsb/history', '/api/v1/adsb/history']) {
        const res = await a.request(`${p}?${qs}`);
        expect(res.status, `${p}?${qs}`).toBe(200);
        const body = (await res.json()) as { atMs: number; trailMinutes: number; data?: { atMs: number; trailMinutes: number } };
        const h = body.data ?? body;
        expect(h.trailMinutes, `${p}?${qs}`).toBe(trail);
        expect(h.atMs, `${p}?${qs}`).toBe(NOW);
      }
    }
  });

  it('outside the retention window — including a missing or empty at, which reads as 0', async () => {
    const a = await app();
    for (const qs of [`at=${OLDEST - 1}`, '', 'at=']) {
      const old = await a.request(`/api/adsb/history${qs ? `?${qs}` : ''}`);
      expect(old.status, qs).toBe(400);
      expect([...old.headers], qs).toEqual([['content-type', JSON_TYPE]]);
      expect(await old.text(), qs).toBe(JSON.stringify({ error: 'outside the retention window', oldestMs: OLDEST }));

      const v1 = await a.request(`/api/v1/adsb/history${qs ? `?${qs}` : ''}`);
      expect(v1.status, qs).toBe(400);
      expect(await v1.json(), qs).toEqual({
        error: {
          code: 'validation_failed',
          message: 'at: older than the retention window',
          details: { issues: [{ path: 'at', message: 'older than the retention window' }], oldest_ms: OLDEST },
        },
      });
    }
    expect(m.query).not.toHaveBeenCalled();
  });

  it('no database: 503 with the old body on the old path', async () => {
    m.pool = false;
    const a = await app();
    const old = await a.request(`/api/adsb/history?at=${NOW}`);
    expect(old.status).toBe(503);
    expect(await old.text()).toBe('{"error":"history unavailable"}');
    const v1 = await a.request(`/api/v1/adsb/history?at=${NOW}`);
    expect(v1.status).toBe(503);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('service_unavailable');
  });

  it('a failing query: 500 with the old body on the old path', async () => {
    m.query.mockRejectedValue(new Error('statement timeout'));
    const a = await app();
    const old = await a.request(`/api/adsb/history?at=${NOW}`);
    expect(old.status).toBe(500);
    expect(await old.text()).toBe('{"error":"history query failed"}');
    const v1 = await a.request(`/api/v1/adsb/history?at=${NOW}`);
    expect(v1.status).toBe(500);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('internal_error');
  });
});

describe('GET /api/adsb/photo/:hex and /api/v1/adsb/photo/:hex', () => {
  const PHOTO = {
    id: '1234',
    thumbnail: { src: 'https://t.plnspttrs.net/1234_280.jpg' },
    thumbnail_large: { src: 'https://t.plnspttrs.net/1234.jpg' },
    link: 'https://www.planespotters.net/photo/1234',
    photographer: 'A. Spotter',
  };
  const SERVED = { src: PHOTO.thumbnail_large.src, link: PHOTO.link, photographer: PHOTO.photographer, source: 'planespotters' };

  it('by hex: the photo, bare on the old path and in data on v1', async () => {
    m.fetchJson.mockResolvedValue({ photos: [PHOTO] });
    const a = await app();
    const old = await a.request('/api/adsb/photo/7C0001');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify({ photo: SERVED }));
    const v1 = await a.request('/api/v1/adsb/photo/7c0001');
    expect(v1.status).toBe(200);
    expect(await v1.text()).toBe(JSON.stringify({ data: { photo: SERVED } }));
    expect(m.fetchJson.mock.calls[0]![0]).toBe('https://api.planespotters.net/pub/photos/hex/7c0001');
  });

  it('falls back to the registration, upper-cased', async () => {
    m.fetchJson.mockImplementation(async (url: string) => (url.includes('/reg/') ? { photos: [PHOTO] } : { photos: [] }));
    const a = await app();
    const v1 = await a.request('/api/v1/adsb/photo/7c0002?reg=vh-abc');
    expect(await v1.json()).toEqual({ data: { photo: SERVED } });
    expect(m.fetchJson.mock.calls.map((c) => c[0])).toEqual([
      'https://api.planespotters.net/pub/photos/hex/7c0002',
      'https://api.planespotters.net/pub/photos/reg/VH-ABC',
    ]);
  });

  it('airport-data.com is the backup when Planespotters has none', async () => {
    m.fetchJson.mockImplementation(async (url: string) =>
      url.includes('airport-data.com')
        ? { status: 200, count: 1, data: [{ image: 'https://airport-data.com/images/aircraft/thumbnails/001/939/001939643.jpg', link: 'https://airport-data.com/aircraft/photo/001939643', photographer: 'PhilR' }] }
        : { photos: [] });
    const a = await app();
    const v1 = await a.request('/api/v1/adsb/photo/7c4925?reg=vh-oqf');
    expect(await v1.json()).toEqual({
      data: {
        photo: {
          src: 'https://airport-data.com/images/aircraft/thumbnails/001/939/001939643.jpg',
          link: 'https://airport-data.com/aircraft/photo/001939643',
          photographer: 'PhilR',
          source: 'airport-data',
        },
      },
    });
    expect(m.fetchJson.mock.calls.map((c) => c[0])).toEqual([
      'https://api.planespotters.net/pub/photos/hex/7c4925',
      'https://api.planespotters.net/pub/photos/reg/VH-OQF',
      'https://airport-data.com/api/ac_thumb.json?m=7C4925&n=1&r=VH-OQF',
    ]);
  });

  it('the backup is used when Planespotters is down, and "none" is not cached off a failed lookup', async () => {
    const { HttpError } = await import('../../../src/sources/shared/http.js');
    m.fetchJson.mockImplementation(async (url: string) => {
      if (url.includes('planespotters')) throw new HttpError('HTTP 503', 503, url);
      throw new HttpError('HTTP 404', 404, url); // airport-data knows none
    });
    const a = await app();
    expect(await (await a.request('/api/adsb/photo/7c0aa1')).text()).toBe('{"photo":null}');
    const before = m.fetchJson.mock.calls.length;
    // Asked again: nothing was cached, so both are tried again.
    await a.request('/api/adsb/photo/7c0aa1');
    expect(m.fetchJson.mock.calls.length).toBeGreaterThan(before);
  });

  it('a lookup that fails is no photo, on both paths', async () => {
    m.fetchJson.mockRejectedValue(new Error('HTTP 429'));
    const a = await app();
    const old = await a.request('/api/adsb/photo/7c0003');
    expect(old.status).toBe(200);
    expect(await old.text()).toBe('{"photo":null}');
    m.fetchJson.mockRejectedValue(new Error('HTTP 429'));
    const v1 = await a.request('/api/v1/adsb/photo/7c0004');
    expect(v1.status).toBe(200);
    expect(await v1.text()).toBe('{"data":{"photo":null}}');
  });

  it('a bad hex or reg: the old bodies, checked hex first; validation_failed on v1', async () => {
    const a = await app();
    const cases: Array<[string, string, string]> = [
      ['zz', '{"error":"invalid hex"}', 'hex'],
      ['7c0005?reg=!!', '{"error":"invalid reg"}', 'reg'],
      ['zz?reg=!!', '{"error":"invalid hex"}', 'hex'], // both bad: the hex, as always
      ['7c0005?reg=ABCDEFGHIJK', '{"error":"invalid reg"}', 'reg'],
    ];
    for (const [tail, oldBody, field] of cases) {
      const old = await a.request(`/api/adsb/photo/${tail}`);
      expect(old.status, tail).toBe(400);
      expect(await old.text(), tail).toBe(oldBody);
      const v1 = await a.request(`/api/v1/adsb/photo/${tail}`);
      expect(v1.status, tail).toBe(400);
      const body = (await v1.json()) as { error: { code: string; details: { issues: Array<{ path: string }> } } };
      expect(body.error.code, tail).toBe('validation_failed');
      expect(body.error.details.issues[0]!.path, tail).toBe(field);
    }
    expect(m.fetchJson).not.toHaveBeenCalled();
  });

  it('a TIS-B id goes straight to the registration', async () => {
    m.fetchJson.mockResolvedValue({ photos: [PHOTO] });
    const a = await app();
    const old = await a.request('/api/adsb/photo/~2a01f0?reg=VH-XYZ');
    expect(old.status).toBe(200);
    expect(m.fetchJson.mock.calls.map((c) => c[0])).toEqual(['https://api.planespotters.net/pub/photos/reg/VH-XYZ']);
  });
});

describe('what is not mirrored', () => {
  it('/api/v1/adsb/receivers is a v1 404, and the old path still answers as before', async () => {
    const a = await app();
    const v1 = await a.request('/api/v1/adsb/receivers');
    expect(v1.status).toBe(404);
    expect(await v1.json()).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });

    // No signed-in user in this app: the role gate's own refusal, not a 404.
    const old = await a.request('/api/adsb/receivers');
    expect(old.status).toBe(401);
    expect(await old.text()).toBe('{"error":"authentication required"}');
  });

  it('mirroring only the in-scope paths registers exactly those', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    await app();
    const adsb = mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/adsb/'));
    expect(adsb.map((r) => `${r.method} ${r.legacyPath}`).sort()).toEqual(IN_SCOPE.map((p) => `GET ${p}`).sort());
  });
});

describe('GET /api/adsb/info/:hex and /api/v1/adsb/info/:hex (adsbdb)', () => {
  const AIRCRAFT = {
    response: {
      aircraft: {
        type: 'A380 842', icao_type: 'A388', manufacturer: 'Airbus', mode_s: '7C4925',
        registration: 'VH-OQF', registered_owner_country_iso_name: 'AU',
        registered_owner_country_name: 'Australia', registered_owner_operator_flag_code: 'QFA',
        registered_owner: 'Qantas', url_photo: null, url_photo_thumbnail: null,
      },
    },
  };
  const ROUTE = {
    response: {
      flightroute: {
        callsign: 'QFA1', callsign_icao: 'QFA1', callsign_iata: 'QF1',
        airline: { name: 'Qantas', icao: 'QFA', iata: 'QF', country: 'Australia', country_iso: 'AU', callsign: 'QANTAS' },
        origin: { country_iso_name: 'AU', country_name: 'Australia', elevation: 21, iata_code: 'SYD', icao_code: 'YSSY', latitude: -33.9461, longitude: 151.177, municipality: 'Sydney', name: 'Sydney Kingsford Smith International Airport' },
        destination: { country_iso_name: 'SG', country_name: 'Singapore', elevation: 22, iata_code: 'SIN', icao_code: 'WSSS', latitude: 1.35, longitude: 103.99, municipality: 'Singapore', name: 'Singapore Changi Airport' },
      },
    },
  };
  beforeEach(async () => {
    const { _resetAdsbInfoForTests } = await import('../../../src/api/adsb.js');
    _resetAdsbInfoForTests();
  });

  it('the airframe and the usual route, compacted', async () => {
    m.fetchJson.mockImplementation(async (url: string) => (url.includes('/callsign/') ? ROUTE : AIRCRAFT));
    const a = await app();
    const res = await a.request('/api/adsb/info/7c4925?callsign=qfa1');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({
      aircraft: {
        manufacturer: 'Airbus', type: 'A380 842', icaoType: 'A388', registration: 'VH-OQF',
        owner: 'Qantas', ownerCountry: 'Australia',
      },
      route: {
        callsign: 'QFA1',
        airline: 'Qantas',
        origin: { icao: 'YSSY', iata: 'SYD', name: 'Sydney Kingsford Smith International Airport', municipality: 'Sydney', country: 'AU', lat: -33.9461, lon: 151.177 },
        destination: { icao: 'WSSS', iata: 'SIN', name: 'Singapore Changi Airport', municipality: 'Singapore', country: 'SG', lat: 1.35, lon: 103.99 },
        midpoint: null,
      },
    });
    expect(m.fetchJson.mock.calls.map((c) => c[0]).sort()).toEqual([
      'https://api.adsbdb.com/v0/aircraft/7C4925',
      'https://api.adsbdb.com/v0/callsign/QFA1',
    ]);
    // Cached: a second ask goes nowhere. On v1 the same answer, in data.
    const v1 = await a.request('/api/v1/adsb/info/7c4925?callsign=QFA1');
    expect(v1.status).toBe(200);
    expect(((await v1.json()) as { data: { route: { callsign: string } } }).data.route.callsign).toBe('QFA1');
    expect(m.fetchJson).toHaveBeenCalledTimes(2);
  });

  it('unknown to adsbdb (404) is null, and one half failing leaves the other', async () => {
    const { HttpError } = await import('../../../src/sources/shared/http.js');
    m.fetchJson.mockImplementation(async (url: string) => {
      if (url.includes('/callsign/')) throw new HttpError('HTTP 503', 503, url);
      throw new HttpError('HTTP 404', 404, url);
    });
    const a = await app();
    const res = await a.request('/api/adsb/info/7c0bb1?callsign=JST501');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ aircraft: null, route: null });
  });

  it('a TIS-B id skips the airframe lookup; no callsign skips the route', async () => {
    m.fetchJson.mockResolvedValue(ROUTE);
    const a = await app();
    expect(await (await a.request('/api/adsb/info/~7c0cc1?callsign=QFA1')).json()).toMatchObject({ aircraft: null });
    expect(m.fetchJson.mock.calls.map((c) => c[0])).toEqual(['https://api.adsbdb.com/v0/callsign/QFA1']);
    m.fetchJson.mockClear();
    m.fetchJson.mockResolvedValue(AIRCRAFT);
    expect(await (await a.request('/api/adsb/info/7c0cc2')).json()).toMatchObject({ route: null });
    expect(m.fetchJson.mock.calls.map((c) => c[0])).toEqual(['https://api.adsbdb.com/v0/aircraft/7C0CC2']);
  });

  it('bad input: the old bodies, validation_failed on v1', async () => {
    const a = await app();
    const cases: Array<[string, string, string]> = [
      ['zz', '{"error":"invalid hex"}', 'hex'],
      ['7c0dd1?callsign=QF%201!', '{"error":"invalid callsign"}', 'callsign'],
    ];
    for (const [tail, oldBody, field] of cases) {
      const old = await a.request(`/api/adsb/info/${tail}`);
      expect(old.status, tail).toBe(400);
      expect(await old.text(), tail).toBe(oldBody);
      const v1 = await a.request(`/api/v1/adsb/info/${tail}`);
      expect(v1.status, tail).toBe(400);
      const body = (await v1.json()) as { error: { code: string; details: { issues: Array<{ path: string }> } } };
      expect(body.error.code, tail).toBe('validation_failed');
      expect(body.error.details.issues[0]!.path, tail).toBe(field);
    }
    expect(m.fetchJson).not.toHaveBeenCalled();
  });
});
