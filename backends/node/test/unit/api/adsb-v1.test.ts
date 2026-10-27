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

vi.mock('../../../src/sources/adsb.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/sources/adsb.js')>()),
  adsbSnapshot: () => m.snapshot,
  adsbTrailsSnapshot: () => m.trails,
}));

const IN_SCOPE = ['/api/adsb/aircraft', '/api/adsb/history', '/api/adsb/trails', '/api/adsb/photo/:hex'];

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
});
afterEach(() => {
  vi.useRealTimers();
});

describe('GET /api/adsb/aircraft and /api/v1/adsb/aircraft', () => {
  it('the snapshot plus the history window — bare on the old path, in data on v1', async () => {
    const a = await app();
    const body = { ...SNAPSHOT, history: { retentionDays: DATA_RETENTION_DAYS, oldestMs: OLDEST, newestMs: NOW } };

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

describe('GET /api/adsb/history and /api/v1/adsb/history', () => {
  const H = NOW - 3_600_000; // the hour before NOW
  const row = (offsetSec: number) => ({
    hex: '7c6b2d', hour_bucket: new Date(H), callsign: 'POL30', reg: 'VH-PVA', type: 'B429',
    es_tag: 'polair', sources: ['adsb_lol'], points: [[offsetSec, -33.86, 151.21, 1500]],
  });

  /** What the route answers for this instant: adsbHistoryAt's result, as it always was. */
  async function served(at: number, trail: number) {
    const { adsbHistoryAt } = await import('../../../src/services/adsbHistory.js');
    return JSON.stringify(await adsbHistoryAt(at, trail, NOW));
  }

  it('a settled minute: the history and the long cache, on both paths', async () => {
    m.query.mockResolvedValue({ rows: [row(3_000)] }); // 03:50
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

  it('the minute still filling gets the short cache', async () => {
    const a = await app();
    for (const p of ['/api/adsb/history', '/api/v1/adsb/history']) {
      const res = await a.request(`${p}?at=${NOW - 30_000}`);
      expect(res.status, p).toBe(200);
      expect(res.headers.get('Cache-Control'), p).toBe('public, max-age=30, stale-while-revalidate=120');
    }
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
  const SERVED = { src: PHOTO.thumbnail_large.src, link: PHOTO.link, photographer: PHOTO.photographer };

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
