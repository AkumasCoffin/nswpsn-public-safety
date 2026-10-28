/**
 * GET /api/summaries/latest and GET /api/summaries on /api/v1/ and on their
 * old /api/ paths.
 *
 * One handler chain serves both (api/v1/mirror.ts): the old paths answer byte
 * for byte as they did, v1 wraps the body in `data` and answers errors in the
 * shared shape. /api/summaries/trigger (both methods) is owner tooling and is
 * not mirrored.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const m = vi.hoisted(() => ({
  pool: 'ok' as 'none' | 'ok' | 'fail',
  rows: [] as unknown[],
  total: 0,
  calls: [] as Array<[string, unknown[] | undefined]>,
}));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => {
    if (m.pool === 'none') return null;
    return {
      query: async (sql: string, params?: unknown[]) => {
        m.calls.push([sql, params]);
        if (m.pool === 'fail') throw new Error('invalid input syntax for type date');
        if (sql.includes('COUNT(*)')) return { rows: [{ n: m.total }] };
        return { rows: m.rows };
      },
    };
  }),
  getWriterPool: vi.fn(async () => null),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

const JSON_TYPE = 'application/json';

const ROW = {
  id: 41,
  summary_type: 'hourly',
  period_start: new Date('2026-10-28T03:00:00Z'),
  period_end: new Date('2026-10-28T04:00:00Z'),
  day_date: new Date('2026-10-28T00:00:00Z'),
  hour_slot: 15,
  summary: 'A quiet hour: two grass fires near Galston, both contained.',
  call_count: 12,
  transcript_chars: 3456,
  model: 'gemini-2.5-flash',
  details: { period_label: '28/10/2026, 14:00 AEDT to 15:00 AEDT', tz: 'Australia/Sydney' },
  release_at: new Date('2026-10-28T04:00:00Z'),
  created_at: new Date('2026-10-28T03:55:12.345Z'),
};
const ADHOC = { ...ROW, id: 42, summary_type: 'adhoc', hour_slot: null, details: null, release_at: null, model: null };

/** rowToSummary, written out. */
const SUMMARY = {
  id: 41,
  type: 'hourly',
  period_start: '2026-10-28T03:00:00.000Z',
  period_end: '2026-10-28T04:00:00.000Z',
  day_date: '2026-10-28',
  hour_slot: 15,
  summary: 'A quiet hour: two grass fires near Galston, both contained.',
  call_count: 12,
  transcript_chars: 3456,
  model: 'gemini-2.5-flash',
  details: { period_label: '28/10/2026, 14:00 AEDT to 15:00 AEDT', tz: 'Australia/Sydney' },
  release_at: '2026-10-28T04:00:00.000Z',
  created_at: '2026-10-28T03:55:12.345Z',
};
const ADHOC_SUMMARY = { ...SUMMARY, id: 42, type: 'adhoc', hour_slot: null, model: null, details: {}, release_at: null };

async function app() {
  const { summariesRouter } = await import('../../../src/api/summaries.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', summariesRouter);
  mirror(a, summariesRouter, ['/api/summaries/latest', '/api/summaries']);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

beforeEach(() => {
  m.pool = 'ok';
  m.rows = [ROW];
  m.total = 1;
  m.calls = [];
});

describe('GET /api/summaries/latest and /api/v1/summaries/latest', () => {
  it('the latest released summary: bare on the old path, in data on v1', async () => {
    const a = await app();
    const old = await a.request('/api/summaries/latest');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify({ hourly: SUMMARY }));

    const v1 = await a.request('/api/v1/summaries/latest');
    expect(v1.status).toBe(200);
    expect([...v1.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await v1.text()).toBe(JSON.stringify({ data: { hourly: SUMMARY } }));
  });

  it('an ad-hoc one, and none at all', async () => {
    const a = await app();
    m.rows = [ADHOC];
    expect(await (await a.request('/api/v1/summaries/latest')).json()).toEqual({ data: { hourly: ADHOC_SUMMARY } });
    m.rows = [];
    expect(await (await a.request('/api/summaries/latest')).text()).toBe('{"hourly":null}');
    expect(await (await a.request('/api/v1/summaries/latest')).text()).toBe('{"data":{"hourly":null}}');
  });

  it('no database: 503 with the old body on the old path', async () => {
    m.pool = 'none';
    const a = await app();
    const old = await a.request('/api/summaries/latest');
    expect(old.status).toBe(503);
    expect(await old.text()).toBe('{"error":"database not configured"}');
    const v1 = await a.request('/api/v1/summaries/latest');
    expect(v1.status).toBe(503);
    expect(await v1.json()).toEqual({
      error: { code: 'service_unavailable', message: 'The radio summaries are not available on this server.' },
    });
  });

  it('a failing query: 500 with the old body on the old path', async () => {
    m.pool = 'fail';
    const a = await app();
    const old = await a.request('/api/summaries/latest');
    expect(old.status).toBe(500);
    expect(await old.text()).toBe('{"error":"failed to fetch summaries"}');
    const v1 = await a.request('/api/v1/summaries/latest');
    expect(v1.status).toBe(500);
    expect(await v1.json()).toEqual({ error: { code: 'internal_error', message: 'The radio summaries could not be read.' } });
  });
});

describe('GET /api/summaries and /api/v1/summaries', () => {
  it('a page: bare on the old path, in data on v1', async () => {
    m.rows = [ROW, ADHOC];
    m.total = 57;
    const a = await app();
    const body = { total: 57, limit: 50, offset: 0, results: [SUMMARY, ADHOC_SUMMARY] };
    const old = await a.request('/api/summaries');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify(body));
    const v1 = await a.request('/api/v1/summaries');
    expect(v1.status).toBe(200);
    expect(await v1.text()).toBe(JSON.stringify({ data: body }));
  });

  it('filters reach the query alike on both paths; hour alone means hourly', async () => {
    const a = await app();
    for (const p of ['/api/summaries', '/api/v1/summaries']) {
      m.calls = [];
      await a.request(`${p}?hour=3&date_from=2026-10-01&date_to=2026-10-31&limit=5&offset=10`);
      const data = m.calls.find(([sql]) => !sql.includes('COUNT(*)'))!;
      expect(data[1], p).toEqual([3, 'hourly', '2026-10-01', '2026-10-31', 5, 10]);
    }
  });

  it('lenient paging stays lenient: junk is the default, never a 400', async () => {
    const a = await app();
    const cases: Array<[string, number, number]> = [
      ['limit=0', 50, 0],
      ['limit=abc&offset=abc', 50, 0],
      ['limit=-5&offset=-1', 1, 0],
      ['limit=9999', 500, 0],
      ['limit=10&limit=20&offset=4', 10, 4], // a repeat: the first value
      ['type=', 50, 0], // empty type: no filter
    ];
    for (const [qs, limit, offset] of cases) {
      const old = (await (await a.request(`/api/summaries?${qs}`)).json()) as { limit: number; offset: number };
      expect([old.limit, old.offset], qs).toEqual([limit, offset]);
      const v1 = (await (await a.request(`/api/v1/summaries?${qs}`)).json()) as { data: { limit: number; offset: number } };
      expect([v1.data.limit, v1.data.offset], qs).toEqual([limit, offset]);
    }
  });

  it('a bad hour or type: the old 400 bodies, hour checked first; validation_failed on v1', async () => {
    const a = await app();
    const hourBody = '{"error":"hour must be 1..24"}';
    const typeBody = `{"error":"type must be 'hourly' or 'adhoc'"}`;
    const cases: Array<[string, string, string]> = [
      ['hour=0', hourBody, 'hour'],
      ['hour=25', hourBody, 'hour'],
      ['hour=', hourBody, 'hour'],
      ['hour=abc', hourBody, 'hour'],
      ['type=bad', typeBody, 'type'],
      ['hour=5&type=bad', typeBody, 'type'],
      ['hour=0&type=bad', hourBody, 'hour'], // both bad: the hour, as always
    ];
    for (const [qs, oldBody, field] of cases) {
      const old = await a.request(`/api/summaries?${qs}`);
      expect(old.status, qs).toBe(400);
      expect([...old.headers], qs).toEqual([['content-type', JSON_TYPE]]);
      expect(await old.text(), qs).toBe(oldBody);
      const v1 = await a.request(`/api/v1/summaries?${qs}`);
      expect(v1.status, qs).toBe(400);
      const body = (await v1.json()) as { error: { code: string; details: { issues: Array<{ path: string }> } } };
      expect(body.error.code, qs).toBe('validation_failed');
      expect(body.error.details.issues[0]!.path, qs).toBe(field);
    }
    expect(m.calls).toEqual([]);
  });

  it('no database: 503 with the old body on the old path', async () => {
    m.pool = 'none';
    const a = await app();
    const old = await a.request('/api/summaries');
    expect(old.status).toBe(503);
    expect(await old.text()).toBe('{"error":"database not configured"}');
    const v1 = await a.request('/api/v1/summaries');
    expect(v1.status).toBe(503);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('service_unavailable');
  });

  it('a failing query (a date that is not one, say): 500 with the old body on the old path', async () => {
    m.pool = 'fail';
    const a = await app();
    const old = await a.request('/api/summaries?date=garbage');
    expect(old.status).toBe(500);
    expect(await old.text()).toBe('{"error":"failed to fetch summaries"}');
    const v1 = await a.request('/api/v1/summaries?date=garbage');
    expect(v1.status).toBe(500);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('internal_error');
  });
});

describe('what is not mirrored', () => {
  it('/api/v1/summaries/trigger is a v1 404 by either method; the old paths answer as before', async () => {
    const a = await app();
    for (const method of ['GET', 'POST']) {
      const v1 = await a.request('/api/v1/summaries/trigger', { method });
      expect(v1.status, method).toBe(404);
      expect(await v1.json(), method).toEqual({ error: { code: 'not_found', message: 'No endpoint at this path.' } });
    }
    const get = await a.request('/api/summaries/trigger');
    expect(get.status).toBe(405);
    expect(await get.text()).toBe('{"error":"method not allowed; POST to /api/summaries/trigger to generate"}');
    const post = await a.request('/api/summaries/trigger', { method: 'POST', body: '{}' });
    expect(post.status).toBe(401);
    expect(await post.text()).toBe('{"error":"authentication required"}');
  });

  it('mirroring registers exactly the two reads', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    await app();
    expect(
      mirroredRoutes()
        .filter((r) => r.path.startsWith('/api/v1/summaries'))
        .map((r) => `${r.method} ${r.legacyPath}`)
        .sort(),
    ).toEqual(['GET /api/summaries', 'GET /api/summaries/latest']);
  });
});
