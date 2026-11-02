/**
 * Stored history on /api/v1/ — /api/v1/history, the v1 name of
 * /api/data/history — and what a named key reads of it.
 *
 *  - /api/v1/ wraps the page in `data`, writes every time in UTC, and answers
 *    errors in the v1 shape; the old path keeps its bodies and naive Sydney
 *    times (data-history.test.ts pins those).
 *  - feed= names feeds of the key table instead of archived sources.
 *  - /api/v1/history, the old v1 name, still answers for this site's
 *    pages until they move, and is not described.
 *  - A named key reads only the archived sources of the feeds whose history
 *    cell it holds (`fires:history` → rfs, qld_fire, …), on both paths, and
 *    its answers are never cached in between.
 *  - /sources and /stats are not on /api/v1/.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const state = vi.hoisted(() => ({ pool: true, fail: false }));
const queryMock = vi.fn();
const connectMock = vi.fn(() => Promise.resolve({ query: queryMock, release: vi.fn() }));
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve(state.pool ? { connect: connectMock, query: queryMock } : null)),
  closePool: vi.fn(),
}));
vi.mock('../../../src/store/filterCache.js', () => ({
  getFilterFacets: vi.fn(async () => ({
    providers: [],
    date_range: { oldest: '2026-10-01T10:00:00', newest: '2026-10-28T14:00:00', oldest_unix: 1790812800, newest_unix: 1793156400 },
  })),
}));

import { _resetDataHistoryAggregateCache, dataHistoryRouter } from '../../../src/api/data-history.js';
import { mirror } from '../../../src/api/v1/mirror.js';
import { notFound, onError } from '../../../src/api/v1/respond.js';
import { FEEDS, HISTORY_SOURCES } from '../../../src/services/auth/apiKeyScopes.js';

const FETCHED = 1_793_000_000; // 2026-10-26T07:33:20Z, 18:33:20 in Sydney

function row(id: number, source = 'rfs') {
  return {
    id: String(id),
    source,
    source_id: `inc-${id}`,
    fetched_at_epoch: String(FETCHED),
    fetched_at: new Date(FETCHED * 1000),
    last_seen_at_epoch: String(FETCHED + 60),
    lat: -33.8,
    lng: 151.2,
    category: 'Bush Fire',
    subcategory: null,
    state: 'NSW',
    data: { title: `Fire ${id}` },
  };
}

/** The SQL parameters of every data query (not counts) since the last reset. */
const dataQueryParams = () =>
  queryMock.mock.calls
    .filter(([sql]) => !/statement_timeout|BEGIN|COMMIT|ROLLBACK|COUNT\(/i.test(String(sql)))
    .map(([, params]) => params as unknown[]);

/** The archived source names among a query's parameters (sources go in as `source IN ($1, $2…)`). */
const SOURCE_NAMES = new Set([...Object.keys(HISTORY_SOURCES), 'user_incident']);
const askedSources = (params: unknown[]) =>
  [...new Set(params.filter((v): v is string => typeof v === 'string' && SOURCE_NAMES.has(v)))].sort();

beforeEach(() => {
  state.pool = true;
  state.fail = false;
  _resetDataHistoryAggregateCache();
  queryMock.mockReset();
  queryMock.mockImplementation(async (sql: string) => {
    if (/statement_timeout|BEGIN|COMMIT|ROLLBACK/.test(sql)) return { rows: [] };
    if (state.fail) throw new Error('boom');
    if (/COUNT\(/i.test(sql)) return { rows: [{ total: '1' }] };
    return { rows: [row(7)] };
  });
});

/** The router with its v1 routes, called as a named key with these scopes (or as the site, with none). */
function app(scopes?: string[]) {
  const a = new Hono();
  if (scopes) {
    a.use('*', async (c, next) => {
      c.set('authClass', 'apikey');
      c.set('apiKeyScopes', scopes);
      await next();
    });
  }
  a.route('/', dataHistoryRouter);
  mirror(a, dataHistoryRouter, [
    '/api/data/history',
    '/api/data/history/filters',
    '/api/data/history/incident/:source/:source_id',
  ]);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

describe('GET /api/v1/history', () => {
  it('the page in data, every time in UTC', async () => {
    const res = await app().request('/api/v1/history?source=rfs&date_from=2026-10-01');
    expect(res.status).toBe(200);
    const { data } = await res.json();
    expect(data.records[0]).toMatchObject({
      id: 7,
      source: 'rfs',
      fetched_at: FETCHED,
      fetched_at_iso: '2026-10-26T07:33:20.000Z',
      last_seen: FETCHED + 60,
      last_seen_iso: '2026-10-26T07:34:20.000Z',
    });
    // date_from is Sydney midnight: 2026-10-01T00:00 AEST is 2026-09-30T14:00Z.
    expect(data.query.filters_applied).toEqual({ source: ['rfs'], since: '2026-09-30T14:00:00.000Z', unique: true });
    expect(data).toMatchObject({ total: 1, count: 1, limit: 100, offset: 0, next_cursor: null });
  });

  it('the old path keeps naive Sydney times', async () => {
    const body = await (await app().request('/api/data/history?source=rfs&date_from=2026-10-01')).json();
    expect(body.records[0].fetched_at_iso).toBe('2026-10-26T18:33:20');
    expect(body.query.filters_applied.since).toBe('2026-10-01T00:00:00');
  });

  it('a malformed cursor or an offset past 10,000 is validation_failed', async () => {
    const a = app();
    const cursor = await (await a.request('/api/v1/history?cursor=@@')).json();
    expect(cursor.error).toEqual({ code: 'validation_failed', message: 'cursor is malformed; drop it and start over' });
    const offset = await a.request('/api/v1/history?offset=20000');
    expect(offset.status).toBe(400);
    const body = await offset.json();
    expect(body.error.code).toBe('validation_failed');
    expect(body.error.details).toEqual({ max_offset: 10_000 });
    // The old path keeps the body logs.html reads.
    expect((await (await a.request('/api/data/history?offset=20000')).json()).error).toBe('offset_too_large');
  });

  it('503 when the archive cannot be read, where the old path answers an empty page', async () => {
    state.pool = false;
    const a = app();
    const v1 = await a.request('/api/v1/history');
    expect(v1.status).toBe(503);
    expect((await v1.json()).error.code).toBe('service_unavailable');
    const old = await a.request('/api/data/history');
    expect(old.status).toBe(200);
    expect((await old.json()).records).toEqual([]);
  });

  it('503 with the tables that did not answer, rather than a partial page', async () => {
    state.fail = true;
    const res = await app().request('/api/v1/history?source=rfs');
    expect(res.status).toBe(503);
    const body = await res.json();
    expect(body.error.code).toBe('service_unavailable');
    expect(body.error.details).toEqual({ failed_tables: ['archive_rfs'] });
  });
});

describe('a named key reads only the history of the feeds it holds', () => {
  const FIRES_HISTORY = ['fires:history'];
  /** Every cell of the staff table. */
  const EVERY_CELL = Object.entries(FEEDS).flatMap(([feed, f]) => f.windows.map((w) => `${feed}:${w}`));
  const FIRE_SOURCES = ['nt_fire', 'qld_fire', 'qld_warning', 'rfs', 'sa_cfs', 'sa_mfs', 'vic_emergency', 'wa_incident', 'wa_warning'];

  it('with no source given, only its own feeds\' sources are asked for, and the answer is not cached', async () => {
    const res = await app(['fires:history', 'pager:history']).request('/api/v1/history');
    expect(res.status).toBe(200);
    expect(res.headers.get('Cache-Control')).toBe('private, no-store');
    const params = dataQueryParams();
    expect(params.length).toBe(2); // archive_rfs and archive_misc, nothing else
    for (const p of params) expect(askedSources(p)).toEqual([...FIRE_SOURCES, 'pager'].sort());
  });

  it('a feed it does not hold is not read: fire incidents and warnings, but not weather warnings', async () => {
    const a = app(FIRES_HISTORY);
    const res = await a.request('/api/v1/history?source=bom_land');
    expect(res.status).toBe(403);
    expect((await res.json()).error).toEqual({
      code: 'api_key_scope_forbidden',
      message: "This key's scopes do not cover the stored history of: bom_land.",
    });
    // The QFD and DFES warnings are part of the fires feed.
    expect((await a.request('/api/v1/history?source=qld_fire,rfs,qld_warning,wa_warning')).status).toBe(200);
  });

  it('the live cell of a feed reads none of its history', async () => {
    const res = await app(['fires:live', 'warnings:live']).request('/api/v1/history?source=rfs');
    expect(res.status).toBe(403);
    expect((await res.json()).error.message).toBe("This key's scopes do not cover the stored history of: rfs.");
  });

  it('refused on the old path too, with a plain body', async () => {
    const res = await app(FIRES_HISTORY).request('/api/data/history?source=pager');
    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({
      error: 'api_key_scope_forbidden',
      message: "This key's scopes do not cover the stored history of: pager.",
    });
  });

  it('people\'s own reports and the radio summaries are never readable here, even with every cell', async () => {
    const a = app(EVERY_CELL);
    for (const source of ['user_incident', 'radio_summary']) {
      const res = await a.request(`/api/v1/history?source=${source}`);
      expect(res.status, source).toBe(403);
    }
    // And with no source given, every cell asks for every archived source but those.
    queryMock.mockClear();
    expect((await a.request('/api/v1/history')).status).toBe(200);
    const params = dataQueryParams();
    expect(params.length).toBe(4); // every archive table
    for (const p of params) {
      expect(askedSources(p)).toEqual(Object.keys(HISTORY_SOURCES).sort());
      expect(p).not.toContain('radio_summary');
    }
  });

  it('a key whose history cells have nothing archived is told so', async () => {
    // Aircraft history is its own route, and summaries are theirs.
    const res = await app(['aircraft:history', 'summaries:history']).request('/api/v1/history');
    expect(res.status).toBe(403);
    expect((await res.json()).error.message).toBe("This key's scopes cover no stored history.");
  });

  it('one incident: only from a feed it holds the history of', async () => {
    const a = app(FIRES_HISTORY);
    const ok = await a.request('/api/v1/history/incident/rfs/inc-7');
    expect(ok.status).toBe(200);
    expect(ok.headers.get('Cache-Control')).toBe('private, no-store');
    const { data } = await ok.json();
    expect(data).toMatchObject({ source: 'rfs', source_id: 'inc-7', snapshots: 1 });
    expect(data.history[0].fetched_at_iso).toBe('2026-10-26T07:33:20.000Z');
    expect((await a.request('/api/v1/history/incident/pager/x')).status).toBe(403);
  });

  it('the site\'s own pages are not narrowed, and keep the shared cache', async () => {
    const res = await app().request('/api/data/history');
    expect(res.status).toBe(200);
    expect(res.headers.get('Cache-Control')).toBeNull(); // server.ts adds the 30 s public cache
    const params = dataQueryParams();
    expect(params.length).toBe(4); // every archive table
    for (const p of params) expect(askedSources(p)).toEqual([]);
  });
});

describe('GET /api/v1/history/filters', () => {
  it('the filter values in data, the date range in UTC', async () => {
    const { data } = await (await app().request('/api/v1/history/filters')).json();
    expect(data.date_range).toEqual({
      oldest: '2026-10-01T00:00:00.000Z',
      newest: '2026-10-28T03:00:00.000Z',
      oldest_unix: 1790812800,
      newest_unix: 1793156400,
    });
    const old = await (await app().request('/api/data/history/filters')).json();
    expect(old.date_range.oldest).toBe('2026-10-01T10:00:00');
  });
});

describe('not on /api/v1/', () => {
  it('the cross-source counts, under either name', async () => {
    const a = app();
    for (const p of ['/api/v1/history/sources', '/api/v1/history/stats', '/api/v1/data/history/sources', '/api/v1/data/history/stats']) {
      const res = await a.request(p);
      expect(res.status, p).toBe(404);
      expect((await res.json()).error.code, p).toBe('not_found');
    }
  });
});

describe('the old v1 name, kept for this site\'s pages until they move', () => {
  it('/api/v1/data/history and its filters and incident routes answer as /api/v1/history does', async () => {
    const a = app();
    for (const [old, now] of [
      ['/api/v1/data/history?source=rfs', '/api/v1/history?source=rfs'],
      ['/api/v1/data/history/filters', '/api/v1/history/filters'],
      ['/api/v1/data/history/incident/rfs/inc-7', '/api/v1/history/incident/rfs/inc-7'],
    ] as const) {
      const was = await a.request(old);
      expect(was.status, old).toBe(200);
      expect(await was.json(), old).toEqual(await (await a.request(now)).json());
    }
  });

  it('a named key is judged the same there', async () => {
    const res = await app(['pager:history']).request('/api/v1/data/history?source=rfs');
    expect(res.status).toBe(403);
    expect((await res.json()).error.code).toBe('api_key_scope_forbidden');
  });
});

describe('?feed=: the feeds of the key table instead of archived sources', () => {
  const FIRE_SOURCES = ['nt_fire', 'qld_fire', 'qld_warning', 'rfs', 'sa_cfs', 'sa_mfs', 'vic_emergency', 'wa_incident', 'wa_warning'];

  it("asks for every archived source of the feeds named, in any case, and echoes the feed", async () => {
    const res = await app().request('/api/v1/history?feed=FIRES');
    expect(res.status).toBe(200);
    const params = dataQueryParams();
    expect(params.length).toBe(1); // the fire services live in archive_rfs
    expect(askedSources(params[0]!)).toEqual(FIRE_SOURCES);
    const { data } = await res.json();
    expect(data.query.filters_applied.feed).toEqual(['fires']);
    queryMock.mockClear();
    await app().request('/api/v1/history?feed=fires,pager');
    const both = dataQueryParams();
    expect(both.length).toBe(2); // archive_rfs and archive_misc
    for (const p of both) expect(askedSources(p)).toEqual([...FIRE_SOURCES, 'pager'].sort());
  });

  it('with source as well: the sources named, which must be in those feeds', async () => {
    const a = app();
    expect((await a.request('/api/v1/history?feed=fires&source=rfs,qld_fire')).status).toBe(200);
    expect(dataQueryParams().flatMap(askedSources).sort()).toEqual(['qld_fire', 'rfs']);
    const res = await a.request('/api/v1/history?feed=fires&source=rfs,pager');
    expect(res.status).toBe(400);
    expect((await res.json()).error).toEqual({ code: 'validation_failed', message: 'not a source of fires: pager' });
  });

  it('a feed with no stored history is refused, naming the ones that have', async () => {
    const res = await app().request('/api/v1/history?feed=aircraft');
    expect(res.status).toBe(400);
    expect((await res.json()).error).toEqual({
      code: 'validation_failed',
      message: 'unknown feed: aircraft (one of fires, warnings, ambulance, roads, outages, beaches, pager)',
    });
    // The old path: its own plain body.
    const old = await app().request('/api/data/history?feed=aircraft');
    expect(old.status).toBe(400);
    expect(await old.json()).toEqual({
      error: 'unknown_feed',
      message: 'unknown feed: aircraft (one of fires, warnings, ambulance, roads, outages, beaches, pager)',
    });
  });

  it("a named key is refused a feed whose history it does not hold, told which", async () => {
    const res = await app(['pager:history']).request('/api/v1/history?feed=fires,pager');
    expect(res.status).toBe(403);
    expect((await res.json()).error).toEqual({
      code: 'api_key_scope_forbidden',
      message: "This key's scopes do not cover the history of: fires.",
    });
    expect((await app(['pager:history', 'fires:history']).request('/api/v1/history?feed=fires,pager')).status).toBe(200);
  });
});
