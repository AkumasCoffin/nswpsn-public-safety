/**
 * Ausgrid outages on /api/v1/ and on their old /api/ paths.
 *
 * One handler serves both (api/v1/mirror.ts): wrapped in `data` on v1, bare on
 * the old paths, checked byte for byte. Both read the poller's snapshot, so an
 * empty one is just empty — the poller is off by default (AUSGRID_DISABLED).
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';
import { normaliseAusgridOutage } from '../../../src/sources/ausgrid.js';

async function app() {
  const { ausgridRouter } = await import('../../../src/api/ausgrid.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', ausgridRouter);
  mirror(a, ausgridRouter, ['/api/ausgrid/outages', '/api/ausgrid/stats']);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function get(a: Hono, path: string) {
  const res = await a.request(path);
  return { status: res.status, type: res.headers.get('content-type'), text: await res.text() };
}

beforeEach(() => {
  liveStore.delete('ausgrid');
  liveStore.delete('ausgrid_stats');
});

describe('/ausgrid/outages', () => {
  it('answers the snapshot — wrapped in data on v1, bare on the old path', async () => {
    const polygon = [[151.1, -33.8], [151.2, -33.8], [151.2, -33.9]];
    const marker = normaliseAusgridOutage({
      WebId: 123,
      Area: 'Ryde',
      MarkerLocation: { lat: -33.81, lng: 151.1 },
      OutageDisplayType: 'U',
      Customers: '57',
      Cause: 'Fallen tree',
      StartDateTime: '2026-10-28T09:00:00',
      EstRestTime: '2026-10-28T13:00:00',
      Status: 1,
      Polygons: [polygon],
    });
    const snap = { Markers: [marker], Polygons: [polygon] };
    liveStore.set('ausgrid', snap);
    const a = await app();
    expect(await get(a, '/api/ausgrid/outages')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(snap) });
    expect(await get(a, '/api/v1/ausgrid/outages')).toEqual({
      status: 200,
      type: 'application/json',
      text: JSON.stringify({ data: snap }),
    });
  });

  it('empty lists when there is no snapshot', async () => {
    const a = await app();
    expect((await get(a, '/api/ausgrid/outages')).text).toBe('{"Markers":[],"Polygons":[]}');
    expect((await get(a, '/api/v1/ausgrid/outages')).text).toBe('{"data":{"Markers":[],"Polygons":[]}}');
  });
});

describe('/ausgrid/stats', () => {
  it("passes Ausgrid's counters on — wrapped in data on v1", async () => {
    const stats = { TotalUnplanned: 3, TotalPlanned: 1, CustomersAffected: 120 };
    liveStore.set('ausgrid_stats', stats);
    const a = await app();
    expect(await get(a, '/api/ausgrid/stats')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(stats) });
    expect(await get(a, '/api/v1/ausgrid/stats')).toEqual({
      status: 200,
      type: 'application/json',
      text: JSON.stringify({ data: stats }),
    });
  });

  it('null when there is no snapshot', async () => {
    const a = await app();
    expect(await get(a, '/api/ausgrid/stats')).toEqual({ status: 200, type: 'application/json', text: 'null' });
    expect(await get(a, '/api/v1/ausgrid/stats')).toEqual({ status: 200, type: 'application/json', text: '{"data":null}' });
  });
});

describe('the mirror', () => {
  it('an unknown v1 path is a v1 404; the old one keeps its own', async () => {
    const a = await app();
    const miss = await get(a, '/api/v1/ausgrid/nope');
    expect(miss.status).toBe(404);
    expect(JSON.parse(miss.text).error.code).toBe('not_found');
    expect(await get(a, '/api/ausgrid/nope')).toEqual({ status: 404, type: 'text/plain; charset=UTF-8', text: '404 Not Found' });
  });
});
