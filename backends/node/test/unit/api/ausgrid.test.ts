/**
 * Ausgrid outages on their /api/ paths — this site's and the bot's, not on
 * /api/v1/ (an API key reads /api/v1/outages).
 *
 * Both read the poller's snapshot, so an empty one is just empty — the poller
 * is off by default (AUSGRID_DISABLED). Checked byte for byte.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';
import { normaliseAusgridOutage } from '../../../src/sources/ausgrid.js';

async function app() {
  const { ausgridRouter } = await import('../../../src/api/ausgrid.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', ausgridRouter);
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
  it('answers the snapshot as it is', async () => {
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
  });

  it('empty lists when there is no snapshot', async () => {
    const a = await app();
    expect((await get(a, '/api/ausgrid/outages')).text).toBe('{"Markers":[],"Polygons":[]}');
  });
});

describe('/ausgrid/stats', () => {
  it("passes Ausgrid's counters on", async () => {
    const stats = { TotalUnplanned: 3, TotalPlanned: 1, CustomersAffected: 120 };
    liveStore.set('ausgrid_stats', stats);
    const a = await app();
    expect(await get(a, '/api/ausgrid/stats')).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(stats) });
  });

  it('null when there is no snapshot', async () => {
    const a = await app();
    expect(await get(a, '/api/ausgrid/stats')).toEqual({ status: 200, type: 'application/json', text: 'null' });
  });
});

it('an unknown path keeps the plain-text 404', async () => {
  const a = await app();
  expect(await get(a, '/api/ausgrid/nope')).toEqual({ status: 404, type: 'text/plain; charset=UTF-8', text: '404 Not Found' });
});
