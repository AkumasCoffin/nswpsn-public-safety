/**
 * Ausgrid outages on their /api/ paths — this site's and the bot's, not on
 * /api/v1/ (an API key reads /api/v1/outages).
 *
 * Both read the poller's snapshot, so an empty one is just empty — as it is
 * before the first poll, or with the poller switched off (AUSGRID_DISABLED).
 * Checked byte for byte.
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
    const marker = normaliseAusgridOutage({
      EventId: 141462,
      OutageDisplayType: 'P',
      PowerStatus: 'RestorationHasCommenced',
      Cause: 'working on overhead powerlines in your neighbourhood to prevent future outages and improve service',
      Reason: null,
      Area: ['Lovedale'],
      WhenUpdated: '2026-11-13T01:10:16Z',
      StartDateTime: '2026-11-12T21:00:00Z',
      EstRestTime: '2026-11-13T12:30:00Z',
      MarkerLocation: { lat: -32.72156, lng: 151.34336 },
      StreetsAffected: ['Talga Rd'],
      AffectedCustomers: 1,
    });
    const snap = { Markers: [marker], Polygons: [] };
    liveStore.set('ausgrid', snap);
    const a = await app();
    const res = await get(a, '/api/ausgrid/outages');
    expect(res).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(snap) });
    expect(JSON.parse(res.text).Markers[0]).toMatchObject({
      OutageId: 141462,
      Streets: 'Talga Rd',
      streets: 'Talga Rd',
      StatusText: 'Restoration has commenced',
      WhenUpdated: '2026-11-13T01:10:16Z',
      EstRestoration: '2026-11-13T12:30:00Z',
    });
  });

  it('leaves out planned work yet to start: the bot announces every record it finds here', async () => {
    const event = (id: number, type: string, start: string) =>
      normaliseAusgridOutage({ EventId: id, OutageDisplayType: type, PowerStatus: 'InProgress', Area: ['Lovedale'], StartDateTime: start, AffectedCustomers: 1 });
    const past = new Date(Date.now() - 3_600_000).toISOString();
    const ahead = new Date(Date.now() + 86_400_000).toISOString();
    liveStore.set('ausgrid', { Markers: [event(1, 'U', past), event(2, 'P', past), event(3, 'P', ahead)], Polygons: [] });
    const res = await get(await app(), '/api/ausgrid/outages');
    expect(JSON.parse(res.text).Markers.map((m: { OutageId: number }) => m.OutageId)).toEqual([1, 2]);
  });

  it('empty lists when there is no snapshot', async () => {
    const a = await app();
    expect((await get(a, '/api/ausgrid/outages')).text).toBe('{"Markers":[],"Polygons":[]}');
  });
});

describe('/ausgrid/stats', () => {
  it("passes Ausgrid's counters on", async () => {
    const stats = { OutagePlannedTodayCount: 3, OutagePlannedTodayCustomers: 266, OutageUnplannedPdoCount: 11, OutageUnplannedPdoCustomers: 6118 };
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
