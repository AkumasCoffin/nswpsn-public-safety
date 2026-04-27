/**
 * Stats route tests.
 *
 * Exercises the routes against the real LiveStore + activityMode
 * singletons. We seed LiveStore with hand-built snapshots so the
 * roll-up logic has something to count.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { statsRouter } from '../../../src/api/stats.js';
import { liveStore } from '../../../src/store/live.js';
import { _resetForTests } from '../../../src/services/activityMode.js';

function makeApp() {
  const app = new Hono();
  app.route('/', statsRouter);
  return app;
}

function clearLiveStore() {
  for (const k of liveStore.keys()) liveStore.delete(k);
}

describe('stats router', () => {
  beforeEach(() => {
    clearLiveStore();
    _resetForTests();
  });

  it('GET /api/stats/history returns empty array on empty store', async () => {
    const app = makeApp();
    const res = await app.request('/api/stats/history');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual([]);
  });

  it('GET /api/stats/summary rolls up power buckets from LiveStore', async () => {
    liveStore.set('endeavour_current', [
      { customersAffected: 10, outageType: 'Unplanned' },
      { customersAffected: 5, outageType: 'Unplanned' },
    ]);
    liveStore.set('endeavour_maintenance', [
      { customersAffected: 3, outageType: 'Current Maintenance' },
    ]);
    liveStore.set('endeavour_planned', [
      { customersAffected: 1, outageType: 'Future Maintenance' },
    ]);
    liveStore.set('ausgrid', {
      Markers: [
        { customersAffected: 2, outageType: 'Unplanned' },
        { customersAffected: 4, outageType: 'Planned' },
      ],
      Polygons: [],
    });
    liveStore.set('essential_current', [
      { customersAffected: 7, outageType: 'unplanned', feedType: 'current' },
      { customersAffected: 2, outageType: 'planned', feedType: 'current' },
    ]);
    liveStore.set('essential_future', [
      { customersAffected: 1, outageType: 'planned', feedType: 'future' },
    ]);

    const app = makeApp();
    const res = await app.request('/api/stats/summary');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;

    const power = body['power'] as Record<string, Record<string, number>>;
    expect(power['endeavour']?.['current']).toBe(2);
    expect(power['endeavour']?.['current_maintenance']).toBe(1);
    expect(power['endeavour']?.['future']).toBe(1);
    // 10+5+3+1.
    expect(power['endeavour']?.['customers_affected']).toBe(19);

    expect(power['ausgrid']?.['unplanned']).toBe(1);
    expect(power['ausgrid']?.['planned']).toBe(1);
    expect(power['ausgrid']?.['total']).toBe(2);
    expect(power['ausgrid']?.['customers_affected']).toBe(6);

    expect(power['essential']?.['unplanned']).toBe(1);
    expect(power['essential']?.['planned']).toBe(2);
    expect(power['essential']?.['future']).toBe(1);
    expect(power['essential']?.['total']).toBe(3);
    expect(power['essential']?.['customers_affected']).toBe(10);

    // Skeleton sections still present even with no data.
    expect(body['traffic']).toBeDefined();
    expect(body['emergency']).toBeDefined();
    expect(body['environment']).toBeDefined();
    expect(typeof body['timestamp']).toBe('string');
  });

  it('GET /api/cache/status returns entries pulled from LiveStore', async () => {
    liveStore.set('foo', { hello: 'world' });
    liveStore.set('bar', [1, 2, 3]);

    const app = makeApp();
    const res = await app.request('/api/cache/status');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body['total_endpoints']).toBe(2);
    const entries = body['entries'] as Array<Record<string, unknown>>;
    expect(entries.map((e) => e['key']).sort()).toEqual(['bar', 'foo']);
    expect(body['archive']).toBeDefined();
    expect(body['poller']).toBeDefined();
  });

  it('/api/cache/stats is an alias of /api/cache/status', async () => {
    const app = makeApp();
    const a = await app.request('/api/cache/status');
    const b = await app.request('/api/cache/stats');
    const aBody = (await a.json()) as Record<string, unknown>;
    const bBody = (await b.json()) as Record<string, unknown>;
    expect(Object.keys(aBody).sort()).toEqual(Object.keys(bBody).sort());
  });

  it('GET /api/collection/status reflects active sessions', async () => {
    const app = makeApp();
    // Cleanly idle to start.
    let res = await app.request('/api/collection/status');
    let body = (await res.json()) as Record<string, unknown>;
    expect(body['mode']).toBe('idle');
    expect(body['total_viewers']).toBe(0);

    // Manually drive the activity-mode tracker via a heartbeat so we
    // don't need to import the route here.
    const { recordHeartbeat } = await import(
      '../../../src/services/activityMode.js'
    );
    recordHeartbeat('tab-1', 'open', { isDataPage: true, pageType: 'live' });

    res = await app.request('/api/collection/status');
    body = (await res.json()) as Record<string, unknown>;
    expect(body['mode']).toBe('active');
    expect(body['total_viewers']).toBe(1);
    expect(body['data_viewers']).toBe(1);
    const sessions = body['sessions'] as Array<Record<string, unknown>>;
    expect(sessions).toHaveLength(1);
    expect(sessions[0]?.['is_data_page']).toBe(true);
  });
});
