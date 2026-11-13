/**
 * The per-state counts in /api/stats/summary and the archived snapshot, and
 * ?step= on /api/stats/history.
 *
 * by_state is counted from the merged lists the public feeds answer with, so
 * the LiveStore is seeded in each source's own shape, as the fires, traffic
 * and outages tests seed it. The history query is checked for its shape and
 * parameters against a stubbed pool: nothing here reaches a database.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Hono } from 'hono';

const m = vi.hoisted(() => ({ query: vi.fn(), pool: false }));

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve(m.pool ? { query: m.query } : null)),
  getWriterPool: vi.fn(() => Promise.resolve(m.pool ? { query: m.query } : null)),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

import { statsRouter, summariseByState, parseHistoryStep } from '../../../src/api/stats.js';
import { buildStatsSnapshot } from '../../../src/services/statsArchiver.js';
import { liveStore } from '../../../src/store/live.js';

const fc = (features: unknown[]) => ({ type: 'FeatureCollection', features, count: features.length });

/** A fire-service record: only what the merged feed reads to place and count it. */
const fire = (guid: string, props: Record<string, unknown> = {}) => ({
  type: 'Feature',
  geometry: { type: 'Point', coordinates: [150, -33] },
  properties: { guid, title: guid, is_active: true, ...props },
});

/** A road record of one state and type. */
const road = (id: string, state: string, type: string) => ({
  type: 'Feature',
  geometry: { type: 'Point', coordinates: [150, -33] },
  properties: { id, title: id, state, type },
});

function app() {
  const a = new Hono();
  a.route('/', statsRouter);
  return a;
}

beforeEach(() => {
  for (const k of liveStore.keys()) liveStore.delete(k);
  m.query.mockReset();
  m.pool = false;
});

describe('summariseByState', () => {
  it('is empty when nothing has been read', () => {
    expect(summariseByState()).toEqual({});
  });

  it('counts open fires by state, and warnings apart from them', () => {
    // RFS records carry no layer and are all fires.
    liveStore.set('rfs_incidents', fc([fire('rfs:1'), fire('rfs:2')]));
    liveStore.set('qld_fire', fc([fire('qld:1', { layer: 'fire' })]));
    liveStore.set('qld_warning', fc([fire('qld:w1', { layer: 'fire' }), fire('qld:w2', { layer: 'flood' })]));
    // A flood and a hazard the service attends are not fires.
    liveStore.set('vic_emergency', fc([fire('vic:1', { layer: 'fire' }), fire('vic:2', { layer: 'flood' }), fire('vic:3', { layer: 'hazard' })]));
    // A closed record is not counted.
    liveStore.set('nt_fire', fc([fire('nt:1', { layer: 'fire' }), fire('nt:2', { layer: 'fire', is_active: false })]));
    liveStore.set('wa_incident', fc([]));
    liveStore.set('sa_cfs', fc([fire('sa:1', { layer: 'fire' })]));
    liveStore.set('sa_mfs', fc([fire('sa:2', { layer: 'fire' })]));

    expect(summariseByState()).toEqual({
      NSW: { fires: 2 },
      QLD: { fires: 1, fire_warnings: 2 },
      VIC: { fires: 1 },
      SA: { fires: 2 },
      // Read and empty is 0; WA's warnings feed has not been read, so it is absent.
      WA: { fires: 0 },
      NT: { fires: 1 },
    });
  });

  it('counts each road feed by state, as /api/traffic/<feed> lists it', () => {
    liveStore.set('traffic_incidents', fc([road('n1', 'NSW', 'Incident'), road('n2', 'NSW', 'Incident')]));
    liveStore.set('traffic_roadwork', fc([road('n3', 'NSW', 'Roadwork')]));
    liveStore.set('traffic_flood', fc([]));
    liveStore.set('traffic_majorevent', fc([road('n4', 'NSW', 'Major Event')]));
    // Not one of the four feeds: counted nowhere.
    liveStore.set('traffic_alpine', fc([road('n5', 'NSW', 'Alpine')]));
    liveStore.set('roads_qld', fc([road('q1', 'QLD', 'Incident'), road('q2', 'QLD', 'Flood'), road('q3', 'QLD', 'Flood')]));
    liveStore.set('roads_vic', fc([road('v1', 'VIC', 'Roadwork'), road('v2', 'VIC', 'Major Event')]));
    liveStore.set('roads_sa', fc([road('s1', 'SA', 'Roadwork')]));
    liveStore.set('roads_act', fc([]));

    expect(summariseByState()).toEqual({
      NSW: { roads: 2, roadwork: 1, road_floods: 0, major_events: 1 },
      QLD: { roads: 1, roadwork: 0, road_floods: 2, major_events: 0 },
      VIC: { roads: 0, roadwork: 1, road_floods: 0, major_events: 1 },
      // SA's feed gives no floods or events, the ACT's no floods.
      SA: { roads: 0, roadwork: 1 },
      ACT: { roads: 0, roadwork: 0, major_events: 0 },
    });
  });

  it('gives an NSW record saved without its state to NSW', () => {
    liveStore.set('traffic_incidents', fc([{ type: 'Feature', geometry: null, properties: { id: 'old', title: 'old' } }]));
    expect(summariseByState()).toEqual({ NSW: { roads: 1 } });
  });

  it('counts outages off now and their customers, each id once', () => {
    liveStore.set('endeavour_current', [
      { id: 'INCD-1', customersAffected: 10, outageType: 'Unplanned' },
      { id: 'INCD-2', customersAffected: 5, outageType: 'Unplanned' },
    ]);
    // INCD-2 again: Endeavour lists some outages in both.
    liveStore.set('endeavour_maintenance', [
      { id: 'INCD-2', customersAffected: 5, outageType: 'Current Maintenance' },
      { id: 'INCD-3', customersAffected: 3, outageType: 'Current Maintenance' },
    ]);
    // Work still to come is not off now.
    liveStore.set('endeavour_planned', [{ id: 'INCD-4', customersAffected: 100, outageType: 'Future Maintenance' }]);
    liveStore.set('essential_current', [
      { incidentId: 'ES-1', customersAffected: 7, outageType: 'unplanned', status: 'active', feedType: 'current' },
    ]);
    liveStore.set('essential_future', [
      { incidentId: 'ES-2', customersAffected: 50, outageType: 'planned', status: 'scheduled', feedType: 'future' },
    ]);

    expect(summariseByState()).toEqual({ NSW: { outages: 4, customers: 25 } });
  });

  it('says 0 outages once a distributor has been read and has none', () => {
    liveStore.set('endeavour_current', []);
    expect(summariseByState()).toEqual({ NSW: { outages: 0, customers: 0 } });
  });

  it('lists states and metrics in a fixed order, whole numbers only', () => {
    liveStore.set('wa_incident', fc([fire('wa:1', { layer: 'fire' })]));
    liveStore.set('roads_act', fc([road('a1', 'ACT', 'Incident')]));
    liveStore.set('roads_qld', fc([]));
    liveStore.set('qld_fire', fc([]));
    liveStore.set('rfs_incidents', fc([fire('rfs:1')]));
    liveStore.set('traffic_incidents', fc([road('n1', 'NSW', 'Incident')]));
    liveStore.set('endeavour_current', [{ id: 'INCD-1', customersAffected: 2.6, outageType: 'Unplanned' }]);

    const by = summariseByState();
    expect(Object.keys(by)).toEqual(['NSW', 'QLD', 'ACT', 'WA']);
    expect(Object.keys(by['NSW']!)).toEqual(['fires', 'roads', 'outages', 'customers']);
    expect(Object.keys(by['QLD']!)).toEqual(['fires', 'roads', 'roadwork', 'road_floods', 'major_events']);
    for (const row of Object.values(by)) for (const n of Object.values(row)) expect(Number.isInteger(n)).toBe(true);
    expect(by['NSW']!.customers).toBe(3);
  });
});

describe('by_state where it is served and archived', () => {
  it('is in /api/stats/summary beside the fields that were always there', async () => {
    liveStore.set('rfs_incidents', fc([fire('rfs:1')]));
    const res = await app().request('/api/stats/summary');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(Object.keys(body)).toEqual(['timestamp', 'power', 'traffic', 'emergency', 'environment', 'by_state']);
    expect(body['by_state']).toEqual({ NSW: { fires: 1 } });
    expect((body['emergency'] as { rfs_incidents: number }).rfs_incidents).toBe(1);
  });

  it('is in the archived snapshot', () => {
    liveStore.set('roads_vic', fc([road('v1', 'VIC', 'Incident')]));
    const snap = buildStatsSnapshot();
    expect(Object.keys(snap)).toEqual(['power', 'traffic', 'emergency', 'environment', 'by_state']);
    expect(snap.by_state).toEqual({ VIC: { roads: 1, roadwork: 0, road_floods: 0, major_events: 0 } });
  });
});

describe('/api/stats/history ?step=', () => {
  it('reads step as whole minutes held to 5..1440, and nothing else as a step', () => {
    expect(parseHistoryStep(null)).toBeNull();
    expect(parseHistoryStep('')).toBeNull();
    expect(parseHistoryStep('abc')).toBeNull();
    expect(parseHistoryStep('1.5')).toBeNull();
    expect(parseHistoryStep('60')).toBe(60);
    expect(parseHistoryStep('5')).toBe(5);
    expect(parseHistoryStep('1')).toBe(5);
    expect(parseHistoryStep('0')).toBe(5);
    expect(parseHistoryStep('-30')).toBe(5);
    expect(parseHistoryStep('1440')).toBe(1440);
    expect(parseHistoryStep('99999')).toBe(1440);
  });

  it('without step asks for every row, as before', async () => {
    m.pool = true;
    m.query.mockResolvedValue({ rows: [{ timestamp: '1000', data: { a: 1 } }] });
    const res = await app().request('/api/stats/history?hours=24');
    expect(await res.json()).toEqual([{ timestamp: 1000, data: { a: 1 } }]);
    const [sql, params] = m.query.mock.calls[0] as [string, unknown[]];
    expect(sql).not.toContain('DISTINCT');
    expect(params).toHaveLength(2);
    expect(params[1]).toBe(2200);
  });

  it('with step keeps the newest row of each bucket, in one range scan', async () => {
    m.pool = true;
    m.query.mockResolvedValue({ rows: [{ timestamp: '1000', data: '{"a":1}' }, { timestamp: '2000', data: { a: 2 } }] });
    const before = Date.now();
    const res = await app().request('/api/stats/history?hours=999&step=60');
    expect(await res.json()).toEqual([
      { timestamp: 1000, data: { a: 1 } },
      { timestamp: 2000, data: { a: 2 } },
    ]);
    expect(m.query).toHaveBeenCalledTimes(1);
    const [sql, params] = m.query.mock.calls[0] as [string, number[]];
    const flat = sql.replace(/\s+/g, ' ');
    expect(flat).toContain('SELECT DISTINCT ON ("timestamp" / $3::bigint) "timestamp", data FROM stats_snapshots');
    expect(flat).toContain('WHERE "timestamp" >= $1');
    // The newest of a bucket first, so DISTINCT ON keeps it; newest buckets under the cap.
    expect(flat).toContain('ORDER BY "timestamp" / $3::bigint DESC, "timestamp" DESC LIMIT $2');
    expect(flat.endsWith('ORDER BY "timestamp" ASC')).toBe(true);
    // The window is still held to 168 hours, the rows to the cap, the step in ms.
    expect(params[0]).toBeGreaterThanOrEqual(before - 168 * 3_600_000);
    expect(params[0]).toBeLessThanOrEqual(Date.now() - 168 * 3_600_000);
    expect(params.slice(1)).toEqual([2200, 3_600_000]);
  });

  it('holds an out-of-range step to the range', async () => {
    m.pool = true;
    m.query.mockResolvedValue({ rows: [] });
    await app().request('/api/stats/history?step=1');
    await app().request('/api/stats/history?step=100000');
    expect((m.query.mock.calls[0] as [string, number[]])[1][2]).toBe(5 * 60_000);
    expect((m.query.mock.calls[1] as [string, number[]])[1][2]).toBe(1440 * 60_000);
  });
});
