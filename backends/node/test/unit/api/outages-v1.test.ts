/**
 * GET /api/v1/outages — every electricity distributor's outages in one feed
 * (api/outages.ts).
 *
 * Read from the same LiveStore snapshots the per-distributor routes serve,
 * each in its source's own shape. Pinned here: the shared snake_case shape and
 * what goes in `details`; kind and status from each distributor's own terms;
 * the stored-history source each is filed under; times in UTC — a zone as
 * written, none as Sydney time, and one that cannot be read kept in details;
 * the distributor/kind/status filters and their refusals; `sources` per
 * distributor; the bare FeatureCollection on both paths; and what a key needs.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';
import { createHash } from 'node:crypto';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  getWriterPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
  READ_POOL_MAX: 4,
}));

import { liveStore } from '../../../src/store/live.js';
import { outagesRouter, outageUtc } from '../../../src/api/outages.js';
import { mirror } from '../../../src/api/v1/mirror.js';
import { notFound, onError } from '../../../src/api/v1/respond.js';
import { requireApiKey } from '../../../src/services/auth/apiKey.js';
import { _clearApiKeyCache, lookupPrefix } from '../../../src/services/auth/apiKeys.js';
import { parseEssentialKml } from '../../../src/sources/essential.js';
import { normaliseAusgridOutage } from '../../../src/sources/ausgrid.js';

const NOW = Date.UTC(2026, 10, 2, 1, 0, 0); // 2026-11-02T01:00:00Z
const READ_AT = new Date(NOW).toISOString();
const STORES = ['endeavour_current', 'endeavour_maintenance', 'endeavour_planned', 'essential_current', 'essential_future', 'ausgrid'];

// --- Each distributor in its source's own shape ------------------------------

function endeavour(id: string, outageType: string, extra: Record<string, unknown> = {}) {
  return {
    id,
    suburb: 'Penrith',
    streets: 'High St',
    customersAffected: 12,
    status: 'Active',
    cause: 'Tree on lines',
    outageType,
    startTime: '2026-10-28T08:00:00+11:00',
    estimatedRestoration: '2026-10-28T12:30:00+00:00',
    lastUpdated: '2026-10-28T08:05:00.123456+00:00',
    latitude: -33.75,
    longitude: 150.69,
    postcode: '2750',
    hasGPS: true,
    ...extra,
  };
}
const EN_CURRENT = endeavour('MMJ 1', 'Unplanned');
const EN_MAINT = endeavour('SP 2', 'Current Maintenance', { status: 'Scheduled', cause: 'Maintenance Work', endTime: '2026-10-28T12:30:00+00:00', duration: '' });
// No suburb from Endeavour (the poller wrote "Unknown"), no position, no times.
const EN_FUTURE = endeavour('SP 3', 'Future Maintenance', {
  suburb: 'Unknown', streets: '', status: 'Scheduled', startTime: null, estimatedRestoration: null, lastUpdated: '',
  latitude: null, longitude: null, hasGPS: false, endTime: null, duration: '',
});

const KML = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://earth.google.com/kml/2.1">
  <Document>
    <Placemark id="INCD-1-r">
      <name>WAGGA WAGGA</name>
      <styleUrl>#unplanned</styleUrl>
      <description><![CDATA[
        <div><span>Time Off:</span>28/10/2026 14:30:00</div>
        <div><span>Est. Time On:</span>soon</div>
        <div><span>Customers affected:</span> 42</div>
        <div><span>Last Updated:</span>28/10/2026 14:35:00</div>
      ]]></description>
      <Point><coordinates>147.367,-35.108</coordinates></Point>
    </Placemark>
    <Placemark id="INCD-2-p">
      <name>DUBBO</name>
      <styleUrl>#planned</styleUrl>
      <description><![CDATA[
        <div><span>Time Off:</span>15/07/2027 08:00:00</div>
        <div><span>Est. Time On:</span>15/07/2027 15:00:00</div>
        <div><span>Customers affected:</span> 5</div>
        <div><span>Reason:</span>General network maintenance</div>
      ]]></description>
      <Point><coordinates>148.601,-32.243</coordinates></Point>
      <Polygon><outerBoundaryIs><LinearRing>
        <coordinates>148.6,-32.2 148.61,-32.2 148.61,-32.25 148.6,-32.2</coordinates>
      </LinearRing></outerBoundaryIs></Polygon>
    </Placemark>
  </Document>
</kml>`;
const [ES_UNPLANNED, ES_PLANNED_NOW] = parseEssentialKml(KML, 'current');
const ES_FUTURE = parseEssentialKml(KML, 'future').filter((o) => o.outageType === 'planned');

const AUSGRID = normaliseAusgridOutage({
  WebId: 123,
  Area: 'Ryde',
  MarkerLocation: { lat: -33.81, lng: 151.1 },
  OutageDisplayType: 'U',
  Customers: '57',
  Cause: 'Fallen tree',
  StartDateTime: '2026-10-28T09:00:00',
  EstRestTime: '2026-10-28T13:00:00',
  Status: 1,
  Polygons: [],
})!;

type Props = Record<string, any>;
type Answer = {
  type: string;
  features: Array<{ type: string; geometry: { type: string; coordinates: number[] } | null; properties: Props }>;
  count: number;
  sources: Array<{ distributor: string; name: string; count: number; updated_at: string | null }>;
};

const PROPERTY_NAMES = [
  'area', 'cause', 'customers_affected', 'details', 'distributor', 'id', 'kind', 'polygons', 'restore_at', 'source',
  'start_at', 'state', 'status', 'updated_at',
].sort();

function app(opts: { gate?: boolean } = {}) {
  const a = new Hono();
  if (opts.gate) a.use('*', requireApiKey);
  a.route('/', outagesRouter);
  mirror(a, outagesRouter, ['/api/outages']);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function read(path: string): Promise<Answer> {
  const res = await app().request(path);
  expect(res.status, path).toBe(200);
  return (await res.json()) as Answer;
}
const byId = (body: Answer, id: string) => body.features.find((f) => f.properties['id'] === id)!;
const ids = (body: Answer) => body.features.map((f) => f.properties['id']);

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
  liveStore.set('endeavour_current', [EN_CURRENT]);
  liveStore.set('endeavour_maintenance', [EN_MAINT]);
  liveStore.set('endeavour_planned', [EN_FUTURE]);
  liveStore.set('essential_current', [ES_UNPLANNED, ES_PLANNED_NOW]);
  liveStore.set('essential_future', ES_FUTURE);
  // Ausgrid is not being read (AUSGRID_DISABLED): its store is empty.
  liveStore.delete('ausgrid');
});
afterEach(() => {
  vi.useRealTimers();
  for (const s of STORES) liveStore.delete(s);
});

describe('GET /api/v1/outages', () => {
  it('one bare FeatureCollection, the same on both paths, every distributor in order', async () => {
    const v1 = await app().request('/api/v1/outages');
    expect(v1.status).toBe(200);
    expect(v1.headers.get('content-type')).toBe('application/json');
    const body = (await v1.json()) as Answer;
    expect(await (await app().request('/api/outages')).json()).toEqual(body);
    expect(Object.keys(body).sort()).toEqual(['count', 'features', 'sources', 'type']);
    expect(ids(body)).toEqual(['MMJ 1', 'SP 2', 'SP 3', 'INCD-1-r', 'INCD-2-p', 'INCD-2-p']);
    expect(body.count).toBe(6);
    for (const f of body.features) expect(Object.keys(f.properties).sort(), f.properties['id']).toEqual(PROPERTY_NAMES);
  });

  it('an Endeavour Energy outage: zoned times in UTC, its own fields in details', async () => {
    const body = await read('/api/v1/outages?distributor=endeavour&kind=unplanned');
    expect(body.features).toEqual([
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [150.69, -33.75] },
        properties: {
          id: 'MMJ 1',
          source: 'endeavour_current',
          distributor: 'endeavour',
          state: 'NSW',
          kind: 'unplanned',
          status: 'active',
          area: 'Penrith',
          customers_affected: 12,
          cause: 'Tree on lines',
          start_at: '2026-10-27T21:00:00.000Z',
          restore_at: '2026-10-28T12:30:00.000Z',
          updated_at: '2026-10-28T08:05:00.123Z',
          polygons: [],
          details: { streets: 'High St', status: 'Active', postcode: '2750' },
        },
      },
    ]);
  });

  it("Endeavour's planned work: under way is active, to come is scheduled; both filed under endeavour_planned", async () => {
    const body = await read('/api/v1/outages?distributor=endeavour');
    expect(byId(body, 'SP 2').properties).toMatchObject({ kind: 'planned', status: 'active', source: 'endeavour_planned' });
    // No suburb (the poller's "Unknown"), no position and no times: empty, null and null.
    const future = byId(body, 'SP 3');
    expect(future.geometry).toBeNull();
    expect(future.properties).toMatchObject({
      kind: 'planned', status: 'scheduled', source: 'endeavour_planned', area: '', start_at: null, restore_at: null, updated_at: null,
    });
    expect(future.properties['details']).toEqual({ streets: '', status: 'Scheduled', postcode: '2750', duration: '' });
  });

  it('an Essential Energy outage: its times read as Sydney time; one that cannot be read kept in details', async () => {
    const body = await read('/api/v1/outages?distributor=essential&kind=unplanned');
    expect(body.features).toEqual([
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [147.367, -35.108] },
        properties: {
          id: 'INCD-1-r',
          source: 'essential_current',
          distributor: 'essential',
          state: 'NSW',
          kind: 'unplanned',
          status: 'active',
          area: 'WAGGA WAGGA',
          customers_affected: 42,
          // No reason given: the poller's "Unknown" is empty here.
          cause: '',
          // 14:30 in Sydney in daylight saving (+11:00).
          start_at: '2026-10-28T03:30:00.000Z',
          restore_at: null,
          updated_at: '2026-10-28T03:35:00.000Z',
          polygons: [],
          // The restoration time as written, since it is not a time; the read ones are not repeated.
          details: { title: 'WAGGA WAGGA', estTimeOn: 'soon' },
        },
      },
    ]);
  });

  it("Essential's planned outages: now and to come, filed as the poller files them, with the area's outline", async () => {
    const body = await read('/api/v1/outages?distributor=essential&kind=planned');
    expect(body.features.map((f) => [f.properties['source'], f.properties['status']])).toEqual([
      ['essential_planned', 'active'],
      ['essential_future', 'scheduled'],
    ]);
    const [now] = body.features;
    expect(now!.properties).toMatchObject({
      cause: 'General network maintenance',
      // 08:00 in Sydney in winter (+10:00).
      start_at: '2027-07-14T22:00:00.000Z',
      restore_at: '2027-07-15T05:00:00.000Z',
      polygons: [[[148.6, -32.2], [148.61, -32.2], [148.61, -32.25], [148.6, -32.2]]],
    });
  });

  it('an Ausgrid outage: its PascalCase fields mapped, the camelCase copies left out', async () => {
    liveStore.set('ausgrid', { Markers: [AUSGRID], Polygons: [] });
    const body = await read('/api/v1/outages?distributor=ausgrid');
    expect(body.features).toEqual([
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [151.1, -33.81] },
        properties: {
          id: '123',
          source: 'ausgrid',
          distributor: 'ausgrid',
          state: 'NSW',
          kind: 'unplanned',
          status: 'active',
          area: 'Ryde',
          customers_affected: 57,
          cause: 'Fallen tree',
          start_at: '2026-10-27T22:00:00.000Z',
          restore_at: '2026-10-28T02:00:00.000Z',
          updated_at: null,
          polygons: [],
          details: {
            JobId: '', StreetName: '', Streets: '', Postcode: '', Reason: '', Detail: '', StatusText: '',
            Status: 1, Classification: null, Polygons: [],
          },
        },
      },
    ]);
  });

  it('filters by distributor, kind and status (any case), and the sources list follows', async () => {
    expect(ids(await read('/api/v1/outages?status=scheduled'))).toEqual(['SP 3', 'INCD-2-p']);
    expect(ids(await read('/api/v1/outages?kind=PLANNED&status=active'))).toEqual(['SP 2', 'INCD-2-p']);
    const one = await read('/api/v1/outages?distributor=Essential');
    expect(one.sources.map((s) => s.distributor)).toEqual(['essential']);
    expect(ids(await read('/api/v1/outages?distributor=endeavour,ausgrid&kind=unplanned'))).toEqual(['MMJ 1']);
  });

  it('`sources`: one entry per distributor, counted after the filters, null when not every feed has been read', async () => {
    const body = await read('/api/v1/outages?kind=planned');
    expect(body.sources).toEqual([
      { distributor: 'endeavour', name: 'Endeavour Energy', count: 2, updated_at: READ_AT },
      { distributor: 'essential', name: 'Essential Energy', count: 2, updated_at: READ_AT },
      { distributor: 'ausgrid', name: 'Ausgrid', count: 0, updated_at: null },
    ]);
    liveStore.delete('essential_future');
    const partial = await read('/api/v1/outages');
    expect(partial.sources.find((s) => s.distributor === 'essential')).toEqual({
      distributor: 'essential', name: 'Essential Energy', count: 2, updated_at: null,
    });
  });

  it('an unknown value is refused: 400 validation_failed, naming each bad field', async () => {
    const res = await app().request('/api/v1/outages?distributor=agl&kind=maybe&status=done');
    expect(res.status).toBe(400);
    const { error } = (await res.json()) as { error: { code: string; message: string; details: { issues: Array<{ path: string }> } } };
    expect(error.code).toBe('validation_failed');
    expect(error.details.issues.map((i) => i.path)).toEqual(['distributor', 'kind', 'status']);
    expect(error.message).toBe('distributor: unknown distributor: agl (one of endeavour, essential, ausgrid)');
  });
});

describe('outageUtc', () => {
  it('a time with a zone as written; without one as Sydney time; anything else null', () => {
    expect(outageUtc('2026-11-04T20:30:00+00:00')).toBe('2026-11-04T20:30:00.000Z');
    expect(outageUtc('2026-11-02T01:10:13.919746+00:00')).toBe('2026-11-02T01:10:13.919Z');
    expect(outageUtc('2026-11-02T12:00:00Z')).toBe('2026-11-02T12:00:00.000Z');
    expect(outageUtc('02/11/2026 12:00:00')).toBe('2026-11-02T01:00:00.000Z');
    expect(outageUtc('15/07/2026 12:00')).toBe('2026-07-15T02:00:00.000Z');
    expect(outageUtc('2026-07-15T12:00:00')).toBe('2026-07-15T02:00:00.000Z');
    for (const bad of ['', '  ', 'soon', '31/13/2026 10:00:00', '2026-11-02', null, undefined, 12]) expect(outageUtc(bad), String(bad)).toBeNull();
  });
});

describe('what a key needs to read it', () => {
  const KEY = 'ak_abcdefghij_' + 'O'.repeat(32);
  function withScopes(scopes: string[]) {
    _clearApiKeyCache();
    queryMock.mockReset();
    queryMock.mockResolvedValue({
      rows: [{
        id: 'k1', user_id: 'u1', name: 'cli', prefix: lookupPrefix(KEY),
        key_hash: createHash('sha256').update(KEY).digest('hex'),
        scopes, rate_limit_per_min: 100, created_at: 'x', last_used_at: null, expires_at: null, revoked_at: null,
      }],
      rowCount: 1,
    });
  }
  const asKey = { headers: { 'X-API-Key': KEY } };

  it('a key holding outages:live reads it', async () => {
    withScopes(['outages:live']);
    const res = await app({ gate: true }).request('/api/v1/outages', asKey);
    expect(res.status).toBe(200);
    expect(((await res.json()) as Answer).count).toBe(6);
  });

  it('a key with another feed, or outages history, is refused, told what it needs', async () => {
    for (const scopes of [['fires:live'], ['outages:history']]) {
      withScopes(scopes);
      const res = await app({ gate: true }).request('/api/v1/outages', asKey);
      expect(res.status, scopes[0]).toBe(403);
      expect(await res.json()).toEqual({
        error: { code: 'api_key_scope_forbidden', message: "This key's scopes do not cover this endpoint. It needs outages:live." },
      });
    }
  });
});
