/**
 * Essential Energy outages on /api/v1/ and on their old /api/ paths.
 *
 * One handler serves both (api/v1/mirror.ts): wrapped in `data` on v1, bare on
 * the old paths. /outages/raw reads both of Essential Energy's feeds on the
 * request: one failing leaves what the other gave on both paths; both failing
 * is the old path's empty 200 and a v1 503. /outages/future stays off v1. The
 * old paths are checked byte for byte.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { liveStore } from '../../../src/store/live.js';
import { parseEssentialKml } from '../../../src/sources/essential.js';

// Hoisted: the static import of sources/essential.js above loads the mocked
// module before this file's own top-level code runs.
const { fetchTextMock } = vi.hoisted(() => ({ fetchTextMock: vi.fn() }));

vi.mock('../../../src/sources/shared/http.js', () => ({
  fetchJson: vi.fn(),
  fetchText: fetchTextMock,
  fetchRaw: vi.fn(),
  HttpError: class extends Error {},
}));

const V1_PATHS = [
  '/api/essential/outages',
  '/api/essential/planned',
  '/api/essential/future',
  '/api/essential/outages/current',
  '/api/essential/outages/raw',
];

async function app() {
  const { essentialRouter } = await import('../../../src/api/essential.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', essentialRouter);
  mirror(a, essentialRouter, V1_PATHS);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

async function get(a: Hono, path: string) {
  const res = await a.request(path);
  return { status: res.status, type: res.headers.get('content-type'), text: await res.text() };
}

const v1 = (p: string) => p.replace(/^\/api\//, '/api/v1/');

// Essential Energy's KML (trimmed): one unplanned outage, one planned with an area.
const KML = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://earth.google.com/kml/2.1">
  <Document>
    <Placemark id="INCD-1-r">
      <name>WAGGA WAGGA</name>
      <styleUrl>#unplanned</styleUrl>
      <description><![CDATA[
        <div><span>Time Off:</span>28/10/2026 14:30:00</div>
        <div><span>Est. Time On:</span>28/10/2026 17:00:00</div>
        <div><span>Customers affected:</span> 42</div>
        <div><span>Reason:</span>Storm damage</div>
        <div><span>Last Updated:</span>28/10/2026 14:35:00</div>
      ]]></description>
      <Point><coordinates>147.367,-35.108</coordinates></Point>
    </Placemark>
    <Placemark id="INCD-2-p">
      <name>DUBBO</name>
      <styleUrl>#planned</styleUrl>
      <description><![CDATA[
        <div><span>Time Off:</span>30/10/2026 08:00:00</div>
        <div><span>Customers affected:</span> 5</div>
        <div><span>Reason:</span>Maintenance</div>
      ]]></description>
      <Point><coordinates>148.601,-32.243</coordinates></Point>
      <Polygon><outerBoundaryIs><LinearRing>
        <coordinates>148.6,-32.2 148.61,-32.2 148.61,-32.25 148.6,-32.2</coordinates>
      </LinearRing></outerBoundaryIs></Polygon>
    </Placemark>
  </Document>
</kml>`;

const CURRENT = parseEssentialKml(KML, 'current');
const FUTURE = parseEssentialKml(KML, 'future');
const sum = (xs: Array<{ customersAffected: number }>) => xs.reduce((n, o) => n + o.customersAffected, 0);
const list = (xs: typeof CURRENT) => ({ outages: xs, count: xs.length, totalCustomersAffected: sum(xs) });

beforeEach(() => {
  fetchTextMock.mockReset();
  liveStore.set('essential_current', CURRENT);
  liveStore.set('essential_future', FUTURE);
});

describe('/essential/outages', () => {
  it("answers the bot's query (feed=current&type=unplanned) — wrapped in data on v1", async () => {
    const a = await app();
    const outages = CURRENT.filter((o) => o.outageType === 'unplanned');
    const want = { outages, count: 1, planned: 0, unplanned: 1, future: 0, totalCustomersAffected: 42 };
    const p = '/api/essential/outages?feed=current&type=unplanned';
    expect(await get(a, p)).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(want) });
    expect(await get(a, v1(p))).toEqual({ status: 200, type: 'application/json', text: JSON.stringify({ data: want }) });
  });

  it('answers both feeds with counts when not narrowed, and ignores values it does not know', async () => {
    const a = await app();
    const all = [...CURRENT, ...FUTURE];
    const want = { outages: all, count: 4, planned: 2, unplanned: 2, future: 2, totalCustomersAffected: 94 };
    for (const q of ['', '?feed=junk&type=junk&lite=yes', '?feed=&type=']) {
      expect(await get(a, '/api/essential/outages' + q), q).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(want) });
      expect((await get(a, '/api/v1/essential/outages' + q)).text, q).toBe(JSON.stringify({ data: want }));
    }
  });

  it('reads feed and type in any case, and the first of a repeated value', async () => {
    const a = await app();
    const outages = FUTURE.filter((o) => o.outageType === 'planned');
    const want = { outages, count: 1, planned: 1, unplanned: 0, future: 1, totalCustomersAffected: 5 };
    const p = '/api/essential/outages?feed=FUTURE&type=Planned&feed=current';
    expect((await get(a, p)).text).toBe(JSON.stringify(want));
    expect((await get(a, v1(p))).text).toBe(JSON.stringify({ data: want }));
  });

  it('lite=1 leaves out the polygons', async () => {
    const a = await app();
    const res = JSON.parse((await get(a, '/api/v1/essential/outages?lite=1')).text);
    expect(res.data.count).toBe(4);
    for (const o of res.data.outages) expect(o).not.toHaveProperty('polygon');
    const full = JSON.parse((await get(a, '/api/essential/outages?lite=0')).text);
    expect(full.outages[1].polygon).toHaveLength(4);
  });
});

describe('the other lists', () => {
  it('answers each list — wrapped in data on v1, bare on the old path', async () => {
    const a = await app();
    const want: Record<string, unknown> = {
      '/api/essential/planned': list([...CURRENT, ...FUTURE].filter((o) => o.outageType === 'planned')),
      '/api/essential/future': list(FUTURE),
      '/api/essential/outages/current': list(CURRENT),
    };
    for (const [p, body] of Object.entries(want)) {
      expect(await get(a, p), p).toEqual({ status: 200, type: 'application/json', text: JSON.stringify(body) });
      expect(await get(a, v1(p)), p).toEqual({ status: 200, type: 'application/json', text: JSON.stringify({ data: body }) });
    }
  });

  it('empty lists before the first poll', async () => {
    liveStore.delete('essential_current');
    liveStore.delete('essential_future');
    const a = await app();
    const empty = '{"outages":[],"count":0,"totalCustomersAffected":0}';
    for (const p of ['/api/essential/planned', '/api/essential/future', '/api/essential/outages/current']) {
      expect((await get(a, p)).text, p).toBe(empty);
      expect((await get(a, v1(p))).text, p).toBe(`{"data":${empty}}`);
    }
  });
});

describe('/essential/outages/raw', () => {
  const feeds = (current: () => Promise<string>, future: () => Promise<string>) =>
    fetchTextMock.mockImplementation((url: string) => (url.includes('current.kml') ? current() : future()));
  const kml = () => Promise.resolve(KML);
  const down = () => Promise.reject(new Error('HTTP 503'));

  it('reads both feeds on the request — wrapped in data on v1', async () => {
    feeds(kml, kml);
    const a = await app();
    const want = JSON.stringify([...CURRENT, ...FUTURE]);
    expect(await get(a, '/api/essential/outages/raw')).toEqual({ status: 200, type: 'application/json', text: want });
    expect(await get(a, '/api/v1/essential/outages/raw')).toEqual({ status: 200, type: 'application/json', text: `{"data":${want}}` });
    expect(fetchTextMock).toHaveBeenCalledTimes(4);
  });

  it('when one feed cannot be read, answers what the other gave, on both paths', async () => {
    const a = await app();
    feeds(down, kml);
    expect((await get(a, '/api/essential/outages/raw')).text).toBe(JSON.stringify(FUTURE));
    expect((await get(a, '/api/v1/essential/outages/raw')).text).toBe(JSON.stringify({ data: FUTURE }));
    feeds(kml, down);
    expect((await get(a, '/api/essential/outages/raw')).text).toBe(JSON.stringify(CURRENT));
    expect((await get(a, '/api/v1/essential/outages/raw')).text).toBe(JSON.stringify({ data: CURRENT }));
  });

  it('when neither can be read: the old path its empty 200, v1 a 503', async () => {
    feeds(down, down);
    const a = await app();
    expect(await get(a, '/api/essential/outages/raw')).toEqual({ status: 200, type: 'application/json', text: '[]' });
    const res = await get(a, '/api/v1/essential/outages/raw');
    expect(res.status).toBe(503);
    expect(JSON.parse(res.text)).toEqual({ error: { code: 'service_unavailable', message: expect.any(String) } });
  });
});

describe('the mirror', () => {
  it('/outages/future stays off v1, and is unchanged on the old path', async () => {
    const a = await app();
    expect(await get(a, '/api/essential/outages/future')).toEqual({
      status: 200,
      type: 'application/json',
      text: JSON.stringify(list(FUTURE)),
    });
    const res = await get(a, '/api/v1/essential/outages/future');
    expect(res.status).toBe(404);
    expect(JSON.parse(res.text).error.code).toBe('not_found');
  });

  it('serves every in-scope route on /api/v1/', async () => {
    fetchTextMock.mockResolvedValue(KML);
    const a = await app();
    for (const p of V1_PATHS) expect((await a.request(v1(p))).status, p).toBe(200);
  });
});
