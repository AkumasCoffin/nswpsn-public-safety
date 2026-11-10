/**
 * GET /api/rdio/calls/:callId on /api/v1/ and on its old /api/ path — one
 * reception and its transcript (the path keeps rdio-scanner's word).
 *
 * One handler chain serves both (api/v1/mirror.ts): the old path answers byte
 * for byte as it did, v1 wraps the body in `data` and answers errors in the
 * shared shape. The old bodies are written out literally.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';
import { config } from '../../../src/config.js';

const m = vi.hoisted(() => ({
  configured: true,
  pool: 'ok' as 'none' | 'ok' | 'fail',
  rows: [] as unknown[],
  params: [] as unknown[],
}));

vi.mock('../../../src/services/rdio.js', async (orig) => ({
  ...(await orig<typeof import('../../../src/services/rdio.js')>()),
  isRdioConfigured: () => m.configured,
  getRdioPool: async () => {
    if (m.pool === 'none') return null;
    return {
      query: async (_sql: string, params: unknown[]) => {
        m.params.push(params);
        if (m.pool === 'fail') throw new Error('canceling statement due to statement timeout');
        return { rows: m.rows };
      },
    };
  },
  resolveLabels: async (s: number | null) =>
    s === 1 ? { systemLabel: 'NSW GRN', talkgroupLabel: 'RFS Hornsby Ops' } : { systemLabel: null, talkgroupLabel: null },
  ensureUnitLabelsLoaded: async () => undefined,
  getUnitLabel: (rid: number) => (rid === 2010167 ? 'Galston 1' : null),
}));

const JSON_TYPE = 'application/json';

const ROW = {
  id: 7,
  date_time: new Date('2026-10-28T03:42:17.481Z'),
  system: 1,
  talkgroup: 2401,
  transcript: 'Galston 1 on scene, small grass fire, will advise.',
  source: 2010167,
  sources: null,
};

/** rowToShape, written out. */
const RECEPTION = {
  id: 7,
  datetime: '2026-10-28T03:42:17Z',
  system: 1,
  system_label: 'NSW GRN',
  talkgroup: 2401,
  talkgroup_label: 'RFS Hornsby Ops',
  transcript: 'Galston 1 on scene, small grass fire, will advise.',
  radio_id: 2010167,
  radio_label: 'Galston 1',
  call_url: `${config.RDIO_CALL_URL_BASE}7`,
};

async function app() {
  const { transcriptsRouter } = await import('../../../src/api/transcripts.js');
  const { mirror } = await import('../../../src/api/v1/mirror.js');
  const { notFound, onError } = await import('../../../src/api/v1/respond.js');
  const a = new Hono();
  a.route('/', transcriptsRouter);
  mirror(a, transcriptsRouter, ['/api/rdio/calls/:callId']);
  a.notFound(notFound);
  a.onError(onError);
  return a;
}

beforeEach(() => {
  m.configured = true;
  m.pool = 'ok';
  m.rows = [ROW];
  m.params = [];
});

describe('GET /api/rdio/calls/:callId and /api/v1/rdio/calls/:callId', () => {
  it('one reception: bare on the old path, in data on v1', async () => {
    const a = await app();
    const old = await a.request('/api/rdio/calls/7');
    expect(old.status).toBe(200);
    expect([...old.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await old.text()).toBe(JSON.stringify(RECEPTION));

    const v1 = await a.request('/api/v1/rdio/calls/7');
    expect(v1.status).toBe(200);
    expect([...v1.headers]).toEqual([['content-type', JSON_TYPE]]);
    expect(await v1.text()).toBe(JSON.stringify({ data: RECEPTION }));
    expect(m.params).toEqual([[7], [7]]);
  });

  it('nothing known about it but its id', async () => {
    m.rows = [{ id: 8, date_time: null, system: null, talkgroup: null, transcript: null, source: null, sources: '[{"src": 55}]' }];
    const a = await app();
    expect(await (await a.request('/api/v1/rdio/calls/008')).json()).toEqual({
      data: {
        id: 8,
        datetime: null,
        system: null,
        system_label: null,
        talkgroup: null,
        talkgroup_label: null,
        transcript: null,
        radio_id: 55,
        radio_label: null,
        call_url: `${config.RDIO_CALL_URL_BASE}8`,
      },
    });
    expect(m.params).toEqual([[8]]);
  });

  it('an id that is not digits: the old 400 body; validation_failed on v1', async () => {
    const a = await app();
    for (const id of ['abc', '-1', '7.0', '1e3']) {
      const old = await a.request(`/api/rdio/calls/${id}`);
      expect(old.status, id).toBe(400);
      expect([...old.headers], id).toEqual([['content-type', JSON_TYPE]]);
      expect(await old.text(), id).toBe('{"error":"callId must be numeric"}');
      const v1 = await a.request(`/api/v1/rdio/calls/${id}`);
      expect(v1.status, id).toBe(400);
      expect(await v1.json(), id).toEqual({
        error: {
          code: 'validation_failed',
          message: 'callId: must be a whole number',
          details: { issues: [{ path: 'callId', message: 'must be a whole number' }] },
        },
      });
    }
    expect(m.params).toEqual([]);
  });

  it('no such reception: the old 404 body; not_found on v1', async () => {
    m.rows = [];
    const a = await app();
    const old = await a.request('/api/rdio/calls/99');
    expect(old.status).toBe(404);
    expect(await old.text()).toBe('{"error":"call not found"}');
    const v1 = await a.request('/api/v1/rdio/calls/99');
    expect(v1.status).toBe(404);
    expect(await v1.json()).toEqual({ error: { code: 'not_found', message: 'No reception with that id.' } });
  });

  it('no radio database on this server: 503, before the id is even looked at', async () => {
    m.configured = false;
    const a = await app();
    for (const id of ['7', 'abc']) {
      const old = await a.request(`/api/rdio/calls/${id}`);
      expect(old.status, id).toBe(503);
      expect(await old.text(), id).toBe('{"error":"RDIO_DATABASE_URL not configured"}');
      const v1 = await a.request(`/api/v1/rdio/calls/${id}`);
      expect(v1.status, id).toBe(503);
      expect(await v1.json(), id).toEqual({
        error: { code: 'service_unavailable', message: 'Radio receptions are not available on this server.' },
      });
    }
  });

  it('configured but no pool: the same 503', async () => {
    m.pool = 'none';
    const a = await app();
    const old = await a.request('/api/rdio/calls/7');
    expect(old.status).toBe(503);
    expect(await old.text()).toBe('{"error":"RDIO_DATABASE_URL not configured"}');
    const v1 = await a.request('/api/v1/rdio/calls/7');
    expect(v1.status).toBe(503);
    expect(((await v1.json()) as { error: { code: string } }).error.code).toBe('service_unavailable');
  });

  it('a failing query: 500 with the old body on the old path', async () => {
    m.pool = 'fail';
    const a = await app();
    const old = await a.request('/api/rdio/calls/7');
    expect(old.status).toBe(500);
    expect(await old.text()).toBe('{"error":"failed to load transcripts"}');
    const v1 = await a.request('/api/v1/rdio/calls/7');
    expect(v1.status).toBe(500);
    expect(await v1.json()).toEqual({ error: { code: 'internal_error', message: 'The reception could not be read.' } });
  });

  it('mirroring registers exactly this route', async () => {
    const { mirroredRoutes } = await import('../../../src/api/v1/mirror.js');
    await app();
    expect(mirroredRoutes().filter((r) => r.path.startsWith('/api/v1/rdio/')).map((r) => `${r.method} ${r.legacyPath}`)).toEqual([
      'GET /api/rdio/calls/:callId',
    ]);
  });
});
