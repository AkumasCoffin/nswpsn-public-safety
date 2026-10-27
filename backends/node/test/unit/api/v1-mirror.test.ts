/**
 * mirror() and the v1 response helpers, on small apps of their own.
 */
import { describe, it, expect, vi } from 'vitest';
import { Hono } from 'hono';
import { HTTPException } from 'hono/http-exception';

vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

import { mirror, mirroredRoutes } from '../../../src/api/v1/mirror.js';
import { fail, geo, notFound, ok, onError } from '../../../src/api/v1/respond.js';
import { declaredNeedOf, scoped } from '../../../src/services/auth/apiKeyScopes.js';

describe('mirror', () => {
  function router() {
    const r = new Hono();
    const order: string[] = [];
    r.use('/api/mx/*', async (c, next) => {
      c.header('X-Router-Mw', '1');
      await next();
    });
    r.get('/api/mx/a', scoped({ source: 'rfs', type: 'incidents', window: 'live' }), (c) => ok(c, { a: 1 }));
    r.get('/api/mx/b/:id', (c) => ok(c, { id: c.req.param('id') }));
    r.get('/api/mx/private', (c) => c.json({ secret: true }));
    r.post('/api/mx/a', (c) => c.json({ posted: true }));
    return { r, order };
  }

  it('serves the named routes on /api/v1/ with the same chain, and leaves the rest off it', async () => {
    const { r } = router();
    const app = new Hono();
    app.route('/', r);
    mirror(app, r, ['/api/mx/a', '/api/mx/b/:id']);

    const a = await app.request('/api/v1/mx/a');
    expect(await a.json()).toEqual({ data: { a: 1 } });
    expect(a.headers.get('X-Router-Mw')).toBe('1'); // router middleware travelled
    expect(await (await app.request('/api/v1/mx/b/7')).json()).toEqual({ data: { id: '7' } });
    expect((await app.request('/api/v1/mx/private')).status).toBe(404);
    // Every method of a named path comes along — the gate refuses writes for keys.
    expect((await app.request('/api/v1/mx/a', { method: 'POST' })).status).toBe(200);
    // The old paths are untouched.
    expect(await (await app.request('/api/mx/a')).json()).toEqual({ a: 1 });
  });

  it('records each mirrored route with its chain, so its scope can be found', () => {
    const { r } = router();
    mirror(new Hono(), r, ['/api/mx/a']);
    const route = mirroredRoutes().find((x) => x.method === 'GET' && x.path === '/api/v1/mx/a');
    expect(route?.legacyPath).toBe('/api/mx/a');
    expect(declaredNeedOf(route!.handlers)).toEqual({ source: 'rfs', type: 'incidents', window: 'live' });
  });

  it('throws on a name the router does not have', () => {
    const { r } = router();
    expect(() => mirror(new Hono(), r, ['/api/mx/gone'])).toThrow(/no route \/api\/mx\/gone/);
  });

  it('carries middleware on /prefix/* to the prefix itself, as Hono runs it', async () => {
    const r = new Hono();
    r.use('/api/mx/*', async (c, next) => {
      c.header('X-Prefix-Mw', '1');
      await next();
    });
    r.get('/api/mx', (c) => c.text('root'));
    const app = new Hono();
    app.route('/', r);
    mirror(app, r, ['/api/mx']);
    expect((await app.request('/api/mx')).headers.get('X-Prefix-Mw')).toBe('1');
    expect((await app.request('/api/v1/mx')).headers.get('X-Prefix-Mw')).toBe('1');
  });

  it('carries middleware on a parameterised prefix', async () => {
    const r = new Hono();
    r.use('/api/mx/:id/*', async (c, next) => {
      c.header('X-Param-Mw', '1');
      await next();
    });
    r.get('/api/mx/a', (c) => c.text('a'));
    const app = new Hono();
    mirror(app, r, ['/api/mx/a']);
    expect((await app.request('/api/v1/mx/a')).headers.get('X-Param-Mw')).toBe('1');
  });

  it('throws when an unmirrored route would run first on the old path', () => {
    // A method-specific guard…
    const guarded = new Hono();
    guarded.get('/api/mx/*', async (_c, next) => next());
    guarded.get('/api/mx/a', (c) => c.text('a'));
    expect(() => mirror(new Hono(), guarded, ['/api/mx/a'])).toThrow(/runs before "\/api\/mx\/a"/);
    // …or a :param route registered ahead of the literal one.
    const param = new Hono();
    param.get('/api/mx/:kind', (c) => c.text('kind'));
    param.get('/api/mx/names', (c) => c.text('names'));
    expect(() => mirror(new Hono(), param, ['/api/mx/names'])).toThrow(/runs before/);
  });

  it('does not mind a route registered after, or one of another method', () => {
    const r = new Hono();
    r.post('/api/mx/:kind', (c) => c.text('post'));
    r.get('/api/mx/names', (c) => c.text('names'));
    r.get('/api/mx/:kind', (c) => c.text('kind'));
    expect(() => mirror(new Hono(), r, ['/api/mx/names'])).not.toThrow();
  });

  it('throws rather than guess at a wildcard mid-pattern', () => {
    const r = new Hono();
    r.use('/api/*/x', async (_c, next) => next());
    r.get('/api/mx/a', (c) => c.text('a'));
    expect(() => mirror(new Hono(), r, ['/api/mx/a'])).toThrow(/cannot tell/);
  });
});

describe('respond', () => {
  const app = new Hono();
  for (const base of ['/api', '/api/v1']) {
    app.get(`${base}/ok`, (c) => ok(c, { n: 1 }));
    app.get(`${base}/geo`, (c) => geo(c, { type: 'FeatureCollection', features: [] }));
    app.get(`${base}/fail`, (c) => fail(c, 400, 'validation_failed', 'bad n', { error: 'bad n', max: 5 }, { field: 'n' }));
    app.get(`${base}/fail-plain`, (c) => fail(c, 503, 'service_unavailable', 'database unavailable'));
    app.get(`${base}/throw`, () => {
      throw new Error('kaboom');
    });
    app.get(`${base}/http`, () => {
      throw new HTTPException(413, { message: 'too big' });
    });
  }
  app.notFound(notFound);
  app.onError(onError);
  const json = async (p: string) => {
    const res = await app.request(p);
    return { status: res.status, body: await res.json() };
  };

  it('ok wraps on v1 only', async () => {
    expect((await json('/api/v1/ok')).body).toEqual({ data: { n: 1 } });
    expect((await json('/api/ok')).body).toEqual({ n: 1 });
  });

  it('geo is bare on both', async () => {
    expect((await json('/api/v1/geo')).body).toEqual({ type: 'FeatureCollection', features: [] });
  });

  it('fail: the v1 shape with details, or the old body', async () => {
    expect(await json('/api/v1/fail')).toEqual({
      status: 400,
      body: { error: { code: 'validation_failed', message: 'bad n', details: { field: 'n' } } },
    });
    expect(await json('/api/fail')).toEqual({ status: 400, body: { error: 'bad n', max: 5 } });
    expect((await json('/api/fail-plain')).body).toEqual({ error: 'database unavailable' });
  });

  it('a thrown error: JSON 500 on v1 that says nothing of the cause, plain text elsewhere', async () => {
    const v1 = await json('/api/v1/throw');
    expect(v1).toEqual({ status: 500, body: { error: { code: 'internal_error', message: 'Something went wrong on our side.' } } });
    const old = await app.request('/api/throw');
    expect(old.status).toBe(500);
    expect(await old.text()).toBe('Internal Server Error');
  });

  it('an HTTPException keeps its status', async () => {
    expect((await json('/api/v1/http')).status).toBe(413);
    expect((await app.request('/api/http')).status).toBe(413);
  });

  it('not found', async () => {
    expect((await json('/api/v1/missing')).body.error.code).toBe('not_found');
    expect(await (await app.request('/api/missing')).text()).toBe('404 Not Found');
  });
});
