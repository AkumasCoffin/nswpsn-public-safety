/**
 * GET /api/whisper/status — which transcription server is doing the work.
 *
 * Two faster-whisper instances sit behind whisper_router: a VM that is always
 * up, and a PC that only runs while nobody is using it. rdio points at the
 * router, so the router is the only thing that knows which one took the last
 * call, and this route is how the staff panel asks.
 *
 * What is pinned here is the difference between the three states that all look
 * like "no transcripts" from a distance and mean completely different things:
 * not configured, router unreachable, and every backend down.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

vi.mock('../../../src/services/auth/roles.js', async (orig) => {
  const actual = await orig<typeof import('../../../src/services/auth/roles.js')>();
  return { ...actual, canViewNodeData: vi.fn(() => Promise.resolve(true)) };
});

vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

async function appWith(routerUrl?: string, token?: string) {
  vi.resetModules();
  vi.doMock('../../../src/config.js', () => ({
    config: { WHISPER_ROUTER_URL: routerUrl, WHISPER_ROUTER_TOKEN: token },
  }));
  const { whisperRouter } = await import('../../../src/api/whisper.js');
  const app = new Hono();
  app.use('*', async (c, next) => {
    c.set('userId', 'u1');
    await next();
  });
  app.route('/', whisperRouter);
  return app;
}

/** The router's own /status shape, epoch seconds and all. */
const ROUTER_BODY = {
  ok: true,
  generatedAt: 1_787_000_000,
  current: 'pc',
  backends: [
    {
      name: 'pc', url: 'http://10.1.0.50:8000', priority: 0,
      healthy: true, draining: false, inFlight: 1,
      requests: 40, failures: 1, avgMs: 3200,
      lastOkAt: 1_787_000_000, lastErrorAt: null, lastError: null,
      stateSince: 1_786_990_000,
    },
    {
      name: 'vm', url: 'http://10.1.0.118:8000', priority: 1,
      healthy: true, draining: false, inFlight: 0,
      requests: 900, failures: 0, avgMs: 8100,
      lastOkAt: 1_787_000_000, lastErrorAt: null, lastError: null,
      stateSince: 1_786_000_000,
    },
  ],
};

const ok = (body: unknown) =>
  vi.fn(() => Promise.resolve(new Response(JSON.stringify(body), { status: 200 })));

beforeEach(() => {
  vi.unstubAllGlobals();
});

describe('GET /api/whisper/status', () => {
  it('reports which backend is taking the next call', async () => {
    vi.stubGlobal('fetch', ok(ROUTER_BODY));
    const app = await appWith('http://10.1.0.118:8010');
    const res = await app.request('/api/whisper/status');
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.configured).toBe(true);
    expect(body.reachable).toBe(true);
    expect(body.current).toBe('pc');
    expect(body.backends.map((b: { name: string }) => b.name)).toEqual(['pc', 'vm']);
    expect(body.backends[0].inFlight).toBe(1);
  });

  it('converts the router epoch seconds to ISO, like every other date on the page', async () => {
    vi.stubGlobal('fetch', ok(ROUTER_BODY));
    const app = await appWith('http://10.1.0.118:8010');
    const body = await (await app.request('/api/whisper/status')).json();
    expect(body.generatedAt).toBe(new Date(1_787_000_000_000).toISOString());
    expect(body.backends[0].lastOkAt).toBe(new Date(1_787_000_000_000).toISOString());
    // A null stays null rather than becoming 1970.
    expect(body.backends[0].lastErrorAt).toBeNull();
  });

  it('says NOT CONFIGURED rather than showing an outage', async () => {
    // A deployment with no router set has nothing wrong with it. Reporting it
    // as unreachable would send someone looking for a server that was never
    // supposed to exist.
    const app = await appWith(undefined);
    const body = await (await app.request('/api/whisper/status')).json();
    expect(body.configured).toBe(false);
    expect(body.reachable).toBe(false);
    expect(body.backends).toEqual([]);
  });

  it('distinguishes an unreachable router from a router with nothing healthy', async () => {
    // Both render as "no transcripts happening" but one is a network problem
    // and the other is both whisper servers being down. Conflating them sends
    // you to the wrong machine.
    vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new TypeError('fetch failed'))));
    let body = await (await (await appWith('http://10.1.0.118:8010')).request('/api/whisper/status')).json();
    expect(body).toMatchObject({ configured: true, reachable: false });
    expect(body.backends).toEqual([]);

    vi.stubGlobal('fetch', ok({ ...ROUTER_BODY, ok: false, current: null }));
    body = await (await (await appWith('http://10.1.0.118:8010')).request('/api/whisper/status')).json();
    expect(body).toMatchObject({ configured: true, reachable: true, current: null, anyAvailable: false });
    expect(body.backends).toHaveLength(2);
  });

  it('names a rejected token as a config problem, not a dead router', async () => {
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve(new Response('', { status: 401 }))));
    const app = await appWith('http://10.1.0.118:8010', 'wrong');
    const body = await (await app.request('/api/whisper/status')).json();
    expect(body.reachable).toBe(false);
    expect(body.error).toContain('token');
  });

  it('sends the token when there is one, and no header when there is not', async () => {
    const withTok = ok(ROUTER_BODY);
    vi.stubGlobal('fetch', withTok);
    await (await appWith('http://10.1.0.118:8010/', 's3cret')).request('/api/whisper/status');
    const [url, init] = withTok.mock.calls[0] as unknown as [string, RequestInit];
    // Trailing slash on the base must not produce a double slash.
    expect(url).toBe('http://10.1.0.118:8010/status');
    expect((init.headers as Record<string, string>)['x-router-token']).toBe('s3cret');
    expect(init.signal).toBeInstanceOf(AbortSignal);

    const noTok = ok(ROUTER_BODY);
    vi.stubGlobal('fetch', noTok);
    await (await appWith('http://10.1.0.118:8010')).request('/api/whisper/status');
    const [, init2] = noTok.mock.calls[0] as unknown as [string, RequestInit];
    expect((init2.headers as Record<string, string>)['x-router-token']).toBeUndefined();
  });

  it('caches a good answer so several open tabs cost the router one request', async () => {
    const f = ok(ROUTER_BODY);
    vi.stubGlobal('fetch', f);
    const app = await appWith('http://10.1.0.118:8010');
    await app.request('/api/whisper/status');
    await app.request('/api/whisper/status');
    expect(f).toHaveBeenCalledTimes(1);
  });

  it('does not cache a failure, so the panel recovers on the next poll', async () => {
    // Caching "down" would keep the card red for seconds after the router came
    // back — and the router coming back is exactly the moment someone is
    // staring at this page.
    const f = vi.fn()
      .mockRejectedValueOnce(new TypeError('fetch failed'))
      .mockResolvedValueOnce(new Response(JSON.stringify(ROUTER_BODY), { status: 200 }));
    vi.stubGlobal('fetch', f);
    const app = await appWith('http://10.1.0.118:8010');
    expect((await (await app.request('/api/whisper/status')).json()).reachable).toBe(false);
    expect((await (await app.request('/api/whisper/status')).json()).reachable).toBe(true);
    expect(f).toHaveBeenCalledTimes(2);
  });

  it('keeps draining separate from unhealthy', async () => {
    // A draining backend is a healthy server being deliberately emptied before
    // the PC shuts it down. Rendering that as an outage would make every idle
    // handover look like a fault.
    vi.stubGlobal('fetch', ok({
      ...ROUTER_BODY,
      current: 'vm',
      backends: [{ ...ROUTER_BODY.backends[0], draining: true, inFlight: 2 }, ROUTER_BODY.backends[1]],
    }));
    const app = await appWith('http://10.1.0.118:8010');
    const body = await (await app.request('/api/whisper/status')).json();
    expect(body.current).toBe('vm');
    expect(body.backends[0]).toMatchObject({ healthy: true, draining: true, inFlight: 2 });
  });
});
