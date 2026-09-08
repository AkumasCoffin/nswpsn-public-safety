/**
 * Transcription routing: the endpoint rdio transcribes through.
 *
 * Two faster-whisper servers sit behind this — a VM that is always up, and a
 * PC that only runs while nobody is using it. rdio's transcripts plugin takes
 * exactly one base URL, so it points here and the backend picks a healthy one
 * per call.
 *
 * These were ported from the standalone Python router's smoke test, which ran
 * against two real backends on loopback. The behaviour pinned is the same, and
 * it is the behaviour that is invisible until the day it matters: preference
 * order, failover, retrying a backend that passes its health check but fails
 * the actual work, and draining a healthy server without killing the
 * transcription already running on it.
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

const PC = 'http://pc.local:8000';
const VM = 'http://vm.local:8000';

/** Per-backend behaviour the fake fetch obeys. */
type Fake = { up: boolean; fail: boolean; hits: number };
let pc: Fake;
let vm: Fake;

/** One fetch stub standing in for both whisper servers. */
function stubBackends() {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string, init?: RequestInit) => {
      const who = String(url).startsWith(PC) ? pc : vm;
      const name = String(url).startsWith(PC) ? 'pc' : 'vm';
      if (String(url).endsWith('/v1/models')) {
        return who.up
          ? new Response(JSON.stringify({ data: [] }), { status: 200 })
          : new Response('', { status: 503 });
      }
      // BEFORE the transcription fallthrough, or every stats probe counts as
      // a transcription hit and the pc.hits assertions below break. pc has
      // the endpoint, vm 404s — production's exact shape today.
      if (String(url).endsWith('/v1/stats')) {
        if (name === 'pc') {
          return new Response(JSON.stringify({
            model: 'large-v3', device: 'cuda', computeType: 'float16',
            waiting: 1, active: 2, totalOk: 40, totalFailed: 0,
            avgS: 0.98, p95S: 3.2, uptimeS: 600,
          }), { status: 200 });
        }
        return new Response('', { status: 404 });
      }
      // A transcription.
      if (!who.up) throw new TypeError('fetch failed');
      who.hits += 1;
      void init;
      if (who.fail) return new Response('{"error":"boom"}', { status: 500 });
      return new Response(JSON.stringify({ text: `hello from ${name}` }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    }),
  );
}

const ADMIN = 'admin-token';
const SITE_KEY = 'site-api-key';

/**
 * @param gated mount the real requireApiKey, as server.ts does on every route.
 *   Off by default so the routing tests stay about routing; the block at the
 *   bottom turns it on, because that middleware is where this feature can be
 *   broken without any of these tests noticing.
 */
async function setup(backends = `pc=${PC},vm=${VM}`, gated = false) {
  vi.resetModules();
  vi.doMock('../../../src/config.js', () => ({
    config: {
      WHISPER_BACKENDS: backends,
      WHISPER_ADMIN_TOKEN: ADMIN,
      NSWPSN_API_KEY: SITE_KEY,
    },
  }));
  const svc = await import('../../../src/services/whisperRouter.js');
  const { whisperRouter } = await import('../../../src/api/whisper.js');
  const app = new Hono();
  if (gated) {
    const { requireApiKey } = await import('../../../src/services/auth/apiKey.js');
    app.use('*', requireApiKey);
  } else {
    app.use('*', async (c, next) => {
      c.set('userId', 'u1');
      await next();
    });
  }
  app.route('/', whisperRouter);
  return { app, svc };
}

/** Run one health sweep and wait for it, instead of sleeping on the timer. */
async function probeOnce(svc: Awaited<ReturnType<typeof setup>>['svc']) {
  svc.startWhisperHealth();
  svc.stopWhisperHealth();
  // Two sweeps: FAIL_THRESHOLD is 2, so one failure is not enough to take a
  // backend out — which is the point, a single dropped packet must not flap
  // the whole feed to the other server.
  await new Promise((r) => setTimeout(r, 0));
  svc.startWhisperHealth();
  svc.stopWhisperHealth();
  await new Promise((r) => setTimeout(r, 0));
}

const post = (body = 'RIFFfake') => ({
  method: 'POST',
  headers: {
    Authorization: `Bearer ${SITE_KEY}`,
    'Content-Type': 'multipart/form-data; boundary=x',
  },
  body,
});

beforeEach(() => {
  pc = { up: true, fail: false, hits: 0 };
  vm = { up: true, fail: false, hits: 0 };
  stubBackends();
});

describe('POST /api/whisper/v1/audio/transcriptions', () => {
  it('prefers the first backend listed', async () => {
    const { app, svc } = await setup();
    await probeOnce(svc);
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ text: 'hello from pc' });
    // Named on the response, so a transcript traces to a machine without
    // reading the log.
    expect(res.headers.get('X-Whisper-Backend')).toBe('pc');
  });

  it('falls over when the preferred backend goes away', async () => {
    const { app, svc } = await setup();
    pc.up = false;
    await probeOnce(svc);
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(await res.json()).toEqual({ text: 'hello from vm' });
    expect(vm.hits).toBe(1);
    const st = svc.whisperStatus();
    expect(st.current).toBe('vm');
    expect(st.backends[0]).toMatchObject({ name: 'pc', healthy: false });
    expect(st.backends[0]!.lastError).toBeTruthy();
  });

  it('retries the other backend when one errors mid-request', async () => {
    // Healthy on the probe, 500 on the real work — the case a health check
    // alone cannot catch, and the reason this path retries at all rather than
    // trusting the probe.
    const { app, svc } = await setup();
    pc.fail = true;
    await probeOnce(svc);
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(await res.json()).toEqual({ text: 'hello from vm' });
    expect(pc.hits).toBe(1);
    expect(vm.hits).toBe(1);
  });

  it('hands a 4xx straight back instead of retrying it', async () => {
    // A 5xx is the backend failing; a 4xx is the REQUEST being wrong, and
    // sending it to the other server changes nothing but wastes a second GPU.
    const { app, svc } = await setup();
    await probeOnce(svc);
    vi.stubGlobal('fetch', vi.fn(async (url: string) =>
      String(url).endsWith('/v1/models')
        ? new Response('{}', { status: 200 })
        : new Response('{"error":"bad audio"}', { status: 400 })));
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(res.status).toBe(400);
  });

  it('refuses rather than pretending when everything is down', async () => {
    // A fabricated empty transcript would be stored as though it were real.
    const { app, svc } = await setup();
    pc.up = vm.up = false;
    await probeOnce(svc);
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(res.status).toBe(503);
    expect(svc.whisperStatus().current).toBeNull();
  });

  it('404s when the feature is off, giving nothing away', async () => {
    const { app } = await setup('');
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(res.status).toBe(404);
  });

  it('refuses an oversize body before reading it', async () => {
    const { app, svc } = await setup();
    await probeOnce(svc);
    const res = await app.request('/api/whisper/v1/audio/transcriptions', {
      ...post(),
      headers: {
        Authorization: `Bearer ${SITE_KEY}`,
        'Content-Type': 'multipart/form-data',
        'Content-Length': String(30 * 1024 * 1024),
      },
    });
    expect(res.status).toBe(413);
    expect(pc.hits).toBe(0);
  });
});

describe('GET /api/whisper/v1/models', () => {
  it('is answered here, not forwarded', async () => {
    // Forwarding it would make us look down whenever the preferred backend was
    // mid-restart, when the real work can still go to the other one.
    const { app, svc } = await setup();
    pc.up = vm.up = false;
    await probeOnce(svc);
    const res = await app.request('/api/whisper/v1/models');
    expect(res.status).toBe(200);
    expect((await res.json()).data).toHaveLength(1);
  });
});

describe('POST /api/whisper/drain', () => {
  it('stops new work without stopping the backend', async () => {
    const { app, svc } = await setup();
    await probeOnce(svc);
    const d = await app.request('/api/whisper/drain', {
      method: 'POST',
      headers: { 'x-whisper-token': ADMIN, 'Content-Type': 'application/json' },
      body: JSON.stringify({ backend: 'pc', draining: true }),
    });
    expect(d.status).toBe(200);

    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(await res.json()).toEqual({ text: 'hello from vm' });
    expect(pc.hits).toBe(0);

    // Still HEALTHY while draining — the panel has to tell an idle handover
    // apart from an outage, and the watcher polls inFlight to know when the
    // stop is safe.
    const b = svc.whisperStatus().backends[0]!;
    expect(b).toMatchObject({ name: 'pc', healthy: true, draining: true, inFlight: 0 });
  });

  it('undrains', async () => {
    const { app, svc } = await setup();
    await probeOnce(svc);
    const body = (draining: boolean) => ({
      method: 'POST',
      headers: { 'x-whisper-token': ADMIN, 'Content-Type': 'application/json' },
      body: JSON.stringify({ backend: 'pc', draining }),
    });
    await app.request('/api/whisper/drain', body(true));
    await app.request('/api/whisper/drain', body(false));
    expect(svc.whisperStatus().current).toBe('pc');
  });

  it('needs the admin token, and 404s an unknown backend rather than no-opping', async () => {
    const { app, svc } = await setup();
    await probeOnce(svc);
    const noTok = await app.request('/api/whisper/drain', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ backend: 'pc' }),
    });
    expect(noTok.status).toBe(401);

    const unknown = await app.request('/api/whisper/drain', {
      method: 'POST',
      headers: { 'x-whisper-token': ADMIN, 'Content-Type': 'application/json' },
      body: JSON.stringify({ backend: 'nope' }),
    });
    expect(unknown.status).toBe(404);
  });
});

describe('GET /api/whisper/status', () => {
  it('answers a staff session', async () => {
    const { app, svc } = await setup();
    await probeOnce(svc);
    const body = await (await app.request('/api/whisper/status')).json();
    expect(body.configured).toBe(true);
    expect(body.current).toBe('pc');
    expect(body.backends.map((b: { name: string }) => b.name)).toEqual(['pc', 'vm']);
  });

  it('also answers the headless watcher, which has a token and no session', async () => {
    const { app, svc } = await setup();
    await probeOnce(svc);
    const res = await app.request('/api/whisper/status', {
      headers: { 'x-whisper-token': ADMIN },
    });
    expect(res.status).toBe(200);
    expect((await res.json()).current).toBe('pc');
  });

  it('says NOT CONFIGURED rather than showing an outage', async () => {
    // A deployment with no backends set has nothing wrong with it; the panel
    // hides its card instead of pointing someone at a server that never
    // existed.
    const { app } = await setup('');
    const body = await (await app.request('/api/whisper/status')).json();
    expect(body).toEqual({ configured: false, backends: [] });
  });

  it('ignores a malformed backend entry instead of refusing to start', async () => {
    const { app, svc } = await setup(`pc=${PC},garbage,vm=${VM}`);
    await probeOnce(svc);
    const body = await (await app.request('/api/whisper/status')).json();
    expect(body.backends.map((b: { name: string }) => b.name)).toEqual(['pc', 'vm']);
  });
});

describe('behind requireApiKey, as server.ts actually mounts it', () => {
  // Every test above mounts the whisper router ALONE, which is how this
  // feature shipped broken for a moment: requireApiKey runs on '*' and reads
  // the same Authorization header rdio uses, so it answered 403 before any
  // handler ran. Routing tests cannot see that. These can.

  it('lets rdio transcribe with the site API key in its whisper key field', async () => {
    const { app, svc } = await setup(`pc=${PC},vm=${VM}`, true);
    await probeOnce(svc);
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ text: 'hello from pc' });
  });

  it('still refuses a transcription with no key at all', async () => {
    // The one route here that spends GPU time is the one that keeps the gate.
    const { app, svc } = await setup(`pc=${PC},vm=${VM}`, true);
    await probeOnce(svc);
    const res = await app.request('/api/whisper/v1/audio/transcriptions', {
      method: 'POST',
      headers: { 'Content-Type': 'multipart/form-data; boundary=x' },
      body: 'RIFFfake',
    });
    expect(res.status).toBe(401);
    expect(pc.hits).toBe(0);
  });

  it('lets the headless watcher drain with only its own token', async () => {
    // The watcher has no session and no reason to hold the site key, so
    // /drain and /status are public to that gate and check their own
    // credential instead.
    const { app, svc } = await setup(`pc=${PC},vm=${VM}`, true);
    await probeOnce(svc);
    const res = await app.request('/api/whisper/drain', {
      method: 'POST',
      headers: { 'x-whisper-token': ADMIN, 'Content-Type': 'application/json' },
      body: JSON.stringify({ backend: 'pc', draining: true }),
    });
    expect(res.status).toBe(200);
    const st = await app.request('/api/whisper/status', {
      headers: { 'x-whisper-token': ADMIN },
    });
    expect(st.status).toBe(200);
  });

  it('does not let that exemption become an open door', async () => {
    const { app, svc } = await setup(`pc=${PC},vm=${VM}`, true);
    await probeOnce(svc);
    const res = await app.request('/api/whisper/drain', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ backend: 'pc' }),
    });
    expect(res.status).toBe(401);
  });
});

describe('quarantine: a backend that passes its probe but fails real work', () => {
  // The fault this guards against actually happened: a whisper whose CUDA
  // libraries were off the loader path loaded its model, answered /v1/models,
  // and 500'd every transcription. The probe cannot see that, so the backend
  // stayed healthy and preferred, and every call paid a failed attempt before
  // the retry landed on the other server.

  it('stops choosing a backend after repeated transcription failures', async () => {
    const { app, svc } = await setup();
    pc.fail = true;      // probe passes, work 500s
    await probeOnce(svc);

    // Three calls: each fails on pc and retries onto vm.
    for (let i = 0; i < 3; i++) {
      const r = await app.request('/api/whisper/v1/audio/transcriptions', post());
      expect(await r.json()).toEqual({ text: 'hello from vm' });
    }
    expect(pc.hits).toBe(3);

    // The fourth call must not touch pc at all.
    await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(pc.hits).toBe(3);
    expect(svc.whisperStatus().current).toBe('vm');

    // And the panel can say WHY, distinctly from down or draining.
    const b = svc.whisperStatus().backends[0]!;
    expect(b).toMatchObject({ name: 'pc', healthy: true });
    expect(b.quarantinedUntil).not.toBeNull();
  });

  it('a success clears the strike count, so occasional blips never accumulate', async () => {
    const { app, svc } = await setup();
    await probeOnce(svc);
    pc.fail = true;
    await app.request('/api/whisper/v1/audio/transcriptions', post());
    await app.request('/api/whisper/v1/audio/transcriptions', post());
    pc.fail = false;     // recovered before the third strike
    await app.request('/api/whisper/v1/audio/transcriptions', post());

    pc.fail = true;      // two more failures — still under threshold
    await app.request('/api/whisper/v1/audio/transcriptions', post());
    await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(svc.whisperStatus().backends[0]!.quarantinedUntil).toBeNull();
  });

  it('re-tests after the quarantine expires, with a clean slate', async () => {
    // Fake ONLY Date: the quarantine expiry is a Date.now() comparison, but
    // probeOnce awaits a real setTimeout — full fake timers leave it parked
    // forever and the test dies on its own timeout instead.
    vi.useFakeTimers({ toFake: ['Date'] });
    try {
      const { app, svc } = await setup();
      await probeOnce(svc);
      pc.fail = true;
      for (let i = 0; i < 3; i++) {
        await app.request('/api/whisper/v1/audio/transcriptions', post());
      }
      expect(svc.whisperStatus().current).toBe('vm');

      pc.fail = false;   // the operator fixed it during the rest
      vi.advanceTimersByTime(61_000);
      const r = await app.request('/api/whisper/v1/audio/transcriptions', post());
      // Preferred again, first call after expiry IS the re-test.
      expect(await r.json()).toEqual({ text: 'hello from pc' });
      expect(svc.whisperStatus().current).toBe('pc');
      expect(svc.whisperStatus().backends[0]!.quarantinedUntil).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('per-backend stats enrichment', () => {
  it('carries each backend own /v1/stats, and tolerates one without the endpoint', async () => {
    // waiting (queue depth) is the point: a call queued inside whisper looks
    // in-flight to this router, so only the server itself can report it. The
    // vm has not been updated yet and 404s — that must read as "no stats",
    // never as unhealthy, and must not count as a transcription attempt.
    const { app, svc } = await setup();
    await probeOnce(svc);

    const st = svc.whisperStatus();
    const pcB = st.backends[0]!;
    const vmB = st.backends[1]!;
    expect(pcB.stats).toMatchObject({ model: 'large-v3', device: 'cuda', waiting: 1 });
    expect(pcB.statsAt).not.toBeNull();
    expect(vmB.stats).toBeNull();
    expect(vmB.healthy).toBe(true);

    // Probing cost no transcription hits.
    expect(pc.hits).toBe(0);
    expect(vm.hits).toBe(0);

    // And the HTTP surface carries it through.
    const body = await (await app.request('/api/whisper/status')).json();
    expect(body.backends[0].stats.model).toBe('large-v3');
    expect(body.backends[1].stats).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Durable hourly stats (whisper_hourly) + the history endpoint.
// ---------------------------------------------------------------------------
// The setups above mock config WITHOUT DATABASE_URL, so the real
// getWriterPool() returns null and recording no-ops — which is why none of
// the earlier tests needed to change. Here the pool module is mocked so the
// upserts (fire-and-forget from whisperForward) become observable.

const statsQueryMock = vi.fn(async () => ({ rows: [] as unknown[] }));

async function setupWithDb(backends = `pc=${PC},vm=${VM}`) {
  vi.resetModules();
  vi.doMock('../../../src/config.js', () => ({
    config: {
      WHISPER_BACKENDS: backends,
      WHISPER_ADMIN_TOKEN: ADMIN,
      NSWPSN_API_KEY: SITE_KEY,
    },
  }));
  vi.doMock('../../../src/db/pool.js', () => ({
    getPool: async () => ({ query: statsQueryMock }),
    getWriterPool: async () => ({ query: statsQueryMock }),
    closePool: async () => undefined,
  }));
  const svc = await import('../../../src/services/whisperRouter.js');
  const { whisperRouter } = await import('../../../src/api/whisper.js');
  const app = new Hono();
  app.use('*', async (c, next) => {
    c.set('userId', 'u1');
    await next();
  });
  app.route('/', whisperRouter);
  return { app, svc };
}

/** Recording is fire-and-forget (void async) — give it a macrotask to land. */
const flushStats = () => new Promise((r) => setTimeout(r, 0));

/** The whisper_hourly upsert calls only (history reads use SELECT). */
const upserts = () =>
  statsQueryMock.mock.calls.filter((c) => String(c[0]).includes('INSERT INTO whisper_hourly'));

describe('whisper_hourly recording', () => {
  beforeEach(() => {
    statsQueryMock.mockClear();
    statsQueryMock.mockImplementation(async () => ({ rows: [] }));
  });

  it('a served transcription upserts one success row for the serving backend', async () => {
    const { app, svc } = await setupWithDb();
    await probeOnce(svc);
    statsQueryMock.mockClear(); // drop anything from the probe path
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(res.status).toBe(200);
    await flushStats();
    const calls = upserts();
    expect(calls).toHaveLength(1);
    const sql = String(calls[0]![0]);
    // Bucketing happens in SQL so app clocks never skew the hour.
    expect(sql).toContain("date_trunc('hour', now())");
    expect(sql).toContain('ON CONFLICT (hour, backend)');
    const params = calls[0]![1] as unknown[];
    expect(params[0]).toBe('pc');
    expect(params[1]).toBe(0); // no failure
    expect(typeof params[2]).toBe('number'); // success latency, ms
  });

  it('a failover writes a failure row for pc and a success row for vm', async () => {
    const { app, svc } = await setupWithDb();
    pc.fail = true;
    await probeOnce(svc);
    statsQueryMock.mockClear();
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(res.status).toBe(200);
    await flushStats();
    const rows = upserts().map((c) => c[1] as unknown[]);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual(['pc', 1, 0]); // failed attempt: no latency recorded
    expect(rows[1]![0]).toBe('vm');
    expect(rows[1]![1]).toBe(0);
  });

  it("no backend available records a single 'none' failure", async () => {
    const { app, svc } = await setupWithDb();
    pc.up = vm.up = false;
    await probeOnce(svc);
    statsQueryMock.mockClear();
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(res.status).toBe(503);
    await flushStats();
    const rows = upserts().map((c) => c[1] as unknown[]);
    expect(rows).toEqual([['none', 1, 0]]);
  });

  it('a stats write failure never touches the transcription response', async () => {
    const { app, svc } = await setupWithDb();
    statsQueryMock.mockRejectedValue(new Error('db down'));
    await probeOnce(svc);
    const res = await app.request('/api/whisper/v1/audio/transcriptions', post());
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ text: 'hello from pc' });
    await flushStats();
  });
});

describe('GET /api/whisper/history', () => {
  beforeEach(() => {
    statsQueryMock.mockClear();
    statsQueryMock.mockImplementation(async () => ({ rows: [] }));
  });

  it('needs a session or the admin token (real gate: public path, role inside)', async () => {
    // Mount the REAL api-key gate the way server.ts does: /api/whisper/history
    // is a public path, so the request reaches the handler where the role
    // check rejects an anonymous caller.
    vi.resetModules();
    vi.doMock('../../../src/config.js', () => ({
      config: { WHISPER_BACKENDS: `pc=${PC}`, WHISPER_ADMIN_TOKEN: ADMIN, NSWPSN_API_KEY: SITE_KEY },
    }));
    vi.doMock('../../../src/db/pool.js', () => ({
      getPool: async () => ({ query: statsQueryMock }),
      getWriterPool: async () => ({ query: statsQueryMock }),
      closePool: async () => undefined,
    }));
    const { whisperRouter } = await import('../../../src/api/whisper.js');
    const { requireApiKey } = await import('../../../src/services/auth/apiKey.js');
    const app = new Hono();
    app.use('*', requireApiKey);
    app.route('/', whisperRouter);
    const res = await app.request('/api/whisper/history');
    expect([401, 403]).toContain(res.status);
    // The admin token short-circuits the role gate, same as /status.
    const ok = await app.request('/api/whisper/history', { headers: { 'x-whisper-token': ADMIN } });
    expect(ok.status).toBe(200);
  });

  it('serves rows with computed avgMs (null when every attempt failed)', async () => {
    const { app } = await setupWithDb();
    statsQueryMock.mockResolvedValueOnce({
      rows: [
        { hour: new Date('2026-09-09T03:00:00Z'), backend: 'pc', requests: 5, failures: 2, total_ms: '3000' },
        { hour: new Date('2026-09-09T03:00:00Z'), backend: 'vm', requests: 2, failures: 2, total_ms: '0' },
      ],
    } as never);
    const res = await app.request('/api/whisper/history?hours=24');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.configured).toBe(true);
    expect(body.rows).toEqual([
      { hour: '2026-09-09T03:00:00.000Z', backend: 'pc', requests: 5, failures: 2, avgMs: 1000 },
      { hour: '2026-09-09T03:00:00.000Z', backend: 'vm', requests: 2, failures: 2, avgMs: null },
    ]);
  });

  it('clamps hours to 1..720 and defaults to 24', async () => {
    const { app } = await setupWithDb();
    const hoursParam = async (qs: string) => {
      statsQueryMock.mockClear();
      await app.request(`/api/whisper/history${qs}`);
      const sel = statsQueryMock.mock.calls.find((c) => String(c[0]).includes('FROM whisper_hourly'));
      return (sel?.[1] as unknown[] | undefined)?.[0];
    };
    expect(await hoursParam('')).toBe(24);
    expect(await hoursParam('?hours=0')).toBe(1);
    expect(await hoursParam('?hours=9999')).toBe(720);
    expect(await hoursParam('?hours=168')).toBe(168);
  });

  it('reports configured:false without touching the DB when whisper is off', async () => {
    const { app } = await setupWithDb('');
    const res = await app.request('/api/whisper/history');
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({ configured: false, hours: 24, rows: [] });
    expect(statsQueryMock).not.toHaveBeenCalled();
  });
});
