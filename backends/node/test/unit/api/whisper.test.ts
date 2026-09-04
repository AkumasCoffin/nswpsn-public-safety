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

const INGEST = 'ingest-key';
const ADMIN = 'admin-token';

async function setup(backends = `pc=${PC},vm=${VM}`) {
  vi.resetModules();
  vi.doMock('../../../src/config.js', () => ({
    config: {
      WHISPER_BACKENDS: backends,
      WHISPER_INGEST_KEY: INGEST,
      WHISPER_ADMIN_TOKEN: ADMIN,
    },
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
  headers: { Authorization: `Bearer ${INGEST}`, 'Content-Type': 'multipart/form-data; boundary=x' },
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

  it('needs its own key, not the site API key', async () => {
    // The site key is public via /api/config. Transcription is GPU time, so an
    // endpoint gated on it would be a free one for anyone who looked.
    const { app, svc } = await setup();
    await probeOnce(svc);
    const res = await app.request('/api/whisper/v1/audio/transcriptions', {
      ...post(),
      headers: { Authorization: 'Bearer wrong', 'Content-Type': 'multipart/form-data' },
    });
    expect(res.status).toBe(401);
    expect(pc.hits).toBe(0);
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
        Authorization: `Bearer ${INGEST}`,
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
