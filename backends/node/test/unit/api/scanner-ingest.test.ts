/**
 * Scanner-feed ingest: a contributor's rdio DOWNSTREAM posting straight to us.
 *
 * The two things that would silently corrupt data are pinned here: the key
 * gate (this endpoint is not behind the site API key) and the fact that rdio
 * authenticates with a FORM FIELD, because it cannot send custom headers.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const queryMock = vi.fn();
vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  getWriterPool: vi.fn(() => Promise.resolve({ query: queryMock })),
  closePool: vi.fn(),
}));

const recordScannerCall = vi.fn(() => Promise.resolve());
vi.mock('../../../src/services/nodeEvents.js', () => ({ recordScannerCall }));

// The stubbed config below has no log level, so stub the logger with it.
vi.mock('../../../src/lib/log.js', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

async function appWith(key?: string, relay?: { url: string; key: string }) {
  vi.resetModules();
  vi.doMock('../../../src/config.js', () => ({
    config: {
      SCANNER_INGEST_KEY: key,
      SCANNER_INGEST_NAME: 'Scanner feed',
      SCANNER_TIME_OFFSET_MS: -1000,
      RDIO_INTERNAL_URL: relay?.url,
      RDIO_INTERNAL_API_KEY: relay?.key,
    },
  }));
  const { scannerIngestRouter } = await import('../../../src/api/scanner-ingest.js');
  const app = new Hono();
  app.route('/', scannerIngestRouter);
  return app;
}

/** The multipart shape rdio's downstream actually sends. */
function upload(fields: Record<string, string>) {
  const fd = new FormData();
  for (const [k, v] of Object.entries(fields)) fd.append(k, v);
  fd.append('audio', new File([new Uint8Array(64)], 'audio.m4a'));
  return { method: 'POST', body: fd };
}

beforeEach(() => {
  queryMock.mockReset();
  recordScannerCall.mockClear();
  // ensureScannerNode's upsert
  queryMock.mockResolvedValue({ rows: [{ id: 'scanner-abc', enabled: true }] });
});

describe('POST /api/scanner-ingest/api/call-upload', () => {
  it('404s when no key is configured, so an unused deployment gives nothing away', async () => {
    const app = await appWith(undefined);
    const res = await app.request('/api/scanner-ingest/api/call-upload',
      upload({ key: 'anything', talkgroup: '30013' }));
    expect(res.status).toBe(404);
    expect(recordScannerCall).not.toHaveBeenCalled();
  });

  it('rejects a wrong key', async () => {
    const app = await appWith('right-key');
    const res = await app.request('/api/scanner-ingest/api/call-upload',
      upload({ key: 'wrong-key', talkgroup: '30013' }));
    expect(res.status).toBe(401);
    expect(recordScannerCall).not.toHaveBeenCalled();
  });

  it('accepts the key as a FORM FIELD — rdio cannot send headers', async () => {
    const app = await appWith('right-key');
    const res = await app.request('/api/scanner-ingest/api/call-upload',
      upload({ key: 'right-key', talkgroup: '30013', source: '2073252', dateTime: '1787000000' }));
    expect(res.status).toBe(200);
    expect(recordScannerCall).toHaveBeenCalledTimes(1);
    const call = recordScannerCall.mock.calls[0]![0] as Record<string, unknown>;
    expect(call['talkgroup']).toBe(30013);
    expect(call['sourceUnit']).toBe(2073252);
    // rdio's dateTime is SECONDS since epoch and is the call's own start.
    expect((call['receivedAt'] as Date).getTime()).toBe(1787000000 * 1000);
  });

  it('requires a talkgroup — a call we cannot attribute is not worth storing', async () => {
    const app = await appWith('right-key');
    const res = await app.request('/api/scanner-ingest/api/call-upload',
      upload({ key: 'right-key' }));
    expect(res.status).toBe(400);
    expect(recordScannerCall).not.toHaveBeenCalled();
  });

  it('accepts-and-drops when the feed is disabled, so rdio does not retry forever', async () => {
    queryMock.mockResolvedValue({ rows: [{ id: 'scanner-abc', enabled: false }] });
    const app = await appWith('right-key');
    const res = await app.request('/api/scanner-ingest/api/call-upload',
      upload({ key: 'right-key', talkgroup: '30013' }));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true, fed: false });
    expect(recordScannerCall).not.toHaveBeenCalled();
  });
});

describe('relay to central rdio', () => {
  it('shifts the forwarded dateTime by one second and swaps in the internal key', async () => {
    // The point of the alignment: rdio collapses the copies of a PATCHED
    // transmission on an exact dateTime equality (Calls.GetPatchDuplicate has
    // no window at all). A scanner stamps audio start, a node stamps call
    // setup ~1s earlier — so without this shift the copies of one patched
    // transmission never match and the patch is never recognised.
    const fetchMock = vi.fn(() => Promise.resolve(new Response('{}', { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    const app = await appWith('right-key', { url: 'http://rdio.internal', key: 'INTERNAL' });

    const res = await app.request('/api/scanner-ingest/api/call-upload',
      upload({ key: 'right-key', talkgroup: '30013', source: '2073252', dateTime: '1787000000' }));
    expect(res.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe('http://rdio.internal/api/call-upload');
    const sent = init.body as FormData;
    expect(sent.get('dateTime')).toBe('1786999999');   // one second earlier
    expect(sent.get('key')).toBe('INTERNAL');           // never their key
    expect(sent.get('talkgroup')).toBe('30013');
    vi.unstubAllGlobals();
  });

  it('surfaces a relay failure instead of swallowing it into a 200', async () => {
    // rdio retries a failed downstream and our side is idempotent, so a real
    // upstream problem must not be reported as success.
    vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new Error('down'))));
    const app = await appWith('right-key', { url: 'http://rdio.internal', key: 'INTERNAL' });
    const res = await app.request('/api/scanner-ingest/api/call-upload',
      upload({ key: 'right-key', talkgroup: '30013', dateTime: '1787000000' }));
    expect(res.status).toBe(502);
    expect(recordScannerCall).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });
});

// ---------------------------------------------------------------------------
// Transcript forwarding.
//
// The capability probe is the load-bearing part: rdio skips a downstream that
// does not advertise the feature SILENTLY (plugin_host.go:90-134), so if the
// probe regresses no transcript is ever attempted and nothing is logged to say
// so. That failure mode is invisible in production, which is why it is pinned.
// ---------------------------------------------------------------------------
describe('GET /api/scanner-ingest/api/capabilities', () => {
  it('advertises transcript-forward, unauthenticated — rdio probes with no key', async () => {
    const app = await appWith('right-key');
    const res = await app.request('/api/scanner-ingest/api/capabilities');
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ features: ['transcript-forward'] });
  });

  it('404s when the feed is off, rather than advertising what we would reject', async () => {
    const app = await appWith(undefined);
    const res = await app.request('/api/scanner-ingest/api/capabilities');
    expect(res.status).toBe(404);
  });
});

describe('POST /api/scanner-ingest/api/call-transcript', () => {
  const push = (body: unknown) => ({
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const good = {
    key: 'right-key',
    system: 3,
    talkgroup: 30302,
    dateTime: '2026-08-23T04:26:12Z',
    transcript: 'car 850 responding',
  };

  it('rejects a bad key — the key is in the BODY, not a header', async () => {
    const app = await appWith('right-key');
    const res = await app.request(
      '/api/scanner-ingest/api/call-transcript', push({ ...good, key: 'wrong' }));
    expect(res.status).toBe(401);
  });

  it('rejects an epoch dateTime: central requires RFC3339 and would 400 one hop later', async () => {
    const app = await appWith('right-key', { url: 'http://rdio.internal', key: 'INTERNAL' });
    const res = await app.request(
      '/api/scanner-ingest/api/call-transcript', push({ ...good, dateTime: '1787000000' }));
    expect(res.status).toBe(400);
  });

  it('relays with dateTime UNCHANGED and our own key', async () => {
    // Central matches a transcript to its call on (system, talkgroup, dateTime).
    // The call was relayed with the sender's own timestamp, so any rewrite here
    // orphans every transcript.
    const fetchMock = vi.fn(() => Promise.resolve(new Response('ok', { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    const app = await appWith('right-key', { url: 'http://rdio.internal', key: 'INTERNAL' });
    const res = await app.request('/api/scanner-ingest/api/call-transcript', push(good));
    expect(res.status).toBe(200);

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe('http://rdio.internal/api/call-transcript');
    const sent = JSON.parse(init.body as string);
    expect(sent.dateTime).toBe('2026-08-23T04:26:12Z');
    expect(sent.key).toBe('INTERNAL');
    expect(sent.transcript).toBe('car 850 responding');
    vi.unstubAllGlobals();
  });

  it('drops an empty transcript without troubling central', async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    const app = await appWith('right-key', { url: 'http://rdio.internal', key: 'INTERNAL' });
    const res = await app.request(
      '/api/scanner-ingest/api/call-transcript', push({ ...good, transcript: '   ' }));
    expect(res.status).toBe(200);
    expect(fetchMock).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });
});
