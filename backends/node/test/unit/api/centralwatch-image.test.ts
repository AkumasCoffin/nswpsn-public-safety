/**
 * /api/centralwatch/image/:cameraId tests.
 *
 * Mocks the image cache so we can drive HIT / STALE / MISS branches.
 * The browser worker is mocked to never spawn chromium.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Hono } from 'hono';

const getImageMock = vi.fn();

vi.mock('../../../src/services/centralwatchImageCache.js', () => ({
  getImage: (...args: unknown[]) => getImageMock(...args),
  // Re-export the constant the handler imports — match the source.
  STALE_AFTER_MS_EXPORT: 2 * 60 * 1000,
  setImage: vi.fn(),
  hasImage: vi.fn(),
  cacheSize: vi.fn(() => 0),
  cleanup: vi.fn(() => ({ evicted: 0, remaining: 0 })),
  runBatchOnce: vi.fn(async () => ({ attempted: 0, cached: 0, evicted: 0 })),
  startCentralwatchImageBatchLoop: vi.fn(),
  stopCentralwatchImageBatchLoop: vi.fn(),
  _resetCentralwatchImageCacheForTests: vi.fn(),
}));

vi.mock('../../../src/services/centralwatchBrowser.js', () => ({
  centralwatchBrowser: {
    isReady: vi.fn(() => false),
    init: vi.fn(async () => undefined),
    shutdown: vi.fn(async () => undefined),
    fetchJson: vi.fn(),
    fetchImage: vi.fn(),
    fetchImagesBatch: vi.fn(async () => []),
  },
}));

// Stub the source so the cameras/sites endpoints don't try to read disk.
vi.mock('../../../src/sources/centralwatch.js', () => ({
  getCentralwatchCameras: vi.fn(async () => []),
  getCentralwatchSites: vi.fn(async () => []),
  startCentralwatchRefreshLoop: vi.fn(),
  stopCentralwatchRefreshLoop: vi.fn(),
  refreshCentralwatchJson: vi.fn(async () => false),
  writeCentralwatchJson: vi.fn(async () => false),
  _resetCentralwatchCacheForTests: vi.fn(),
  _testHooks: { setCacheForTests: vi.fn() },
}));

describe('/api/centralwatch/image/:cameraId', () => {
  beforeEach(() => {
    getImageMock.mockReset();
  });

  it('returns 200 + cached bytes with X-Cache: HIT for fresh entries', async () => {
    const bytes = Buffer.alloc(1024, 0xab);
    getImageMock.mockReturnValue({
      data: bytes,
      contentType: 'image/jpeg',
      ts: Date.now() - 30 * 1000, // 30s old → fresh
    });
    const { centralwatchRouter } = await import(
      '../../../src/api/centralwatch.js'
    );
    const app = new Hono().route('/', centralwatchRouter);
    const res = await app.request('/api/centralwatch/image/cam-1');
    expect(res.status).toBe(200);
    expect(res.headers.get('Content-Type')).toBe('image/jpeg');
    expect(res.headers.get('X-Cache')).toBe('HIT');
    const ageHeader = res.headers.get('X-Cache-Age');
    expect(ageHeader).toBeDefined();
    expect(Number.parseInt(ageHeader ?? '0', 10)).toBeGreaterThanOrEqual(0);
    const body = new Uint8Array(await res.arrayBuffer());
    expect(body.length).toBe(1024);
    expect(body[0]).toBe(0xab);
  });

  it('returns X-Cache: STALE for entries older than the freshness window', async () => {
    const bytes = Buffer.alloc(1024, 0x12);
    getImageMock.mockReturnValue({
      data: bytes,
      contentType: 'image/jpeg',
      ts: Date.now() - 4 * 60 * 1000, // 4 min old → stale
    });
    const { centralwatchRouter } = await import(
      '../../../src/api/centralwatch.js'
    );
    const app = new Hono().route('/', centralwatchRouter);
    const res = await app.request('/api/centralwatch/image/cam-2');
    expect(res.status).toBe(200);
    expect(res.headers.get('X-Cache')).toBe('STALE');
  });

  it('returns 200 + SVG placeholder when cache is empty', async () => {
    getImageMock.mockReturnValue(undefined);
    const { centralwatchRouter } = await import(
      '../../../src/api/centralwatch.js'
    );
    const app = new Hono().route('/', centralwatchRouter);
    const res = await app.request('/api/centralwatch/image/cam-missing');
    expect(res.status).toBe(200);
    expect(res.headers.get('Content-Type')).toBe('image/svg+xml');
    expect(res.headers.get('X-Cache')).toBe('PLACEHOLDER');
    const body = await res.text();
    expect(body).toContain('<svg');
    expect(body).toContain('Fire Watch Camera');
  });

  it('sets the no-cache headers parity with python', async () => {
    getImageMock.mockReturnValue({
      data: Buffer.alloc(800, 0x55),
      contentType: 'image/jpeg',
      ts: Date.now() - 5_000,
    });
    const { centralwatchRouter } = await import(
      '../../../src/api/centralwatch.js'
    );
    const app = new Hono().route('/', centralwatchRouter);
    const res = await app.request('/api/centralwatch/image/cam-3');
    expect(res.headers.get('Cache-Control')).toBe(
      'no-cache, no-store, must-revalidate',
    );
    expect(res.headers.get('Pragma')).toBe('no-cache');
    expect(res.headers.get('Access-Control-Allow-Origin')).toBe('*');
  });

  it('preserves the cached content-type (e.g. image/png)', async () => {
    getImageMock.mockReturnValue({
      data: Buffer.alloc(700, 0xcd),
      contentType: 'image/png',
      ts: Date.now() - 1000,
    });
    const { centralwatchRouter } = await import(
      '../../../src/api/centralwatch.js'
    );
    const app = new Hono().route('/', centralwatchRouter);
    const res = await app.request('/api/centralwatch/image/cam-png');
    expect(res.headers.get('Content-Type')).toBe('image/png');
  });
});
