/**
 * Central Watch image cache unit tests.
 *
 * Covers set/get/cleanup, age-based eviction, and active-id eviction.
 * The browser worker is mocked — these tests must NOT spawn chromium.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchImagesBatchMock = vi.fn();
const fetchImagesBatchViaDomMock = vi.fn();
const isReadyMock = vi.fn();

vi.mock('../../../src/services/centralwatchBrowser.js', () => ({
  centralwatchBrowser: {
    isReady: () => isReadyMock() as boolean,
    fetchImagesBatch: (...args: unknown[]) => fetchImagesBatchMock(...args),
    fetchImagesBatchViaDom: (...args: unknown[]) =>
      fetchImagesBatchViaDomMock(...args),
    init: vi.fn(async () => undefined),
    shutdown: vi.fn(async () => undefined),
  },
}));

const getCentralwatchCamerasMock = vi.fn();
vi.mock('../../../src/sources/centralwatch.js', () => ({
  getCentralwatchCameras: () => getCentralwatchCamerasMock() as Promise<unknown>,
}));

describe('centralwatchImageCache', () => {
  beforeEach(async () => {
    fetchImagesBatchMock.mockReset();
    fetchImagesBatchViaDomMock.mockReset();
    isReadyMock.mockReset();
    getCentralwatchCamerasMock.mockReset();
    isReadyMock.mockReturnValue(true);
    // Default: DOM path returns nothing — older tests only configure the
    // fetch path. Successful DOM (rate >= 40%) skips the fetch fallback,
    // so we keep DOM "empty" by default and let the fetch path handle
    // the inputs (success rate 0/N triggers fallback to fetch).
    fetchImagesBatchViaDomMock.mockResolvedValue([]);
    const mod = await import('../../../src/services/centralwatchImageCache.js');
    mod._resetCentralwatchImageCacheForTests();
  });

  it('set + get round-trips bytes and content type', async () => {
    const { setImage, getImage, hasImage, cacheSize } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    setImage('cam-a', Buffer.from([0xff, 0xd8, 0xff, 0xe0]), 'image/jpeg');
    expect(hasImage('cam-a')).toBe(true);
    expect(cacheSize()).toBe(1);
    const e = getImage('cam-a');
    expect(e?.contentType).toBe('image/jpeg');
    expect(e?.data.length).toBe(4);
  });

  it('cleanup evicts entries older than 5 minutes', async () => {
    const { setImage, cleanup, cacheSize } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    const now = 10_000_000;
    // Old entry — 6 min ago
    setImage('cam-old', Buffer.from([1]), 'image/jpeg', now - 6 * 60 * 1000);
    // Fresh entry — 1 min ago
    setImage('cam-new', Buffer.from([2]), 'image/jpeg', now - 60 * 1000);
    expect(cacheSize()).toBe(2);
    const r = cleanup(new Set(['cam-old', 'cam-new']), now);
    expect(r.evicted).toBe(1);
    expect(r.remaining).toBe(1);
  });

  it('cleanup evicts cameras no longer in the active list', async () => {
    const { setImage, cleanup, hasImage } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    const now = 10_000_000;
    setImage('cam-x', Buffer.from([1]), 'image/jpeg', now);
    setImage('cam-y', Buffer.from([2]), 'image/jpeg', now);
    setImage('cam-removed', Buffer.from([3]), 'image/jpeg', now);
    const active = new Set(['cam-x', 'cam-y']);
    const r = cleanup(active, now);
    expect(r.evicted).toBe(1);
    expect(r.remaining).toBe(2);
    expect(hasImage('cam-removed')).toBe(false);
    expect(hasImage('cam-x')).toBe(true);
    expect(hasImage('cam-y')).toBe(true);
  });

  it('cleanup with empty / null active set skips active-id check', async () => {
    const { setImage, cleanup, cacheSize } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    const now = 10_000_000;
    setImage('cam-x', Buffer.from([1]), 'image/jpeg', now);
    cleanup(null, now);
    expect(cacheSize()).toBe(1);
    cleanup(new Set(), now);
    expect(cacheSize()).toBe(1);
  });

  it('runBatchOnce populates cache from browser worker results', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([
      { id: 'cam-1' },
      { id: 'cam-2' },
    ]);
    const bytes1 = Buffer.alloc(1024, 1);
    const bytes2 = Buffer.alloc(1024, 2);
    fetchImagesBatchMock.mockResolvedValue([
      { id: 'cam-1', ok: true, bytes: bytes1, contentType: 'image/jpeg' },
      { id: 'cam-2', ok: true, bytes: bytes2, contentType: 'image/jpeg' },
    ]);
    const { runBatchOnce, getImage, cacheSize } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    const r = await runBatchOnce();
    expect(r.attempted).toBe(2);
    expect(r.cached).toBe(2);
    expect(cacheSize()).toBe(2);
    expect(getImage('cam-1')?.data.length).toBe(1024);
    expect(getImage('cam-2')?.data.length).toBe(1024);
  });

  it('runBatchOnce drops sub-500-byte payloads as junk', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([{ id: 'cam-bad' }]);
    fetchImagesBatchMock.mockResolvedValue([
      {
        id: 'cam-bad',
        ok: true,
        bytes: Buffer.alloc(100), // too small
        contentType: 'image/jpeg',
      },
    ]);
    const { runBatchOnce, hasImage } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    const r = await runBatchOnce();
    expect(r.cached).toBe(0);
    expect(hasImage('cam-bad')).toBe(false);
  });

  it('runBatchOnce no-ops when browser is not ready', async () => {
    isReadyMock.mockReturnValue(false);
    const { runBatchOnce } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    const r = await runBatchOnce();
    expect(r.attempted).toBe(0);
    expect(r.cached).toBe(0);
    expect(fetchImagesBatchMock).not.toHaveBeenCalled();
  });

  it('runBatchOnce uses DOM path when success rate >= 40% (no fetch fallback)', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([
      { id: 'cam-1' },
      { id: 'cam-2' },
      { id: 'cam-3' },
    ]);
    // DOM gets all 3 — 100% >= 40%, fetch must NOT be called.
    fetchImagesBatchViaDomMock.mockResolvedValue([
      { id: 'cam-1', ok: true, bytes: Buffer.alloc(1024, 1), contentType: 'image/jpeg' },
      { id: 'cam-2', ok: true, bytes: Buffer.alloc(1024, 2), contentType: 'image/jpeg' },
      { id: 'cam-3', ok: true, bytes: Buffer.alloc(1024, 3), contentType: 'image/jpeg' },
    ]);
    const { runBatchOnce, _getStrategyStateForTests } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    const r = await runBatchOnce();
    expect(r.cached).toBe(3);
    expect(fetchImagesBatchViaDomMock).toHaveBeenCalledOnce();
    expect(fetchImagesBatchMock).not.toHaveBeenCalled();
    expect(_getStrategyStateForTests().lastStrategy).toBe('dom');
  });

  it('runBatchOnce falls back to fetch when DOM success rate < 40%', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([
      { id: 'cam-1' },
      { id: 'cam-2' },
      { id: 'cam-3' },
      { id: 'cam-4' },
      { id: 'cam-5' },
    ]);
    // DOM gets 1/5 = 20% < 40%
    fetchImagesBatchViaDomMock.mockResolvedValue([
      { id: 'cam-1', ok: true, bytes: Buffer.alloc(1024, 1), contentType: 'image/jpeg' },
      { id: 'cam-2', ok: false, error: 'load-error' },
      { id: 'cam-3', ok: false, error: 'timeout' },
      { id: 'cam-4', ok: false, error: 'load-error' },
      { id: 'cam-5', ok: false, error: 'load-error' },
    ]);
    // Fetch picks up the rest
    fetchImagesBatchMock.mockResolvedValue([
      { id: 'cam-2', ok: true, bytes: Buffer.alloc(1024, 2), contentType: 'image/jpeg' },
      { id: 'cam-3', ok: true, bytes: Buffer.alloc(1024, 3), contentType: 'image/jpeg' },
      { id: 'cam-4', ok: false, status: 429 },
      { id: 'cam-5', ok: false, status: 429 },
    ]);
    const { runBatchOnce, _getStrategyStateForTests } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    const r = await runBatchOnce();
    expect(fetchImagesBatchViaDomMock).toHaveBeenCalledOnce();
    expect(fetchImagesBatchMock).toHaveBeenCalledOnce();
    // Fetch fallback should only be called for the failed cameras (4 of them)
    const fetchInputs = fetchImagesBatchMock.mock.calls[0][0] as Array<{ id: string }>;
    expect(fetchInputs.map((x) => x.id).sort()).toEqual(['cam-2', 'cam-3', 'cam-4', 'cam-5']);
    expect(r.cached).toBe(3); // 1 from DOM + 2 from fetch
    expect(_getStrategyStateForTests().lastStrategy).toBe('fetch');
  });

  it('runBatchOnce skips DOM and goes straight to fetch when last strategy was fetch and 5min not elapsed', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([{ id: 'cam-1' }]);
    fetchImagesBatchMock.mockResolvedValue([
      { id: 'cam-1', ok: true, bytes: Buffer.alloc(1024, 1), contentType: 'image/jpeg' },
    ]);
    const { runBatchOnce, _setStrategyStateForTests } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    // Pin strategy to fetch with retry timer just refreshed
    _setStrategyStateForTests('fetch', Date.now());
    const r = await runBatchOnce();
    expect(fetchImagesBatchViaDomMock).not.toHaveBeenCalled();
    expect(fetchImagesBatchMock).toHaveBeenCalledOnce();
    expect(r.cached).toBe(1);
  });

  it('runBatchOnce retries DOM after the 5min strategy retry timer elapses', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([{ id: 'cam-1' }]);
    fetchImagesBatchViaDomMock.mockResolvedValue([
      { id: 'cam-1', ok: true, bytes: Buffer.alloc(1024, 1), contentType: 'image/jpeg' },
    ]);
    const { runBatchOnce, _setStrategyStateForTests, _getStrategyStateForTests } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    // Strategy is fetch, but retry timer was 6 minutes ago — so DOM is due.
    _setStrategyStateForTests('fetch', Date.now() - 6 * 60 * 1000);
    const r = await runBatchOnce();
    expect(fetchImagesBatchViaDomMock).toHaveBeenCalledOnce();
    expect(r.cached).toBe(1);
    // 1/1 = 100% >= 40%, flip back to dom
    expect(_getStrategyStateForTests().lastStrategy).toBe('dom');
  });

  it('runBatchOnce DOM-path filters sub-500-byte placeholders the same as fetch path', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([
      { id: 'cam-1' },
      { id: 'cam-2' },
    ]);
    fetchImagesBatchViaDomMock.mockResolvedValue([
      { id: 'cam-1', ok: true, bytes: Buffer.alloc(1024, 1), contentType: 'image/jpeg' },
      { id: 'cam-2', ok: true, bytes: Buffer.alloc(100), contentType: 'image/jpeg' }, // junk
    ]);
    const { runBatchOnce, hasImage } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    await runBatchOnce();
    expect(hasImage('cam-1')).toBe(true);
    expect(hasImage('cam-2')).toBe(false);
  });

  it('runBatchOnce evicts stale entries for cameras now absent from list', async () => {
    const { setImage, runBatchOnce, hasImage } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    setImage('cam-stale', Buffer.alloc(1024, 7), 'image/jpeg');
    getCentralwatchCamerasMock.mockResolvedValue([{ id: 'cam-1' }]);
    fetchImagesBatchMock.mockResolvedValue([
      {
        id: 'cam-1',
        ok: true,
        bytes: Buffer.alloc(1024, 1),
        contentType: 'image/jpeg',
      },
    ]);
    await runBatchOnce();
    expect(hasImage('cam-1')).toBe(true);
    // cam-stale was not in the active list — must be evicted.
    expect(hasImage('cam-stale')).toBe(false);
  });
});
