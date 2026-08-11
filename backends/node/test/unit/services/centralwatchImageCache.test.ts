/**
 * Central Watch image cache unit tests.
 *
 * Covers set/get/cleanup, age-based eviction, active-id eviction, and the
 * token-gated batch fetch (mint view token → fetch image with x-wt-token).
 * The browser worker is mocked — these tests must NOT spawn chromium.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const fetchImagesBatchMock = vi.fn();
const mintViewTokensMock = vi.fn();
const isReadyMock = vi.fn();

vi.mock('../../../src/services/centralwatchBrowser.js', () => ({
  centralwatchBrowser: {
    isReady: () => isReadyMock() as boolean,
    fetchImagesBatch: (...args: unknown[]) => fetchImagesBatchMock(...args),
    mintViewTokens: (...args: unknown[]) => mintViewTokensMock(...args),
    init: vi.fn(async () => undefined),
    shutdown: vi.fn(async () => undefined),
  },
}));

const getCentralwatchCamerasMock = vi.fn();
vi.mock('../../../src/sources/centralwatch.js', () => ({
  getCentralwatchCameras: () => getCentralwatchCamerasMock() as Promise<unknown>,
}));

// Default token minter: hands back a fresh (far-future exp) token per requested id.
function defaultMint(ids: string[]): Record<string, { token: string; exp: number }> {
  const exp = Math.floor(Date.now() / 1000) + 600;
  return Object.fromEntries(ids.map((id) => [id, { token: `t-${id}`, exp }]));
}

describe('centralwatchImageCache', () => {
  beforeEach(async () => {
    fetchImagesBatchMock.mockReset();
    mintViewTokensMock.mockReset();
    isReadyMock.mockReset();
    getCentralwatchCamerasMock.mockReset();
    isReadyMock.mockReturnValue(true);
    mintViewTokensMock.mockImplementation((ids: string[]) => Promise.resolve(defaultMint(ids)));
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
    setImage('cam-old', Buffer.from([1]), 'image/jpeg', now - 6 * 60 * 1000);
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

  it('runBatchOnce mints tokens then populates cache from fetch results', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([{ id: 'cam-1' }, { id: 'cam-2' }]);
    fetchImagesBatchMock.mockResolvedValue([
      { id: 'cam-1', ok: true, bytes: Buffer.alloc(1024, 1), contentType: 'image/jpeg' },
      { id: 'cam-2', ok: true, bytes: Buffer.alloc(1024, 2), contentType: 'image/jpeg' },
    ]);
    const { runBatchOnce, getImage, cacheSize } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    const r = await runBatchOnce();
    expect(r.attempted).toBe(2);
    expect(r.cached).toBe(2);
    expect(cacheSize()).toBe(2);
    expect(getImage('cam-1')?.data.length).toBe(1024);
    // The image fetch must carry the minted token on each input.
    const inputs = fetchImagesBatchMock.mock.calls[0][0] as Array<{ id: string; token: string }>;
    expect(inputs.map((x) => x.token).sort()).toEqual(['t-cam-1', 't-cam-2']);
  });

  it('runBatchOnce reuses a cached token on the next pass (no re-mint)', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([{ id: 'cam-1' }]);
    fetchImagesBatchMock.mockResolvedValue([
      { id: 'cam-1', ok: true, bytes: Buffer.alloc(1024, 1), contentType: 'image/jpeg' },
    ]);
    const { runBatchOnce } = await import('../../../src/services/centralwatchImageCache.js');
    await runBatchOnce();
    await runBatchOnce();
    // Token minted once, reused on the second pass (fresh exp, above renew margin).
    expect(mintViewTokensMock).toHaveBeenCalledTimes(1);
    expect(fetchImagesBatchMock).toHaveBeenCalledTimes(2);
  });

  it('runBatchOnce skips cameras it cannot mint a token for', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([{ id: 'cam-1' }, { id: 'cam-2' }]);
    // Only cam-1 gets a token.
    mintViewTokensMock.mockResolvedValue({
      'cam-1': { token: 't-cam-1', exp: Math.floor(Date.now() / 1000) + 600 },
    });
    fetchImagesBatchMock.mockResolvedValue([
      { id: 'cam-1', ok: true, bytes: Buffer.alloc(1024, 1), contentType: 'image/jpeg' },
    ]);
    const { runBatchOnce, hasImage } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    const r = await runBatchOnce();
    const inputs = fetchImagesBatchMock.mock.calls[0][0] as Array<{ id: string }>;
    expect(inputs.map((x) => x.id)).toEqual(['cam-1']); // cam-2 dropped (no token)
    expect(r.attempted).toBe(1);
    expect(r.cached).toBe(1);
    expect(hasImage('cam-1')).toBe(true);
    expect(hasImage('cam-2')).toBe(false);
  });

  it('runBatchOnce drops sub-500-byte payloads as junk', async () => {
    getCentralwatchCamerasMock.mockResolvedValue([{ id: 'cam-bad' }]);
    fetchImagesBatchMock.mockResolvedValue([
      { id: 'cam-bad', ok: true, bytes: Buffer.alloc(100), contentType: 'image/jpeg' },
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
    expect(mintViewTokensMock).not.toHaveBeenCalled();
  });

  it('runBatchOnce evicts stale entries for cameras now absent from list', async () => {
    const { setImage, runBatchOnce, hasImage } = await import(
      '../../../src/services/centralwatchImageCache.js'
    );
    setImage('cam-stale', Buffer.alloc(1024, 7), 'image/jpeg');
    getCentralwatchCamerasMock.mockResolvedValue([{ id: 'cam-1' }]);
    fetchImagesBatchMock.mockResolvedValue([
      { id: 'cam-1', ok: true, bytes: Buffer.alloc(1024, 1), contentType: 'image/jpeg' },
    ]);
    await runBatchOnce();
    expect(hasImage('cam-1')).toBe(true);
    // cam-stale was not in the active list — must be evicted.
    expect(hasImage('cam-stale')).toBe(false);
  });
});
