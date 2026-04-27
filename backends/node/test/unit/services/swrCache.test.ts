/**
 * SWR cache tests.
 *
 * Verifies the three-zone behaviour (fresh → stale → expired), in-flight
 * coalescing, and that a failing background refresh keeps the stale value
 * in place rather than blowing up the awaiting caller.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { SwrCache } from '../../../src/services/swrCache.js';

describe('SwrCache', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  it('serves the freshly-fetched value on first call', async () => {
    const cache = new SwrCache<number>();
    const fetcher = vi.fn(async () => 42);
    const r = await cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    expect(r.value).toBe(42);
    expect(r.warming).toBe(false);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it('returns the cached value without refetching while fresh', async () => {
    const cache = new SwrCache<number>();
    const fetcher = vi.fn(async () => 42);
    await cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    vi.advanceTimersByTime(500);
    const r = await cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    expect(r.value).toBe(42);
    expect(r.warming).toBe(false);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it('serves stale + triggers background refresh between fresh and stale windows', async () => {
    const cache = new SwrCache<number>();
    let next = 1;
    const fetcher = vi.fn(async () => next++);
    await cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    expect(fetcher).toHaveBeenCalledTimes(1);

    // Advance past fresh, before stale
    vi.advanceTimersByTime(2000);
    const r = await cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    expect(r.value).toBe(1); // got the stale value
    expect(r.warming).toBe(true);
    // Background refresh has been kicked off — let it resolve.
    await vi.runAllTimersAsync();
    expect(fetcher).toHaveBeenCalledTimes(2);

    // After the refresh resolves, peek shows the new value.
    const peeked = cache._peek('k');
    expect(peeked?.value).toBe(2);
  });

  it('blocks on a fresh fetch past the stale window', async () => {
    const cache = new SwrCache<number>();
    let next = 1;
    const fetcher = vi.fn(async () => next++);
    await cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    vi.advanceTimersByTime(6000);
    const r = await cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    expect(r.value).toBe(2); // forced refresh, awaited
    expect(r.warming).toBe(false);
    expect(r.ageMs).toBe(0);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it('coalesces concurrent fetchers per key', async () => {
    const cache = new SwrCache<number>();
    let resolveFn: (v: number) => void = () => {};
    const fetcher = vi.fn(
      () =>
        new Promise<number>((resolve) => {
          resolveFn = resolve;
        }),
    );
    const p1 = cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    const p2 = cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    const p3 = cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    expect(fetcher).toHaveBeenCalledTimes(1);
    resolveFn(7);
    const [r1, r2, r3] = await Promise.all([p1, p2, p3]);
    expect(r1.value).toBe(7);
    expect(r2.value).toBe(7);
    expect(r3.value).toBe(7);
  });

  it('keeps the stale value when a background refresh fails', async () => {
    const cache = new SwrCache<number>();
    let attempt = 0;
    const fetcher = vi.fn(async () => {
      attempt++;
      if (attempt === 1) return 1;
      throw new Error('upstream down');
    });
    const onError = vi.fn();
    await cache.get('k', fetcher, { fresh: 1000, stale: 5000, onError });
    vi.advanceTimersByTime(2000);
    // Stale read — kicks off a refresh that will fail.
    const r = await cache.get('k', fetcher, { fresh: 1000, stale: 5000, onError });
    expect(r.value).toBe(1);
    expect(r.warming).toBe(true);
    await vi.runAllTimersAsync();
    expect(onError).toHaveBeenCalled();
    // Stale value still cached; another stale read returns it.
    expect(cache._peek('k')?.value).toBe(1);
  });

  it('propagates the error when the cold-path fetcher fails', async () => {
    const cache = new SwrCache<number>();
    const fetcher = vi.fn(async () => {
      throw new Error('boom');
    });
    await expect(
      cache.get('k', fetcher, { fresh: 1000, stale: 5000 }),
    ).rejects.toThrow('boom');
  });

  it('invalidate drops a single key', async () => {
    const cache = new SwrCache<number>();
    let next = 1;
    const fetcher = vi.fn(async () => next++);
    await cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    cache.invalidate('k');
    const r = await cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    expect(r.value).toBe(2);
  });
});
