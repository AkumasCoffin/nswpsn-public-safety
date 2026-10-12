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

  // The 15-minute age bound used to be applied as a ceiling on every caller,
  // so the Planespotters photo cache (24h fresh / 7d stale) and the AnyTrip
  // shape/line/stop caches lost every entry a quarter of an hour after it was
  // written — a blocking upstream fetch against a rate-limited API instead of
  // a fresh hit, and a thrown error instead of the stale value.
  it('honours a caller window longer than the cache age bound', async () => {
    const cache = new SwrCache<number>(10);
    let next = 1;
    const fetcher = vi.fn(async () => next++);
    const opts = { fresh: 86_400_000, stale: 604_800_000 };
    const first = await cache.get('k', fetcher, opts);
    expect(first.value).toBe(1);

    // Sixteen minutes: past the cache's own age bound, far inside the
    // caller's fresh window.
    vi.advanceTimersByTime(16 * 60_000);
    const second = await cache.get('k', fetcher, opts);
    expect(second.value).toBe(1);
    expect(second.warming).toBe(false);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it('still serves the stale value to a long-window caller when the upstream fails', async () => {
    const cache = new SwrCache<number>(10);
    let attempt = 0;
    const fetcher = vi.fn(async () => {
      attempt++;
      if (attempt === 1) return 1;
      throw new Error('rate limited');
    });
    const onError = vi.fn();
    const opts = { fresh: 60_000, stale: 604_800_000, onError };
    await cache.get('k', fetcher, opts);

    // Past fresh and past the cache age bound, but inside the 7-day stale
    // window, so this is an SWR hit — not a blocking refetch that throws.
    vi.advanceTimersByTime(16 * 60_000);
    const r = await cache.get('k', fetcher, opts);
    expect(r.value).toBe(1);
    expect(r.warming).toBe(true);
    await vi.runAllTimersAsync();
    expect(onError).toHaveBeenCalled();
    expect(cache._peek('k')?.value).toBe(1);
  });

  it('still ages out an entry once its own stale window has passed', async () => {
    const cache = new SwrCache<number>(10);
    const fetcher = vi.fn(async () => 1);
    await cache.get('k', fetcher, { fresh: 1000, stale: 5000 });
    expect(cache.size()).toBe(1);
    // Past both the caller's stale window and the 15-min age bound.
    vi.advanceTimersByTime(16 * 60_000);
    await cache.get('other', fetcher, { fresh: 1000, stale: 5000 });
    expect(cache.has('k')).toBe(false);
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
