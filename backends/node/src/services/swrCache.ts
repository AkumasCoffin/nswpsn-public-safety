/**
 * Stale-while-revalidate cache.
 *
 * Mirrors python's preset-stats SWR pattern: fresh hits return immediately,
 * stale-but-still-valid hits ALSO return immediately and kick off a
 * background refresh, miss/expired entries block on the fetcher.
 *
 * Lifetimes (per entry, both windows tick from the moment the value was
 * stored):
 *   - within `fresh` ms      → return value, no refresh
 *   - within `stale` ms      → return value AND fire-and-forget refresh
 *   - past `stale` ms        → block on a fresh fetch
 *
 * In-flight refreshes are coalesced — concurrent callers share one fetcher
 * promise instead of stampeding the upstream. A failing refresh leaves the
 * existing value in place (stays stale) and is logged via the optional
 * `onError` hook so callers can surface the failure without breaking.
 */
import { log } from '../lib/log.js';

interface CacheEntry<T> {
  value: T;
  storedAt: number;
}

export interface SwrOptions<T> {
  /** Window during which a value is considered fresh (ms). */
  fresh: number;
  /** Outer window — past `fresh` but within `stale` triggers SWR (ms).
   *  Past this, callers block on a new fetch. */
  stale: number;
  /** Optional hook for refresh failures (don't throw — SWR keeps stale). */
  onError?: (err: unknown) => void;
}

/** Hard cap on stored entries. When exceeded, the oldest-inserted entry
 *  is evicted (Map preserves insertion order, so .keys().next() is the
 *  oldest). Bounds memory for callers that key on unbounded inputs
 *  (per-filter/per-query keys) without changing SWR semantics. */
const DEFAULT_MAX_ENTRIES = 500;

export class SwrCache<T> {
  private readonly entries = new Map<string, CacheEntry<T>>();
  // Coalesce in-flight refreshes per key — concurrent callers share the
  // promise instead of all hitting the upstream at once.
  private readonly inflight = new Map<string, Promise<T>>();
  private readonly maxEntries: number;

  constructor(maxEntries: number = DEFAULT_MAX_ENTRIES) {
    this.maxEntries = Math.max(1, maxEntries);
  }

  /**
   * Return cached value if fresh; otherwise return stale value AND kick off
   * a refresh; otherwise block on the fetcher.
   *
   * `fetcher` is the function used to repopulate the cache. It must throw
   * on failure (caught + logged; stale value retained).
   */
  async get(
    key: string,
    fetcher: () => Promise<T>,
    opts: SwrOptions<T>,
  ): Promise<{ value: T; ageMs: number; warming: boolean }> {
    const now = Date.now();
    const entry = this.entries.get(key);
    const ageMs = entry ? now - entry.storedAt : Infinity;

    if (entry && ageMs < opts.fresh) {
      return { value: entry.value, ageMs, warming: false };
    }

    if (entry && ageMs < opts.stale) {
      // Stale-but-usable: return the value, kick off a background refresh.
      // The background refresh's onError hook handles logging; the
      // .catch below silences the (already-handled) rejection so it
      // doesn't surface as an unhandled rejection at process level.
      this.refresh(key, fetcher, opts).catch(() => {});
      return { value: entry.value, ageMs, warming: true };
    }

    // No cached value, or past the stale window — block.
    const fresh = await this.refresh(key, fetcher, opts);
    return { value: fresh, ageMs: 0, warming: false };
  }

  /**
   * Force-refresh the cache for `key`. Coalesces with any in-flight refresh
   * so concurrent callers share the same fetcher promise.
   */
  refresh(
    key: string,
    fetcher: () => Promise<T>,
    opts: SwrOptions<T>,
  ): Promise<T> {
    const inflight = this.inflight.get(key);
    if (inflight) return inflight;
    const p = (async () => {
      try {
        const v = await fetcher();
        // Refresh insertion order so the cap evicts the genuinely
        // least-recently-written key, then store.
        this.entries.delete(key);
        this.entries.set(key, { value: v, storedAt: Date.now() });
        // Bound memory: drop the oldest entry once over the cap.
        while (this.entries.size > this.maxEntries) {
          const oldest = this.entries.keys().next().value;
          if (oldest === undefined) break;
          this.entries.delete(oldest);
        }
        return v;
      } catch (err) {
        if (opts.onError) {
          try {
            opts.onError(err);
          } catch {
            // Hook itself threw — swallow so SWR semantics aren't broken.
          }
        } else {
          log.warn({ err, key }, 'swrCache refresh failed');
        }
        // Re-throw so the awaiting caller (cold path) sees the error;
        // background refreshes ignore the rejection by being voided.
        throw err;
      } finally {
        this.inflight.delete(key);
      }
    })();
    this.inflight.set(key, p);
    return p;
  }

  /** Drop a single key from the cache. */
  invalidate(key: string): void {
    this.entries.delete(key);
  }

  /** Whether `key` has any cached value (regardless of fresh/stale state).
   *  Used by callers that want to skip the blocking cold-start fetch and
   *  return a stub immediately when there's nothing yet. */
  has(key: string): boolean {
    return this.entries.has(key);
  }

  /** Wipe everything. Mostly for tests. */
  clear(): void {
    this.entries.clear();
    this.inflight.clear();
  }

  /** TEST-ONLY: peek at the stored timestamp for a key. */
  _peek(key: string): CacheEntry<T> | undefined {
    return this.entries.get(key);
  }
}
