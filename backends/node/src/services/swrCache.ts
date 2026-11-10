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
  /** The caller's own stale window, from the get/refresh that stored this
   *  value. An entry is only aged out once BOTH this and the cache's own age
   *  bound have passed — see MAX_ENTRY_AGE_MS. */
  staleMs: number;
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

/**
 * Drop an entry this long after its last write, regardless of the cap.
 *
 * The count cap alone does not bound memory in any useful sense: a cache of 500
 * bbox-keyed viewports, each holding up to 1500 vehicle objects, is ~500MB, and
 * an entry nobody has asked for since yesterday is retained until 500 NEWER
 * keys push it out. Since keys are viewports, that eviction may never happen —
 * the working set simply plateaus at the cap and stays resident. A hard age
 * bound turns "bounded by entry count" into "bounded by recent traffic", which
 * is what the cap was meant to express.
 *
 * It is a FLOOR, not a ceiling. It was applied as the latter, so every caller
 * whose own windows were longer than it — the Planespotters photo cache at 24h
 * fresh / 7d stale, the AnyTrip shape, line and stop caches — lost its entries
 * after 15 minutes: no fresh hit, no stale fallback, a blocking upstream fetch
 * every quarter hour against a rate-limited API, and a thrown error where the
 * design promised the week-old value. An entry now survives until both this
 * and its own stale deadline have passed, so this only ever collects entries
 * that are already dead to SWR, which is what it was always meant to say.
 */
const MAX_ENTRY_AGE_MS = 15 * 60_000;

export class SwrCache<T> {
  private readonly entries = new Map<string, CacheEntry<T>>();
  // Coalesce in-flight refreshes per key — concurrent callers share the
  // promise instead of all hitting the upstream at once.
  private readonly inflight = new Map<string, Promise<T>>();
  private readonly maxEntries: number;
  private readonly maxAgeMs: number;

  constructor(maxEntries: number = DEFAULT_MAX_ENTRIES, maxAgeMs: number = MAX_ENTRY_AGE_MS) {
    this.maxEntries = Math.max(1, maxEntries);
    this.maxAgeMs = Math.max(1000, maxAgeMs);
  }

  /** Drop entries past both the cache's age bound and their own stale window.
   *  Walks the whole map rather than stopping at the first survivor: with a
   *  per-entry deadline the Map's write order no longer implies deadline
   *  order, so an early exit would strand dead entries behind a long-lived
   *  one. Bounded by maxEntries, so it is a few hundred iterations at worst. */
  private evictAged(now: number): void {
    for (const [key, entry] of this.entries) {
      if (now - entry.storedAt > Math.max(this.maxAgeMs, entry.staleMs)) {
        this.entries.delete(key);
      }
    }
  }

  /** Entries currently held — for the memory report. */
  size(): number {
    return this.entries.size;
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
    // Sweep on read as well as on write: a cache whose traffic stops (a map
    // nobody is looking at overnight) would otherwise hold its whole working
    // set until the next refresh, which may never come.
    this.evictAged(now);
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
        const storedAt = Date.now();
        this.entries.set(key, { value: v, storedAt, staleMs: Math.max(0, opts.stale) });
        // Age first, then the count cap as a backstop for a burst.
        this.evictAged(storedAt);
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
