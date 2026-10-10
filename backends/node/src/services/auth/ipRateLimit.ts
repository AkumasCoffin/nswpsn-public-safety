/**
 * Fixed-window counter keyed by an arbitrary string (an IP, a key id).
 *
 * In-process and deliberately simple: one backend process serves the API, so
 * a shared store would add a dependency for no gain. The window is per key,
 * starting at its first hit, which is enough to bound a loop or a script
 * without ever rejecting a real page load.
 */
export interface Limiter {
  /** Count one hit; false when the key is over its limit for this window. */
  ok(key: string): boolean;
  /** Test seam. */
  reset(): void;
}

export function makeLimiter(max: number, windowMs: number): Limiter {
  const hits = new Map<string, { count: number; resetAt: number }>();
  // Bound memory on a long-running process: every so often drop the windows
  // that have already expired rather than letting one entry per address
  // accumulate forever.
  let sweepAt = Date.now() + windowMs;
  return {
    ok(key: string): boolean {
      const now = Date.now();
      if (now > sweepAt) {
        for (const [k, v] of hits) if (now > v.resetAt) hits.delete(k);
        sweepAt = now + windowMs;
      }
      const cur = hits.get(key);
      if (!cur || now > cur.resetAt) {
        hits.set(key, { count: 1, resetAt: now + windowMs });
        return true;
      }
      cur.count += 1;
      return cur.count <= max;
    },
    reset(): void {
      hits.clear();
    },
  };
}
