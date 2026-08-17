/**
 * Bounding of the police-heatmap response cache.
 *
 * This is the test that would have caught the leak. The cache key embeds the
 * caller's bbox — four arbitrary floats off the query string — so every pan
 * and zoom of the map mints a key that is never requested again. The TTL was
 * only ever consulted on read, so nothing revisited those keys and nothing
 * removed them, and each entry pins up to 60k [lat, lng, count] triples for
 * the life of the process.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(async () => null),
  getWriterPool: vi.fn(async () => null),
}));

const { _rememberHeatmapForTests, _heatmapCacheSize, _resetHeatmapCacheForTests, _heatmapCacheKeyForTests } =
  await import('../../../src/api/waze.js');

/** A response the size the endpoint really produces at the top end. */
const bigResult = () => ({ points: Array.from({ length: 1000 }, (_, i) => [i, i, i]) });

beforeEach(() => _resetHeatmapCacheForTests());

describe('police-heatmap cache bounding', () => {
  it('does not grow without limit as the viewport changes', () => {
    const now = Date.now();
    // A user panning the map: every request a slightly different bbox, all
    // inside the TTL window so none can be dropped as expired.
    for (let i = 0; i < 5000; i++) {
      const key = _heatmapCacheKeyForTests([], [-33.8 - i * 1e-4, 150.9, -33.7, 151.2]);
      _rememberHeatmapForTests(key, bigResult(), now);
    }
    // Before the fix this was 5000 entries, each holding its own points array.
    expect(_heatmapCacheSize()).toBeLessThanOrEqual(64);
  });

  it('drops entries that have outlived their TTL', () => {
    const t0 = Date.now();
    for (let i = 0; i < 10; i++) {
      _rememberHeatmapForTests(`old-${i}`, bigResult(), t0);
    }
    expect(_heatmapCacheSize()).toBe(10);

    // One write, two minutes later: the stale ones can never be served again,
    // so they should be gone rather than waiting for a size cap to push them out.
    _rememberHeatmapForTests('fresh', bigResult(), t0 + 120_000);
    expect(_heatmapCacheSize()).toBe(1);
  });

  it('keeps entries that are still within their TTL', () => {
    const t0 = Date.now();
    _rememberHeatmapForTests('a', bigResult(), t0);
    _rememberHeatmapForTests('b', bigResult(), t0 + 1_000);
    expect(_heatmapCacheSize()).toBe(2);
  });

  it('evicts oldest-first when the cap is reached', () => {
    const now = Date.now();
    for (let i = 0; i < 70; i++) _rememberHeatmapForTests(`k${i}`, bigResult(), now);
    expect(_heatmapCacheSize()).toBe(64);
    // The survivors are the most recent 64, so the earliest keys are gone.
    _rememberHeatmapForTests('k0', bigResult(), now);
    expect(_heatmapCacheSize()).toBe(64);
  });

  it('treats a different bbox as a different key', () => {
    const a = _heatmapCacheKeyForTests([], [-33.8, 150.9, -33.7, 151.2]);
    const b = _heatmapCacheKeyForTests([], [-33.8, 150.9, -33.7, 151.3]);
    expect(a).not.toBe(b);
  });
});
