/**
 * Poller — focused tests for prewarmAll and the prewarm/startPolling
 * handoff. Mocks the source registry, LiveStore, and ArchiveWriter so
 * the poller's logic runs against a deterministic set of sources.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const okSource = {
  name: 'test_ok',
  family: 'misc' as const,
  intervalActiveMs: 60_000,
  intervalIdleMs: 120_000,
  fetch: vi.fn(async () => ({ rows: [1, 2, 3] })),
};
const failSource = {
  name: 'test_fail',
  family: 'misc' as const,
  intervalActiveMs: 60_000,
  intervalIdleMs: 120_000,
  fetch: vi.fn(async () => {
    throw new Error('upstream-broke');
  }),
};
const slowSource = {
  name: 'test_slow',
  family: 'misc' as const,
  intervalActiveMs: 60_000,
  intervalIdleMs: 120_000,
  fetch: vi.fn(
    () =>
      // Resolves AFTER our timeout — proves prewarmAll returns without
      // waiting for it.
      new Promise((resolve) => setTimeout(() => resolve({ ok: true }), 1_000)),
  ),
};

let activeSources: typeof okSource[] = [];

vi.mock('../../../src/services/sourceRegistry.js', () => ({
  allSources: () => activeSources,
  familyTable: () => 'archive_misc',
}));

const liveStoreSet = vi.fn();
vi.mock('../../../src/store/live.js', () => ({
  liveStore: { set: liveStoreSet },
}));

const archiveWriterPush = vi.fn();
vi.mock('../../../src/store/archive.js', () => ({
  archiveWriter: { push: archiveWriterPush },
}));

const {
  prewarmAll,
  startPolling,
  stopPolling,
  _resetPollerStateForTests,
  pollerHealth,
} = await import('../../../src/services/poller.js');

beforeEach(() => {
  _resetPollerStateForTests();
  okSource.fetch.mockClear();
  failSource.fetch.mockClear();
  slowSource.fetch.mockClear();
  liveStoreSet.mockClear();
  archiveWriterPush.mockClear();
  activeSources = [];
});

describe('prewarmAll', () => {
  it('runs every source\'s first poll in parallel and reports counts', async () => {
    activeSources = [okSource, failSource];
    const summary = await prewarmAll(5_000);
    expect(summary.attempted).toBe(2);
    expect(summary.succeeded).toBe(1);
    expect(summary.failed).toBe(1);
    expect(summary.pending).toBe(0);
    expect(okSource.fetch).toHaveBeenCalledOnce();
    expect(failSource.fetch).toHaveBeenCalledOnce();
    // Successful source's snapshot landed in LiveStore.
    expect(liveStoreSet).toHaveBeenCalledWith('test_ok', { rows: [1, 2, 3] });
    // Failed source did NOT call set (runOnce only writes on success).
    const setKeys = liveStoreSet.mock.calls.map((c) => c[0]);
    expect(setKeys).not.toContain('test_fail');
  });

  it('returns within the timeout when a source is still fetching', async () => {
    activeSources = [okSource, slowSource];
    const t0 = Date.now();
    const summary = await prewarmAll(100); // tight timeout
    const elapsed = Date.now() - t0;
    expect(elapsed).toBeLessThan(900); // well under slowSource's 1s
    expect(summary.attempted).toBe(2);
    expect(summary.succeeded).toBe(1);
    expect(summary.pending).toBe(1); // slowSource hadn't finished yet
  });

  it('startPolling skips initial-tick scheduling when prewarm already ran', async () => {
    activeSources = [okSource];
    await prewarmAll(5_000);
    expect(okSource.fetch).toHaveBeenCalledOnce();
    startPolling();
    // No new fetch — armOne queues the next tick at the regular
    // interval, not immediately.
    expect(okSource.fetch).toHaveBeenCalledOnce();
    stopPolling();
  });

  it('updates pollerHealth state for both succeeding and failing sources', async () => {
    activeSources = [okSource, failSource];
    await prewarmAll(5_000);
    const health = pollerHealth();
    expect(health['test_ok']?.failure_count).toBe(0);
    expect(health['test_ok']?.last_error).toBeNull();
    expect(health['test_fail']?.failure_count).toBe(1);
    expect(health['test_fail']?.last_error).toBe('upstream-broke');
  });

  it('coalesces a second runOnce into the in-flight one when prewarm timed out', async () => {
    // Hold-open source: resolves only when we call `release`. Lets us
    // simulate the prewarm-timeout-then-armOne race.
    let release!: () => void;
    const heldSource = {
      name: 'test_held',
      family: 'misc' as const,
      intervalActiveMs: 60_000,
      intervalIdleMs: 120_000,
      fetch: vi.fn(
        () =>
          new Promise<{ ok: true }>((resolve) => {
            release = () => resolve({ ok: true });
          }),
      ),
    };
    activeSources = [heldSource];

    // Kick off a prewarm but don't await — the fetch is still in
    // flight, blocked on `release`.
    const prewarmPromise = prewarmAll(50); // tight timeout so it returns
    await prewarmPromise;
    expect(heldSource.fetch).toHaveBeenCalledOnce();

    // While the original prewarm fetch is still in flight, simulate
    // armOne firing by calling runOnce again via startPolling's path.
    // We call _internal_ runOnce by triggering a second prewarmAll —
    // which iterates allSources() and fires runOnce per source. With
    // the in-flight mutex, the second invocation should NOT trigger
    // another upstream fetch.
    const secondPass = prewarmAll(200);
    // No second fetch attempt yet — the mutex coalesced it.
    expect(heldSource.fetch).toHaveBeenCalledOnce();

    // Release the original. Both prewarm invocations resolve.
    release();
    await secondPass;
    // Still only one fetch ever happened.
    expect(heldSource.fetch).toHaveBeenCalledOnce();
  });
});
