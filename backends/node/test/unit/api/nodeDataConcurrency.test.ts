/**
 * The overview's bounded query runner.
 *
 * /api/node-data/overview fired nine queries at once, seven of them scanning
 * the same seven-day slice of node_radio_events. Measured on production one of
 * them alone is 660ms and touches 220,360 buffers, every one a cache hit —
 * but the table's heap and indexes total 2.1GB, so seven at once evict each
 * other, the hits become reads, and the endpoint died on the 30s statement
 * timeout with a perfectly good query plan.
 *
 * A limiter is only worth having if it actually limits, still returns every
 * result in the caller's order, and does not swallow a failure. All three are
 * pinned here, because the first is invisible in the output and the other two
 * are how a "fix" like this quietly corrupts a page instead of speeding it up.
 */
import { describe, it, expect } from 'vitest';
import { inFlightLimit } from '../../../src/api/node-data.js';

/** A task that records overlap and resolves after a tick. */
function tracker() {
  let inFlight = 0;
  let peak = 0;
  const task = <T>(value: T, ticks = 1) => async (): Promise<T> => {
    inFlight += 1;
    peak = Math.max(peak, inFlight);
    for (let i = 0; i < ticks; i += 1) await Promise.resolve();
    await new Promise((r) => setTimeout(r, 1));
    inFlight -= 1;
    return value;
  };
  return { task, peak: () => peak };
}

describe('inFlightLimit', () => {
  it('never runs more than the ceiling at once', async () => {
    const t = tracker();
    await inFlightLimit(3, [
      t.task(1), t.task(2), t.task(3), t.task(4), t.task(5),
      t.task(6), t.task(7), t.task(8), t.task(9),
    ]);
    expect(t.peak()).toBeLessThanOrEqual(3);
  });

  it('runs them all — a ceiling is not a cap on how many complete', async () => {
    const t = tracker();
    const out = await inFlightLimit(2, [
      t.task('a'), t.task('b'), t.task('c'), t.task('d'), t.task('e'),
    ]);
    expect(out).toEqual(['a', 'b', 'c', 'd', 'e']);
  });

  it('returns results in the caller order, not the completion order', async () => {
    // The overview destructures nine results positionally. If a slow task
    // landed in a fast task's slot the page would render one query's answer
    // under another query's label — wrong numbers, no error.
    const slow = <T>(value: T, ms: number) => async (): Promise<T> => {
      await new Promise((r) => setTimeout(r, ms));
      return value;
    };
    const out = await inFlightLimit(4, [
      slow('first', 30), slow('second', 1), slow('third', 20), slow('fourth', 5),
    ]);
    expect(out).toEqual(['first', 'second', 'third', 'fourth']);
  });

  it('keeps a null arm null, the way Promise.all did', async () => {
    // Every query in the overview is `wantRadio ? pool.query(..) : null`, and
    // the read path does `rTot?.rows`. A limiter that turned null into
    // undefined or a wrapper object would break the pager-only scope.
    const out = await inFlightLimit(2, [
      () => Promise.resolve({ rows: [1] }),
      () => null,
      () => Promise.resolve({ rows: [2] }),
    ]);
    expect(out[0]).toEqual({ rows: [1] });
    expect(out[1]).toBeNull();
    expect(out[2]).toEqual({ rows: [2] });
  });

  it('rejects when a task rejects, rather than resolving with a hole', async () => {
    // Promise.all rejects on the first failure and the handler's catch turns
    // that into a 500. Swallowing it here would render a page of zeroes and
    // call it success, which is worse than the error it replaced.
    await expect(
      inFlightLimit(2, [
        () => Promise.resolve(1),
        () => Promise.reject(new Error('statement timeout')),
        () => Promise.resolve(3),
      ]),
    ).rejects.toThrow('statement timeout');
  });

  it('handles a ceiling larger than the task list', async () => {
    const t = tracker();
    const out = await inFlightLimit(10, [t.task('x'), t.task('y')]);
    expect(out).toEqual(['x', 'y']);
    expect(t.peak()).toBeLessThanOrEqual(2);
  });

  it('does nothing gracefully when there is nothing to do', async () => {
    expect(await inFlightLimit(3, [])).toEqual([]);
  });
});
