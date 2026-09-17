/**
 * Durable last-seen for a node that uploads over HTTP.
 *
 * `nodes.last_seen_at` was written only by the WebSocket status path, so an
 * ADS-B receiver whose socket had dropped — but whose uploads were arriving
 * every five seconds — read as last seen hours ago while working perfectly.
 * The upload is the better evidence of life: it carries data, where a heartbeat
 * only carries a claim.
 *
 * The throttle is the load-bearing part. Uploads land about twelve times a
 * minute per node, and the column's resolution is "recently", so a write per
 * upload would be noise against the database for no extra truth.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

const queryMock = vi.fn();
let poolAvailable = true;

vi.mock('../../../src/db/pool.js', () => ({
  getPool: async () => (poolAvailable ? { query: queryMock } : null),
}));
vi.mock('../../../src/services/staffNotify.js', () => ({ notifyStaff: vi.fn() }));

const { touchNodeSeenThrottled, _resetTouchSeenThrottle } = await import(
  '../../../src/services/nodes/registry.js'
);

const seenWrites = () =>
  queryMock.mock.calls.filter(([sql]) => String(sql).includes('last_seen_at')).length;

beforeEach(() => {
  queryMock.mockReset();
  queryMock.mockResolvedValue({ rowCount: 1, rows: [] });
  poolAvailable = true;
  _resetTouchSeenThrottle();
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-09-17T05:00:00Z'));
});

afterEach(() => {
  vi.useRealTimers();
});

it('writes once, however many uploads arrive', async () => {
  for (let i = 0; i < 12; i += 1) await touchNodeSeenThrottled('node-a');
  expect(seenWrites()).toBe(1);
});

it('writes again once the interval has passed', async () => {
  await touchNodeSeenThrottled('node-a');
  vi.advanceTimersByTime(61_000);
  await touchNodeSeenThrottled('node-a');
  expect(seenWrites()).toBe(2);
});

it('throttles each node separately', async () => {
  // One busy receiver must not keep another's last-seen from being written.
  await touchNodeSeenThrottled('node-a');
  await touchNodeSeenThrottled('node-b');
  expect(seenWrites()).toBe(2);
});

it('retries on the next upload after a failed write', async () => {
  // Marking the node touched before knowing the write landed would leave it
  // looking dead for a full interval over a transient error.
  queryMock.mockRejectedValueOnce(new Error('connection terminated'));
  await touchNodeSeenThrottled('node-a');
  await touchNodeSeenThrottled('node-a');
  expect(seenWrites()).toBe(2);
});

it('never throws, because an upload must not fail over a liveness hint', async () => {
  queryMock.mockRejectedValue(new Error('down'));
  await expect(touchNodeSeenThrottled('node-a')).resolves.toBeUndefined();
  poolAvailable = false;
  await expect(touchNodeSeenThrottled('node-b')).resolves.toBeUndefined();
});
