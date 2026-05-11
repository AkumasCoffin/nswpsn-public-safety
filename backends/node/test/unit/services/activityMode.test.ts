/**
 * Activity-mode tracker unit tests.
 *
 * Mocks the poller's setActivityMode so we can assert on how the
 * tracker drives mode transitions without needing real timers.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

// vi.hoisted so the mock factory can reference the spy below; vi.mock
// is hoisted above all imports.
const { setActivityMode } = vi.hoisted(() => ({ setActivityMode: vi.fn() }));
vi.mock('../../../src/services/poller.js', () => ({
  setActivityMode,
}));

import {
  recordHeartbeat,
  activeViewerCount,
  dataViewerCount,
  listSessions,
  _resetForTests,
} from '../../../src/services/activityMode.js';

describe('activityMode', () => {
  beforeEach(() => {
    setActivityMode.mockReset();
    _resetForTests();
  });

  it('open + ping bump session counts and flip to active for data pages', () => {
    const r = recordHeartbeat('page-1', 'open', {
      pageType: 'live',
      isDataPage: true,
    });
    expect(r.totalViewers).toBe(1);
    expect(r.dataViewers).toBe(1);
    expect(r.active).toBe(true);
    // First report should fire setActivityMode(true).
    expect(setActivityMode).toHaveBeenCalledWith(true);
  });

  it('does not flip to active for non-data pages', () => {
    const r = recordHeartbeat('page-2', 'open', {
      pageType: 'about',
      isDataPage: false,
    });
    expect(r.totalViewers).toBe(1);
    expect(r.dataViewers).toBe(0);
    expect(r.active).toBe(false);
    expect(setActivityMode).toHaveBeenCalledWith(false);
  });

  it('close removes the session and flips back to idle', () => {
    recordHeartbeat('p', 'open', { isDataPage: true });
    expect(setActivityMode).toHaveBeenLastCalledWith(true);

    const r = recordHeartbeat('p', 'close', {});
    expect(r.totalViewers).toBe(0);
    expect(r.active).toBe(false);
    expect(setActivityMode).toHaveBeenLastCalledWith(false);
  });

  it('ping refreshes lastSeen but preserves openedAt', () => {
    recordHeartbeat('p', 'open', { isDataPage: true, pageType: 'live' });
    const before = listSessions();
    expect(before).toHaveLength(1);
    const openedAt = before[0]!.session.openedAt;

    recordHeartbeat('p', 'ping', { isDataPage: true });
    const after = listSessions();
    expect(after[0]!.session.openedAt).toBe(openedAt);
    expect(after[0]!.session.lastSeen).toBeGreaterThanOrEqual(openedAt);
  });

  it('only emits setActivityMode on transitions, not every heartbeat', () => {
    recordHeartbeat('p1', 'open', { isDataPage: true });
    recordHeartbeat('p1', 'ping', { isDataPage: true });
    recordHeartbeat('p2', 'open', { isDataPage: true });
    // Three heartbeats, one transition (idle→active).
    expect(setActivityMode).toHaveBeenCalledTimes(1);
    expect(setActivityMode).toHaveBeenCalledWith(true);
  });

  it('counts both data and non-data sessions in totalViewers', () => {
    recordHeartbeat('a', 'open', { isDataPage: true });
    recordHeartbeat('b', 'open', { isDataPage: false });
    expect(activeViewerCount()).toBe(2);
    expect(dataViewerCount()).toBe(1);
  });

  it('preserves isDataPage from open across pings without explicit value', () => {
    // Open as data page
    recordHeartbeat('p', 'open', { isDataPage: true, pageType: 'live' });
    // Ping without isDataPage should not downgrade it
    const r = recordHeartbeat('p', 'ping', {});
    expect(r.dataViewers).toBe(1);
  });
});
