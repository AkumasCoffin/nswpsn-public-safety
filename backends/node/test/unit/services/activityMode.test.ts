/**
 * Activity-mode tracker unit tests.
 *
 * The tracker now only tracks viewer/session counts for display — it no
 * longer drives the poller cadence (the active/idle polling split was
 * removed). These tests cover session counting + the `active` flag the
 * heartbeat response still reports.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import {
  recordHeartbeat,
  activeViewerCount,
  dataViewerCount,
  listSessions,
  _resetForTests,
} from '../../../src/services/activityMode.js';

describe('activityMode', () => {
  beforeEach(() => {
    _resetForTests();
  });

  it('open + ping bump session counts; data pages report active', () => {
    const r = recordHeartbeat('page-1', 'open', {
      pageType: 'live',
      isDataPage: true,
    });
    expect(r.totalViewers).toBe(1);
    expect(r.dataViewers).toBe(1);
    expect(r.active).toBe(true);
  });

  it('non-data pages count as viewers but do not report active', () => {
    const r = recordHeartbeat('page-2', 'open', {
      pageType: 'about',
      isDataPage: false,
    });
    expect(r.totalViewers).toBe(1);
    expect(r.dataViewers).toBe(0);
    expect(r.active).toBe(false);
  });

  it('close removes the session and drops back to not-active', () => {
    recordHeartbeat('p', 'open', { isDataPage: true });
    const r = recordHeartbeat('p', 'close', {});
    expect(r.totalViewers).toBe(0);
    expect(r.active).toBe(false);
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

  it('counts both data and non-data sessions in totalViewers', () => {
    recordHeartbeat('a', 'open', { isDataPage: true });
    recordHeartbeat('b', 'open', { isDataPage: false });
    expect(activeViewerCount()).toBe(2);
    expect(dataViewerCount()).toBe(1);
  });

  it('preserves isDataPage from open across pings without explicit value', () => {
    recordHeartbeat('p', 'open', { isDataPage: true, pageType: 'live' });
    const r = recordHeartbeat('p', 'ping', {});
    expect(r.dataViewers).toBe(1);
  });
});
