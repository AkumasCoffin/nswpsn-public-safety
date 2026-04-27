/**
 * Page-active heartbeat tracker.
 *
 * Mirrors the Python `_page_sessions` map + `is_page_active()` logic
 * (external_api_proxy.py lines 1207-1212, 2944-3000, 6348-6464). The
 * frontend pings /api/heartbeat every ~30s with `?action=ping&page_id=X`;
 * we keep an in-memory map of page_id → last seen timestamp and prune
 * anything older than 120 seconds (PAGE_SESSION_TIMEOUT in Python).
 *
 * "Active" mode is on whenever at least one *data* page is non-stale.
 * Non-data pages count toward the viewer total (so /api/heartbeat /
 * /api/collection/status can show them) but don't switch the poller
 * cadence — that matches Python behaviour where info pages don't bump
 * `last_heartbeat`.
 *
 * The activity-mode flip is sticky: when a page joins, we immediately
 * call setActivityMode(true) on the poller; when the last data page
 * goes stale or closes, we flip back to false. The poller treats this
 * as "rearm every source on the next tick" so cadence catches up
 * within seconds.
 *
 * Sweeper:
 *   - Runs every 30s (default; configurable via start()).
 *   - Removes any session whose last_seen is older than STALE_AFTER_MS.
 *   - Re-evaluates active mode after pruning so a tab going silent
 *     doesn't strand the pollers in active.
 */
import { setActivityMode } from './poller.js';
import { log } from '../lib/log.js';

export type HeartbeatAction = 'open' | 'ping' | 'close';

export interface PageSession {
  /** Wall-clock epoch ms of the most recent heartbeat. */
  lastSeen: number;
  /** First-time-seen timestamp; never updated. */
  openedAt: number;
  /** Page-type label from the heartbeat query string. */
  pageType: string;
  /** Whether this page fetches live data (drives active mode). */
  isDataPage: boolean;
  /** Best-effort client identifier; passed in for /api/collection/status. */
  ip: string;
}

/** Time after which a heartbeat-less session is considered gone. */
const STALE_AFTER_MS = 120_000;
/** How often the sweeper runs. */
const DEFAULT_SWEEP_INTERVAL_MS = 30_000;

const _sessions = new Map<string, PageSession>();
let _sweepTimer: NodeJS.Timeout | null = null;
let _lastReportedActive: boolean | null = null;

/** Internal: prune stale sessions. Returns count removed. */
function pruneStale(now: number): number {
  let removed = 0;
  for (const [id, s] of _sessions) {
    if (now - s.lastSeen > STALE_AFTER_MS) {
      _sessions.delete(id);
      removed += 1;
    }
  }
  return removed;
}

/** True if any non-stale session has isDataPage=true. */
function hasActiveDataPage(now: number): boolean {
  for (const s of _sessions.values()) {
    if (!s.isDataPage) continue;
    if (now - s.lastSeen <= STALE_AFTER_MS) return true;
  }
  return false;
}

/**
 * Re-evaluate active state and notify the poller iff it changed.
 * Centralised so both the heartbeat handler and the sweeper use the
 * same code path.
 */
function reevaluate(): void {
  const now = Date.now();
  const active = hasActiveDataPage(now);
  if (_lastReportedActive !== active) {
    _lastReportedActive = active;
    setActivityMode(active);
    log.debug({ active, sessions: _sessions.size }, 'activityMode: state change');
  }
}

/**
 * Record a heartbeat from the frontend. Returns the resulting state
 * snapshot the route handler can echo back in the JSON response.
 */
export function recordHeartbeat(
  pageId: string,
  action: HeartbeatAction,
  opts: {
    pageType?: string;
    isDataPage?: boolean;
    ip?: string;
  } = {},
): {
  totalViewers: number;
  dataViewers: number;
  active: boolean;
} {
  const now = Date.now();

  if (pageId) {
    if (action === 'close') {
      _sessions.delete(pageId);
    } else {
      const existing = _sessions.get(pageId);
      const session: PageSession = {
        lastSeen: now,
        openedAt: existing?.openedAt ?? now,
        pageType: opts.pageType ?? existing?.pageType ?? 'unknown',
        isDataPage: opts.isDataPage ?? existing?.isDataPage ?? false,
        ip: opts.ip ?? existing?.ip ?? '',
      };
      _sessions.set(pageId, session);
    }
  }

  // Always prune before counting so the values we report are accurate
  // even between sweeper ticks.
  pruneStale(now);
  reevaluate();

  let total = 0;
  let dataPages = 0;
  for (const s of _sessions.values()) {
    total += 1;
    if (s.isDataPage) dataPages += 1;
  }
  return { totalViewers: total, dataViewers: dataPages, active: dataPages > 0 };
}

/** Total non-stale sessions (data + info pages). */
export function activeViewerCount(): number {
  pruneStale(Date.now());
  return _sessions.size;
}

/** Subset of activeViewerCount that triggers the "active" cadence. */
export function dataViewerCount(): number {
  pruneStale(Date.now());
  let n = 0;
  for (const s of _sessions.values()) if (s.isDataPage) n += 1;
  return n;
}

/** Snapshot for /api/collection/status. */
export function listSessions(): Array<{ id: string; session: PageSession }> {
  pruneStale(Date.now());
  return Array.from(_sessions.entries()).map(([id, session]) => ({ id, session }));
}

/** Start the periodic sweeper. Idempotent. */
export function start(intervalMs: number = DEFAULT_SWEEP_INTERVAL_MS): void {
  if (_sweepTimer) return;
  _sweepTimer = setInterval(() => {
    const before = _sessions.size;
    pruneStale(Date.now());
    if (_sessions.size !== before) {
      log.debug(
        { removed: before - _sessions.size, remaining: _sessions.size },
        'activityMode: sweeper pruned stale sessions',
      );
    }
    reevaluate();
  }, intervalMs);
  // Sweeper shouldn't keep the process alive on its own.
  _sweepTimer.unref?.();
}

export function stop(): void {
  if (_sweepTimer) {
    clearInterval(_sweepTimer);
    _sweepTimer = null;
  }
}

/**
 * Test helper: clear all in-memory state. Not exported on a public
 * surface for production code (callers can just stop() instead).
 */
export function _resetForTests(): void {
  _sessions.clear();
  _lastReportedActive = null;
}
