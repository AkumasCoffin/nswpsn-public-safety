/**
 * Generic source poller — replaces python's `prewarm_loop`.
 *
 * Walks every registered source and runs it on its own setInterval.
 * Each source's failures are tracked independently; on consecutive
 * failure the next attempt waits backoffSeconds(failureCount) past the
 * normal interval. Success resets the counter.
 *
 * Activity-mode awareness: when activeMode flips, every source's
 * interval is reset to the matching cadence on the next tick.
 *
 * Why each source is its own setInterval rather than one big tick:
 *   - One slow source can't starve the others
 *   - Backoff per-source is simpler to reason about
 *   - Idle/active switch only re-arms the affected timers
 */
import { liveStore } from '../store/live.js';
import { archiveWriter } from '../store/archive.js';
import { log } from '../lib/log.js';
import { backoffSeconds } from '../sources/shared/backoff.js';
import { defaultArchiveItems } from './archiveExtract.js';
import {
  allSources,
  familyTable,
  type SourceDefinition,
} from './sourceRegistry.js';

// Sources that go to LiveStore for live serving but are NEVER archived
// to the per-family archive_* tables. Mirrors python's `skip_keys` at
// external_api_proxy.py:4424. These are reference / static data that
// don't represent incidents — including them would flood the logs
// page with camera locations and clog the archive tables.
const SKIP_ARCHIVE = new Set<string>([
  'traffic_cameras',
  'aviation_cameras',
  'centralwatch_cameras',
  'weather_current',
  'weather_radar',
  'ausgrid_stats',
  'beachsafe',
  'beachwatch',
]);

interface SourceState {
  timer: NodeJS.Timeout | null;
  failureCount: number;
  /** Wall-clock of last successful fetch (epoch seconds). */
  lastSuccessTs: number | null;
  /** Wall-clock of last attempt (success or failure). */
  lastAttemptTs: number | null;
  lastError: string | null;
}

let activeMode = true; // true = active interval, false = idle
const _state = new Map<string, SourceState>();
let _running = false;

/**
 * Toggle activity mode. Causes every source's next tick to use the
 * appropriate interval. Called by the heartbeat / activityMode service
 * when page-active state changes.
 */
export function setActivityMode(active: boolean): void {
  if (active === activeMode) return;
  activeMode = active;
  log.info({ active }, 'activity mode changed; rearming pollers');
  if (_running) {
    // Re-arm immediately so cadence reflects the new mode.
    for (const src of allSources()) {
      armOne(src);
    }
  }
}

export function isActiveMode(): boolean {
  return activeMode;
}

function intervalFor(src: SourceDefinition): number {
  return activeMode ? src.intervalActiveMs : src.intervalIdleMs;
}

function ensureState(name: string): SourceState {
  let s = _state.get(name);
  if (!s) {
    s = {
      timer: null,
      failureCount: 0,
      lastSuccessTs: null,
      lastAttemptTs: null,
      lastError: null,
    };
    _state.set(name, s);
  }
  return s;
}

async function runOnce(src: SourceDefinition): Promise<void> {
  const state = ensureState(src.name);
  const startedAt = Date.now();
  state.lastAttemptTs = Math.floor(startedAt / 1000);
  try {
    const data = await src.fetch();
    liveStore.set(src.name, data);
    const fetchedAt = Math.floor(Date.now() / 1000);
    // Mirror to archive (append-only) UNLESS this source is in the
    // skip list (cameras, static metadata, etc.). Each archived poll
    // snapshot is fanned out into one row PER INCIDENT so
    // /api/data/history rows get title/severity/etc. projected from
    // JSONB the same way python's data_history did.
    if (!SKIP_ARCHIVE.has(src.name)) {
      const rows = defaultArchiveItems(src.name, data, fetchedAt);
      const tbl = familyTable(src.family);
      for (const row of rows) {
        archiveWriter.push(tbl, row);
      }
    }

    const wasFailing = state.failureCount > 0;
    state.failureCount = 0;
    state.lastSuccessTs = fetchedAt;
    state.lastError = null;
    if (wasFailing) {
      log.info({ source: src.name }, 'poll recovered');
    } else {
      log.debug(
        { source: src.name, ms: Date.now() - startedAt },
        'poll success',
      );
    }
  } catch (err) {
    state.failureCount += 1;
    const newErr = (err as Error).message;
    const errChanged = newErr !== state.lastError;
    state.lastError = newErr;
    // Log policy: warn on the FIRST failure (transition) and on every
    // 10th repeat thereafter — chatty upstreams (open-meteo 429,
    // beachsafe 422) used to fire every poll cycle, drowning the log
    // with no new signal. The poll itself still backs off via
    // backoffSeconds(state.failureCount) so cadence drops naturally.
    const shouldLogWarn =
      state.failureCount === 1 || errChanged || state.failureCount % 10 === 0;
    if (shouldLogWarn) {
      log.warn(
        {
          source: src.name,
          consec_fails: state.failureCount,
          err: newErr,
        },
        'poll failed',
      );
    } else {
      log.debug(
        {
          source: src.name,
          consec_fails: state.failureCount,
          err: newErr,
        },
        'poll failed (repeat)',
      );
    }
  }
}

function armOne(src: SourceDefinition): void {
  const state = ensureState(src.name);
  if (state.timer) {
    clearTimeout(state.timer);
    state.timer = null;
  }
  // Backoff added on top of the normal interval after consecutive
  // failures so an unhappy source doesn't spam its upstream every cycle.
  const backoffMs = backoffSeconds(state.failureCount) * 1000;
  const delayMs = intervalFor(src) + backoffMs;
  state.timer = setTimeout(() => {
    void runOnce(src).finally(() => {
      // Re-arm regardless of outcome so the next tick is scheduled.
      if (_running) armOne(src);
    });
  }, delayMs);
  state.timer.unref?.();
}

/** Start polling every registered source on its own interval. */
export function startPolling(initialDelayMs: number = 0): void {
  if (_running) return;
  _running = true;
  for (const src of allSources()) {
    // Stagger the first tick of each source so a fleet of pollers
    // doesn't all hit the DB / network at the same instant after boot.
    const stagger = Math.floor(Math.random() * 2000);
    const delay = initialDelayMs + stagger;
    setTimeout(() => {
      if (!_running) return;
      void runOnce(src).finally(() => {
        if (_running) armOne(src);
      });
    }, delay).unref?.();
  }
  log.info({ count: allSources().length }, 'pollers started');
}

export function stopPolling(): void {
  _running = false;
  for (const s of _state.values()) {
    if (s.timer) {
      clearTimeout(s.timer);
      s.timer = null;
    }
  }
}

/** Snapshot of poller health — used by /api/status and /api/collection/status. */
export function pollerHealth(): Record<
  string,
  {
    failure_count: number;
    last_success_age_secs: number | null;
    last_attempt_age_secs: number | null;
    last_error: string | null;
  }
> {
  const now = Math.floor(Date.now() / 1000);
  const out: Record<string, ReturnType<typeof entryFor>> = {};
  for (const [name, s] of _state) {
    out[name] = entryFor(s, now);
  }
  return out;

  function entryFor(s: SourceState, n: number) {
    return {
      failure_count: s.failureCount,
      last_success_age_secs: s.lastSuccessTs ? n - s.lastSuccessTs : null,
      last_attempt_age_secs: s.lastAttemptTs ? n - s.lastAttemptTs : null,
      last_error: s.lastError,
    };
  }
}

// ---------------------------------------------------------------------------
// Per-source metric getters used by the source-health module. These expose
// the same in-memory state used by `pollerHealth()` but in a richer shape
// (raw timestamps + family + last_error) — that lets the admin source-health
// view compute its own derived state. NOT changing existing exports.
// ---------------------------------------------------------------------------
export interface SourceMetricSnapshot {
  name: string;
  family: string;
  /** Epoch seconds. null = source has never succeeded since process start. */
  last_ok_at: number | null;
  /** Epoch seconds. null = source has never errored since process start. */
  last_error_at: number | null;
  /** Last error message, if any (cleared on next success). */
  last_error: string | null;
  consec_fails: number;
}

/** Returns one entry per registered source. Sources that have never been
 *  polled still appear with all timestamps null and consec_fails 0. */
export function getSourceMetrics(): SourceMetricSnapshot[] {
  const out: SourceMetricSnapshot[] = [];
  for (const src of allSources()) {
    const s = _state.get(src.name);
    out.push({
      name: src.name,
      family: src.family,
      last_ok_at: s?.lastSuccessTs ?? null,
      // The per-source state tracks last_attempt + last_error together;
      // when lastError is set, lastAttemptTs is the failure timestamp.
      last_error_at: s?.lastError ? (s.lastAttemptTs ?? null) : null,
      last_error: s?.lastError ?? null,
      consec_fails: s?.failureCount ?? 0,
    });
  }
  return out;
}

/** Reset every (or just one) source's failure/error counters. Used by the
 *  admin "Clear stats" button. Last-success timestamps are preserved so
 *  recently-OK sources don't suddenly appear "unknown". */
export function resetSourceMetrics(name?: string): void {
  if (name) {
    const s = _state.get(name);
    if (s) {
      s.failureCount = 0;
      s.lastError = null;
    }
    return;
  }
  for (const s of _state.values()) {
    s.failureCount = 0;
    s.lastError = null;
  }
}

/** TEST-ONLY: seed one source's state directly so source-health tests can
 *  observe specific shapes without driving the full polling lifecycle. */
export function _seedSourceStateForTests(
  name: string,
  patch: Partial<SourceState>,
): void {
  const s = ensureState(name);
  Object.assign(s, patch);
}

/** TEST-ONLY: reset the in-memory map. */
export function _resetPollerStateForTests(): void {
  for (const s of _state.values()) {
    if (s.timer) clearTimeout(s.timer);
  }
  _state.clear();
  _running = false;
}
