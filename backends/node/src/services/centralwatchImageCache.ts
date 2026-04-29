/**
 * Central Watch image cache + batch refresh worker.
 *
 * Mirrors python `_centralwatch_image_cache` and `_continuous_cw_image_worker`
 * (external_api_proxy.py:8563-9022).
 *
 * Cache shape:
 *   Map<cameraId, { data: Buffer, contentType: string, ts: number(ms) }>
 *
 * The HTTP route for /api/centralwatch/image/:id reads from this Map and
 * never triggers a fetch — that's the batch worker's job. Everything is
 * cache-only on the request path so on-demand requests don't compete with
 * the batch worker for the upstream rate-limit budget.
 *
 * Cleanup runs as part of every batch loop: entries older than 5 min get
 * dropped, as do entries for cameras that fell out of the active list.
 */
import { log } from '../lib/log.js';
import { centralwatchBrowser } from './centralwatchBrowser.js';
import { getCentralwatchCameras } from '../sources/centralwatch.js';

export interface CachedImage {
  data: Buffer;
  contentType: string;
  ts: number;
}

const MAX_AGE_MS = 5 * 60 * 1000; // 5 min — matches python _CENTRALWATCH_IMAGE_MAX_AGE eviction
const STALE_AFTER_MS = 2 * 60 * 1000; // 2 min — the X-Cache: HIT vs STALE threshold
const BATCH_INTERVAL_MS = 30 * 1000; // 30 s between batch passes
const MIN_IMAGE_BYTES = 500; // python: anything smaller is treated as a 1x1/error pixel

// Two-phase strategy state — mirrors python `use_dom` / `last_dom_retry`
// (external_api_proxy.py:8763-8766). DOM-load via <img> sends
// `Sec-Fetch-Dest: image` and is generally not rate-limited; page-context
// fetch() sends `Sec-Fetch-Dest: empty` and is. Start optimistically with
// DOM. If the success rate falls below 40% we switch to fetch, but we
// retry the other path every 5 min in case the limiter has reset.
const DOM_SUCCESS_THRESHOLD = 0.4; // python: _CW_DOM_SUCCESS_THRESHOLD
const STRATEGY_RETRY_INTERVAL_MS = 5 * 60 * 1000; // python: 300s (last_dom_retry > 300)

type Strategy = 'dom' | 'fetch';
let lastStrategy: Strategy = 'dom';
let lastStrategyRetryAt = 0;

const cache = new Map<string, CachedImage>();

let batchTimer: NodeJS.Timeout | null = null;
let batchInFlight = false;
let stopRequested = false;

export function setImage(
  cameraId: string,
  data: Buffer,
  contentType: string,
  ts: number = Date.now(),
): void {
  cache.set(cameraId, { data, contentType, ts });
}

export function getImage(cameraId: string): CachedImage | undefined {
  return cache.get(cameraId);
}

export function hasImage(cameraId: string): boolean {
  return cache.has(cameraId);
}

export function cacheSize(): number {
  return cache.size;
}

/**
 * Evict cache entries that are either too old or no longer in the active
 * camera id set. Mirrors python `_cleanup_centralwatch_image_cache`.
 *
 * If `activeIds` is empty / undefined, the active-id check is skipped
 * (python's behaviour: don't blow away the cache just because the
 * camera list happens to be unloaded for a moment).
 */
export function cleanup(
  activeIds?: ReadonlySet<string> | null,
  now: number = Date.now(),
): { evicted: number; remaining: number } {
  let evicted = 0;
  for (const [id, entry] of cache.entries()) {
    const tooOld = now - entry.ts > MAX_AGE_MS;
    const notActive = activeIds && activeIds.size > 0 && !activeIds.has(id);
    if (tooOld || notActive) {
      cache.delete(id);
      evicted++;
    }
  }
  return { evicted, remaining: cache.size };
}

/**
 * Build the upstream image URL for a camera.
 *
 * Central Watch's image endpoint requires an ISO timestamp in the path.
 * Python tries (now-2min) first because the latest image isn't always
 * indexed yet; we mirror that here.
 */
function buildImageUrl(cameraId: string): string {
  const t = new Date(Date.now() - 2 * 60 * 1000);
  // python: '%Y-%m-%dT%H:%M:%S.000Z'
  const ts = `${t.getUTCFullYear()}-${pad2(t.getUTCMonth() + 1)}-${pad2(
    t.getUTCDate(),
  )}T${pad2(t.getUTCHours())}:${pad2(t.getUTCMinutes())}:${pad2(t.getUTCSeconds())}.000Z`;
  return `https://centralwatch.watchtowers.io/au/api/cameras/${cameraId}/image/${ts}`;
}

function pad2(n: number): string {
  return n < 10 ? `0${n}` : `${n}`;
}

/**
 * Walk the active camera list, batch-fetch all images, populate the cache,
 * then evict stale + non-active entries.
 */
export async function runBatchOnce(): Promise<{
  attempted: number;
  cached: number;
  evicted: number;
}> {
  if (!centralwatchBrowser.isReady()) {
    return { attempted: 0, cached: 0, evicted: 0 };
  }
  let cameras: Array<{ id: string }> = [];
  try {
    cameras = (await getCentralwatchCameras()).map((c) => ({ id: c.id }));
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'centralwatch image batch: failed to load camera list',
    );
    return { attempted: 0, cached: 0, evicted: 0 };
  }

  const activeIds = new Set(cameras.map((c) => c.id));
  if (cameras.length === 0) {
    const { evicted } = cleanup(activeIds);
    return { attempted: 0, cached: 0, evicted };
  }

  const inputs = cameras.map((c) => ({ id: c.id, url: buildImageUrl(c.id) }));
  let cached = 0;

  // Decide which path to try first. Python: start with DOM; once we've
  // flipped to fetch, retry DOM every 5 min in case the limiter resets.
  // Symmetrically, if we're on DOM, periodically reconsider — but DOM is
  // the cheaper / less-rate-limited path so the bias is to stay on it.
  const now = Date.now();
  const dueForRetry = now - lastStrategyRetryAt > STRATEGY_RETRY_INTERVAL_MS;
  const tryDomFirst = lastStrategy === 'dom' || dueForRetry;
  let domPath: 'dom' | 'fetch' | null = null;
  const cachedOk = new Set<string>();

  if (tryDomFirst) {
    domPath = 'dom';
    try {
      const results = await centralwatchBrowser.fetchImagesBatchViaDom(inputs);
      for (const r of results) {
        if (
          r.ok &&
          r.id &&
          r.bytes &&
          r.bytes.length > MIN_IMAGE_BYTES &&
          activeIds.has(r.id)
        ) {
          setImage(r.id, r.bytes, r.contentType ?? 'image/jpeg');
          cached++;
          cachedOk.add(r.id);
        }
      }
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'centralwatch image batch: fetchImagesBatchViaDom threw',
      );
    }

    const successRate = inputs.length > 0 ? cached / inputs.length : 0;
    if (successRate >= DOM_SUCCESS_THRESHOLD) {
      lastStrategy = 'dom';
    } else {
      // DOM degraded — flip the next-pass default to fetch, and fall
      // through *now* to retry the cameras DOM didn't get via fetch().
      lastStrategy = 'fetch';
      const failedInputs = inputs.filter((i) => !cachedOk.has(i.id));
      if (failedInputs.length > 0) {
        try {
          const results = await centralwatchBrowser.fetchImagesBatch(failedInputs);
          for (const r of results) {
            if (
              r.ok &&
              r.id &&
              r.bytes &&
              r.bytes.length > MIN_IMAGE_BYTES &&
              activeIds.has(r.id)
            ) {
              setImage(r.id, r.bytes, r.contentType ?? 'image/jpeg');
              cached++;
              cachedOk.add(r.id);
            }
          }
          domPath = 'fetch';
        } catch (err) {
          log.warn(
            { err: (err as Error).message },
            'centralwatch image batch: fetchImagesBatch fallback threw',
          );
        }
      }
    }
    lastStrategyRetryAt = now;
  } else {
    // Default-fetch mode (we're cooling on DOM); 5min retry timer hasn't
    // come up yet, so go straight to the fetch() path.
    domPath = 'fetch';
    try {
      const results = await centralwatchBrowser.fetchImagesBatch(inputs);
      for (const r of results) {
        if (
          r.ok &&
          r.id &&
          r.bytes &&
          r.bytes.length > MIN_IMAGE_BYTES &&
          activeIds.has(r.id)
        ) {
          setImage(r.id, r.bytes, r.contentType ?? 'image/jpeg');
          cached++;
          cachedOk.add(r.id);
        }
      }
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'centralwatch image batch: fetchImagesBatch threw',
      );
    }
  }

  const { evicted, remaining } = cleanup(activeIds);
  // Demoted to debug — 30 s cadence drowns the log otherwise. The
  // info-level warning still fires on every fetch failure inside the
  // browser worker, which is the only signal worth surfacing here.
  log.debug(
    `centralwatch image batch: ${cached}/${inputs.length} cached, ${evicted} evicted, size=${remaining} (${domPath}/${lastStrategy})`,
  );
  return { attempted: inputs.length, cached, evicted };
}

export function startCentralwatchImageBatchLoop(): void {
  if (batchTimer) return;
  if (process.env['CENTRALWATCH_DISABLED'] === 'true') {
    log.info('centralwatch image batch loop disabled via env');
    return;
  }
  stopRequested = false;
  const tick = async (): Promise<void> => {
    if (stopRequested) return;
    if (batchInFlight) return;
    batchInFlight = true;
    try {
      await runBatchOnce();
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'centralwatch image batch loop tick failed',
      );
    } finally {
      batchInFlight = false;
    }
  };
  batchTimer = setInterval(() => void tick(), BATCH_INTERVAL_MS);
  // Kick off the first tick on the next event loop turn so init can
  // finish wiring up before we hit the browser.
  setImmediate(() => void tick());
}

export function stopCentralwatchImageBatchLoop(): void {
  stopRequested = true;
  if (batchTimer) {
    clearInterval(batchTimer);
    batchTimer = null;
  }
}

export const STALE_AFTER_MS_EXPORT = STALE_AFTER_MS;

/** Test hooks. */
export function _resetCentralwatchImageCacheForTests(): void {
  cache.clear();
  if (batchTimer) {
    clearInterval(batchTimer);
    batchTimer = null;
  }
  batchInFlight = false;
  stopRequested = false;
  lastStrategy = 'dom';
  lastStrategyRetryAt = 0;
}

/** Test hook — peek at strategy state. */
export function _getStrategyStateForTests(): {
  lastStrategy: Strategy;
  lastStrategyRetryAt: number;
} {
  return { lastStrategy, lastStrategyRetryAt };
}

/** Test hook — force-set the strategy state for retry-timer tests. */
export function _setStrategyStateForTests(
  s: Strategy,
  retryAt = 0,
): void {
  lastStrategy = s;
  lastStrategyRetryAt = retryAt;
}
