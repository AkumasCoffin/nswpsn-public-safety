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
      }
    }
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'centralwatch image batch: fetchImagesBatch threw',
    );
  }

  const { evicted, remaining } = cleanup(activeIds);
  log.info(
    {
      attempted: inputs.length,
      cached,
      evicted,
      cacheSize: remaining,
    },
    'centralwatch image batch complete',
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
}
