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

// Central Watch now gates each camera image behind a per-camera "view token":
// POST /au/api/cameras/{id}/view-token → { token, exp } (exp in unix seconds,
// ~10 min TTL), and the token is sent as the `x-wt-token` header on the image
// GET (which also requires a 15-second-aligned timestamp). We cache tokens per
// camera and re-mint shortly before expiry. The old DOM-vs-fetch two-phase
// strategy is gone — the DOM <img> path can't set the required header, so only
// the fetch() path works now.
const TOKEN_RENEW_MARGIN_S = 60; // re-mint a token this many seconds before its exp
const tokenCache = new Map<string, { token: string; exp: number }>();

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
 * Build the upstream image URL for a camera. Central Watch serves frames on a
 * 15-second grid and 403s any timestamp that isn't 15s-aligned, so we floor to
 * the previous 15s boundary (a little in the past so the frame is indexed).
 */
function buildImageUrl(cameraId: string): string {
  const alignedSec = Math.floor((Date.now() / 1000 - 30) / 15) * 15;
  const t = new Date(alignedSec * 1000);
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

  // Ensure a fresh view token for every active camera — mint the ones we don't
  // hold or that are about to expire. The image GET 403s without it.
  const nowSec = Math.floor(Date.now() / 1000);
  const needToken = cameras
    .map((c) => c.id)
    .filter((id) => {
      const t = tokenCache.get(id);
      return !t || t.exp - nowSec <= TOKEN_RENEW_MARGIN_S;
    });
  if (needToken.length > 0) {
    try {
      const minted = await centralwatchBrowser.mintViewTokens(needToken);
      for (const [id, v] of Object.entries(minted)) tokenCache.set(id, v);
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'centralwatch image batch: view-token mint failed',
      );
    }
  }
  // Forget tokens for cameras no longer in the active list.
  for (const id of [...tokenCache.keys()]) {
    if (!activeIds.has(id)) tokenCache.delete(id);
  }

  // Only fetch cameras we hold a token for; the endpoint 403s without it.
  const inputs = cameras
    .map((c) => {
      const tok = tokenCache.get(c.id);
      return tok ? { id: c.id, url: buildImageUrl(c.id), token: tok.token } : null;
    })
    .filter((x): x is { id: string; url: string; token: string } => x !== null);

  let cached = 0;
  if (inputs.length > 0) {
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
  }

  const { evicted, remaining } = cleanup(activeIds);
  log.debug(
    `centralwatch image batch: ${cached}/${inputs.length} cached, ${tokenCache.size} tokens, ${evicted} evicted, size=${remaining}`,
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
  tokenCache.clear();
  if (batchTimer) {
    clearInterval(batchTimer);
    batchTimer = null;
  }
  batchInFlight = false;
  stopRequested = false;
}
