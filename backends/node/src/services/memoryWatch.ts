/**
 * Heap telemetry.
 *
 * Added after a production OOM (~18.6h uptime) that could not be diagnosed
 * from the logs: the process died inside a JSON.stringify with ~1.93GB live
 * after a full mark-compact, and there was no memory history to say whether
 * that had crept up over hours or spiked in seconds. The two readings imply
 * completely different causes, so the first job is to stop guessing.
 *
 * Two things happen here:
 *
 *   - A periodic gauge, so the shape of the curve is on record before the
 *     next event rather than reconstructed after it.
 *   - Above a high-water mark, a fuller dump: the largest LiveStore payloads
 *     (read as FILE SIZES from STATE_DIR — the store already persists each
 *     source there, so this costs a stat() rather than serialising a
 *     multi-hundred-MB object to measure it, which would itself be a
 *     meaningful allocation at exactly the wrong moment) plus the archive
 *     queue depth.
 *
 * The archive queue is worth watching specifically because it is bounded by
 * ROW COUNT (50,000) and not by bytes, so its memory ceiling depends entirely
 * on how fat the rows are.
 *
 * This does not fix anything. It makes the next occurrence diagnosable.
 * For the definitive answer, run with --heapsnapshot-near-heap-limit=1 (see
 * backends/ecosystem.config.js) and open the resulting snapshot in Chrome
 * DevTools.
 */
import { readdir, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { archiveWriter } from '../store/archive.js';

const SAMPLE_INTERVAL_MS = 5 * 60_000;
/** Fraction of the heap limit above which we log the fuller breakdown. */
const DETAIL_THRESHOLD = 0.75;

let timer: NodeJS.Timeout | null = null;
let peakHeapMB = 0;

const mb = (bytes: number): number => Math.round(bytes / 1048576);

/**
 * The V8 old-space ceiling this process will actually die at. Read from
 * v8.getHeapStatistics() rather than assumed, because it depends on
 * --max-old-space-size and on how much RAM the box reported at startup.
 */
async function heapLimitMB(): Promise<number> {
  try {
    const v8 = await import('node:v8');
    return mb(v8.getHeapStatistics().heap_size_limit);
  } catch {
    return 0;
  }
}

/** Largest persisted source snapshots, as {source, mb}, biggest first. */
async function largestSnapshots(n = 5): Promise<Array<{ source: string; mb: number }>> {
  try {
    const dir = config.STATE_DIR;
    const files = (await readdir(dir)).filter((f) => f.endsWith('.json'));
    const sized = await Promise.all(
      files.map(async (f) => {
        try {
          const s = await stat(join(dir, f));
          return { source: f.slice(0, -5), mb: +(s.size / 1048576).toFixed(2) };
        } catch {
          return null;
        }
      }),
    );
    return sized
      .filter((x): x is { source: string; mb: number } => x !== null)
      .sort((a, b) => b.mb - a.mb)
      .slice(0, n);
  } catch {
    return [];
  }
}

/** One sample. Never throws — telemetry must not be able to take the process down. */
export async function sampleMemoryOnce(): Promise<void> {
  try {
    const m = process.memoryUsage();
    const limit = await heapLimitMB();
    const used = mb(m.heapUsed);
    if (used > peakHeapMB) peakHeapMB = used;
    const pct = limit > 0 ? used / limit : 0;

    const base = {
      heapUsedMB: used,
      heapTotalMB: mb(m.heapTotal),
      heapLimitMB: limit,
      pctOfLimit: Math.round(pct * 100),
      peakHeapMB,
      rssMB: mb(m.rss),
      // Buffers and ArrayBuffers live OUTSIDE the old-space heap, so they
      // can't cause the mark-compact OOM — but they can still exhaust the
      // box, and separating them tells the two failures apart.
      externalMB: mb(m.external),
      arrayBuffersMB: mb(m.arrayBuffers),
    };

    if (pct < DETAIL_THRESHOLD) {
      log.info(base, 'memory');
      return;
    }

    log.warn(
      { ...base, archiveQueue: archiveWriter.metrics().queue_size, snapshots: await largestSnapshots() },
      'memory: heap above threshold',
    );
  } catch (err) {
    log.debug({ err }, 'memory: sample failed');
  }
}

/** Start periodic sampling. Idempotent. */
export function startMemoryWatch(intervalMs: number = SAMPLE_INTERVAL_MS): void {
  if (timer) return;
  // A sample shortly after boot gives the baseline every later reading is
  // compared against.
  setTimeout(() => void sampleMemoryOnce(), 30_000).unref?.();
  timer = setInterval(() => void sampleMemoryOnce(), intervalMs);
  timer.unref?.();
}

export function stopMemoryWatch(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}
