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
import { procMemorySummary } from './procMemory.js';

// 5 min by default. MEMORY_SAMPLE_SECS turns it down while actively chasing a
// leak — a 60s cadence during firefighting is worth the extra log volume, and
// nobody wants to redeploy to change a sampling rate.
/**
 * Percent-of-limit thresholds at which to write a heap snapshot. Comma
 * separated; each fires at most once. Off unless MEMORY_SNAPSHOT_AT_PCT is set.
 *
 * Prefer two (MEMORY_SNAPSHOT_AT_PCT=35,70) over one. Chrome DevTools can
 * compare two snapshots and list exactly what was allocated between them and
 * never freed, which names a leak far more directly than reading retained
 * sizes off a single dump.
 *
 * --heapsnapshot-near-heap-limit (see ecosystem.config.js) fires at the very
 * edge, where V8 is already thrashing and the write can fail for want of the
 * memory to serialise it. Firing deliberately at, say, 85% catches the same
 * retainers with room to actually produce the file.
 *
 * The snapshot is roughly heap-sized (~2GB here), so this is opt-in and
 * one-shot: it exists to answer a specific question, not to run forever.
 */
const SNAPSHOT_AT_PCT = (process.env['MEMORY_SNAPSHOT_AT_PCT'] ?? '')
  .split(',')
  .map((x) => Number(x.trim()))
  .filter((n) => Number.isFinite(n) && n > 0)
  .sort((a, b) => a - b);
/** Thresholds already used, so each fires at most once. */
const snapshotsTaken = new Set<number>();

const SAMPLE_INTERVAL_MS = Math.max(
  10_000,
  (Number(process.env['MEMORY_SAMPLE_SECS']) || 300) * 1000,
);
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

/**
 * Write a heap snapshot to STATE_DIR. Synchronous and slow (seconds, and it
 * stops the world) — acceptable because it happens at most once, and only when
 * the process is already heading for an OOM that would stop it permanently.
 */
async function writeHeapSnapshot(threshold: number, pctOfLimit: number): Promise<void> {
  if (snapshotsTaken.has(threshold)) return;
  snapshotsTaken.add(threshold); // first: a failed attempt must not retry every sample
  try {
    const v8 = await import('node:v8');
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const path = join(config.STATE_DIR, `heap-${stamp}-${pctOfLimit}pct.heapsnapshot`);
    log.warn({ path, pctOfLimit }, 'memory: writing heap snapshot (one-shot) — this pauses the process');
    const written = v8.writeHeapSnapshot(path);
    log.warn({ written }, 'memory: heap snapshot written — open it in Chrome DevTools > Memory');
  } catch (err) {
    log.error({ err }, 'memory: heap snapshot failed');
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

    // Whole-box attribution on every sample: the JS heap is capped at ~2GB by
    // V8, so it cannot by itself explain the VM going from 6GB to 10GB. This
    // says which processes hold the rest — Chromium in particular, which
    // process.memoryUsage() is blind to.
    const box = await procMemorySummary();
    const withBox = box
      ? {
          ...base,
          // memAvailable is the figure to trust; sumRss double-counts shared
          // pages and is only meaningful as a trend against earlier samples.
          memAvailableMB: box.memAvailableMB,
          memTotalMB: box.memTotalMB,
          sumRssMB: box.sumRssMB,
          byProcess: box.groups,
        }
      : base;

    // Checked before the early return below: the snapshot threshold is
    // configured independently, and nesting it under DETAIL_THRESHOLD would
    // mean any value under 75 silently never fired.
    // Each configured threshold fires once. Two of them (e.g. "35,70") is the
    // point: DevTools can diff two snapshots and show precisely which objects
    // grew between them, which is a far more direct answer than eyeballing
    // retained sizes in a single dump — and the first file arrives hours
    // sooner than one taken near the ceiling.
    for (const threshold of SNAPSHOT_AT_PCT) {
      if (pct * 100 >= threshold) await writeHeapSnapshot(threshold, Math.round(pct * 100));
    }

    if (pct < DETAIL_THRESHOLD) {
      log.info(withBox, 'memory');
      return;
    }

    log.warn(
      { ...withBox, archiveQueue: archiveWriter.metrics().queue_size, snapshots: await largestSnapshots() },
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
