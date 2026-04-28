/**
 * LiveStore — the live half of the new architecture.
 *
 * Holds the *current* state of every source in memory. All `/api/*` live
 * endpoints (waze/police, rfs/incidents, traffic/incidents, etc.) read
 * from here — never from Postgres. That separation is the point: the
 * old Python backend put live state and archive in the same `data_history`
 * table with `is_live`/`is_latest` UPDATE columns, and the resulting
 * write contention was the root cause of every cascade we fought today.
 *
 * Disk persistence is per-source under {STATE_DIR}/<source>.json. Atomic
 * via tempfile + rename so we never see a partial file. On startup,
 * `hydrateFromDisk()` repopulates the map.
 *
 * Each source's snapshot is opaque to the store — it's whatever shape
 * the source poller decides. Only requirement is that it's
 * JSON-serializable. Source-specific code (e.g. WazeIngestCache) wraps
 * LiveStore for richer semantics like bbox-keyed sub-snapshots.
 */
import { mkdir, readFile, rename, writeFile, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { config } from '../config.js';
import { log } from '../lib/log.js';

export interface LiveSnapshot<T = unknown> {
  /** Wall-clock when this snapshot was written (epoch seconds). */
  ts: number;
  /** The source-specific payload. */
  data: T;
}

export class LiveStore {
  private map = new Map<string, LiveSnapshot>();
  private dirty = new Set<string>();
  private persistTimer: NodeJS.Timeout | null = null;
  private dir: string;
  // Hit/miss counters for /api/status. A "hit" is a get() that found
  // a snapshot; "miss" is a get() returning null. Excludes internal
  // walks (keys() iteration, etc.). Reset on process restart.
  private hits = 0;
  private misses = 0;

  constructor(dir: string = config.STATE_DIR) {
    this.dir = dir;
  }

  /**
   * Read all *.json files from STATE_DIR and load them into the map.
   * Called once at startup. Failures on individual files are logged
   * and skipped — partial hydration is better than failing to start.
   */
  async hydrateFromDisk(): Promise<{ loaded: number; failed: number }> {
    let loaded = 0;
    let failed = 0;
    try {
      await mkdir(this.dir, { recursive: true });
    } catch {
      // Best effort — if the dir already exists, mkdir is fine.
    }
    let entries: string[];
    try {
      entries = await readdir(this.dir);
    } catch (err) {
      log.warn({ err, dir: this.dir }, 'LiveStore: state dir unreadable');
      return { loaded: 0, failed: 0 };
    }
    for (const f of entries) {
      if (!f.endsWith('.json')) continue;
      const source = f.slice(0, -5);
      try {
        const raw = await readFile(join(this.dir, f), 'utf8');
        const parsed = JSON.parse(raw) as LiveSnapshot;
        // Defensive: ignore obviously malformed entries.
        if (typeof parsed?.ts === 'number') {
          this.map.set(source, parsed);
          loaded += 1;
        } else {
          failed += 1;
        }
      } catch (err) {
        log.warn({ err, file: f }, 'LiveStore: hydrate failed for file');
        failed += 1;
      }
    }
    log.info({ loaded, failed, dir: this.dir }, 'LiveStore hydrated');
    return { loaded, failed };
  }

  /** Set the snapshot for a source. Marks it dirty for the next persist tick. */
  set<T>(source: string, data: T): void {
    this.map.set(source, { ts: Math.floor(Date.now() / 1000), data });
    this.dirty.add(source);
  }

  /** Read the current snapshot for a source, or null if never set.
   *  Tracks hit/miss for /api/status ram_cache panel. */
  get<T = unknown>(source: string): LiveSnapshot<T> | null {
    const snap = this.map.get(source) as LiveSnapshot<T> | undefined;
    if (snap) this.hits += 1;
    else this.misses += 1;
    return snap ?? null;
  }

  /** Hit/miss counters for /api/status. Reset on process restart. */
  cacheStats(): { hits: number; misses: number; hit_rate_pct: number | null } {
    const total = this.hits + this.misses;
    return {
      hits: this.hits,
      misses: this.misses,
      hit_rate_pct: total > 0 ? Math.round((this.hits / total) * 1000) / 10 : null,
    };
  }

  /** Just the data payload, or null. Convenience for handlers. */
  getData<T = unknown>(source: string): T | null {
    return this.get<T>(source)?.data ?? null;
  }

  /** All source names currently tracked. Used by /api/health cache_keys. */
  keys(): string[] {
    return Array.from(this.map.keys());
  }

  /** Count of sources currently tracked. */
  size(): number {
    return this.map.size;
  }

  /** Remove a source. */
  delete(source: string): void {
    this.map.delete(source);
    this.dirty.add(source);
  }

  /**
   * Start the periodic persist loop. Returns the interval handle so the
   * caller can stop it on shutdown.
   */
  startPersistLoop(intervalMs: number = config.LIVE_PERSIST_INTERVAL_MS): void {
    if (this.persistTimer) return;
    this.persistTimer = setInterval(() => {
      void this.persistDirty();
    }, intervalMs);
    // unref so this timer doesn't keep the process alive on its own.
    this.persistTimer.unref?.();
  }

  /** Stop the persist loop and do one final flush. */
  async stopAndFlush(): Promise<void> {
    if (this.persistTimer) {
      clearInterval(this.persistTimer);
      this.persistTimer = null;
    }
    await this.persistDirty();
  }

  /**
   * Write each dirty source to disk via tempfile + rename. POSIX
   * guarantees the rename is atomic so readers never see a partial
   * file. Errors are logged but don't propagate — best-effort.
   */
  async persistDirty(): Promise<{ written: number; errors: number }> {
    if (this.dirty.size === 0) return { written: 0, errors: 0 };

    try {
      await mkdir(this.dir, { recursive: true });
    } catch {
      /* already exists */
    }

    // Snapshot the dirty set so concurrent writes don't race with us.
    const toWrite = Array.from(this.dirty);
    this.dirty.clear();

    let written = 0;
    let errors = 0;
    for (const source of toWrite) {
      const snap = this.map.get(source);
      if (!snap) continue; // deleted between mark and flush
      const finalPath = join(this.dir, `${source}.json`);
      const tempPath = `${finalPath}.${process.pid}.tmp`;
      try {
        await writeFile(tempPath, JSON.stringify(snap), 'utf8');
        await rename(tempPath, finalPath);
        written += 1;
      } catch (err) {
        log.error({ err, source }, 'LiveStore: persist failed');
        // Re-mark dirty so the next tick retries.
        this.dirty.add(source);
        errors += 1;
      }
    }
    return { written, errors };
  }
}

/** Process-wide singleton. */
export const liveStore = new LiveStore();
