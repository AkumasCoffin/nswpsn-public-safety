/**
 * Registry of every upstream source the backend polls.
 *
 * Each entry declares:
 *   - `name`: stable identifier (also the LiveStore key)
 *   - `family`: maps to the archive table family (waze | traffic |
 *               rfs | power | misc) for when we wire ArchiveWriter
 *   - `intervalActiveMs` / `intervalIdleMs`: poll cadence in active vs
 *                                            idle mode (page-active
 *                                            heartbeat dictates which)
 *   - `fetch`: async function that returns a snapshot (whatever shape
 *              this source uses; LiveStore is opaque to it)
 *
 * services/poller.ts walks the registry and schedules each. The
 * activity-mode service flips intervals between active/idle when the
 * heartbeat tracker reports a state change.
 */
import type { ArchiveRow, ArchiveTable } from '../store/archive.js';

export type SourceFamily = 'waze' | 'traffic' | 'rfs' | 'power' | 'misc';

export interface SourceDefinition<T = unknown> {
  /** LiveStore key + log identifier. */
  name: string;
  /** Which archive_<family> table archive rows from this source go to. */
  family: SourceFamily;
  /** Active mode (someone watching a page). Mirrors python's
   *  PREWARM_ACTIVE_INTERVAL = 60s default. */
  intervalActiveMs: number;
  /** Idle mode (no active page). Mirrors PREWARM_IDLE_INTERVAL = 120s. */
  intervalIdleMs: number;
  /** Returns the snapshot to store in LiveStore. Throws on upstream
   *  failure (poller catches, increments failure counter, applies
   *  backoff). */
  fetch: () => Promise<T>;
  /**
   * Override for the `source` value written to archive_* rows. Defaults
   * to `name` when omitted. Use this when the LiveStore key (which other
   * Node-side code reads) differs from the canonical python source value
   * the migration backfilled with — e.g. the LiveStore key is
   * `rfs_incidents` but archive rows must be tagged `rfs` to match
   * historical data and the SOURCE_TO_FAMILY lookup the data-history
   * route uses. Without this, /api/data/history?source=rfs returns 0
   * rows because the new poller-written rows used the LiveStore key.
   */
  archiveSource?: string;
  /**
   * Optional override for the archive fan-out. When omitted, the poller
   * uses `defaultArchiveItems` (which handles GeoJSON FeatureCollections
   * and flat arrays). Sources whose snapshot shape is neither — e.g. the
   * pager source returns `{ messages: [...], count: N }` — must provide
   * this so each message becomes its own archive row instead of the
   * whole snapshot landing as a single "Unknown" wrapper row.
   */
  archiveItems?: (data: unknown, fetched_at: number, source: string) => ArchiveRow[];
}

const _registry = new Map<string, SourceDefinition>();

export function registerSource<T>(def: SourceDefinition<T>): void {
  _registry.set(def.name, def as SourceDefinition);
}

export function getSource(name: string): SourceDefinition | undefined {
  return _registry.get(name);
}

export function allSources(): SourceDefinition[] {
  return Array.from(_registry.values());
}

/** Convenience for archive_<family> table name lookup. */
export function familyTable(family: SourceFamily): ArchiveTable {
  switch (family) {
    case 'waze':
      return 'archive_waze';
    case 'traffic':
      return 'archive_traffic';
    case 'rfs':
      return 'archive_rfs';
    case 'power':
      return 'archive_power';
    case 'misc':
      return 'archive_misc';
  }
}
