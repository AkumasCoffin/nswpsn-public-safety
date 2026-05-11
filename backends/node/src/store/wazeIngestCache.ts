/**
 * WazeIngestCache — bbox-keyed live cache for Waze.
 *
 * Wraps LiveStore for richer semantics. The plain LiveStore has one
 * snapshot per source; Waze needs **per-bbox** snapshots because the
 * userscript rotates through ~190 regions and each region is its own
 * piece of the live picture. We merge all per-bbox snapshots into a
 * single deduplicated alert/jam list at read time.
 *
 * Replaces the Python `_waze_ingest_cache` dict and the surrounding
 * `_waze_ingest_lock` + `_waze_ingest_snapshot()` helpers.
 *
 * Stale entries (older than WAZE_INGEST_MAX_AGE_SECS) are pruned on
 * every read so a userscript that crashed with stale Sydney-only data
 * doesn't keep that data alive forever.
 */
import { config } from '../config.js';
import { liveStore } from './live.js';
import {
  bboxKeyStr,
  type WazeAlert,
  type WazeBboxSnapshot,
  type WazeIngestPayload,
  type WazeJam,
} from '../types/waze.js';

/** What we serialise into LiveStore under the synthetic 'waze' source. */
interface WazeStoreShape {
  bboxes: Record<string, WazeBboxSnapshot>;
  /** Wall-clock of the most recent ingest (any bbox). 0 if never. */
  last_ingest_ts: number;
}

function emptyShape(): WazeStoreShape {
  return { bboxes: {}, last_ingest_ts: 0 };
}

// Process-lifetime counters (not persisted — these are diagnostic only).
// last_ingest_age_secs alone is uninformative because the userscript
// hits /api/waze/ingest every ~5s, so age is always near zero. Total
// + 60s rate make the dev panel actually convey activity.
let totalIngests = 0;
const recentIngestTs: number[] = [];
const RATE_WINDOW_SECS = 60;

function readShape(): WazeStoreShape {
  return liveStore.getData<WazeStoreShape>('waze') ?? emptyShape();
}

function writeShape(shape: WazeStoreShape): void {
  liveStore.set('waze', shape);
}

/** Save an ingest payload from the userscript. */
export function ingest(payload: WazeIngestPayload): {
  bboxKey: string;
  regions: number;
  alerts: number;
  jams: number;
} {
  const key = bboxKeyStr(payload.bbox);
  const ts = Math.floor(Date.now() / 1000);
  const shape = readShape();
  shape.bboxes[key] = {
    alerts: payload.alerts,
    jams: payload.jams,
    users: payload.users,
    ts,
    bbox: payload.bbox,
  };
  shape.last_ingest_ts = ts;
  writeShape(shape);
  // Update counters. Trim the rate window so the last_60s computation
  // doesn't grow unbounded on a process that's been up for days.
  totalIngests += 1;
  recentIngestTs.push(ts);
  const cutoff = ts - RATE_WINDOW_SECS;
  while (recentIngestTs.length > 0 && recentIngestTs[0]! < cutoff) {
    recentIngestTs.shift();
  }
  return {
    bboxKey: key,
    regions: Object.keys(shape.bboxes).length,
    alerts: payload.alerts.length,
    jams: payload.jams.length,
  };
}

/** Diagnostic counters surfaced via /api/status → checks.waze_ingest. */
export function ingestStats(): {
  total_ingests: number;
  ingests_per_min: number;
} {
  // recentIngestTs is trimmed on push, but trim again here in case
  // ingest stopped firing — otherwise the rate would lag behind reality.
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - RATE_WINDOW_SECS;
  while (recentIngestTs.length > 0 && recentIngestTs[0]! < cutoff) {
    recentIngestTs.shift();
  }
  return {
    total_ingests: totalIngests,
    ingests_per_min: recentIngestTs.length,
  };
}

/**
 * Fold the per-bbox snapshots into one deduplicated alert + jam list.
 * Called by the live waze GET endpoints.
 *
 * Stale bboxes (last seen > WAZE_INGEST_MAX_AGE_SECS ago) are pruned
 * during the merge so callers never see ancient data.
 */
export function snapshot(): {
  alerts: WazeAlert[];
  jams: WazeJam[];
  regions_cached: number;
  last_ingest_age_secs: number | null;
} {
  const shape = readShape();
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - config.WAZE_INGEST_MAX_AGE_SECS;

  // Prune stale bbox entries (write back if anything changed).
  let pruned = 0;
  for (const [k, snap] of Object.entries(shape.bboxes)) {
    if (snap.ts < cutoff) {
      delete shape.bboxes[k];
      pruned += 1;
    }
  }
  if (pruned > 0) writeShape(shape);

  // Merge by uuid/id; first occurrence wins (consistent with python).
  const allAlerts = new Map<string, WazeAlert>();
  const allJams = new Map<string, WazeJam>();
  for (const snap of Object.values(shape.bboxes)) {
    for (const a of snap.alerts) {
      const id = a.uuid ?? a.id;
      if (id && !allAlerts.has(id)) allAlerts.set(id, a);
    }
    for (const j of snap.jams) {
      const id = j.uuid ?? j.id;
      if (id && !allJams.has(id)) allJams.set(id, j);
    }
  }

  return {
    alerts: Array.from(allAlerts.values()),
    jams: Array.from(allJams.values()),
    regions_cached: Object.keys(shape.bboxes).length,
    last_ingest_age_secs: shape.last_ingest_ts
      ? now - shape.last_ingest_ts
      : null,
  };
}
