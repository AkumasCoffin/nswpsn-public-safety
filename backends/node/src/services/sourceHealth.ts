/**
 * Admin source-health module.
 *
 * Exposes a runtime view over the poller's per-source metrics, plus a
 * clear operation backing the admin "Clear stats" button. Mirrors the
 * payload shape from python's `dashboard_admin_sources` (external_api_proxy.py
 * lines 18981-19038) without persisting anything to Postgres — the python
 * version flushed counters to a `source_health` table via a 60s flusher
 * thread, but on the Node side the runtime state is enough for the admin
 * panel and we can rebuild it on every restart.
 *
 * State buckets (`state` field on each row) follow the python rules:
 *   - last_ok_at is null         → 'unknown'
 *   - consec_fails >= 5          → 'down'
 *   - age > hard threshold       → 'down'
 *   - consec_fails > 0           → 'degraded'
 *   - age > soft threshold       → 'degraded'
 *   - otherwise                  → 'ok'
 */
import {
  getSourceMetrics,
  resetSourceMetrics,
  type SourceMetricSnapshot,
} from './poller.js';

// Per-source soft/hard age thresholds + UI label. Mirrors python's
// _SOURCE_THRESHOLDS dict (external_api_proxy.py:308-334). Sources whose
// upstream is slow / rate-limited get more headroom.
//
// Keys here are the *node* source names from the registry. They don't
// have to match python 1:1 — anything the registry surfaces but we don't
// have a threshold for falls back to (soft=300, hard=900), same default
// the python code uses.
interface ThresholdEntry { soft: number; hard: number; label: string }
const SOURCE_THRESHOLDS: Record<string, ThresholdEntry> = {
  rfs:               { soft: 600,  hard: 1800, label: 'RFS incidents' },
  bom:               { soft: 600,  hard: 1800, label: 'BOM warnings' },
  traffic_incidents: { soft: 600,  hard: 1800, label: 'LiveTraffic — incidents' },
  traffic_roadwork:  { soft: 1800, hard: 3600, label: 'LiveTraffic — roadwork' },
  traffic_flood:     { soft: 1800, hard: 3600, label: 'LiveTraffic — flood' },
  traffic_fire:      { soft: 1800, hard: 3600, label: 'LiveTraffic — fire' },
  traffic_major:     { soft: 1800, hard: 3600, label: 'LiveTraffic — major events' },
  power_endeavour:   { soft: 1200, hard: 3600, label: 'Endeavour outages' },
  power_ausgrid:     { soft: 1800, hard: 5400, label: 'Ausgrid outages' },
  waze:              { soft: 600,  hard: 1800, label: 'Waze' },
  pager:             { soft: 1200, hard: 3600, label: 'Pagermon' },
  rdio:              { soft: 3900, hard: 12600, label: 'rdio-scanner' },
  // FIRMS NRT updates every few hours; loose thresholds match its cadence.
  firms_hotspots:    { soft: 3600, hard: 10800, label: 'NASA FIRMS hotspots' },
};

// Reference / static data sources that are polled for the live map but
// don't represent alertable incidents. They're shown nowhere in the bot's
// alert flow, so listing them in the admin "Data sources" health panel just
// adds noise (and surfaces raw snake_case names with no friendly label).
// Mirror of the poller's SKIP_ARCHIVE set (services/poller.ts) — the same
// "not an incident feed" class — kept as an explicit local copy so the
// health panel owns its own display policy.
const NON_INCIDENT_SOURCES = new Set<string>([
  'traffic_cameras',
  'aviation_cameras',
  'centralwatch_cameras',
  'weather_current',
  'weather_radar',
  'ausgrid_stats',
  'beachsafe',
  'beachwatch',
]);

const DEFAULT_SOFT_S = 300;
const DEFAULT_HARD_S = 900;
const DOWN_FAIL_THRESHOLD = 5;

export type SourceState = 'unknown' | 'ok' | 'degraded' | 'down';

export interface SourceHealthRow {
  name: string;
  family: string;
  label: string;
  /** Epoch seconds, or null if never succeeded. */
  last_ok_at: number | null;
  /** Epoch seconds, or null if never errored. */
  last_error_at: number | null;
  last_error: string | null;
  consec_fails: number;
  total_success: number;
  total_fail: number;
  /** Seconds since last success, or null if never successful. */
  age_seconds: number | null;
  state: SourceState;
}

function thresholdsFor(name: string): ThresholdEntry {
  return SOURCE_THRESHOLDS[name] ?? {
    soft: DEFAULT_SOFT_S,
    hard: DEFAULT_HARD_S,
    label: name,
  };
}

function deriveState(
  metric: SourceMetricSnapshot,
  ageSecs: number | null,
  cfg: ThresholdEntry,
): SourceState {
  if (metric.last_ok_at === null) return 'unknown';
  if (metric.consec_fails >= DOWN_FAIL_THRESHOLD) return 'down';
  if (ageSecs !== null && ageSecs > cfg.hard) return 'down';
  if (metric.consec_fails > 0) return 'degraded';
  if (ageSecs !== null && ageSecs > cfg.soft) return 'degraded';
  return 'ok';
}

/**
 * Snapshot of every registered source's health. Sources that have never
 * been polled appear with state='unknown'.
 *
 * Pure function over the poller's in-memory state — no I/O. Safe to call
 * from a request handler.
 */
export function getSourceHealthSnapshot(): SourceHealthRow[] {
  const now = Math.floor(Date.now() / 1000);
  const out: SourceHealthRow[] = [];
  for (const m of getSourceMetrics()) {
    // Skip reference/static feeds (cameras, radar tiles, beach data) — they
    // produce no alerts, so they don't belong in the alert source-health panel.
    if (NON_INCIDENT_SOURCES.has(m.name)) continue;
    const cfg = thresholdsFor(m.name);
    const age = m.last_ok_at ? now - m.last_ok_at : null;
    out.push({
      name: m.name,
      family: m.family,
      label: cfg.label,
      last_ok_at: m.last_ok_at,
      last_error_at: m.last_error_at,
      last_error: m.last_error,
      consec_fails: m.consec_fails,
      total_success: m.total_success,
      total_fail: m.total_fail,
      age_seconds: age,
      state: deriveState(m, age, cfg),
    });
  }
  return out;
}

/** Wipe failure/error counters. Backs the admin "Clear stats" button. */
export function clearSourceErrors(name?: string): void {
  resetSourceMetrics(name);
}
