/**
 * Admin source-health module.
 *
 * Exposes a runtime view over the poller's per-source metrics. The success
 * and failure counts cover each source's last RECENT_POLLS polls, so there is
 * nothing to clear: an old run of failures ages out. Mirrors the
 * payload shape from python's `dashboard_admin_sources` (external_api_proxy.py
 * lines 18981-19038) without persisting anything to Postgres — the python
 * version flushed counters to a `source_health` table via a 60s flusher
 * thread, but on the Node side the runtime state is enough for the admin
 * panel and we can rebuild it on every restart.
 *
 * State buckets (`state` field on each row):
 *   - last_ok_at is null         → 'unknown'
 *   - consec_fails >= 5          → 'down'
 *   - age > hard threshold       → 'down'
 *   - consec_fails >= 2          → 'degraded'
 *   - age > soft threshold       → 'degraded'
 *   - otherwise                  → 'ok'
 *
 * Two things keep a healthy source from being called degraded:
 *   - the thresholds are never shorter than the source's own poll interval
 *     allows (see thresholdsFor). A source polled every 10 minutes, or once
 *     a day, was "degraded" or "down" for most of each cycle with no
 *     failure at all, because the default was 5 / 15 minutes;
 *   - one failed poll is not degradation. Several upstreams 404 or time out
 *     for a single poll and are back on the next.
 */
import {
  getSourceMetrics,
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
  traffic_alpine:    { soft: 1800, hard: 3600, label: 'LiveTraffic — alpine' },
  traffic_lga:       { soft: 1800, hard: 3600, label: 'LiveTraffic — council roads' },
  roads_qld:         { soft: 900,  hard: 2700, label: 'QLDTraffic road events' },
  roads_sa:          { soft: 1800, hard: 3600, label: 'SA roadworks, closures and outback roads' },
  roads_vic:         { soft: 900,  hard: 2700, label: 'VIC road disruptions' },
  roads_act:         { soft: 1800, hard: 3600, label: 'Roads ACT closures' },
  power_endeavour:   { soft: 1200, hard: 3600, label: 'Endeavour outages' },
  power_ausgrid:     { soft: 1800, hard: 5400, label: 'Ausgrid outages' },
  // The other states' distributors, under their registry names. Some are
  // read every 5 to 15 minutes, so the default 5 / 15 minutes would call a
  // healthy one degraded between polls.
  ausnet:            { soft: 900,  hard: 2700, label: 'AusNet outages' },
  citipower:         { soft: 900,  hard: 2700, label: 'CitiPower outages' },
  powercor:          { soft: 900,  hard: 2700, label: 'Powercor outages' },
  jemena:            { soft: 900,  hard: 2700, label: 'Jemena outages' },
  sapn:              { soft: 900,  hard: 2700, label: 'SA Power Networks outages' },
  westernpower:      { soft: 900,  hard: 2700, label: 'Western Power outages' },
  horizon:           { soft: 900,  hard: 2700, label: 'Horizon Power outages' },
  pager:             { soft: 1200, hard: 3600, label: 'Pagermon' },
  rdio:              { soft: 3900, hard: 12600, label: 'rdio-scanner' },
  // FIRMS NRT updates every few hours; loose thresholds match its cadence.
  firms_hotspots:    { soft: 3600, hard: 10800, label: 'NASA FIRMS hotspots' },
  act_ambulance:     { soft: 600,  hard: 1800, label: 'ACT Ambulance responses' },
  // ~24 reports a day nationally, polled every 5 min — a quiet stretch is
  // normal, so the thresholds are about staleness of the feed, not of events.
  dorsal_reports:    { soft: 900,  hard: 2700, label: 'Dorsal Watch shark reports' },
  // The rest of the registry, mostly for a readable label. Thresholds are
  // floored by the poll interval, so a slow source needs no entry of its own.
  rfs_incidents:         { soft: 600,  hard: 1800, label: 'RFS incidents' },
  bom_warnings:          { soft: 600,  hard: 1800, label: 'BOM warnings' },
  traffic_majorevent:    { soft: 1800, hard: 3600, label: 'LiveTraffic — major events' },
  endeavour_current:     { soft: 1200, hard: 3600, label: 'Endeavour Energy — current' },
  endeavour_planned:     { soft: 1200, hard: 3600, label: 'Endeavour Energy — planned' },
  endeavour_maintenance: { soft: 1200, hard: 3600, label: 'Endeavour Energy — maintenance' },
  ausgrid:               { soft: 1800, hard: 5400, label: 'Ausgrid outages' },
  essential_current:     { soft: 900,  hard: 2700, label: 'Essential Energy — unplanned' },
  essential_future:      { soft: 900,  hard: 2700, label: 'Essential Energy — future' },
  vic_emergency:         { soft: 600,  hard: 1800, label: 'VIC Emergency' },
  qld_fire:              { soft: 600,  hard: 1800, label: 'QLD Fire incidents' },
  qld_warning:           { soft: 600,  hard: 1800, label: 'QLD Fire warnings' },
  wa_incident:           { soft: 600,  hard: 1800, label: 'DFES (WA) incidents' },
  wa_warning:            { soft: 600,  hard: 1800, label: 'DFES (WA) warnings' },
  sa_cfs:                { soft: 600,  hard: 1800, label: 'SA CFS incidents' },
  sa_mfs:                { soft: 600,  hard: 1800, label: 'SA MFS incidents' },
  nt_fire:               { soft: 600,  hard: 1800, label: 'NT fire incidents' },
  user_incidents:        { soft: 300,  hard: 900,  label: 'User incidents' },
  adsb_aircraft:         { soft: 300,  hard: 900,  label: 'ADS-B aircraft' },
  weather_grid:          { soft: 300,  hard: 900,  label: 'Weather grid' },
  weather_marine:        { soft: 300,  hard: 900,  label: 'Weather grid — marine' },
  weather_flood:         { soft: 300,  hard: 900,  label: 'Weather grid — flood' },
  weather_air:           { soft: 300,  hard: 900,  label: 'Weather grid — air quality' },
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
  'qld_cameras',
  'qld_flood_cameras',
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
const DEGRADED_FAIL_THRESHOLD = 2;

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
  /** Successes and failures among the last `window` polls. */
  total_success: number;
  total_fail: number;
  window: number;
  /** Seconds since last success, or null if never successful. */
  age_seconds: number | null;
  state: SourceState;
}

/**
 * The thresholds for a source, never tighter than its poll interval allows:
 * "degraded" needs two polls to have been missed, "down" four. Without the
 * floor, anything polled less often than the table (or the 5 / 15 minute
 * default) assumed was stale between its own polls.
 */
function thresholdsFor(name: string, intervalS: number): ThresholdEntry {
  const base = SOURCE_THRESHOLDS[name] ?? {
    soft: DEFAULT_SOFT_S,
    hard: DEFAULT_HARD_S,
    label: name,
  };
  const every = Number.isFinite(intervalS) && intervalS > 0 ? intervalS : 0;
  return {
    label: base.label,
    soft: Math.max(base.soft, every * 2 + 60),
    hard: Math.max(base.hard, every * 4 + 60),
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
  if (metric.consec_fails >= DEGRADED_FAIL_THRESHOLD) return 'degraded';
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
    const cfg = thresholdsFor(m.name, m.interval_s);
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
      window: m.window,
      age_seconds: age,
      state: deriveState(m, age, cfg),
    });
  }
  return out;
}
