/**
 * Facet aggregation for /api/data/history/filters.
 *
 * Replaces Python's `data_history_filter_cache` table that was refreshed
 * every 5 min via 5×GROUP BY scans of `data_history`. Hybrid model:
 *
 * 1. **Non-waze sources** (rfs / power / traffic / misc) come from a
 *    GROUP BY scan over the small archive_* tables on a 5-min loop.
 *    Bounded to the last 24h of rows so the dropdown reflects what the
 *    user would actually see when filtering. archive_misc has hundreds
 *    of pager hits per day; the LiveStore-only walk used to surface
 *    just the latest poll's 100 messages and showed "Pager: 1".
 * 2. **Waze** stays on the in-memory path — archive_waze is the only
 *    multi-million-row table and waze categorisation is hardcoded
 *    (`wazeAlertType()`) so the live snapshot is sufficient.
 *
 * Response shape exactly matches Python's `_build_filters_response`:
 *   {
 *     providers: [{
 *       key, name, icon, color, count,
 *       types: [{ alert_type, name, count, categories, subcategories,
 *                 statuses, severities }]
 *     }],
 *     date_range: { oldest, newest, oldest_unix, newest_unix }
 *   }
 */
import { liveStore } from './live.js';
import { log } from '../lib/log.js';
import { getPool } from '../db/pool.js';
import type { ArchiveTable } from './archive.js';
import { sydneyIsoFromUnix } from '../lib/sydneyTime.js';

// ---------------------------------------------------------------------------
// Source / provider mapping — mirrors external_api_proxy.py:1114-1196
// ---------------------------------------------------------------------------

const RAW_SOURCE_TO_ALERT_TYPE: Record<string, string> = {
  // python canonical source names (also what migration 006 + the
  // archiveSource overrides write into archive_*).
  rfs: 'rfs',
  bom_marine: 'bom_marine',
  bom_land: 'bom_land',
  bom_warning: 'bom_land',
  bom: 'bom_land',
  traffic_incident: 'traffic_incident',
  traffic_roadwork: 'traffic_roadwork',
  traffic_flood: 'traffic_flood',
  traffic_fire: 'traffic_fire',
  traffic_majorevent: 'traffic_majorevent',
  livetraffic: 'traffic_incident',
  endeavour_current: 'endeavour_current',
  endeavour_planned: 'endeavour_planned',
  endeavour: 'endeavour_current',
  ausgrid: 'ausgrid',
  essential_current: 'essential_planned',
  essential_planned: 'essential_planned',
  essential_future: 'essential_future',
  essential: 'essential_planned',
  waze_hazard: 'waze_hazard',
  waze_jam: 'waze_jam',
  waze_police: 'waze_police',
  waze_roadwork: 'waze_roadwork',
  pager: 'pager',
  // LiveStore keys (the Node poller's source name) → alert_type. Without
  // these, the filter facet computation walks liveStore.keys() and
  // returns 0 for RFS, BoM, traffic_incidents because their LiveStore
  // keys don't match any alert_type. Symptom: logs.html dropdown shows
  // 0 incidents even when those tables are receiving fresh data.
  rfs_incidents: 'rfs',
  bom_warnings: 'bom_land',
  traffic_incidents: 'traffic_incident',
  // Single-key 'waze' is the WazeIngestCache shape — handled specially
  // in computeFacets() because its bbox-keyed snapshot needs to be
  // unpacked into per-type counts (police/hazard/roadwork/jam).
  waze: 'waze_hazard',
};

const ALERT_TYPE_PROVIDER: Record<string, [string, string]> = {
  rfs: ['rfs', 'Major Incidents'],
  bom_land: ['bom', 'Land Warnings'],
  bom_marine: ['bom', 'Marine Warnings'],
  traffic_incident: ['livetraffic', 'Incidents'],
  traffic_roadwork: ['livetraffic', 'Roadwork'],
  traffic_flood: ['livetraffic', 'Flooding'],
  traffic_fire: ['livetraffic', 'Fires'],
  traffic_majorevent: ['livetraffic', 'Major Events'],
  endeavour_current: ['endeavour', 'Current Outages'],
  endeavour_planned: ['endeavour', 'Planned Outages'],
  ausgrid: ['ausgrid', 'Outages'],
  essential_planned: ['essential', 'Planned Outages'],
  essential_future: ['essential', 'Future Outages'],
  waze_hazard: ['waze', 'Hazards'],
  waze_jam: ['waze', 'Traffic Jams'],
  waze_police: ['waze', 'Police'],
  waze_roadwork: ['waze', 'Roadwork'],
  pager: ['pager', 'Messages'],
  user_incident: ['user', 'User Incidents'],
  radio_summary: ['rdio', 'Hourly Summaries'],
};

const PROVIDER_DISPLAY: Record<string, { name: string; icon: string; color: string }> = {
  rfs: { name: 'NSW Rural Fire Service', icon: 'fire', color: '#ef4444' },
  bom: { name: 'Bureau of Meteorology', icon: 'cloud', color: '#3b82f6' },
  livetraffic: { name: 'LiveTraffic NSW', icon: 'road', color: '#f97316' },
  endeavour: { name: 'Endeavour Energy', icon: 'bolt', color: '#fbbf24' },
  ausgrid: { name: 'Ausgrid', icon: 'plug', color: '#f59e0b' },
  essential: { name: 'Essential Energy', icon: 'bolt', color: '#06b6d4' },
  waze: { name: 'Waze', icon: 'car', color: '#00d4ff' },
  pager: { name: 'Pager', icon: 'pager', color: '#8b5cf6' },
  user: { name: 'NSW PSN User Submissions', icon: 'user', color: '#a855f7' },
  rdio: { name: 'Radio Scanner', icon: 'radio', color: '#10b981' },
};

const PROVIDER_ORDER = [
  'rfs',
  'bom',
  'livetraffic',
  'endeavour',
  'ausgrid',
  'essential',
  'waze',
  'pager',
  'user',
  'rdio',
];

const PROVIDER_TYPE_ORDER: Record<string, string[]> = {
  bom: ['bom_land', 'bom_marine'],
  livetraffic: [
    'traffic_incident',
    'traffic_roadwork',
    'traffic_flood',
    'traffic_fire',
    'traffic_majorevent',
  ],
  endeavour: ['endeavour_current', 'endeavour_planned'],
  essential: ['essential_planned', 'essential_future'],
  waze: ['waze_hazard', 'waze_jam', 'waze_police', 'waze_roadwork'],
};

const DEPRECATED_SOURCES = new Set<string>(['essential_energy_cancelled']);

export function canonicalAlertType(rawSource: string | null | undefined): string | null {
  if (!rawSource) return null;
  return RAW_SOURCE_TO_ALERT_TYPE[rawSource] ?? rawSource;
}

// ---------------------------------------------------------------------------
// Facet shape
// ---------------------------------------------------------------------------

export interface FacetEntry {
  value: string;
  count: number;
}

export interface TypeFacets {
  alert_type: string;
  name: string;
  count: number;
  categories: FacetEntry[];
  subcategories: FacetEntry[];
  statuses: FacetEntry[];
  severities: FacetEntry[];
}

export interface ProviderFacets {
  key: string;
  name: string;
  icon: string;
  color: string;
  count: number;
  types: TypeFacets[];
}

export interface FilterFacets {
  providers: ProviderFacets[];
  date_range: {
    oldest: string | null;
    newest: string | null;
    oldest_unix: number | null;
    newest_unix: number | null;
  };
}

// ---------------------------------------------------------------------------
// Facet computation
// ---------------------------------------------------------------------------

/** Per-(alert_type, dimension) value->count map. */
type DimMap = Record<string, Record<string, Record<string, number>>>;

function bumpCount(target: Record<string, number>, key: string): void {
  target[key] = (target[key] ?? 0) + 1;
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function readDim(record: Record<string, unknown>, field: string): string | null {
  const v = record[field];
  if (v === null || v === undefined) return null;
  if (typeof v === 'string') {
    const t = v.trim();
    return t === '' ? null : t;
  }
  if (typeof v === 'number' || typeof v === 'boolean') {
    return String(v);
  }
  return null;
}

/**
 * Walk a snapshot payload and count one record. Most LiveStore snapshots
 * are arrays of records (e.g. `endeavour_current` is an array of
 * outages), some are objects with feature collections (e.g. waze is an
 * object with `alerts`/`jams`). We handle both shapes — anything else
 * counts as a single "snapshot" record contributing 1 to the type total.
 */
function recordsFromSnapshot(data: unknown): Record<string, unknown>[] {
  if (Array.isArray(data)) {
    return data.filter(isPlainObject) as Record<string, unknown>[];
  }
  if (isPlainObject(data)) {
    // GeoJSON-ish: { features: [...] }
    if (Array.isArray(data['features'])) {
      return (data['features'] as unknown[]).filter(isPlainObject) as Record<
        string,
        unknown
      >[];
    }
    // Waze-ish: { alerts: [...], jams: [...] } — flatten both.
    const merged: Record<string, unknown>[] = [];
    if (Array.isArray(data['alerts'])) {
      for (const a of data['alerts'] as unknown[]) {
        if (isPlainObject(a)) merged.push(a);
      }
    }
    if (Array.isArray(data['jams'])) {
      for (const j of data['jams'] as unknown[]) {
        if (isPlainObject(j)) merged.push(j);
      }
    }
    if (merged.length > 0) return merged;
    // Single object snapshot — count as one record.
    return [data];
  }
  return [];
}

/**
 * Pull the dimension fields out of an individual record. Records can
 * have these fields at top level (legacy normalised payloads) or
 * inside a nested `properties` blob (GeoJSON features); check both.
 */
function dimsFromRecord(rec: Record<string, unknown>): {
  category: string | null;
  subcategory: string | null;
  status: string | null;
  severity: string | null;
} {
  const props =
    isPlainObject(rec['properties']) ? (rec['properties'] as Record<string, unknown>) : null;
  const pick = (field: string): string | null => {
    return readDim(rec, field) ?? (props ? readDim(props, field) : null);
  };
  return {
    category: pick('category'),
    subcategory: pick('subcategory'),
    status: pick('status'),
    severity: pick('severity'),
  };
}

interface ComputedFacets {
  /** alert_type -> total record count */
  typeCounts: Record<string, number>;
  /** alert_type -> dim -> value -> count */
  perTypeDims: DimMap;
  /** Min/Max liveStore snapshot timestamp (epoch seconds). */
  oldestUnix: number | null;
  newestUnix: number | null;
}

/**
 * Categorise a single waze alert into one of the four alert_types we
 * surface in the filter dropdown. Mirrors the isPoliceAlert /
 * isRoadworkAlert / isHazardAlert helpers in services/wazeAlerts —
 * duplicated here to avoid a cross-module cycle (filterCache shouldn't
 * import from a route's service layer).
 */
function wazeAlertType(rec: Record<string, unknown>): string {
  const t = String(rec['type'] ?? '').toUpperCase();
  const s = String(rec['subtype'] ?? '').toUpperCase();
  if (t === 'POLICE' || s.includes('POLICE')) return 'waze_police';
  if (t === 'CONSTRUCTION' || s.includes('CONSTRUCTION')) return 'waze_roadwork';
  return 'waze_hazard';
}

/** Pull all alerts + jams from the bbox-keyed waze LiveStore snapshot,
 *  deduplicated by uuid/id. Mirrors store/wazeIngestCache.snapshot(). */
function wazeRecords(data: unknown): {
  alerts: Record<string, unknown>[];
  jams: Record<string, unknown>[];
} {
  if (!isPlainObject(data)) return { alerts: [], jams: [] };
  const bboxes = (data['bboxes'] as Record<string, unknown> | undefined) ?? {};
  if (!isPlainObject(bboxes)) return { alerts: [], jams: [] };
  const alertsById = new Map<string, Record<string, unknown>>();
  const jamsById = new Map<string, Record<string, unknown>>();
  for (const snap of Object.values(bboxes)) {
    if (!isPlainObject(snap)) continue;
    const a = Array.isArray(snap['alerts']) ? (snap['alerts'] as unknown[]) : [];
    for (const rec of a) {
      if (!isPlainObject(rec)) continue;
      const id = String(rec['uuid'] ?? rec['id'] ?? '');
      if (!id || alertsById.has(id)) continue;
      alertsById.set(id, rec);
    }
    const j = Array.isArray(snap['jams']) ? (snap['jams'] as unknown[]) : [];
    for (const rec of j) {
      if (!isPlainObject(rec)) continue;
      const id = String(rec['uuid'] ?? rec['id'] ?? '');
      if (!id || jamsById.has(id)) continue;
      jamsById.set(id, rec);
    }
  }
  return { alerts: Array.from(alertsById.values()), jams: Array.from(jamsById.values()) };
}

function dimSlotFor(
  perTypeDims: DimMap,
  alertType: string,
): Record<string, Record<string, number>> {
  return (perTypeDims[alertType] ??= {
    category: {},
    subcategory: {},
    status: {},
    severity: {},
  });
}

/** Full sync LiveStore walk — counts every registered source's current
 *  snapshot. Used as a synchronous fallback (cold start, tests without
 *  DB). The async archive scan replaces the non-waze counts when it
 *  completes. */
function computeFacetsLive(): ComputedFacets {
  const typeCounts: Record<string, number> = {};
  const perTypeDims: DimMap = {};
  let oldestUnix: number | null = null;
  let newestUnix: number | null = null;

  for (const source of liveStore.keys()) {
    if (DEPRECATED_SOURCES.has(source)) continue;
    const snap = liveStore.get(source);
    if (!snap) continue;
    if (oldestUnix === null || snap.ts < oldestUnix) oldestUnix = snap.ts;
    if (newestUnix === null || snap.ts > newestUnix) newestUnix = snap.ts;

    if (source === 'waze') {
      const { alerts, jams } = wazeRecords(snap.data);
      for (const a of alerts) {
        const at = wazeAlertType(a);
        typeCounts[at] = (typeCounts[at] ?? 0) + 1;
        const slot = dimSlotFor(perTypeDims, at);
        const sub = String(a['subtype'] ?? '').toUpperCase();
        if (sub) bumpCount(slot['subcategory'] as Record<string, number>, sub);
        const cat = String(a['type'] ?? '').toUpperCase();
        if (cat) bumpCount(slot['category'] as Record<string, number>, cat);
      }
      typeCounts['waze_jam'] = (typeCounts['waze_jam'] ?? 0) + jams.length;
      continue;
    }

    const alertType = canonicalAlertType(source);
    if (!alertType) continue;
    if (!ALERT_TYPE_PROVIDER[alertType]) continue;

    const records = recordsFromSnapshot(snap.data);
    typeCounts[alertType] = (typeCounts[alertType] ?? 0) + records.length;

    const slot = dimSlotFor(perTypeDims, alertType);
    for (const rec of records) {
      const dims = dimsFromRecord(rec);
      if (dims.category) bumpCount(slot['category'] as Record<string, number>, dims.category);
      if (dims.subcategory) {
        // Pager numeric capcodes are noise — drop just like Python's filter
        // cache refresh does (line 12635-12636).
        if (!/^\d+$/.test(dims.subcategory)) {
          bumpCount(slot['subcategory'] as Record<string, number>, dims.subcategory);
        }
      }
      if (dims.status) bumpCount(slot['status'] as Record<string, number>, dims.status);
      if (dims.severity) bumpCount(slot['severity'] as Record<string, number>, dims.severity);
    }
  }

  return { typeCounts, perTypeDims, oldestUnix, newestUnix };
}

/** Waze-only LiveStore walk. Used during the archive merge: archive
 *  scan covers non-waze; this fills the waze types from LiveStore. */
function liveWazeFacets(): ComputedFacets {
  const typeCounts: Record<string, number> = {};
  const perTypeDims: DimMap = {};
  let oldestUnix: number | null = null;
  let newestUnix: number | null = null;

  const snap = liveStore.get('waze');
  if (!snap) return { typeCounts, perTypeDims, oldestUnix, newestUnix };
  oldestUnix = snap.ts;
  newestUnix = snap.ts;

  const { alerts, jams } = wazeRecords(snap.data);
  for (const a of alerts) {
    const at = wazeAlertType(a);
    typeCounts[at] = (typeCounts[at] ?? 0) + 1;
    const slot = dimSlotFor(perTypeDims, at);
    const sub = String(a['subtype'] ?? '').toUpperCase();
    if (sub) bumpCount(slot['subcategory'] as Record<string, number>, sub);
    const cat = String(a['type'] ?? '').toUpperCase();
    if (cat) bumpCount(slot['category'] as Record<string, number>, cat);
  }
  typeCounts['waze_jam'] = (typeCounts['waze_jam'] ?? 0) + jams.length;
  return { typeCounts, perTypeDims, oldestUnix, newestUnix };
}

/**
 * Tables we GROUP BY for facet counts and the per-table lookback
 * window. archive_waze is deliberately excluded — too big to scan,
 * and waze counts come from LiveStore.
 *
 * Per-table windows: archive_traffic + archive_rfs see ~140k+
 * rows/24h; a 7d GROUP BY scan times out at 60s. archive_misc +
 * archive_power are smaller AND host the sparse sources (BOM in
 * particular can go days between warnings) so they get the wider
 * window so their dropdown categories stay populated.
 */
interface FacetTable {
  table: ArchiveTable;
  windowDays: number;
}
const ARCHIVE_FACET_TABLES: FacetTable[] = [
  { table: 'archive_misc', windowDays: 7 },
  { table: 'archive_power', windowDays: 7 },
  { table: 'archive_traffic', windowDays: 1 },
  { table: 'archive_rfs', windowDays: 1 },
];

interface ArchiveFacetRow {
  source: string;
  category: string | null;
  subcategory: string | null;
  cnt: string;
  oldest: number | null;
  newest: number | null;
}

/** Window for the incremental facets read. Mirrors the legacy
 *  ARCHIVE_FACET_TABLES windows but applies a single bound to the
 *  pre-aggregated read. 7 days is the wider of the two legacy bounds;
 *  the smaller (1d) was only there to keep the live GROUP BY scan
 *  fast on large partitions. The pre-aggregated table makes that moot. */
const FACETS_WINDOW_DAYS = 7;

/**
 * GROUP BY scan over the small archive_* tables. Bounded to the last
 * 7d so quiet sources (BOM warnings can go days without firing,
 * Ausgrid maintenance is sparse) still surface category options in
 * the dropdown. 24h was too tight — BOM in particular regularly
 * showed `count: 0` with empty categories.
 *
 * Earlier revisions GROUPED BY status + severity (extracted from
 * `data->>` JSONB), which forced per-row JSONB parses for every row
 * in the window — on heavily-loaded archive_traffic that timed out at
 * 30s. Now we only GROUP BY the indexed columns (source, category,
 * subcategory) and skip status/severity facets in the dropdown. Worth
 * the trade: the dropdown's primary use is per-source + per-category
 * filtering; status/severity were nice-to-have.
 */
/**
 * Preferred read path: aggregate from the archive_*_latest sidecar
 * tables (migration 017). Each sidecar row is a unique incident, so
 * counts here match what /api/data/history?unique=1 returns to the
 * frontend list. Joining back to the parent picks up the latest
 * row's category/subcategory for the per-type dim breakdown.
 *
 * Replaces filter_facets_daily-based counting which summed every poll
 * snapshot — Ausgrid showed "1380" in the dropdown but the unique=1
 * list only had 3 rows because every outage was being polled ~460
 * times. Sidecar-driven counts can't drift like that.
 *
 * Returns null if the sidecar is empty (boot before backfill, or no
 * data in the window) so the caller can fall back to filter_facets_daily.
 */
async function archiveFacetsFromSidecar(): Promise<{
  typeCounts: Record<string, number>;
  perTypeDims: DimMap;
  oldestUnix: number | null;
  newestUnix: number | null;
} | null> {
  const pool = await getPool();
  if (!pool) return null;

  const tables: ArchiveTable[] = [
    'archive_misc',
    'archive_power',
    'archive_traffic',
    'archive_rfs',
    // archive_waze deliberately excluded — waze facets still come from
    // LiveStore (the in-memory wazeIngestCache); including the sidecar
    // here would double-count.
  ];

  const typeCounts: Record<string, number> = {};
  const perTypeDims: DimMap = {};
  let oldestUnix: number | null = null;
  let newestUnix: number | null = null;

  for (const table of tables) {
    // Window on the upstream source's own publish timestamp with
    // last_seen_at fallback (same semantics the unique=1 list uses
    // — see dataHistoryQuery.ts buildUniqueQuery). Backend ingest
    // times deliberately not used here; the user means "incidents
    // published in the last 7 days", not "rows we polled in the last
    // 7 days".
    const sql = `
      SELECT a.source,
             a.category,
             a.subcategory,
             COUNT(*)::text AS cnt,
             MIN(COALESCE(l.source_timestamp_unix,
                          EXTRACT(EPOCH FROM l.latest_fetched_at)::bigint))::bigint AS oldest,
             MAX(COALESCE(l.source_timestamp_unix,
                          EXTRACT(EPOCH FROM l.latest_fetched_at)::bigint))::bigint AS newest
        FROM ${table}_latest l
        JOIN ${table} a
          ON a.source = l.source
         AND a.source_id = l.source_id
         AND a.fetched_at = l.latest_fetched_at
       WHERE COALESCE(l.source_timestamp_unix,
                      EXTRACT(EPOCH FROM l.last_seen_at)::bigint)
             >= EXTRACT(EPOCH FROM NOW() - ($1 || ' days')::interval)::bigint
       GROUP BY 1, 2, 3
    `;
    let client;
    try {
      client = await pool.connect();
    } catch (err) {
      log.warn({ err: (err as Error).message, table }, 'filterCache (sidecar): pool acquire failed');
      continue;
    }
    try {
      await client.query('BEGIN');
      try {
        await client.query("SET LOCAL statement_timeout = '30s'");
        const result = await client.query<ArchiveFacetRow>(sql, [String(FACETS_WINDOW_DAYS)]);
        await client.query('COMMIT');
        for (const row of result.rows) {
          const cnt = parseInt(row.cnt, 10);
          if (!Number.isFinite(cnt) || cnt <= 0) continue;
          if (DEPRECATED_SOURCES.has(row.source)) continue;
          const alertType = canonicalAlertType(row.source);
          if (!alertType || !ALERT_TYPE_PROVIDER[alertType]) continue;
          if (alertType.startsWith('waze_')) continue;

          typeCounts[alertType] = (typeCounts[alertType] ?? 0) + cnt;
          const slot = dimSlotFor(perTypeDims, alertType);
          if (row.category) {
            slot['category']![row.category] = (slot['category']![row.category] ?? 0) + cnt;
          }
          if (row.subcategory && !/^\d+$/.test(row.subcategory)) {
            slot['subcategory']![row.subcategory] =
              (slot['subcategory']![row.subcategory] ?? 0) + cnt;
          }
          if (typeof row.oldest === 'number') {
            if (oldestUnix === null || row.oldest < oldestUnix) oldestUnix = row.oldest;
          }
          if (typeof row.newest === 'number') {
            if (newestUnix === null || row.newest > newestUnix) newestUnix = row.newest;
          }
        }
      } catch (err) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        log.warn({ err: (err as Error).message, table }, 'filterCache (sidecar): query failed');
      }
    } finally {
      client.release();
    }
  }

  // If every table came up empty (sidecar not populated yet) hand back
  // null so the caller falls through to the legacy paths.
  if (Object.keys(typeCounts).length === 0) return null;
  return { typeCounts, perTypeDims, oldestUnix, newestUnix };
}

/**
 * Fast read path: aggregate filter_facets_daily over the 7-day window.
 * Replaces the per-table GROUP BY scans that timed out at 60s. Returns
 * null if the table is empty (i.e. backfill hasn't run yet) so the
 * caller can fall back to the legacy archiveFacetsLegacy() path.
 */
async function archiveFacetsFromDaily(): Promise<{
  typeCounts: Record<string, number>;
  perTypeDims: DimMap;
  oldestUnix: number | null;
  newestUnix: number | null;
} | null> {
  const pool = await getPool();
  if (!pool) return null;
  const sql = `
    SELECT source,
           category,
           subcategory,
           SUM(count)::bigint AS cnt,
           EXTRACT(EPOCH FROM MIN(day))::bigint AS oldest,
           EXTRACT(EPOCH FROM MAX(day) + INTERVAL '1 day')::bigint AS newest
      FROM filter_facets_daily
     WHERE day >= (NOW() - ($1 || ' days')::interval)::date
     GROUP BY 1, 2, 3
  `;
  let rows: ArchiveFacetRow[];
  try {
    const r = await pool.query<ArchiveFacetRow>(sql, [String(FACETS_WINDOW_DAYS)]);
    rows = r.rows;
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'filterCache: facets-daily query failed; falling back to legacy scan',
    );
    return null;
  }
  if (rows.length === 0) return null;

  const typeCounts: Record<string, number> = {};
  const perTypeDims: DimMap = {};
  let oldestUnix: number | null = null;
  let newestUnix: number | null = null;
  for (const row of rows) {
    const cnt = parseInt(row.cnt, 10);
    if (!Number.isFinite(cnt) || cnt <= 0) continue;
    if (DEPRECATED_SOURCES.has(row.source)) continue;
    const alertType = canonicalAlertType(row.source);
    if (!alertType || !ALERT_TYPE_PROVIDER[alertType]) continue;
    if (alertType.startsWith('waze_')) continue;

    typeCounts[alertType] = (typeCounts[alertType] ?? 0) + cnt;
    const slot = dimSlotFor(perTypeDims, alertType);
    if (row.category && row.category !== '') {
      slot['category']![row.category] = (slot['category']![row.category] ?? 0) + cnt;
    }
    if (row.subcategory && row.subcategory !== '' && !/^\d+$/.test(row.subcategory)) {
      slot['subcategory']![row.subcategory] =
        (slot['subcategory']![row.subcategory] ?? 0) + cnt;
    }
    if (typeof row.oldest === 'number') {
      if (oldestUnix === null || row.oldest < oldestUnix) oldestUnix = row.oldest;
    }
    if (typeof row.newest === 'number') {
      if (newestUnix === null || row.newest > newestUnix) newestUnix = row.newest;
    }
  }
  return { typeCounts, perTypeDims, oldestUnix, newestUnix };
}

async function archiveFacets(): Promise<{
  typeCounts: Record<string, number>;
  perTypeDims: DimMap;
  oldestUnix: number | null;
  newestUnix: number | null;
}> {
  // Preferred path: counts from the archive_*_latest sidecars so the
  // dropdown numbers match the unique=1 list. See migration 017.
  const fromSidecar = await archiveFacetsFromSidecar();
  if (fromSidecar) return fromSidecar;

  // Fallback: filter_facets_daily-based counts. These over-count
  // because the rollup is per-poll, not per-incident — but better
  // than empty dropdowns while the sidecar's backfilling.
  const fast = await archiveFacetsFromDaily();
  if (fast) return fast;

  const typeCounts: Record<string, number> = {};
  const perTypeDims: DimMap = {};
  let oldestUnix: number | null = null;
  let newestUnix: number | null = null;

  const pool = await getPool();
  if (!pool) return { typeCounts, perTypeDims, oldestUnix, newestUnix };

  for (const { table, windowDays } of ARCHIVE_FACET_TABLES) {
    const sql = `
      SELECT
        source,
        category,
        subcategory,
        COUNT(*)::text AS cnt,
        EXTRACT(EPOCH FROM MIN(fetched_at))::bigint AS oldest,
        EXTRACT(EPOCH FROM MAX(fetched_at))::bigint AS newest
      FROM ${table}
      WHERE fetched_at >= NOW() - ($1 || ' days')::interval
      GROUP BY 1, 2, 3
    `;
    let client;
    try {
      client = await pool.connect();
    } catch (err) {
      log.warn({ err: (err as Error).message, table }, 'filterCache: pool acquire failed');
      continue;
    }
    try {
      await client.query('BEGIN');
      try {
        // 60s — bumped from 30s after seeing the 24h GROUP BY exceed
        // 30s on heavily-loaded archive_traffic. The refresh runs every
        // 5 min; long-tail single failures are tolerable but timing out
        // every cycle is not.
        await client.query("SET LOCAL statement_timeout = '60s'");
        const result = await client.query<ArchiveFacetRow>(sql, [String(windowDays)]);
        await client.query('COMMIT');
        for (const row of result.rows) {
          const cnt = parseInt(row.cnt, 10);
          if (!Number.isFinite(cnt) || cnt <= 0) continue;
          if (DEPRECATED_SOURCES.has(row.source)) continue;
          const alertType = canonicalAlertType(row.source);
          if (!alertType || !ALERT_TYPE_PROVIDER[alertType]) continue;
          // Skip waze types — they come from LiveStore, including them
          // here would double-count.
          if (alertType.startsWith('waze_')) continue;

          typeCounts[alertType] = (typeCounts[alertType] ?? 0) + cnt;
          const slot = dimSlotFor(perTypeDims, alertType);
          if (row.category) {
            slot['category']![row.category] = (slot['category']![row.category] ?? 0) + cnt;
          }
          if (row.subcategory && !/^\d+$/.test(row.subcategory)) {
            slot['subcategory']![row.subcategory] =
              (slot['subcategory']![row.subcategory] ?? 0) + cnt;
          }
          // status/severity facets dropped — see archiveFacets() docstring.
          if (typeof row.oldest === 'number') {
            if (oldestUnix === null || row.oldest < oldestUnix) oldestUnix = row.oldest;
          }
          if (typeof row.newest === 'number') {
            if (newestUnix === null || row.newest > newestUnix) newestUnix = row.newest;
          }
        }
      } catch (err) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        log.warn({ err: (err as Error).message, table }, 'filterCache: query failed');
      }
    } finally {
      client.release();
    }
  }

  return { typeCounts, perTypeDims, oldestUnix, newestUnix };
}

/** Merge two ComputedFacets into one. Counts add; date range widens. */
function mergeFacets(a: ComputedFacets, b: ComputedFacets): ComputedFacets {
  const typeCounts: Record<string, number> = { ...a.typeCounts };
  for (const [k, v] of Object.entries(b.typeCounts)) {
    typeCounts[k] = (typeCounts[k] ?? 0) + v;
  }
  const perTypeDims: DimMap = {};
  for (const map of [a.perTypeDims, b.perTypeDims]) {
    for (const [alertType, dims] of Object.entries(map)) {
      const slot = (perTypeDims[alertType] ??= {
        category: {},
        subcategory: {},
        status: {},
        severity: {},
      });
      for (const dimName of ['category', 'subcategory', 'status', 'severity'] as const) {
        const src = dims[dimName] ?? {};
        const dst = slot[dimName]!;
        for (const [val, n] of Object.entries(src)) {
          dst[val] = (dst[val] ?? 0) + n;
        }
      }
    }
  }
  const oldestUnix =
    a.oldestUnix === null
      ? b.oldestUnix
      : b.oldestUnix === null
        ? a.oldestUnix
        : Math.min(a.oldestUnix, b.oldestUnix);
  const newestUnix =
    a.newestUnix === null
      ? b.newestUnix
      : b.newestUnix === null
        ? a.newestUnix
        : Math.max(a.newestUnix, b.newestUnix);
  return { typeCounts, perTypeDims, oldestUnix, newestUnix };
}

/** Sync entry point. Same behaviour as the original implementation —
 *  walks LiveStore for every source. Tests rely on this. */
function computeFacets(): ComputedFacets {
  return computeFacetsLive();
}

/** Async entry point used by the periodic refresh: archive scan for
 *  non-waze types + LiveStore for waze. The merged shape is what the
 *  user sees in the dropdown. */
async function computeFacetsAsync(): Promise<ComputedFacets> {
  const [waze, archive] = await Promise.all([
    Promise.resolve(liveWazeFacets()),
    archiveFacets(),
  ]);
  return mergeFacets(waze, archive);
}

// ---------------------------------------------------------------------------
// Response shaping (mirrors Python _build_filters_response)
// ---------------------------------------------------------------------------

function mergeCaseInsensitive(d: Record<string, number>): Record<string, number> {
  const totals: Record<string, number> = {};
  const variants: Record<string, Record<string, number>> = {};
  for (const [k, v] of Object.entries(d)) {
    const lk = k.toLowerCase();
    totals[lk] = (totals[lk] ?? 0) + v;
    const vmap = (variants[lk] ??= {});
    vmap[k] = (vmap[k] ?? 0) + v;
  }
  const out: Record<string, number> = {};
  for (const [lk, total] of Object.entries(totals)) {
    const vmap = variants[lk] ?? {};
    let bestKey = lk;
    let bestCount = -1;
    for (const [orig, count] of Object.entries(vmap)) {
      if (count > bestCount || (count === bestCount && orig < bestKey)) {
        bestKey = orig;
        bestCount = count;
      }
    }
    out[bestKey] = total;
  }
  return out;
}

// Sentinel values that the underlying source uses to mean "missing
// data" — they're not real filter targets and just clutter the
// dropdown. Compared case-insensitively after trimming. Kept narrow:
// "Other", "Not Applicable" etc. ARE real classification buckets users
// may want to filter by, so we don't strip them.
const PLACEHOLDER_VALUES = new Set<string>([
  '',
  'none',
  'null',
  'undefined',
  'n/a',
  'unknown',
  'unspecified',
]);

function isPlaceholderValue(v: string): boolean {
  return PLACEHOLDER_VALUES.has(v.trim().toLowerCase());
}

function toSortedFacetList(d: Record<string, number>, cap?: number): FacetEntry[] {
  const merged = mergeCaseInsensitive(d);
  const entries = Object.entries(merged)
    .filter(([value]) => !isPlaceholderValue(value))
    .map(([value, count]) => ({ value, count }));
  entries.sort((a, b) => {
    if (a.count !== b.count) return b.count - a.count;
    return a.value.localeCompare(b.value);
  });
  return cap ? entries.slice(0, cap) : entries;
}

function resolveFilterTarget(
  sourceFilter: string | null | undefined,
): { alertType: string; provider: string } | null {
  if (!sourceFilter) return null;
  const s = sourceFilter.trim();
  if (!s) return null;
  const direct = ALERT_TYPE_PROVIDER[s];
  if (direct) return { alertType: s, provider: direct[0] };
  const canonical = RAW_SOURCE_TO_ALERT_TYPE[s];
  if (canonical && ALERT_TYPE_PROVIDER[canonical]) {
    return { alertType: canonical, provider: ALERT_TYPE_PROVIDER[canonical][0] };
  }
  return null;
}

function buildResponse(
  computed: ComputedFacets,
  sourceFilter: string | null | undefined,
): FilterFacets {
  const { typeCounts, perTypeDims, oldestUnix, newestUnix } = computed;

  // Group alert_types by provider.
  const byProvider: Record<string, string[]> = {};
  for (const [alertType, [provider]] of Object.entries(ALERT_TYPE_PROVIDER)) {
    (byProvider[provider] ??= []).push(alertType);
  }

  const providersMap: Record<string, TypeFacets[]> = {};
  for (const provider of PROVIDER_ORDER) {
    const types = byProvider[provider] ?? [];
    const explicit = PROVIDER_TYPE_ORDER[provider] ?? [];
    const ordered: string[] = [];
    for (const t of explicit) {
      if (types.includes(t)) ordered.push(t);
    }
    for (const t of [...types].sort()) {
      if (!ordered.includes(t)) ordered.push(t);
    }

    const typesOut: TypeFacets[] = [];
    for (const alertType of ordered) {
      const provType = ALERT_TYPE_PROVIDER[alertType];
      if (!provType) continue;
      const cnt = typeCounts[alertType] ?? 0;
      const dims = perTypeDims[alertType] ?? {};
      typesOut.push({
        alert_type: alertType,
        name: provType[1],
        count: cnt,
        categories: toSortedFacetList(dims['category'] ?? {}),
        subcategories: toSortedFacetList(dims['subcategory'] ?? {}, 100),
        statuses: toSortedFacetList(dims['status'] ?? {}),
        severities: toSortedFacetList(dims['severity'] ?? {}),
      });
    }
    providersMap[provider] = typesOut;
  }

  const target = resolveFilterTarget(sourceFilter);
  let providersOut: ProviderFacets[];
  if (sourceFilter) {
    if (!target) {
      providersOut = [];
    } else {
      const types = (providersMap[target.provider] ?? []).filter(
        (t) => t.alert_type === target.alertType,
      );
      if (types.length === 0) {
        providersOut = [];
      } else {
        const disp = PROVIDER_DISPLAY[target.provider];
        if (!disp) {
          providersOut = [];
        } else {
          providersOut = [
            {
              key: target.provider,
              name: disp.name,
              icon: disp.icon,
              color: disp.color,
              count: types.reduce((sum, t) => sum + t.count, 0),
              types,
            },
          ];
        }
      }
    }
  } else {
    providersOut = [];
    for (const provider of PROVIDER_ORDER) {
      const disp = PROVIDER_DISPLAY[provider];
      if (!disp) continue;
      const types = providersMap[provider] ?? [];
      providersOut.push({
        key: provider,
        name: disp.name,
        icon: disp.icon,
        color: disp.color,
        count: types.reduce((sum, t) => sum + t.count, 0),
        types,
      });
    }
  }

  // Naive Sydney to match python's filters_applied date_range.
  return {
    providers: providersOut,
    date_range: {
      oldest: sydneyIsoFromUnix(oldestUnix),
      newest: sydneyIsoFromUnix(newestUnix),
      oldest_unix: oldestUnix,
      newest_unix: newestUnix,
    },
  };
}

// ---------------------------------------------------------------------------
// Cache + scheduler
// ---------------------------------------------------------------------------

let cached: ComputedFacets | null = null;
let lastRefreshAt: number = 0;
let timer: NodeJS.Timeout | null = null;
let refreshing = false;

// 5 min: matches python's data_history_filter_cache cadence
// (external_api_proxy.py:12568+). The DB-backed scan is too heavy
// for the previous 60s cadence and the values don't change that
// fast — the dropdown reflects 24h windows.
const DEFAULT_REFRESH_MS = 5 * 60_000;

/**
 * Recompute facets. Returns sync (using a LiveStore-only walk so tests
 * and cold reads see immediate results) AND fires off an async archive
 * scan that will replace the non-waze counts when it completes. Idem-
 * potent — concurrent archive scans collapse onto one in-flight call.
 */
export function refreshFilterCache(): void {
  try {
    cached = computeFacets();
    lastRefreshAt = Math.floor(Date.now() / 1000);
  } catch (err) {
    log.error({ err }, 'filterCache: live refresh failed');
  }
  // Fire-and-forget archive scan for the historical counts. When the
  // promise resolves, `cached` is overwritten with the merged shape.
  void refreshFromArchive();
}

async function refreshFromArchive(): Promise<void> {
  if (refreshing) return;
  refreshing = true;
  try {
    const merged = await computeFacetsAsync();
    cached = merged;
    lastRefreshAt = Math.floor(Date.now() / 1000);
  } catch (err) {
    log.error({ err }, 'filterCache: archive refresh failed');
  } finally {
    refreshing = false;
  }
}

/**
 * Build and return the filter facets response, scoped (optionally) to a
 * single source. **Sync** — returns whatever's in the cache (or a
 * waze-only in-memory fallback on cold start). The async refresh runs
 * in the background and the response stays sub-millisecond.
 */
export function getFilterFacets(sourceFilter?: string | null): FilterFacets {
  if (!cached) refreshFilterCache();
  return buildResponse(cached ?? computeFacets(), sourceFilter ?? null);
}

/** Wall-clock of the last successful refresh (epoch seconds). */
export function filterCacheLastRefreshAt(): number {
  return lastRefreshAt;
}

/**
 * Start the periodic refresh timer. Idempotent — second call is a no-op.
 * Called from index.ts after the routes are wired.
 */
export function startFilterCacheRefresh(intervalMs: number = DEFAULT_REFRESH_MS): void {
  if (timer) return;
  // First refresh fires sync (LiveStore walk) + async (archive scan)
  // so the cache is warming while the rest of boot continues.
  refreshFilterCache();
  timer = setInterval(() => refreshFilterCache(), intervalMs);
  timer.unref?.();
}

export function stopFilterCacheRefresh(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}

/** Test-only: drop the cached snapshot so the next read recomputes. */
export function _resetFilterCacheForTests(): void {
  cached = null;
  lastRefreshAt = 0;
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}
