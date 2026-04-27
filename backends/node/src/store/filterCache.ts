/**
 * In-memory facet aggregation for /api/data/history/filters.
 *
 * Replaces Python's `data_history_filter_cache` table that was refreshed
 * every 5 min via 5×GROUP BY scans of `data_history`. The new schema is
 * append-only and partitioned, so a periodic GROUP BY against
 * `archive_*` tables would defeat the partitioning. Instead we build the
 * facet shape from `liveStore`: every source's *current* snapshot has
 * the dimension fields we need for the "live" filter dropdowns. A 60s
 * background timer recomputes; readers always see the previous snapshot
 * (no locking — JS is single-threaded).
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
 *
 * `oldest`/`newest` are derived from the LiveStore snapshot timestamps —
 * a reasonable proxy now that `data_history` no longer exists; if the
 * frontend needs the true archive bracket we can run an O(1) MIN/MAX
 * against archive_*_2025_xx partitions in a follow-up.
 */
import { liveStore } from './live.js';
import { log } from '../lib/log.js';

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

function computeFacets(): ComputedFacets {
  const typeCounts: Record<string, number> = {};
  const perTypeDims: DimMap = {};
  let oldestUnix: number | null = null;
  let newestUnix: number | null = null;

  const dimSlotFor = (alertType: string): Record<string, Record<string, number>> => {
    return (perTypeDims[alertType] ??= {
      category: {},
      subcategory: {},
      status: {},
      severity: {},
    });
  };

  for (const source of liveStore.keys()) {
    if (DEPRECATED_SOURCES.has(source)) continue;
    const snap = liveStore.get(source);
    if (!snap) continue;
    if (oldestUnix === null || snap.ts < oldestUnix) oldestUnix = snap.ts;
    if (newestUnix === null || snap.ts > newestUnix) newestUnix = snap.ts;

    // Special case: the 'waze' LiveStore entry is a bbox-keyed cache
    // (WazeIngestCache shape) — one snapshot covers all four alert
    // types. Unpack and categorise each alert/jam individually so the
    // filter dropdown shows correct counts per (police/hazard/
    // roadwork/jam). Without this branch, every waze alert_type
    // returned 0 because the bbox-keyed object doesn't surface alerts
    // through the generic recordsFromSnapshot() walker.
    if (source === 'waze') {
      const { alerts, jams } = wazeRecords(snap.data);
      for (const a of alerts) {
        const at = wazeAlertType(a);
        typeCounts[at] = (typeCounts[at] ?? 0) + 1;
        const dimSlot = dimSlotFor(at);
        const sub = String(a['subtype'] ?? '').toUpperCase();
        if (sub) bumpCount(dimSlot['subcategory'] as Record<string, number>, sub);
        const cat = String(a['type'] ?? '').toUpperCase();
        if (cat) bumpCount(dimSlot['category'] as Record<string, number>, cat);
      }
      typeCounts['waze_jam'] = (typeCounts['waze_jam'] ?? 0) + jams.length;
      continue;
    }

    const alertType = canonicalAlertType(source);
    if (!alertType) continue;
    if (!ALERT_TYPE_PROVIDER[alertType]) continue;

    const records = recordsFromSnapshot(snap.data);
    typeCounts[alertType] = (typeCounts[alertType] ?? 0) + records.length;

    const dimSlot = dimSlotFor(alertType);
    for (const rec of records) {
      const dims = dimsFromRecord(rec);
      if (dims.category) bumpCount(dimSlot['category'] as Record<string, number>, dims.category);
      if (dims.subcategory) {
        // Pager numeric capcodes are noise — drop just like Python's filter
        // cache refresh does (line 12635-12636).
        if (!/^\d+$/.test(dims.subcategory)) {
          bumpCount(dimSlot['subcategory'] as Record<string, number>, dims.subcategory);
        }
      }
      if (dims.status) bumpCount(dimSlot['status'] as Record<string, number>, dims.status);
      if (dims.severity) bumpCount(dimSlot['severity'] as Record<string, number>, dims.severity);
    }
  }

  return { typeCounts, perTypeDims, oldestUnix, newestUnix };
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

function toSortedFacetList(d: Record<string, number>, cap?: number): FacetEntry[] {
  const merged = mergeCaseInsensitive(d);
  const entries = Object.entries(merged).map(([value, count]) => ({ value, count }));
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

  const isoOrNull = (unix: number | null): string | null =>
    unix !== null ? new Date(unix * 1000).toISOString() : null;

  return {
    providers: providersOut,
    date_range: {
      oldest: isoOrNull(oldestUnix),
      newest: isoOrNull(newestUnix),
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

const DEFAULT_REFRESH_MS = 60_000;

/**
 * Force a recompute. Cheap (single pass over LiveStore) — call from
 * tests directly so they don't have to wait for the timer tick.
 */
export function refreshFilterCache(): void {
  try {
    cached = computeFacets();
    lastRefreshAt = Math.floor(Date.now() / 1000);
  } catch (err) {
    log.error({ err }, 'filterCache: refresh failed');
  }
}

/**
 * Build and return the filter facets response, scoped (optionally) to a
 * single source. Lazily refreshes on first call so tests don't have to
 * remember to seed.
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
  // First refresh immediately so the cache is hot before the first
  // request lands; subsequent refreshes on interval.
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
