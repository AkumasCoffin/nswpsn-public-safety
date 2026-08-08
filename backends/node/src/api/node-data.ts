/**
 * Staff "Data" tab — feeder-node event analytics (owner|dev only).
 *
 * Reads the per-event capture written by services/nodeEvents.ts:
 *   - node_radio_events / node_pager_events   (30-day detail, migrations 043/044)
 *   - node_radio_hourly / node_radio_hourly_sys / node_pager_hourly (forever)
 *
 * Radio rows are vce ACTIVITY events (migration 044): `system` is the P25
 * systemId, and each event carries action/event_type/encrypted plus a
 * `recorded` flag set when the matching rdio call upload landed (audio
 * exists). talkgroup_label/system_label are no longer populated by ingest —
 * labels stay null for now (planned: resolve from the global agencies
 * config at read time).
 *
 *   GET /api/node-data/overview   — totals, per-node volume, top lists, series
 *   GET /api/node-data/events     — logical-call event browser (grouped)
 *   GET /api/node-data/systems    — one row per observed (wacn, system)
 *   GET /api/node-data/system     — drill-down for one P25 system
 *   GET /api/node-data/site       — drill-down for one site of a system
 *   GET /api/node-data/talkgroups — paged talkgroup monitor (per-TG rollup)
 *   GET /api/node-data/radios     — paged radio (source unit) monitor
 *   GET /api/node-data/pager-overview — pager totals, top capcodes/nodes, series
 *   GET /api/node-data/capcodes   — paged per-capcode rollup (with top node)
 *   GET /api/node-data/capcode    — message browser for one capcode (grouped)
 *
 * overview/events: windows ≤30d compute from the detail tables (logical
 * counts via COUNT(DISTINCT logical_*)); window=all uses the hourly forever
 * buckets, except topUnits which only exists in detail (capped to 30d and
 * flagged with unitsWindowCapped: true).
 *
 * The monitoring endpoints (systems/system/site/talkgroups/radios) read the
 * 30-day detail table only — window is 24h|7d|30d (default 7d), no 'all'.
 */
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Hono } from 'hono';
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { requireRole, canManageNodes } from '../services/auth/roles.js';
import { getGlobalConfig } from '../services/nodes/globalConfig.js';

/**
 * Talkgroup id → label, resolved from the imported sdrtrunk-vce alias list (the
 * single source of truth already pushed to nodes). Ingest stores no labels, so
 * the Data views resolve them here. Cached briefly to avoid re-reading the global
 * config on every request. Only individual talkgroup matchers are mapped (ranges
 * label a span, not a single id).
 */
let _tgLabelCache: { at: number; map: Map<number, string> } | null = null;
async function talkgroupLabels(): Promise<Map<number, string>> {
  if (_tgLabelCache && Date.now() - _tgLabelCache.at < 60_000) return _tgLabelCache.map;
  const map = new Map<number, string>();
  try {
    const cfg = await getGlobalConfig();
    for (const a of cfg.sdrtrunkConfig?.aliases ?? []) {
      const name = (a.name ?? '').trim();
      if (!name) continue;
      for (const id of a.ids ?? []) {
        if (id.type === 'talkgroup') {
          const v = Number(id.attrs?.['value']);
          if (Number.isInteger(v) && !map.has(v)) map.set(v, name);
        }
      }
    }
  } catch (e) {
    log.warn({ err: e }, 'talkgroupLabels: failed to load global config');
  }
  _tgLabelCache = { at: Date.now(), map };
  return map;
}

// ---------------------------------------------------------------------------
// Pager capcode → alias resolution.
//
// The operator's Pagermon server owns the capcode↔alias mapping; we read its
// exported reference CSV (data/pager/Capcode-Aliases.csv at the repo root)
// purely to LABEL bare capcodes in the Data views — same display-only pattern
// as talkgroupLabels above. Nothing is imported into the feeder config.
// ---------------------------------------------------------------------------

/** Normalise a capcode for matching: trim + strip leading zeros. '000'→'0'. */
export function normalizeCapcode(v: unknown): string {
  const s = String(v ?? '').trim();
  const stripped = s.replace(/^0+/, '');
  return stripped === '' ? (s === '' ? '' : '0') : stripped;
}

/**
 * Minimal RFC-4180-ish CSV splitter: handles quoted fields containing commas,
 * newlines and escaped quotes (""). Strips a leading UTF-8 BOM. Returns rows
 * of string cells. Not a general CSV lib — just enough for the alias export.
 */
export function parseCsvRows(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = '';
  let inQuotes = false;
  // Strip BOM if present.
  let i = text.charCodeAt(0) === 0xfeff ? 1 : 0;
  const pushField = () => {
    row.push(field);
    field = '';
  };
  const pushRow = () => {
    pushField();
    rows.push(row);
    row = [];
  };
  for (; i < text.length; i++) {
    const ch = text[i]!;
    if (inQuotes) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ',') {
      pushField();
    } else if (ch === '\n') {
      pushRow();
    } else if (ch === '\r') {
      // swallow — the following \n (if any) triggers the row push
    } else {
      field += ch;
    }
  }
  // Trailing field/row (file without a final newline).
  if (field !== '' || row.length > 0) pushRow();
  return rows;
}

/**
 * Build the capcode→{alias, agency} map from CSV text. Header-driven so the
 * column order can drift; requires at least an `address` and `alias` column.
 */
export function parseCapcodeAliasCsv(
  text: string,
): Map<string, { alias: string; agency: string | null }> {
  const map = new Map<string, { alias: string; agency: string | null }>();
  const rows = parseCsvRows(text);
  if (rows.length === 0) return map;
  const header = rows[0]!.map((h) => h.trim().toLowerCase());
  const iAddr = header.indexOf('address');
  const iAlias = header.indexOf('alias');
  const iAgency = header.indexOf('agency');
  if (iAddr === -1 || iAlias === -1) return map;
  for (let r = 1; r < rows.length; r++) {
    const cells = rows[r]!;
    const key = normalizeCapcode(cells[iAddr]);
    const alias = (cells[iAlias] ?? '').trim();
    if (key === '' || alias === '') continue;
    if (map.has(key)) continue; // first alias wins
    const agency = iAgency !== -1 ? (cells[iAgency] ?? '').trim() || null : null;
    map.set(key, { alias, agency });
  }
  return map;
}

const HERE = path.dirname(fileURLToPath(import.meta.url));
// Backend runs from backends/node; the data folder is at the repo root
// (../../data/pager/... from the backend dir). Resolve from both the module
// anchor (dist/api or src/api → repo root) and process.cwd() so it works in
// prod build, dev via tsx, and tests regardless of the launch directory.
function capcodeCsvCandidates(): string[] {
  const env = (process.env['PAGER_CAPCODE_CSV'] ?? '').trim();
  const rel = 'data/pager/Capcode-Aliases.csv';
  return [
    ...(env ? [env] : []),
    path.resolve(HERE, '../../../..', rel), // dist/api|src/api → repo root
    path.resolve(process.cwd(), '../..', rel), // cwd = backends/node
    path.resolve(process.cwd(), rel), // cwd = repo root
  ];
}

let _capcodeCache: {
  at: number;
  map: Map<string, { alias: string; agency: string | null }>;
} | null = null;
let _capcodeMissingWarned = false;

/** capcode (normalised) → {alias, agency}. Cached ~5 min (static file). */
export function capcodeAliases(): Map<string, { alias: string; agency: string | null }> {
  if (_capcodeCache && Date.now() - _capcodeCache.at < 5 * 60_000) return _capcodeCache.map;
  let map = new Map<string, { alias: string; agency: string | null }>();
  const candidates = capcodeCsvCandidates();
  let loaded = false;
  for (const p of candidates) {
    try {
      const text = readFileSync(p, 'utf8');
      map = parseCapcodeAliasCsv(text);
      loaded = true;
      log.info({ path: p, aliases: map.size }, 'loaded pager capcode aliases');
      break;
    } catch {
      /* try next candidate */
    }
  }
  if (!loaded && !_capcodeMissingWarned) {
    _capcodeMissingWarned = true;
    log.warn({ candidates }, 'pager capcode alias CSV not found — capcodes shown unlabelled');
  }
  _capcodeCache = { at: Date.now(), map };
  return map;
}

export const nodeDataRouter = new Hono();

type Windows = '24h' | '7d' | '30d' | 'all';

const WINDOW_INTERVAL: Record<Exclude<Windows, 'all'>, string> = {
  '24h': '24 hours',
  '7d': '7 days',
  '30d': '30 days',
};

/** Parse a query param as a non-negative int, else null. */
function qpInt(url: URL, name: string): number | null {
  const raw = url.searchParams.get(name);
  if (raw === null || raw === '' || !/^\d+$/.test(raw)) return null;
  return Number.parseInt(raw, 10);
}

/** pg returns BIGINT/SUM() as strings — normalise to JS number. */
function num(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function iso(v: unknown): string {
  return v instanceof Date ? v.toISOString() : String(v);
}

// ---------------------------------------------------------------------------
// GET /api/node-data/overview?window=24h|7d|30d|all&scope=all|radio|pager
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/overview',
  requireRole(canManageNodes),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const windowRaw = (url.searchParams.get('window') ?? '24h').toLowerCase();
      const window: Windows = (['24h', '7d', '30d', 'all'] as const).find(
        (w) => w === windowRaw,
      ) ?? '24h';
      const scopeRaw = (url.searchParams.get('scope') ?? 'all').toLowerCase();
      const scope = (['all', 'radio', 'pager'] as const).find((s) => s === scopeRaw) ?? 'all';
      const wantRadio = scope !== 'pager';
      const wantPager = scope !== 'radio';

      // Node metadata for perNode name/kind (small table, one read).
      const nodesRes = await pool.query<{ id: string; name: string; kind: string }>(
        `SELECT id, name, kind FROM nodes`,
      );
      const nodeMeta = new Map(nodesRes.rows.map((r) => [r.id, r]));

      let radioRaw = 0;
      let radioLogical = 0;
      let pages = 0;
      let pagesLogical = 0;
      let perNodeRadioRows: Array<{ node_id: string; calls: unknown; bytes: unknown }> = [];
      let perNodePagerRows: Array<{ node_id: string; pages: unknown }> = [];
      let topTgRows: Array<{
        system: number | null;
        talkgroup: number | null;
        calls: unknown;
        logical: unknown;
        label: string | null;
      }> = [];
      let topUnitRows: Array<{ unit: number; calls: unknown }> = [];
      let topSiteRows: Array<{ site_rfss: number; site_id: number; calls: unknown }> = [];
      let seriesRadioRows: Array<{ bucket: Date; n: unknown }> = [];
      let seriesPagerRows: Array<{ bucket: Date; n: unknown }> = [];

      if (window === 'all') {
        // Forever path: hourly bucket tables (topUnits only exists in
        // detail, so it stays capped to the last 30 days).
        const [radioRawQ, radioLogQ, pagerTotQ, pnR, pnP, tg, un, si, sr, sp] =
          await Promise.all([
            wantRadio
              ? pool.query<{ raw: unknown }>(
                  `SELECT COALESCE(SUM(calls), 0)::bigint AS raw FROM node_radio_hourly`,
                )
              : null,
            wantRadio
              ? pool.query<{ logical: unknown }>(
                  `SELECT COALESCE(SUM(logical_calls), 0)::bigint AS logical
                     FROM node_radio_hourly_sys`,
                )
              : null,
            wantPager
              ? pool.query<{ raw: unknown; logical: unknown }>(
                  `SELECT COALESCE(SUM(pages), 0)::bigint AS raw,
                          COALESCE(SUM(logical_pages), 0)::bigint AS logical
                     FROM node_pager_hourly`,
                )
              : null,
            wantRadio
              ? pool.query<{ node_id: string; calls: unknown; bytes: unknown }>(
                  `SELECT node_id, SUM(calls)::bigint AS calls,
                          SUM(audio_bytes)::bigint AS bytes
                     FROM node_radio_hourly GROUP BY node_id`,
                )
              : null,
            wantPager
              ? pool.query<{ node_id: string; pages: unknown }>(
                  `SELECT node_id, SUM(pages)::bigint AS pages
                     FROM node_pager_hourly GROUP BY node_id`,
                )
              : null,
            wantRadio
              ? pool.query<{
                  system: number;
                  talkgroup: number;
                  calls: unknown;
                  logical: unknown;
                  label: string | null;
                }>(
                  // label deliberately NULL: activity-event ingest stores no
                  // labels (they'll resolve from the agencies config later).
                  `SELECT h.system, h.talkgroup,
                          SUM(h.calls)::bigint AS calls,
                          SUM(h.logical_calls)::bigint AS logical,
                          NULL::text AS label
                     FROM node_radio_hourly_sys h
                    GROUP BY h.system, h.talkgroup
                    ORDER BY calls DESC LIMIT 15`,
                )
              : null,
            wantRadio
              ? pool.query<{ unit: number; calls: unknown }>(
                  `SELECT source_unit AS unit, COUNT(*)::int AS calls
                     FROM node_radio_events
                    WHERE received_at >= now() - interval '30 days'
                      AND source_unit IS NOT NULL
                    GROUP BY source_unit ORDER BY calls DESC LIMIT 15`,
                )
              : null,
            wantRadio
              ? pool.query<{ site_rfss: number; site_id: number; calls: unknown }>(
                  `SELECT site_rfss, site_id, SUM(calls)::bigint AS calls
                     FROM node_radio_hourly_sys
                    WHERE site_rfss <> -1 AND site_id <> -1
                    GROUP BY site_rfss, site_id ORDER BY calls DESC LIMIT 15`,
                )
              : null,
            wantRadio
              ? pool.query<{ bucket: Date; n: unknown }>(
                  `SELECT date_trunc('day', hour) AS bucket, SUM(calls)::bigint AS n
                     FROM node_radio_hourly GROUP BY 1 ORDER BY 1`,
                )
              : null,
            wantPager
              ? pool.query<{ bucket: Date; n: unknown }>(
                  `SELECT date_trunc('day', hour) AS bucket, SUM(pages)::bigint AS n
                     FROM node_pager_hourly GROUP BY 1 ORDER BY 1`,
                )
              : null,
          ]);
        radioRaw = num(radioRawQ?.rows[0]?.raw);
        radioLogical = num(radioLogQ?.rows[0]?.logical);
        pages = num(pagerTotQ?.rows[0]?.raw);
        pagesLogical = num(pagerTotQ?.rows[0]?.logical);
        perNodeRadioRows = pnR?.rows ?? [];
        perNodePagerRows = pnP?.rows ?? [];
        topTgRows = tg?.rows ?? [];
        topUnitRows = un?.rows ?? [];
        topSiteRows = si?.rows ?? [];
        seriesRadioRows = sr?.rows ?? [];
        seriesPagerRows = sp?.rows ?? [];
      } else {
        // Detail path: exact logical counts via COUNT(DISTINCT logical_*).
        const iv = WINDOW_INTERVAL[window];
        const cond = `received_at >= now() - $1::interval`;
        const [rTot, pTot, pnR, pnP, tg, un, si, sr, sp] = await Promise.all([
          wantRadio
            ? pool.query<{ raw: unknown; logical: unknown }>(
                `SELECT COUNT(*)::int AS raw,
                        COUNT(DISTINCT logical_call_id)::int AS logical
                   FROM node_radio_events WHERE ${cond}`,
                [iv],
              )
            : null,
          wantPager
            ? pool.query<{ raw: unknown; logical: unknown }>(
                `SELECT COUNT(*)::int AS raw,
                        COUNT(DISTINCT logical_id)::int AS logical
                   FROM node_pager_events WHERE ${cond}`,
                [iv],
              )
            : null,
          wantRadio
            ? pool.query<{ node_id: string; calls: unknown; bytes: unknown }>(
                `SELECT node_id, COUNT(*)::int AS calls,
                        COALESCE(SUM(audio_bytes), 0)::bigint AS bytes
                   FROM node_radio_events WHERE ${cond} GROUP BY node_id`,
                [iv],
              )
            : null,
          wantPager
            ? pool.query<{ node_id: string; pages: unknown }>(
                `SELECT node_id, COUNT(*)::int AS pages
                   FROM node_pager_events WHERE ${cond} GROUP BY node_id`,
                [iv],
              )
            : null,
          wantRadio
            ? pool.query<{
                system: number | null;
                talkgroup: number | null;
                calls: unknown;
                logical: unknown;
                label: string | null;
              }>(
                // label deliberately NULL: activity-event ingest stores no
                // labels (they'll resolve from the agencies config later).
                `SELECT e.system, e.talkgroup,
                        COUNT(*)::int AS calls,
                        COUNT(DISTINCT e.logical_call_id)::int AS logical,
                        NULL::text AS label
                   FROM node_radio_events e WHERE ${cond}
                  GROUP BY e.system, e.talkgroup
                  ORDER BY calls DESC LIMIT 15`,
                [iv],
              )
            : null,
          wantRadio
            ? pool.query<{ unit: number; calls: unknown }>(
                `SELECT source_unit AS unit, COUNT(*)::int AS calls
                   FROM node_radio_events
                  WHERE ${cond} AND source_unit IS NOT NULL
                  GROUP BY source_unit ORDER BY calls DESC LIMIT 15`,
                [iv],
              )
            : null,
          wantRadio
            ? pool.query<{ site_rfss: number; site_id: number; calls: unknown }>(
                `SELECT site_rfss, site_id, COUNT(*)::int AS calls
                   FROM node_radio_events
                  WHERE ${cond} AND site_rfss IS NOT NULL AND site_id IS NOT NULL
                  GROUP BY site_rfss, site_id ORDER BY calls DESC LIMIT 15`,
                [iv],
              )
            : null,
          wantRadio
            ? pool.query<{ bucket: Date; n: unknown }>(
                `SELECT date_trunc('hour', received_at) AS bucket, COUNT(*)::int AS n
                   FROM node_radio_events WHERE ${cond} GROUP BY 1 ORDER BY 1`,
                [iv],
              )
            : null,
          wantPager
            ? pool.query<{ bucket: Date; n: unknown }>(
                `SELECT date_trunc('hour', received_at) AS bucket, COUNT(*)::int AS n
                   FROM node_pager_events WHERE ${cond} GROUP BY 1 ORDER BY 1`,
                [iv],
              )
            : null,
        ]);
        radioRaw = num(rTot?.rows[0]?.raw);
        radioLogical = num(rTot?.rows[0]?.logical);
        pages = num(pTot?.rows[0]?.raw);
        pagesLogical = num(pTot?.rows[0]?.logical);
        perNodeRadioRows = pnR?.rows ?? [];
        perNodePagerRows = pnP?.rows ?? [];
        topTgRows = tg?.rows ?? [];
        topUnitRows = un?.rows ?? [];
        topSiteRows = si?.rows ?? [];
        seriesRadioRows = sr?.rows ?? [];
        seriesPagerRows = sp?.rows ?? [];
      }

      // Merge the two perNode result sets on node_id.
      const perNodeMap = new Map<
        string,
        { nodeId: string; name: string | null; kind: string | null; calls: number; pages: number; bytes: number }
      >();
      const ensureNode = (id: string) => {
        let e = perNodeMap.get(id);
        if (!e) {
          const meta = nodeMeta.get(id);
          e = {
            nodeId: id,
            name: meta?.name ?? null,
            kind: meta?.kind ?? null,
            calls: 0,
            pages: 0,
            bytes: 0,
          };
          perNodeMap.set(id, e);
        }
        return e;
      };
      for (const r of perNodeRadioRows) {
        const e = ensureNode(r.node_id);
        e.calls += num(r.calls);
        e.bytes += num(r.bytes);
      }
      for (const r of perNodePagerRows) {
        const e = ensureNode(r.node_id);
        e.pages += num(r.pages);
      }
      const perNode = [...perNodeMap.values()].sort(
        (a, b) => b.calls + b.pages - (a.calls + a.pages),
      );

      // Hourly (or daily for window=all) activity series, merged radio+pager.
      const seriesMap = new Map<string, { radio: number; pager: number }>();
      for (const r of seriesRadioRows) {
        const k = iso(r.bucket);
        const e = seriesMap.get(k) ?? { radio: 0, pager: 0 };
        e.radio += num(r.n);
        seriesMap.set(k, e);
      }
      for (const r of seriesPagerRows) {
        const k = iso(r.bucket);
        const e = seriesMap.get(k) ?? { radio: 0, pager: 0 };
        e.pager += num(r.n);
        seriesMap.set(k, e);
      }
      const bucketField = window === 'all' ? 'bucket' : 'hour';
      const series = [...seriesMap.entries()]
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([k, v]) => ({ [bucketField]: k, radio: v.radio, pager: v.pager }));

      const tgLabels = await talkgroupLabels();
      const body: Record<string, unknown> = {
        window,
        scope,
        totals: {
          radioRaw,
          radioLogical,
          pages,
          pagesLogical,
          activeNodes: perNodeMap.size,
        },
        perNode,
        topTalkgroups: topTgRows.map((r) => ({
          system: r.system,
          talkgroup: r.talkgroup,
          label: r.label ?? (r.talkgroup !== null ? tgLabels.get(r.talkgroup) ?? null : null),
          calls: num(r.calls),
          logicalCalls: num(r.logical),
        })),
        topUnits: topUnitRows.map((r) => ({ unit: r.unit, calls: num(r.calls) })),
        topSites: topSiteRows.map((r) => ({
          siteRfss: r.site_rfss,
          siteId: r.site_id,
          calls: num(r.calls),
        })),
        series,
      };
      // topUnits can only come from the 30-day detail window — flag the cap
      // so the UI can annotate it when the rest of the page shows all-time.
      if (window === 'all') body['unitsWindowCapped'] = true;
      return c.json(body);
    } catch (err) {
      log.error({ err }, '/api/node-data/overview error');
      return c.json({ error: 'failed to load overview' }, 500);
    }
  },
);

// ---------------------------------------------------------------------------
// GET /api/node-data/events
//   ?type=all|radio|pager&talkgroup=&unit=&node=&site=&system=&encrypted=
//   &q=&from=&to=&limit=50&offset=0
//
// Newest first, GROUPED into logical calls/pages. Pagination is over the
// merged logical-event stream: a WITH-union of both grouped id/at sets is
// ordered + limited in SQL (correct combined paging), then the page's ids
// are hydrated per type with full group aggregates.
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/events',
  requireRole(canManageNodes),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);

      const typeRaw = (url.searchParams.get('type') ?? 'all').toLowerCase();
      const type = (['all', 'radio', 'pager'] as const).find((t) => t === typeRaw) ?? 'all';
      const talkgroup = qpInt(url, 'talkgroup');
      const unit = qpInt(url, 'unit');
      const system = qpInt(url, 'system');
      const nodeId = (url.searchParams.get('node') ?? '').trim() || null;
      const q = (url.searchParams.get('q') ?? '').trim() || null;

      // encrypted=true|false (radio-only filter; absent = both).
      const encRaw = (url.searchParams.get('encrypted') ?? '').trim().toLowerCase();
      let encrypted: boolean | null = null;
      if (encRaw) {
        if (encRaw !== 'true' && encRaw !== 'false') {
          return c.json({ error: 'encrypted must be true or false' }, 400);
        }
        encrypted = encRaw === 'true';
      }

      // site = "rfss-site" (e.g. "1-12").
      const siteRaw = (url.searchParams.get('site') ?? '').trim();
      let siteRfss: number | null = null;
      let siteId: number | null = null;
      if (siteRaw) {
        const m = /^(\d+)-(\d+)$/.exec(siteRaw);
        if (!m) return c.json({ error: 'site must be "rfss-site", e.g. 1-12' }, 400);
        siteRfss = Number.parseInt(m[1]!, 10);
        siteId = Number.parseInt(m[2]!, 10);
      }

      const fromRaw = url.searchParams.get('from');
      const toRaw = url.searchParams.get('to');
      let from: Date | null = null;
      let to: Date | null = null;
      if (fromRaw) {
        from = new Date(fromRaw);
        if (Number.isNaN(from.getTime())) {
          return c.json({ error: 'from must be an ISO timestamp' }, 400);
        }
      }
      if (toRaw) {
        to = new Date(toRaw);
        if (Number.isNaN(to.getTime())) {
          return c.json({ error: 'to must be an ISO timestamp' }, 400);
        }
      }

      const limit = Math.max(1, Math.min(200, Number(url.searchParams.get('limit') ?? 50) || 50));
      const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

      // Radio-only filters silently exclude the pager stream (a page has no
      // talkgroup/unit/site/system to match).
      const radioOnlyFilter =
        talkgroup !== null ||
        unit !== null ||
        system !== null ||
        siteRfss !== null ||
        encrypted !== null;
      const includeRadio = type !== 'pager';
      const includePager = type !== 'radio' && !radioOnlyFilter;
      if (!includeRadio && !includePager) {
        return c.json({ total: 0, limit, offset, events: [] });
      }

      const params: unknown[] = [];
      const add = (v: unknown): string => {
        params.push(v);
        return `$${params.length}`;
      };

      // Row-level filter sets. Group membership: a logical call matches when
      // ANY of its receptions matches (node/site are per-reception fields).
      const rConds: string[] = ['logical_call_id IS NOT NULL'];
      const pConds: string[] = ['logical_id IS NOT NULL'];
      if (includeRadio) {
        if (system !== null) rConds.push(`system = ${add(system)}`);
        if (talkgroup !== null) rConds.push(`talkgroup = ${add(talkgroup)}`);
        if (unit !== null) rConds.push(`source_unit = ${add(unit)}`);
        if (nodeId !== null) rConds.push(`node_id = ${add(nodeId)}`);
        if (siteRfss !== null) {
          rConds.push(`site_rfss = ${add(siteRfss)}`);
          rConds.push(`site_id = ${add(siteId)}`);
        }
        if (encrypted !== null) rConds.push(`encrypted = ${add(encrypted)}`);
        if (q !== null) {
          const like = add(`%${q}%`);
          rConds.push(`(talkgroup_label ILIKE ${like} OR system_label ILIKE ${like})`);
        }
        if (from !== null) rConds.push(`received_at >= ${add(from.toISOString())}::timestamptz`);
        if (to !== null) rConds.push(`received_at <= ${add(to.toISOString())}::timestamptz`);
      }
      if (includePager) {
        if (nodeId !== null) pConds.push(`node_id = ${add(nodeId)}`);
        if (q !== null) pConds.push(`message ILIKE ${add(`%${q}%`)}`);
        if (from !== null) pConds.push(`received_at >= ${add(from.toISOString())}::timestamptz`);
        if (to !== null) pConds.push(`received_at <= ${add(to.toISOString())}::timestamptz`);
      }

      const ctes: string[] = [];
      const unionParts: string[] = [];
      if (includeRadio) {
        ctes.push(
          `r AS (SELECT logical_call_id AS id, MIN(received_at) AS at
                   FROM node_radio_events
                  WHERE ${rConds.join(' AND ')}
                  GROUP BY logical_call_id)`,
        );
        unionParts.push(`SELECT 'radio'::text AS type, id, at FROM r`);
      }
      if (includePager) {
        ctes.push(
          `p AS (SELECT logical_id AS id, MIN(received_at) AS at
                   FROM node_pager_events
                  WHERE ${pConds.join(' AND ')}
                  GROUP BY logical_id)`,
        );
        unionParts.push(`SELECT 'pager'::text AS type, id, at FROM p`);
      }
      const withSql = `WITH ${ctes.join(', ')}, u AS (${unionParts.join(' UNION ALL ')})`;

      // Count uses only the filter params (snapshot before limit/offset are
      // appended — pg rejects unused bind params).
      const countParams = [...params];
      const countSql = `${withSql} SELECT COUNT(*)::int AS n FROM u`;
      const pageSql =
        `${withSql} SELECT type, id::text AS id, at FROM u ` +
        `ORDER BY at DESC, id DESC LIMIT ${add(limit)} OFFSET ${add(offset)}`;

      const [countRes, pageRes] = await Promise.all([
        pool.query<{ n: number }>(countSql, countParams),
        pool.query<{ type: 'radio' | 'pager'; id: string; at: Date }>(pageSql, params),
      ]);
      const total = countRes.rows[0]?.n ?? 0;
      const page = pageRes.rows;

      const radioIds = page.filter((r) => r.type === 'radio').map((r) => r.id);
      const pagerIds = page.filter((r) => r.type === 'pager').map((r) => r.id);

      // Hydrate each page id with FULL group aggregates (all receptions of
      // the logical call, not just the filter-matching ones).
      const [radioDetail, pagerDetail] = await Promise.all([
        radioIds.length > 0
          ? pool.query<{
              id: string;
              at: Date;
              system: number | null;
              talkgroup: number | null;
              talkgroup_label: string | null;
              system_label: string | null;
              source_unit: number | null;
              source_alias: string | null;
              frequency: string | null;
              action: string | null;
              event_type: string | null;
              encrypted: boolean;
              recorded: boolean;
              receptions: number;
              sites: Array<{ rfss: number; site: number }>;
              nodes: Array<{ id: string; name: string }>;
            }>(
              `SELECT e.logical_call_id::text AS id,
                      MIN(e.received_at) AS at,
                      MIN(e.system) AS system,
                      MIN(e.talkgroup) AS talkgroup,
                      MAX(e.talkgroup_label) AS talkgroup_label,
                      MAX(e.system_label) AS system_label,
                      MIN(e.source_unit) AS source_unit,
                      (array_agg(e.source_alias ORDER BY e.received_at DESC)
                         FILTER (WHERE e.source_alias IS NOT NULL))[1] AS source_alias,
                      MIN(e.frequency)::bigint AS frequency,
                      MIN(e.action) AS action,
                      MIN(e.event_type) AS event_type,
                      bool_or(e.encrypted) AS encrypted,
                      bool_or(e.recorded) AS recorded,
                      COUNT(*)::int AS receptions,
                      COALESCE(
                        jsonb_agg(DISTINCT jsonb_build_object('rfss', e.site_rfss, 'site', e.site_id))
                          FILTER (WHERE e.site_rfss IS NOT NULL AND e.site_id IS NOT NULL),
                        '[]'::jsonb) AS sites,
                      jsonb_agg(DISTINCT jsonb_build_object('id', e.node_id, 'name', n.name)) AS nodes
                 FROM node_radio_events e
                 JOIN nodes n ON n.id = e.node_id
                WHERE e.logical_call_id = ANY(${'$1'}::bigint[])
                GROUP BY e.logical_call_id`,
              [radioIds],
            )
          : null,
        pagerIds.length > 0
          ? pool.query<{
              id: string;
              at: Date;
              capcode: string;
              message: string | null;
              freq_mhz: number | null;
              receptions: number;
              nodes: Array<{ id: string; name: string }>;
            }>(
              `SELECT e.logical_id::text AS id,
                      MIN(e.received_at) AS at,
                      MIN(e.capcode) AS capcode,
                      MIN(e.message) AS message,
                      MIN(e.freq_mhz)::float8 AS freq_mhz,
                      COUNT(*)::int AS receptions,
                      jsonb_agg(DISTINCT jsonb_build_object('id', e.node_id, 'name', n.name)) AS nodes
                 FROM node_pager_events e
                 JOIN nodes n ON n.id = e.node_id
                WHERE e.logical_id = ANY(${'$1'}::bigint[])
                GROUP BY e.logical_id`,
              [pagerIds],
            )
          : null,
      ]);

      const radioMap = new Map((radioDetail?.rows ?? []).map((r) => [r.id, r]));
      const pagerMap = new Map((pagerDetail?.rows ?? []).map((r) => [r.id, r]));
      const labels = await talkgroupLabels();
      const capAliases = pagerIds.length > 0 ? capcodeAliases() : null;

      const events = page
        .map((row) => {
          if (row.type === 'radio') {
            const d = radioMap.get(row.id);
            if (!d) return null;
            return {
              type: 'radio' as const,
              id: Number(d.id),
              at: iso(d.at),
              system: d.system,
              talkgroup: d.talkgroup,
              talkgroupLabel: d.talkgroup_label ?? (d.talkgroup !== null ? labels.get(d.talkgroup) ?? null : null),
              systemLabel: d.system_label,
              sourceUnit: d.source_unit,
              sourceAlias: d.source_alias,
              frequency: d.frequency !== null ? Number(d.frequency) : null,
              action: d.action,
              eventType: d.event_type,
              // A logical call is "encrypted"/"recorded" if ANY reception was.
              encrypted: d.encrypted === true,
              recorded: d.recorded === true,
              sites: d.sites,
              nodes: d.nodes,
              receptions: d.receptions,
            };
          }
          const d = pagerMap.get(row.id);
          if (!d) return null;
          const alias = capAliases?.get(normalizeCapcode(d.capcode)) ?? null;
          return {
            type: 'pager' as const,
            id: Number(d.id),
            at: iso(d.at),
            capcode: d.capcode,
            capcodeAlias: alias?.alias ?? null,
            agency: alias?.agency ?? null,
            message: d.message,
            freqMhz: d.freq_mhz,
            nodes: d.nodes,
            receptions: d.receptions,
          };
        })
        .filter((e) => e !== null);

      return c.json({ total, limit, offset, events });
    } catch (err) {
      log.error({ err }, '/api/node-data/events error');
      return c.json({ error: 'failed to load events' }, 500);
    }
  },
);

// ===========================================================================
// System / site / talkgroup / radio monitoring.
//
// All of these read node_radio_events (30-day detail) only, so the window
// is 24h|7d|30d with a 7d default — there is no 'all'. The P25 identity of
// a network is the (wacn, system) pair; wacn can be NULL on rows from
// agents that don't decode it, and NULL is kept as its own group (matched
// with IS NOT DISTINCT FROM in drill-down laterals).
// ===========================================================================

type DetailWindow = Exclude<Windows, 'all'>;

/** Detail-endpoint window: 24h|7d|30d, default (and fallback) 7d. */
function detailWindow(url: URL): DetailWindow {
  const raw = (url.searchParams.get('window') ?? '7d').toLowerCase();
  return (['24h', '7d', '30d'] as const).find((w) => w === raw) ?? '7d';
}

/** Optional strict int param: absent → null, present-but-non-numeric → error. */
function qpIntOpt(url: URL, name: string): { value: number | null; error?: string } {
  const raw = url.searchParams.get(name);
  if (raw === null || raw === '') return { value: null };
  if (!/^\d+$/.test(raw)) return { value: null, error: `${name} must be a non-negative integer` };
  return { value: Number.parseInt(raw, 10) };
}

interface ScopedDetail {
  totals: {
    calls: number;
    logicalCalls: number;
    encryptedCalls: number;
    talkgroups: number;
    radios: number;
    sites: number;
  };
  topTalkgroups: Array<{
    talkgroup: number;
    label: string | null;
    calls: number;
    logicalCalls: number;
    encryptedCalls: number;
    lastSeen: string;
  }>;
  topRadios: Array<{ radio: number; calls: number; lastSeen: string }>;
  series: Array<{ hour: string; calls: number }>;
}

/**
 * Shared drill-down aggregates for a scoped slice of node_radio_events
 * (one system, or one site of a system). `where` renders the full WHERE
 * clause with a column prefix ('' or 'e.') so it can be reused inside
 * correlated subqueries; `params` are its bind values.
 */
async function scopedRadioDetail(
  pool: Pool,
  where: (prefix: string) => string,
  params: unknown[],
): Promise<ScopedDetail> {
  const _scopedTgLabels = await talkgroupLabels();
  const [totQ, tgQ, unQ, srQ] = await Promise.all([
    pool.query<{
      calls: unknown;
      logical: unknown;
      enc: unknown;
      talkgroups: unknown;
      radios: unknown;
      sites: unknown;
    }>(
      `SELECT COUNT(*)::int AS calls,
              COUNT(DISTINCT logical_call_id)::int AS logical,
              (COUNT(*) FILTER (WHERE encrypted))::int AS enc,
              COUNT(DISTINCT talkgroup)::int AS talkgroups,
              COUNT(DISTINCT source_unit)::int AS radios,
              (COUNT(DISTINCT (site_rfss, site_id))
                 FILTER (WHERE site_rfss IS NOT NULL AND site_id IS NOT NULL))::int AS sites
         FROM node_radio_events
        WHERE ${where('')}`,
      params,
    ),
    pool.query<{
      talkgroup: number;
      calls: unknown;
      logical: unknown;
      enc: unknown;
      last_seen: Date;
    }>(
      `SELECT talkgroup,
              COUNT(*)::int AS calls,
              COUNT(DISTINCT logical_call_id)::int AS logical,
              (COUNT(*) FILTER (WHERE encrypted))::int AS enc,
              MAX(received_at) AS last_seen
         FROM node_radio_events
        WHERE ${where('')} AND talkgroup IS NOT NULL
        GROUP BY talkgroup
        ORDER BY calls DESC, talkgroup ASC
        LIMIT 20`,
      params,
    ),
    pool.query<{ radio: number; calls: unknown; last_seen: Date }>(
      `SELECT source_unit AS radio,
              COUNT(*)::int AS calls,
              MAX(received_at) AS last_seen
         FROM node_radio_events
        WHERE ${where('')} AND source_unit IS NOT NULL
        GROUP BY source_unit
        ORDER BY calls DESC, radio ASC
        LIMIT 20`,
      params,
    ),
    pool.query<{ hour: Date; calls: unknown }>(
      `SELECT date_trunc('hour', received_at) AS hour, COUNT(*)::int AS calls
         FROM node_radio_events
        WHERE ${where('')}
        GROUP BY 1 ORDER BY 1`,
      params,
    ),
  ]);
  const t = totQ.rows[0];
  return {
    totals: {
      calls: num(t?.calls),
      logicalCalls: num(t?.logical),
      encryptedCalls: num(t?.enc),
      talkgroups: num(t?.talkgroups),
      radios: num(t?.radios),
      sites: num(t?.sites),
    },
    topTalkgroups: tgQ.rows.map((r) => ({
      talkgroup: r.talkgroup,
      label: _scopedTgLabels.get(r.talkgroup) ?? null,
      calls: num(r.calls),
      logicalCalls: num(r.logical),
      encryptedCalls: num(r.enc),
      lastSeen: iso(r.last_seen),
    })),
    topRadios: unQ.rows.map((r) => ({
      radio: r.radio,
      calls: num(r.calls),
      lastSeen: iso(r.last_seen),
    })),
    series: srQ.rows.map((r) => ({ hour: iso(r.hour), calls: num(r.calls) })),
  };
}

// ---------------------------------------------------------------------------
// GET /api/node-data/systems?window=24h|7d|30d
// One row per distinct (wacn, system) observed in-window, incl. NULLs.
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/systems',
  requireRole(canManageNodes),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);

      const res = await pool.query<{
        wacn: number | null;
        system: number | null;
        name: string | null;
        calls: unknown;
        logical: unknown;
        enc: unknown;
        sites: unknown;
        talkgroups: unknown;
        radios: unknown;
        first_seen: Date;
        last_seen: Date;
      }>(
        // name: the most-recent non-null friendly system label (system_name
        // from vce, e.g. "NSWPSN"); null when no reception carried one.
        `SELECT wacn, system,
                (array_agg(system_label ORDER BY received_at DESC)
                   FILTER (WHERE system_label IS NOT NULL))[1] AS name,
                COUNT(*)::int AS calls,
                COUNT(DISTINCT logical_call_id)::int AS logical,
                (COUNT(*) FILTER (WHERE encrypted))::int AS enc,
                (COUNT(DISTINCT (site_rfss, site_id))
                   FILTER (WHERE site_rfss IS NOT NULL AND site_id IS NOT NULL))::int AS sites,
                COUNT(DISTINCT talkgroup)::int AS talkgroups,
                COUNT(DISTINCT source_unit)::int AS radios,
                MIN(received_at) AS first_seen,
                MAX(received_at) AS last_seen
           FROM node_radio_events
          WHERE received_at >= now() - $1::interval
            AND system IS NOT NULL
          GROUP BY wacn, system
          ORDER BY calls DESC, system ASC NULLS LAST
          LIMIT 50`,
        [WINDOW_INTERVAL[window]],
      );

      return c.json({
        window,
        systems: res.rows.map((r) => ({
          wacn: r.wacn,
          system: r.system,
          name: r.name ?? null,
          calls: num(r.calls),
          logicalCalls: num(r.logical),
          encryptedCalls: num(r.enc),
          sites: num(r.sites),
          talkgroups: num(r.talkgroups),
          radios: num(r.radios),
          firstSeen: iso(r.first_seen),
          lastSeen: iso(r.last_seen),
        })),
      });
    } catch (err) {
      log.error({ err }, '/api/node-data/systems error');
      return c.json({ error: 'failed to load systems' }, 500);
    }
  },
);

// ---------------------------------------------------------------------------
// GET /api/node-data/system?system=<int>&wacn=<int optional>&window=
// Drill-down for one P25 system: totals, per-site rollup (NULL-site events
// count in totals but not the sites array), top talkgroups/radios, series.
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/system',
  requireRole(canManageNodes),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);
      const system = qpInt(url, 'system');
      if (system === null) {
        return c.json({ error: 'system must be a non-negative integer' }, 400);
      }
      const wacnP = qpIntOpt(url, 'wacn');
      if (wacnP.error) return c.json({ error: wacnP.error }, 400);
      const wacn = wacnP.value;

      const params: unknown[] = [WINDOW_INTERVAL[window], system];
      if (wacn !== null) params.push(wacn);
      const scope = (p: string) =>
        `${p}received_at >= now() - $1::interval AND ${p}system = $2` +
        (wacn !== null ? ` AND ${p}wacn = $3` : '');

      const [detail, sitesQ, nameQ] = await Promise.all([
        scopedRadioDetail(pool, scope, params),
        pool.query<{
          rfss: number;
          site: number;
          nac: number | null;
          calls: unknown;
          logical: unknown;
          last_seen: Date;
          top_tg: number | null;
          top_tg_calls: unknown;
          channel_name: string | null;
          control_frequency_mhz: number | null;
          channel_count: number | null;
          neighbor_count: number | null;
        }>(
          // meta: deep-metadata enrichment (migration 047) — the latest
          // node_site_snapshots row for this (system, rfss, site) across
          // nodes, so each site in the drill-down carries its control freq +
          // channel/neighbor counts alongside the event-derived call rollup.
          `SELECT s.rfss, s.site, s.nac, s.calls, s.logical, s.last_seen,
                  tt.talkgroup AS top_tg, tt.calls AS top_tg_calls,
                  meta.channel_name,
                  meta.control_frequency_mhz,
                  meta.channel_count, meta.neighbor_count
             FROM (
               SELECT site_rfss AS rfss, site_id AS site,
                      MAX(site_nac) AS nac,
                      COUNT(*)::int AS calls,
                      COUNT(DISTINCT logical_call_id)::int AS logical,
                      MAX(received_at) AS last_seen
                 FROM node_radio_events
                WHERE ${scope('')} AND site_rfss IS NOT NULL AND site_id IS NOT NULL
                GROUP BY site_rfss, site_id
             ) s
             LEFT JOIN LATERAL (
               SELECT e.talkgroup, COUNT(*)::int AS calls
                 FROM node_radio_events e
                WHERE ${scope('e.')} AND e.site_rfss = s.rfss AND e.site_id = s.site
                  AND e.talkgroup IS NOT NULL
                GROUP BY e.talkgroup
                ORDER BY calls DESC, e.talkgroup ASC
                LIMIT 1
             ) tt ON true
             LEFT JOIN LATERAL (
               SELECT m.channel_name,
                      m.control_frequency_mhz,
                      jsonb_array_length(m.channels) AS channel_count,
                      jsonb_array_length(m.neighbors) AS neighbor_count
                 FROM node_site_snapshots m
                WHERE m.system_id = $2 AND m.rfss = s.rfss AND m.site_id = s.site
                ORDER BY m.received_at DESC
                LIMIT 1
             ) meta ON true
            ORDER BY s.calls DESC, s.rfss ASC, s.site ASC`,
          params,
        ),
        // Friendly system name for the drill-down heading (most-recent
        // non-null system_label within the scope); null when none seen.
        pool.query<{ name: string | null }>(
          `SELECT (array_agg(system_label ORDER BY received_at DESC)
                     FILTER (WHERE system_label IS NOT NULL))[1] AS name
             FROM node_radio_events
            WHERE ${scope('')}`,
          params,
        ),
      ]);

      return c.json({
        window,
        system,
        wacn,
        name: nameQ.rows[0]?.name ?? null,
        totals: detail.totals,
        sites: sitesQ.rows.map((r) => ({
          rfss: r.rfss,
          site: r.site,
          name: r.channel_name ?? null,
          nac: r.nac,
          calls: num(r.calls),
          logicalCalls: num(r.logical),
          lastSeen: iso(r.last_seen),
          topTalkgroup:
            r.top_tg !== null
              ? { talkgroup: r.top_tg, calls: num(r.top_tg_calls) }
              : null,
          // Deep-metadata enrichment (null until a node forwards site
          // snapshots — see migration 047 / node_site_snapshots).
          controlFrequencyMhz: r.control_frequency_mhz ?? null,
          channelCount: r.channel_count ?? null,
          neighborCount: r.neighbor_count ?? null,
        })),
        topTalkgroups: detail.topTalkgroups,
        topRadios: detail.topRadios,
        series: detail.series,
      });
    } catch (err) {
      log.error({ err }, '/api/node-data/system error');
      return c.json({ error: 'failed to load system' }, 500);
    }
  },
);

// ---------------------------------------------------------------------------
// GET /api/node-data/site?system=<int>&rfss=<int>&site=<int>&window=
// Same shape as /system minus the sites array, plus nodes[] — which feeder
// nodes receive this site and how much.
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/site',
  requireRole(canManageNodes),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);
      const system = qpInt(url, 'system');
      const rfss = qpInt(url, 'rfss');
      const site = qpInt(url, 'site');
      if (system === null || rfss === null || site === null) {
        return c.json(
          { error: 'system, rfss and site must be non-negative integers' },
          400,
        );
      }

      const params: unknown[] = [WINDOW_INTERVAL[window], system, rfss, site];
      const scope = (p: string) =>
        `${p}received_at >= now() - $1::interval AND ${p}system = $2` +
        ` AND ${p}site_rfss = $3 AND ${p}site_id = $4`;

      const [detail, nodesQ, metaQ] = await Promise.all([
        scopedRadioDetail(pool, scope, params),
        pool.query<{ id: string; name: string | null; calls: unknown; last_seen: Date }>(
          `SELECT e.node_id AS id, n.name, COUNT(*)::int AS calls,
                  MAX(e.received_at) AS last_seen
             FROM node_radio_events e
             LEFT JOIN nodes n ON n.id = e.node_id
            WHERE ${scope('e.')}
            GROUP BY e.node_id, n.name
            ORDER BY calls DESC, e.node_id ASC`,
          params,
        ),
        // Deep P25 site metadata (migration 047): the latest node_site_snapshots
        // row for this (systemId, rfss, site) across nodes. Null until a node
        // forwards site snapshots — the drill-down renders an empty state then.
        pool.query<{
          guid: string | null;
          system_name: string | null;
          wacn: number | null;
          nac: number | null;
          lra: number | null;
          channel_name: string | null;
          control_frequency_mhz: number | null;
          control_lcn: string | null;
          affiliated_radio_count: number | null;
          observation_count: number | null;
          site_first_seen_ms: string | null;
          site_last_seen_ms: string | null;
          status: unknown;
          channels: unknown;
          neighbors: unknown;
          bands: unknown;
          quality: unknown;
          received_at: Date;
        }>(
          `SELECT guid, system_name, wacn, nac, lra, channel_name,
                  control_frequency_mhz, control_lcn, affiliated_radio_count,
                  observation_count, site_first_seen_ms, site_last_seen_ms,
                  status, channels, neighbors, bands, quality, received_at
             FROM node_site_snapshots
            WHERE system_id = $2 AND rfss = $3 AND site_id = $4
            ORDER BY received_at DESC
            LIMIT 1`,
          params,
        ),
      ]);

      const m = metaQ.rows[0] ?? null;
      const meta = m
        ? {
            guid: m.guid,
            systemName: m.system_name,
            wacn: m.wacn,
            nac: m.nac,
            lra: m.lra,
            channelName: m.channel_name,
            controlFrequencyMhz: m.control_frequency_mhz ?? null,
            controlLcn: m.control_lcn,
            affiliatedRadioCount: m.affiliated_radio_count ?? null,
            observationCount: m.observation_count ?? null,
            firstSeenMs: m.site_first_seen_ms !== null ? Number(m.site_first_seen_ms) : null,
            lastSeenMs: m.site_last_seen_ms !== null ? Number(m.site_last_seen_ms) : null,
            status: m.status ?? null,
            channels: Array.isArray(m.channels) ? m.channels : [],
            neighbors: Array.isArray(m.neighbors) ? m.neighbors : [],
            bands: Array.isArray(m.bands) ? m.bands : [],
            quality: m.quality ?? null,
            updatedAt: iso(m.received_at),
          }
        : null;

      return c.json({
        window,
        system,
        rfss,
        site,
        totals: detail.totals,
        // Deep P25 site metadata (null when no node has forwarded it yet).
        meta,
        topTalkgroups: detail.topTalkgroups,
        topRadios: detail.topRadios,
        series: detail.series,
        nodes: nodesQ.rows.map((r) => ({
          id: r.id,
          name: r.name,
          calls: num(r.calls),
          lastSeen: iso(r.last_seen),
        })),
      });
    } catch (err) {
      log.error({ err }, '/api/node-data/site error');
      return c.json({ error: 'failed to load site' }, 500);
    }
  },
);

// ---------------------------------------------------------------------------
// GET /api/node-data/talkgroups
//   ?window=&system=<opt>&q=<opt numeric prefix>&sort=calls|lastSeen
//   &limit=50(≤100)&offset=0
//
// Paged per-talkgroup rollup keyed on (wacn, system, talkgroup). Grouping +
// pagination happen FIRST (cheap grouped scan over the window); lastSite /
// topSite / topNode then resolve via lateral subqueries for the returned
// page's keys only.
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/talkgroups',
  requireRole(canManageNodes),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);
      const systemP = qpIntOpt(url, 'system');
      if (systemP.error) return c.json({ error: systemP.error }, 400);
      const system = systemP.value;
      const qRaw = (url.searchParams.get('q') ?? '').trim();
      if (qRaw && !/^\d+$/.test(qRaw)) {
        return c.json({ error: 'q must be a numeric talkgroup prefix' }, 400);
      }
      const sortRaw = url.searchParams.get('sort') ?? 'calls';
      const sort = (['calls', 'lastSeen'] as const).find((s) => s === sortRaw) ?? 'calls';
      const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') ?? 50) || 50));
      const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

      const params: unknown[] = [WINDOW_INTERVAL[window]];
      const conds = ['received_at >= now() - $1::interval', 'talkgroup IS NOT NULL'];
      if (system !== null) {
        params.push(system);
        conds.push(`system = $${params.length}`);
      }
      if (qRaw) {
        params.push(`${qRaw}%`);
        conds.push(`talkgroup::text LIKE $${params.length}`);
      }
      const where = conds.join(' AND ');
      const orderCol = sort === 'lastSeen' ? 'last_seen' : 'calls';

      const pageParams = [...params];
      pageParams.push(limit);
      const limIdx = pageParams.length;
      pageParams.push(offset);
      const offIdx = pageParams.length;

      const [countQ, pageQ] = await Promise.all([
        pool.query<{ n: unknown }>(
          `SELECT COUNT(DISTINCT (wacn, system, talkgroup))::int AS n
             FROM node_radio_events WHERE ${where}`,
          params,
        ),
        pool.query<{
          wacn: number | null;
          system: number | null;
          talkgroup: number;
          calls: unknown;
          logical: unknown;
          enc: unknown;
          last_seen: Date;
        }>(
          `SELECT wacn, system, talkgroup,
                  COUNT(*)::int AS calls,
                  COUNT(DISTINCT logical_call_id)::int AS logical,
                  (COUNT(*) FILTER (WHERE encrypted))::int AS enc,
                  MAX(received_at) AS last_seen
             FROM node_radio_events
            WHERE ${where}
            GROUP BY wacn, system, talkgroup
            ORDER BY ${orderCol} DESC, talkgroup ASC
            LIMIT $${limIdx} OFFSET $${offIdx}`,
          pageParams,
        ),
      ]);
      const total = num(countQ.rows[0]?.n);
      const page = pageQ.rows;

      // Resolve lastSite/topSite/topNode for the page's keys only (lateral
      // per key; the (system, talkgroup, received_at) index carries these).
      const extras = new Map<
        number,
        {
          lastSite: { rfss: number; site: number } | null;
          topSite: { rfss: number; site: number; calls: number } | null;
          topNode: { id: string; name: string | null; calls: number } | null;
        }
      >();
      if (page.length > 0) {
        const enrich = await pool.query<{
          ord: number;
          last_rfss: number | null;
          last_site: number | null;
          top_rfss: number | null;
          top_site: number | null;
          top_site_calls: unknown;
          top_node_id: string | null;
          top_node_name: string | null;
          top_node_calls: unknown;
        }>(
          `SELECT k.ord::int AS ord,
                  ls.rfss AS last_rfss, ls.site AS last_site,
                  ts.rfss AS top_rfss, ts.site AS top_site, ts.calls AS top_site_calls,
                  tn.node_id AS top_node_id, n.name AS top_node_name, tn.calls AS top_node_calls
             FROM unnest($2::int[], $3::int[], $4::int[])
                  WITH ORDINALITY AS k(wacn, system, talkgroup, ord)
             LEFT JOIN LATERAL (
               SELECT e.site_rfss AS rfss, e.site_id AS site
                 FROM node_radio_events e
                WHERE e.received_at >= now() - $1::interval
                  AND e.wacn IS NOT DISTINCT FROM k.wacn
                  AND e.system IS NOT DISTINCT FROM k.system
                  AND e.talkgroup = k.talkgroup
                  AND e.site_rfss IS NOT NULL AND e.site_id IS NOT NULL
                ORDER BY e.received_at DESC
                LIMIT 1
             ) ls ON true
             LEFT JOIN LATERAL (
               SELECT e.site_rfss AS rfss, e.site_id AS site, COUNT(*)::int AS calls
                 FROM node_radio_events e
                WHERE e.received_at >= now() - $1::interval
                  AND e.wacn IS NOT DISTINCT FROM k.wacn
                  AND e.system IS NOT DISTINCT FROM k.system
                  AND e.talkgroup = k.talkgroup
                  AND e.site_rfss IS NOT NULL AND e.site_id IS NOT NULL
                GROUP BY e.site_rfss, e.site_id
                ORDER BY calls DESC, rfss ASC, site ASC
                LIMIT 1
             ) ts ON true
             LEFT JOIN LATERAL (
               SELECT e.node_id, COUNT(*)::int AS calls
                 FROM node_radio_events e
                WHERE e.received_at >= now() - $1::interval
                  AND e.wacn IS NOT DISTINCT FROM k.wacn
                  AND e.system IS NOT DISTINCT FROM k.system
                  AND e.talkgroup = k.talkgroup
                GROUP BY e.node_id
                ORDER BY calls DESC, e.node_id ASC
                LIMIT 1
             ) tn ON true
             LEFT JOIN nodes n ON n.id = tn.node_id`,
          [
            WINDOW_INTERVAL[window],
            page.map((r) => r.wacn),
            page.map((r) => r.system),
            page.map((r) => r.talkgroup),
          ],
        );
        for (const r of enrich.rows) {
          extras.set(num(r.ord), {
            lastSite:
              r.last_rfss !== null && r.last_site !== null
                ? { rfss: r.last_rfss, site: r.last_site }
                : null,
            topSite:
              r.top_rfss !== null && r.top_site !== null
                ? { rfss: r.top_rfss, site: r.top_site, calls: num(r.top_site_calls) }
                : null,
            topNode:
              r.top_node_id !== null
                ? { id: r.top_node_id, name: r.top_node_name, calls: num(r.top_node_calls) }
                : null,
          });
        }
      }

      const labels = await talkgroupLabels();
      return c.json({
        window,
        total,
        limit,
        offset,
        talkgroups: page.map((r, i) => {
          const ex = extras.get(i + 1); // unnest ordinality is 1-based
          return {
            wacn: r.wacn,
            system: r.system,
            talkgroup: r.talkgroup,
            label: labels.get(r.talkgroup) ?? null,
            calls: num(r.calls),
            logicalCalls: num(r.logical),
            encryptedCalls: num(r.enc),
            lastSeen: iso(r.last_seen),
            lastSite: ex?.lastSite ?? null,
            topSite: ex?.topSite ?? null,
            topNode: ex?.topNode ?? null,
          };
        }),
      });
    } catch (err) {
      log.error({ err }, '/api/node-data/talkgroups error');
      return c.json({ error: 'failed to load talkgroups' }, 500);
    }
  },
);

// ---------------------------------------------------------------------------
// GET /api/node-data/radios
//   ?window=&system=<opt>&q=<opt numeric prefix>&sort=calls|lastSeen
//   &limit=50(≤100)&offset=0
//
// Same pattern as /talkgroups, keyed on (wacn, system, source_unit) with
// NULL source_unit excluded; each row also carries its top 3 talkgroups.
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/radios',
  requireRole(canManageNodes),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);
      const systemP = qpIntOpt(url, 'system');
      if (systemP.error) return c.json({ error: systemP.error }, 400);
      const system = systemP.value;
      const qRaw = (url.searchParams.get('q') ?? '').trim();
      if (qRaw && !/^\d+$/.test(qRaw)) {
        return c.json({ error: 'q must be a numeric radio-id prefix' }, 400);
      }
      const sortRaw = url.searchParams.get('sort') ?? 'calls';
      const sort = (['calls', 'lastSeen'] as const).find((s) => s === sortRaw) ?? 'calls';
      const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') ?? 50) || 50));
      const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

      const params: unknown[] = [WINDOW_INTERVAL[window]];
      const conds = ['received_at >= now() - $1::interval', 'source_unit IS NOT NULL'];
      if (system !== null) {
        params.push(system);
        conds.push(`system = $${params.length}`);
      }
      if (qRaw) {
        params.push(`${qRaw}%`);
        conds.push(`source_unit::text LIKE $${params.length}`);
      }
      const where = conds.join(' AND ');
      const orderCol = sort === 'lastSeen' ? 'last_seen' : 'calls';

      const pageParams = [...params];
      pageParams.push(limit);
      const limIdx = pageParams.length;
      pageParams.push(offset);
      const offIdx = pageParams.length;

      const [countQ, pageQ] = await Promise.all([
        pool.query<{ n: unknown }>(
          `SELECT COUNT(DISTINCT (wacn, system, source_unit))::int AS n
             FROM node_radio_events WHERE ${where}`,
          params,
        ),
        pool.query<{
          wacn: number | null;
          system: number | null;
          radio: number;
          alias: string | null;
          calls: unknown;
          last_seen: Date;
        }>(
          // alias: the most-recent non-null OTA/talker alias for this radio
          // (source_alias from vce); null when none was ever captured.
          `SELECT wacn, system, source_unit AS radio,
                  (array_agg(source_alias ORDER BY received_at DESC)
                     FILTER (WHERE source_alias IS NOT NULL))[1] AS alias,
                  COUNT(*)::int AS calls,
                  MAX(received_at) AS last_seen
             FROM node_radio_events
            WHERE ${where}
            GROUP BY wacn, system, source_unit
            ORDER BY ${orderCol} DESC, radio ASC
            LIMIT $${limIdx} OFFSET $${offIdx}`,
          pageParams,
        ),
      ]);
      const total = num(countQ.rows[0]?.n);
      const page = pageQ.rows;

      const extras = new Map<
        number,
        {
          lastSite: { rfss: number; site: number } | null;
          topSite: { rfss: number; site: number; calls: number } | null;
          topNode: { id: string; name: string | null; calls: number } | null;
          topTalkgroups: Array<{ talkgroup: number; calls: number }>;
        }
      >();
      if (page.length > 0) {
        const enrich = await pool.query<{
          ord: number;
          last_rfss: number | null;
          last_site: number | null;
          top_rfss: number | null;
          top_site: number | null;
          top_site_calls: unknown;
          top_node_id: string | null;
          top_node_name: string | null;
          top_node_calls: unknown;
          top_tgs: Array<{ talkgroup: number; calls: number }>;
        }>(
          `SELECT k.ord::int AS ord,
                  ls.rfss AS last_rfss, ls.site AS last_site,
                  ts.rfss AS top_rfss, ts.site AS top_site, ts.calls AS top_site_calls,
                  tn.node_id AS top_node_id, n.name AS top_node_name, tn.calls AS top_node_calls,
                  tg.tgs AS top_tgs
             FROM unnest($2::int[], $3::int[], $4::int[])
                  WITH ORDINALITY AS k(wacn, system, radio, ord)
             LEFT JOIN LATERAL (
               SELECT e.site_rfss AS rfss, e.site_id AS site
                 FROM node_radio_events e
                WHERE e.received_at >= now() - $1::interval
                  AND e.wacn IS NOT DISTINCT FROM k.wacn
                  AND e.system IS NOT DISTINCT FROM k.system
                  AND e.source_unit = k.radio
                  AND e.site_rfss IS NOT NULL AND e.site_id IS NOT NULL
                ORDER BY e.received_at DESC
                LIMIT 1
             ) ls ON true
             LEFT JOIN LATERAL (
               SELECT e.site_rfss AS rfss, e.site_id AS site, COUNT(*)::int AS calls
                 FROM node_radio_events e
                WHERE e.received_at >= now() - $1::interval
                  AND e.wacn IS NOT DISTINCT FROM k.wacn
                  AND e.system IS NOT DISTINCT FROM k.system
                  AND e.source_unit = k.radio
                  AND e.site_rfss IS NOT NULL AND e.site_id IS NOT NULL
                GROUP BY e.site_rfss, e.site_id
                ORDER BY calls DESC, rfss ASC, site ASC
                LIMIT 1
             ) ts ON true
             LEFT JOIN LATERAL (
               SELECT e.node_id, COUNT(*)::int AS calls
                 FROM node_radio_events e
                WHERE e.received_at >= now() - $1::interval
                  AND e.wacn IS NOT DISTINCT FROM k.wacn
                  AND e.system IS NOT DISTINCT FROM k.system
                  AND e.source_unit = k.radio
                GROUP BY e.node_id
                ORDER BY calls DESC, e.node_id ASC
                LIMIT 1
             ) tn ON true
             LEFT JOIN LATERAL (
               SELECT COALESCE(
                        jsonb_agg(jsonb_build_object('talkgroup', t.talkgroup, 'calls', t.calls)
                                  ORDER BY t.calls DESC, t.talkgroup ASC),
                        '[]'::jsonb) AS tgs
                 FROM (
                   SELECT e.talkgroup, COUNT(*)::int AS calls
                     FROM node_radio_events e
                    WHERE e.received_at >= now() - $1::interval
                      AND e.wacn IS NOT DISTINCT FROM k.wacn
                      AND e.system IS NOT DISTINCT FROM k.system
                      AND e.source_unit = k.radio
                      AND e.talkgroup IS NOT NULL
                    GROUP BY e.talkgroup
                    ORDER BY calls DESC, e.talkgroup ASC
                    LIMIT 3
                 ) t
             ) tg ON true
             LEFT JOIN nodes n ON n.id = tn.node_id`,
          [
            WINDOW_INTERVAL[window],
            page.map((r) => r.wacn),
            page.map((r) => r.system),
            page.map((r) => r.radio),
          ],
        );
        for (const r of enrich.rows) {
          extras.set(num(r.ord), {
            lastSite:
              r.last_rfss !== null && r.last_site !== null
                ? { rfss: r.last_rfss, site: r.last_site }
                : null,
            topSite:
              r.top_rfss !== null && r.top_site !== null
                ? { rfss: r.top_rfss, site: r.top_site, calls: num(r.top_site_calls) }
                : null,
            topNode:
              r.top_node_id !== null
                ? { id: r.top_node_id, name: r.top_node_name, calls: num(r.top_node_calls) }
                : null,
            topTalkgroups: Array.isArray(r.top_tgs) ? r.top_tgs : [],
          });
        }
      }

      return c.json({
        window,
        total,
        limit,
        offset,
        radios: page.map((r, i) => {
          const ex = extras.get(i + 1);
          return {
            wacn: r.wacn,
            system: r.system,
            radio: r.radio,
            alias: r.alias ?? null,
            calls: num(r.calls),
            lastSeen: iso(r.last_seen),
            lastSite: ex?.lastSite ?? null,
            topSite: ex?.topSite ?? null,
            topNode: ex?.topNode ?? null,
            topTalkgroups: ex?.topTalkgroups ?? [],
          };
        }),
      });
    } catch (err) {
      log.error({ err }, '/api/node-data/radios error');
      return c.json({ error: 'failed to load radios' }, 500);
    }
  },
);

// ===========================================================================
// Pager monitoring.
//
// These read node_pager_events (30-day detail) only, so the window is
// 24h|7d|30d with a 7d default — there is no 'all'. A page heard by N nodes
// shares one logical_id (stamped to the first row's id); COALESCE(logical_id,
// id) treats a not-yet-stamped row as its own group. Capcodes are labelled at
// read time from the capcodeAliases() reference map (display-only), matched on
// the normalised capcode so zero-padding differences don't split a page.
// ===========================================================================

/** topNode-per-capcode lateral, shared by pager-overview and capcodes. */
async function pagerTopNodeByCapcode(
  pool: Pool,
  interval: string,
  capcodes: string[],
): Promise<Map<string, { id: string; name: string | null; pages: number }>> {
  const out = new Map<string, { id: string; name: string | null; pages: number }>();
  if (capcodes.length === 0) return out;
  const res = await pool.query<{
    capcode: string;
    node_id: string | null;
    name: string | null;
    pages: unknown;
  }>(
    `SELECT k.capcode, tn.node_id, n.name, tn.pages
       FROM unnest($2::text[]) AS k(capcode)
       LEFT JOIN LATERAL (
         SELECT e.node_id, COUNT(*)::int AS pages
           FROM node_pager_events e
          WHERE e.received_at >= now() - $1::interval
            AND e.capcode = k.capcode
          GROUP BY e.node_id
          ORDER BY pages DESC, e.node_id ASC
          LIMIT 1
       ) tn ON true
       LEFT JOIN nodes n ON n.id = tn.node_id`,
    [interval, capcodes],
  );
  for (const r of res.rows) {
    if (r.node_id !== null) {
      out.set(r.capcode, { id: r.node_id, name: r.name, pages: num(r.pages) });
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// GET /api/node-data/pager-overview?window=24h|7d|30d
//   Totals, top capcodes (with their busiest node), top nodes, hourly series.
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/pager-overview',
  requireRole(canManageNodes),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);
      const interval = WINDOW_INTERVAL[window];

      const [totalsQ, topCapsQ, topNodesQ, seriesQ] = await Promise.all([
        pool.query<{ pages: unknown; logical: unknown; nodes: unknown; capcodes: unknown }>(
          `SELECT COUNT(*)::int AS pages,
                  COUNT(DISTINCT COALESCE(logical_id, id))::int AS logical,
                  COUNT(DISTINCT node_id)::int AS nodes,
                  COUNT(DISTINCT capcode)::int AS capcodes
             FROM node_pager_events
            WHERE received_at >= now() - $1::interval`,
          [interval],
        ),
        pool.query<{ capcode: string; pages: unknown; last_seen: Date }>(
          `SELECT capcode, COUNT(*)::int AS pages, MAX(received_at) AS last_seen
             FROM node_pager_events
            WHERE received_at >= now() - $1::interval
            GROUP BY capcode
            ORDER BY pages DESC, capcode ASC
            LIMIT 15`,
          [interval],
        ),
        pool.query<{ node_id: string; name: string | null; pages: unknown }>(
          `SELECT e.node_id, n.name, COUNT(*)::int AS pages
             FROM node_pager_events e
             JOIN nodes n ON n.id = e.node_id
            WHERE e.received_at >= now() - $1::interval
            GROUP BY e.node_id, n.name
            ORDER BY pages DESC, e.node_id ASC
            LIMIT 15`,
          [interval],
        ),
        pool.query<{ hour: Date; pages: unknown }>(
          `SELECT date_trunc('hour', received_at) AS hour, COUNT(*)::int AS pages
             FROM node_pager_events
            WHERE received_at >= now() - $1::interval
            GROUP BY 1 ORDER BY 1`,
          [interval],
        ),
      ]);

      const topNodeByCap = await pagerTopNodeByCapcode(
        pool,
        interval,
        topCapsQ.rows.map((r) => r.capcode),
      );
      const aliases = capcodeAliases();
      const t = totalsQ.rows[0];

      return c.json({
        window,
        totals: {
          pages: num(t?.pages),
          pagesLogical: num(t?.logical),
          activeNodes: num(t?.nodes),
          capcodes: num(t?.capcodes),
        },
        topCapcodes: topCapsQ.rows.map((r) => {
          const a = aliases.get(normalizeCapcode(r.capcode)) ?? null;
          return {
            capcode: r.capcode,
            alias: a?.alias ?? null,
            agency: a?.agency ?? null,
            pages: num(r.pages),
            lastSeen: iso(r.last_seen),
            topNode: topNodeByCap.get(r.capcode) ?? null,
          };
        }),
        topNodes: topNodesQ.rows.map((r) => ({
          nodeId: r.node_id,
          name: r.name,
          pages: num(r.pages),
        })),
        series: seriesQ.rows.map((r) => ({ hour: iso(r.hour), pages: num(r.pages) })),
      });
    } catch (err) {
      log.error({ err }, '/api/node-data/pager-overview error');
      return c.json({ error: 'failed to load pager overview' }, 500);
    }
  },
);

// ---------------------------------------------------------------------------
// GET /api/node-data/capcodes
//   ?window=&q=<opt capcode prefix>&sort=pages|lastSeen&limit=50(≤100)&offset=0
//
// Paged per-capcode rollup. Grouping + pagination first, then topNode resolves
// per returned capcode via lateral — same shape as the talkgroups endpoint.
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/capcodes',
  requireRole(canManageNodes),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);
      const interval = WINDOW_INTERVAL[window];
      const qRaw = (url.searchParams.get('q') ?? '').trim();
      if (qRaw && !/^\d+$/.test(qRaw)) {
        return c.json({ error: 'q must be a numeric capcode prefix' }, 400);
      }
      const sortRaw = url.searchParams.get('sort') ?? 'pages';
      const sort = (['pages', 'lastSeen'] as const).find((s) => s === sortRaw) ?? 'pages';
      const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') ?? 50) || 50));
      const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

      const params: unknown[] = [interval];
      const conds = ['received_at >= now() - $1::interval'];
      if (qRaw) {
        params.push(`${qRaw}%`);
        conds.push(`capcode LIKE $${params.length}`);
      }
      const where = conds.join(' AND ');
      const orderCol = sort === 'lastSeen' ? 'last_seen' : 'pages';

      const pageParams = [...params];
      pageParams.push(limit);
      const limIdx = pageParams.length;
      pageParams.push(offset);
      const offIdx = pageParams.length;

      const [countQ, pageQ] = await Promise.all([
        pool.query<{ n: unknown }>(
          `SELECT COUNT(DISTINCT capcode)::int AS n
             FROM node_pager_events WHERE ${where}`,
          params,
        ),
        pool.query<{ capcode: string; pages: unknown; last_seen: Date }>(
          `SELECT capcode, COUNT(*)::int AS pages, MAX(received_at) AS last_seen
             FROM node_pager_events
            WHERE ${where}
            GROUP BY capcode
            ORDER BY ${orderCol} DESC, capcode ASC
            LIMIT $${limIdx} OFFSET $${offIdx}`,
          pageParams,
        ),
      ]);
      const total = num(countQ.rows[0]?.n);
      const page = pageQ.rows;

      const topNodeByCap = await pagerTopNodeByCapcode(
        pool,
        interval,
        page.map((r) => r.capcode),
      );
      const aliases = capcodeAliases();

      return c.json({
        total,
        limit,
        offset,
        capcodes: page.map((r) => {
          const a = aliases.get(normalizeCapcode(r.capcode)) ?? null;
          return {
            capcode: r.capcode,
            alias: a?.alias ?? null,
            agency: a?.agency ?? null,
            pages: num(r.pages),
            lastSeen: iso(r.last_seen),
            topNode: topNodeByCap.get(r.capcode) ?? null,
          };
        }),
      });
    } catch (err) {
      log.error({ err }, '/api/node-data/capcodes error');
      return c.json({ error: 'failed to load capcodes' }, 500);
    }
  },
);

// ---------------------------------------------------------------------------
// GET /api/node-data/capcode?capcode=<exact>&window=&limit=50(≤100)&offset=0
//
// Message browser for one capcode. Grouped by logical_id (COALESCE(logical_id,
// id)) so each distinct page shows once with its reception count + node chips,
// newest first.
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/capcode',
  requireRole(canManageNodes),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);
      const interval = WINDOW_INTERVAL[window];
      const capcode = (url.searchParams.get('capcode') ?? '').trim();
      if (!capcode) return c.json({ error: 'capcode is required' }, 400);
      const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') ?? 50) || 50));
      const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

      const [countQ, pageQ] = await Promise.all([
        pool.query<{ n: unknown }>(
          `SELECT COUNT(DISTINCT COALESCE(logical_id, id))::int AS n
             FROM node_pager_events
            WHERE received_at >= now() - $1::interval AND capcode = $2`,
          [interval, capcode],
        ),
        pool.query<{
          at: Date;
          message: string | null;
          freq_mhz: number | null;
          receptions: number;
          nodes: Array<{ id: string; name: string }>;
        }>(
          `SELECT MIN(e.received_at) AS at,
                  MIN(e.message) AS message,
                  MIN(e.freq_mhz)::float8 AS freq_mhz,
                  COUNT(*)::int AS receptions,
                  jsonb_agg(DISTINCT jsonb_build_object('id', e.node_id, 'name', n.name)) AS nodes
             FROM node_pager_events e
             JOIN nodes n ON n.id = e.node_id
            WHERE e.received_at >= now() - $1::interval AND e.capcode = $2
            GROUP BY COALESCE(e.logical_id, e.id)
            ORDER BY at DESC
            LIMIT $3 OFFSET $4`,
          [interval, capcode, limit, offset],
        ),
      ]);

      const total = num(countQ.rows[0]?.n);
      const a = capcodeAliases().get(normalizeCapcode(capcode)) ?? null;
      return c.json({
        total,
        capcode,
        alias: a?.alias ?? null,
        agency: a?.agency ?? null,
        messages: pageQ.rows.map((r) => ({
          at: iso(r.at),
          message: r.message,
          freqMhz: r.freq_mhz,
          nodes: r.nodes,
          receptions: r.receptions,
        })),
      });
    } catch (err) {
      log.error({ err }, '/api/node-data/capcode error');
      return c.json({ error: 'failed to load capcode' }, 500);
    }
  },
);
