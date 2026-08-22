/**
 * Staff "Data" tab — feeder-node event analytics (owner|dev|node_monitor).
 * Every route here is a read, gated with requireRole(canViewNodeData).
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
import { learnedAliasMap } from '../services/capcodeAliasSync.js';
import { requireRole, canViewNodeData } from '../services/auth/roles.js';
import { hub } from '../services/nodes/hub.js';
import {
  talkgroupCatalog,
  talkgroupLabels,
  talkgroupAgencies,
  talkgroupColors,
  radioDisplay,
} from '../services/talkgroupCatalog.js';
import { shapeNodeLive, sortLiveChannels } from '../services/nodeLive.js';
import { liveCallWindow } from '../services/nodeCallWindow.js';

/**
 * Talkgroup label / agency / colour lookups live in services/talkgroupCatalog
 * so the live WebSocket push (services/nodeLive.ts) can share the same ~60s
 * cache instead of importing this router back and forming a cycle.
 */

/**
 * Site (system, rfss, site) → friendly name (channel_name from the latest
 * node_site_snapshots row, e.g. "Cambewarra MT"). Same display-only pattern
 * (and ~60s cache) as talkgroupLabels. Keys are "system:rfss:site" plus an
 * "rfss:site" fallback for callers that don't have the system id in scope
 * (e.g. the overview topSites rollup).
 */
let _siteNameCache: { at: number; map: Map<string, string> } | null = null;
async function siteNames(pool: Pool): Promise<Map<string, string>> {
  if (_siteNameCache && Date.now() - _siteNameCache.at < 60_000) return _siteNameCache.map;
  const map = new Map<string, string>();
  try {
    const res = await pool.query<{
      system_id: number;
      rfss: number;
      site_id: number;
      channel_name: string;
    }>(
      `SELECT DISTINCT ON (system_id, rfss, site_id)
              system_id, rfss, site_id, channel_name
         FROM node_site_snapshots
        WHERE channel_name IS NOT NULL
        ORDER BY system_id, rfss, site_id, received_at DESC`,
    );
    for (const r of res.rows) {
      map.set(`${r.system_id}:${r.rfss}:${r.site_id}`, r.channel_name);
      const fallback = `${r.rfss}:${r.site_id}`;
      if (!map.has(fallback)) map.set(fallback, r.channel_name);
    }
  } catch (e) {
    log.warn({ err: e }, 'siteNames: failed to load site snapshots');
  }
  _siteNameCache = { at: Date.now(), map };
  return map;
}

/** Resolve a site name from the siteNames() map (system key, then fallback). */
function siteNameFor(
  map: Map<string, string>,
  system: number | null | undefined,
  rfss: number | null | undefined,
  site: number | null | undefined,
): string | null {
  if (rfss == null || site == null) return null;
  if (system != null) {
    const hit = map.get(`${system}:${rfss}:${site}`);
    if (hit !== undefined) return hit;
  }
  return map.get(`${rfss}:${site}`) ?? null;
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

/**
 * Merged capcode -> {alias, agency} lookup: aliases LEARNED from Pagermon
 * (pager_capcode_aliases, kept current by services/capcodeAliasSync.ts) layered
 * over the static CSV. Pagermon wins because it's the operator's live source of
 * truth — the CSV is a point-in-time export that covers only a subset of the
 * capcodes the nodes actually receive. Callers still render the capcode itself
 * and only add the alias when one resolves.
 */
async function capcodeAliasLookup(
  pool: Pool,
): Promise<Map<string, { alias: string; agency: string | null }>> {
  const merged = new Map(capcodeAliases());
  for (const [capcode, v] of await learnedAliasMap(pool)) merged.set(capcode, v);
  return merged;
}

export const nodeDataRouter = new Hono();

type Windows = '24h' | '7d' | '30d' | 'all';

const WINDOW_INTERVAL: Record<Exclude<Windows, 'all'>, string> = {
  '24h': '24 hours',
  '7d': '7 days',
  '30d': '30 days',
};

/**
 * A real P25 talkgroup id is 16-bit (1..65535). Ingest stores every event's
 * `target` in the talkgroup column regardless of kind (nodeEvents.ts), so
 * unit/data calls drop 7-digit RADIO IDs there and they masquerade as bogus
 * "TG 2315291" talkgroups. Gate EVERY talkgroup list/count on this predicate
 * so radio ids stop showing up as talkgroups. (The general call/radio counts
 * are NOT filtered — unit/data calls are still real calls.) Prefixable ('' or
 * 'e.') to match queries that alias the events table, mirroring the scope
 * helpers. `TG_VALID` is the bare (unprefixed) form.
 */
const tgValid = (prefix = ''): string => `${prefix}talkgroup BETWEEN 1 AND 65535`;
const TG_VALID = tgValid();

/**
 * "This row is a talkgroup VOICE call." node_radio_events stores EVERY P25
 * activity event the vce feed emits — voice AND data/signaling — and each
 * event's `target` lands in the `talkgroup` column regardless of kind
 * (nodeEvents.ts). Only CALL_GROUP / CALL_GROUP_ENCRYPTED events are real
 * talkgroup voice calls (their target is a TALKGROUP); DATA_CALL / RESPONSE /
 * QUERY / PAGE etc. target a RADIO id and masquerade as bogus "TG 2315291"
 * talkgroups. This is the PRIMARY call filter for the Data page's radio side:
 * a "call" on this page = a CALL_GROUP% event, so data/signaling stops being
 * counted as calls or talkgroups (mirrors vce, which lists Calls separately
 * from signaling observations). TG_VALID stays as a secondary guard wherever a
 * talkgroup is grouped/counted. event_type is stored UPPERCASE by the vce feed
 * (the Go agent forwards it verbatim); upper() is a zero-cost guard against
 * case drift. Prefixable ('' or 'e.') to match queries that alias the events
 * table; `CALL_GROUP` is the bare (unprefixed) form. NULL event_type (none post
 * migration 044) is treated as not-a-call — LIKE over NULL is not true.
 *
 * CALL_PATCH_GROUP has to be spelled out separately: it does NOT start with
 * "CALL_GROUP", so a single prefix match silently dropped every patched call —
 * 34k events over 5.5k calls here, about a tenth of the voice traffic, missing
 * from call counts, talkgroup rollups and radio rollups alike. They are real
 * voice calls on a patched talkgroup, and they overlap a plain CALL_GROUP on
 * the same logical call 5 times in 400k rows, so counting them adds calls
 * rather than double-counting existing ones.
 *
 * It is also why no radio had an over-the-air alias: essentially every alias
 * this network transmits arrives on a patch call, so the rollups excluded all
 * of them.
 */
const callGroup = (prefix = ''): string =>
  `(upper(${prefix}event_type) LIKE 'CALL_GROUP%'` +
  ` OR upper(${prefix}event_type) LIKE 'CALL_PATCH_GROUP%')`;
const CALL_GROUP = callGroup();

/** Parse a query param as a non-negative int, else null. */
function qpInt(url: URL, name: string): number | null {
  const raw = url.searchParams.get(name);
  if (raw === null || raw === '' || !/^\d+$/.test(raw)) return null;
  return Number.parseInt(raw, 10);
}

/**
 * Optional per-node scope (Radio mode). `?node=<id>` restricts every
 * node_radio_events query to that one feeder node; absent/empty = fleet-wide
 * (all nodes). Parameterised at the call site so the raw id never touches SQL.
 */
function qpNode(url: URL): string | null {
  return (url.searchParams.get('node') ?? '').trim() || null;
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
  requireRole(canViewNodeData),
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
      // Optional per-node scope (Radio mode). Applied to node_radio_events and
      // node_radio_hourly (both carry node_id); node_radio_hourly_sys has no
      // node_id, so window=all's radioLogical + topSites stay fleet-wide. Pager
      // queries are never node-filtered (the selector is radio-only).
      const nodeId = qpNode(url);

      // Node metadata for perNode name/kind (small table, one read).
      const nodesRes = await pool.query<{ id: string; name: string; kind: string }>(
        `SELECT id, name, kind FROM nodes`,
      );
      const nodeMeta = new Map(nodesRes.rows.map((r) => [r.id, r]));

      let radioRaw = 0;
      let radioLogical = 0;
      // Counted over DISTINCT logical calls, so both are directly comparable
      // to radioLogical (never to the larger per-reception radioRaw).
      let radioEncrypted = 0;
      let radioRecorded = 0;
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
      let topUnitRows: Array<{ unit: number; alias: string | null; calls: unknown }> = [];
      let topSiteRows: Array<{ site_rfss: number; site_id: number; calls: unknown }> = [];
      let seriesRadioRows: Array<{ bucket: Date; n: unknown }> = [];
      let seriesPagerRows: Array<{ bucket: Date; n: unknown }> = [];

      if (window === 'all') {
        // Forever path: hourly bucket tables (topUnits only exists in
        // detail, so it stays capped to the last 30 days).
        // window=all RADIO metrics are re-sourced from node_radio_events with
        // the CALL_GROUP filter — NOT the hourly rollups. The rollups
        // (node_radio_hourly / node_radio_hourly_sys) have no event_type column
        // and bucket EVERY ingested event (data/signaling included), so reading
        // them counts non-calls as calls. node_radio_events is 30-day retained,
        // so `all` radio is effectively capped to 30 days (radioWindowCapped is
        // set on the response) — the same tradeoff already accepted for
        // topUnits and the receptions top-list. Pager `all` still reads its
        // forever rollup (node_pager_hourly), which is unaffected by P25
        // signaling. Optional node scope ($1) applies via nAllAnd.
        const nAllAnd = nodeId !== null ? ` AND node_id = $1` : '';
        const nAllParams: unknown[] = nodeId !== null ? [nodeId] : [];
        const [radioRawQ, radioLogQ, pagerTotQ, pnR, pnP, tg, un, si, sr, sp] =
          await Promise.all([
            wantRadio
              ? pool.query<{ raw: unknown }>(
                  `SELECT COUNT(*)::int AS raw FROM node_radio_events
                    WHERE ${CALL_GROUP}${nAllAnd}`,
                  nAllParams,
                )
              : null,
            wantRadio
              ? pool.query<{ logical: unknown; encrypted: unknown; recorded: unknown }>(
                  // Encrypted/recorded ride along on this scan, exactly as they
                  // do on the detail path. Left out, they defaulted to 0 and the
                  // tiles asserted "0% encrypted, 0 of N calls" for window=all —
                  // stating that none of the fleet's traffic is encrypted when
                  // most of it is. They cost nothing here: window=all radio
                  // already reads node_radio_events (the hourly rollups have no
                  // event_type and would count signaling as calls), so the rows
                  // are being counted anyway.
                  `SELECT COUNT(DISTINCT logical_call_id)::int AS logical,
                          COUNT(DISTINCT logical_call_id)
                            FILTER (WHERE encrypted)::int AS encrypted,
                          COUNT(DISTINCT logical_call_id)
                            FILTER (WHERE recorded)::int AS recorded
                     FROM node_radio_events WHERE ${CALL_GROUP}${nAllAnd}`,
                  nAllParams,
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
                  `SELECT node_id, COUNT(*)::int AS calls,
                          COALESCE(SUM(audio_bytes), 0)::bigint AS bytes
                     FROM node_radio_events
                    WHERE ${CALL_GROUP}${nAllAnd} GROUP BY node_id`,
                  nAllParams,
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
                  // Receptions = a distinct (logical call, node, site) that
                  // heard the call — NOT raw rows (the vce feed emits GRANT+CALL
                  // per site, so COUNT(*) over-counts ~2x and by site). Sourced
                  // from node_radio_events even for window=all (no interval) so
                  // the distinct-count is expressible; cheap at LIMIT 15.
                  // label deliberately NULL: activity-event ingest stores no
                  // labels (they'll resolve from the agencies config later).
                  `SELECT system, talkgroup,
                          COUNT(DISTINCT (logical_call_id, node_id,
                            COALESCE(site_rfss, -1), COALESCE(site_id, -1)))::int AS calls,
                          COUNT(DISTINCT logical_call_id)::int AS logical,
                          NULL::text AS label
                     FROM node_radio_events
                    WHERE ${CALL_GROUP} AND ${TG_VALID}${nAllAnd}
                    GROUP BY system, talkgroup
                    ORDER BY calls DESC LIMIT 15`,
                  nAllParams,
                )
              : null,
            wantRadio
              ? pool.query<{ unit: number; alias: string | null; calls: unknown }>(
                  `SELECT source_unit AS unit,
                          (array_agg(source_alias ORDER BY received_at DESC)
                             FILTER (WHERE source_alias IS NOT NULL))[1] AS alias,
                          COUNT(*)::int AS calls
                     FROM node_radio_events
                    WHERE received_at >= now() - interval '30 days'
                      AND ${CALL_GROUP}
                      AND source_unit IS NOT NULL${nAllAnd}
                    GROUP BY source_unit ORDER BY calls DESC LIMIT 15`,
                  nAllParams,
                )
              : null,
            wantRadio
              ? pool.query<{ site_rfss: number; site_id: number; calls: unknown }>(
                  `SELECT site_rfss, site_id, COUNT(*)::int AS calls
                     FROM node_radio_events
                    WHERE ${CALL_GROUP}
                      AND site_rfss IS NOT NULL AND site_id IS NOT NULL${nAllAnd}
                    GROUP BY site_rfss, site_id ORDER BY calls DESC LIMIT 15`,
                  nAllParams,
                )
              : null,
            wantRadio
              ? pool.query<{ bucket: Date; n: unknown }>(
                  `SELECT date_trunc('day', received_at) AS bucket, COUNT(*)::int AS n
                     FROM node_radio_events
                    WHERE ${CALL_GROUP}${nAllAnd} GROUP BY 1 ORDER BY 1`,
                  nAllParams,
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
        radioEncrypted = num(radioLogQ?.rows[0]?.encrypted);
        radioRecorded = num(radioLogQ?.rows[0]?.recorded);
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
        // Radio queries additionally scope to the selected node ($2) when set;
        // pager queries keep the fleet-wide `cond`/`[iv]`.
        const radioCond = nodeId !== null ? `${cond} AND node_id = $2` : cond;
        const radioParams: unknown[] = nodeId !== null ? [iv, nodeId] : [iv];
        const [rTot, pTot, pnR, pnP, tg, un, si, sr, sp] = await Promise.all([
          wantRadio
            ? pool.query<{ raw: unknown; logical: unknown; encrypted: unknown; recorded: unknown }>(
                // raw = call-group receptions, logical = distinct calls. Both
                // gated on CALL_GROUP so data/signaling isn't counted as calls.
                // Encrypted/recorded ride along on the SAME scan rather than
                // as extra queries: both are plain columns on the rows already
                // being counted. Encrypted matters because an encrypted call is
                // counted like any other yet is never listenable, so a bare
                // call total overstates what the fleet actually captured;
                // recorded is the audio-coverage counterpart (it drives the
                // per-event speaker icon) and exposes traffic channels that
                // aren't being followed. Both are DISTINCT over logical calls
                // so they compare like-for-like against `logical`, not against
                // the larger per-reception `raw`.
                `SELECT COUNT(*)::int AS raw,
                        COUNT(DISTINCT logical_call_id)::int AS logical,
                        COUNT(DISTINCT logical_call_id)
                          FILTER (WHERE encrypted)::int AS encrypted,
                        COUNT(DISTINCT logical_call_id)
                          FILTER (WHERE recorded)::int AS recorded
                   FROM node_radio_events WHERE ${radioCond} AND ${CALL_GROUP}`,
                radioParams,
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
                   FROM node_radio_events
                  WHERE ${radioCond} AND ${CALL_GROUP} GROUP BY node_id`,
                radioParams,
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
                // Receptions (calls) = distinct (logical call, node, site) that
                // heard the call — de-dupes the vce GRANT+CALL double-emit and
                // counts one per receiving node/site. logical = distinct calls.
                // label deliberately NULL: activity-event ingest stores no
                // labels (they'll resolve from the agencies config later).
                `SELECT e.system, e.talkgroup,
                        COUNT(DISTINCT (e.logical_call_id, e.node_id,
                          COALESCE(e.site_rfss, -1), COALESCE(e.site_id, -1)))::int AS calls,
                        COUNT(DISTINCT e.logical_call_id)::int AS logical,
                        NULL::text AS label
                   FROM node_radio_events e
                  WHERE ${radioCond} AND ${callGroup('e.')} AND ${tgValid('e.')}
                  GROUP BY e.system, e.talkgroup
                  ORDER BY calls DESC LIMIT 15`,
                radioParams,
              )
            : null,
          wantRadio
            ? pool.query<{ unit: number; alias: string | null; calls: unknown }>(
                `SELECT source_unit AS unit,
                        (array_agg(source_alias ORDER BY received_at DESC)
                           FILTER (WHERE source_alias IS NOT NULL))[1] AS alias,
                        COUNT(*)::int AS calls
                   FROM node_radio_events
                  WHERE ${radioCond} AND ${CALL_GROUP} AND source_unit IS NOT NULL
                  GROUP BY source_unit ORDER BY calls DESC LIMIT 15`,
                radioParams,
              )
            : null,
          wantRadio
            ? pool.query<{ site_rfss: number; site_id: number; calls: unknown }>(
                `SELECT site_rfss, site_id, COUNT(*)::int AS calls
                   FROM node_radio_events
                  WHERE ${radioCond} AND ${CALL_GROUP}
                    AND site_rfss IS NOT NULL AND site_id IS NOT NULL
                  GROUP BY site_rfss, site_id ORDER BY calls DESC LIMIT 15`,
                radioParams,
              )
            : null,
          wantRadio
            ? pool.query<{ bucket: Date; n: unknown }>(
                `SELECT date_trunc('hour', received_at) AS bucket, COUNT(*)::int AS n
                   FROM node_radio_events
                  WHERE ${radioCond} AND ${CALL_GROUP} GROUP BY 1 ORDER BY 1`,
                radioParams,
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
        radioEncrypted = num(rTot?.rows[0]?.encrypted);
        radioRecorded = num(rTot?.rows[0]?.recorded);
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
      const tgAgencies = await talkgroupAgencies();
      const tgColors = await talkgroupColors();
      const radioDisp = await radioDisplay();
      const siteMap = await siteNames(pool);
      const body: Record<string, unknown> = {
        window,
        scope,
        node: nodeId,
        totals: {
          radioRaw,
          radioLogical,
          radioEncrypted,
          radioRecorded,
          pages,
          pagesLogical,
          activeNodes: perNodeMap.size,
        },
        perNode,
        topTalkgroups: topTgRows.map((r) => ({
          system: r.system,
          talkgroup: r.talkgroup,
          label: r.label ?? (r.talkgroup !== null ? tgLabels.get(r.talkgroup) ?? null : null),
          agency: r.talkgroup !== null ? tgAgencies.get(r.talkgroup) ?? null : null,
          color: r.talkgroup !== null ? tgColors.get(r.talkgroup) ?? null : null,
          calls: num(r.calls),
          logicalCalls: num(r.logical),
        })),
        topUnits: topUnitRows.map((r) => ({
          unit: r.unit,
          // A radio can have BOTH: `ota` is the alias it transmits over the air
          // (P25 talker alias), `alias` is what the operator named it in the
          // agency's unit list. Separate columns — neither replaces the other.
          ota: r.alias ?? null,
          alias: radioDisp.labels.get(r.unit) ?? null,
          agency: radioDisp.agencies.get(r.unit) ?? null,
          color: radioDisp.colors.get(r.unit) ?? null,
          calls: num(r.calls),
        })),
        topSites: topSiteRows.map((r) => ({
          siteRfss: r.site_rfss,
          siteId: r.site_id,
          // These rollup rows carry no system id — resolve via the
          // "rfss:site" fallback key.
          name: siteNameFor(siteMap, null, r.site_rfss, r.site_id),
          calls: num(r.calls),
        })),
        series,
      };
      // topUnits can only come from the 30-day detail window — flag the cap
      // so the UI can annotate it when the rest of the page shows all-time.
      // radioWindowCapped: on window=all the ENTIRE radio side (totals,
      // perNode, top-lists, series) is now sourced from the 30-day detail
      // table with the CALL_GROUP filter rather than the forever rollups
      // (which have no event_type column and can't exclude data/signaling), so
      // radio all-time is likewise capped to 30 days. Pager all-time stays
      // forever.
      if (window === 'all') {
        body['unitsWindowCapped'] = true;
        body['radioWindowCapped'] = true;
      }
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
  requireRole(canViewNodeData),
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
      // CALL_GROUP: the events list shows only real talkgroup voice calls —
      // never DATA_CALL/RESPONSE/QUERY/PAGE signaling (which would render as
      // bogus "TG —" rows targeting a radio id).
      const rConds: string[] = ['logical_call_id IS NOT NULL', CALL_GROUP];
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
        if (encrypted !== null) {
          rConds.push(`encrypted = ${add(encrypted)}`);
          // Hiding encrypted traffic also has to drop talkgroups the playlist
          // declares encrypted — their individual events are often flagged
          // false simply because the decoder had not established it yet.
          if (encrypted === false) {
            const encTgs = await encryptedTalkgroupIds();
            if (encTgs.length) {
              rConds.push(`(talkgroup IS NULL OR talkgroup <> ALL(${add(encTgs)}::int[]))`);
            }
          }
        }
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
              // ONE row per logical_call_id (a group call emits GRANT + CALL/
              // ACTIVE receptions sharing the id). Scalar identity fields come
              // from a REPRESENTATIVE reception — the action='CALL' row if any,
              // else the most recent — via array_agg(... ORDER BY
              // (action='CALL') DESC, received_at DESC)[1]; sites/nodes/
              // receptions/encrypted/recorded still aggregate across ALL
              // receptions. CALL_GROUP guards the hydrate too so only call-group
              // receptions contribute (the page ids are already call-group).
              `SELECT e.logical_call_id::text AS id,
                      MIN(e.received_at) AS at,
                      (array_agg(e.system ORDER BY (e.action = 'CALL') DESC, e.received_at DESC)
                         FILTER (WHERE e.system IS NOT NULL))[1] AS system,
                      (array_agg(e.talkgroup ORDER BY (e.action = 'CALL') DESC, e.received_at DESC)
                         FILTER (WHERE e.talkgroup IS NOT NULL))[1] AS talkgroup,
                      MAX(e.talkgroup_label) AS talkgroup_label,
                      MAX(e.system_label) AS system_label,
                      (array_agg(e.source_unit ORDER BY (e.action = 'CALL') DESC, e.received_at DESC)
                         FILTER (WHERE e.source_unit IS NOT NULL))[1] AS source_unit,
                      (array_agg(e.source_alias ORDER BY e.received_at DESC)
                         FILTER (WHERE e.source_alias IS NOT NULL))[1] AS source_alias,
                      (array_agg(e.frequency ORDER BY (e.action = 'CALL') DESC, e.received_at DESC)
                         FILTER (WHERE e.frequency IS NOT NULL))[1]::bigint AS frequency,
                      (array_agg(e.action ORDER BY (e.action = 'CALL') DESC, e.received_at DESC))[1] AS action,
                      (array_agg(e.event_type ORDER BY (e.action = 'CALL') DESC, e.received_at DESC))[1] AS event_type,
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
                WHERE e.logical_call_id = ANY(${'$1'}::bigint[]) AND ${callGroup('e.')}
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
      const tgColorMap = await talkgroupColors();
      const agencies = await talkgroupAgencies();
      const colors = await talkgroupColors();
      const evAgencies = await talkgroupAgencies();
      const siteMap = radioIds.length > 0 ? await siteNames(pool) : null;
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
              talkgroupColor: d.talkgroup !== null ? tgColorMap.get(d.talkgroup) ?? null : null,
              // Owning agency, from the talkgroup's SDR-Trunk alias group —
              // the events table shows it in its own column, same as every
              // other list that renders a talkgroup.
              agency: d.talkgroup !== null ? evAgencies.get(d.talkgroup) ?? null : null,
              systemLabel: d.system_label,
              sourceUnit: d.source_unit,
              sourceAlias: d.source_alias,
              frequency: d.frequency !== null ? Number(d.frequency) : null,
              action: d.action,
              eventType: d.event_type,
              // A logical call is "encrypted"/"recorded" if ANY reception was.
              encrypted: d.encrypted === true,
              recorded: d.recorded === true,
              sites: d.sites.map((s) => ({
                ...s,
                name: siteMap ? siteNameFor(siteMap, d.system, s.rfss, s.site) : null,
              })),
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

/**
 * Talkgroups declared encrypted — primarily by their AGENCY's encrypted toggle
 * (an SDR-Trunk-only agency like NSW PF, where every talkgroup is encrypted by
 * construction), with the historical label sniff kept as a fallback for
 * configs from before the unified-talkgroup merge (e.g. "065 - NSW PF (ENC)").
 *
 * Hiding encrypted traffic cannot rely on the per-event `encrypted` flag alone:
 * it comes from the decoder and is not set at the instant a call starts, so an
 * always-encrypted talkgroup still produces rows flagged false and they sail
 * straight through a `encrypted = false` filter. The agency toggle, by
 * contrast, is a standing declaration — every call on those talkgroups is
 * encrypted regardless of what any single event managed to decode in time.
 * (The old label regex missed the 135 police talkgroups marked only by their
 * alias GROUP; the flag covers all of them.)
 *
 * Word-boundary matched so a talkgroup merely CONTAINING those letters
 * (e.g. "FENCE", "ENCORE") is not swept up.
 */
async function encryptedTalkgroupIds(): Promise<number[]> {
  const cat = await talkgroupCatalog();
  const out = new Set<number>(cat.encrypted);
  for (const [id, label] of cat.labels) {
    if (/\b(?:ENC|ENCRYPTED|ENCRYPT)\b/i.test(label)) out.add(id);
  }
  return [...out];
}

/**
 * Optional site scope, as the two separate `rfss` + `site` params the site
 * drill-down already holds in its state (NOT the "rfss-site" string /events
 * takes — that shape exists because the events filter is a free-text box).
 * Both or neither: a lone rfss would silently widen the scope to every site
 * in that RFSS, which reads as a bug from the UI.
 */
function qpSiteScope(url: URL): {
  rfss: number | null;
  site: number | null;
  error?: string;
} {
  const r = qpIntOpt(url, 'rfss');
  if (r.error) return { rfss: null, site: null, error: r.error };
  const s = qpIntOpt(url, 'site');
  if (s.error) return { rfss: null, site: null, error: s.error };
  if ((r.value === null) !== (s.value === null)) {
    return { rfss: null, site: null, error: 'rfss and site must be given together' };
  }
  return { rfss: r.value, site: s.value };
}

interface ScopedDetail {
  totals: {
    calls: number;
    logicalCalls: number;
    encryptedCalls: number;
    /** Calls whose audio reached central rdio. The counterpart to
     *  encryptedCalls: encrypted traffic can never have audio, so a
     *  contributor comparing the two can tell "my node isn't uploading" apart
     *  from "this talkgroup is encrypted". Counted over DISTINCT logical calls
     *  so it compares like-for-like against logicalCalls. */
    recordedCalls: number;
    talkgroups: number;
    radios: number;
    sites: number;
    /** Feeder nodes that heard this scope. The drill-downs already list the
     *  receiving nodes; without a count there was no headline figure to match
     *  the table against. */
    nodes: number;
  };
  topTalkgroups: Array<{
    talkgroup: number;
    label: string | null;
    /** Owning agency + alias colour, same source as the paged /talkgroups list.
     *  The drill-down cards render these as their own column / pill, so they
     *  have to travel with every scoped list, not just the paged one. */
    agency: string | null;
    color: string | null;
    calls: number;
    logicalCalls: number;
    encryptedCalls: number;
    lastSeen: string;
  }>;
  topRadios: Array<{
    radio: number;
    /** OTA alias the radio transmitted; `alias` is its configured unit label. */
    ota: string | null;
    alias: string | null;
    agency: string | null;
    color: string | null;
    calls: number;
    lastSeen: string;
  }>;
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
  const _scopedTg = await talkgroupCatalog();
  const _scopedTgLabels = _scopedTg.labels;
  const _scopedRadio = {
    labels: _scopedTg.unitLabels,
    agencies: _scopedTg.unitAgencies,
    colors: _scopedTg.unitColors,
  };
  const [totQ, tgQ, unQ, srQ] = await Promise.all([
    pool.query<{
      calls: unknown;
      logical: unknown;
      enc: unknown;
      rec: unknown;
      talkgroups: unknown;
      radios: unknown;
      sites: unknown;
      nodes: unknown;
    }>(
      `SELECT COUNT(*)::int AS calls,
              COUNT(DISTINCT logical_call_id)::int AS logical,
              (COUNT(DISTINCT logical_call_id) FILTER (WHERE encrypted))::int AS enc,
              (COUNT(DISTINCT logical_call_id) FILTER (WHERE recorded))::int AS rec,
              (COUNT(DISTINCT talkgroup) FILTER (WHERE ${TG_VALID}))::int AS talkgroups,
              COUNT(DISTINCT source_unit)::int AS radios,
              (COUNT(DISTINCT (site_rfss, site_id))
                 FILTER (WHERE site_rfss IS NOT NULL AND site_id IS NOT NULL))::int AS sites,
              COUNT(DISTINCT node_id)::int AS nodes
         FROM node_radio_events
        WHERE ${where('')} AND ${CALL_GROUP}`,
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
        WHERE ${where('')} AND ${CALL_GROUP} AND ${TG_VALID}
        GROUP BY talkgroup
        ORDER BY calls DESC, talkgroup ASC
        LIMIT 20`,
      params,
    ),
    pool.query<{ radio: number; alias: string | null; calls: unknown; last_seen: Date }>(
      `SELECT source_unit AS radio,
              (array_agg(source_alias ORDER BY received_at DESC)
                 FILTER (WHERE source_alias IS NOT NULL))[1] AS alias,
              COUNT(*)::int AS calls,
              MAX(received_at) AS last_seen
         FROM node_radio_events
        WHERE ${where('')} AND ${CALL_GROUP} AND source_unit IS NOT NULL
        GROUP BY source_unit
        ORDER BY calls DESC, radio ASC
        LIMIT 20`,
      params,
    ),
    pool.query<{ hour: Date; calls: unknown }>(
      `SELECT date_trunc('hour', received_at) AS hour, COUNT(*)::int AS calls
         FROM node_radio_events
        WHERE ${where('')} AND ${CALL_GROUP}
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
      recordedCalls: num(t?.rec),
      talkgroups: num(t?.talkgroups),
      radios: num(t?.radios),
      sites: num(t?.sites),
      nodes: num(t?.nodes),
    },
    topTalkgroups: tgQ.rows.map((r) => ({
      talkgroup: r.talkgroup,
      label: _scopedTgLabels.get(r.talkgroup) ?? null,
      agency: _scopedTg.agencies.get(r.talkgroup) ?? null,
      color: _scopedTg.colors.get(r.talkgroup) ?? null,
      calls: num(r.calls),
      logicalCalls: num(r.logical),
      encryptedCalls: num(r.enc),
      lastSeen: iso(r.last_seen),
    })),
    topRadios: unQ.rows.map((r) => ({
      radio: r.radio,
      // Same split as every other radio surface: the OTA the radio transmitted
      // vs the label the operator configured for that UID, plus its agency.
      ota: r.alias ?? null,
      alias: _scopedRadio.labels.get(r.radio) ?? null,
      agency: _scopedRadio.agencies.get(r.radio) ?? null,
      color: _scopedRadio.colors.get(r.radio) ?? null,
      calls: num(r.calls),
      lastSeen: iso(r.last_seen),
    })),
    series: srQ.rows.map((r) => ({ hour: iso(r.hour), calls: num(r.calls) })),
  };
}

// ---------------------------------------------------------------------------
// Feeder-facing per-node summary — a LIGHT slice of the Data page, scoped to
// one node the contributor owns. Backs GET /api/feeder/nodes/:id/stats so a
// volunteer can see their own node's calls/receptions + top talkgroup / unit /
// site + a short recent-activity feed, WITHOUT the staff Data page's full
// drill-downs. Reads node_radio_events scoped to one node_id + window only.
// ---------------------------------------------------------------------------
export interface FeederRadioStats {
  window: DetailWindow;
  totals: ScopedDetail['totals'];
  topTalkgroups: ScopedDetail['topTalkgroups'];
  topRadios: ScopedDetail['topRadios'];
  topSites: Array<{
    rfss: number;
    site: number;
    name: string | null;
    calls: number;
    receptions: number;
    lastSeen: string;
  }>;
  activity: Array<{
    id: number;
    at: string;
    talkgroup: number | null;
    talkgroupLabel: string | null;
    system: number | null;
    sourceUnit: number | null;
    sourceAlias: string | null;
    rfss: number | null;
    site: number | null;
    siteName: string | null;
    encrypted: boolean;
    receptions: number;
  }>;
}

export async function feederRadioStats(
  pool: Pool,
  nodeId: string,
  window: DetailWindow,
): Promise<FeederRadioStats> {
  const params: unknown[] = [WINDOW_INTERVAL[window], nodeId];
  // $2 = node id, $1 = window interval. Shared by every query below.
  const where = () => `node_id = $2 AND received_at >= now() - $1::interval`;
  const [detail, siteMap, labels, tgColorsFeeder, evRadioDisp, siteQ, actQ] = await Promise.all([
    scopedRadioDetail(pool, where, params),
    siteNames(pool),
    talkgroupLabels(),
    talkgroupColors(),
    radioDisplay(),
    pool.query<{
      rfss: number;
      site: number;
      system: number | null;
      calls: unknown;
      receptions: unknown;
      last_seen: Date;
    }>(
      `SELECT site_rfss AS rfss, site_id AS site,
              (array_agg(system ORDER BY received_at DESC)
                 FILTER (WHERE system IS NOT NULL))[1] AS system,
              COUNT(DISTINCT logical_call_id)::int AS calls,
              COUNT(*)::int AS receptions,
              MAX(received_at) AS last_seen
         FROM node_radio_events
        WHERE ${where()} AND ${CALL_GROUP}
          AND site_rfss IS NOT NULL AND site_id IS NOT NULL
        GROUP BY site_rfss, site_id
        ORDER BY calls DESC, receptions DESC
        LIMIT 5`,
      params,
    ),
    pool.query<{
      id: string;
      at: Date;
      talkgroup: number | null;
      system: number | null;
      source_unit: number | null;
      source_alias: string | null;
      rfss: number | null;
      site: number | null;
      encrypted: boolean;
      receptions: unknown;
    }>(
      // One row per logical call (deduped receptions), newest first — the
      // per-field array_agg prefers the ACTION='CALL' reception for the
      // identity fields, mirroring the Data page's events list.
      `SELECT logical_call_id::text AS id,
              MAX(received_at) AS at,
              (array_agg(talkgroup ORDER BY (action = 'CALL') DESC, received_at DESC)
                 FILTER (WHERE talkgroup IS NOT NULL))[1] AS talkgroup,
              (array_agg(system ORDER BY received_at DESC)
                 FILTER (WHERE system IS NOT NULL))[1] AS system,
              (array_agg(source_unit ORDER BY (action = 'CALL') DESC, received_at DESC)
                 FILTER (WHERE source_unit IS NOT NULL))[1] AS source_unit,
              (array_agg(source_alias ORDER BY received_at DESC)
                 FILTER (WHERE source_alias IS NOT NULL))[1] AS source_alias,
              (array_agg(site_rfss ORDER BY received_at DESC)
                 FILTER (WHERE site_rfss IS NOT NULL))[1] AS rfss,
              (array_agg(site_id ORDER BY received_at DESC)
                 FILTER (WHERE site_id IS NOT NULL))[1] AS site,
              bool_or(encrypted) AS encrypted,
              COUNT(*)::int AS receptions
         FROM node_radio_events
        WHERE ${where()} AND ${CALL_GROUP}
        GROUP BY logical_call_id
        ORDER BY at DESC
        LIMIT 15`,
      params,
    ),
  ]);
  return {
    window,
    totals: detail.totals,
    topTalkgroups: detail.topTalkgroups.slice(0, 5),
    topRadios: detail.topRadios.slice(0, 5),
    topSites: siteQ.rows.map((r) => ({
      rfss: r.rfss,
      site: r.site,
      name: siteNameFor(siteMap, r.system, r.rfss, r.site),
      calls: num(r.calls),
      receptions: num(r.receptions),
      lastSeen: iso(r.last_seen),
    })),
    activity: actQ.rows.map((r) => ({
      id: Number(r.id),
      at: iso(r.at),
      talkgroup: r.talkgroup,
      talkgroupLabel: r.talkgroup != null ? labels.get(r.talkgroup) ?? null : null,
      talkgroupColor: r.talkgroup != null ? tgColorsFeeder.get(r.talkgroup) ?? null : null,
      system: r.system,
      sourceUnit: r.source_unit,
      // The OTA alias this event carried (what the radio transmitted), plus
      // the radio's configured alias + agency colour, which are properties of
      // the UID rather than of the event.
      sourceAlias: r.source_alias,
      sourceUnitAlias: r.source_unit != null ? evRadioDisp.labels.get(r.source_unit) ?? null : null,
      sourceAgency: r.source_unit != null ? evRadioDisp.agencies.get(r.source_unit) ?? null : null,
      sourceColor: r.source_unit != null ? evRadioDisp.colors.get(r.source_unit) ?? null : null,
      rfss: r.rfss,
      site: r.site,
      siteName: siteNameFor(siteMap, r.system, r.rfss, r.site),
      encrypted: r.encrypted === true,
      receptions: num(r.receptions),
    })),
  };
}

// ---------------------------------------------------------------------------
// GET /api/node-data/systems?window=24h|7d|30d&node=<id opt>
// One row per distinct (wacn, system) observed in-window, incl. NULLs. Each
// row EAGER-LOADS its per-site rollup as `sites: [{rfss, site, name, calls,
// logicalCalls, lastSeen}]` so the Systems folder-tree can show a system and
// its sites together without a per-system round-trip (few systems/sites).
// `siteCount` keeps the DISTINCT site tally for the folder counts.
// ?node scopes both queries to a single feeder node (fleet-wide when absent).
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/systems',
  requireRole(canViewNodeData),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);
      const nodeId = qpNode(url);

      // Shared params: $1 window interval, optional $2 node id. `nodeCond` is
      // spliced into both the system rollup and the per-site rollup below.
      const params: unknown[] = [WINDOW_INTERVAL[window]];
      let nodeCond = '';
      if (nodeId !== null) {
        params.push(nodeId);
        nodeCond = ` AND node_id = $${params.length}`;
      }

      const [res, sitesRes, siteMap] = await Promise.all([
        pool.query<{
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
                  (COUNT(DISTINCT talkgroup) FILTER (WHERE ${TG_VALID}))::int AS talkgroups,
                  COUNT(DISTINCT source_unit)::int AS radios,
                  MIN(received_at) AS first_seen,
                  MAX(received_at) AS last_seen
             FROM node_radio_events
            WHERE received_at >= now() - $1::interval
              AND ${CALL_GROUP}
              AND system IS NOT NULL${nodeCond}
            GROUP BY wacn, system
            ORDER BY calls DESC, system ASC NULLS LAST
            LIMIT 50`,
          params,
        ),
        // Per-site rollup for every system in one scan (grouped by system so we
        // can bucket the rows client-side); attribution-bearing sites only.
        pool.query<{
          system: number;
          rfss: number;
          site: number;
          calls: unknown;
          logical: unknown;
          last_seen: Date;
        }>(
          `SELECT system, site_rfss AS rfss, site_id AS site,
                  COUNT(*)::int AS calls,
                  COUNT(DISTINCT logical_call_id)::int AS logical,
                  MAX(received_at) AS last_seen
             FROM node_radio_events
            WHERE received_at >= now() - $1::interval
              AND ${CALL_GROUP}
              AND system IS NOT NULL
              AND site_rfss IS NOT NULL AND site_id IS NOT NULL${nodeCond}
            GROUP BY system, site_rfss, site_id
            ORDER BY system, calls DESC, rfss ASC, site ASC`,
          params,
        ),
        siteNames(pool),
      ]);

      // Bucket the per-site rows under their system id (reusing the shared
      // siteNames() resolver for the friendly channel name).
      const sitesBySystem = new Map<
        number,
        Array<{ rfss: number; site: number; name: string | null; calls: number; logicalCalls: number; lastSeen: string }>
      >();
      for (const r of sitesRes.rows) {
        const arr = sitesBySystem.get(r.system) ?? [];
        arr.push({
          rfss: r.rfss,
          site: r.site,
          name: siteNameFor(siteMap, r.system, r.rfss, r.site),
          calls: num(r.calls),
          logicalCalls: num(r.logical),
          lastSeen: iso(r.last_seen),
        });
        sitesBySystem.set(r.system, arr);
      }

      return c.json({
        window,
        node: nodeId,
        systems: res.rows.map((r) => ({
          wacn: r.wacn,
          system: r.system,
          name: r.name ?? null,
          calls: num(r.calls),
          logicalCalls: num(r.logical),
          encryptedCalls: num(r.enc),
          // Numeric DISTINCT-site tally for the folder counts …
          siteCount: num(r.sites),
          talkgroups: num(r.talkgroups),
          radios: num(r.radios),
          firstSeen: iso(r.first_seen),
          lastSeen: iso(r.last_seen),
          // … and the eager-loaded per-site rows for the folder's children.
          sites: r.system != null ? sitesBySystem.get(r.system) ?? [] : [],
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
  requireRole(canViewNodeData),
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
      const nodeId = qpNode(url);

      const params: unknown[] = [WINDOW_INTERVAL[window], system];
      if (wacn !== null) params.push(wacn);
      const wacnIdx = wacn !== null ? 3 : null;
      let nodeIdx: number | null = null;
      if (nodeId !== null) {
        params.push(nodeId);
        nodeIdx = params.length;
      }
      const scope = (p: string) =>
        `${p}received_at >= now() - $1::interval AND ${p}system = $2` +
        (wacnIdx !== null ? ` AND ${p}wacn = $${wacnIdx}` : '') +
        (nodeIdx !== null ? ` AND ${p}node_id = $${nodeIdx}` : '');

      // Same scope against node_site_snapshots, whose system column is
      // `system_id`. Snapshots are UPSERTed per (node, system, rfss, site)
      // every ~60s, so received_at tracks "still being monitored" and the
      // window filter carries the same meaning it does for events.
      const snapScope = (p: string) =>
        `${p}received_at >= now() - $1::interval AND ${p}system_id = $2` +
        (wacnIdx !== null ? ` AND ${p}wacn = $${wacnIdx}` : '') +
        (nodeIdx !== null ? ` AND ${p}node_id = $${nodeIdx}` : '');

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
               -- Sites come from BOTH sources, because they answer different
               -- questions. node_radio_events only knows a site once someone
               -- keys up on it, so a site being decoded perfectly but carrying
               -- no group call in the window was invisible here — a quiet site
               -- looked identical to one that was never monitored at all.
               -- node_site_snapshots is SDR-Trunk's own view and is refreshed
               -- every ~60s regardless of traffic, so it is the authority on
               -- "this node is watching this site"; the events side supplies
               -- the call rollup. channel_name IS NOT NULL keeps this to sites
               -- the node actually has a channel for, excluding the many
               -- neighbour sites SDR-Trunk learns about from control broadcasts.
               SELECT COALESCE(ev.rfss, sn.rfss)          AS rfss,
                      COALESCE(ev.site, sn.site)          AS site,
                      COALESCE(ev.nac, sn.nac)            AS nac,
                      COALESCE(ev.calls, 0)               AS calls,
                      COALESCE(ev.logical, 0)             AS logical,
                      COALESCE(ev.last_seen, sn.last_seen) AS last_seen
                 FROM (
                   SELECT site_rfss AS rfss, site_id AS site,
                          MAX(site_nac) AS nac,
                          COUNT(*)::int AS calls,
                          COUNT(DISTINCT logical_call_id)::int AS logical,
                          MAX(received_at) AS last_seen
                     FROM node_radio_events
                    WHERE ${scope('')} AND ${CALL_GROUP}
                      AND site_rfss IS NOT NULL AND site_id IS NOT NULL
                    GROUP BY site_rfss, site_id
                 ) ev
                 FULL OUTER JOIN (
                   SELECT rfss, site_id AS site,
                          MAX(nac) AS nac,
                          MAX(received_at) AS last_seen
                     FROM node_site_snapshots
                    WHERE ${snapScope('')} AND channel_name IS NOT NULL
                    GROUP BY rfss, site_id
                 ) sn ON sn.rfss = ev.rfss AND sn.site = ev.site
             ) s
             LEFT JOIN LATERAL (
               SELECT e.talkgroup, COUNT(*)::int AS calls
                 FROM node_radio_events e
                WHERE ${scope('e.')} AND e.site_rfss = s.rfss AND e.site_id = s.site
                  AND ${callGroup('e.')} AND ${tgValid('e.')}
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
            WHERE ${scope('')} AND ${CALL_GROUP}`,
          params,
        ),
      ]);

      // Cached (~60s) — same map scopedRadioDetail used for its own lists.
      const tgLabels = await talkgroupLabels();
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
              ? {
                  talkgroup: r.top_tg,
                  label: tgLabels.get(r.top_tg) ?? null,
                  calls: num(r.top_tg_calls),
                }
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
  requireRole(canViewNodeData),
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

      const nodeId = qpNode(url);
      const params: unknown[] = [WINDOW_INTERVAL[window], system, rfss, site];
      let nodeIdx: number | null = null;
      if (nodeId !== null) {
        params.push(nodeId);
        nodeIdx = params.length;
      }
      const scope = (p: string) =>
        `${p}received_at >= now() - $1::interval AND ${p}system = $2` +
        ` AND ${p}site_rfss = $3 AND ${p}site_id = $4` +
        (nodeIdx !== null ? ` AND ${p}node_id = $${nodeIdx}` : '');

      const [detail, nodesQ, metaQ] = await Promise.all([
        scopedRadioDetail(pool, scope, params),
        pool.query<{ id: string; name: string | null; calls: unknown; last_seen: Date }>(
          `SELECT e.node_id AS id, n.name, COUNT(*)::int AS calls,
                  MAX(e.received_at) AS last_seen
             FROM node_radio_events e
             LEFT JOIN nodes n ON n.id = e.node_id
            WHERE ${scope('e.')} AND ${callGroup('e.')}
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
          patches: unknown;
          quality: unknown;
          received_at: Date;
        }>(
          // NOTE: its own params — this query never references the $1 window
          // interval, and Postgres rejects a statement with an unused untyped
          // parameter ("could not determine data type of parameter $1", 42P18).
          `SELECT guid, system_name, wacn, nac, lra, channel_name,
                  control_frequency_mhz, control_lcn, affiliated_radio_count,
                  observation_count, site_first_seen_ms, site_last_seen_ms,
                  status, channels, neighbors, bands, patches, quality, received_at
             FROM node_site_snapshots
            WHERE system_id = $1 AND rfss = $2 AND site_id = $3
            ORDER BY received_at DESC
            LIMIT 1`,
          [system, rfss, site],
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
            patches: Array.isArray(m.patches) ? m.patches : [],
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
      // Surface the message to staff (this route is canViewNodeData-gated) so a
      // data-triggered failure is diagnosable without server log access.
      return c.json(
        { error: 'failed to load site', detail: err instanceof Error ? err.message : String(err) },
        500,
      );
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
  requireRole(canViewNodeData),
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
      const nodeId = qpNode(url);
      const scope = qpSiteScope(url);
      if (scope.error) return c.json({ error: scope.error }, 400);
      // enc=hide drops always-encrypted talkgroups (every call encrypted).
      // Server-side rather than a client filter because the list is paged: a
      // client filter would punch holes in pages and break the total.
      const encHide = url.searchParams.get('enc') === 'hide';

      const params: unknown[] = [WINDOW_INTERVAL[window]];
      // CALL_GROUP restricts to talkgroup voice calls; TG_VALID additionally
      // drops NULL talkgroups and excludes out-of-range radio ids.
      const conds = ['received_at >= now() - $1::interval', CALL_GROUP, TG_VALID];
      if (system !== null) {
        params.push(system);
        conds.push(`system = $${params.length}`);
      }
      if (scope.rfss !== null) {
        params.push(scope.rfss);
        conds.push(`site_rfss = $${params.length}`);
        params.push(scope.site);
        conds.push(`site_id = $${params.length}`);
      }
      if (nodeId !== null) {
        params.push(nodeId);
        conds.push(`node_id = $${params.length}`);
      }
      if (qRaw) {
        params.push(`${qRaw}%`);
        conds.push(`talkgroup::text LIKE $${params.length}`);
      }
      if (encHide) {
        // Playlist-declared ENC talkgroups go regardless of what their events
        // were flagged — the HAVING below only sees the per-event flag, which
        // lags the start of a call and so never reaches 100% for them.
        const encTgs = await encryptedTalkgroupIds();
        if (encTgs.length) {
          params.push(encTgs);
          conds.push(`talkgroup <> ALL($${params.length}::int[])`);
        }
      }
      // Agency is a property of the talkgroup's alias group, not of the events,
      // so it resolves to the id set for that agency (empty set = no rows).
      const agencyFilter = (url.searchParams.get('agency') ?? '').trim();
      if (agencyFilter) {
        const tgAgencyMap = await talkgroupAgencies();
        const ids: number[] = [];
        for (const [tg, ag] of tgAgencyMap) if (ag === agencyFilter) ids.push(tg);
        params.push(ids);
        conds.push(`talkgroup = ANY($${params.length}::int[])`);
      }
      // Decode noise lists as real talkgroups. Two rules, in order of strength:
      //
      // 1. A P25 talkgroup on this network is ALWAYS 5 digits — anything else
      //    is a corrupted decode, full stop (798, 1085, 40, …). Always applied,
      //    even with unknown=show. The one exception is a talkgroup the
      //    operator has explicitly configured: AirBand is an aviation channel
      //    carried as pseudo-talkgroup 5, and a blind digit filter would hide
      //    it, so anything in the config survives regardless of its id.
      // 2. Of the ids that ARE 5 digits, the ones absent from the config are
      //    still overwhelmingly noise (in a 24h sample all 34 had exactly one
      //    call, while every configured talkgroup had far more). Hidden by
      //    default, restored by unknown=show so a genuinely new talkgroup can
      //    be spotted before it is added to the config.
      const showUnknown = url.searchParams.get('unknown') === 'show';
      const configured = (await talkgroupCatalog()).configuredTalkgroups;
      // Never filter on an EMPTY configured set — a config with no talkgroups
      // yet would otherwise hide the entire list rather than the noise.
      if (configured.size > 0) {
        params.push([...configured]);
        const inConfig = `talkgroup = ANY($${params.length}::int[])`;
        conds.push(showUnknown ? `(talkgroup BETWEEN 10000 AND 99999 OR ${inConfig})` : inConfig);
      } else {
        conds.push('talkgroup BETWEEN 10000 AND 99999');
      }
      const where = conds.join(' AND ');
      const orderCol = sort === 'lastSeen' ? 'last_seen' : 'calls';
      // "Always encrypted" = no clear call at all. A talkgroup that carries
      // BOTH stays listed — hiding it would lose its clear traffic too.
      const having = encHide ? 'HAVING COUNT(*) FILTER (WHERE encrypted) < COUNT(*)' : '';

      const pageParams = [...params];
      pageParams.push(limit);
      const limIdx = pageParams.length;
      pageParams.push(offset);
      const offIdx = pageParams.length;

      const [countQ, pageQ] = await Promise.all([
        pool.query<{ n: unknown }>(
          // With enc=hide the predicate is per-GROUP, so the count has to
          // group first and count the surviving groups.
          encHide
            ? `SELECT COUNT(*)::int AS n FROM (
                 SELECT 1 FROM node_radio_events WHERE ${where}
                  GROUP BY wacn, system, talkgroup ${having}
               ) g`
            : `SELECT COUNT(DISTINCT (wacn, system, talkgroup))::int AS n
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
            GROUP BY wacn, system, talkgroup ${having}
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
      // Scope for the per-key laterals below. They run on their OWN param
      // array ($1 interval, $2-$4 the page's keys), so optional filters append
      // from $5 — independent of the main query's numbering.
      const latParams: unknown[] = [];
      const latAdd = (v: unknown): string => {
        latParams.push(v);
        return `$${latParams.length + 4}`;
      };
      let nodeLat = '';
      if (nodeId !== null) nodeLat += ` AND e.node_id = ${latAdd(nodeId)}`;
      // Site scope matters here too: without it a site-level list would report
      // each talkgroup's FLEET-wide top site/node, contradicting the page.
      if (scope.rfss !== null) {
        nodeLat += ` AND e.site_rfss = ${latAdd(scope.rfss)} AND e.site_id = ${latAdd(scope.site)}`;
      }
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
                  AND e.talkgroup = k.talkgroup${nodeLat}
                  AND ${callGroup('e.')}
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
                  AND e.talkgroup = k.talkgroup${nodeLat}
                  AND ${callGroup('e.')}
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
                  AND e.talkgroup = k.talkgroup${nodeLat}
                  AND ${callGroup('e.')}
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
            ...latParams,
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
      const agencies = await talkgroupAgencies();
      const colors = await talkgroupColors();
      const siteMap = await siteNames(pool);
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
            // Owning agency, from the SDR-Trunk alias group.
            agency: agencies.get(r.talkgroup) ?? null,
            color: colors.get(r.talkgroup) ?? null,
            calls: num(r.calls),
            logicalCalls: num(r.logical),
            encryptedCalls: num(r.enc),
            lastSeen: iso(r.last_seen),
            lastSite: ex?.lastSite
              ? { ...ex.lastSite, name: siteNameFor(siteMap, r.system, ex.lastSite.rfss, ex.lastSite.site) }
              : null,
            topSite: ex?.topSite
              ? { ...ex.topSite, name: siteNameFor(siteMap, r.system, ex.topSite.rfss, ex.topSite.site) }
              : null,
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
  requireRole(canViewNodeData),
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

      const nodeId = qpNode(url);
      const scope = qpSiteScope(url);
      if (scope.error) return c.json({ error: scope.error }, 400);
      const params: unknown[] = [WINDOW_INTERVAL[window]];
      // CALL_GROUP: a radio here = a unit that made talkgroup voice calls
      // (source_unit of CALL_GROUP% events), not one merely seen in signaling.
      const conds = ['received_at >= now() - $1::interval', CALL_GROUP, 'source_unit IS NOT NULL'];
      if (system !== null) {
        params.push(system);
        conds.push(`system = $${params.length}`);
      }
      if (scope.rfss !== null) {
        params.push(scope.rfss);
        conds.push(`site_rfss = $${params.length}`);
        params.push(scope.site);
        conds.push(`site_id = $${params.length}`);
      }
      if (nodeId !== null) {
        params.push(nodeId);
        conds.push(`node_id = $${params.length}`);
      }
      if (qRaw) {
        params.push(`${qRaw}%`);
        conds.push(`source_unit::text LIKE $${params.length}`);
      }
      // Agency and "has a configured alias" are both properties of the UID, not
      // of the events, so they resolve to a set of unit ids from the config and
      // filter as a plain WHERE. Both together = the intersection.
      const agencyFilter = (url.searchParams.get('agency') ?? '').trim();
      const hasFilter = url.searchParams.get('has'); // 'ota' | 'alias' | null
      if (agencyFilter || hasFilter === 'alias') {
        const disp = await radioDisplay();
        let ids: number[] = [];
        if (agencyFilter) {
          for (const [uid, ag] of disp.agencies) if (ag === agencyFilter) ids.push(uid);
          if (hasFilter === 'alias') ids = ids.filter((uid) => disp.labels.has(uid));
        } else {
          ids = [...disp.labels.keys()];
        }
        // An empty set must match nothing, not everything.
        params.push(ids);
        conds.push(`source_unit = ANY($${params.length}::bigint[])`);
      }
      const where = conds.join(' AND ');
      const orderCol = sort === 'lastSeen' ? 'last_seen' : 'calls';
      // Only radios that have transmitted an over-the-air alias. Applied as a
      // HAVING rather than a WHERE because the alias is an aggregate over the
      // radio's calls — most calls from a named radio carry no alias, so
      // filtering rows would drop nearly all of its traffic and wreck the
      // counts alongside it.
      const namedOnly = url.searchParams.get('named') === '1' || hasFilter === 'ota';
      const having = namedOnly ? 'HAVING COUNT(source_alias) > 0' : '';

      const pageParams = [...params];
      pageParams.push(limit);
      const limIdx = pageParams.length;
      pageParams.push(offset);
      const offIdx = pageParams.length;

      const [countQ, pageQ] = await Promise.all([
        pool.query<{ n: unknown }>(
          namedOnly
            ? `SELECT COUNT(*)::int AS n FROM (
                 SELECT 1 FROM node_radio_events WHERE ${where}
                  GROUP BY wacn, system, source_unit ${having}) t`
            : `SELECT COUNT(DISTINCT (wacn, system, source_unit))::int AS n
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
            ${having}
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
      // Scope for the per-key laterals below. Own param array ($1 interval,
      // $2-$4 the page's keys), so optional filters append from $5.
      const latParams: unknown[] = [];
      const latAdd = (v: unknown): string => {
        latParams.push(v);
        return `$${latParams.length + 4}`;
      };
      let nodeLat = '';
      if (nodeId !== null) nodeLat += ` AND e.node_id = ${latAdd(nodeId)}`;
      if (scope.rfss !== null) {
        nodeLat += ` AND e.site_rfss = ${latAdd(scope.rfss)} AND e.site_id = ${latAdd(scope.site)}`;
      }
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
                  AND e.source_unit = k.radio${nodeLat}
                  AND ${callGroup('e.')}
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
                  AND e.source_unit = k.radio${nodeLat}
                  AND ${callGroup('e.')}
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
                  AND e.source_unit = k.radio${nodeLat}
                  AND ${callGroup('e.')}
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
                      AND e.source_unit = k.radio${nodeLat}
                  AND ${callGroup('e.')}
                      AND ${tgValid('e.')}
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
            ...latParams,
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

      const tgLabels = await talkgroupLabels();
      const tgAgencies = await talkgroupAgencies();
      const tgColors = await talkgroupColors();
      const radioDisp = await radioDisplay();
      const siteMap = await siteNames(pool);
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
            // OTA = transmitted over the air; alias = configured unit label.
            ota: r.alias ?? null,
            alias: radioDisp.labels.get(r.radio) ?? null,
            agency: radioDisp.agencies.get(r.radio) ?? null,
            color: radioDisp.colors.get(r.radio) ?? null,
            calls: num(r.calls),
            lastSeen: iso(r.last_seen),
            lastSite: ex?.lastSite
              ? { ...ex.lastSite, name: siteNameFor(siteMap, r.system, ex.lastSite.rfss, ex.lastSite.site) }
              : null,
            topSite: ex?.topSite
              ? { ...ex.topSite, name: siteNameFor(siteMap, r.system, ex.topSite.rfss, ex.topSite.site) }
              : null,
            topNode: ex?.topNode ?? null,
            topTalkgroups: (ex?.topTalkgroups ?? []).map((t) => ({
              ...t,
              label: tgLabels.get(t.talkgroup) ?? null,
            })),
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
  requireRole(canViewNodeData),
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
      const aliases = await capcodeAliasLookup(pool);
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
  requireRole(canViewNodeData),
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
      const aliases = await capcodeAliasLookup(pool);

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
  requireRole(canViewNodeData),
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
      const a = (await capcodeAliasLookup(pool)).get(normalizeCapcode(capcode)) ?? null;
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

// ---------------------------------------------------------------------------
// GET /api/node-data/pager-node?nodeId=&window=&limit=&offset=
//
// Per-node pager drill-down: what THIS node is hearing. Answers the operational
// question the aggregate view can't — is a node deaf, or just duplicating what
// another already hears?
//
//   totals    — pages/receptions/capcodes for this node, plus `unique`: pages
//               ONLY this node heard in the window (its real coverage
//               contribution) and lastHeard.
//   topCapcodes — busiest capcodes on this node (alias-resolved).
//   series    — hourly page counts, same shape as the overview chart.
//   recent    — latest pages with message text, newest first (paged).
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/pager-node',
  requireRole(canViewNodeData),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);
      const interval = WINDOW_INTERVAL[window];
      const nodeId = (url.searchParams.get('nodeId') ?? '').trim();
      if (!nodeId) return c.json({ error: 'nodeId is required' }, 400);
      const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit') ?? 50) || 50));
      const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

      const [nodeQ, totalsQ, uniqueQ, topCapsQ, seriesQ, recentQ, recentCountQ] = await Promise.all([
        pool.query<{ id: string; name: string | null; kind: string | null }>(
          'SELECT id, name, kind FROM nodes WHERE id = $1',
          [nodeId],
        ),
        pool.query<{ pages: unknown; receptions: unknown; capcodes: unknown; last_heard: unknown }>(
          `SELECT COUNT(DISTINCT COALESCE(logical_id, id))::int AS pages,
                  COUNT(*)::int                                 AS receptions,
                  COUNT(DISTINCT capcode)::int                  AS capcodes,
                  MAX(received_at)                              AS last_heard
             FROM node_pager_events
            WHERE received_at >= now() - $1::interval AND node_id = $2`,
          [interval, nodeId],
        ),
        // Pages in the window whose logical group was heard by ONLY this node.
        pool.query<{ n: unknown }>(
          `SELECT COUNT(*)::int AS n FROM (
             SELECT COALESCE(logical_id, id) AS g
               FROM node_pager_events
              WHERE received_at >= now() - $1::interval
              GROUP BY 1
             HAVING COUNT(DISTINCT node_id) = 1
                AND MIN(node_id) = $2
           ) t`,
          [interval, nodeId],
        ),
        pool.query<{ capcode: string; pages: unknown; last_seen: unknown }>(
          `SELECT capcode,
                  COUNT(DISTINCT COALESCE(logical_id, id))::int AS pages,
                  MAX(received_at)                              AS last_seen
             FROM node_pager_events
            WHERE received_at >= now() - $1::interval AND node_id = $2
            GROUP BY capcode
            ORDER BY pages DESC, last_seen DESC
            LIMIT 20`,
          [interval, nodeId],
        ),
        pool.query<{ hour: unknown; pages: unknown }>(
          `SELECT date_trunc('hour', received_at) AS hour,
                  COUNT(DISTINCT COALESCE(logical_id, id))::int AS pages
             FROM node_pager_events
            WHERE received_at >= now() - $1::interval AND node_id = $2
            GROUP BY 1 ORDER BY 1`,
          [interval, nodeId],
        ),
        pool.query<{ at: unknown; capcode: string; message: string | null; freq_mhz: unknown }>(
          `SELECT received_at AS at, capcode, message, freq_mhz
             FROM node_pager_events
            WHERE received_at >= now() - $1::interval AND node_id = $2
            ORDER BY received_at DESC
            LIMIT $3 OFFSET $4`,
          [interval, nodeId, limit, offset],
        ),
        pool.query<{ n: unknown }>(
          `SELECT COUNT(*)::int AS n FROM node_pager_events
            WHERE received_at >= now() - $1::interval AND node_id = $2`,
          [interval, nodeId],
        ),
      ]);

      const aliases = await capcodeAliasLookup(pool);
      const withAlias = (capcode: string) => {
        const a = aliases.get(normalizeCapcode(capcode)) ?? null;
        return { alias: a?.alias ?? null, agency: a?.agency ?? null };
      };
      const t = totalsQ.rows[0];
      // The nodes row only supplies a display name and kind — every figure
      // below is computed from node_pager_events. A node that is producing
      // events but has no registry row (deleted, or never registered) is still
      // perfectly inspectable, so this used to 404 exactly the nodes the list
      // had just shown traffic for: the drill-down was unreachable for them.
      const node = nodeQ.rows[0] ?? { id: nodeId, name: null, kind: null };

      return c.json({
        window,
        node: { id: node.id, name: node.name, kind: node.kind },
        totals: {
          pages: num(t?.pages),
          receptions: num(t?.receptions),
          capcodes: num(t?.capcodes),
          unique: num(uniqueQ.rows[0]?.n),
          lastHeard: iso(t?.last_heard),
        },
        topCapcodes: topCapsQ.rows.map((r) => ({
          capcode: r.capcode,
          ...withAlias(r.capcode),
          pages: num(r.pages),
          lastSeen: iso(r.last_seen),
        })),
        series: seriesQ.rows.map((r) => ({ hour: iso(r.hour), pages: num(r.pages) })),
        recent: {
          total: num(recentCountQ.rows[0]?.n),
          limit,
          offset,
          messages: recentQ.rows.map((r) => ({
            at: iso(r.at),
            capcode: r.capcode,
            ...withAlias(r.capcode),
            message: r.message,
            freqMhz: r.freq_mhz,
          })),
        },
      });
    } catch (err) {
      log.error({ err }, '/api/node-data/pager-node error');
      return c.json({ error: 'failed to load node pager view' }, 500);
    }
  },
);

// ---------------------------------------------------------------------------
// GET /api/node-data/relationships?radio=<id>|talkgroup=<id>[&system=&node=]
//
// The radio <-> talkgroup association graph: given a radio, every talkgroup it
// has been heard on; given a talkgroup, every radio heard on it. Either one is
// required (both together would be a single cell, not a graph).
//
// vce serves the same view from a precomputed trunked_radio_talkgroup_summary
// table, but node_radio_events already carries source_unit + talkgroup + the
// event type on every row, so the association is derivable here and needs no
// extra feed from the agent — the summary table is an optimisation, not a
// source of truth we lack.
//
// `group`/`private` split the counterpart's calls by event type the same way
// vce's target_kind_code does, so a radio that mostly talks direct reads
// differently from one that works a talkgroup.
// ---------------------------------------------------------------------------
nodeDataRouter.get(
  '/api/node-data/relationships',
  requireRole(canViewNodeData),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) return c.json({ error: 'database unavailable' }, 503);
      const url = new URL(c.req.url);
      const window = detailWindow(url);
      const radio = qpInt(url, 'radio');
      const talkgroup = qpInt(url, 'talkgroup');
      if ((radio === null) === (talkgroup === null)) {
        return c.json({ error: 'exactly one of radio or talkgroup is required' }, 400);
      }
      const nodeId = qpNode(url);
      const system = qpInt(url, 'system');

      // The counterpart column is whichever one wasn't pinned.
      const pinCol = radio !== null ? 'source_unit' : 'talkgroup';
      const outCol = radio !== null ? 'talkgroup' : 'source_unit';

      const params: unknown[] = [WINDOW_INTERVAL[window], radio ?? talkgroup];
      let where =
        `received_at >= now() - $1::interval AND ${pinCol} = $2` +
        ` AND ${outCol} IS NOT NULL`;
      if (system !== null) {
        params.push(system);
        where += ` AND system = $${params.length}`;
      }
      if (nodeId !== null) {
        params.push(nodeId);
        where += ` AND node_id = $${params.length}`;
      }

      const rows = await pool.query<{
        id: number;
        calls: number;
        grp: number;
        priv: number;
        encrypted: number;
        recorded: number;
        alias: string | null;
        label: string | null;
        last_seen: Date;
      }>(
        `SELECT ${outCol} AS id,
                COUNT(*)::int AS calls,
                COUNT(*) FILTER (WHERE ${CALL_GROUP})::int AS grp,
                COUNT(*) FILTER (WHERE NOT (${CALL_GROUP}))::int AS priv,
                COUNT(*) FILTER (WHERE encrypted)::int AS encrypted,
                COUNT(*) FILTER (WHERE recorded)::int AS recorded,
                (array_agg(source_alias ORDER BY received_at DESC)
                   FILTER (WHERE source_alias IS NOT NULL))[1] AS alias,
                (array_agg(talkgroup_label ORDER BY received_at DESC)
                   FILTER (WHERE talkgroup_label IS NOT NULL))[1] AS label,
                MAX(received_at) AS last_seen
           FROM node_radio_events
          WHERE ${where}
          GROUP BY ${outCol}
          ORDER BY calls DESC, id ASC
          LIMIT 200`,
        params,
      );

      const tgLabels = await talkgroupLabels();
      const tgRelColors = await talkgroupColors();
      const tgRelAgencies = await talkgroupAgencies();
      const radioDisp = await radioDisplay();
      // of === 'radio'  → counterparts are TALKGROUPS
      // of === 'talkgroup' → counterparts are RADIOS
      const ofRadio = radio !== null;
      return c.json({
        window,
        of: ofRadio ? 'radio' : 'talkgroup',
        id: radio ?? talkgroup,
        counterparts: rows.rows.map((r) => ({
          id: r.id,
          // A talkgroup counterpart takes its label, preferring the event's own
          // over the catalogue. A radio counterpart has no single "label" — it
          // has an OTA alias AND a configured alias, carried separately below.
          label: ofRadio ? (r.label ?? tgLabels.get(r.id) ?? null) : null,
          // Radio counterparts only: the two aliases, kept distinct.
          ota: ofRadio ? null : (r.alias ?? null),
          alias: ofRadio ? null : (radioDisp.labels.get(r.id) ?? null),
          // Both kinds are colour-coded now: a talkgroup by its own alias
          // colour, a radio by the colour of the agency that owns its UID.
          color: ofRadio
            ? (tgRelColors.get(r.id) ?? null)
            : (radioDisp.colors.get(r.id) ?? null),
          agency: ofRadio
            ? (tgRelAgencies.get(r.id) ?? null)
            : (radioDisp.agencies.get(r.id) ?? null),
          calls: num(r.calls),
          group: num(r.grp),
          private: num(r.priv),
          encrypted: num(r.encrypted),
          recorded: num(r.recorded),
          lastSeen: iso(r.last_seen),
        })),
      });
    } catch (err) {
      log.error({ err }, '/api/node-data/relationships error');
      return c.json({ error: 'failed to load relationships' }, 500);
    }
  },
);

// ---------------------------------------------------------------------------
// GET /api/node-data/nodes?window=
//
// The RECEIVING side of the Systems view: one row per feeder node with what it
// actually heard in the window. The overview's perNode carries raw receptions
// only; this adds the distinct-call, talkgroup and site counts that make two
// nodes comparable, plus live presence from the WS hub.
//
// Radio nodes only — a pager node has no calls, talkgroups or sites, and a row
// of zeros reads as a fault rather than a different kind of node.
// ---------------------------------------------------------------------------
nodeDataRouter.get('/api/node-data/nodes', requireRole(canViewNodeData), async (c) => {
  try {
    const pool = await getPool();
    if (!pool) return c.json({ error: 'database unavailable' }, 503);
    const url = new URL(c.req.url);
    const window = detailWindow(url);

    const rows = await pool.query<{
      node_id: string;
      calls: unknown;
      receptions: unknown;
      talkgroups: unknown;
      sites: unknown;
      last_seen: Date;
    }>(
      `SELECT node_id,
              COUNT(DISTINCT logical_call_id)::int AS calls,
              COUNT(*)::int AS receptions,
              (COUNT(DISTINCT talkgroup) FILTER (WHERE ${TG_VALID}))::int AS talkgroups,
              -- Packed into one bigint rather than COUNT(DISTINCT (a, b)).
              -- A composite DISTINCT builds and sorts row values for every row
              -- in the window; on a node with ~125k receptions a day that
              -- dominated the query. rfss and site are both small ints, so the
              -- pack is lossless (site < 100000).
              (COUNT(DISTINCT (site_rfss::bigint * 100000 + site_id))
                 FILTER (WHERE site_rfss IS NOT NULL AND site_id IS NOT NULL))::int AS sites,
              MAX(received_at) AS last_seen
         FROM node_radio_events
        WHERE received_at >= now() - $1::interval AND ${CALL_GROUP}
        GROUP BY node_id`,
      [WINDOW_INTERVAL[window]],
    );
    const byId = new Map(rows.rows.map((r) => [r.node_id, r]));

    // Every RADIO node, not just those with traffic: a node that heard nothing
    // is precisely the one worth seeing, and grouping the events alone would
    // silently omit it.
    const meta = await pool.query<{ id: string; name: string | null; kind: string | null }>(
      'SELECT id, name, kind FROM nodes',
    );
    const online = new Set(hub.agentList().map((a) => a.nodeId));

    const nodes = meta.rows
      .filter((m) => (m.kind ?? 'radio') === 'radio')
      .map((m) => {
        const r = byId.get(m.id);
        return {
          nodeId: m.id,
          name: m.name,
          online: online.has(m.id),
          calls: num(r?.calls),
          receptions: num(r?.receptions),
          talkgroups: num(r?.talkgroups),
          sites: num(r?.sites),
          lastSeen: r?.last_seen ? iso(r.last_seen) : null,
        };
      })
      .sort((a, b) => b.calls - a.calls || (a.name ?? '').localeCompare(b.name ?? ''));

    return c.json({ window, nodes });
  } catch (err) {
    log.error({ err }, '/api/node-data/nodes error');
    return c.json({ error: 'failed to load nodes' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/node-data/live[?node=<id>]
//
// Fleet-wide live channel state — the equivalent of vce's Live view set to
// "All" with "Active only" on, but across every online node rather than one
// SDR-Trunk instance.
//
// This is the SNAPSHOT form. The Live tab normally streams the same rows over
// the staff WebSocket (subscribeLive), which pushes the instant an agent
// reports instead of waiting for the next poll; this endpoint backs the first
// paint and the fallback when that socket is unavailable. Both shape rows with
// shapeNodeLive() so the two paths are byte-for-byte the same.
//
// Reads the WS hub's in-memory status rather than Postgres: node_radio_events
// is a HISTORY of calls that have already ended and carries no control-channel
// state at all. Nothing is stored for this endpoint; an offline node simply
// isn't in the hub and so doesn't appear.
// ---------------------------------------------------------------------------
nodeDataRouter.get('/api/node-data/live', requireRole(canViewNodeData), async (c) => {
  try {
    const url = new URL(c.req.url);
    const only = qpNode(url);

    const nodes: Array<Record<string, unknown>> = [];
    const channels: Array<Record<string, unknown>> = [];
    const calls: Array<Record<string, unknown>> = [];

    for (const a of hub.agentList()) {
      if (only !== null && a.nodeId !== only) continue;
      const live = hub.liveStatus(a.nodeId);
      const slice = await shapeNodeLive(
        a.nodeId,
        live.status,
        live.lastStatusAt,
        liveCallWindow.callsFor(a.nodeId),
      );
      if (!slice) continue; // pager node, or nothing reported yet
      nodes.push({
        node: slice.node,
        nodeName: slice.nodeName,
        lastStatusAt: slice.lastStatusAt,
      });
      channels.push(...slice.channels);
      calls.push(...slice.calls);
    }

    sortLiveChannels(channels);
    return c.json({ nodes, channels, calls });
  } catch (err) {
    log.error({ err }, '/api/node-data/live error');
    return c.json({ error: 'failed to load live state' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/node-data/site-decode?system=&rfss=&site=[&window=&node=]
//
// Decode-health trend for one site: the series behind the drill-down chart.
//
// Reads node_site_decode_samples (migration 069) rather than
// node_site_snapshots, which is upserted per site and so only ever holds the
// CURRENT reading. Samples arrive ~1/min/site/node and are pruned at 7 days.
//
// Buckets server-side rather than returning raw rows: a week at 1/min is ~10k
// points per node per site, which is both a large payload and more points than
// a chart can meaningfully draw. Averaging per bucket also merges nodes
// watching the same site into one trend line instead of interleaving them.
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET /api/node-data/radio-aliases?q=&sort=lastSeen|timesSeen|radio&limit=&offset=
//
// Every radio that has ever transmitted an over-the-air alias, ONE ROW PER
// RADIO. Reads node_radio_aliases rather than the event detail: aliases are
// durable identity, the detail table is pruned at 30 days, and deduping 400k
// event rows to a few hundred radios on every request would be wasteful.
//
// Deliberately unwindowed — "every radio that has ever named itself" is the
// question, and a radio quiet for a month is exactly the one you are looking
// up.
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET /api/node-data/agencies
//
// The agency names the talkgroup / radio filters offer, with their colours.
// Derived from the same catalogue the tables colour themselves from, so a
// filter option can never name an agency no row could match. Sorted by name.
// ---------------------------------------------------------------------------
nodeDataRouter.get('/api/node-data/agencies', requireRole(canViewNodeData), async (c) => {
  try {
    const cat = await talkgroupCatalog();
    const byName = new Map<string, { name: string; color: string | null; talkgroups: number; radios: number }>();
    const get = (name: string) =>
      byName.get(name) ?? (byName.set(name, { name, color: null, talkgroups: 0, radios: 0 }), byName.get(name)!);
    for (const [tg, name] of cat.agencies) {
      const e = get(name);
      e.talkgroups++;
      if (!e.color) e.color = cat.colors.get(tg) ?? null;
    }
    for (const [uid, name] of cat.unitAgencies) {
      const e = get(name);
      e.radios++;
      if (!e.color) e.color = cat.unitColors.get(uid) ?? null;
    }
    return c.json({
      agencies: [...byName.values()].sort((a, b) => a.name.localeCompare(b.name)),
    });
  } catch (err) {
    log.error({ err }, '/api/node-data/agencies error');
    return c.json({ error: 'failed to load agencies' }, 500);
  }
});

nodeDataRouter.get('/api/node-data/radio-aliases', requireRole(canViewNodeData), async (c) => {
  try {
    const pool = await getPool();
    if (!pool) return c.json({ error: 'database unavailable' }, 503);
    const url = new URL(c.req.url);
    // Free text: matches the alias OR the radio id, since you arrive here with
    // one or the other and shouldn't have to know which box it goes in.
    const qRaw = (url.searchParams.get('q') ?? '').trim().slice(0, 64);
    const sortRaw = url.searchParams.get('sort') ?? 'lastSeen';
    const sort = (['lastSeen', 'timesSeen', 'radio', 'alias'] as const).find((s) => s === sortRaw)
      ?? 'lastSeen';
    const limit = Math.max(1, Math.min(200, Number(url.searchParams.get('limit') ?? 100) || 100));
    const offset = Math.max(0, Number(url.searchParams.get('offset') ?? 0) || 0);

    // Agency / "has a configured alias" live in the config, not this table, so
    // they resolve to a unit-id set the same way /radios does. Every row here
    // has an OTA by definition (that is what the table records), so there is no
    // has=ota filter to apply.
    const agencyFilter = (url.searchParams.get('agency') ?? '').trim();
    const hasFilter = url.searchParams.get('has');
    const radioDisp = await radioDisplay();

    const params: unknown[] = [];
    const conds: string[] = [];
    if (qRaw) {
      params.push(`%${qRaw.toLowerCase()}%`);
      conds.push(`(lower(alias) LIKE $${params.length} OR radio_id::text LIKE $${params.length})`);
    }
    if (agencyFilter || hasFilter === 'alias') {
      let ids: number[] = [];
      if (agencyFilter) {
        for (const [uid, ag] of radioDisp.agencies) if (ag === agencyFilter) ids.push(uid);
        if (hasFilter === 'alias') ids = ids.filter((uid) => radioDisp.labels.has(uid));
      } else {
        ids = [...radioDisp.labels.keys()];
      }
      params.push(ids);
      conds.push(`radio_id = ANY($${params.length}::bigint[])`);
    }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    const orderBy = {
      lastSeen: 'last_seen DESC',
      timesSeen: 'times_seen DESC',
      radio: 'radio_id ASC',
      alias: 'lower(alias) ASC',
    }[sort];

    const totalRes = await pool.query<{ n: string }>(
      `SELECT count(*)::int AS n FROM node_radio_aliases ${where}`,
      params,
    );
    params.push(limit, offset);
    const rows = await pool.query<{
      system: number;
      radio_id: number;
      alias: string;
      first_seen: Date;
      last_seen: Date;
      times_seen: number;
    }>(
      `SELECT system, radio_id, alias, first_seen, last_seen, times_seen
         FROM node_radio_aliases ${where}
        ORDER BY ${orderBy}
        LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params,
    );

    return c.json({
      total: Number(totalRes.rows[0]?.n ?? 0),
      limit,
      offset,
      rows: rows.rows.map((r) => ({
        system: r.system,
        radio: r.radio_id,
        // This table IS the OTA history (node_radio_aliases records what each
        // radio transmitted), so `ota` is the stored alias; `alias` is the
        // separate configured unit label. A radio can have both.
        ota: r.alias,
        alias: radioDisp.labels.get(r.radio_id) ?? null,
        // Radios have no talkgroup, so the agency (and its colour) come from
        // the agency unit list this radio id belongs to.
        agency: radioDisp.agencies.get(r.radio_id) ?? null,
        color: radioDisp.colors.get(r.radio_id) ?? null,
        firstSeen: r.first_seen,
        lastSeen: r.last_seen,
        timesSeen: r.times_seen,
      })),
    });
  } catch (err) {
    log.error({ err }, '/api/node-data/radio-aliases error');
    return c.json({ error: 'failed to load radio aliases' }, 500);
  }
});

nodeDataRouter.get('/api/node-data/site-decode', requireRole(canViewNodeData), async (c) => {
  try {
    const pool = await getPool();
    if (!pool) return c.json({ error: 'database unavailable' }, 503);
    const url = new URL(c.req.url);
    const system = qpInt(url, 'system');
    const rfss = qpInt(url, 'rfss');
    const site = qpInt(url, 'site');
    if (system === null || rfss === null || site === null) {
      return c.json({ error: 'system, rfss and site are required' }, 400);
    }
    const nodeId = qpNode(url);

    // This chart takes its OWN range, independent of the page window, because
    // the underlying data has its own lifetime: samples are pruned at 7 days
    // (nodeEventsPruner) while call detail is kept for 30. Driving it from the
    // page's 30d option promised a month of history that cannot exist — the
    // chart drew about a day of samples stretched across a 30-day axis.
    //
    // Ranges are capped at that 7-day retention for the same reason.
    const RANGES: Record<string, { interval: string; bucketMinutes: number }> = {
      // ~1 sample/min/site/node, so the bucket is sized to land near 150 points
      // whatever range is picked — enough to show shape, few enough to draw.
      '1h': { interval: '1 hour', bucketMinutes: 1 },
      '6h': { interval: '6 hours', bucketMinutes: 3 },
      '24h': { interval: '24 hours', bucketMinutes: 10 },
      '7d': { interval: '7 days', bucketMinutes: 60 },
    };
    const rangeRaw = (url.searchParams.get('range') ?? '24h').toLowerCase();
    const range = RANGES[rangeRaw] ? rangeRaw : '24h';
    const { interval, bucketMinutes } = RANGES[range]!;

    const params: unknown[] = [interval, system, rfss, site];
    let where =
      `sampled_at >= now() - $1::interval AND system_id = $2 AND rfss = $3 AND site_id = $4`;
    if (nodeId !== null) {
      params.push(nodeId);
      where += ` AND node_id = $${params.length}`;
    }

    const r = await pool.query<{
      bucket: Date;
      decode_pct: string | null;
      decode_min: string | null;
      signal_dbfs: string | null;
      invalid_frames: string | null;
      samples: number;
    }>(
      `SELECT to_timestamp(floor(extract(epoch FROM sampled_at) / ${bucketMinutes * 60})
                           * ${bucketMinutes * 60}) AS bucket,
              -- A ZERO decode reading means "not measured", not "0% decode":
              -- the sample is written whenever quality carries EITHER a decode
              -- figure or a signal figure, so a node that reported only signal
              -- stores decode_pct = 0. Counting those pinned Worst to 0
              -- permanently and dragged the average toward the midpoint — a
              -- site reading 98% on its tile drew a flat 50% line, because the
              -- mean was taken across a node receiving it and a node not.
              -- Excluded from both aggregates so the chart describes the nodes
              -- actually hearing the site.
              AVG(decode_pct) FILTER (WHERE decode_pct > 0) AS decode_pct,
              -- Worst reading in the bucket. AVG alone is the wrong summary for
              -- a health metric: at coarse buckets a site that dropped to 40%
              -- for ten minutes averages away entirely, which is precisely the
              -- event the chart exists to show.
              MIN(decode_pct) FILTER (WHERE decode_pct > 0) AS decode_min,
              AVG(signal_dbfs) FILTER (WHERE signal_dbfs <> 0) AS signal_dbfs,
              MAX(invalid_frames) AS invalid_frames,
              COUNT(*)::int AS samples
         FROM node_site_decode_samples
        WHERE ${where}
        GROUP BY bucket
        ORDER BY bucket ASC`,
      params,
    );

    return c.json({
      range,
      bucketMinutes,
      /** Retention ceiling, so the UI can label/limit its own range picker
       *  instead of hard-coding a number that has to track the pruner. */
      maxRangeDays: 7,
      points: r.rows.map((x) => ({
        at: iso(x.bucket),
        decodePct: x.decode_pct !== null ? Number(x.decode_pct) : null,
        decodeMin: x.decode_min !== null ? Number(x.decode_min) : null,
        signalDbfs: x.signal_dbfs !== null ? Number(x.signal_dbfs) : null,
        invalidFrames: x.invalid_frames !== null ? Number(x.invalid_frames) : null,
        samples: num(x.samples),
      })),
    });
  } catch (err) {
    log.error({ err }, '/api/node-data/site-decode error');
    return c.json({ error: 'failed to load decode history' }, 500);
  }
});
