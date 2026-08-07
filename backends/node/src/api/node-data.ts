/**
 * Staff "Data" tab — feeder-node event analytics (owner|dev only).
 *
 * Reads the per-event capture written by services/nodeEvents.ts:
 *   - node_radio_events / node_pager_events   (30-day detail, migration 043)
 *   - node_radio_hourly / node_radio_hourly_sys / node_pager_hourly (forever)
 *
 *   GET /api/node-data/overview  — totals, per-node volume, top lists, series
 *   GET /api/node-data/events    — logical-call event browser (grouped)
 *
 * Windows ≤30d compute from the detail tables (logical counts via
 * COUNT(DISTINCT logical_*)); window=all uses the hourly forever buckets,
 * except topUnits which only exists in detail (capped to 30d and flagged
 * with unitsWindowCapped: true).
 */
import { Hono } from 'hono';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { requireRole, canManageNodes } from '../services/auth/roles.js';

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
                  `SELECT h.system, h.talkgroup,
                          SUM(h.calls)::bigint AS calls,
                          SUM(h.logical_calls)::bigint AS logical,
                          (SELECT e.talkgroup_label FROM node_radio_events e
                            WHERE e.system = h.system AND e.talkgroup = h.talkgroup
                              AND e.talkgroup_label IS NOT NULL
                            ORDER BY e.received_at DESC LIMIT 1) AS label
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
                `SELECT e.system, e.talkgroup,
                        COUNT(*)::int AS calls,
                        COUNT(DISTINCT e.logical_call_id)::int AS logical,
                        (SELECT e2.talkgroup_label FROM node_radio_events e2
                          WHERE e2.system IS NOT DISTINCT FROM e.system
                            AND e2.talkgroup IS NOT DISTINCT FROM e.talkgroup
                            AND e2.talkgroup_label IS NOT NULL
                          ORDER BY e2.received_at DESC LIMIT 1) AS label
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
          label: r.label ?? null,
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
//   ?type=all|radio|pager&talkgroup=&unit=&node=&site=&system=&q=&from=&to=
//   &limit=50&offset=0
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
        talkgroup !== null || unit !== null || system !== null || siteRfss !== null;
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
              frequency: string | null;
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
                      MIN(e.frequency)::bigint AS frequency,
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
              talkgroupLabel: d.talkgroup_label,
              systemLabel: d.system_label,
              sourceUnit: d.source_unit,
              frequency: d.frequency !== null ? Number(d.frequency) : null,
              sites: d.sites,
              nodes: d.nodes,
              receptions: d.receptions,
            };
          }
          const d = pagerMap.get(row.id);
          if (!d) return null;
          return {
            type: 'pager' as const,
            id: Number(d.id),
            at: iso(d.at),
            capcode: d.capcode,
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
