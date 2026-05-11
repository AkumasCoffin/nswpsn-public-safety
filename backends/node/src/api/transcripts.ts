/**
 * /api/rdio/transcripts/search and /api/rdio/calls/:id.
 *
 * Mirrors python external_api_proxy.py:15725-15947. Reads from the
 * SELF-HOSTED rdio-scanner Postgres (RDIO_DATABASE_URL), joins results
 * with the system + talkgroup label cache and the radio-unit label
 * dictionary, and returns the same response shape python emits so any
 * UI / downstream consumer can flip backends without noticing.
 *
 * Local-time-of-day filtering is delegated to Postgres' AT TIME ZONE
 * arithmetic (same approach python uses) — keeps DST handling out of
 * application code.
 */
import { Hono } from 'hono';
import {
  isRdioConfigured,
  getRdioPool,
  resolveLabels,
  getUnitLabel,
  ensureUnitLabelsLoaded,
} from '../services/rdio.js';
import { config } from '../config.js';
import { log } from '../lib/log.js';

interface RdioCallRow {
  id: number;
  date_time: Date | null;
  system: number | null;
  talkgroup: number | null;
  transcript: string | null;
  source: number | string | null;
  sources: unknown;
}

function extractRadioId(row: RdioCallRow): number | null {
  // Mirror of python's _extract_radio_id at line 14634.
  const src = row.source;
  if (src !== null && src !== undefined && src !== '') {
    const n = Number(src);
    if (Number.isFinite(n) && n > 0) return n;
  }
  let raw = row.sources;
  if (typeof raw === 'string') {
    try {
      raw = JSON.parse(raw);
    } catch {
      return null;
    }
  }
  if (Array.isArray(raw)) {
    for (const item of raw) {
      if (item && typeof item === 'object') {
        const obj = item as Record<string, unknown>;
        const sid = obj['src'] ?? obj['source'];
        if (sid !== undefined && sid !== null && sid !== '') {
          const n = Number(sid);
          if (Number.isFinite(n) && n > 0) return n;
        }
      }
    }
  }
  return null;
}

async function rowToShape(row: RdioCallRow): Promise<Record<string, unknown>> {
  await ensureUnitLabelsLoaded();
  const { systemLabel, talkgroupLabel } = await resolveLabels(
    row.system,
    row.talkgroup,
  );
  const rid = extractRadioId(row);
  const dt = row.date_time
    ? row.date_time.toISOString().replace(/\.\d+Z$/, 'Z')
    : null;
  return {
    id: row.id,
    datetime: dt,
    system: row.system,
    system_label: systemLabel,
    talkgroup: row.talkgroup,
    talkgroup_label: talkgroupLabel,
    transcript: row.transcript,
    radio_id: rid,
    radio_label: rid !== null ? getUnitLabel(rid) : null,
    call_url: `${config.RDIO_CALL_URL_BASE}${row.id}`,
  };
}

function parseHm(s: string): { h: number; m: number } | null {
  const parts = s.split(':');
  if (parts.length !== 2) return null;
  const h = Number(parts[0]);
  const m = Number(parts[1]);
  if (!Number.isFinite(h) || !Number.isFinite(m)) return null;
  if (h < 0 || h >= 24 || m < 0 || m >= 60) return null;
  return { h, m };
}

export const transcriptsRouter = new Hono();

transcriptsRouter.get('/api/rdio/transcripts/search', async (c) => {
  if (!isRdioConfigured()) {
    return c.json({ error: 'RDIO_DATABASE_URL not configured' }, 503);
  }
  try {
    const url = new URL(c.req.url);
    const q = (url.searchParams.get('q') ?? '').trim();
    const callIdRaw = url.searchParams.get('call_id');
    const callId =
      callIdRaw !== null && callIdRaw !== '' && /^\d+$/.test(callIdRaw)
        ? Number.parseInt(callIdRaw, 10)
        : null;
    if (!q && callId === null) {
      return c.json({ error: 'q (keyword) or call_id is required' }, 400);
    }
    const systemRaw = url.searchParams.get('system');
    const talkgroupRaw = url.searchParams.get('talkgroup');
    const systemId =
      systemRaw && /^\d+$/.test(systemRaw) ? Number.parseInt(systemRaw, 10) : null;
    const talkgroupId =
      talkgroupRaw && /^\d+$/.test(talkgroupRaw)
        ? Number.parseInt(talkgroupRaw, 10)
        : null;

    let dateFrom = url.searchParams.get('date_from');
    let dateTo = url.searchParams.get('date_to');
    const date = url.searchParams.get('date');
    const timeFrom = url.searchParams.get('time_from');
    const timeTo = url.searchParams.get('time_to');
    const limit = Math.max(
      1,
      Math.min(200, Number(url.searchParams.get('limit') ?? 20) || 20),
    );
    const offset = Math.max(
      0,
      Number(url.searchParams.get('offset') ?? 0) || 0,
    );
    const order =
      (url.searchParams.get('order') ?? 'desc').toLowerCase() === 'asc'
        ? 'ASC'
        : 'DESC';

    if (date && !dateFrom && !dateTo) {
      dateFrom = date;
      dateTo = date;
    }

    const clauses: string[] = [];
    const params: unknown[] = [];
    const next = (): string => `$${params.length + 1}`;

    if (callId !== null) {
      clauses.push(`"id" = ${next()}`);
      params.push(callId);
    } else {
      clauses.push('"transcript" IS NOT NULL');
      // Comma-separated terms = OR of ILIKE; matches python's behaviour.
      const terms = q
        .split(',')
        .map((t) => t.trim())
        .filter((t) => t.length >= 2);
      if (terms.length === 0) {
        return c.json(
          { error: 'q must contain at least one term of 2+ chars' },
          400,
        );
      }
      if (terms.length === 1) {
        clauses.push(`"transcript" ILIKE ${next()}`);
        params.push(`%${terms[0]}%`);
      } else {
        const placeholders = terms.map(() => `"transcript" ILIKE ${next()}`);
        // Re-emit placeholders but only after pushing each param; the
        // simple loop above mutates params, so use a separate builder:
        clauses.pop(); // drop the simple ILIKE we tentatively pushed
        const parts: string[] = [];
        for (const t of terms) {
          parts.push(`"transcript" ILIKE ${next()}`);
          params.push(`%${t}%`);
        }
        // Strip the unused single placeholders we just generated:
        // (we used `placeholders` only as a counter — drop it)
        void placeholders;
        clauses.push(`(${parts.join(' OR ')})`);
      }
    }

    if (systemId !== null) {
      clauses.push(`"system" = ${next()}`);
      params.push(systemId);
    }
    if (talkgroupId !== null) {
      clauses.push(`"talkgroup" = ${next()}`);
      params.push(talkgroupId);
    }

    // YYYY-MM-DD bounds in SUMMARY_TZ → naive UTC instants Postgres
    // can compare against the rdioScanner naive `dateTime` column.
    // Python computes the bound in app code; we let Postgres do the
    // tz arithmetic via AT TIME ZONE so DST is handled identically.
    if (dateFrom) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(dateFrom)) {
        return c.json({ error: 'date_from must be YYYY-MM-DD' }, 400);
      }
      // (date::date AT TIME ZONE tz) = midnight in tz, returned as UTC.
      // Cast to ::timestamp to drop tz info so it compares against the
      // naive `dateTime` column (which already holds UTC).
      clauses.push(
        `"dateTime" >= ((${next()}::date) AT TIME ZONE ${next()})::timestamp`,
      );
      params.push(dateFrom, config.SUMMARY_TZ);
    }
    if (dateTo) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(dateTo)) {
        return c.json({ error: 'date_to must be YYYY-MM-DD' }, 400);
      }
      // End of local day = start of next local day, exclusive. The
      // python version uses 23:59:59 inclusive; using (next_day) with
      // a strict-less-than is equivalent without rounding loss.
      clauses.push(
        `"dateTime" < ((${next()}::date + INTERVAL '1 day') AT TIME ZONE ${next()})::timestamp`,
      );
      params.push(dateTo, config.SUMMARY_TZ);
    }

    if (timeFrom) {
      const hm = parseHm(timeFrom);
      if (!hm) return c.json({ error: 'time_from must be HH:MM' }, 400);
      clauses.push(
        `EXTRACT(HOUR FROM "dateTime" AT TIME ZONE 'UTC' AT TIME ZONE ${next()}) * 60 ` +
          `+ EXTRACT(MINUTE FROM "dateTime" AT TIME ZONE 'UTC' AT TIME ZONE ${next()}) >= ${next()}`,
      );
      params.push(config.SUMMARY_TZ, config.SUMMARY_TZ, hm.h * 60 + hm.m);
    }
    if (timeTo) {
      const hm = parseHm(timeTo);
      if (!hm) return c.json({ error: 'time_to must be HH:MM' }, 400);
      clauses.push(
        `EXTRACT(HOUR FROM "dateTime" AT TIME ZONE 'UTC' AT TIME ZONE ${next()}) * 60 ` +
          `+ EXTRACT(MINUTE FROM "dateTime" AT TIME ZONE 'UTC' AT TIME ZONE ${next()}) <= ${next()}`,
      );
      params.push(config.SUMMARY_TZ, config.SUMMARY_TZ, hm.h * 60 + hm.m);
    }

    const where = `WHERE ${clauses.join(' AND ')}`;
    const pool = await getRdioPool();
    if (!pool) return c.json({ error: 'RDIO_DATABASE_URL not configured' }, 503);

    const countSql = `SELECT COUNT(*)::int AS n FROM "rdioScannerCalls" ${where}`;
    const limitParam = `$${params.length + 1}`;
    const offsetParam = `$${params.length + 2}`;
    const dataSql =
      `SELECT "id", "dateTime" AS date_time, "system", "talkgroup", ` +
      `"transcript", "source", "sources" FROM "rdioScannerCalls" ${where} ` +
      `ORDER BY "dateTime" ${order} LIMIT ${limitParam} OFFSET ${offsetParam}`;

    const [countRes, dataRes] = await Promise.all([
      pool.query<{ n: number }>(countSql, params),
      pool.query<RdioCallRow>(dataSql, [...params, limit, offset]),
    ]);
    const total = countRes.rows[0]?.n ?? 0;
    const results = await Promise.all(dataRes.rows.map(rowToShape));
    return c.json({
      total,
      limit,
      offset,
      query: q,
      call_id: callId,
      results,
    });
  } catch (err) {
    log.error({ err }, '/api/rdio/transcripts/search error');
    return c.json({ error: (err as Error).message }, 500);
  }
});

transcriptsRouter.get('/api/rdio/calls/:callId', async (c) => {
  if (!isRdioConfigured()) {
    return c.json({ error: 'RDIO_DATABASE_URL not configured' }, 503);
  }
  const callIdRaw = c.req.param('callId');
  if (!/^\d+$/.test(callIdRaw)) {
    return c.json({ error: 'callId must be numeric' }, 400);
  }
  const callId = Number.parseInt(callIdRaw, 10);
  try {
    const pool = await getRdioPool();
    if (!pool) {
      return c.json({ error: 'RDIO_DATABASE_URL not configured' }, 503);
    }
    const res = await pool.query<RdioCallRow>(
      'SELECT "id", "dateTime" AS date_time, "system", "talkgroup", ' +
        '"transcript", "source", "sources" FROM "rdioScannerCalls" ' +
        'WHERE "id" = $1',
      [callId],
    );
    const row = res.rows[0];
    if (!row) {
      return c.json({ error: 'call not found' }, 404);
    }
    return c.json(await rowToShape(row));
  } catch (err) {
    log.error({ err, callId }, '/api/rdio/calls/:id error');
    return c.json({ error: (err as Error).message }, 500);
  }
});
