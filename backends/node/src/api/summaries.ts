/**
 * /api/summaries/latest, /api/summaries (search), /api/summaries/trigger.
 *
 * Mirrors python external_api_proxy.py:15600-15722 (latest + search) and
 * 15951-... (trigger). The summaries table itself (`rdio_summaries`)
 * lives in our MAIN Postgres (DATABASE_URL) — it's populated by the
 * python summary-generation loop. We don't port the generation loop in
 * this round; the read endpoints serve whatever rows exist.
 *
 * The `/api/summaries/trigger` POST is intentionally stubbed with a 503
 * — generating a new summary requires Gemini's HTTP API + the prompt
 * files at backends/prompts/. Keeping that on python is fine during the
 * strangler-fig migration; Apache routes /api/summaries/trigger to
 * python, /api/summaries/* (read) to whichever backend is preferred.
 */
import { Hono } from 'hono';
import type { QueryResultRow } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';

interface SummaryRow extends QueryResultRow {
  id: number;
  summary_type: string;
  period_start: Date | null;
  period_end: Date | null;
  day_date: Date | null;
  hour_slot: number | null;
  summary: string;
  call_count: number;
  transcript_chars: number;
  model: string | null;
  details: Record<string, unknown> | null;
  release_at: Date | null;
  created_at: Date | null;
}

interface SummaryShape {
  id: number;
  type: string;
  period_start: string | null;
  period_end: string | null;
  day_date: string | null;
  hour_slot: number | null;
  summary: string;
  call_count: number;
  transcript_chars: number;
  model: string | null;
  details: Record<string, unknown>;
  release_at: string | null;
  created_at: string | null;
}

function rowToSummary(row: SummaryRow): SummaryShape {
  return {
    id: row.id,
    type: row.summary_type,
    period_start: row.period_start ? row.period_start.toISOString() : null,
    period_end: row.period_end ? row.period_end.toISOString() : null,
    day_date: row.day_date
      ? row.day_date.toISOString().slice(0, 10)
      : null,
    hour_slot: row.hour_slot,
    summary: row.summary,
    call_count: row.call_count,
    transcript_chars: row.transcript_chars,
    model: row.model,
    details: row.details ?? {},
    release_at: row.release_at ? row.release_at.toISOString() : null,
    created_at: row.created_at ? row.created_at.toISOString() : null,
  };
}

export const summariesRouter = new Hono();

summariesRouter.get('/api/summaries/latest', async (c) => {
  try {
    const pool = await getPool();
    if (!pool) {
      return c.json({ error: 'database not configured' }, 503);
    }
    // Order by created_at DESC, not period_start: an ad-hoc row's
    // period_start is the trigger time, which would otherwise always
    // beat a scheduled hourly with period_start = hour-top.
    const sql =
      "SELECT * FROM rdio_summaries " +
      "WHERE summary_type IN ('hourly', 'adhoc') " +
      "  AND (release_at IS NULL OR release_at <= now()) " +
      'ORDER BY created_at DESC LIMIT 1';
    const res = await pool.query<SummaryRow>(sql);
    const row = res.rows[0];
    return c.json({ hourly: row ? rowToSummary(row) : null });
  } catch (err) {
    log.error({ err }, '/api/summaries/latest error');
    const e = err as Error;
    return c.json({ error: `${e.name}: ${e.message}` }, 500);
  }
});

summariesRouter.get('/api/summaries', async (c) => {
  try {
    const url = new URL(c.req.url);
    let stype = url.searchParams.get('type');
    const date = url.searchParams.get('date');
    const hourRaw = url.searchParams.get('hour');
    const dateFrom = url.searchParams.get('date_from');
    const dateTo = url.searchParams.get('date_to');
    const limit = Math.max(
      1,
      Math.min(500, Number(url.searchParams.get('limit') ?? 50) || 50),
    );
    const offset = Math.max(
      0,
      Number(url.searchParams.get('offset') ?? 0) || 0,
    );

    const clauses: string[] = [];
    const params: unknown[] = [];

    if (hourRaw !== null) {
      const hour = Number(hourRaw);
      if (!Number.isFinite(hour) || hour < 1 || hour > 24) {
        return c.json({ error: 'hour must be 1..24' }, 400);
      }
      clauses.push(`hour_slot = $${params.length + 1}`);
      params.push(hour);
      if (!stype) stype = 'hourly';
    }
    if (stype) {
      if (stype !== 'hourly' && stype !== 'adhoc') {
        return c.json({ error: "type must be 'hourly' or 'adhoc'" }, 400);
      }
      clauses.push(`summary_type = $${params.length + 1}`);
      params.push(stype);
    }
    if (date) {
      clauses.push(`day_date = $${params.length + 1}`);
      params.push(date);
    }
    if (dateFrom) {
      clauses.push(`day_date >= $${params.length + 1}`);
      params.push(dateFrom);
    }
    if (dateTo) {
      clauses.push(`day_date <= $${params.length + 1}`);
      params.push(dateTo);
    }
    // Hide embargoed rows.
    clauses.push('(release_at IS NULL OR release_at <= now())');

    const where = `WHERE ${clauses.join(' AND ')}`;
    const countParams = [...params];
    const dataParams = [...params, limit, offset];
    const dataSql =
      `SELECT * FROM rdio_summaries ${where} ` +
      `ORDER BY period_start DESC LIMIT $${dataParams.length - 1} OFFSET $${dataParams.length}`;
    const countSql = `SELECT COUNT(*)::int AS n FROM rdio_summaries ${where}`;

    const pool = await getPool();
    if (!pool) {
      return c.json({ error: 'database not configured' }, 503);
    }
    const [countRes, dataRes] = await Promise.all([
      pool.query<{ n: number }>(countSql, countParams),
      pool.query<SummaryRow>(dataSql, dataParams),
    ]);

    return c.json({
      total: countRes.rows[0]?.n ?? 0,
      limit,
      offset,
      results: dataRes.rows.map(rowToSummary),
    });
  } catch (err) {
    log.error({ err }, '/api/summaries error');
    return c.json({ error: (err as Error).message }, 500);
  }
});

summariesRouter.post('/api/summaries/trigger', (c) =>
  c.json(
    {
      error: 'summary generation not yet ported to node backend',
      message:
        'The Gemini-driven summary generation loop still runs on the python ' +
        'backend. Route /api/summaries/trigger to the python service via Apache ' +
        'until W8 lands the LLM port.',
    },
    503,
  ),
);
