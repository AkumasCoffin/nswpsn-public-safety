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
 *
 * The two reads are also served at /api/v1/ (server.ts mirrors them),
 * wrapped in `data` there (api/v1/respond.ts). /api/summaries/trigger, in
 * both methods, is owner tooling and is not.
 */
import { Hono } from 'hono';
import type { Context } from 'hono';
import type { QueryResultRow } from 'pg';
import { z } from 'zod';
import { describeRoute, resolver } from 'hono-openapi';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { config } from '../config.js';
import { isRdioConfigured } from '../services/rdio.js';
import { requireRole, isOwner } from '../services/auth/roles.js';
import {
  generateRdioHourlySummary,
  generateRdioRecentSummary,
} from '../services/llm.js';
import { scoped } from '../services/auth/apiKeyScopes.js';
import { fail, ok } from './v1/respond.js';
import {
  assertShape,
  envelope,
  ErrorBody,
  GATED_ERRORS,
  okJson,
  queryValidator,
  UNAVAILABLE_ERROR,
  type SameShape,
} from './v1/schemas.js';

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

// What a scoped API key needs to read these routes (services/auth/apiKeyScopes.ts).
const SCOPE_SUMMARIES_HISTORY = scoped({ source: 'rdio', type: 'summaries', window: 'history' });

// ---------------------------------------------------------------------------
// What /api/v1/ documents (api/v1/openapi.ts)
// ---------------------------------------------------------------------------

const errorJson = (description: string) => ({
  description,
  content: { 'application/json': { schema: resolver(ErrorBody) } },
});

const RadioSummary = z
  .object({
    id: z.number().int(),
    type: z
      .string()
      .describe('hourly: one local hour, written on the hourly schedule; adhoc: the latest transcripts, written on demand'),
    period_start: z
      .string()
      .nullable()
      .describe('ISO 8601. For an hourly summary, the start of its hour; for an ad-hoc one, when it was asked for'),
    period_end: z
      .string()
      .nullable()
      .describe('ISO 8601. For an hourly summary, the end of its hour; for an ad-hoc one, the same as period_start'),
    day_date: z
      .string()
      .nullable()
      .describe('The local (Australia/Sydney) date the summary belongs to, YYYY-MM-DD: the day its hour starts on, or for an ad-hoc summary the day of its first transcript'),
    hour_slot: z
      .number()
      .int()
      .nullable()
      .describe('The local hour an hourly summary covers, numbered by the hour it ends: 1 is 00:00 to 01:00 and 24 is 23:00 to midnight. null for an ad-hoc summary'),
    summary: z.string().describe('The summary text; empty when the model gave none that could be read'),
    call_count: z.number().int().describe('How many transcribed receptions it was written from'),
    transcript_chars: z.number().int().describe('Total characters of transcript it was written from'),
    model: z.string().nullable().describe('The language model asked to write it'),
    details: z
      .record(z.string(), z.unknown())
      .describe(
        'Further detail, which varies by summary. Current ones carry period_label (the period in words) and tz (the time zone used); ad-hoc ones also source, n, requested_n, transcripts_start and transcripts_end; and, when the model answered in the expected structure, structured — the incidents it found, each with the receptions it grouped (member_call_ids) and up to ten of their transcripts. An empty object when there is none.',
      ),
    release_at: z
      .string()
      .nullable()
      .describe('ISO 8601. Before this time the summary is held back; null when it was never held'),
    created_at: z.string().nullable().describe('When it was written, ISO 8601'),
  })
  .meta({ id: 'RadioSummary' });
assertShape<SameShape<z.infer<typeof RadioSummary>, SummaryShape>>();

const RadioLatestSummary = z
  .object({
    hourly: RadioSummary.nullable().describe(
      'The most recently written summary that has been released, hourly or ad-hoc despite the name; null when there is none',
    ),
  })
  .meta({ id: 'RadioLatestSummary' });

const RadioSummaries = z
  .object({
    total: z.number().int().describe('How many summaries match, across all pages'),
    limit: z.number().describe('The page size used'),
    offset: z.number().describe('The offset used'),
    results: z.array(RadioSummary).describe('This page, newest period first'),
  })
  .meta({ id: 'RadioSummaries' });

// Read by hand in the handler, exactly as it always was: the validator only
// describes the parameters and never refuses. The handler's own checks on
// hour and type are the only 400s.
const SummariesQuery = z.object({
  type: z
    .string()
    .optional()
    .catch(undefined)
    .describe("hourly or adhoc; anything else is a 400. Empty means both. With hour and no type, hourly."),
  hour: z.coerce
    .number()
    .int()
    .min(1)
    .max(24)
    .optional()
    .catch(undefined)
    .describe('Only hourly summaries for this hour_slot, 1 to 24; anything else, empty included, is a 400'),
  date: z.string().optional().catch(undefined).describe('Only this day_date, YYYY-MM-DD'),
  date_from: z.string().optional().catch(undefined).describe('Only day_date on or after this, YYYY-MM-DD'),
  date_to: z.string().optional().catch(undefined).describe('Only day_date on or before this, YYYY-MM-DD'),
  limit: z.coerce
    .number()
    .optional()
    .catch(undefined)
    .describe('Page size, held to 1 to 500. Default 50, which is also what 0 or something that is not a number reads as.'),
  offset: z.coerce
    .number()
    .optional()
    .catch(undefined)
    .describe('How many to skip. Default 0, which is also what a negative number or something that is not a number reads as.'),
});

const SUMMARIES_UNAVAILABLE = 'The radio summaries are not available on this server.';
const SUMMARIES_FAILED = 'The radio summaries could not be read.';

summariesRouter.get(
  '/api/summaries/latest',
  SCOPE_SUMMARIES_HISTORY,
  describeRoute({
    operationId: 'getLatestSummary',
    tags: ['Radio'],
    summary: 'The latest radio summary',
    description:
      "The most recent AI-written summary of the radio traffic AusAware receives. AusAware records each reception and transcribes it by speech recognition; once an hour, at five minutes to the hour (Sydney time), a language model summarises that hour's transcripts, and the summary is released on the hour — so the last few minutes of an hour may not be in it. Summaries can also be written on demand from the latest transcripts. A summary is a model's reading of machine transcripts: it can be wrong.",
    responses: {
      ...okJson(envelope(RadioLatestSummary), 'The latest summary, or null'),
      ...GATED_ERRORS,
      ...UNAVAILABLE_ERROR,
      500: errorJson('The summaries could not be read (internal_error)'),
    },
  }),
  async (c) => {
    try {
      const pool = await getPool();
      if (!pool) {
        return fail(c, 503, 'service_unavailable', SUMMARIES_UNAVAILABLE, { error: 'database not configured' });
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
      return ok(c, { hourly: row ? rowToSummary(row) : null } satisfies z.infer<typeof RadioLatestSummary>);
    } catch (err) {
      log.error({ err }, '/api/summaries/latest error');
      return fail(c, 500, 'internal_error', SUMMARIES_FAILED, { error: 'failed to fetch summaries' });
    }
  },
);

summariesRouter.get(
  '/api/summaries',
  SCOPE_SUMMARIES_HISTORY,
  describeRoute({
    operationId: 'getSummaries',
    tags: ['Radio'],
    summary: 'Radio summaries',
    description:
      'The AI-written summaries of the radio traffic AusAware receives (see the latest summary), newest period first, a page at a time. Summaries not yet released are left out. Filter by type, by hour, or by date.',
    responses: {
      ...okJson(envelope(RadioSummaries), 'A page of summaries'),
      ...GATED_ERRORS,
      400: errorJson('hour is not 1 to 24, or type is not hourly or adhoc (validation_failed)'),
      ...UNAVAILABLE_ERROR,
      500: errorJson('The summaries could not be read, or a parameter could not be used as given, such as a date that is not one (internal_error)'),
    },
  }),
  queryValidator(SummariesQuery),
  async (c) => {
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
          return fail(
            c,
            400,
            'validation_failed',
            'hour: must be 1 to 24',
            { error: 'hour must be 1..24' },
            { issues: [{ path: 'hour', message: 'must be 1 to 24' }] },
          );
        }
        clauses.push(`hour_slot = $${params.length + 1}`);
        params.push(hour);
        if (!stype) stype = 'hourly';
      }
      if (stype) {
        if (stype !== 'hourly' && stype !== 'adhoc') {
          return fail(
            c,
            400,
            'validation_failed',
            "type: must be 'hourly' or 'adhoc'",
            { error: "type must be 'hourly' or 'adhoc'" },
            { issues: [{ path: 'type', message: "must be 'hourly' or 'adhoc'" }] },
          );
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
        return fail(c, 503, 'service_unavailable', SUMMARIES_UNAVAILABLE, { error: 'database not configured' });
      }
      const [countRes, dataRes] = await Promise.all([
        pool.query<{ n: number }>(countSql, countParams),
        pool.query<SummaryRow>(dataSql, dataParams),
      ]);

      return ok(c, {
        total: countRes.rows[0]?.n ?? 0,
        limit,
        offset,
        results: dataRes.rows.map(rowToSummary),
      } satisfies z.infer<typeof RadioSummaries>);
    } catch (err) {
      log.error({ err }, '/api/summaries error');
      return fail(c, 500, 'internal_error', SUMMARIES_FAILED, { error: 'failed to fetch summaries' });
    }
  },
);

// /api/summaries/trigger — manual summary generation. Only POST (json
// body) drives generation and it is owner-gated (see route registration
// below); GET returns 405 so a browser address bar can't start a job.
// Recent triggers run synchronously by default for short N; longer Ns
// can opt into a background queue with `?sync=0` (kept for python-cli
// parity but the response shape stays the same).
async function handleTrigger(c: Context): Promise<Response> {
  if (!isRdioConfigured()) {
    return c.json({ error: 'RDIO_DATABASE_URL not configured' }, 503);
  }
  if (!config.GEMINI_API_KEY) {
    return c.json({ error: 'GEMINI_API_KEY not configured' }, 503);
  }
  let body: Record<string, unknown> = {};
  try {
    if (c.req.method === 'POST') {
      const parsed = await c.req.json();
      if (parsed && typeof parsed === 'object') {
        body = parsed as Record<string, unknown>;
      }
    }
  } catch {
    body = {};
  }
  const url = new URL(c.req.url);
  const pick = (k: string, d: string | null = null): string | null => {
    const fromBody = body[k];
    if (typeof fromBody === 'string' || typeof fromBody === 'number') {
      return String(fromBody);
    }
    return url.searchParams.get(k) ?? d;
  };

  const stype = pick('type', 'hourly') ?? 'hourly';

  if (stype === 'hourly') {
    let hourStartUtc: Date;
    const hs = pick('hour_start');
    if (hs) {
      const ms = Date.parse(hs);
      if (!Number.isFinite(ms)) {
        return c.json({ error: 'hour_start must be ISO8601' }, 400);
      }
      // Snap to top-of-hour in UTC to match python's
      // .replace(minute=0, second=0, microsecond=0) behaviour.
      const d = new Date(ms);
      d.setUTCMinutes(0, 0, 0);
      hourStartUtc = d;
    } else {
      // Default: previous hour, top-of-hour UTC. Python uses the local
      // tz then converts; for the trigger this matches because
      // top-of-hour-local is also top-of-hour-UTC for any whole-hour
      // offset like Australia/Sydney.
      const now = new Date();
      now.setUTCHours(now.getUTCHours() - 1);
      now.setUTCMinutes(0, 0, 0);
      hourStartUtc = now;
    }
    try {
      const result = await generateRdioHourlySummary({
        hourStartUtc,
        force: true,
      });
      return c.json({
        ok: true,
        type: 'hourly',
        period_start: hourStartUtc.toISOString(),
        result,
      });
    } catch (err) {
      log.error({ err }, '/api/summaries/trigger hourly error');
      return c.json({ error: 'failed to generate summary' }, 500);
    }
  }

  if (stype === 'recent') {
    const rawN = pick('last_n', '500') ?? '500';
    const n = Number.parseInt(rawN, 10);
    if (!Number.isFinite(n)) {
      return c.json({ error: 'last_n must be an integer' }, 400);
    }
    if (n < 1 || n > 5000) {
      return c.json({ error: 'last_n must be between 1 and 5000' }, 400);
    }
    const sync = (pick('sync', '') ?? '').toLowerCase() === '1' || (pick('sync', '') ?? '').toLowerCase() === 'true';
    if (!sync) {
      // Background queue: kick off the work and return immediately so
      // edge timeouts (Cloudflare 100s) don't kill long Gemini calls.
      void (async () => {
        try {
          await generateRdioRecentSummary({ n, force: true });
        } catch (err) {
          log.warn({ err: (err as Error).message }, 'background recent summary failed');
        }
      })();
      return c.json({
        ok: true,
        queued: true,
        requested_n: n,
        message:
          'Summary is running in the background. Poll /api/summaries/latest ' +
          'to see it when ready (typically 30–120s for large N).',
      });
    }
    try {
      const result = await generateRdioRecentSummary({ n, force: true });
      return c.json({ ok: true, type: 'recent', result });
    } catch (err) {
      log.error({ err }, '/api/summaries/trigger recent error');
      return c.json({ error: 'failed to generate summary' }, 500);
    }
  }

  return c.json({ error: "type must be 'hourly' or 'recent'" }, 400);
}

// Only a logged-in owner (verified Supabase JWT + owner role) may drive
// Gemini generation — the public NSWPSN_API_KEY (handed to every client
// via /api/config) must not be able to trigger quota/billing drain or
// hammer the rdio DB.
summariesRouter.post('/api/summaries/trigger', requireRole(isOwner), handleTrigger);

// GET must NOT be able to start a job — a browser address bar hitting
// this URL would otherwise kick off generation. Return 405 and tell the
// caller to POST instead.
summariesRouter.get('/api/summaries/trigger', (c) =>
  c.json(
    { error: 'method not allowed; POST to /api/summaries/trigger to generate' },
    405,
  ),
);
