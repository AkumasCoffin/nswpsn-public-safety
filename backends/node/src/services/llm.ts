/**
 * Gemini-powered rdio-scanner summary generation.
 *
 * Mirrors python external_api_proxy.py:14736-15578. Three public entry
 * points:
 *   - generateRdioHourlySummary({ hourStartLocal, force, releaseAt })
 *   - generateRdioRecentSummary({ n, force })
 *   - startRdioSummaryScheduler()  (gated by NODE_RDIO_SCHEDULER=true)
 *
 * Notes vs python:
 *   - The structured-output validator at python lines 14874-15164 is
 *     ported in `rdioValidator.ts` and wired into both summary
 *     generators below as a best-effort cleanup. If validation throws,
 *     we log and persist the un-validated structured object — matches
 *     python's "best-effort cleanup" semantics so a validator bug never
 *     blocks the summary save.
 *   - The transcript dedup pass (_dedupe_calls) is similarly skipped.
 *     Both backends may end up summarising slightly more verbose call
 *     lists than python; acceptable trade-off for keeping the port
 *     bounded.
 *   - JSON-mode parsing uses the same lenient strategy: fenced code,
 *     brace-extraction, double-quote typo scrub.
 */
import { promises as fs } from 'node:fs';
import * as path from 'node:path';
import { fetch } from 'undici';
import type { Pool } from 'pg';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { getRdioPool, resolveLabels, getUnitLabel, ensureUnitLabelsLoaded } from './rdio.js';
import { getPool } from '../db/pool.js';
import { validateStructuredAgainstTranscripts } from './rdioValidator.js';

const LLM_URL =
  'https://generativelanguage.googleapis.com/v1beta/openai/chat/completions';

const SUMMARY_MAX_PROMPT_CHARS = 1_000_000; // matches python's threshold

const HOURLY_PROMPT_FALLBACK =
  'You are an emergency-services dispatch analyst. You are given transcripts of ' +
  'public-safety radio calls, grouped by agency (system) and talkgroup label. ' +
  'Produce a concise text summary of the period in JSON form: ' +
  '{"overview": "...", "incidents": [...], "quiet_hour": false}.';

interface RdioCallRow {
  call_id: number;
  date_time: Date | null;
  system: number | null;
  talkgroup: number | null;
  transcript: string | null;
  source: number | string | null;
  sources: unknown;
}

// ---------------------------------------------------------------------------
// Prompt loader
// ---------------------------------------------------------------------------

const PROMPTS_DIR = path.resolve(process.cwd(), '..', 'prompts');

export async function loadRdioPrompt(kind: 'hourly'): Promise<string> {
  const envOverride = process.env[`RDIO_PROMPT_${kind.toUpperCase()}`];
  const fp = envOverride ?? path.join(PROMPTS_DIR, `rdio_${kind}.txt`);
  try {
    const text = (await fs.readFile(fp, 'utf8')).trim();
    if (text) return text;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
      log.warn(
        { err: (err as Error).message, path: fp },
        'rdio prompt load error',
      );
    }
  }
  return HOURLY_PROMPT_FALLBACK;
}

// ---------------------------------------------------------------------------
// Radio-id extractor (same logic as transcripts.ts but local copy keeps
// the LLM module self-contained and testable in isolation).
// ---------------------------------------------------------------------------

function extractRadioId(row: RdioCallRow): number | null {
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

// ---------------------------------------------------------------------------
// Prompt formatter — groups calls by (system, talkgroup) and emits one
// `[HH:MM:SS #call_id <Label> (RID:N)] <transcript>` line per call.
// ---------------------------------------------------------------------------

function formatLocalTime(d: Date | null): string {
  if (!d) return '';
  // `Date` already holds UTC ms. Format HH:MM:SS in the configured
  // SUMMARY_TZ via Intl. Postgres returns naive timestamps as Date
  // objects in UTC, so we feed them through directly.
  try {
    return new Intl.DateTimeFormat('en-GB', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
      timeZone: config.SUMMARY_TZ,
    }).format(d);
  } catch {
    return d.toISOString().slice(11, 19);
  }
}

export async function formatRdioPrompt(
  calls: RdioCallRow[],
  periodLabel: string,
): Promise<{ prompt: string; totalChars: number }> {
  await ensureUnitLabelsLoaded();
  type GroupKey = string;
  const groups = new Map<
    GroupKey,
    {
      systemLabel: string;
      talkgroupDisplay: string;
      items: Array<{
        dt: Date | null;
        text: string;
        radioId: number | null;
        callId: number;
      }>;
    }
  >();
  let totalChars = 0;
  for (const row of calls) {
    const text = (row.transcript ?? '').trim();
    if (!text) continue;
    const { systemLabel, talkgroupLabel } = await resolveLabels(
      row.system,
      row.talkgroup,
    );
    const sysLabel = systemLabel ?? 'Unknown System';
    const tgDisplay = talkgroupLabel ?? 'Unknown Talkgroup';
    const radioId = extractRadioId(row);
    const key = `${sysLabel}|${tgDisplay}`;
    let group = groups.get(key);
    if (!group) {
      group = {
        systemLabel: sysLabel,
        talkgroupDisplay: tgDisplay,
        items: [],
      };
      groups.set(key, group);
    }
    group.items.push({
      dt: row.date_time,
      text,
      radioId,
      callId: row.call_id,
    });
    totalChars += text.length;
  }

  const lines: string[] = [`Period: ${periodLabel}`, ''];
  const sortedGroups = Array.from(groups.values()).sort((a, b) =>
    `${a.systemLabel}|${a.talkgroupDisplay}`.localeCompare(
      `${b.systemLabel}|${b.talkgroupDisplay}`,
    ),
  );
  for (const group of sortedGroups) {
    lines.push(
      `=== ${group.systemLabel} — ${group.talkgroupDisplay} (${group.items.length} transmissions) ===`,
    );
    for (const item of group.items) {
      const t = formatLocalTime(item.dt);
      const cidTag = item.callId ? ` #${item.callId}` : '';
      let ridTag = '';
      if (item.radioId) {
        const lbl = getUnitLabel(item.radioId);
        ridTag = lbl ? ` ${lbl} (RID:${item.radioId})` : ` RID:${item.radioId}`;
      }
      lines.push(`[${t}${cidTag}${ridTag}] ${item.text}`);
    }
    lines.push('');
  }

  let prompt = lines.join('\n');
  if (prompt.length > SUMMARY_MAX_PROMPT_CHARS) {
    prompt = `${prompt.slice(0, SUMMARY_MAX_PROMPT_CHARS)}\n\n[... truncated ...]`;
  }
  return { prompt, totalChars };
}

// ---------------------------------------------------------------------------
// Gemini HTTP client with retries on 429/5xx + connect-timeout.
// ---------------------------------------------------------------------------

interface ChatChoice {
  finish_reason?: string;
  native_finish_reason?: string;
  message: { content: string };
}
interface ChatResponse {
  choices: ChatChoice[];
}

const TRANSIENT_STATUSES = new Set([429, 500, 502, 503, 504]);

export async function callLlm(opts: {
  systemPrompt: string;
  userPrompt: string;
  model: string;
  jsonMode?: boolean;
  maxTokens?: number;
  maxAttempts?: number;
}): Promise<string> {
  const apiKey = config.GEMINI_API_KEY;
  if (!apiKey) throw new Error('GEMINI_API_KEY not set');
  const maxAttempts = opts.maxAttempts ?? 4;
  const payload: Record<string, unknown> = {
    model: opts.model,
    temperature: 0.2,
    max_tokens: opts.maxTokens ?? 60_000,
    messages: [
      { role: 'system', content: opts.systemPrompt },
      { role: 'user', content: opts.userPrompt },
    ],
  };
  if (opts.jsonMode !== false) {
    payload['response_format'] = { type: 'json_object' };
  }

  let lastErr: Error | null = null;
  let lastStatus: number | null = null;
  let lastBody = '';
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), 10 * 60_000); // 10 min hard cap
    try {
      const res = await fetch(LLM_URL, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
        signal: ac.signal,
      });
      lastStatus = res.status;
      if (res.ok) {
        const data = (await res.json()) as ChatResponse;
        const choice = data.choices?.[0];
        if (!choice) throw new Error('Gemini response had no choices');
        const finish = choice.finish_reason ?? choice.native_finish_reason ?? '';
        if (finish && !['stop', 'STOP'].includes(finish)) {
          log.warn({ finish }, 'LLM finish_reason non-stop (output may be truncated)');
        }
        return choice.message.content.trim();
      }
      lastBody = (await res.text()).slice(0, 1000);
      if (TRANSIENT_STATUSES.has(res.status) && attempt < maxAttempts - 1) {
        const retryAfter = Number(res.headers.get('Retry-After') ?? '0');
        const wait =
          retryAfter > 0 ? retryAfter * 1000 : Math.min(60_000, 5_000 * 2 ** attempt);
        log.warn(
          { status: res.status, attempt: attempt + 1, waitMs: wait },
          'Gemini transient error, backing off',
        );
        await new Promise((r) => setTimeout(r, wait));
        continue;
      }
      throw new Error(`Gemini HTTP ${res.status}: ${lastBody}`);
    } catch (err) {
      const e = err as Error;
      // AbortError or network issues — retry.
      if (e.name === 'AbortError' || (e as { code?: string }).code) {
        lastErr = e;
        if (attempt < maxAttempts - 1) {
          const wait = Math.min(60_000, 10_000 * 2 ** attempt);
          log.warn({ err: e.message, attempt: attempt + 1, waitMs: wait }, 'Gemini network error');
          await new Promise((r) => setTimeout(r, wait));
          continue;
        }
        throw new Error(`Gemini network error after ${maxAttempts} attempts: ${e.message}`);
      }
      // Non-transient HTTP error or programming error — bubble.
      throw e;
    } finally {
      clearTimeout(t);
    }
  }
  throw new Error(
    `Gemini still ${lastStatus ?? 'unreachable'} after ${maxAttempts} attempts: ${lastBody || lastErr?.message || ''}`,
  );
}

// ---------------------------------------------------------------------------
// Lenient JSON parser — fenced code, brace-extraction, typo scrub.
// ---------------------------------------------------------------------------

function scrubLlmTypos(text: string): string {
  // Collapse 2+ opening quotes before a field-name-like token.
  return text.replace(/"{2,}([A-Za-z_]\w*)"\s*:/g, '"$1":');
}

export interface ParsedSummary {
  overview: string;
  structured: Record<string, unknown> | null;
}

export function parseSummaryOutput(text: string): ParsedSummary {
  if (!text) return { overview: '', structured: null };

  let cleaned = text.replace(/^﻿/, '').trim();
  if (cleaned.startsWith('```')) {
    const nl = cleaned.indexOf('\n');
    cleaned = nl >= 0 ? cleaned.slice(nl + 1) : cleaned.slice(3);
    if (cleaned.endsWith('```')) cleaned = cleaned.slice(0, -3);
    cleaned = cleaned.trim();
  }
  if (cleaned.toLowerCase().startsWith('json\n')) {
    cleaned = cleaned.slice(5).trim();
  }

  const attempts: Array<[string, string]> = [['raw', cleaned]];
  const first = cleaned.indexOf('{');
  const last = cleaned.lastIndexOf('}');
  if (first >= 0 && last > first) {
    attempts.push(['braces', cleaned.slice(first, last + 1)]);
  }
  const scrubbed = scrubLlmTypos(cleaned);
  if (scrubbed !== cleaned) attempts.push(['scrubbed', scrubbed]);

  for (const [label, candidate] of attempts) {
    try {
      const data = JSON.parse(candidate);
      if (data && typeof data === 'object' && !Array.isArray(data)) {
        const obj = data as Record<string, unknown>;
        let overview = (obj['overview'] as string | undefined) ?? '';
        if (!overview && obj['quiet_hour']) {
          overview = 'Quiet hour — no significant incidents detected.';
        }
        if (label !== 'raw') {
          log.info({ label }, 'summary parse recovered');
        }
        return { overview, structured: obj };
      }
    } catch {
      // try next attempt
    }
  }

  log.warn(
    {
      len: text.length,
      head: text.slice(0, 120).replace(/\n/g, ' '),
      tail: text.slice(-120).replace(/\n/g, ' '),
    },
    'summary parse failed',
  );
  return { overview: text, structured: null };
}

// ---------------------------------------------------------------------------
// rdio_summaries persistence
// ---------------------------------------------------------------------------

interface SaveSummaryParams {
  summaryType: 'hourly' | 'adhoc';
  periodStart: Date;
  periodEnd: Date;
  dayDate: string; // YYYY-MM-DD
  hourSlot: number | null;
  summary: string;
  callCount: number;
  transcriptChars: number;
  model: string;
  details: Record<string, unknown>;
  releaseAt: Date | null;
}

export async function saveRdioSummary(p: SaveSummaryParams): Promise<void> {
  const pool = await getPool();
  if (!pool) throw new Error('main DATABASE_URL not configured');
  await pool.query(
    `INSERT INTO rdio_summaries
       (summary_type, period_start, period_end, day_date, hour_slot,
        summary, call_count, transcript_chars, model, details, release_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
     ON CONFLICT (summary_type, period_start) DO UPDATE SET
       period_end = EXCLUDED.period_end,
       day_date = EXCLUDED.day_date,
       hour_slot = EXCLUDED.hour_slot,
       summary = EXCLUDED.summary,
       call_count = EXCLUDED.call_count,
       transcript_chars = EXCLUDED.transcript_chars,
       model = EXCLUDED.model,
       details = EXCLUDED.details,
       release_at = EXCLUDED.release_at,
       created_at = now()`,
    [
      p.summaryType,
      p.periodStart,
      p.periodEnd,
      p.dayDate,
      p.hourSlot,
      p.summary,
      p.callCount,
      p.transcriptChars,
      p.model,
      JSON.stringify(p.details),
      p.releaseAt,
    ],
  );
}

// ---------------------------------------------------------------------------
// Transcript fetchers
// ---------------------------------------------------------------------------

async function fetchCallsBetween(
  startUtc: Date,
  endUtc: Date,
  minTranscriptLen = 2,
): Promise<RdioCallRow[]> {
  const rdio = await getRdioPool();
  if (!rdio) throw new Error('RDIO_DATABASE_URL not configured');
  // rdio-scanner's `dateTime` column is `timestamp WITHOUT time zone`
  // holding UTC values. pg-node sends a JS Date as `timestamptz`, so a
  // naked `WHERE "dateTime" >= $1` makes Postgres convert the param to
  // the session's local time (e.g. Australia/Sydney) before comparing
  // to the naive column — which silently shifts the whole window by
  // ~10 hours and the query returns 0 calls. Mirrors python's
  // `_rdio_fetch_calls_between` which strips tz-info before passing
  // the bounds (psycopg2 → naive `timestamp`). Force-cast to
  // `timestamptz AT TIME ZONE 'UTC'` so the param is treated as UTC
  // regardless of session timezone.
  const res = await rdio.query<RdioCallRow>(
    `SELECT "id" AS call_id, "dateTime" AS date_time, "system", "talkgroup",
            "transcript", "source", "sources"
       FROM "rdioScannerCalls"
      WHERE "dateTime" >= ($1::timestamptz AT TIME ZONE 'UTC')
        AND "dateTime" <  ($2::timestamptz AT TIME ZONE 'UTC')
        AND "transcript" IS NOT NULL
        AND length(btrim("transcript")) >= $3
      ORDER BY "dateTime" ASC`,
    [startUtc, endUtc, minTranscriptLen],
  );
  return res.rows;
}

async function fetchLastNCalls(n: number): Promise<RdioCallRow[]> {
  const rdio = await getRdioPool();
  if (!rdio) throw new Error('RDIO_DATABASE_URL not configured');
  const res = await rdio.query<RdioCallRow>(
    `SELECT "id" AS call_id, "dateTime" AS date_time, "system", "talkgroup",
            "transcript", "source", "sources"
       FROM "rdioScannerCalls"
      WHERE "transcript" IS NOT NULL
        AND length(btrim("transcript")) >= 2
      ORDER BY "dateTime" DESC
      LIMIT $1`,
    [n],
  );
  // Reverse so caller sees oldest-first like the hourly fetcher.
  return res.rows.slice().reverse();
}

// ---------------------------------------------------------------------------
// Hourly summary
// ---------------------------------------------------------------------------

function localDayString(d: Date): string {
  // YYYY-MM-DD in SUMMARY_TZ. Used for `day_date` column.
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: config.SUMMARY_TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  return fmt.format(d);
}

function localHourSlot(d: Date): number {
  // 1-24 convention to match python's `_save_rdio_summary` callers
  // (24 = 23:00-24:00 local).
  const fmt = new Intl.DateTimeFormat('en-GB', {
    timeZone: config.SUMMARY_TZ,
    hour: '2-digit',
    hour12: false,
  });
  const parts = fmt.format(d).split(':');
  const h = Number(parts[0] ?? 0);
  return h === 0 ? 24 : h;
}

function formatPeriodLabel(start: Date, end: Date): string {
  const fmtBoth = new Intl.DateTimeFormat('en-AU', {
    timeZone: config.SUMMARY_TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZoneName: 'short',
  });
  const fmtTime = new Intl.DateTimeFormat('en-AU', {
    timeZone: config.SUMMARY_TZ,
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZoneName: 'short',
  });
  return `${fmtBoth.format(start)} to ${fmtTime.format(end)}`;
}

export interface GenerateHourlyParams {
  /** UTC instant marking the start of the hour to summarise. */
  hourStartUtc: Date;
  force?: boolean;
  releaseAt?: Date | null;
}

export async function generateRdioHourlySummary(
  p: GenerateHourlyParams,
): Promise<{ hour_slot: number; call_count: number } | null> {
  const model = config.LLM_MODEL;
  const start = p.hourStartUtc;
  const end = new Date(start.getTime() + 60 * 60_000);
  const calls = await fetchCallsBetween(start, end);
  if (calls.length === 0 && !p.force && !p.releaseAt) {
    log.info({ start: start.toISOString() }, 'hourly: no transcripts, skipping');
    return null;
  }
  const periodLabel = formatPeriodLabel(start, end);
  const { prompt, totalChars } = await formatRdioPrompt(calls, periodLabel);
  let summaryText: string;
  let structured: Record<string, unknown> | null = null;
  if (calls.length === 0) {
    summaryText =
      'No radio traffic with transcripts was recorded during this hour.';
  } else {
    const systemPrompt = await loadRdioPrompt('hourly');
    const raw = await callLlm({ systemPrompt, userPrompt: prompt, model });
    const parsed = parseSummaryOutput(raw);
    summaryText = parsed.overview;
    structured = parsed.structured;
    if (structured) {
      try {
        structured = validateStructuredAgainstTranscripts(
          structured,
          calls,
        ) as Record<string, unknown> | null;
      } catch (err) {
        log.warn(
          { err: (err as Error).message },
          'rdio validator threw; persisting un-validated structured',
        );
      }
    }
  }
  const details: Record<string, unknown> = {
    period_label: periodLabel,
    tz: config.SUMMARY_TZ,
  };
  if (structured) details['structured'] = structured;
  await saveRdioSummary({
    summaryType: 'hourly',
    periodStart: start,
    periodEnd: end,
    dayDate: localDayString(start),
    hourSlot: localHourSlot(end),
    summary: summaryText,
    callCount: calls.length,
    transcriptChars: totalChars,
    model,
    details,
    releaseAt: p.releaseAt ?? null,
  });
  return { hour_slot: localHourSlot(end), call_count: calls.length };
}

// ---------------------------------------------------------------------------
// Recent summary (ad-hoc)
// ---------------------------------------------------------------------------

export interface GenerateRecentParams {
  n: number;
  force?: boolean;
}

export async function generateRdioRecentSummary(
  p: GenerateRecentParams,
): Promise<{ call_count: number; requested_n: number } | null> {
  const n = Math.max(1, Math.min(5000, Math.floor(p.n)));
  const model = config.LLM_MODEL;
  const calls = await fetchLastNCalls(n);
  if (calls.length === 0 && !p.force) {
    log.info({ n }, 'recent: no transcripts, skipping');
    return null;
  }
  const startUtc = calls[0]?.date_time ?? new Date();
  const endUtc = calls[calls.length - 1]?.date_time ?? new Date();
  const periodLabel = `Last ${calls.length} transcripts (${formatPeriodLabel(startUtc, endUtc)})`;
  const { prompt, totalChars } = await formatRdioPrompt(calls, periodLabel);
  let summaryText = '';
  let structured: Record<string, unknown> | null = null;
  try {
    const systemPrompt = await loadRdioPrompt('hourly');
    const raw = await callLlm({ systemPrompt, userPrompt: prompt, model });
    const parsed = parseSummaryOutput(raw);
    summaryText = parsed.overview;
    structured = parsed.structured;
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'recent llm error');
    return null;
  }
  if (structured) {
    try {
      structured = validateStructuredAgainstTranscripts(
        structured,
        calls,
      ) as Record<string, unknown> | null;
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'rdio validator threw; persisting un-validated structured',
      );
    }
  }
  const nowUtc = new Date();
  const details: Record<string, unknown> = {
    tz: config.SUMMARY_TZ,
    source: 'last_n',
    n: calls.length,
    requested_n: n,
    period_label: periodLabel,
    transcripts_start: startUtc.toISOString(),
    transcripts_end: endUtc.toISOString(),
  };
  if (structured) details['structured'] = structured;
  await saveRdioSummary({
    summaryType: 'adhoc',
    periodStart: nowUtc,
    periodEnd: nowUtc,
    dayDate: localDayString(startUtc),
    hourSlot: null,
    summary: summaryText,
    callCount: calls.length,
    transcriptChars: totalChars,
    model,
    details,
    releaseAt: null,
  });
  return { call_count: calls.length, requested_n: n };
}

// ---------------------------------------------------------------------------
// Hourly scheduler — fires at HH:55 each hour, releases at HH+1:00.
// ---------------------------------------------------------------------------

let schedulerTimer: NodeJS.Timeout | null = null;
let schedulerStopped = false;
// Diagnostics surface: every fire updates these so /api/status can show
// "scheduler armed: yes, next fire 13:55Z, last fire 12:55Z (42 calls)".
// Without this we had to grep logs to confirm the scheduler was even
// running — and recent deploys went without a fire-line for hours.
interface SchedulerStats {
  enabled: boolean;
  reason: string | null;
  next_fire_at: number | null; // epoch seconds
  last_fire_at: number | null; // epoch seconds
  last_run_ms: number | null;
  last_result: { hour_slot: number; call_count: number } | null;
  last_error: string | null;
  total_fires: number;
}
const schedulerStats: SchedulerStats = {
  enabled: false,
  reason: null,
  next_fire_at: null,
  last_fire_at: null,
  last_run_ms: null,
  last_result: null,
  last_error: null,
  total_fires: 0,
};

export function rdioSchedulerStats(): SchedulerStats {
  return { ...schedulerStats };
}

function localMinute(d: Date): number {
  const fmt = new Intl.DateTimeFormat('en-GB', {
    timeZone: config.SUMMARY_TZ,
    minute: '2-digit',
    hour12: false,
  });
  return Number(fmt.format(d));
}

function nextFireTimeMs(): number {
  // Next HH:55 in SUMMARY_TZ. Use a rolling-minute approach: get the
  // current minute in tz, compute how many minutes until :55, schedule.
  const now = Date.now();
  const minute = localMinute(new Date(now));
  let waitMin = 55 - minute;
  if (waitMin <= 0) waitMin += 60;
  return now + waitMin * 60_000 + (60 - new Date().getSeconds()) * 1000;
}

async function runHourlyJob(): Promise<void> {
  // Fire-time minute is :55; the hour we summarise starts at the top of
  // the in-progress hour. Compute hourStartUtc as "the most recent
  // top-of-hour in UTC strictly before now".
  const now = new Date();
  const ms = now.getTime();
  const hourStartUtc = new Date(ms - (ms % (60 * 60_000)));
  const releaseAtUtc = new Date(hourStartUtc.getTime() + 60 * 60_000);
  schedulerStats.last_fire_at = Math.floor(ms / 1000);
  schedulerStats.total_fires += 1;
  schedulerStats.last_error = null;
  log.info(
    { hour_start: hourStartUtc.toISOString() },
    'rdio hourly: firing',
  );
  const t0 = Date.now();
  try {
    const result = await generateRdioHourlySummary({
      hourStartUtc,
      force: true,
      releaseAt: releaseAtUtc,
    });
    schedulerStats.last_result = result;
    schedulerStats.last_run_ms = Date.now() - t0;
    log.info(
      { ms: schedulerStats.last_run_ms, result },
      'rdio hourly: complete',
    );
  } catch (err) {
    const msg = (err as Error).message;
    schedulerStats.last_error = msg;
    schedulerStats.last_run_ms = Date.now() - t0;
    log.warn({ err: msg }, 'hourly scheduler job error');
  }
}

export function startRdioSummaryScheduler(): void {
  if (!config.NODE_RDIO_SCHEDULER) {
    schedulerStats.enabled = false;
    schedulerStats.reason = 'NODE_RDIO_SCHEDULER=false';
    log.info('rdio summary scheduler disabled (NODE_RDIO_SCHEDULER=false)');
    return;
  }
  if (!config.RDIO_DATABASE_URL) {
    schedulerStats.enabled = false;
    schedulerStats.reason = 'RDIO_DATABASE_URL not set';
    log.info('rdio scheduler skipped: RDIO_DATABASE_URL not set');
    return;
  }
  if (!config.GEMINI_API_KEY) {
    schedulerStats.enabled = false;
    schedulerStats.reason = 'GEMINI_API_KEY not set';
    log.info('rdio scheduler skipped: GEMINI_API_KEY not set');
    return;
  }
  schedulerStopped = false;
  schedulerStats.enabled = true;
  schedulerStats.reason = null;
  const arm = (waitMs: number): void => {
    schedulerStats.next_fire_at = Math.floor((Date.now() + waitMs) / 1000);
    schedulerTimer = setTimeout(fire, waitMs);
  };
  const fire = (): void => {
    if (schedulerStopped) return;
    void runHourlyJob().finally(() => {
      if (schedulerStopped) return;
      const wait = Math.max(60_000, nextFireTimeMs() - Date.now());
      arm(wait);
    });
  };
  const wait = Math.max(60_000, nextFireTimeMs() - Date.now());
  arm(wait);
  const nextIso = new Date(Date.now() + wait).toISOString();
  log.info({ next_fire: nextIso, wait_ms: wait }, 'rdio summary scheduler started');
}

export function stopRdioSummaryScheduler(): void {
  schedulerStopped = true;
  schedulerStats.enabled = false;
  schedulerStats.reason = 'stopped';
  schedulerStats.next_fire_at = null;
  if (schedulerTimer) {
    clearTimeout(schedulerTimer);
    schedulerTimer = null;
  }
}

// Re-exported for tests so they can drive the pool directly without
// needing live env config.
export const _testHooks = {
  fetchCallsBetween,
  fetchLastNCalls,
  setPoolForTests: (_: Pool): void => {
    // Tests should mock getRdioPool / getPool from the modules; this
    // helper is a placeholder to discourage direct pool injection.
    void _;
  },
};
