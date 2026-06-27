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
 *   - The transcript dedup pass (_dedupe_calls) is ported as
 *     `dedupeCalls` below — required because the hourly prompt at
 *     prompts/rdio_hourly.txt explicitly tells Gemini "BACKEND HAS
 *     ALREADY DEDUPED", and skipping the pass meant the prompt was
 *     lying to the model on the Node path.
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
import { polishStructuredIncidents } from './rdioValidator.js';
import {
  ensureSpellcheckerLoaded,
  spellcheckTranscript,
} from './rdioSpell.js';

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
// Transcript text extraction. rdio-scanner stores the transcript column as
// either plain text (older / simpler setups) or as a JSON blob like
// `{"text": "Sydney from Pumper 7", "words": [...]}` when AI / Whisper
// transcription is wired through their pipeline. The prompt expects plain
// readable speech, so unwrap the JSON shape here. Falls back to the raw
// string if the JSON parse fails or the field isn't where we expect — safer
// to send Gemini a slightly-noisy line than to drop content.
// ---------------------------------------------------------------------------

export function extractTranscriptText(
  transcript: string | null | undefined,
): string {
  const raw = (transcript ?? '').trim();
  if (!raw) return '';
  if (raw.startsWith('{') && raw.includes('"text"')) {
    try {
      const obj = JSON.parse(raw) as { text?: unknown };
      if (typeof obj.text === 'string') return obj.text.trim();
    } catch {
      // fall through to raw
    }
  }
  return raw;
}

// ---------------------------------------------------------------------------
// Pre-LLM dedup — mirror of python's _dedupe_calls (external_api_proxy.py
// lines 14661-14733). A call is a duplicate of a previously-kept one when:
//   - same talkgroup, AND
//   - same RID (when both have one — if either is missing, talkgroup +
//     text is enough), AND
//   - timestamps within 180s, AND
//   - the shorter normalized transcript is a prefix of the longer.
// The longer/more-informative row wins. Order is preserved.
// ---------------------------------------------------------------------------

const DEDUP_TEXT_RE = /[^a-z0-9 ]+/g;

function normalizeTranscript(text: string | null | undefined): string {
  const raw = extractTranscriptText(text);
  const lowered = raw.toLowerCase().replace(DEDUP_TEXT_RE, ' ');
  return lowered.replace(/\s+/g, ' ').trim();
}

interface DedupSlot {
  row: RdioCallRow;
  ts: number | null;
  textNorm: string;
  rid: number | null;
  tg: number | null;
}

export function dedupeCalls(
  calls: RdioCallRow[],
  windowSeconds = 180,
): RdioCallRow[] {
  const kept: DedupSlot[] = [];
  const skipped = new Set<number>();
  for (const row of calls) {
    const textNorm = normalizeTranscript(row.transcript);
    if (!textNorm) {
      // Empty / whitespace-only transcripts can't dedup; carry through.
      kept.push({ row, ts: null, textNorm: '', rid: null, tg: null });
      continue;
    }
    const ts = row.date_time ? row.date_time.getTime() / 1000 : null;
    const rid = extractRadioId(row);
    const tg = row.talkgroup;

    let replaced = false;
    for (let idx = 0; idx < kept.length; idx++) {
      const k = kept[idx]!;
      if (!k.textNorm) continue;
      if (k.tg !== tg) continue;
      // Both sides have RIDs → must match. Either side missing → still
      // allow dedup on talkgroup + text alone (mirrors python behaviour).
      if (rid !== null && k.rid !== null && rid !== k.rid) continue;
      if (ts !== null && k.ts !== null && Math.abs(ts - k.ts) > windowSeconds) continue;
      const a = k.textNorm;
      const b = textNorm;
      const [shortText, longText] = a.length <= b.length ? [a, b] : [b, a];
      if (longText.startsWith(shortText)) {
        if (textNorm.length > k.textNorm.length) {
          if (typeof k.row.call_id === 'number') skipped.add(k.row.call_id);
          kept[idx] = { row, ts, textNorm, rid, tg };
        } else {
          if (typeof row.call_id === 'number') skipped.add(row.call_id);
        }
        replaced = true;
        break;
      }
    }
    if (!replaced) {
      kept.push({ row, ts, textNorm, rid, tg });
    }
  }
  if (skipped.size > 0) {
    log.info({ dropped: skipped.size }, 'rdio: pre-LLM dedup');
  }
  return kept.map((k) => k.row);
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

/**
 * One row in the server-authoritative input map. Built alongside the
 * Gemini prompt so the rebuilder can reattach verbatim transcripts and
 * units to the LLM's incident groupings without trusting anything
 * Gemini emits beyond `member_call_ids[]`. Keyed by call_id in the
 * returned `inputMap`.
 */
export interface RdioInputRow {
  call_id: number;
  /** Local-tz formatted time string ("HH:MM:SS"), already display-ready. */
  time: string;
  /** Verbatim transcript text from rdio-scanner. */
  text: string;
  /** Numeric radio-unit ID extracted from rdioScannerCalls.source / sources[]. */
  uid: number | null;
  /** Human label for `uid` from rdio_units.csv. Null when the UID isn't known. */
  unit_label: string | null;
  /** "<systemLabel> — <talkgroupLabel>" — useful for tooltips later. */
  context: string;
}

export async function formatRdioPrompt(
  calls: RdioCallRow[],
  periodLabel: string,
): Promise<{
  prompt: string;
  totalChars: number;
  inputMap: Map<number, RdioInputRow>;
}> {
  await ensureUnitLabelsLoaded();
  await ensureSpellcheckerLoaded();
  let totalSpellChanges = 0;
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
  const inputMap = new Map<number, RdioInputRow>();
  let totalChars = 0;
  let sampleLogged = false;
  for (const row of calls) {
    if (!sampleLogged && row.transcript) {
      // One-shot per fire: surface the actual shape of the transcript
      // column. If rdio-scanner is storing JSON blobs and we're failing
      // to unwrap them, this is where it shows up — useful when an
      // hour produces an empty summary despite a healthy call count.
      const t = row.transcript;
      log.info(
        {
          len: t.length,
          looks_json: t.trimStart().startsWith('{'),
          head: t.slice(0, 120).replace(/\n/g, ' '),
        },
        'rdio: transcript sample',
      );
      sampleLogged = true;
    }
    const rawText = extractTranscriptText(row.transcript);
    if (!rawText) continue;
    // Conservative spell-check (en-AU + rdio_units.csv label corpus +
    // rdio_lexicon.txt). Both the LLM input and the displayed
    // transcript see the same corrected string so we don't store one
    // version and feed Gemini another. See services/rdioSpell.ts for
    // the gates that make this safe for callsigns / place names.
    const { corrected: text, changes } = spellcheckTranscript(rawText);
    if (changes.length > 0) {
      totalSpellChanges += changes.length;
      log.debug(
        { call_id: row.call_id, changes },
        'rdio spell: corrections applied',
      );
    }
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
    // Populate the rebuilder's lookup table. First write wins per
    // call_id — duplicate rows for the same call (rare but possible if
    // dedupeCalls didn't merge them) would otherwise overwrite with the
    // shorter row. The dedupe at the orchestrator level should already
    // have handled this, but the guard is cheap.
    if (!inputMap.has(row.call_id)) {
      inputMap.set(row.call_id, {
        call_id: row.call_id,
        time: formatLocalTime(row.date_time),
        text,
        uid: radioId,
        unit_label: radioId ? getUnitLabel(radioId) : null,
        context: `${sysLabel} — ${tgDisplay}`,
      });
    }
    totalChars += text.length;
  }

  if (totalSpellChanges > 0) {
    log.info(
      { corrections: totalSpellChanges, calls: calls.length },
      'rdio spell: hourly summary',
    );
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
  return { prompt, totalChars, inputMap };
}

// ---------------------------------------------------------------------------
// Rebuilder — replaces Gemini's emitted transcripts/units with server-
// authoritative rows from the input map. See plan in dev-beta thread.
// ---------------------------------------------------------------------------

/** Cap on how many transcript rows are rendered per incident. A multi-
 *  agency job can pull hundreds of transmissions; the live.html /
 *  logs.html cards aren't built for that and the operator gets the
 *  most value out of dispatch context + resolution. We keep the first
 *  TRANSCRIPTS_HEAD_KEEP and the last TRANSCRIPTS_TAIL_KEEP rows
 *  chronologically; the full member_call_ids list still rides along
 *  in the response so callers with real reason to inspect more can. */
const TRANSCRIPTS_CAP = 10;
const TRANSCRIPTS_HEAD_KEEP = 8;
const TRANSCRIPTS_TAIL_KEEP = 2;

/**
 * Pull a list of member call_ids out of a single incident object. New
 * schema (post-prompt-rewrite): `member_call_ids: number[]`. Legacy
 * schema (pre-rewrite, still in use during the rollout window):
 * derive from `transcripts[].call_id`. Both forms accepted so the
 * rebuilder can run against unchanged Gemini output behind the flag.
 */
function extractMemberCallIds(inc: Record<string, unknown>): number[] {
  const out: number[] = [];
  const seen = new Set<number>();
  const push = (v: unknown): void => {
    if (v === null || v === undefined || v === '') return;
    const n = typeof v === 'number' ? v : Number(v);
    if (!Number.isFinite(n)) return;
    const id = Math.trunc(n);
    if (seen.has(id)) return;
    seen.add(id);
    out.push(id);
  };
  const explicit = inc['member_call_ids'];
  if (Array.isArray(explicit)) {
    for (const v of explicit) push(v);
    return out;
  }
  const trs = inc['transcripts'];
  if (Array.isArray(trs)) {
    for (const t of trs) {
      if (t && typeof t === 'object' && !Array.isArray(t)) {
        push((t as Record<string, unknown>)['call_id']);
      }
    }
  }
  return out;
}

/**
 * Rebuild `structured.incidents[]` so every transcript line and unit
 * entry is server-authoritative. Gemini's `member_call_ids` (or
 * legacy `transcripts[].call_id`) is the only thing read from the LLM
 * output; the text, time, UID, and label are looked up in `inputMap`.
 *
 * Drops:
 *   - call_ids absent from `inputMap` (Gemini fabrications) with a
 *     warn line so we can track hallucination rate;
 *   - incidents whose member set is empty after the filter — there's
 *     nothing to display and the live page would render a blank row.
 *
 * Mutates `structured` in place. Returns the same reference so the
 * caller can chain validators.
 */
export function rebuildIncidents(
  structured: Record<string, unknown>,
  inputMap: Map<number, RdioInputRow>,
): Record<string, unknown> {
  const incidents = structured['incidents'];
  if (!Array.isArray(incidents)) return structured;
  const out: Array<Record<string, unknown>> = [];
  let droppedUnknown = 0;
  let droppedEmpty = 0;
  for (const inc of incidents) {
    if (!inc || typeof inc !== 'object' || Array.isArray(inc)) continue;
    const obj = inc as Record<string, unknown>;
    const ids = extractMemberCallIds(obj);
    const valid: number[] = [];
    for (const id of ids) {
      if (inputMap.has(id)) valid.push(id);
      else droppedUnknown += 1;
    }
    if (valid.length === 0) {
      droppedEmpty += 1;
      continue;
    }
    obj['member_call_ids'] = valid;
    // Sort by time using the unix dt embedded in the input row's order —
    // input map insertion order matches transcript chronological order
    // because formatRdioPrompt walks calls in dt order. Stable sort on
    // valid[] via the input-row time string keeps ties deterministic.
    valid.sort((a, b) => {
      const ta = inputMap.get(a)!.time;
      const tb = inputMap.get(b)!.time;
      return ta < tb ? -1 : ta > tb ? 1 : a - b;
    });
    // Cap displayed transcripts at TRANSCRIPTS_CAP (first 8 chronologically
    // + last 2) so a 100-row multi-agency job doesn't blow out the
    // hourly-summary card on live.html / logs.html. The full
    // member_call_ids array stays in the response so callers can fetch
    // additional transcripts if they want; only the rendered list is
    // trimmed. Both frontends already render transcripts_truncated /
    // transcripts_total — they'll show "Showing 10 of 123" automatically.
    const displayedIds =
      valid.length > TRANSCRIPTS_CAP
        ? [
            ...valid.slice(0, TRANSCRIPTS_HEAD_KEEP),
            ...valid.slice(-TRANSCRIPTS_TAIL_KEEP),
          ]
        : valid;
    obj['transcripts'] = displayedIds.map((id) => {
      const row = inputMap.get(id)!;
      return { call_id: row.call_id, time: row.time, text: row.text };
    });
    if (valid.length > TRANSCRIPTS_CAP) {
      obj['transcripts_truncated'] = true;
      obj['transcripts_total'] = valid.length;
    }
    // Build units[] as a deduped string array — the friendly label
    // from rdio_units.csv when known, "UID:<n>" otherwise. live.html /
    // logs.html both render units via
    //   typeof u === 'string' ? u : (u.id || u.label || '?')
    // so a string array drops straight in. An earlier draft of this
    // function emitted {uid,label} objects, which both frontends
    // rendered as "?".
    const unitSeen = new Set<number>();
    const unitList: string[] = [];
    for (const id of valid) {
      const row = inputMap.get(id)!;
      if (row.uid === null || unitSeen.has(row.uid)) continue;
      unitSeen.add(row.uid);
      unitList.push(row.unit_label ?? `UID:${row.uid}`);
    }
    obj['units'] = unitList;
    out.push(obj);
  }
  structured['incidents'] = out;
  structured['incident_count'] = out.length;
  if (droppedUnknown > 0 || droppedEmpty > 0) {
    log.warn(
      { droppedUnknown, droppedEmpty, kept: out.length },
      'rdio rebuilder: filtered structured incidents',
    );
  }
  return structured;
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

// Gemini API keys. GEMINI_API_KEY may be a single key or a comma-separated
// list ("keyA,keyB,keyC") to spread load and survive per-key rate limits.
// We rotate through them and, on a 429/RESOURCE_EXHAUSTED, roll over to the
// next key immediately (no backoff). `_geminiKeyIdx` persists across calls
// so we keep using the last good key instead of always hammering key #1.
let _geminiKeys: string[] | null = null;
let _geminiKeyIdx = 0;
function getGeminiKeys(): string[] {
  if (_geminiKeys) return _geminiKeys;
  _geminiKeys = (config.GEMINI_API_KEY ?? '')
    .split(',')
    .map((k) => k.trim())
    .filter((k) => k.length > 0);
  return _geminiKeys;
}

// Compress a Gemini error response body into one short line. The full
// JSON is hundreds of characters of help URLs + nested quota details that
// drown the log. We pluck the bits operators actually care about:
//   - HTTP status
//   - Gemini error status (e.g. RESOURCE_EXHAUSTED)
//   - Failing quota metric, if any (e.g. generate_content_free_tier_requests)
//   - "Retry in Xs" hint, if any
// Falls back to a short prefix of the raw body when parse fails.
function summarizeGeminiError(status: number, body: string): string {
  const head = `HTTP ${status}`;
  if (!body) return `${head} (empty body)`;
  try {
    let data: unknown = JSON.parse(body);
    if (Array.isArray(data) && data.length > 0) data = data[0];
    const err =
      data && typeof data === 'object'
        ? ((data as Record<string, unknown>)['error'] as
            | Record<string, unknown>
            | undefined)
        : undefined;
    if (!err) {
      const truncated = body.replace(/\s+/g, ' ').slice(0, 200);
      return `${head}: ${truncated}`;
    }
    const bits: string[] = [head];
    const gstatus = err['status'];
    if (typeof gstatus === 'string' && gstatus) bits.push(gstatus);

    const msg = typeof err['message'] === 'string' ? (err['message'] as string) : '';
    let quotaMetric = '';
    const details = err['details'];
    if (Array.isArray(details)) {
      for (const det of details) {
        if (!det || typeof det !== 'object') continue;
        const violations = (det as Record<string, unknown>)['violations'];
        if (!Array.isArray(violations)) continue;
        for (const v of violations) {
          if (!v || typeof v !== 'object') continue;
          const qm = (v as Record<string, unknown>)['quotaMetric'];
          if (typeof qm === 'string' && qm) {
            quotaMetric = qm.split('/').pop() ?? qm;
            break;
          }
        }
        if (quotaMetric) break;
      }
    }
    if (quotaMetric) bits.push(quotaMetric);

    const retryMatch = msg.match(/retry in\s*([\d.]+\s*s)/i);
    if (retryMatch) bits.push(`retry in ${retryMatch[1]!.replace(/\s+/g, '')}`);

    return bits.join(' / ');
  } catch {
    const truncated = body.replace(/\s+/g, ' ').slice(0, 200);
    return `${head}: ${truncated}`;
  }
}

export async function callLlm(opts: {
  systemPrompt: string;
  userPrompt: string;
  model: string;
  jsonMode?: boolean;
  maxTokens?: number;
  maxAttempts?: number;
}): Promise<string> {
  const keys = getGeminiKeys();
  if (keys.length === 0) throw new Error('GEMINI_API_KEY not set');
  const maxAttempts = opts.maxAttempts ?? 4;
  const payload: Record<string, unknown> = {
    model: opts.model,
    temperature: 0.2,
    // Gemini 2.5 Flash hard caps output at 65,536 tokens. Pre-bump
    // (60k) was leaving headroom that meant busy hours with dense
    // per-call transcripts[] arrays were truncating mid-string.
    max_tokens: opts.maxTokens ?? 65_000,
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
  // Each "round" is one backoff cycle. Within a round we try every API key
  // in rotation; a 429 on one key rolls over to the next IMMEDIATELY (no
  // wait), so a single quota-exhausted key never stalls the call. Only when
  // every key is rate-limited — or we hit a 5xx/network error — do we back
  // off and start the next round.
  for (let round = 0; round < maxAttempts; round++) {
    let keysRateLimited = 0;
    let retryAfterMs = 0;
    let sawTransient = false;

    while (keysRateLimited < keys.length) {
      const apiKey = keys[_geminiKeyIdx % keys.length]!;
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
          return (choice.message.content ?? '').trim();
        }
        lastBody = (await res.text()).slice(0, 1000);
        if (res.status === 429) {
          // This key is rate-limited — roll over to the next key and retry
          // immediately within the same round (no backoff).
          retryAfterMs = Number(res.headers.get('Retry-After') ?? '0') * 1000;
          keysRateLimited += 1;
          const prevIdx = _geminiKeyIdx % keys.length;
          _geminiKeyIdx = (_geminiKeyIdx + 1) % keys.length;
          if (keys.length > 1 && keysRateLimited < keys.length) {
            log.warn(
              { keyIndex: prevIdx, nextKeyIndex: _geminiKeyIdx, totalKeys: keys.length },
              'Gemini key rate-limited, rolling to next key',
            );
            continue; // try the next key right away
          }
          break; // every key is rate-limited this round → back off below
        }
        if (TRANSIENT_STATUSES.has(res.status)) {
          sawTransient = true;
          break; // 5xx → back off below
        }
        // Non-transient HTTP error — bubble.
        throw new Error(`Gemini ${summarizeGeminiError(res.status, lastBody)}`);
      } catch (err) {
        const e = err as Error;
        // AbortError or network issues — treat as transient, back off.
        if (e.name === 'AbortError' || (e as { code?: string }).code) {
          lastErr = e;
          sawTransient = true;
          break;
        }
        throw e; // programming / non-transient error
      } finally {
        clearTimeout(t);
      }
    }

    // Got here because all keys were rate-limited this round, or a 5xx /
    // network error broke the inner loop. Back off before the next round.
    if (round < maxAttempts - 1) {
      const wait =
        retryAfterMs > 0
          ? Math.min(retryAfterMs, 60_000)
          : Math.min(60_000, (sawTransient ? 10_000 : 5_000) * 2 ** round);
      log.warn(
        {
          status: lastStatus,
          round: round + 1,
          keysRateLimited,
          totalKeys: keys.length,
          waitMs: wait,
        },
        keysRateLimited >= keys.length && keys.length > 0
          ? 'all Gemini keys rate-limited, backing off'
          : 'Gemini transient error, backing off',
      );
      await new Promise((r) => setTimeout(r, wait));
      continue;
    }
  }
  const summary =
    lastStatus !== null
      ? summarizeGeminiError(lastStatus, lastBody)
      : lastErr?.message || 'unreachable';
  throw new Error(
    `Gemini failed after ${maxAttempts} rounds across ${keys.length} key(s): ${summary}`,
  );
}

// ---------------------------------------------------------------------------
// Lenient JSON parser — fenced code, brace-extraction, typo scrub.
// ---------------------------------------------------------------------------

function scrubLlmTypos(text: string): string {
  // Collapse 2+ opening quotes before a field-name-like token.
  return text.replace(/"{2,}([A-Za-z_]\w*)"\s*:/g, '"$1":');
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

// Salvage a partial object from truncated JSON. Gemini 2.5 Flash sometimes
// stops mid-string even though finish_reason=stop, leaving us with an
// unclosed JSON document. We try to close any open string/object/array
// brackets so the prefix we did receive parses. Returns null on failure.
function repairTruncatedJson(input: string): Record<string, unknown> | null {
  if (!input) return null;
  // Walk the text tracking string state and bracket stack so we know what
  // needs closing.
  const stack: string[] = [];
  let inStr = false;
  let escape = false;
  let lastValueEnd = -1; // index after last comma/colon/}-]/quote close
  for (let i = 0; i < input.length; i++) {
    const c = input[i];
    if (escape) {
      escape = false;
      continue;
    }
    if (inStr) {
      if (c === '\\') {
        escape = true;
        continue;
      }
      if (c === '"') {
        inStr = false;
        lastValueEnd = i + 1;
      }
      continue;
    }
    if (c === '"') {
      inStr = true;
      continue;
    }
    if (c === '{' || c === '[') {
      stack.push(c);
      continue;
    }
    if (c === '}' || c === ']') {
      stack.pop();
      lastValueEnd = i + 1;
      continue;
    }
    if (c === ',' || c === ':') {
      lastValueEnd = i;
    }
  }
  if (stack.length === 0 && !inStr) return null; // not actually truncated

  // Trim back to the last complete value boundary so we don't leave a
  // dangling key like `"foo":` with no value, or `"foo": "ba` mid-string.
  let trimmed = lastValueEnd > 0 ? input.slice(0, lastValueEnd) : '';
  if (!trimmed) return null;
  // Drop any trailing comma — `"a": 1, ` would parse, `"a": 1,` won't.
  trimmed = trimmed.replace(/,\s*$/, '');
  // If we cut right after a key's colon (`"foo":`), drop that key too.
  trimmed = trimmed.replace(/,?\s*"[^"\\]*"\s*:\s*$/, '');

  // Re-walk the trimmed prefix to recompute the bracket stack.
  const closeStack: string[] = [];
  let s = false;
  let esc = false;
  for (let i = 0; i < trimmed.length; i++) {
    const c = trimmed[i];
    if (esc) {
      esc = false;
      continue;
    }
    if (s) {
      if (c === '\\') {
        esc = true;
        continue;
      }
      if (c === '"') s = false;
      continue;
    }
    if (c === '"') {
      s = true;
      continue;
    }
    if (c === '{' || c === '[') closeStack.push(c);
    else if (c === '}' || c === ']') closeStack.pop();
  }
  // Close any still-open brackets.
  let closing = '';
  while (closeStack.length > 0) {
    const open = closeStack.pop();
    closing += open === '{' ? '}' : ']';
  }

  try {
    const parsed: unknown = JSON.parse(trimmed + closing);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
  } catch {
    // give up
  }
  return null;
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
        // Coerce overview to string. Gemini occasionally returns
        // non-string values (object, array) under "overview"; without
        // the type guard those leaked into the embed as raw JSON.
        const ov = obj['overview'];
        let overview = typeof ov === 'string' ? ov : '';
        if (!overview && obj['quiet_hour']) {
          overview = 'Quiet hour — no significant incidents detected.';
        }
        if (!overview && Array.isArray(obj['incidents']) && obj['incidents'].length > 0) {
          // Gemini parsed cleanly but skipped/blanked the `overview`
          // field. Falling back to "(no summary)" wastes the rest of
          // the structured response — synthesise a one-liner from the
          // counts + the highest-severity incident title so the embed
          // stays useful. The full structured object is still saved.
          const incidents = obj['incidents'] as Array<Record<string, unknown>>;
          const order: Record<string, number> = {
            emergency: 0,
            high: 1,
            medium: 2,
            low: 3,
          };
          const ranked = incidents
            .filter(isPlainObject)
            .slice()
            .sort((a, b) => {
              const sa = String(a['severity'] ?? 'low').toLowerCase();
              const sb = String(b['severity'] ?? 'low').toLowerCase();
              return (order[sa] ?? 99) - (order[sb] ?? 99);
            });
          const top = ranked[0];
          const title =
            top && typeof top['title'] === 'string'
              ? (top['title'] as string)
              : null;
          const lvl =
            typeof obj['activity_level'] === 'string'
              ? (obj['activity_level'] as string)
              : null;
          overview = title
            ? `${incidents.length} incident${incidents.length === 1 ? '' : 's'}${lvl ? ` (${lvl})` : ''} — top: ${title}.`
            : `${incidents.length} incident${incidents.length === 1 ? '' : 's'}${lvl ? ` (${lvl})` : ''} reported.`;
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

  // Salvage path: Gemini sometimes truncates mid-output even with
  // finish_reason=stop. Trim back to the last valid value boundary,
  // close any open brackets, and re-parse. Recovers `overview` plus
  // however many `incidents` finished writing.
  const repairFrom = first >= 0 ? cleaned.slice(first) : cleaned;
  const repaired = repairTruncatedJson(repairFrom);
  if (repaired) {
    log.info(
      {
        keys: Object.keys(repaired),
        incidents: Array.isArray(repaired['incidents'])
          ? (repaired['incidents'] as unknown[]).length
          : null,
      },
      'summary parse: recovered truncated JSON',
    );
    const ov = repaired['overview'];
    let overview = typeof ov === 'string' ? ov : '';
    if (
      !overview &&
      Array.isArray(repaired['incidents']) &&
      (repaired['incidents'] as unknown[]).length > 0
    ) {
      const incidents = repaired['incidents'] as Array<Record<string, unknown>>;
      const top = incidents.find(isPlainObject);
      const title =
        top && typeof top['title'] === 'string' ? (top['title'] as string) : null;
      overview = title
        ? `${incidents.length} incident${incidents.length === 1 ? '' : 's'} — top: ${title}.`
        : `${incidents.length} incident${incidents.length === 1 ? '' : 's'} reported.`;
    }
    return { overview, structured: repaired };
  }

  // Last-resort: regex-extract just the `overview` string. When the
  // JSON is truncated deep inside the per-incident transcripts[] array
  // (a common pattern on busy hours that bust the 65k token cap), the
  // structured repair can't reconstruct anything useful — but the
  // overview field appears very early in Gemini's output, so it's
  // intact. Pull it out as a plain string so the discord embed and
  // dashboard panel still render the high-level "what happened this
  // hour" sentence rather than going blank.
  const overviewMatch = /"overview"\s*:\s*"((?:\\.|[^"\\])*)"/.exec(cleaned);
  if (overviewMatch && overviewMatch[1]) {
    let overview = overviewMatch[1];
    // Unescape JSON string escapes — \" \\ \n etc.
    try {
      overview = JSON.parse(`"${overview}"`);
    } catch {
      // leave as-is if unescape fails
    }
    if (overview.trim().length > 0) {
      log.info(
        { len: overview.length },
        'summary parse: recovered overview via regex (structured discarded)',
      );
      return { overview, structured: null };
    }
  }

  // Parse fully failed. Returning the raw LLM body as overview puts
  // raw JSON into the rdio_summaries.summary column, which the discord
  // embed then renders verbatim. Return an empty overview instead so
  // the embed falls through to the incidents-from-details path or to
  // its "(no summary)" placeholder. The raw text is logged here for
  // debugging — operators can grep for `summary parse failed` to see
  // the head/tail of the offending response.
  log.warn(
    {
      len: text.length,
      head: text.slice(0, 120).replace(/\n/g, ' '),
      tail: text.slice(-120).replace(/\n/g, ' '),
    },
    'summary parse failed',
  );
  return { overview: '', structured: null };
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

/**
 * Round `now` down to the top of its containing hour in `tz`, returned as
 * the UTC instant that wall-clock corresponds to. Mirrors python's
 * `datetime.now(local_tz).replace(minute=0, second=0, microsecond=0)`
 * followed by `.astimezone(timezone.utc)`.
 *
 * Doing this in UTC (`ms - ms % 3_600_000`) is equivalent for whole-hour
 * zones like Australia/Sydney, but drifts ~30 min for half-hour zones
 * (Australia/Adelaide UTC+9:30/+10:30). The Intl-based round is robust
 * across both kinds.
 */
function localHourStartUtc(now: Date, tz: string): Date {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: tz,
    hourCycle: 'h23',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).formatToParts(now);
  const get = (t: string): number =>
    Number(parts.find((p) => p.type === t)?.value ?? '0');
  const y = get('year');
  const mo = get('month');
  const d = get('day');
  const h = get('hour');
  const mn = get('minute');
  const s = get('second');
  // Re-read the tz wall-clock as if it were UTC. The difference between
  // that and the actual instant gives the tz offset for `now`. We then
  // apply that same offset to the top-of-hour wall-clock to get the
  // matching UTC instant. (DST transitions inside the rounding window
  // would still be off — fine for a once-per-hour scheduler that fires
  // ~5 minutes before the boundary.)
  const nowAsIfUtcMs = Date.UTC(y, mo - 1, d, h, mn, s);
  const topAsIfUtcMs = Date.UTC(y, mo - 1, d, h, 0, 0);
  const offsetMs = nowAsIfUtcMs - now.getTime();
  return new Date(topAsIfUtcMs - offsetMs);
}

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

// 0..23 — what local-clock hour does this UTC instant fall in?
function localHourOfUtc(d: Date, tz: string): number {
  const fmt = new Intl.DateTimeFormat('en-GB', {
    timeZone: tz,
    hour: '2-digit',
    hour12: false,
  });
  const h = Number(fmt.format(d).split(':')[0] ?? '0');
  return h === 24 ? 0 : h;
}

// Local-clock quiet-hour merge schedule. Each entry collapses [startHour,
// endHour) local time into a SINGLE Gemini call instead of one per hour.
// Hours outside every bucket fire normally (one call per hour). Daily
// Gemini calls = (24 − sum_of_spans) + count_of_buckets. With the current
// setup (3 buckets covering 8 hours) we get (24-8)+3 = 19 fires/day —
// under the 20/day free-tier limit, leaving 1 call of headroom for the
// startup catchup. Buckets must not cross midnight (00:00 boundary).
const QUIET_HOUR_BUCKETS: ReadonlyArray<{
  readonly startHour: number;
  readonly endHour: number;
}> = [
  { startHour: 22, endHour: 24 }, // 10 PM – 12 AM local
  { startHour: 0, endHour: 3 },   // 12 AM – 3 AM local
  { startHour: 3, endHour: 6 },   // 3 AM – 6 AM local
];

function bucketForLocalHour(
  localStartHour: number,
): { startHour: number; endHour: number } | null {
  for (const b of QUIET_HOUR_BUCKETS) {
    if (localStartHour >= b.startHour && localStartHour < b.endHour) {
      return { startHour: b.startHour, endHour: b.endHour };
    }
  }
  return null;
}

/**
 * Decide what window to summarise at a given fire time. The scheduler
 * still ticks once an hour (at HH:55 local); this resolves into either:
 *   - a normal 1h window for hours outside any quiet bucket,
 *   - a multi-hour window covering the full bucket when the just-ended
 *     local hour is the LAST hour of a quiet bucket,
 *   - null when we're mid-bucket and should wait for the bucket to close.
 *
 * `fireTime` is the UTC instant the scheduler ticked (≈ HH:55 local).
 * The just-ended local hour is the one that contains `fireTime` — i.e.
 * the hour whose start is `localHourStartUtc(fireTime, tz)` and whose
 * end is +1h after that.
 */
export function resolveSummaryWindow(
  fireTime: Date,
  tz: string,
): { startUtc: Date; endUtc: Date } | null {
  const startedHourUtc = localHourStartUtc(fireTime, tz);
  const endedHourUtc = new Date(startedHourUtc.getTime() + 60 * 60_000);
  const localStartHour = localHourOfUtc(startedHourUtc, tz);

  const bucket = bucketForLocalHour(localStartHour);
  if (!bucket) {
    return { startUtc: startedHourUtc, endUtc: endedHourUtc };
  }
  // The bucket's last hour is the one whose start is `bucket.endHour - 1`.
  if (localStartHour + 1 !== bucket.endHour) {
    return null;
  }
  const spanHours = bucket.endHour - bucket.startHour;
  const bucketStartUtc = new Date(
    endedHourUtc.getTime() - spanHours * 60 * 60_000,
  );
  return { startUtc: bucketStartUtc, endUtc: endedHourUtc };
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
  /** UTC instant marking the start of the summary window. */
  hourStartUtc: Date;
  /**
   * Optional UTC instant marking the END of the window (exclusive).
   * Defaults to `hourStartUtc + 1h` — used as-is by the legacy hourly
   * path. The scheduler passes an explicit end when a quiet-hour bucket
   * merges 2–3 hours into a single Gemini call.
   */
  hourEndUtc?: Date;
  force?: boolean;
  releaseAt?: Date | null;
}

export async function generateRdioHourlySummary(
  p: GenerateHourlyParams,
): Promise<{ hour_slot: number; call_count: number } | null> {
  const model = config.LLM_MODEL;
  const start = p.hourStartUtc;
  const end = p.hourEndUtc ?? new Date(start.getTime() + 60 * 60_000);
  const calls = dedupeCalls(await fetchCallsBetween(start, end));
  if (calls.length === 0 && !p.force && !p.releaseAt) {
    log.info({ start: start.toISOString() }, 'hourly: no transcripts, skipping');
    return null;
  }
  const periodLabel = formatPeriodLabel(start, end);
  const { prompt, totalChars, inputMap } = await formatRdioPrompt(
    calls,
    periodLabel,
  );
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
    if (!summaryText && calls.length > 0) {
      // Gemini gave us a parseable response but no overview — or parse
      // failed entirely. Log the head/tail of the raw output and the
      // structured keys so we can tell which case it is without bumping
      // log level.
      log.warn(
        {
          raw_len: raw.length,
          raw_head: raw.slice(0, 200).replace(/\n/g, ' '),
          raw_tail: raw.slice(-200).replace(/\n/g, ' '),
          structured_keys: structured ? Object.keys(structured) : null,
          incidents: structured && Array.isArray(structured['incidents'])
            ? (structured['incidents'] as unknown[]).length
            : null,
          call_count: calls.length,
        },
        'rdio: hourly produced empty overview',
      );
    }
    if (structured) {
      try {
        structured = rebuildIncidents(structured, inputMap);
        structured = polishStructuredIncidents(
          structured,
        ) as Record<string, unknown> | null;
      } catch (err) {
        log.warn(
          { err: (err as Error).message },
          'rdio polish threw; persisting un-validated structured',
        );
      }
    }

    // Don't persist empty summaries. If we got no overview AND no
    // structured incidents, the row would render as "(empty)" in the
    // discord embed and on dashboard.html — strictly worse than just
    // letting the previous hour's good summary remain "latest" until
    // the next run lands. Better an absent hour than a misleading one.
    const hasIncidents =
      structured &&
      Array.isArray(structured['incidents']) &&
      (structured['incidents'] as unknown[]).length > 0;
    if (!summaryText && !hasIncidents && calls.length > 0 && !p.force) {
      log.warn(
        { call_count: calls.length, period: periodLabel },
        'rdio: hourly skipping persist — empty overview AND no structured incidents',
      );
      return null;
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
  const calls = dedupeCalls(await fetchLastNCalls(n));
  if (calls.length === 0 && !p.force) {
    log.info({ n }, 'recent: no transcripts, skipping');
    return null;
  }
  const startUtc = calls[0]?.date_time ?? new Date();
  const endUtc = calls[calls.length - 1]?.date_time ?? new Date();
  const periodLabel = `Last ${calls.length} transcripts (${formatPeriodLabel(startUtc, endUtc)})`;
  const { prompt, totalChars, inputMap } = await formatRdioPrompt(
    calls,
    periodLabel,
  );
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
      structured = rebuildIncidents(structured, inputMap);
      structured = polishStructuredIncidents(
        structured,
      ) as Record<string, unknown> | null;
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'rdio polish threw; persisting un-validated structured',
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
// Startup catch-up — fill in any missing hourly summary rows that were
// dropped while this process was down. Mirrors python's
// _rdio_summary_catchup (external_api_proxy.py:15466-15512).
// ---------------------------------------------------------------------------

async function rdioSummaryRowExists(periodStart: Date): Promise<boolean> {
  const pool = await getPool();
  if (!pool) return false;
  const res = await pool.query(
    `SELECT 1 FROM rdio_summaries
      WHERE summary_type = 'hourly' AND period_start = $1
      LIMIT 1`,
    [periodStart],
  );
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * On boot:
 *  - Always try the previous local-clock hour. force=true so an empty
 *    hour writes a "no traffic" stub instead of silently skipping —
 *    /api/summaries/latest must never show a gap for a completed hour.
 *  - If `now` is past HH:55 in SUMMARY_TZ, also kick off the current
 *    hour with the same release_at gating the scheduler would have used,
 *    so a restart inside the prefetch window doesn't lose the prefetch.
 *
 * Both calls swallow errors so a transient LLM/DB failure can't block
 * the scheduler from arming. The unique constraint on
 * (summary_type, period_start) keeps this idempotent against a race
 * with a normal scheduled fire.
 */
export async function runRdioSummaryCatchup(): Promise<void> {
  const now = new Date();
  const tz = config.SUMMARY_TZ;
  const currentHourStart = localHourStartUtc(now, tz);
  // Previous closed window: starts at currentHourStart - 1h normally, but
  // if that previous local hour is inside a quiet bucket we widen the
  // window to the whole bucket. If it's mid-bucket (not the bucket's
  // last hour), we skip — the next scheduled fire at bucket close will
  // pick it up.
  const prevHourStart = new Date(currentHourStart.getTime() - 60 * 60_000);
  const prevLocalHour = localHourOfUtc(prevHourStart, tz);
  const prevBucket = bucketForLocalHour(prevLocalHour);
  let prevWindowStart: Date | null = prevHourStart;
  if (prevBucket) {
    if (prevLocalHour + 1 === prevBucket.endHour) {
      const spanHours = prevBucket.endHour - prevBucket.startHour;
      prevWindowStart = new Date(
        currentHourStart.getTime() - spanHours * 60 * 60_000,
      );
    } else {
      prevWindowStart = null;
    }
  }
  if (prevWindowStart) {
    try {
      if (!(await rdioSummaryRowExists(prevWindowStart))) {
        log.info(
          {
            hour_start: prevWindowStart.toISOString(),
            hour_end: currentHourStart.toISOString(),
          },
          'rdio catchup: filling missing previous window',
        );
        await generateRdioHourlySummary({
          hourStartUtc: prevWindowStart,
          hourEndUtc: currentHourStart,
          force: true,
        });
      }
    } catch (err) {
      log.warn(
        { err: (err as Error).message },
        'rdio catchup prev-window error',
      );
    }
  }
  if (localMinute(now) >= 55) {
    // Re-use the scheduler's window resolver so the prefetch matches what
    // the regular HH:55 fire would do — including skipping mid-bucket.
    const window = resolveSummaryWindow(now, tz);
    if (window) {
      try {
        if (!(await rdioSummaryRowExists(window.startUtc))) {
          log.info(
            {
              hour_start: window.startUtc.toISOString(),
              hour_end: window.endUtc.toISOString(),
              release_at: window.endUtc.toISOString(),
            },
            'rdio catchup: prefetching current window',
          );
          await generateRdioHourlySummary({
            hourStartUtc: window.startUtc,
            hourEndUtc: window.endUtc,
            force: true,
            releaseAt: window.endUtc,
          });
        }
      } catch (err) {
        log.warn(
          { err: (err as Error).message },
          'rdio catchup current-window error',
        );
      }
    }
  }
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
  // Next HH:55:00 in SUMMARY_TZ. Compute the local-tz minute, derive
  // how many full minutes until :55, then SUBTRACT the elapsed
  // seconds + millis of the current minute so the timer lands on
  // :55:00 exactly. Previous version added `(60 - seconds) * 1000`
  // which fired 0-60s past the intended boundary on every cycle —
  // e.g. now=12:30:15 produced 12:56:00 instead of 12:55:00.
  const now = Date.now();
  const nowDate = new Date(now);
  const minute = localMinute(nowDate);
  let waitMin = 55 - minute;
  if (waitMin <= 0) waitMin += 60;
  return (
    now +
    waitMin * 60_000 -
    nowDate.getSeconds() * 1000 -
    nowDate.getMilliseconds()
  );
}

async function runHourlyJob(): Promise<void> {
  // The scheduler ticks at HH:55 in SUMMARY_TZ. Most ticks fire a normal
  // 1h summary, but a tick that falls inside a quiet-hour bucket either
  // skips (mid-bucket — wait for the bucket to close) or fires a merged
  // multi-hour summary (last hour of the bucket). See QUIET_HOUR_BUCKETS.
  // Half-hour timezones (Adelaide) are handled by computing the top of
  // the local hour first and converting to UTC.
  const now = new Date();
  const ms = now.getTime();
  const window = resolveSummaryWindow(now, config.SUMMARY_TZ);
  if (!window) {
    const hourStartUtc = localHourStartUtc(now, config.SUMMARY_TZ);
    log.info(
      { hour_start: hourStartUtc.toISOString() },
      'rdio hourly: skipping (inside quiet-hour bucket; waits for bucket end)',
    );
    return;
  }
  const spanHours = Math.round(
    (window.endUtc.getTime() - window.startUtc.getTime()) / 3_600_000,
  );
  schedulerStats.last_fire_at = Math.floor(ms / 1000);
  schedulerStats.total_fires += 1;
  schedulerStats.last_error = null;
  log.info(
    {
      hour_start: window.startUtc.toISOString(),
      hour_end: window.endUtc.toISOString(),
      span_hours: spanHours,
    },
    'rdio hourly: firing',
  );
  const t0 = Date.now();
  try {
    const result = await generateRdioHourlySummary({
      hourStartUtc: window.startUtc,
      hourEndUtc: window.endUtc,
      force: true,
      releaseAt: window.endUtc,
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
  // Boot-time catchup is intentionally disabled. Every restart was
  // calling Gemini to backfill the previous hour, which (a) burned
  // boot CPU + Gemini quota every time pm2 cycled, and (b) ran the
  // pre-LLM dedup pass at boot regardless of whether the row
  // already existed. The scheduler's HH:55 fire is now the only
  // path that talks to Gemini — missed hours stay missed. Set
  // NODE_RDIO_CATCHUP=true to re-enable.
  if (process.env['NODE_RDIO_CATCHUP'] === 'true') {
    void runRdioSummaryCatchup();
  }
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
  localHourStartUtc,
  nextFireTimeMs,
  setPoolForTests: (_: Pool): void => {
    // Tests should mock getRdioPool / getPool from the modules; this
    // helper is a placeholder to discourage direct pool injection.
    void _;
  },
};
