/**
 * rdio → ntfy "major incident" push notifier.
 *
 * A genuinely large incident sustains radio traffic: many calls on one
 * talkgroup over a few minutes. This loop polls the SELF-HOSTED
 * rdio-scanner Postgres for those bursts and publishes ONE push per
 * incident to a single ntfy topic that users subscribe to. Detection is
 * heuristic only (no LLM): a burst threshold over a rolling window, with
 * an optional keyword gate and keyword-driven priority escalation.
 *
 * Why ntfy needs no per-user logic: ntfy's model is publish-to-topic, so
 * we decide "this is big" once and POST it; every subscriber to
 * NTFY_TOPIC receives it. Subscriptions live entirely on the ntfy side.
 *
 * Dedup: radio chatter about one job spans many calls over many minutes,
 * so a per-(system,talkgroup) cooldown row (migration 025, in the MAIN
 * archive DB) suppresses re-fires until RDIO_ALERT_COOLDOWN_MIN passes.
 *
 * Bootstrap: on the first tick after start, every currently-bursting
 * talkgroup is written to the cooldown table WITHOUT publishing, so a
 * restart in the middle of a busy period doesn't re-alert active jobs.
 *
 * The detection + formatting building blocks (detectBursts,
 * analyzeKeywords, buildNotification, publishToNtfy) are exported so the
 * `scripts/test-ntfy-detection.ts` preview tool exercises the EXACT same
 * code path the live loop does — the preview can't drift from reality.
 */
import { fetch } from 'undici';
import type { Pool } from 'pg';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { getPool } from '../db/pool.js';
import { getRdioPool, isRdioConfigured, resolveLabels } from './rdio.js';

// NO hardcoded keyword lists. Both the incident (trigger) list and the
// urgent (priority-only) list come exclusively from the .env —
// RDIO_ALERT_KEYWORDS and RDIO_ALERT_URGENT_KEYWORDS. If a list is unset,
// it's empty: nothing the operator didn't configure can ever match. With
// RDIO_ALERT_REQUIRE_KEYWORD=true and no incident keywords set, the loop
// never fires (it warns about this at startup).

const MAX_ALERTS_PER_TICK = 5;
// ntfy's default max message size is 4096 bytes; keep the body under that
// with headroom for the keyword/summary header.
const MAX_BODY_CHARS = 3800;

export interface CallLine {
  id: number;
  transcript: string;
  dt: string;
}

export interface BurstCandidate {
  system: number;
  talkgroup: number;
  n: number;
  latestId: number;
  calls: CallLine[];
  /** Lowercased, joined transcript text for keyword scanning. */
  allText: string;
}

export interface KeywordHit {
  /** Incident-list keywords that matched — the trigger + display set. */
  matched: string[];
  /** True if any URGENT-list keyword matched (priority escalation only). */
  urgent: boolean;
}

function parseKeywordEnv(raw: string | undefined): string[] | null {
  if (raw === undefined) return null; // unset → caller uses its default
  return raw
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter((s) => s.length > 0);
}

export interface Notification {
  title: string;
  body: string;
  priority: 'urgent' | 'high' | 'default';
  tags: string[];
  click: string | null;
  matched: string[];
  urgent: boolean;
}

let timer: NodeJS.Timeout | null = null;
let running = false;
let bootstrapped = false;

/** Incident keywords — the SOLE trigger gate + display set. Comes only
 *  from RDIO_ALERT_KEYWORDS; empty when unset (no built-in defaults). */
export function incidentKeywords(): string[] {
  return parseKeywordEnv(config.RDIO_ALERT_KEYWORDS) ?? [];
}

/** URGENT keywords — priority escalation only, never a trigger and never
 *  displayed. Comes only from RDIO_ALERT_URGENT_KEYWORDS; empty when
 *  unset (so everything stays 'high' until you configure it). */
export function urgentKeywords(): string[] {
  return parseKeywordEnv(config.RDIO_ALERT_URGENT_KEYWORDS) ?? [];
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Cache compiled matchers — the keyword lists are tiny and stable per
// process, so we don't rebuild a RegExp on every burst.
const _kwRe = new Map<string, RegExp>();

/**
 * Whole-token keyword match. The keyword must NOT be flanked by an
 * alphanumeric character, so short codes match as standalone words but not
 * as a fragment of a longer one:
 *   keywordMatches('gsw to the leg', 'gsw')   → true
 *   keywordMatches('unit at cogswell st', 'gsw') → false  (…co[gsw]ell…)
 *   keywordMatches('kingswood', 'gsw')        → false  (…kin[gsw]ood…)
 * Adjacency to spaces/punctuation/slashes still matches ('gsw/stab' → true),
 * and multi-word phrases work ('structure fire'). Both `text` and `keyword`
 * are expected lowercased (parseKeywordEnv lowercases the keywords;
 * analyzeKeywords lowercases the text).
 */
export function keywordMatches(text: string, keyword: string): boolean {
  if (!keyword) return false;
  let re = _kwRe.get(keyword);
  if (!re) {
    // (?<![a-z0-9]) / (?![a-z0-9]) = not glued to an alphanumeric run, i.e.
    // a "word-ish" boundary that also treats digits as part of a token so
    // "gsw" never matches inside "gsw2"/"2gsw".
    re = new RegExp(`(?<![a-z0-9])${escapeRegExp(keyword)}(?![a-z0-9])`);
    _kwRe.set(keyword, re);
  }
  return re.test(text);
}

/** Match a burst's transcripts. `matched` is the incident list only — it
 *  is the trigger gate AND the displayed set, so nothing the operator
 *  didn't list (e.g. the urgent terms) can ever fire or show. `urgent`
 *  comes from the separate urgent list and only affects priority.
 *
 *  Matching is whole-token (see keywordMatches) so a short code like "GSW"
 *  fires on a spoken "GSW" but not on "Cogswell"/"Kingswood". */
export function analyzeKeywords(allText: string): KeywordHit {
  const text = allText.toLowerCase();
  const matched: string[] = [];
  const seen = new Set<string>();
  for (const k of incidentKeywords()) {
    if (keywordMatches(text, k) && !seen.has(k)) {
      seen.add(k);
      matched.push(k);
    }
  }
  const urgent = urgentKeywords().some((k) => keywordMatches(text, k));
  return { matched, urgent };
}

/** ntfy requires ASCII header values; transcripts/labels can carry the
 *  odd non-ASCII char. Strip to a safe single-line ASCII string. */
function asciiHeader(s: string): string {
  return s
    .replace(/[\r\n]+/g, ' ')
    // eslint-disable-next-line no-control-regex
    .replace(/[^\x20-\x7E]/g, '')
    .trim()
    .slice(0, 250);
}

function callUrl(id: number): string | null {
  return config.RDIO_CALL_URL_BASE ? `${config.RDIO_CALL_URL_BASE}${id}` : null;
}

/** Format a naive-UTC rdio timestamp as HH:MM:SS in SUMMARY_TZ. */
function fmtTime(raw: string | null | undefined): string {
  if (!raw) return '';
  const s = String(raw);
  const hasTz = /[zZ]$|[+-]\d\d:?\d\d$/.test(s);
  const d = new Date(hasTz ? s : `${s}Z`);
  if (Number.isNaN(d.getTime())) return '';
  try {
    return new Intl.DateTimeFormat('en-AU', {
      timeZone: config.SUMMARY_TZ,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(d);
  } catch {
    return d.toISOString().slice(11, 19);
  }
}

/** Pull every talkgroup bursting right now. One query; the window is in
 *  naive-UTC to match rdio-scanner's `dateTime` (timestamp WITHOUT tz,
 *  UTC). windowMin/threshold default to config but are overridable so the
 *  preview tool can surface candidates in a quiet period. */
export async function detectBursts(
  rdio: Pool,
  opts?: { windowMin?: number; threshold?: number; talkgroup?: number | null },
): Promise<BurstCandidate[]> {
  const windowMin = opts?.windowMin ?? config.RDIO_BURST_WINDOW_MIN;
  const threshold = opts?.threshold ?? config.RDIO_BURST_THRESHOLD;
  const tgFilter = opts?.talkgroup ?? null;
  const params: unknown[] = [String(windowMin), threshold];
  let tgClause = '';
  if (tgFilter !== null) {
    params.push(tgFilter);
    tgClause = `AND "talkgroup" = $${params.length}`;
  }
  const sql = `
    SELECT
      "system"    AS system,
      "talkgroup" AS talkgroup,
      COUNT(*)::int  AS n,
      MAX("id")::int AS latest_id,
      json_agg(
        json_build_object('id', "id", 'transcript', "transcript", 'dt', "dateTime")
        ORDER BY "dateTime"
      ) AS calls,
      lower(string_agg("transcript", ' | ')) AS all_text
    FROM "rdioScannerCalls"
    WHERE "dateTime" > (now() AT TIME ZONE 'UTC') - ($1 || ' minutes')::interval
      AND "transcript" IS NOT NULL
      AND "transcript" <> ''
      AND "talkgroup" IS NOT NULL
      ${tgClause}
    GROUP BY "system", "talkgroup"
    HAVING COUNT(*) >= $2
    ORDER BY COUNT(*) DESC
  `;
  const res = await rdio.query<{
    system: number;
    talkgroup: number;
    n: number;
    latest_id: number;
    calls: CallLine[] | null;
    all_text: string | null;
  }>(sql, params);
  return res.rows.map((r) => ({
    system: r.system,
    talkgroup: r.talkgroup,
    n: r.n,
    latestId: r.latest_id,
    calls: (r.calls ?? []).filter((c) => c && c.transcript),
    allText: r.all_text ?? '',
  }));
}

/** Build the full notification (title, body, priority, tags, click) for a
 *  burst — the exact push the live loop sends. Resolves system/talkgroup
 *  labels. */
export async function buildNotification(c: BurstCandidate): Promise<Notification> {
  const { systemLabel, talkgroupLabel } = await resolveLabels(c.system, c.talkgroup);
  const tgName = talkgroupLabel ?? `TG ${c.talkgroup}`;
  const sysName = systemLabel ?? `System ${c.system}`;
  const { matched, urgent } = analyzeKeywords(c.allText);

  const priority: Notification['priority'] = urgent ? 'urgent' : 'high';
  // Emoji is carried by the Tags header (ASCII shortcodes), NEVER the
  // body. A raw unicode emoji in the message body suppresses ntfy's
  // web-app markdown rendering (ntfy #1410), which is why the keyword
  // body — which used to lead with 🔑 — showed raw **markdown** while the
  // emoji-free no-keyword body rendered fine.
  const tags = urgent ? ['rotating_light', 'warning'] : ['radio'];
  const title = `Major radio activity — ${tgName} (${sysName})`;
  const click = callUrl(c.latestId);

  // Body is markdown (ntfy renders it when the `Markdown: yes` header is
  // set). Bare URLs aren't auto-linked, so every call link is emitted as
  // a [label](url) markdown link. Keep the body strictly ASCII markdown —
  // no raw emoji (see the tags note above).
  const header = matched.length ? `**Keywords:** ${matched.join(', ')}\n\n` : '';
  const summary =
    `**${c.n} calls** on ${tgName} (${sysName}) in ` +
    `${config.RDIO_BURST_WINDOW_MIN} min${urgent ? ' — **URGENT**' : ''}\n`;

  let lines = '';
  let shown = 0;
  for (const call of c.calls) {
    const t = (call.transcript ?? '').trim();
    if (!t) continue;
    const url = callUrl(call.id);
    const link = url ? `\n[▶ Open call ${call.id}](${url})` : '';
    const entry = `\n**[${fmtTime(call.dt)}]** ${t}${link}\n`;
    if (header.length + summary.length + lines.length + entry.length > MAX_BODY_CHARS) {
      break;
    }
    lines += entry;
    shown += 1;
  }
  const omitted = c.calls.length - shown;
  const tail =
    omitted > 0
      ? `\n… +${omitted} more call${omitted === 1 ? '' : 's'} — open a link above for the rest`
      : '';

  return {
    title,
    body: header + summary + lines + tail,
    priority,
    tags,
    click,
    matched,
    urgent,
  };
}

/** Publish a built notification to ntfy. Topic defaults to NTFY_TOPIC but
 *  can be overridden (e.g. a private test topic). */
export async function publishToNtfy(
  n: Pick<Notification, 'title' | 'body' | 'priority' | 'tags' | 'click'>,
  topic?: string,
): Promise<boolean> {
  if (!config.NTFY_BASE_URL) {
    log.warn('ntfy publish skipped: NTFY_BASE_URL unset');
    return false;
  }
  const t = topic ?? config.NTFY_TOPIC;
  if (!t) {
    log.warn('ntfy publish skipped: no topic');
    return false;
  }
  const base = config.NTFY_BASE_URL.replace(/\/+$/, '');
  const headers: Record<string, string> = {
    Title: asciiHeader(n.title),
    Priority: n.priority,
    Tags: n.tags.join(','),
    Markdown: 'yes',
  };
  if (n.click) {
    // Deliberately NO `Click` header: tapping the notification should just
    // open it (to read the transcript), not jump to a call. Opening a call
    // is opt-in via this explicit action button, which also works on
    // mobile where the body's markdown links don't render.
    headers['Actions'] = `view, Open latest call, ${asciiHeader(n.click)}`;
  }
  if (config.NTFY_TOKEN) headers['Authorization'] = `Bearer ${config.NTFY_TOKEN}`;
  try {
    const res = await fetch(`${base}/${t}`, {
      method: 'POST',
      headers,
      body: n.body,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      log.warn({ status: res.status, body: text.slice(0, 200) }, 'ntfy publish failed');
      return false;
    }
    return true;
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'ntfy publish error');
    return false;
  }
}

// ---------------------------------------------------------------------------
// Cooldown state (main archive DB).
// ---------------------------------------------------------------------------

async function loadActiveCooldowns(pool: Pool): Promise<Set<string>> {
  const res = await pool.query<{ system: number; talkgroup: number }>(
    `SELECT system, talkgroup FROM ntfy_incident_cooldown
      WHERE last_alert > now() - ($1 || ' minutes')::interval`,
    [String(config.RDIO_ALERT_COOLDOWN_MIN)],
  );
  return new Set(res.rows.map((r) => `${r.system}|${r.talkgroup}`));
}

async function bumpCooldown(pool: Pool, system: number, talkgroup: number): Promise<void> {
  await pool.query(
    `INSERT INTO ntfy_incident_cooldown (system, talkgroup, last_alert)
     VALUES ($1, $2, now())
     ON CONFLICT (system, talkgroup) DO UPDATE SET last_alert = now()`,
    [system, talkgroup],
  );
}

export async function runRdioIncidentAlertsOnce(): Promise<void> {
  if (running) return;
  running = true;
  try {
    const rdio = await getRdioPool();
    const pool = await getPool();
    if (!rdio || !pool) return;

    const bursts = await detectBursts(rdio);
    if (bursts.length === 0) return;

    // Bootstrap: first tick after (re)start just records current bursts
    // as on-cooldown so we don't alert mid-incident on a restart.
    if (!bootstrapped) {
      for (const b of bursts) await bumpCooldown(pool, b.system, b.talkgroup);
      bootstrapped = true;
      log.info(
        { bursts: bursts.length },
        'rdio incident alerts: bootstrapped current bursts (no push)',
      );
      return;
    }

    const onCooldown = await loadActiveCooldowns(pool);
    const requireKeyword = config.RDIO_ALERT_REQUIRE_KEYWORD;

    const fresh = bursts.filter((b) => {
      if (onCooldown.has(`${b.system}|${b.talkgroup}`)) return false;
      if (requireKeyword) {
        const { matched } = analyzeKeywords(b.allText);
        if (matched.length === 0) return false;
      }
      return true;
    });
    if (fresh.length === 0) return;

    // Cap per tick so a backlog can't flood the topic. The overflow isn't
    // dropped — un-sent talkgroups aren't put on cooldown, so they're
    // re-evaluated next tick. Log loudly; never silently truncate.
    let toSend = fresh;
    if (fresh.length > MAX_ALERTS_PER_TICK) {
      log.warn(
        { total: fresh.length, sending: MAX_ALERTS_PER_TICK },
        'rdio incident alerts: capping pushes this tick',
      );
      toSend = fresh.slice(0, MAX_ALERTS_PER_TICK);
    }

    for (const b of toSend) {
      // Bump cooldown BEFORE the send so a slow/failed publish can't
      // re-fire next tick; one missed push beats a storm.
      await bumpCooldown(pool, b.system, b.talkgroup);
      const notif = await buildNotification(b);
      const ok = await publishToNtfy(notif);
      log.info(
        {
          system: b.system,
          talkgroup: b.talkgroup,
          calls: b.n,
          keywords: notif.matched,
          priority: notif.priority,
          ok,
        },
        'rdio incident alert',
      );
    }
  } catch (err) {
    log.error({ err: (err as Error).message }, 'rdio incident alerts: tick failed');
  } finally {
    running = false;
  }
}

/** Start the detector loop. Idempotent. No-op (with a log) unless fully
 *  configured. */
export function startRdioIncidentAlertLoop(): void {
  if (timer) return;
  if (!config.RDIO_INCIDENT_ALERTS_ENABLED) {
    log.info('rdio incident alerts: disabled (RDIO_INCIDENT_ALERTS_ENABLED!=true)');
    return;
  }
  if (!isRdioConfigured()) {
    log.warn('rdio incident alerts: RDIO_DATABASE_URL unset — not starting');
    return;
  }
  if (!config.NTFY_BASE_URL || !config.NTFY_TOPIC) {
    log.warn('rdio incident alerts: NTFY_BASE_URL/NTFY_TOPIC unset — not starting');
    return;
  }
  if (config.RDIO_ALERT_REQUIRE_KEYWORD && incidentKeywords().length === 0) {
    log.warn(
      'rdio incident alerts: RDIO_ALERT_REQUIRE_KEYWORD=true but ' +
        'RDIO_ALERT_KEYWORDS is empty — nothing will ever fire. Set ' +
        'RDIO_ALERT_KEYWORDS or turn the keyword gate off.',
    );
  }
  const secs = Math.max(5, config.RDIO_ALERT_POLL_SECS);
  setTimeout(() => void runRdioIncidentAlertsOnce(), 10_000).unref?.();
  timer = setInterval(() => void runRdioIncidentAlertsOnce(), secs * 1000);
  timer.unref?.();
  log.info(
    {
      secs,
      topic: config.NTFY_TOPIC,
      window_min: config.RDIO_BURST_WINDOW_MIN,
      threshold: config.RDIO_BURST_THRESHOLD,
      require_keyword: config.RDIO_ALERT_REQUIRE_KEYWORD,
    },
    'rdio incident alert loop started',
  );
}

export function stopRdioIncidentAlertLoop(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}
