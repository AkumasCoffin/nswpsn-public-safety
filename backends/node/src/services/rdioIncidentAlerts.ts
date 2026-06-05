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
 * restart in the middle of a busy period doesn't re-alert active jobs —
 * mirrors the discord bot's `_first_poll` anti-flood.
 *
 * Entirely gated by config: the loop refuses to start unless
 * RDIO_INCIDENT_ALERTS_ENABLED is true and NTFY_BASE_URL + NTFY_TOPIC +
 * RDIO_DATABASE_URL are all set.
 */
import { fetch } from 'undici';
import type { Pool } from 'pg';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { getPool } from '../db/pool.js';
import { getRdioPool, isRdioConfigured, resolveLabels } from './rdio.js';

// Keywords that escalate a burst to URGENT priority and bypass the
// optional require-keyword gate — radio phrases that mean "this is
// serious right now" regardless of call volume.
const URGENT_KEYWORDS = [
  'mayday',
  'mass casualty',
  'm.c.i',
  ' mci',
  'multiple casualt',
  'persons trapped',
  'person trapped',
  'building collapse',
  'structure collapse',
  'explosion',
  'working fire',
  'fully involved',
  'second alarm',
  'third alarm',
  'strike team',
  'evacuat',
];

// Default "incident keyword" list — used by the require-keyword gate and
// to pick tags. Overridable via RDIO_ALERT_KEYWORDS (comma-separated).
const DEFAULT_INCIDENT_KEYWORDS = [
  'structure fire',
  'house fire',
  'building fire',
  'fire',
  'rescue',
  'mva',
  'accident',
  'hazmat',
  'gas leak',
  'crash',
  'entrapment',
  'casualt',
  'collapse',
];

const MAX_ALERTS_PER_TICK = 5;

interface BurstRow {
  system: number;
  talkgroup: number;
  n: number;
  latest_id: number;
  recent_lines: string[] | null;
  all_text: string | null;
}

let timer: NodeJS.Timeout | null = null;
let running = false;
let bootstrapped = false;

function incidentKeywords(): string[] {
  const raw = config.RDIO_ALERT_KEYWORDS;
  if (!raw) return DEFAULT_INCIDENT_KEYWORDS;
  const parsed = raw
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter((s) => s.length > 0);
  return parsed.length > 0 ? parsed : DEFAULT_INCIDENT_KEYWORDS;
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

/** Pull every talkgroup that's bursting right now (>= threshold calls in
 *  the window). One query; the window is computed in naive-UTC to match
 *  rdio-scanner's `dateTime` column (timestamp WITHOUT time zone, UTC). */
async function findBursts(rdio: Pool): Promise<BurstRow[]> {
  const sql = `
    SELECT
      "system"    AS system,
      "talkgroup" AS talkgroup,
      COUNT(*)::int                                         AS n,
      MAX("id")::int                                        AS latest_id,
      (array_agg("transcript" ORDER BY "dateTime" DESC))[1:4] AS recent_lines,
      lower(string_agg("transcript", ' | '))               AS all_text
    FROM "rdioScannerCalls"
    WHERE "dateTime" > (now() AT TIME ZONE 'UTC') - ($1 || ' minutes')::interval
      AND "transcript" IS NOT NULL
      AND "transcript" <> ''
      AND "talkgroup" IS NOT NULL
    GROUP BY "system", "talkgroup"
    HAVING COUNT(*) >= $2
    ORDER BY COUNT(*) DESC
  `;
  const res = await rdio.query<BurstRow>(sql, [
    String(config.RDIO_BURST_WINDOW_MIN),
    config.RDIO_BURST_THRESHOLD,
  ]);
  return res.rows;
}

/** (system,talkgroup) pairs still inside their cooldown window. */
async function loadActiveCooldowns(pool: Pool): Promise<Set<string>> {
  const res = await pool.query<{ system: number; talkgroup: number }>(
    `SELECT system, talkgroup FROM ntfy_incident_cooldown
      WHERE last_alert > now() - ($1 || ' minutes')::interval`,
    [String(config.RDIO_ALERT_COOLDOWN_MIN)],
  );
  return new Set(res.rows.map((r) => `${r.system}|${r.talkgroup}`));
}

async function bumpCooldown(
  pool: Pool,
  system: number,
  talkgroup: number,
): Promise<void> {
  await pool.query(
    `INSERT INTO ntfy_incident_cooldown (system, talkgroup, last_alert)
     VALUES ($1, $2, now())
     ON CONFLICT (system, talkgroup) DO UPDATE SET last_alert = now()`,
    [system, talkgroup],
  );
}

async function publishToNtfy(opts: {
  title: string;
  body: string;
  priority: 'urgent' | 'high' | 'default';
  tags: string[];
  click: string | null;
}): Promise<boolean> {
  const base = config.NTFY_BASE_URL!.replace(/\/+$/, '');
  const url = `${base}/${config.NTFY_TOPIC}`;
  const headers: Record<string, string> = {
    Title: asciiHeader(opts.title),
    Priority: opts.priority,
    Tags: opts.tags.join(','),
    Markdown: 'yes',
  };
  if (opts.click) headers['Click'] = asciiHeader(opts.click);
  if (config.NTFY_TOKEN) headers['Authorization'] = `Bearer ${config.NTFY_TOKEN}`;
  try {
    const res = await fetch(url, { method: 'POST', headers, body: opts.body });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      log.warn(
        { status: res.status, body: text.slice(0, 200) },
        'ntfy publish failed',
      );
      return false;
    }
    return true;
  } catch (err) {
    log.warn({ err: (err as Error).message }, 'ntfy publish error');
    return false;
  }
}

/** Build the push for one bursting talkgroup. */
async function buildAndSend(b: BurstRow): Promise<boolean> {
  const { systemLabel, talkgroupLabel } = await resolveLabels(
    b.system,
    b.talkgroup,
  );
  const tgName = talkgroupLabel ?? `TG ${b.talkgroup}`;
  const sysName = systemLabel ?? `System ${b.system}`;
  const text = b.all_text ?? '';
  const isUrgent = URGENT_KEYWORDS.some((k) => text.includes(k));
  const priority = isUrgent ? 'urgent' : 'high';

  const tags = isUrgent ? ['rotating_light', 'fire'] : ['radio'];
  const title = `Major radio activity — ${tgName} (${sysName})`;

  const lines = (b.recent_lines ?? [])
    .filter((l) => l && l.trim())
    .slice(0, 3)
    .map((l) => `• ${l.trim()}`);
  const click = config.RDIO_CALL_URL_BASE
    ? `${config.RDIO_CALL_URL_BASE}${b.latest_id}`
    : null;

  const body =
    `**${tgName}** — ${b.n} calls in the last ` +
    `${config.RDIO_BURST_WINDOW_MIN} min` +
    (isUrgent ? ' ⚠️' : '') +
    (lines.length ? `\n\n${lines.join('\n')}` : '');

  return publishToNtfy({ title, body, priority, tags, click });
}

export async function runRdioIncidentAlertsOnce(): Promise<void> {
  if (running) return;
  running = true;
  try {
    const rdio = await getRdioPool();
    const pool = await getPool();
    if (!rdio || !pool) return;

    const bursts = await findBursts(rdio);
    if (bursts.length === 0) return;

    // Bootstrap: first tick after (re)start just records the current
    // bursts as on-cooldown so we don't alert mid-incident on a restart.
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
    const keywords = incidentKeywords();
    const requireKeyword = config.RDIO_ALERT_REQUIRE_KEYWORD;

    const fresh = bursts.filter((b) => {
      if (onCooldown.has(`${b.system}|${b.talkgroup}`)) return false;
      if (requireKeyword) {
        const text = b.all_text ?? '';
        const hit =
          keywords.some((k) => text.includes(k)) ||
          URGENT_KEYWORDS.some((k) => text.includes(k));
        if (!hit) return false;
      }
      return true;
    });

    if (fresh.length === 0) return;

    // Cap per tick so a backlog (e.g. after downtime) can't flood the
    // topic. Loudly log what was dropped — never silently truncate.
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
      // re-fire next tick; one missed push is better than a storm.
      await bumpCooldown(pool, b.system, b.talkgroup);
      const ok = await buildAndSend(b);
      log.info(
        {
          system: b.system,
          talkgroup: b.talkgroup,
          calls: b.n,
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
  const secs = Math.max(5, config.RDIO_ALERT_POLL_SECS);
  // Small initial delay so it doesn't compete with boot-critical work.
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
