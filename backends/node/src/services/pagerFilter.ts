/**
 * Which decoded pages are traffic, and which are filtered out as noise.
 *
 * One set of rules, read in two places:
 *
 *   - the pager relay (api/node-ingest.ts) decides per message whether to
 *     forward it to Pagermon;
 *   - the staff Data tab (api/node-data.ts) splits the stored pages into real
 *     traffic and a separate "filtered" table.
 *
 * Every page is stored in node_pager_events whether it is forwarded or not, so
 * the data view re-applies the rules in SQL rather than reading a stored flag.
 * That keeps history consistent with the current rules: blocking a capcode
 * moves its past pages into the filtered table too. pagerFilterReasonSql must
 * agree with pagerFilterReason; the tests hold them to the same cases.
 *
 * Checked in this order, the same order the relay applies them:
 *   'blocked capcode' — a non-message transmitter (PAGER_BLOCKED_CAPCODES)
 *   'empty message'   — tone-only page, no body
 *   'noise'           — one or two characters of POCSAG bit-error garbage
 *   'time code'       — an FRNSW clock tick, a bare HHMM
 */
import { config } from '../config.js';

export type PagerFilterReason = 'blocked capcode' | 'empty message' | 'noise' | 'time code';

export const PAGER_FILTER_REASONS: readonly PagerFilterReason[] = [
  'blocked capcode',
  'empty message',
  'noise',
  'time code',
];

// Capcodes to drop entirely (non-message data/encoded transmitters), the
// union of built-in defaults and the PAGER_BLOCKED_CAPCODES env. Defaults
// exist because these are properties of the NSW networks this deployment
// listens to, not of any one install:
//   1850000 — FRNSW network idle/keepalive, one page every ~65s around the
//             clock, empty body. The empty-body guard below also drops it;
//             blocking the capcode keeps it out even if the payload varies.
const DEFAULT_BLOCKED_CAPCODES = ['1850000'];
export const BLOCKED_CAPCODES: ReadonlySet<string> = new Set(
  [
    ...DEFAULT_BLOCKED_CAPCODES,
    ...(config.PAGER_BLOCKED_CAPCODES || '').split(','),
  ]
    .map((s) => s.trim())
    .filter(Boolean),
);

/**
 * Why a decoded page is noise rather than a message, or null when it is real.
 *
 * 'empty' — tone-only pages (a valid reception, but bodyless; Pagermon
 * rejects them outright, and one at the head of a node's disk queue wedged it
 * for days — see the relay guards). 'too short' — POCSAG bit-error garbage:
 * corrupted decodes land on random capcodes with one- or two-character
 * bodies ("r", "lj", ";", "=m", all live examples). No genuine page on these
 * networks is two characters; the shortest real traffic (a clock tick) is
 * four digits and has its own filter.
 */
export function pagerNoiseReason(message: string): 'empty' | 'too short' | null {
  const t = (message || '').trim();
  if (t.length === 0) return 'empty';
  if (t.length <= 2) return 'too short';
  return null;
}

/**
 * A page whose whole body is a time of day — `0018`, `2342`, `2111`.
 *
 * FRNSW transmitters send these as a clock tick, once every few minutes, and
 * they arrived in volume the moment the FRNSW capcodes were added: they were
 * half of every window of the feed. There is no incident in them, no capcode
 * to address them to, and nothing downstream can do anything with one.
 *
 * Matched by SHAPE rather than by capcode, because the capcode carrying them
 * is an implementation detail of whichever transmitter is ticking and a new
 * one would start the noise again. Requiring a valid HH:MM — hours under 24,
 * minutes under 60 — is what keeps a genuine four-digit body (a turnout
 * number, a brigade code) out of the filter; `9999` is not a time and is not
 * dropped.
 *
 * Deliberately NOT checked against the message's own timestamp. The tick runs
 * a minute or two behind what stamps it (21:37 carrying `2136`), and a clock
 * comparison would be a second thing to get wrong for no gain: a bare time of
 * day is content-free whether or not it agrees with the clock.
 */
const PAGER_TIME_CODE_RE = /^([01]\d|2[0-3])[0-5]\d$/;

export function isPagerTimeCode(message: string): boolean {
  return PAGER_TIME_CODE_RE.test((message || '').trim());
}

/** Why the relay filters this page, or null when it is forwarded. */
export function pagerFilterReason(capcode: string, message: string): PagerFilterReason | null {
  if (BLOCKED_CAPCODES.has(capcode)) return 'blocked capcode';
  const noise = pagerNoiseReason(message);
  if (noise === 'empty') return 'empty message';
  if (noise) return 'noise';
  if (isPagerTimeCode(message)) return 'time code';
  return null;
}

/**
 * pagerFilterReason as a SQL expression over node_pager_events: the reason
 * text, or NULL for a page that is kept.
 *
 * `blockedParam` is the placeholder bound to blockedCapcodeList(); `alias` is
 * the table alias, if the query uses one. Postgres measures length in
 * characters where JS counts UTF-16 units, so the two can only disagree on a
 * two-character body made of emoji — never a real page.
 */
export function pagerFilterReasonSql(blockedParam: string, alias = ''): string {
  const col = (name: string) => (alias ? `${alias}.${name}` : name);
  const body = `regexp_replace(COALESCE(${col('message')}, ''), '^\\s+|\\s+$', '', 'g')`;
  return `(CASE
      WHEN ${col('capcode')} = ANY(${blockedParam}::text[]) THEN 'blocked capcode'
      WHEN char_length(${body}) = 0 THEN 'empty message'
      WHEN char_length(${body}) <= 2 THEN 'noise'
      WHEN ${body} ~ '^([01][0-9]|2[0-3])[0-5][0-9]$' THEN 'time code'
    END)`;
}

/** The blocked capcodes, for binding to pagerFilterReasonSql's placeholder. */
export function blockedCapcodeList(): string[] {
  return [...BLOCKED_CAPCODES];
}
