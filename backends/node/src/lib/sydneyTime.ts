/**
 * Sydney-local timestamp formatting.
 *
 * Python's data_history rows store ISO strings via
 * `datetime.fromtimestamp(ts).isoformat()` — naive Sydney wall-clock,
 * no offset suffix. The frontend logs page (and dashboard widgets)
 * read those strings as wall-clock when filtering / grouping by day.
 *
 * Node's `Date#toISOString()` emits UTC with a `Z` suffix. When the
 * frontend does `s.startsWith('2026-04-28')` to bucket "today's"
 * incidents, the UTC date can be off by up to ~14 hours during
 * DST-active months, surfacing the wrong day's rows.
 *
 * This helper produces the same string shape python emits:
 *   `YYYY-MM-DDTHH:mm:ss` (no offset, no fractional seconds).
 */
export function formatSydneyNaive(ms: number): string {
  if (!Number.isFinite(ms)) return '';
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Australia/Sydney',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  const parts = fmt.formatToParts(new Date(ms));
  const get = (t: string): string =>
    parts.find((p) => p.type === t)?.value ?? '00';
  const hour = get('hour') === '24' ? '00' : get('hour'); // Intl quirk
  return `${get('year')}-${get('month')}-${get('day')}T${hour}:${get('minute')}:${get('second')}`;
}

/** Convenience: epoch seconds → naive Sydney string, or null. */
export function sydneyIsoFromUnix(unix: number | null | undefined): string | null {
  if (unix === null || unix === undefined || !Number.isFinite(unix)) return null;
  return formatSydneyNaive(unix * 1000);
}

/** Convenience: Date → naive Sydney string, or null. */
export function sydneyIsoFromDate(d: Date | null | undefined): string | null {
  if (!d) return null;
  return formatSydneyNaive(d.getTime());
}

/**
 * Inverse of formatSydneyNaive: interpret (y, mo, d, hh, mm, ss) as a
 * wall-clock time in Australia/Sydney and return the epoch SECONDS for
 * that instant. Used to parse `date_from`/`date_to`/`today` boundaries
 * that the frontend sends as Sydney wall-clock dates — `new Date('...')`
 * would instead parse them in the server's local zone (UTC on prod),
 * shifting the requested day by the Sydney offset (10–11h).
 *
 * Works by computing Sydney's UTC offset at the candidate instant via
 * formatSydneyNaive, then correcting. One refinement iteration handles
 * the DST-transition days where the offset at the UTC guess differs from
 * the offset at the resolved instant.
 */
export function sydneyUnixFromNaive(
  y: number,
  mo: number,
  d: number,
  hh = 0,
  mm = 0,
  ss = 0,
): number | null {
  const wantUTC = Date.UTC(y, mo - 1, d, hh, mm, ss);
  if (!Number.isFinite(wantUTC)) return null;
  // offsetMs(ms) = (Sydney wall-clock of ms, read as if UTC) − ms.
  const offsetMs = (ms: number): number => {
    const syd = formatSydneyNaive(ms); // 'YYYY-MM-DDTHH:mm:ss' in Sydney
    const asUtc = Date.parse(`${syd}Z`);
    return Number.isFinite(asUtc) ? asUtc - ms : 0;
  };
  let instant = wantUTC - offsetMs(wantUTC);
  instant = wantUTC - offsetMs(instant); // refine across DST edges
  return Math.floor(instant / 1000);
}
