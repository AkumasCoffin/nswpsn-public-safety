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
