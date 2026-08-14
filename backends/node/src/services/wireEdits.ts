/**
 * Diffing for the Wire per-post edit log (migration 066).
 *
 * Pure functions only — the SQL lives in api/wire.ts alongside the update it
 * hangs off, matching how the rest of the Wire is split.
 *
 * The output is a *reader-facing* summary, not a version history. Two rules
 * shape it:
 *
 *  - Long prose (body, caption, excerpt) records only that it changed. Storing
 *    before/after would put a second copy of a 100k-character article in the
 *    table every time someone fixes a typo, to power a line that just says
 *    "Body edited".
 *  - Everything short — title, licence, credit, location, agencies, co-authors —
 *    keeps its before and after, because that's the part a reader who saw the
 *    original would actually want to check.
 *
 * Attached media is compared by storage identity rather than by count, so
 * swapping one photo for another reads as a change instead of as nothing.
 */

export interface EditChange {
  /** Machine name, e.g. 'title'. */
  field: string;
  /** Human label for the UI, e.g. 'Title'. */
  label: string;
  /** Previous value, when short enough to be worth keeping. */
  from?: string | null;
  /** New value, same condition. */
  to?: string | null;
  /** Used instead of from/to when a value pair would be meaningless. */
  detail?: string;
}

/** Fields we compare. Long-text ones are flagged, never valued. */
export interface EditSnapshot {
  title?: string | null;
  caption?: string | null;
  excerpt?: string | null;
  body?: string | null;
  location?: { type?: string | null; region?: string | null; lat?: number | null; lng?: number | null } | null;
  agencies?: string[] | null;
  incidentId?: string | null;
  license?: string | null;
  credit?: string | null;
  watermark?: boolean;
  coAuthors?: Array<{ id?: string; name?: string | null }> | null;
  parentArticleId?: string | null;
  /** Storage identity of each attached item (r2 key or CF image id). */
  mediaKeys?: string[] | null;
}

const norm = (v: unknown): string => (v == null ? '' : String(v).trim());

/** Round-then-compare: a pin dragged by a metre isn't an edit worth reporting. */
function locationText(loc: EditSnapshot['location']): string {
  if (!loc) return '';
  if (loc.type === 'region') return norm(loc.region);
  if (loc.lat != null && loc.lng != null) return `${Number(loc.lat).toFixed(4)}, ${Number(loc.lng).toFixed(4)}`;
  return '';
}

function listText(v: string[] | null | undefined): string {
  return (v ?? []).map(norm).filter(Boolean).sort().join(', ');
}

function coAuthorText(v: EditSnapshot['coAuthors']): string {
  return (v ?? []).map((c) => norm(c?.name) || norm(c?.id)).filter(Boolean).sort().join(', ');
}

/** Cap a stored value so one very long title can't bloat the row. */
function clip(s: string): string | null {
  if (!s) return null;
  return s.length > 300 ? `${s.slice(0, 297)}…` : s;
}

function pushIfChanged(out: EditChange[], field: string, label: string, before: string, after: string): void {
  if (before === after) return;
  out.push({ field, label, from: clip(before), to: clip(after) });
}

/** Long-text fields: report the change, never the content. */
function pushIfProseChanged(out: EditChange[], field: string, label: string, before: string, after: string): void {
  if (before === after) return;
  const delta = after.length - before.length;
  const detail =
    before === '' ? 'added'
      : after === '' ? 'removed'
        : delta > 0 ? `expanded by ${delta} characters`
          : delta < 0 ? `shortened by ${-delta} characters`
            : 'reworded';
  out.push({ field, label, detail });
}

function mediaChange(before: string[], after: string[]): EditChange | null {
  const b = new Set(before);
  const a = new Set(after);
  const added = [...a].filter((k) => !b.has(k)).length;
  const removed = [...b].filter((k) => !a.has(k)).length;
  if (added === 0 && removed === 0) {
    // Same set of files — a pure reorder or a caption-only change on an item.
    return before.join('|') === after.join('|') ? null
      : { field: 'media', label: 'Attached media', detail: 'reordered' };
  }
  const parts: string[] = [];
  if (added) parts.push(`${added} added`);
  if (removed) parts.push(`${removed} removed`);
  return { field: 'media', label: 'Attached media', detail: parts.join(', ') };
}

/**
 * Compare two snapshots. Returns [] when nothing a reader would care about
 * changed — the caller uses that to skip writing a row at all, which is what
 * keeps "saved without changing anything" out of the log.
 */
export function diffSnapshots(before: EditSnapshot, after: EditSnapshot): EditChange[] {
  const out: EditChange[] = [];

  pushIfChanged(out, 'title', 'Title', norm(before.title), norm(after.title));
  pushIfProseChanged(out, 'caption', 'Caption', norm(before.caption), norm(after.caption));
  pushIfProseChanged(out, 'excerpt', 'Summary', norm(before.excerpt), norm(after.excerpt));
  pushIfProseChanged(out, 'body', 'Article body', norm(before.body), norm(after.body));
  pushIfChanged(out, 'location', 'Location', locationText(before.location), locationText(after.location));
  pushIfChanged(out, 'agencies', 'Agencies', listText(before.agencies), listText(after.agencies));
  pushIfChanged(out, 'incident', 'Linked incident', norm(before.incidentId), norm(after.incidentId));
  pushIfChanged(out, 'license', 'Licence', norm(before.license), norm(after.license));
  pushIfChanged(out, 'credit', 'Credit line', norm(before.credit), norm(after.credit));
  pushIfChanged(out, 'co_authors', 'Co-authors', coAuthorText(before.coAuthors), coAuthorText(after.coAuthors));
  pushIfChanged(out, 'series', 'Part of series', norm(before.parentArticleId), norm(after.parentArticleId));

  if (before.watermark !== after.watermark && (before.watermark != null || after.watermark != null)) {
    out.push({ field: 'watermark', label: 'Watermark', from: before.watermark ? 'on' : 'off', to: after.watermark ? 'on' : 'off' });
  }

  const m = mediaChange(before.mediaKeys ?? [], after.mediaKeys ?? []);
  if (m) out.push(m);

  return out;
}

/** Storage identity for one media row, for the media comparison above. */
export function mediaKeyOf(m: { r2_key?: string | null; cf_image_id?: string | null }): string {
  return m.r2_key || m.cf_image_id || '';
}
