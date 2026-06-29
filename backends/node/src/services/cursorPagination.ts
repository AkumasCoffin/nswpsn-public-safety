/**
 * Cursor pagination helpers for /api/data/history.
 *
 * Format mirrors Python's _encode_history_cursor / _decode_history_cursor
 * exactly so existing logs.html clients keep working without a code
 * change on the frontend.
 *
 * Encoding: base64-url (no padding) of the literal ASCII string
 *   `<fetched_at_unix_seconds>:<row_id>`              (single-family)
 *   `<fetched_at_unix_seconds>:<row_id>:<table>`      (multi-family)
 *
 * The (fetched_at, id) pair is the keyset position. fetched_at alone
 * isn't unique — an archive flush can write thousands of rows with the
 * same second — so the id is the tiebreaker that keeps the forward
 * walk stable.
 *
 * The optional third `<table>` segment is only emitted when a query
 * fans out across more than one archive family. `id` is a per-table
 * BIGSERIAL, so it is NOT globally unique across families — at a
 * fetched_at tie the merge orders rows by (fetched_at, table_rank, id),
 * and the seek on the next page must know which table the boundary row
 * came from to reproduce that order. Single-family cursors omit it and
 * stay byte-identical to the Python format for backward compatibility.
 *
 * MAX_OFFSET caps offset-based pagination at 10,000 — past that the
 * backend rejects the request and points the caller at `?cursor=`. This
 * matches Python's _DATA_HISTORY_MAX_OFFSET and is the documented
 * contract logs.html already respects.
 */

export const MAX_OFFSET = 10_000;

/**
 * Encode a (fetched_at, row_id) keyset position into a URL-safe cursor.
 * Pass `table` only for multi-family queries; it's appended as a third
 * segment so the next page's seek can reproduce the cross-table order.
 */
export function encodeCursor(
  fetchedAt: number,
  rowId: number,
  table?: string | null,
): string {
  // Match Python's `int(...)` coercion — drop any fractional part. Negative
  // values aren't expected (epoch seconds, BIGSERIAL ids) but we don't
  // validate here; callers shouldn't be passing them, and the decoder is
  // the part that has to be tolerant of bad input.
  const fa = Math.trunc(fetchedAt);
  const id = Math.trunc(rowId);
  const raw = table ? `${fa}:${id}:${table}` : `${fa}:${id}`;
  // URL-safe base64 (`-`/`_` instead of `+`/`/`), strip `=` padding.
  return Buffer.from(raw, 'ascii').toString('base64url');
}

export interface DecodedCursor {
  fetchedAt: number;
  rowId: number;
  /** Archive table the boundary row came from. Only set for multi-family
   *  cursors; undefined collapses the seek to single-table behaviour. */
  table?: string;
}

/**
 * Decode a cursor produced by encodeCursor. Returns null on any
 * malformed input — matches Python's "fail-soft" behaviour (callers
 * surface a 400 response themselves).
 */
export function decodeCursor(cursor: string | null | undefined): DecodedCursor | null {
  if (!cursor) return null;
  try {
    const raw = Buffer.from(cursor, 'base64url').toString('ascii');
    // Defensive: if the buffer didn't decode to ASCII the toString call
    // can produce replacement characters silently. Reject anything that
    // doesn't fit the `<int>:<int>` or `<int>:<int>:<table>` shape.
    const parts = raw.split(':');
    if (parts.length < 2 || parts.length > 3) return null;
    const [faStr, idStr, tableStr] = parts;
    if (!/^-?\d+$/.test(faStr!) || !/^-?\d+$/.test(idStr!)) return null;
    const fetchedAt = Number.parseInt(faStr!, 10);
    const rowId = Number.parseInt(idStr!, 10);
    if (!Number.isFinite(fetchedAt) || !Number.isFinite(rowId)) return null;
    // The table segment is validated by shape only — the consumer maps it
    // to a rank and falls back to single-table seek for anything unknown,
    // so a stale/foreign value degrades gracefully rather than 400ing.
    if (tableStr !== undefined && !/^[a-z_]{3,40}$/.test(tableStr)) return null;
    return tableStr !== undefined
      ? { fetchedAt, rowId, table: tableStr }
      : { fetchedAt, rowId };
  } catch {
    return null;
  }
}
