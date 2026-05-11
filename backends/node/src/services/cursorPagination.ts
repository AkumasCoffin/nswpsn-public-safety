/**
 * Cursor pagination helpers for /api/data/history.
 *
 * Format mirrors Python's _encode_history_cursor / _decode_history_cursor
 * exactly so existing logs.html clients keep working without a code
 * change on the frontend.
 *
 * Encoding: base64-url (no padding) of the literal ASCII string
 *   `<fetched_at_unix_seconds>:<row_id>`
 *
 * The (fetched_at, id) pair is the keyset position. fetched_at alone
 * isn't unique — an archive flush can write thousands of rows with the
 * same second — so the id is the tiebreaker that keeps the forward
 * walk stable.
 *
 * MAX_OFFSET caps offset-based pagination at 10,000 — past that the
 * backend rejects the request and points the caller at `?cursor=`. This
 * matches Python's _DATA_HISTORY_MAX_OFFSET and is the documented
 * contract logs.html already respects.
 */

export const MAX_OFFSET = 10_000;

/** Encode a (fetched_at, row_id) keyset position into a URL-safe cursor. */
export function encodeCursor(fetchedAt: number, rowId: number): string {
  // Match Python's `int(...)` coercion — drop any fractional part. Negative
  // values aren't expected (epoch seconds, BIGSERIAL ids) but we don't
  // validate here; callers shouldn't be passing them, and the decoder is
  // the part that has to be tolerant of bad input.
  const fa = Math.trunc(fetchedAt);
  const id = Math.trunc(rowId);
  const raw = `${fa}:${id}`;
  // URL-safe base64 (`-`/`_` instead of `+`/`/`), strip `=` padding.
  return Buffer.from(raw, 'ascii').toString('base64url');
}

export interface DecodedCursor {
  fetchedAt: number;
  rowId: number;
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
    // doesn't fit the `<int>:<int>` shape.
    const colon = raw.indexOf(':');
    if (colon < 0) return null;
    const faStr = raw.slice(0, colon);
    const idStr = raw.slice(colon + 1);
    if (!/^-?\d+$/.test(faStr) || !/^-?\d+$/.test(idStr)) return null;
    const fetchedAt = Number.parseInt(faStr, 10);
    const rowId = Number.parseInt(idStr, 10);
    if (!Number.isFinite(fetchedAt) || !Number.isFinite(rowId)) return null;
    return { fetchedAt, rowId };
  } catch {
    return null;
  }
}
