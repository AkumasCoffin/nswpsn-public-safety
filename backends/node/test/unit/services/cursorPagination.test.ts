/**
 * Roundtrip + edge-case tests for the cursor pagination helpers. The
 * encoding has to match Python's _encode_history_cursor / _decode_history_cursor
 * byte-for-byte so existing logs.html clients keep working.
 */
import { describe, it, expect } from 'vitest';
import {
  decodeCursor,
  encodeCursor,
  MAX_OFFSET,
} from '../../../src/services/cursorPagination.js';

describe('cursorPagination', () => {
  it('roundtrips a typical (fetched_at, id) pair', () => {
    const cursor = encodeCursor(1_700_000_000, 12345);
    const decoded = decodeCursor(cursor);
    expect(decoded).toEqual({ fetchedAt: 1_700_000_000, rowId: 12345 });
  });

  it('produces URL-safe base64 with no padding', () => {
    const cursor = encodeCursor(1_700_000_000, 1);
    expect(cursor).not.toContain('=');
    expect(cursor).not.toContain('+');
    expect(cursor).not.toContain('/');
  });

  it('matches Python encoding for a known input', () => {
    // Python: base64.urlsafe_b64encode(b"1700000000:1").decode().rstrip('=')
    //   -> 'MTcwMDAwMDAwMDox'
    expect(encodeCursor(1_700_000_000, 1)).toBe('MTcwMDAwMDAwMDox');
  });

  it('decodes a value that lacks padding', () => {
    // 'MTcwMDAwMDAwMDox' (16 chars) — padding-free, decoder must add it.
    expect(decodeCursor('MTcwMDAwMDAwMDox')).toEqual({
      fetchedAt: 1_700_000_000,
      rowId: 1,
    });
  });

  it('handles huge BIGSERIAL ids', () => {
    const big = 9_999_999_999_999;
    const cursor = encodeCursor(2_000_000_000, big);
    expect(decodeCursor(cursor)).toEqual({ fetchedAt: 2_000_000_000, rowId: big });
  });

  it('truncates fractional inputs (matches Python int() coercion)', () => {
    const cursor = encodeCursor(1_700_000_000.7, 12.9);
    expect(decodeCursor(cursor)).toEqual({ fetchedAt: 1_700_000_000, rowId: 12 });
  });

  it('returns null for empty / null / undefined cursors', () => {
    expect(decodeCursor('')).toBeNull();
    expect(decodeCursor(null)).toBeNull();
    expect(decodeCursor(undefined)).toBeNull();
  });

  it('returns null for malformed base64', () => {
    // Trailing garbage that doesn't decode to ASCII at all.
    expect(decodeCursor('!!!not-base64!!!')).toBeNull();
  });

  it('returns null when the decoded payload lacks a colon separator', () => {
    // base64('hello') = 'aGVsbG8'.
    expect(decodeCursor('aGVsbG8')).toBeNull();
  });

  it('returns null when either side of the colon is non-numeric', () => {
    // base64('abc:123') = 'YWJjOjEyMw'.
    expect(decodeCursor('YWJjOjEyMw')).toBeNull();
    // base64('123:def') = 'MTIzOmRlZg'.
    expect(decodeCursor('MTIzOmRlZg')).toBeNull();
  });

  it('exposes MAX_OFFSET = 10000 (matches Python contract)', () => {
    expect(MAX_OFFSET).toBe(10_000);
  });
});
