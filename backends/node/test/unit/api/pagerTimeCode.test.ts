/**
 * Pager clock ticks.
 *
 * FRNSW transmitters send a bare time of day every few minutes, and adding the
 * FRNSW capcodes filled the feed with them. They carry no incident, so they are
 * dropped at the relay — but the shape has to be narrow enough that a genuine
 * four-digit page is never mistaken for one.
 */
import { describe, it, expect } from 'vitest';
import { isPagerTimeCode } from '../../../src/api/node-ingest.js';

describe('pager clock ticks', () => {
  it('drops a bare time of day', () => {
    // Every example taken from the live feed.
    for (const t of ['0018', '2342', '2331', '2326', '2316', '2256', '2246',
      '2242', '2234', '2149', '2136', '2111', '2125', '2131']) {
      expect(isPagerTimeCode(t)).toBe(true);
    }
    expect(isPagerTimeCode('  2111  ')).toBe(true);   // decoders pad
    expect(isPagerTimeCode('0000')).toBe(true);
    expect(isPagerTimeCode('2359')).toBe(true);
  });

  it('keeps four digits that are not a time', () => {
    // The whole reason for validating HH:MM rather than /^\d{4}$/: a turnout
    // or brigade number is four digits too, and losing one loses a callout.
    expect(isPagerTimeCode('2400')).toBe(false);   // no 24th hour
    expect(isPagerTimeCode('2560')).toBe(false);   // no 60th minute
    expect(isPagerTimeCode('9999')).toBe(false);
    expect(isPagerTimeCode('4640')).toBe(false);
  });

  it('keeps every real page', () => {
    expect(isPagerTimeCode('FRINC TYPE: BUSH FIRE TURNOUT: 464 INC: 185421-20092026')).toBe(false);
    expect(isPagerTimeCode('FRINC TYPE: MVA PERSONS TRAPPED TURNOUT: 325 INC: 185382-19092026')).toBe(false);
    expect(isPagerTimeCode('10:01:46 SUNDAY TEST PAGE FCC')).toBe(false);
    // A time is only a tick when it is the WHOLE message.
    expect(isPagerTimeCode('2111 STRUCTURE FIRE')).toBe(false);
    expect(isPagerTimeCode('TURNOUT: 2111')).toBe(false);
  });

  it('ignores an empty or absent body', () => {
    expect(isPagerTimeCode('')).toBe(false);
    expect(isPagerTimeCode(undefined as unknown as string)).toBe(false);
  });
});
