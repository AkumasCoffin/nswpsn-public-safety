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

/**
 * Empty pages. Tone-only POCSAG decodes arrive with message: '' — a real
 * reception the schema rightly accepts — but Pagermon rejects them with a 500,
 * which the agent reads as retryable. One such page at the head of a node's
 * disk FIFO wedged it permanently: 3,699 messages queued behind it, 232
 * retries in a day, surviving restarts because the queue is disk-backed.
 * Undeliverable has to be decided at the relay, once, with an ack.
 */
import { isPagerTimeCode as _reuse, pagerNoiseReason } from '../../../src/api/node-ingest.js';

/**
 * Bit-error garbage and tone-only pages. Live examples drove the thresholds:
 * "r", "lj", ";", "=m" all arrived on random capcodes with no alias — POCSAG
 * decode corruption, not traffic — and tone-only pages arrive with an empty
 * body. Nothing real on these networks is two characters: the shortest real
 * traffic (a clock tick like "2111") is four digits and has its own filter.
 */
describe('pagerNoiseReason', () => {
  it('flags empty and whitespace bodies', () => {
    expect(pagerNoiseReason('')).toBe('empty');
    expect(pagerNoiseReason('   ')).toBe('empty');
    expect(pagerNoiseReason(undefined as unknown as string)).toBe('empty');
  });
  it('flags one- and two-character garbage (live examples)', () => {
    for (const g of ['r', 'lj', ';', '=m', ' ;', 'x ']) {
      expect(pagerNoiseReason(g)).toBe('too short');
    }
  });
  it('keeps everything three characters and up', () => {
    expect(pagerNoiseReason('SES')).toBeNull();
    expect(pagerNoiseReason('2111')).toBeNull();  // the time-code filter owns these
    expect(pagerNoiseReason('FRINC TYPE: BUSH FIRE TURNOUT: 464 INC: 185421-20092026')).toBeNull();
  });
});

describe('pager empty-message guard exists beside the time-code guard', () => {
  it('the relay source drops empty messages before forwarding and acks them', async () => {
    const { readFile } = await import('node:fs/promises');
    const src = await readFile('src/api/node-ingest.ts', 'utf8');
    // The guard must sit BEFORE the forward and answer ok (an ack the agent
    // dequeues on), not a 5xx (which it retries forever).
    const guard = src.indexOf('pagerNoiseReason(parsed.message)');
    const forward = src.indexOf('/api/messages');
    expect(guard).toBeGreaterThan(-1);
    expect(forward).toBeGreaterThan(-1);
    expect(guard).toBeLessThan(forward);
    // And the belt: a Pagermon validation rejection is acked as dropped, not
    // relayed back as retryable.
    expect(src).toContain('address or message missing');
    expect(src).toContain("dropped: 'rejected by pagermon'");
  });
});
