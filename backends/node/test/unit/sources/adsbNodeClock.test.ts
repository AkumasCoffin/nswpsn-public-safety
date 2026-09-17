/**
 * A node's uploads are timestamped by the node, not by us.
 *
 * The reported fault: aircraft received by a node sometimes did not update, or
 * appeared late. The cause was treating (our clock - their clock) as transit.
 * It is transit plus however far their clock is out, and that error is
 * unbounded — a receiver a minute behind made every record it sent look a
 * minute old, so they lost the freshest-wins merge to whatever an aggregator
 * had, and past MAX_AGE_SEC they were dropped before reaching the map at all.
 *
 * The node was working perfectly the whole time, which is what made it hard to
 * see from the outside.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import {
  nodeTransitSec,
  _resetNodeClock,
  normalizeNodeUpload,
} from '../../../src/sources/adsb.js';

const NOW = Date.UTC(2026, 8, 17, 6, 0, 0);
const NODE = 'node:adsb-test';

/** One upload's worth of clock deltas, as a node sending every 5s produces. */
function feedUploads(sourceId: string, skewMs: number, transits: number[]) {
  const out: number[] = [];
  transits.forEach((tMs, i) => {
    const now = NOW + i * 5_000;
    // The node stamps its own clock, which is `skewMs` out.
    const sent = now - tMs - skewMs;
    out.push(nodeTransitSec(sourceId, sent, now));
  });
  return out;
}

beforeEach(() => {
  _resetNodeClock();
});

describe('measuring how late an upload really is', () => {
  it('reads a prompt upload as prompt however wrong the clock is', () => {
    // 90 seconds behind: every record used to gain 90s of apparent age, which
    // is past the 60s cutoff, so the node fed the map nothing at all.
    const got = feedUploads(NODE, 90_000, [50, 60, 40, 55, 45]);
    for (const v of got) expect(v).toBeLessThan(1);
  });

  it('works the same for a clock running fast', () => {
    // The old guard zeroed transit whenever the node's stamp was in our
    // future, so a fast clock hid real delay instead of correcting for it.
    const got = feedUploads(NODE, -45_000, [50, 60, 40, 55, 45]);
    for (const v of got) expect(v).toBeLessThan(1);
  });

  it('still reports a genuinely delayed upload', () => {
    // The whole point of the figure: a snapshot held by a retry carries stale
    // positions and must not beat a current aggregator record in the merge.
    feedUploads(NODE, 30_000, [50, 60, 40, 55, 45]);
    const late = nodeTransitSec(NODE, NOW + 30_000 - 30_000 - 45_000, NOW + 30_000);
    expect(late).toBeGreaterThan(40);
    expect(late).toBeLessThan(50);
  });

  it('never returns a negative age', () => {
    // An upload cannot arrive before it was sent, whatever the arithmetic of
    // two disagreeing clocks says.
    expect(nodeTransitSec(NODE, NOW + 10_000, NOW)).toBe(0);
  });

  it('treats a node’s first upload as prompt', () => {
    // Nothing to compare against yet, and guessing high would drop its first
    // minute of aircraft.
    expect(nodeTransitSec('node:brand-new', NOW - 120_000, NOW)).toBe(0);
  });

  it('follows the clock when it is corrected', () => {
    // An NTP step must not leave the node permanently credited with the old
    // offset, or every upload after it reads as early and then as late.
    feedUploads(NODE, 60_000, Array(32).fill(50));
    // Clock jumps to correct; keep sending for a full window.
    const after = feedUploads(NODE, 0, Array(32).fill(50));
    expect(after[after.length - 1]).toBeLessThan(1);
  });

  it('keeps each node on its own clock', () => {
    // One node's skew must not be charged to another's records.
    feedUploads('node:a', 90_000, [50, 50, 50]);
    const b = feedUploads('node:b', 0, [50, 50, 50]);
    for (const v of b) expect(v).toBeLessThan(1);
  });
});

describe('what reaches the map', () => {
  const upload = (atMs: number) => ({
    at: new Date(atMs).toISOString(),
    aircraft: [{ hex: 'abc123', lat: -33.8, lon: 151.2, seen_pos: 2 }],
  });

  it('keeps the aircraft of a node whose clock is badly wrong', () => {
    // MAX_AGE_SEC is 60, so a two-minute skew used to discard every record
    // here and the receiver silently contributed nothing.
    const skew = 120_000;
    let out: ReturnType<typeof normalizeNodeUpload> = [];
    for (let i = 0; i < 5; i += 1) {
      const now = NOW + i * 5_000;
      out = normalizeNodeUpload(upload(now - skew), NODE, now);
    }
    expect(out).toHaveLength(1);
    // Only the decoder's own seen_pos is left, which is the honest figure.
    expect(out[0]!.ageSec).toBeCloseTo(2, 0);
  });

  it('does not make a node look fresher than the decoder said', () => {
    // Correcting the clock must not erase the age the decoder itself reported.
    const out = normalizeNodeUpload(
      { at: new Date(NOW).toISOString(),
        aircraft: [{ hex: 'abc123', lat: -33.8, lon: 151.2, seen_pos: 25 }] },
      'node:fresh', NOW);
    expect(out[0]!.ageSec).toBeGreaterThanOrEqual(25);
  });
});
