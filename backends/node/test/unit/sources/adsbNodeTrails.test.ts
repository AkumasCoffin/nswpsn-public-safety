/**
 * The positions a node carries between its uploads.
 *
 * A transponder reports position about twice a second, but dump1090's
 * aircraft.json only ever holds each aircraft's CURRENT state, so the agent
 * reading it once per upload threw away everything in between — and the public
 * trail could never be denser than the poll that sampled it, whatever the
 * receiver actually heard. The agent now reads every second and ships what it
 * saw; this is the half that puts those points on the map.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import {
  normalizeNodeUploadWithTrails,
  ingestNodeTrailPoints,
  adsbTrailsForArchive,
  _resetAdsbTrailsForTests,
  _resetNodeClock,
} from '../../../src/sources/adsb.js';

const NOW = Date.UTC(2026, 8, 17, 7, 0, 0);
const SRC = 'node:adsb-test';

const upload = (atMs: number, positions?: Array<[number, number, number, number | null]>) => ({
  at: new Date(atMs).toISOString(),
  aircraft: [{
    hex: 'abc123', lat: -33.5, lon: 151.5, seen_pos: 1,
    ...(positions ? { positions } : {}),
  }],
});

/** The live buffer, timestamps intact.
 *
 *  Not the served snapshot: that is rebuilt once per poll so requests do no
 *  work, which means points added by an upload land in it on the poller's next
 *  pass rather than the instant they arrive. */
const trailFor = (hex: string) =>
  adsbTrailsForArchive().find((t) => t.hex === hex)?.points;

beforeEach(() => {
  _resetAdsbTrailsForTests();
  _resetNodeClock();
});

describe('reading the carried positions off an upload', () => {
  it('times them against the snapshot, not against our clock', () => {
    // The ages are relative to `at`, exactly like seen_pos, so the correction
    // for a node's wrong clock covers them without knowing they exist.
    const { trails } = normalizeNodeUploadWithTrails(
      upload(NOW, [[4, -33.0, 151.0, 3000], [2, -33.2, 151.2, 3100]]), SRC, NOW);
    const pts = trails.get('abc123')!;
    expect(pts).toHaveLength(2);
    expect(pts[0]![0]).toBe(NOW - 4000);
    expect(pts[1]![0]).toBe(NOW - 2000);
  });

  it('is unmoved by a node whose clock is badly wrong', () => {
    // Feed enough uploads for the clock estimate to settle, then check the
    // carried points land at the same real instants as the aircraft itself.
    const skew = 90_000;
    for (let i = 0; i < 5; i += 1) {
      normalizeNodeUploadWithTrails(upload(NOW + i * 5_000 - skew), SRC, NOW + i * 5_000);
    }
    const at = NOW + 25_000;
    const { trails } = normalizeNodeUploadWithTrails(
      upload(at - skew, [[4, -33.0, 151.0, null]]), SRC, at);
    expect(trails.get('abc123')![0]![0]).toBeCloseTo(at - 4000, -2);
  });

  it('sorts them oldest first whatever order they arrived in', () => {
    const { trails } = normalizeNodeUploadWithTrails(
      upload(NOW, [[1, -33.3, 151.3, null], [5, -33.0, 151.0, null]]), SRC, NOW);
    const pts = trails.get('abc123')!;
    expect(pts[0]![0]).toBeLessThan(pts[1]![0]);
  });

  it('carries nothing when the agent sent nothing', () => {
    // Every node on an agent before 0.1.8, and any aircraft that produced only
    // the one position already on the record.
    const { records, trails } = normalizeNodeUploadWithTrails(upload(NOW), SRC, NOW);
    expect(records).toHaveLength(1);
    expect(trails.size).toBe(0);
  });

  it('ignores a malformed point rather than the whole upload', () => {
    const { records, trails } = normalizeNodeUploadWithTrails(
      upload(NOW, [
        [Number.NaN, -33, 151, null],
        [2, -33.2, 151.2, null],
      ] as Array<[number, number, number, number | null]>), SRC, NOW);
    expect(records).toHaveLength(1);
    expect(trails.get('abc123')).toHaveLength(1);
  });
});

describe('putting them on the map', () => {
  it('builds a denser trail than the poller alone could', () => {
    ingestNodeTrailPoints('abc123', [
      [NOW - 4000, -33.0, 151.0, 3000],
      [NOW - 3000, -33.1, 151.1, 3000],
      [NOW - 2000, -33.2, 151.2, 3000],
      [NOW - 1000, -33.3, 151.3, 3000],
    ], NOW);
    expect(trailFor('abc123')).toHaveLength(4);
  });

  it('keeps the points in time order', () => {
    ingestNodeTrailPoints('abc123', [
      [NOW - 3000, -33.1, 151.1, null],
      [NOW - 1000, -33.3, 151.3, null],
    ], NOW);
    const t = trailFor('abc123')!;
    expect(t[0]![0]).toBeLessThan(t[1]![0]);
  });

  it('merges points older than what is already held', () => {
    // The poller stamps a point when it LOOKED; these are stamped when the fix
    // was taken, so they routinely land behind it. Appending blindly would put
    // the trail out of order; dropping them would lose the density.
    ingestNodeTrailPoints('abc123', [[NOW - 1000, -33.9, 151.9, null]], NOW);
    ingestNodeTrailPoints('abc123', [
      [NOW - 4000, -33.0, 151.0, null],
      [NOW - 3000, -33.1, 151.1, null],
    ], NOW);
    const t = trailFor('abc123')!;
    expect(t).toHaveLength(3);
    const times = t.map((p) => p[0]);
    expect([...times].sort((a, b) => a - b)).toEqual(times);
  });

  it('counts one fix once, however many ways it arrives', () => {
    // The same position can reach us through the poller and through a node.
    ingestNodeTrailPoints('abc123', [[NOW - 2000, -33.2, 151.2, null]], NOW);
    ingestNodeTrailPoints('abc123', [[NOW - 2000, -33.2, 151.2, null]], NOW);
    ingestNodeTrailPoints('abc123', [[NOW - 1900, -33.2, 151.2, null]], NOW);
    expect(trailFor('abc123')).toHaveLength(1);
  });

  it('refuses a point from the future', () => {
    ingestNodeTrailPoints('abc123', [[NOW + 60_000, -33, 151, null]], NOW);
    expect(trailFor('abc123')).toBeUndefined();
  });

  it('refuses a point older than the trail window', () => {
    ingestNodeTrailPoints('abc123', [[NOW - 60 * 60_000, -33, 151, null]], NOW);
    expect(trailFor('abc123')).toBeUndefined();
  });

  it('stays within its budget under a long run', () => {
    // A trail fed every second for an hour must not grow without bound just
    // because a second path now adds to it.
    for (let i = 600; i > 0; i -= 1) {
      ingestNodeTrailPoints('abc123',
        [[NOW - i * 1000, -33 + i * 0.001, 151 + i * 0.001, 3000]], NOW);
    }
    expect(trailFor('abc123')!.length).toBeLessThanOrEqual(150);
  });

  it('does nothing without a hex or without points', () => {
    ingestNodeTrailPoints('', [[NOW - 1000, -33, 151, null]], NOW);
    ingestNodeTrailPoints('abc123', [], NOW);
    expect(adsbTrailsForArchive()).toEqual([]);
  });
});
