/**
 * Cutting a receiver's trace into hour rows (services/nodes/nodeTrackArchive.ts).
 *
 * The per-receiver archive keeps one row per aircraft per hour and rewrites
 * only the current hour and the one just closed. These tests moved here with
 * the slicer when the global history moved to minute rows (migration 139).
 */
import { describe, it, expect, vi } from 'vitest';

vi.mock('../../../src/db/pool.js', () => ({
  getPool: vi.fn(() => Promise.resolve(null)),
  closePool: vi.fn(),
}));

const HOUR = 3_600_000;
/** 2026-09-17T04:00:00Z — an exact hour boundary, so the arithmetic is legible. */
const H0 = Date.UTC(2026, 8, 17, 4, 0, 0);

const { sliceTrailByHour } = await import('../../../src/services/nodes/nodeTrackArchive.js');

const pt = (ms: number, lat: number, lon: number, alt: number | null = 30000) =>
  [ms, lat, lon, alt] as [number, number, number, number | null];

describe('cutting a trail into hours', () => {
  it('splits one flight across the hour boundary', () => {
    const points = [
      pt(H0 + 50 * 60_000, -33.0, 151.0),   // 04:50
      pt(H0 + 58 * 60_000, -33.2, 151.2),   // 04:58
      pt(H0 + 62 * 60_000, -33.4, 151.4),   // 05:02
    ];
    const slices = sliceTrailByHour('abc123', points, H0 + 65 * 60_000);
    expect(slices).toHaveLength(2);

    const first = slices.find((s) => s.hourMs === H0)!;
    const second = slices.find((s) => s.hourMs === H0 + HOUR)!;
    expect(first.points).toHaveLength(2);
    expect(second.points).toHaveLength(1);
    // Offsets are seconds INTO their own hour, not into the flight.
    expect(first.points[0]![0]).toBe(50 * 60);
    expect(second.points[0]![0]).toBe(2 * 60);
  });

  it('leaves hours older than the last one alone', () => {
    // A long flight rewrites only the hour it is in (plus the one just
    // closed), not its whole track, every minute.
    const points = [
      pt(H0 - 3 * HOUR, -31.0, 149.0),
      pt(H0 - 2 * HOUR, -32.0, 150.0),
      pt(H0 - HOUR + 60_000, -32.5, 150.5),
      pt(H0 + 60_000, -33.0, 151.0),
    ];
    const slices = sliceTrailByHour('abc123', points, H0 + 120_000);
    expect(slices.map((s) => s.hourMs).sort()).toEqual([H0 - HOUR, H0]);
    // A ten-hour flight costs two rows a flush, not ten.
    expect(slices).toHaveLength(2);
  });

  it('still writes the hour that just closed', () => {
    // A flush landing after the top of the hour would otherwise leave the
    // closed hour permanently missing its final minutes.
    const points = [pt(H0 + 59 * 60_000, -33.0, 151.0)];
    const slices = sliceTrailByHour('abc123', points, H0 + 61 * 60_000);
    expect(slices.map((s) => s.hourMs)).toEqual([H0]);
  });

  it('reports the real span of each slice', () => {
    const points = [pt(H0 + 600_000, -33, 151), pt(H0 + 1_200_000, -33.1, 151.1)];
    const [s] = sliceTrailByHour('abc123', points, H0 + 1_300_000);
    expect(s!.firstMs).toBe(H0 + 600_000);
    expect(s!.lastMs).toBe(H0 + 1_200_000);
  });

  it('keeps a single-point slice', () => {
    // Enough to say the aircraft was there, which is what a coverage picture
    // is for even when it is not enough to draw a line.
    expect(sliceTrailByHour('abc123', [pt(H0 + 60_000, -33, 151)], H0 + 120_000))
      .toHaveLength(1);
  });
});
