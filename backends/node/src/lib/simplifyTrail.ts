/**
 * Douglas-Peucker on [t, lat, lon, alt] points, by lat/lon distance.
 *
 * Shared by the live trails (sources/adsb.ts) and the track archive
 * (services/adsbTrackArchive.ts). It lives here because the poller imports the
 * archive at runtime, so the archive importing the poller back would close a
 * cycle.
 */
export type TrackPoint = [number, number, number, number | null];

/** Douglas-Peucker on [t, lat, lon, alt] points (lat/lon distance). */
export function simplifyTrail<P extends TrackPoint>(pts: P[], epsilon: number): P[] {
  if (pts.length <= 2) return pts;
  const keep = new Array<boolean>(pts.length).fill(false);
  keep[0] = keep[pts.length - 1] = true;
  const stack: Array<[number, number]> = [[0, pts.length - 1]];
  while (stack.length) {
    const seg = stack.pop() as [number, number];
    const a = pts[seg[0]] as P;
    const b = pts[seg[1]] as P;
    let maxD = 0;
    let maxI = -1;
    const dx = b[2] - a[2];
    const dy = b[1] - a[1];
    const len2 = dx * dx + dy * dy;
    for (let i = seg[0] + 1; i < seg[1]; i++) {
      const p = pts[i] as P;
      let d: number;
      if (len2 === 0) {
        const ex = p[2] - a[2];
        const ey = p[1] - a[1];
        d = Math.sqrt(ex * ex + ey * ey);
      } else {
        const t = ((p[2] - a[2]) * dx + (p[1] - a[1]) * dy) / len2;
        const cx = a[2] + Math.max(0, Math.min(1, t)) * dx;
        const cy = a[1] + Math.max(0, Math.min(1, t)) * dy;
        const ex = p[2] - cx;
        const ey = p[1] - cy;
        d = Math.sqrt(ex * ex + ey * ey);
      }
      if (d > maxD) {
        maxD = d;
        maxI = i;
      }
    }
    if (maxD > epsilon && maxI !== -1) {
      keep[maxI] = true;
      stack.push([seg[0], maxI], [maxI, seg[1]]);
    }
  }
  return pts.filter((_, i) => keep[i]);
}
