/**
 * Australia bbox [W,S,E,N] for aircraft. The filter applies a 0.3° buffer so
 * aircraft riding the edge don't flap in/out between ticks; the API reports
 * the unbuffered box.
 *
 * Its own module so the globe-history import, which runs in a worker thread,
 * can share it without loading the live poller.
 */
export const AU_BBOX: [number, number, number, number] = [112.0, -44.3, 154.3, -9.7];
export const BBOX_BUFFER_DEG = 0.3;

export function inAuBbox(lat: number, lon: number): boolean {
  const [w, s, e, n] = AU_BBOX;
  return (
    lat >= s - BBOX_BUFFER_DEG &&
    lat <= n + BBOX_BUFFER_DEG &&
    lon >= w - BBOX_BUFFER_DEG &&
    lon <= e + BBOX_BUFFER_DEG
  );
}
