/**
 * Google's encoded polyline format, precision 5 — what the Live Traffic NSW
 * feeds give for a record's road, and what map.html's decodePolyline reads.
 * The interstate road sources (sources/interstateRoads.ts) receive their
 * roads as coordinate lists and encode them, so every road record carries
 * its line the same way.
 */

function encodeValue(v: number): string {
  let n = v < 0 ? ~(v << 1) : v << 1;
  let out = '';
  while (n >= 0x20) {
    out += String.fromCharCode((0x20 | (n & 0x1f)) + 63);
    n >>= 5;
  }
  return out + String.fromCharCode(n + 63);
}

/** A line of [longitude, latitude] points (GeoJSON order), encoded. Empty for fewer than two points. */
export function encodePolyline(points: ReadonlyArray<readonly number[]>): string {
  const usable = points.filter(
    (p) => p.length >= 2 && Number.isFinite(p[0]) && Number.isFinite(p[1]),
  );
  if (usable.length < 2) return '';
  let lat = 0;
  let lon = 0;
  let out = '';
  for (const p of usable) {
    const la = Math.round(p[1]! * 1e5);
    const lo = Math.round(p[0]! * 1e5);
    out += encodeValue(la - lat) + encodeValue(lo - lon);
    lat = la;
    lon = lo;
  }
  return out;
}

/** The reverse, as [longitude, latitude] points — for tests and checks. */
export function decodePolyline(str: string): Array<[number, number]> {
  const pts: Array<[number, number]> = [];
  let index = 0;
  let lat = 0;
  let lon = 0;
  const next = (): number => {
    let b: number;
    let shift = 0;
    let result = 0;
    do {
      b = str.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20);
    return result & 1 ? ~(result >> 1) : result >> 1;
  };
  while (index < str.length) {
    lat += next();
    lon += next();
    pts.push([lon / 1e5, lat / 1e5]);
  }
  return pts;
}
