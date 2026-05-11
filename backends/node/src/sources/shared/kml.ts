/**
 * Tiny KML helper for power-outage feeds (Essential Energy).
 *
 * Essential's `current.kml` and `future.kml` both follow the same
 * shape: a flat list of <Placemark> entries with:
 *   - id attribute             — incident id
 *   - <name>                   — placemark label (suburb / locality)
 *   - <styleUrl>               — contains "planned" or "unplanned"
 *   - <description> CDATA HTML — has Time Off, Est. Time On, Customers
 *                                affected, Reason, Last Updated rows
 *   - <Point><coordinates>     — "lon,lat[,alt]"
 *   - <Polygon>                — outer ring coordinates as
 *                                "lon,lat lon,lat ..."
 *
 * fast-xml-parser handles the XML; the description blob is plain
 * HTML so we lift fields out with regex (same approach the Python
 * backend takes — there's no real DOM in there).
 *
 * Pure functions only: no I/O, no logging. The fetch lives in
 * src/sources/essential.ts; this file just turns bytes into objects.
 */
import { XMLParser } from 'fast-xml-parser';

export interface KmlPlacemark {
  /** Value of the Placemark's `id` attribute (e.g. "INCD-118773-r"). */
  id: string;
  /** Inner text of <name>, trimmed. Empty string if absent. */
  name: string;
  /** Inner text of <styleUrl>, lowercased. Empty if absent. */
  styleUrl: string;
  /** Inner text of <description>, raw HTML. Empty if absent. */
  description: string;
  /** First Point's coordinates as [lon, lat], or null. */
  point: [number, number] | null;
  /** First Polygon's outer ring as [[lon, lat], ...]. Empty if none. */
  polygon: Array<[number, number]>;
}

// fast-xml-parser config: keep attributes flat (no $ prefix), preserve
// CDATA for <description> blocks (KML wraps the HTML in CDATA), and
// don't auto-coerce numeric strings — coordinate strings need to stay
// as text so we can split them ourselves.
const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  attributesGroupName: '@_attrs',
  parseAttributeValue: false,
  parseTagValue: false,
  trimValues: true,
  cdataPropName: '__cdata',
  // Always coerce known repeating tags into arrays even when there's
  // only one — saves the caller from "is it array? is it object?"
  // checks at every level.
  isArray: (name) => name === 'Placemark' || name === 'Polygon' || name === 'Point',
});

interface XmlNode {
  [key: string]: unknown;
}

function getString(node: unknown): string {
  if (typeof node === 'string') return node;
  if (typeof node === 'number') return String(node);
  if (node && typeof node === 'object') {
    const o = node as XmlNode;
    if (typeof o['__cdata'] === 'string') return o['__cdata'] as string;
    if (typeof o['#text'] === 'string') return o['#text'] as string;
  }
  return '';
}

function getAttr(node: unknown, key: string): string {
  if (node && typeof node === 'object') {
    const attrs = (node as XmlNode)['@_attrs'];
    if (attrs && typeof attrs === 'object') {
      const v = (attrs as XmlNode)[key];
      if (typeof v === 'string') return v;
    }
  }
  return '';
}

/** Walk into a child by name; returns the (possibly array) child or undefined. */
function child(node: unknown, key: string): unknown {
  if (node && typeof node === 'object') {
    return (node as XmlNode)[key];
  }
  return undefined;
}

/** Find the first descendant matching `key` via DFS. */
function deepFind(node: unknown, key: string): unknown {
  if (!node || typeof node !== 'object') return undefined;
  const obj = node as XmlNode;
  if (key in obj) {
    const v = obj[key];
    return Array.isArray(v) ? v[0] : v;
  }
  for (const k of Object.keys(obj)) {
    if (k === '@_attrs') continue;
    const child = obj[k];
    if (Array.isArray(child)) {
      for (const c of child) {
        const found = deepFind(c, key);
        if (found !== undefined) return found;
      }
    } else if (child && typeof child === 'object') {
      const found = deepFind(child, key);
      if (found !== undefined) return found;
    }
  }
  return undefined;
}

function parsePoint(coordsText: string): [number, number] | null {
  // KML coordinates are "lon,lat" (and sometimes ",alt"). Whitespace
  // can wrap; split on the first comma pair.
  const t = coordsText.trim();
  if (!t) return null;
  const parts = t.split(',');
  if (parts.length < 2) return null;
  const lon = Number(parts[0]);
  const lat = Number(parts[1]);
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null;
  return [lon, lat];
}

function parsePolygon(coordsText: string): Array<[number, number]> {
  // Polygon outer ring is a whitespace-separated list of "lon,lat[,alt]"
  // tuples. Split on any whitespace, then each tuple by comma.
  const out: Array<[number, number]> = [];
  for (const tok of coordsText.trim().split(/\s+/)) {
    if (!tok) continue;
    const parts = tok.split(',');
    if (parts.length < 2) continue;
    const lon = Number(parts[0]);
    const lat = Number(parts[1]);
    if (Number.isFinite(lon) && Number.isFinite(lat)) {
      out.push([lon, lat]);
    }
  }
  return out;
}

/**
 * Parse a KML document and return one record per <Placemark>. Schemas
 * that don't match the expected shape simply yield empty fields rather
 * than throw — the fetcher already deals with malformed feeds upstream.
 */
export function parseKmlPlacemarks(xml: string): KmlPlacemark[] {
  const doc = parser.parse(xml) as XmlNode;
  // KML root: <kml><Document><Placemark/>... or <kml><Folder>... or
  // sometimes just <Document>. Walk into common containers.
  let placemarks: unknown = deepFind(doc, 'Placemark');
  // deepFind returns the *first* placemark; we actually want the
  // array. Re-fetch from the parent that has the array.
  // Easier: search the tree for the first object whose `Placemark`
  // child is an array.
  const arr = collectPlacemarks(doc);
  if (arr.length === 0 && placemarks !== undefined) {
    // Single placemark file — fall back to wrapping the one we found.
    return [extractPlacemark(placemarks)];
  }
  return arr.map(extractPlacemark);
}

function collectPlacemarks(node: unknown): unknown[] {
  if (!node || typeof node !== 'object') return [];
  const obj = node as XmlNode;
  const here = obj['Placemark'];
  if (Array.isArray(here)) return here;
  // Walk children in insertion order so the first match wins; KML
  // documents only have one canonical Placemark list.
  for (const k of Object.keys(obj)) {
    if (k === '@_attrs' || k === 'Placemark') continue;
    const v = obj[k];
    if (Array.isArray(v)) {
      for (const c of v) {
        const r = collectPlacemarks(c);
        if (r.length > 0) return r;
      }
    } else if (v && typeof v === 'object') {
      const r = collectPlacemarks(v);
      if (r.length > 0) return r;
    }
  }
  return [];
}

function extractPlacemark(pm: unknown): KmlPlacemark {
  const id = getAttr(pm, 'id');
  const name = getString(child(pm, 'name')).trim();
  const styleUrl = getString(child(pm, 'styleUrl')).toLowerCase();
  const description = getString(child(pm, 'description'));

  // Point — sometimes wrapped under MultiGeometry, sometimes directly.
  const pointNode = deepFind(pm, 'Point');
  let point: [number, number] | null = null;
  if (pointNode) {
    const coordsText = getString(child(pointNode, 'coordinates'));
    point = parsePoint(coordsText);
  }

  // Polygon outer ring. KML structure is
  // Polygon > outerBoundaryIs > LinearRing > coordinates.
  const polyNode = deepFind(pm, 'Polygon');
  let polygon: Array<[number, number]> = [];
  if (polyNode) {
    // Try outerBoundaryIs > LinearRing > coordinates first.
    const outer = child(polyNode, 'outerBoundaryIs');
    let coordsText = '';
    if (outer) {
      const ring = child(outer, 'LinearRing');
      if (ring) {
        coordsText = getString(child(ring, 'coordinates'));
      }
    }
    if (!coordsText) {
      // Fallback: any descendant <coordinates>.
      const found = deepFind(polyNode, 'coordinates');
      coordsText = getString(found);
    }
    polygon = parsePolygon(coordsText);
  }

  return { id, name, styleUrl, description, point, polygon };
}
