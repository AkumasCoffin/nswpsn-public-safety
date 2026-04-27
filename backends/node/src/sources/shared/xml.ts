/**
 * XML / RSS helpers built on fast-xml-parser.
 *
 * The Python backend uses xml.etree.ElementTree to walk RSS / GeoRSS / BOM
 * warning feeds. This module is the equivalent: a thin wrapper that
 * parses XML once and gives us small accessors for the only shapes we
 * actually care about (RSS-style `<item>` lists and BOM `<warning>`
 * elements). We deliberately do NOT model the whole DOM — we just need
 * enough surface area for the RFS and BOM fetchers.
 */
import { XMLParser } from 'fast-xml-parser';

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  // Always keep these as arrays so consumers don't have to handle the
  // "single item" vs "list" duality fast-xml-parser otherwise creates.
  isArray: (name) => ['item', 'warning'].includes(name),
  // Preserve the raw text inside elements containing GeoRSS coords etc.
  trimValues: true,
});

/**
 * Parsed XML node. fast-xml-parser produces nested objects whose shape
 * depends entirely on the source document, so we keep this typed as
 * `unknown` and the callers narrow as they go.
 */
export type XmlNode = Record<string, unknown>;

export function parseXml(xml: string): XmlNode {
  // Cast through unknown — fast-xml-parser declares `any` here.
  return parser.parse(xml) as unknown as XmlNode;
}

/** Coerce a maybe-array to a real array. fast-xml-parser sometimes
 *  returns a single object when there's only one match. */
export function asArray<T>(v: T | T[] | undefined | null): T[] {
  if (v === undefined || v === null) return [];
  return Array.isArray(v) ? v : [v];
}

/**
 * Read a child element's text content, defaulting to ''. Children may
 * be either a string (when the element has no attributes) or an object
 * with a '#text' property (when it does).
 */
export function textOf(node: unknown, key: string): string {
  if (node === null || typeof node !== 'object') return '';
  const v = (node as Record<string, unknown>)[key];
  return textValue(v);
}

/** Pull a string out of whatever shape fast-xml-parser produced. */
export function textValue(v: unknown): string {
  if (v === undefined || v === null) return '';
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  if (typeof v === 'object') {
    const obj = v as Record<string, unknown>;
    if ('#text' in obj) return textValue(obj['#text']);
  }
  return '';
}
