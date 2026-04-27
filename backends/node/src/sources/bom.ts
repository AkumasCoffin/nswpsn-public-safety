/**
 * BOM warnings — combined NSW land + marine warnings from the master
 * IDZ00054 XML feed (which already contains both categories).
 *
 * Mirrors python's `_fetch_all_bom_warnings` + `/api/bom/warnings`
 * route at external_api_proxy.py:5519, 5602. Same response shape:
 *   { warnings: [...], count, counts: { land, marine } }
 */
import { fetchText } from './shared/http.js';
import { asArray, parseXml, textOf } from './shared/xml.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const BOM_URL = 'https://www.bom.gov.au/fwo/IDZ00054.warnings_nsw.xml';

export type BomCategory = 'land' | 'marine';
export type BomSeverity = 'severe' | 'warning' | 'watch' | 'advice' | 'info';

export interface BomWarning {
  title: string;
  type: string;
  category: BomCategory;
  severity: BomSeverity;
  description: string;
  area: string;
  issued: string;
  expiry: string;
  link: string;
  source: 'bom';
}

export interface BomSnapshot {
  warnings: BomWarning[];
  count: number;
  counts: { land: number; marine: number };
}

const MARINE_KEYWORDS = [
  'marine', 'surf', 'coastal', 'ocean', 'sea', 'wind warning summary',
  'gale', 'storm force', 'hurricane force', 'swell', 'wave',
  'coastal waters', 'offshore', 'boating', 'shipping',
];

export function categorizeBomWarning(title: string, description = ''): BomCategory {
  const combined = `${(title || '').toLowerCase()} ${(description || '').toLowerCase()}`;
  for (const k of MARINE_KEYWORDS) {
    if (combined.includes(k)) return 'marine';
  }
  return 'land';
}

export function getBomSeverity(title: string): BomSeverity {
  const t = (title || '').toLowerCase();
  if (t.includes('severe') || t.includes('emergency') || t.includes('extreme'))
    return 'severe';
  if (t.includes('warning')) return 'warning';
  if (t.includes('watch')) return 'watch';
  if (t.includes('advice') || t.includes('summary')) return 'advice';
  return 'info';
}

export async function fetchBom(): Promise<BomSnapshot> {
  const xml = await fetchText(BOM_URL, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
  const root = parseXml(xml);

  // Python uses .findall('.//warning') first, then falls back to
  // .//item. We replicate that by walking the whole tree shallowly
  // (root keys + one level deeper) to find arrays of either tag.
  const warnings: BomWarning[] = [];
  const seenTitles = new Set<string>();

  type WarningNode = Record<string, unknown> & { '@_type'?: unknown };

  const warningNodes = collectByTag(root, 'warning') as WarningNode[];
  if (warningNodes.length > 0) {
    for (const w of warningNodes) {
      const title =
        textOf(w, 'title') || textOf(w, 'headline');
      const description = textOf(w, 'description');
      const titleKey = title.trim().toLowerCase();
      if (!titleKey || seenTitles.has(titleKey)) continue;
      seenTitles.add(titleKey);

      const category = categorizeBomWarning(title, description);
      const severity = getBomSeverity(title);

      const typeAttr = w['@_type'];
      warnings.push({
        title,
        type: typeof typeAttr === 'string' && typeAttr ? typeAttr : category,
        category,
        severity,
        description,
        area: textOf(w, 'area'),
        issued: textOf(w, 'issued') || textOf(w, 'issue-time-local'),
        expiry: textOf(w, 'expiry') || textOf(w, 'expiry-time-local'),
        link: '',
        source: 'bom',
      });
    }
  } else {
    // RSS-style fallback.
    const items = collectByTag(root, 'item');
    for (const item of items) {
      const title = textOf(item, 'title');
      const description = textOf(item, 'description');
      const titleKey = title.trim().toLowerCase();
      if (!titleKey || seenTitles.has(titleKey)) continue;
      seenTitles.add(titleKey);

      const category = categorizeBomWarning(title, description);
      const severity = getBomSeverity(title);
      warnings.push({
        title,
        type: category,
        category,
        severity,
        description,
        area: '',
        link: textOf(item, 'link'),
        issued: textOf(item, 'pubDate'),
        expiry: '',
        source: 'bom',
      });
    }
  }

  const counts = { land: 0, marine: 0 };
  for (const w of warnings) counts[w.category] += 1;

  return { warnings, count: warnings.length, counts };
}

/** Walk the parsed XML tree and find every node array under the given
 *  tag. fast-xml-parser nests into `rss > channel > item` for RSS, or
 *  `product > warnings > warning` (etc.) for BOM-flavoured XML. We do a
 *  shallow recursive scan rather than guessing the path because the
 *  exact nesting varies. */
function collectByTag(node: unknown, tag: string): Array<Record<string, unknown>> {
  const out: Array<Record<string, unknown>> = [];
  if (node === null || typeof node !== 'object') return out;
  const stack: unknown[] = [node];
  while (stack.length > 0) {
    const cur = stack.pop();
    if (cur === null || typeof cur !== 'object') continue;
    if (Array.isArray(cur)) {
      for (const v of cur) stack.push(v);
      continue;
    }
    const obj = cur as Record<string, unknown>;
    for (const [k, v] of Object.entries(obj)) {
      if (k === tag) {
        for (const item of asArray(v)) {
          if (item && typeof item === 'object') {
            out.push(item as Record<string, unknown>);
          }
        }
      } else if (v && typeof v === 'object') {
        stack.push(v);
      }
    }
  }
  return out;
}

export default function register(): void {
  registerSource<BomSnapshot>({
    name: 'bom_warnings',
    // Match python's data_history source value so archive rows align
    // with the SOURCE_TO_FAMILY map keyed under 'bom_warning'.
    archiveSource: 'bom_warning',
    family: 'misc',
    intervalActiveMs: 60_000,
    intervalIdleMs: 120_000,
    fetch: fetchBom,
  });
}

export function bomSnapshot(): BomSnapshot {
  return (
    liveStore.getData<BomSnapshot>('bom_warnings') ?? {
      warnings: [],
      count: 0,
      counts: { land: 0, marine: 0 },
    }
  );
}
