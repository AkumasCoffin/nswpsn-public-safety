// Candidate GRN sites for a radio node's RF site survey.
//
// A node knows its LGA (the location model requires it); the GRN dataset
// knows every site's coordinates and control-channel frequency. This module
// joins the two: which sites sit in the node's LGA and its neighbouring LGAs
// (to a chosen ring depth), and what frequency should the survey test.
//
// Three derived facts, each computed once and cached for the process:
//   - site → LGA: ray-cast of each GRN site's lat/lon against the ABS LGA
//     polygons (boundaryForPoint — the incident-geo machinery).
//   - LGA adjacency: adjacent ABS polygons share exact vertices (the source
//     is generalised at a fixed offset, so a shared border generalises
//     identically on both sides). Hash every vertex at 5dp; two LGA names on
//     one vertex are neighbours. Verified against the real NSW import: all
//     132 councils correct, Lord Howe correctly isolated, ~57ms.
//   - control frequency: the GRN 'Control Channel' field is free text. 590 of
//     665 are a bare MHz; the rest are ranges, annotations or placeholders
//     ("TBA", "Pending Confirmation"). First plausible MHz match wins;
//     no match = the site is reported as skipped, never guessed.
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { boundaryForPoint } from '../api/boundaries.js';

export interface SurveyCandidate {
  name: string;
  grnKey: string | null;
  mhz: number;
  altMhz: number | null;
  lga: string;
}

export interface SkippedSite {
  name: string;
  grnKey: string | null;
  note: string;
}

/** Parse a control-channel frequency out of GRN free text. Exported for tests. */
export function parseControlMhz(raw: unknown): number | null {
  if (typeof raw !== 'string') return null;
  const m = /\d{3}\.\d{3,6}/.exec(raw);
  if (!m) return null;
  const mhz = Number(m[0]);
  // The GRN lives in the 400MHz government band; anything else is a typo or
  // a mis-parsed annotation and must not be tuned.
  if (!Number.isFinite(mhz) || mhz < 100 || mhz > 1000) return null;
  return mhz;
}

interface GrnSiteRow {
  id: number;
  data: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// site → LGA tagging (cached)
// ---------------------------------------------------------------------------

export interface TaggedSite {
  name: string;
  grnKey: string | null;
  lga: string | null;
  mhz: number | null;
  altMhz: number | null;
  rawCc: string;
  lat: number | null;
  lon: number | null;
  /** The dataset's own system name, for telling two same-named sites apart. */
  system: string | null;
}

let _tagged: { at: number; sites: TaggedSite[] } | null = null;
const TAG_TTL_MS = 6 * 3600_000; // grn_sites is ~static; owner edits are rare

async function taggedSites(): Promise<TaggedSite[]> {
  if (_tagged && Date.now() - _tagged.at < TAG_TTL_MS) return _tagged.sites;
  const pool = await getPool();
  if (!pool) return _tagged?.sites ?? [];
  const r = await pool.query<GrnSiteRow>('SELECT id, data FROM grn_sites');
  const sites: TaggedSite[] = [];
  for (const row of r.rows) {
    const d = row.data ?? {};
    const name = typeof d['NAME'] === 'string' ? (d['NAME'] as string).trim() : '';
    if (!name) continue;
    const lat = Number(d['Latitude']);
    const lon = Number(d['Longitude']);
    let lga: string | null = null;
    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      const b = await boundaryForPoint('lga', lon, lat);
      lga = b?.name ?? null;
    }
    const rawCc = typeof d['Control Channel'] === 'string' ? (d['Control Channel'] as string) : '';
    sites.push({
      name,
      grnKey: typeof d['GRN Site ID #'] === 'string' ? (d['GRN Site ID #'] as string) : null,
      lga,
      mhz: parseControlMhz(d['Control Channel']),
      altMhz: parseControlMhz(d['Alt Control Channel']),
      rawCc,
      lat: Number.isFinite(lat) ? lat : null,
      lon: Number.isFinite(lon) ? lon : null,
      system: typeof d['SYSTEM NAME'] === 'string' ? (d['SYSTEM NAME'] as string) : null,
    });
  }
  _tagged = { at: Date.now(), sites };
  log.info({ sites: sites.length, withLga: sites.filter((s) => s.lga).length }, 'grn sites tagged by LGA');
  return sites;
}

// ---------------------------------------------------------------------------
// LGA adjacency (cached per state)
// ---------------------------------------------------------------------------

const _adjacency = new Map<string, Map<string, Set<string>>>();

function collectVertices(geom: unknown, out: (lon: number, lat: number) => void): void {
  const g = geom as { type?: string; coordinates?: unknown };
  const walkRing = (ring: unknown) => {
    if (!Array.isArray(ring)) return;
    for (const pt of ring) {
      if (Array.isArray(pt) && pt.length >= 2) out(Number(pt[0]), Number(pt[1]));
    }
  };
  const walkPolygon = (rings: unknown) => {
    if (Array.isArray(rings)) for (const ring of rings) walkRing(ring);
  };
  if (g?.type === 'Polygon') walkPolygon(g.coordinates);
  else if (g?.type === 'MultiPolygon' && Array.isArray(g.coordinates)) {
    for (const poly of g.coordinates) walkPolygon(poly);
  }
}

/** name -> neighbouring LGA names, for one state. Exported for tests. */
export async function lgaAdjacency(state: string): Promise<Map<string, Set<string>>> {
  const cached = _adjacency.get(state);
  if (cached) return cached;
  const adj = new Map<string, Set<string>>();
  const pool = await getPool();
  if (!pool) return adj;
  const r = await pool.query<{ name: string; geom: unknown }>(
    `SELECT name, geom FROM boundaries WHERE kind = 'lga' AND state = $1`,
    [state],
  );
  // vertex key -> set of LGA names seen at that vertex
  const byVertex = new Map<string, string[]>();
  for (const row of r.rows) {
    collectVertices(row.geom, (lon, lat) => {
      const key = `${lon.toFixed(5)},${lat.toFixed(5)}`;
      const names = byVertex.get(key);
      if (!names) byVertex.set(key, [row.name]);
      else if (!names.includes(row.name)) names.push(row.name);
    });
  }
  for (const names of byVertex.values()) {
    if (names.length < 2) continue;
    for (const a of names) {
      for (const b of names) {
        if (a === b) continue;
        let set = adj.get(a);
        if (!set) adj.set(a, (set = new Set()));
        set.add(b);
      }
    }
  }
  _adjacency.set(state, adj);
  log.info({ state, lgas: adj.size }, 'lga adjacency built');
  return adj;
}

/**
 * How many LGA borders away each area is from the node's own — 0 for its own
 * LGA, 1 for a direct neighbour, and so on out to `rings`. Areas further than
 * that (or unreachable, like an island council) are simply absent.
 */
export async function lgaRingDepths(state: string, lga: string, rings: number): Promise<Map<string, number>> {
  const adj = await lgaAdjacency(state);
  const maxDepth = Math.max(1, Math.min(4, Math.trunc(rings) || 1));
  const depths = new Map<string, number>([[lga, 0]]);
  let frontier = [lga];
  for (let hop = 1; hop <= maxDepth; hop++) {
    const next: string[] = [];
    for (const name of frontier) {
      for (const n of adj.get(name) ?? []) {
        if (!depths.has(n)) {
          depths.set(n, hop);
          next.push(n);
        }
      }
    }
    frontier = next;
    if (frontier.length === 0) break;
  }
  return depths;
}

/** The node's LGA plus neighbours out to `rings` hops. */
export async function lgaNeighbourhood(state: string, lga: string, rings: number): Promise<Set<string>> {
  return new Set((await lgaRingDepths(state, lga, rings)).keys());
}

/** Every GRN site, tagged with its LGA and parsed control frequency. */
export async function allTaggedSites(): Promise<TaggedSite[]> {
  return taggedSites();
}

// ---------------------------------------------------------------------------
// candidates
// ---------------------------------------------------------------------------

export async function candidatesForNode(
  node: { state: string | null; lga: string | null },
  rings = 1,
): Promise<{ candidates: SurveyCandidate[]; skipped: SkippedSite[] }> {
  const candidates: SurveyCandidate[] = [];
  const skipped: SkippedSite[] = [];
  if (!node.lga) return { candidates, skipped };
  const state = node.state ?? 'NSW';
  const area = await lgaNeighbourhood(state, node.lga, rings);
  const sites = await taggedSites();
  for (const s of sites) {
    if (!s.lga || !area.has(s.lga)) continue;
    if (s.mhz === null) {
      skipped.push({
        name: s.name,
        grnKey: s.grnKey,
        note: s.rawCc ? `control channel not parseable: "${s.rawCc.slice(0, 60)}"` : 'no control channel listed',
      });
      continue;
    }
    candidates.push({ name: s.name, grnKey: s.grnKey, mhz: s.mhz, altMhz: s.altMhz, lga: s.lga });
  }
  // Stable, nearest-first-ish: the node's own LGA first, then by name.
  candidates.sort((a, b) =>
    (a.lga === node.lga ? 0 : 1) - (b.lga === node.lga ? 0 : 1) || a.name.localeCompare(b.name),
  );
  return { candidates, skipped };
}

/** Test seams. */
export function _resetGrnCandidateCaches(): void {
  _tagged = null;
  _adjacency.clear();
}
