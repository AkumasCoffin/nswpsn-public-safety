/**
 * Agency reference data ("Extended" tables) — served straight from the CSV
 * source, replacing the committed agency-extended.json compile-and-commit step.
 *
 * Source of truth: data/Extended/<slug>/meta.json + one CSV per table section
 * (the same layout the old scripts/build-extended-data.py read). This module
 * loads that tree once, resolves each section's `csv` reference into a
 * {headers, rows} table, and caches the result. The API layer serves it whole
 * (/api/agency/extended) or per-agency (/api/agency/extended/:slug).
 *
 * The `csv` field is KEPT on each resolved section (the Python build deleted it)
 * because it is the stable per-agency section key the row-edit feature uses.
 */
import { readFileSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseCsvRows } from '../api/node-data.js';
import { log } from '../lib/log.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));

export interface AgencyTable {
  headers: string[];
  rows: string[][];
}

export interface AgencySection {
  type?: string;
  title?: string;
  csv?: string;
  table?: AgencyTable;
  groups?: Array<Record<string, unknown> & { csv?: string; table?: AgencyTable }>;
  [k: string]: unknown;
}

export interface AgencyExtended {
  title: string;
  tag: string;
  badges: unknown[];
  overview: unknown[];
  sections: AgencySection[];
}

/** Candidate locations for data/Extended, mirroring node-data's data-path resolution. */
function extendedDirCandidates(): string[] {
  const env = (process.env['AGENCY_EXTENDED_DIR'] ?? '').trim();
  const rel = 'data/Extended';
  return [
    ...(env ? [env] : []),
    path.resolve(HERE, '../../../..', rel), // dist/services|src/services → repo root
    path.resolve(process.cwd(), '../..', rel), // cwd = backends/node
    path.resolve(process.cwd(), rel), // cwd = repo root
  ];
}

let _extendedDir: string | null | undefined;
function extendedDir(): string | null {
  if (_extendedDir !== undefined) return _extendedDir;
  _extendedDir = null;
  for (const c of extendedDirCandidates()) {
    try {
      if (statSync(c).isDirectory()) {
        _extendedDir = c;
        break;
      }
    } catch {
      /* try next candidate */
    }
  }
  if (!_extendedDir) {
    log.warn({ candidates: extendedDirCandidates() }, 'agencyData: data/Extended dir not found');
  }
  return _extendedDir;
}

/** Parse one CSV file into {headers, rows}, dropping fully-blank trailing rows. */
function readTable(dir: string, csv: string): AgencyTable {
  try {
    const rows = parseCsvRows(readFileSync(path.join(dir, csv), 'utf8')).filter(
      (r) => !(r.length === 1 && r[0] === ''),
    );
    if (!rows.length) return { headers: [], rows: [] };
    return { headers: rows[0] ?? [], rows: rows.slice(1) };
  } catch (e) {
    log.warn({ err: e, csv }, 'agencyData: failed to read table csv');
    return { headers: [], rows: [] };
  }
}

/** Resolve a section's `csv` (and any group `csv`) into an inline table. */
function resolveSection(section: Record<string, unknown>, dir: string): AgencySection {
  const s: AgencySection = { ...section };
  if (typeof s.csv === 'string') {
    s.table = readTable(dir, s.csv);
  }
  if (Array.isArray(s.groups)) {
    s.groups = s.groups.map((g) => {
      const gg = { ...g };
      if (typeof gg.csv === 'string') gg.table = readTable(dir, gg.csv);
      return gg;
    });
  }
  return s;
}

let _cache: Record<string, AgencyExtended> | null = null;

/** Load + cache every agency's extended data. Cheap, static; reload() clears it. */
export function loadAllAgencyExtended(): Record<string, AgencyExtended> {
  if (_cache) return _cache;
  const out: Record<string, AgencyExtended> = {};
  const dir = extendedDir();
  if (!dir) {
    _cache = out;
    return out;
  }
  for (const name of readdirSync(dir).sort()) {
    const adir = path.join(dir, name);
    try {
      if (!statSync(adir).isDirectory()) continue;
    } catch {
      continue;
    }
    let meta: Record<string, unknown>;
    try {
      meta = JSON.parse(readFileSync(path.join(adir, 'meta.json'), 'utf8'));
    } catch {
      continue; // no/invalid meta.json → skip, mirroring the Python build
    }
    const sections = Array.isArray(meta.sections)
      ? (meta.sections as Record<string, unknown>[]).map((s) => resolveSection(s, adir))
      : [];
    out[name] = {
      title: typeof meta.title === 'string' ? meta.title : '',
      tag: typeof meta.tag === 'string' ? meta.tag : '',
      badges: Array.isArray(meta.badges) ? meta.badges : [],
      overview: Array.isArray(meta.overview) ? meta.overview : [],
      sections,
    };
  }
  _cache = out;
  log.info({ agencies: Object.keys(out).length }, 'agencyData: loaded extended agency data');
  return out;
}

/** Drop the cache so the next read re-parses the CSV tree (used after edits). */
export function reloadAgencyExtended(): void {
  _cache = null;
}

export function getAgencyExtended(slug: string): AgencyExtended | null {
  return loadAllAgencyExtended()[slug] ?? null;
}
