/**
 * Agency reference data ("Extended" tables).
 *
 * Source of truth is Postgres (agency_extended). The data/Extended CSV tree is
 * used ONCE, as a seed: on startup, if agency_extended is empty, we import the
 * CSVs (meta.json + one CSV per table section) into the DB. After that the DB is
 * authoritative — reads come from it and edits are applied to it.
 *
 * Each agency row stores the whole {title, tag, badges, overview, sections} blob
 * the agency page renders; each table section carries an inline {headers, rows}
 * plus its `csv` field, which is the STABLE section key the row-edit feature uses.
 */
import { readFileSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Pool } from 'pg';
import { parseCsvRows } from '../api/node-data.js';
import { getPool } from '../db/pool.js';
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

// ── CSV seed loader (used once to import into the DB) ───────────────────────

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

function extendedDir(): string | null {
  for (const c of extendedDirCandidates()) {
    try {
      if (statSync(c).isDirectory()) return c;
    } catch {
      /* next */
    }
  }
  return null;
}

function readTable(dir: string, csv: string): AgencyTable {
  try {
    const rows = parseCsvRows(readFileSync(path.join(dir, csv), 'utf8')).filter(
      (r) => !(r.length === 1 && r[0] === ''),
    );
    if (!rows.length) return { headers: [], rows: [] };
    return { headers: rows[0] ?? [], rows: rows.slice(1) };
  } catch (e) {
    log.warn({ err: e, csv }, 'agencyData: failed to read seed csv');
    return { headers: [], rows: [] };
  }
}

function resolveSection(section: Record<string, unknown>, dir: string): AgencySection {
  const s: AgencySection = { ...section };
  if (typeof s.csv === 'string') s.table = readTable(dir, s.csv);
  if (Array.isArray(s.groups)) {
    s.groups = s.groups.map((g) => {
      const gg = { ...g };
      if (typeof gg.csv === 'string') gg.table = readTable(dir, gg.csv);
      return gg;
    });
  }
  return s;
}

/** Parse the whole data/Extended CSV tree into per-slug blobs (the DB seed). */
export function loadCsvSeed(): Record<string, AgencyExtended> {
  const out: Record<string, AgencyExtended> = {};
  const dir = extendedDir();
  if (!dir) {
    log.warn({ candidates: extendedDirCandidates() }, 'agencyData: data/Extended dir not found for seed');
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
      continue;
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
  return out;
}

// ── DB-backed reads (authoritative after seed) ──────────────────────────────

let _cache: Record<string, AgencyExtended> | null = null;

/** One-time import of the CSV seed into agency_extended when the table is empty. */
export async function seedAgencyDataIfEmpty(): Promise<void> {
  const pool = await getPool();
  if (!pool) return;
  try {
    const { rows } = await pool.query<{ n: string }>('SELECT COUNT(*)::int AS n FROM agency_extended');
    if (Number(rows[0]?.n ?? 0) > 0) return; // already seeded
    const seed = loadCsvSeed();
    const slugs = Object.keys(seed);
    if (!slugs.length) {
      log.warn('agencyData: CSV seed empty — nothing imported');
      return;
    }
    for (const slug of slugs) {
      await pool.query(
        `INSERT INTO agency_extended (slug, data) VALUES ($1, $2::jsonb)
         ON CONFLICT (slug) DO NOTHING`,
        [slug, JSON.stringify(seed[slug])],
      );
    }
    _cache = null;
    log.info({ agencies: slugs.length }, 'agencyData: seeded agency_extended from CSV tree');
  } catch (e) {
    log.error({ err: e }, 'agencyData: seed failed');
  }
}

/** All agencies keyed by slug, cached in memory until an edit invalidates it. */
export async function getAllAgencyExtended(): Promise<Record<string, AgencyExtended>> {
  if (_cache) return _cache;
  const pool = await getPool();
  if (!pool) return {};
  const out: Record<string, AgencyExtended> = {};
  try {
    const { rows } = await pool.query<{ slug: string; data: AgencyExtended }>(
      'SELECT slug, data FROM agency_extended',
    );
    for (const r of rows) out[r.slug] = r.data;
  } catch (e) {
    log.error({ err: e }, 'agencyData: read failed');
  }
  _cache = out;
  return out;
}

export async function getAgencyExtended(slug: string): Promise<AgencyExtended | null> {
  return (await getAllAgencyExtended())[slug] ?? null;
}

/** Drop the in-memory cache so the next read re-queries the DB (after an edit). */
export function invalidateAgencyCache(): void {
  _cache = null;
}

/** Persist a mutated agency blob and invalidate the cache. */
export async function saveAgencyExtended(pool: Pool, slug: string, data: AgencyExtended): Promise<void> {
  await pool.query(
    `INSERT INTO agency_extended (slug, data, updated_at) VALUES ($1, $2::jsonb, now())
     ON CONFLICT (slug) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
    [slug, JSON.stringify(data)],
  );
  _cache = null;
}
