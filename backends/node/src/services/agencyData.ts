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
    // Don't cache on error: a transient DB blip must not pin an empty result
    // (which would blank the public agency page until the next edit). Return
    // the empty map for this request only and let the next call retry.
    log.error({ err: e }, 'agencyData: read failed');
    return out;
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

// ── Row edits ───────────────────────────────────────────────────────────────

export interface RowChange {
  sectionKey: string; // the section's stable `csv` key
  op: 'add' | 'update' | 'delete';
  rowKey?: string; // first-column value (update/delete)
  afterCells?: string[]; // new row cells (add/update)
}

/** Find the editable table section of a blob by its stable `csv` key. */
export function findEditableSection(data: AgencyExtended, sectionKey: string): AgencySection | null {
  for (const s of data.sections ?? []) {
    if (s.csv === sectionKey && s.table) return s;
  }
  return null;
}

/** The natural row key of a row = its first cell (Reg / Code / Callsign). */
export function rowKeyOf(cells: string[]): string {
  return (cells[0] ?? '').trim();
}

/**
 * Apply a row change to an agency blob IN PLACE. Returns null on success or an
 * error string (bad section / row not found / duplicate key). Does not persist.
 */
export function applyRowChangeInPlace(data: AgencyExtended, change: RowChange): string | null {
  const section = findEditableSection(data, change.sectionKey);
  if (!section || !section.table) return 'unknown section';
  const rows = section.table.rows;
  const width = section.table.headers.length;
  const norm = (cells: string[] | undefined): string[] => {
    const c = (cells ?? []).map((x) => String(x ?? ''));
    // Pad/truncate to the header width so a row can't desync the table.
    if (width > 0) {
      while (c.length < width) c.push('');
      if (c.length > width) c.length = width;
    }
    return c;
  };

  if (change.op === 'add') {
    const cells = norm(change.afterCells);
    const key = rowKeyOf(cells);
    if (!key) return 'new row needs a value in the first column';
    if (rows.some((r) => rowKeyOf(r) === key)) return `a row with ${section.table!.headers[0] || 'key'} "${key}" already exists`;
    rows.push(cells);
    return null;
  }

  const key = (change.rowKey ?? '').trim();
  const idx = rows.findIndex((r) => rowKeyOf(r) === key);
  if (idx === -1) return `row "${key}" not found (it may have changed)`;

  if (change.op === 'delete') {
    rows.splice(idx, 1);
    return null;
  }
  // update
  const cells = norm(change.afterCells);
  if (!rowKeyOf(cells)) return 'row needs a value in the first column';
  const newKey = rowKeyOf(cells);
  if (newKey !== key && rows.some((r, i) => i !== idx && rowKeyOf(r) === newKey)) {
    return `a row with ${section.table.headers[0] || 'key'} "${newKey}" already exists`;
  }
  rows[idx] = cells;
  return null;
}

/**
 * Transactionally apply an approved change to the stored blob. Locks the agency
 * row (FOR UPDATE) so concurrent edits serialise. Returns null on success or an
 * error string.
 */
export async function applyRowChange(pool: Pool, slug: string, change: RowChange): Promise<string | null> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query<{ data: AgencyExtended }>(
      'SELECT data FROM agency_extended WHERE slug = $1 FOR UPDATE',
      [slug],
    );
    const data = rows[0]?.data;
    if (!data) {
      await client.query('ROLLBACK');
      return 'unknown agency';
    }
    const err = applyRowChangeInPlace(data, change);
    if (err) {
      await client.query('ROLLBACK');
      return err;
    }
    await client.query(
      'UPDATE agency_extended SET data = $2::jsonb, updated_at = now() WHERE slug = $1',
      [slug, JSON.stringify(data)],
    );
    await client.query('COMMIT');
    _cache = null;
    return null;
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* ignore */
    }
    log.error({ err: e, slug }, 'agencyData: applyRowChange failed');
    return 'internal error applying change';
  } finally {
    client.release();
  }
}
