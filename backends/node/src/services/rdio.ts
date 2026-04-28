/**
 * Helpers for talking to the SELF-HOSTED rdio-scanner Postgres
 * (RDIO_DATABASE_URL) — separate from the main archive pool.
 *
 * Mirrors the python helpers at external_api_proxy.py:14490-14605:
 *   - isRdioConfigured()       → _rdio_is_configured
 *   - getRdioPool()            → _rdio_get_pool
 *   - resolveLabels(s, t)      → _rdio_resolve_labels (5 min cache)
 *   - getUnitLabel(rid)        → _RDIO_UNIT_LABELS lookup (CSV-driven)
 *
 * All consumers are expected to call isRdioConfigured() first; routes
 * 503 when it's false rather than letting the pool builder throw.
 */
import type { Pool } from 'pg';
import pgPkg from 'pg';
import { promises as fs } from 'node:fs';
import * as path from 'node:path';
import { config } from '../config.js';
import { log } from '../lib/log.js';

// rdio-scanner stores its dateTime column as `timestamp WITHOUT time
// zone` holding UTC values. By default pg-node parses OID 1114
// (TIMESTAMP) using the *Node process* local timezone — so on a server
// not running in UTC the Date object comes back ~hours off from the
// real wall time. Mirrors python's explicit `dt.replace(tzinfo=UTC)`
// at external_api_proxy.py:14766. Set the parser globally so every
// pool sees the same behaviour. OID 1184 (TIMESTAMPTZ, used by our
// archive_* tables) is unaffected.
const TIMESTAMP_OID = 1114;
pgPkg.types.setTypeParser(TIMESTAMP_OID, (str: string) =>
  // Treat the naive value as UTC by appending 'Z' before Date.parse.
  // null-safe: pg-node only invokes the parser for non-null cells.
  new Date(str + 'Z'),
);

let _pool: Pool | null = null;

export function isRdioConfigured(): boolean {
  return Boolean(config.RDIO_DATABASE_URL);
}

export async function getRdioPool(): Promise<Pool | null> {
  if (_pool) return _pool;
  if (!config.RDIO_DATABASE_URL) return null;
  const { Pool: PgPool } = await import('pg');
  _pool = new PgPool({
    connectionString: config.RDIO_DATABASE_URL,
    max: 5,
    statement_timeout: 30_000,
    idleTimeoutMillis: 60_000,
  });
  _pool.on('error', (err) => {
    log.error({ err }, 'rdio pg pool error');
  });
  return _pool;
}

export async function closeRdioPool(): Promise<void> {
  if (_pool) {
    await _pool.end();
    _pool = null;
  }
}

// ---------------------------------------------------------------------------
// System / talkgroup label cache (5 min TTL).
// ---------------------------------------------------------------------------

interface LabelCache {
  systems: Map<number, string>;
  talkgroups: Map<string, { label: string; name: string }>;
  fetchedAt: number;
}

const LABEL_TTL_MS = 5 * 60_000;
let labelCache: LabelCache | null = null;

function tgKey(systemId: number, talkgroupId: number): string {
  return `${systemId}|${talkgroupId}`;
}

async function refreshLabelCache(): Promise<void> {
  const pool = await getRdioPool();
  if (!pool) return;
  const systems = new Map<number, string>();
  const talkgroups = new Map<string, { label: string; name: string }>();
  const sysRes = await pool.query<{ id: number; label: string | null }>(
    'SELECT "id", "label" FROM "rdioScannerSystems"',
  );
  for (const row of sysRes.rows) {
    const id = Number(row.id);
    if (Number.isFinite(id)) {
      systems.set(id, row.label ?? `System ${id}`);
    }
  }
  const tgRes = await pool.query<{
    systemId: number | null;
    id: number | null;
    label: string | null;
    name: string | null;
  }>(
    'SELECT "systemId", "id", "label", "name" FROM "rdioScannerTalkgroups"',
  );
  for (const row of tgRes.rows) {
    if (row.systemId === null || row.id === null) continue;
    const sid = Number(row.systemId);
    const tid = Number(row.id);
    if (!Number.isFinite(sid) || !Number.isFinite(tid)) continue;
    talkgroups.set(tgKey(sid, tid), {
      label: row.label ?? '',
      name: row.name ?? '',
    });
  }
  labelCache = { systems, talkgroups, fetchedAt: Date.now() };
}

export async function resolveLabels(
  systemId: number | null | undefined,
  talkgroupId: number | null | undefined,
): Promise<{ systemLabel: string | null; talkgroupLabel: string | null }> {
  if (!labelCache || Date.now() - labelCache.fetchedAt > LABEL_TTL_MS) {
    try {
      await refreshLabelCache();
    } catch (err) {
      log.warn({ err: (err as Error).message }, 'rdio label cache refresh failed');
    }
  }
  const sid = systemId !== null && systemId !== undefined ? Number(systemId) : null;
  const tid = talkgroupId !== null && talkgroupId !== undefined ? Number(talkgroupId) : null;
  const systemLabel = sid !== null && labelCache?.systems.has(sid)
    ? labelCache.systems.get(sid) ?? null
    : null;
  const tg = sid !== null && tid !== null
    ? labelCache?.talkgroups.get(tgKey(sid, tid)) ?? null
    : null;
  const talkgroupLabel = tg ? tg.name || tg.label || null : null;
  return { systemLabel, talkgroupLabel };
}

// ---------------------------------------------------------------------------
// Radio unit (RID) labels — loaded from CSV at startup. Mirror of
// python's _load_rdio_unit_labels at line 14435.
// ---------------------------------------------------------------------------

const unitLabels = new Map<number, string>();
let unitLabelsLoaded = false;

async function loadUnitLabelsFromCsv(): Promise<void> {
  // backends/reference/ — we run from backends/node/, so two levels up.
  const referenceDir = path.resolve(process.cwd(), '..', 'reference');
  const files = ['rdio_units.csv', 'unit_callsigns.csv'];
  for (const fname of files) {
    const fp = path.join(referenceDir, fname);
    try {
      const text = await fs.readFile(fp, 'utf8');
      const lines = text.split(/\r?\n/);
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (!line || !line.trim()) continue;
        // Naive CSV: handles "id","label",... where label may not have
        // commas. The python loader uses csv.reader which we approximate
        // by stripping outer quotes per cell. Matches the rdio_units.csv
        // format we ship.
        const cells = line.split(',').map((cell) => cell.trim().replace(/^"|"$/g, ''));
        const first = cells[0];
        if (!first) continue;
        if (i === 0 && !/^\d+$/.test(first)) continue; // header row
        if (!/^\d+$/.test(first)) continue;
        const rid = Number.parseInt(first, 10);
        const label = (cells[1] ?? '').trim();
        if (label) unitLabels.set(rid, label);
      }
    } catch (err) {
      const e = err as NodeJS.ErrnoException;
      if (e.code !== 'ENOENT') {
        log.warn({ err: e.message, path: fp }, 'rdio unit-label CSV read failed');
      }
    }
  }
  unitLabelsLoaded = true;
  if (unitLabels.size > 0) {
    log.info({ count: unitLabels.size }, 'rdio unit labels loaded');
  }
}

export async function ensureUnitLabelsLoaded(): Promise<void> {
  if (unitLabelsLoaded) return;
  await loadUnitLabelsFromCsv();
}

export function getUnitLabel(rid: number | null | undefined): string | null {
  if (rid === null || rid === undefined) return null;
  return unitLabels.get(Number(rid)) ?? null;
}

// ---------------------------------------------------------------------------
// Test seams
// ---------------------------------------------------------------------------

export function _resetRdioCachesForTests(): void {
  labelCache = null;
  unitLabels.clear();
  unitLabelsLoaded = false;
}

export function _seedUnitLabelForTests(rid: number, label: string): void {
  unitLabels.set(rid, label);
  unitLabelsLoaded = true;
}

export function _seedLabelCacheForTests(
  systems: Array<[number, string]>,
  talkgroups: Array<[number, number, { label: string; name: string }]>,
): void {
  labelCache = {
    systems: new Map(systems),
    talkgroups: new Map(
      talkgroups.map(([sid, tid, v]) => [tgKey(sid, tid), v] as const),
    ),
    fetchedAt: Date.now(),
  };
}
