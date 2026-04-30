/**
 * Per-incident recompute of sidecar.latest_fetched_at to reflect when
 * each incident's data actually last changed (not when it was last
 * polled). Replaces the giant LAG-over-whole-table SQL in migration 019,
 * which was hours of IO on archive_waze.
 *
 * Why we need this: migration 017's backfill set latest_fetched_at =
 * max(fetched_at), and under the old append-on-every-poll writer that
 * was always "right before deploy". After dedup ships, the unique=1
 * list orders by latest_fetched_at DESC and clusters every still-alive
 * incident at the deploy timestamp — users only see the last few hours
 * of history regardless of how far back they scroll.
 *
 * Algorithm per incident:
 *   1. Fetch the most recent N parent rows (bounded LIMIT).
 *   2. Hash each one with the same stable JSON algorithm the writer uses.
 *   3. Walk newest → older while hashes match the newest. The first
 *      timestamp where hash differs is the boundary; the last matching
 *      row's fetched_at is "when this incident's data last changed".
 *   4. UPDATE sidecar.latest_fetched_at to that timestamp.
 *
 * Cooperative: throttled per-incident sleep keeps the writer's flushes
 * snappy. Idempotent — re-runs converge to the same result, so no need
 * for a checkpoint table.
 *
 * Marks itself done in schema_migrations on completion so it doesn't
 * repeat work on subsequent boots. Manual reset:
 *   DELETE FROM schema_migrations WHERE filename = 'task:archive_latest_recompute';
 */
import { createHash } from 'node:crypto';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import type { ArchiveTable } from '../store/archive.js';

const ARCHIVE_TABLES: ArchiveTable[] = [
  'archive_misc',
  'archive_power',
  'archive_rfs',
  'archive_traffic',
  'archive_waze',
];

const MARKER = 'task:archive_latest_recompute';
const STARTUP_DELAY_MS = 90_000; // give backfill (017) a head start
const PER_INCIDENT_LOOKBACK = 2_000; // most incidents have far fewer rows
const PAUSE_BETWEEN_INCIDENTS_MS = 25;

let running = false;

function stableSerialize(v: unknown): string {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) {
    return '[' + v.map(stableSerialize).join(',') + ']';
  }
  const keys = Object.keys(v as Record<string, unknown>).sort();
  const parts: string[] = [];
  for (const k of keys) {
    parts.push(JSON.stringify(k) + ':' + stableSerialize((v as Record<string, unknown>)[k]));
  }
  return '{' + parts.join(',') + '}';
}

function hashData(data: unknown): string {
  return createHash('sha1').update(stableSerialize(data)).digest('hex').slice(0, 20);
}

interface SidecarEntry {
  source: string;
  source_id: string;
}
interface ParentRow {
  fetched_at: Date;
  data: unknown;
}

async function recomputeOneTable(table: ArchiveTable): Promise<{
  table: string;
  processed: number;
  updated: number;
  skipped: number;
  ms: number;
}> {
  const pool = await getPool();
  if (!pool) return { table, processed: 0, updated: 0, skipped: 0, ms: 0 };
  const start = Date.now();

  const sidecarRes = await pool.query<SidecarEntry>(
    `SELECT source, source_id FROM ${table}_latest ORDER BY source, source_id`,
  );
  const entries = sidecarRes.rows;

  let processed = 0;
  let updated = 0;
  let skipped = 0;

  for (const entry of entries) {
    processed += 1;
    try {
      const parentRes = await pool.query<ParentRow>(
        `SELECT fetched_at, data
           FROM ${table}
          WHERE source = $1 AND source_id = $2
          ORDER BY fetched_at DESC
          LIMIT ${PER_INCIDENT_LOOKBACK}`,
        [entry.source, entry.source_id],
      );
      if (parentRes.rows.length === 0) {
        skipped += 1;
        continue;
      }
      // pg returns JSONB as parsed objects — already in the shape the
      // writer hashed at INSERT time.
      const newest = parentRes.rows[0]!;
      const newestHash = hashData(newest.data);
      let earliestSameHash = newest.fetched_at;
      for (let i = 1; i < parentRes.rows.length; i += 1) {
        const r = parentRes.rows[i]!;
        if (hashData(r.data) !== newestHash) break;
        earliestSameHash = r.fetched_at;
      }
      const upd = await pool.query(
        `UPDATE ${table}_latest
            SET latest_fetched_at = $1
          WHERE source = $2
            AND source_id = $3
            AND latest_fetched_at <> $1`,
        [earliestSameHash, entry.source, entry.source_id],
      );
      if ((upd.rowCount ?? 0) > 0) updated += 1;
      else skipped += 1;
    } catch (err) {
      log.warn(
        { err: (err as Error).message, table, source: entry.source, source_id: entry.source_id },
        'recompute: per-incident failed (continuing)',
      );
    }
    if (PAUSE_BETWEEN_INCIDENTS_MS > 0) {
      await new Promise((r) => setTimeout(r, PAUSE_BETWEEN_INCIDENTS_MS));
    }
    // Heartbeat log every ~250 incidents so operators can see progress.
    if (processed % 250 === 0) {
      log.info({ table, processed, updated, skipped }, 'recompute: progress');
    }
  }

  return {
    table,
    processed,
    updated,
    skipped,
    ms: Date.now() - start,
  };
}

async function alreadyDone(): Promise<boolean> {
  const pool = await getPool();
  if (!pool) return true; // no DB → don't pretend to run
  try {
    const r = await pool.query<{ filename: string }>(
      `SELECT filename FROM schema_migrations WHERE filename = $1`,
      [MARKER],
    );
    return r.rows.length > 0;
  } catch {
    return false;
  }
}

async function markDone(): Promise<void> {
  const pool = await getPool();
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO schema_migrations (filename) VALUES ($1)
         ON CONFLICT (filename) DO NOTHING`,
      [MARKER],
    );
  } catch (err) {
    log.warn(
      { err: (err as Error).message },
      'recompute: failed to write marker (will re-run next boot)',
    );
  }
}

export async function runArchiveLatestRecompute(): Promise<void> {
  if (running) return;
  running = true;
  try {
    if (await alreadyDone()) {
      log.info('archiveLatestRecompute: marker present, skipping');
      return;
    }
    log.info('archiveLatestRecompute: starting (background, throttled)');
    const all: Array<Awaited<ReturnType<typeof recomputeOneTable>>> = [];
    for (const t of ARCHIVE_TABLES) {
      try {
        const stats = await recomputeOneTable(t);
        all.push(stats);
        log.info(stats, 'recompute: table done');
      } catch (err) {
        log.warn(
          { err: (err as Error).message, table: t },
          'recompute: table failed (continuing with next)',
        );
      }
    }
    await markDone();
    log.info({ tables: all }, 'archiveLatestRecompute: complete');
  } finally {
    running = false;
  }
}

export function scheduleArchiveLatestRecompute(): void {
  setTimeout(() => {
    void runArchiveLatestRecompute();
  }, STARTUP_DELAY_MS).unref?.();
}
