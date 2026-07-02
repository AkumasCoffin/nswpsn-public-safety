/**
 * One-shot backfill for sidecar columns promoted from JSONB:
 *   - Migration 021 added category, subcategory
 *   - Migration 022 added title, location_text, status, severity,
 *     is_active
 *
 * Existing sidecar rows have NULL in these columns until the writer
 * naturally re-touches them on the next data_hash change, which could
 * be days for stable incidents. This task copies all of them from the
 * parent row that the sidecar's latest_fetched_at points at.
 *
 * Runs once at startup, opportunistically and in the background. Each
 * chunk is a single UPDATE...FROM so the work happens server-side and
 * Node doesn't carry rows. Idempotent — the WHERE guard skips already-
 * populated rows.
 *
 * Chunking is keyset-paginated over the (source, source_id) PK: each
 * chunk scans a bounded WINDOW of candidate rows past a cursor, fills
 * the fillable subset, and advances the cursor past the whole window.
 * The naive version instead LIMIT-ed on *fillable* rows — for tables
 * where most NULL rows are permanently unfillable (archive_waze: the
 * parent rows simply have no category/title), collecting one chunk
 * meant scanning ~the whole table joined to the parent, which blew the
 * per-chunk statement_timeout on chunk 0 forever, making zero progress
 * on every boot.
 *
 * Completion marker: when a table finishes a full pass (cursor reached
 * the end), a sentinel row goes into schema_migrations — same pattern
 * as archiveLatestRecompute — so later boots skip the table entirely.
 * Post-022 the live writer populates the dims on every upsert, so no
 * new backfill-needing rows accumulate after a completed pass.
 */
import type { Pool } from 'pg';
import { getPool } from '../db/pool.js';
import { log } from '../lib/log.js';
import { SIDECAR_LOCK_NAMESPACE } from '../store/archive.js';

const ARCHIVE_TABLES = [
  'archive_waze',
  'archive_traffic',
  'archive_rfs',
  'archive_power',
  'archive_misc',
] as const;

// Smaller chunks + a short per-chunk statement_timeout keep each unit of
// work brief so the backfill never pins the DB. Previously a 5k-row chunk
// under a 60s timeout could hold the shared sidecar advisory lock long
// enough to stall the live Waze writer (33s flushes) and cascade every
// other query into statement-timeout 500s. Tunable via env for ops.
const CHUNK_SIZE = Math.max(100, Number(process.env['ARCHIVE_DIMS_BACKFILL_CHUNK'] ?? '1000') || 1000);
const PAUSE_BETWEEN_CHUNKS_MS = 750;
// Validate the env-supplied timeout up front — it's interpolated raw into a
// `SET LOCAL statement_timeout` and a malformed value would make every chunk
// throw on the SET, silently disabling the backfill. Only accept the postgres
// interval forms we expect (bare ms, or a number with ms/s/min suffix).
const DEFAULT_CHUNK_STATEMENT_TIMEOUT = '20s';
function validateStatementTimeout(raw: string | undefined): string {
  const v = (raw ?? '').trim();
  if (/^\d+(ms|s|min)?$/.test(v)) return v;
  if (v) {
    log.warn(
      { value: raw, fallback: DEFAULT_CHUNK_STATEMENT_TIMEOUT },
      'archiveLatestDimsBackfill: invalid ARCHIVE_DIMS_BACKFILL_TIMEOUT, using default',
    );
  }
  return DEFAULT_CHUNK_STATEMENT_TIMEOUT;
}
const CHUNK_STATEMENT_TIMEOUT = validateStatementTimeout(
  process.env['ARCHIVE_DIMS_BACKFILL_TIMEOUT'],
);
const CHUNK_LOCK_TIMEOUT = '3s';
const MAX_LOCK_MISSES = 20; // defer the table to a later run if the live writer stays busy
const MAX_CHUNK_FAILS = 3; // give up the table this run after repeated timeouts
const STARTUP_DELAY_MS = 60_000; // start 1 min after boot — well after
                                  // the existing sidecar backfill

// Ops kill-switch: set ARCHIVE_DIMS_BACKFILL_DISABLED=true to skip the
// backfill entirely (e.g. while the DB is under pressure). The promoted
// dim columns just stay NULL — filterCache falls back to its legacy scan.
function backfillDisabled(): boolean {
  const v = (process.env['ARCHIVE_DIMS_BACKFILL_DISABLED'] ?? '').toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

let running = false;

interface BackfillStats {
  table: string;
  chunks: number;
  rowsScanned: number;
  rowsUpdated: number;
  completed: boolean; // full pass reached the end of the table
  ms: number;
}

// A row is a CANDIDATE if ANY of the promoted columns is NULL —
// category IS NULL OR title IS NULL catches both 021 (category) and
// 022 (title + status + severity + ...) targets in one pass. Whether a
// candidate is actually FILLABLE (parent has a value) is only decided
// inside the chunk query; counting fillable rows up-front would need
// the same full parent join that made the naive chunking time out.
async function candidateCount(pool: Pool, table: string): Promise<number> {
  const r = await pool.query<{ n: string }>(
    `SELECT COUNT(*)::text AS n
       FROM ${table}_latest
      WHERE category IS NULL OR title IS NULL`,
  );
  return Number(r.rows[0]?.n ?? '0');
}

// Per-table completion sentinel, stored as a schema_migrations row (same
// trick as archiveLatestRecompute's 'task:archive_latest_recompute').
// Written only after a FULL pass (cursor walked off the end of the
// table) — a deferred (writer-busy) or bailed (repeated chunk failure)
// table gets no marker and retries on the next boot.
function markerFor(table: string): string {
  return `task:archive_dims_backfill:${table}`;
}

async function tableDone(pool: Pool, table: string): Promise<boolean> {
  try {
    const r = await pool.query(
      `SELECT 1 FROM schema_migrations WHERE filename = $1`,
      [markerFor(table)],
    );
    return (r.rowCount ?? 0) > 0;
  } catch {
    return false; // can't read markers → just do the (idempotent) work
  }
}

async function markTableDone(pool: Pool, table: string): Promise<void> {
  try {
    await pool.query(
      `INSERT INTO schema_migrations (filename) VALUES ($1)
         ON CONFLICT (filename) DO NOTHING`,
      [markerFor(table)],
    );
  } catch (err) {
    log.warn(
      { err: (err as Error).message, table },
      'archiveLatestDimsBackfill: failed to write done-marker (will rescan next boot)',
    );
  }
}

async function backfillTable(pool: Pool, table: string): Promise<BackfillStats> {
  const start = Date.now();
  const stats: BackfillStats = {
    table,
    chunks: 0,
    rowsScanned: 0,
    rowsUpdated: 0,
    completed: false,
    ms: 0,
  };

  // Keyset cursor over the (source, source_id) PK. Advanced past the whole
  // scanned window on every committed chunk — INCLUDING windows where
  // nothing was fillable — so progress is monotonic and the pass always
  // terminates. '' sorts before every non-empty text, so ('','') starts
  // from the top of the index.
  let cursorSource = '';
  let cursorSourceId = '';

  // A deadlock (40P01) means another sidecar writer won the race; the
  // chunk rolled back untouched and just needs re-running, not aborting
  // the whole table. Bounded retries with a short backoff guard against
  // a pathological live-lock.
  const MAX_DEADLOCK_RETRIES = 5;
  let deadlockRetries = 0;
  let lockMisses = 0;
  let chunkFails = 0;
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  for (;;) {
    const client = await pool.connect();
    let scanned = 0;
    let outcome: 'ok' | 'deadlock' | 'lockmiss' | 'fail' = 'ok';
    try {
      await client.query('BEGIN');
      await client.query(`SET LOCAL statement_timeout = '${CHUNK_STATEMENT_TIMEOUT}'`);
      await client.query(`SET LOCAL lock_timeout = '${CHUNK_LOCK_TIMEOUT}'`);
      // Cooperative, NON-blocking lock against the live writer's sidecar
      // upsert. If the writer holds it (heavy ingest), don't queue behind
      // it — roll back and yield, so the backfill never stalls live data.
      const lk = await client.query<{ got: boolean }>(
        'SELECT pg_try_advisory_xact_lock($1, hashtext($2)) AS got',
        [SIDECAR_LOCK_NAMESPACE, `${table}_latest`],
      );
      if (!lk.rows[0]?.got) {
        await client.query('ROLLBACK');
        outcome = 'lockmiss';
      } else {
      // scan: the next window of candidate rows past the keyset cursor.
      // The LIMIT bounds the window by CANDIDATES (cheap PK-index walk,
      // no parent join), so per-chunk cost stays flat no matter how many
      // candidates turn out unfillable. batch: the fillable subset —
      // parent joined only for the windowed rows. upd: fills them.
      // Backfill covers both 021 (category/subcategory — parent columns)
      // and 022 (title/location_text/status/severity/is_active —
      // extracted from parent's data JSONB). The CASE on is_active uses
      // the same truthy-string semantics as extractBoolField in
      // archive.ts so backfilled values match what the writer would
      // produce going forward. Rows whose parent can't fill them are
      // simply stepped over by the cursor and never revisited.
      // batch keeps (source, source_id) order to match the live
      // writer's upsert lock order; without a shared order, overlapping
      // batches deadlock (SQLSTATE 40P01).
      const r = await client.query<{
        scanned: number;
        updated: number;
        last_source: string | null;
        last_source_id: string | null;
      }>(
        `WITH scan AS (
           SELECT l.source, l.source_id, l.latest_fetched_at,
                  l.category, l.title
           FROM ${table}_latest l
           WHERE (l.source, l.source_id) > ($2::text, $3::text)
             AND (l.category IS NULL OR l.title IS NULL)
           ORDER BY l.source, l.source_id
           LIMIT $1
         ),
         batch AS (
           SELECT s.source, s.source_id, s.latest_fetched_at
           FROM scan s
           JOIN ${table} a
             ON a.source = s.source
            AND a.source_id = s.source_id
            AND a.fetched_at = s.latest_fetched_at
           WHERE (s.category IS NULL AND a.category IS NOT NULL)
              OR (s.title    IS NULL AND NULLIF(a.data->>'title', '') IS NOT NULL)
           ORDER BY s.source, s.source_id
         ),
         upd AS (
           UPDATE ${table}_latest l
              SET category      = a.category,
                  subcategory   = a.subcategory,
                  title         = NULLIF(a.data->>'title', ''),
                  location_text = NULLIF(a.data->>'location_text', ''),
                  status        = NULLIF(a.data->>'status', ''),
                  severity      = NULLIF(a.data->>'severity', ''),
                  is_active     = CASE
                    WHEN a.data->>'is_active' IN ('1','true','True','TRUE') THEN true
                    WHEN a.data->>'is_active' IN ('0','false','False','FALSE') THEN false
                    ELSE NULL
                  END
             FROM batch b
             JOIN ${table} a
               ON a.source = b.source
              AND a.source_id = b.source_id
              AND a.fetched_at = b.latest_fetched_at
            WHERE l.source = b.source
              AND l.source_id = b.source_id
           RETURNING 1
         )
         SELECT (SELECT COUNT(*) FROM scan)::int AS scanned,
                (SELECT COUNT(*) FROM upd)::int  AS updated,
                (SELECT s.source FROM scan s
                  ORDER BY s.source DESC, s.source_id DESC LIMIT 1) AS last_source,
                (SELECT s.source_id FROM scan s
                  ORDER BY s.source DESC, s.source_id DESC LIMIT 1) AS last_source_id`,
          [CHUNK_SIZE, cursorSource, cursorSourceId],
        );
        const row = r.rows[0];
        scanned = row?.scanned ?? 0;
        const updated = row?.updated ?? 0;
        await client.query('COMMIT');

        // Advance the cursor past the whole window — only after COMMIT so
        // a rolled-back chunk is retried from the same position.
        if (row?.last_source != null && row.last_source_id != null) {
          cursorSource = row.last_source;
          cursorSourceId = row.last_source_id;
        }
        stats.chunks += 1;
        stats.rowsScanned += scanned;
        stats.rowsUpdated += updated;
        deadlockRetries = 0; // progress made — reset the retry budget
        if (stats.chunks % 50 === 0) {
          log.info(
            { table, chunks: stats.chunks, scanned: stats.rowsScanned, updated: stats.rowsUpdated },
            'archiveLatestDimsBackfill: progress',
          );
        }
      }
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch { /* ignore */ }
      const code = (err as { code?: string }).code;
      if (code === '40P01' && deadlockRetries < MAX_DEADLOCK_RETRIES) {
        outcome = 'deadlock';
        deadlockRetries += 1;
        log.info(
          { table, attempt: deadlockRetries, chunks: stats.chunks },
          'archiveLatestDimsBackfill: deadlock, retrying chunk',
        );
      } else {
        outcome = 'fail';
        log.warn(
          { err: (err as Error).message, code, table, chunks: stats.chunks },
          'archiveLatestDimsBackfill: chunk failed',
        );
      }
    } finally {
      client.release();
    }

    if (outcome === 'lockmiss') {
      // Live writer holds the lock — back off and try again, but don't
      // chase it forever; defer the rest of the table to a later run.
      lockMisses += 1;
      if (lockMisses > MAX_LOCK_MISSES) {
        log.info(
          { table, chunks: stats.chunks },
          'archiveLatestDimsBackfill: writer busy, deferring table to a later run',
        );
        break;
      }
      await sleep(1000);
      continue;
    }
    if (outcome === 'deadlock') {
      await sleep(250 * deadlockRetries);
      continue;
    }
    if (outcome === 'fail') {
      // Timeout / other error on this chunk. Back off briefly and retry a
      // few times (smaller chunks usually get through); bail the table if
      // it keeps failing so we never hammer the DB.
      chunkFails += 1;
      if (chunkFails >= MAX_CHUNK_FAILS) {
        log.warn({ table, chunks: stats.chunks }, 'archiveLatestDimsBackfill: too many chunk failures, stopping table');
        break;
      }
      await sleep(2000);
      continue;
    }

    // Success.
    lockMisses = 0;
    chunkFails = 0;
    if (scanned < CHUNK_SIZE) {
      // Window came back short — the cursor walked off the end of the
      // candidate set. Full pass complete (fillable rows filled,
      // unfillable ones permanently stepped over).
      stats.completed = true;
      break;
    }

    if (PAUSE_BETWEEN_CHUNKS_MS > 0) {
      await sleep(PAUSE_BETWEEN_CHUNKS_MS);
    }
  }

  stats.ms = Date.now() - start;
  return stats;
}

export async function runArchiveLatestDimsBackfill(): Promise<void> {
  if (running) return;
  if (backfillDisabled()) {
    log.info('archiveLatestDimsBackfill: disabled via ARCHIVE_DIMS_BACKFILL_DISABLED, skipping');
    return;
  }
  running = true;
  try {
    const pool = await getPool();
    if (!pool) {
      log.info('archiveLatestDimsBackfill: no pool, skipping');
      return;
    }
    for (const table of ARCHIVE_TABLES) {
      try {
        if (await tableDone(pool, table)) {
          log.info({ table }, 'archiveLatestDimsBackfill: marker present, skipping');
          continue;
        }
        const candidates = await candidateCount(pool, table);
        if (candidates === 0) {
          log.info({ table }, 'archiveLatestDimsBackfill: no NULL rows, marking done');
          await markTableDone(pool, table);
          continue;
        }
        log.info({ table, candidates }, 'archiveLatestDimsBackfill: starting');
        const stats = await backfillTable(pool, table);
        log.info(stats, 'archiveLatestDimsBackfill: table done');
        if (stats.completed) {
          // Only a full pass earns the marker; a deferred (writer busy)
          // or bailed (repeated chunk failure) table retries next boot.
          await markTableDone(pool, table);
        }
      } catch (err) {
        log.warn(
          { err: (err as Error).message, table },
          'archiveLatestDimsBackfill: table failed (non-fatal)',
        );
      }
    }
  } finally {
    running = false;
  }
}

/** Schedule the backfill 60s after boot so it doesn't compete with the
 *  existing archiveLatestBackfill (30s startup delay) or any other
 *  startup work. Idempotent — multiple calls just no-op. */
export function scheduleArchiveLatestDimsBackfill(): void {
  setTimeout(() => {
    void runArchiveLatestDimsBackfill();
  }, STARTUP_DELAY_MS).unref?.();
}
