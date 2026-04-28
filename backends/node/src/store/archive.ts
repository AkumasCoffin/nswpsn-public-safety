/**
 * ArchiveWriter — the archive half of the new architecture.
 *
 * Append-only batched insert into the per-source `archive_*` tables.
 * **No UPDATE statements ever.** No `is_live`/`is_latest` columns. The
 * write side does exactly one thing: every poll snapshot becomes one
 * INSERT, batched with siblings for the same flush window.
 *
 * Replaces Python's _archive_buffer + _archive_writer_loop. Kept the
 * same shape (push to in-RAM buffer, drain on a timer, single writer)
 * because it worked well — the fix here is the schema, not the writer.
 *
 * Per-record shape:
 *   {
 *     table: 'archive_waze' | 'archive_traffic' | 'archive_rfs' |
 *            'archive_power' | 'archive_misc',
 *     row:   { source, source_id?, fetched_at, lat?, lng?, category?,
 *              subcategory?, data }
 *   }
 *
 * Rows are bucketed by destination table and INSERTed with executemany-
 * style multi-VALUES. Postgres handles the partition routing.
 */
import type { Pool } from 'pg';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { getWriterPool } from '../db/pool.js';

export type ArchiveTable =
  | 'archive_waze'
  | 'archive_traffic'
  | 'archive_rfs'
  | 'archive_power'
  | 'archive_misc';

export interface ArchiveRow {
  source: string; // e.g. 'waze_police', 'rfs', 'traffic_incident'
  source_id?: string | null;
  /** Epoch seconds when the record was fetched. */
  fetched_at: number;
  lat?: number | null;
  lng?: number | null;
  category?: string | null;
  subcategory?: string | null;
  /** The full upstream payload, JSONB-stored. Whatever the source returns. */
  data: unknown;
}

interface QueueItem {
  table: ArchiveTable;
  row: ArchiveRow;
}

const HARD_CAP = 50_000; // bound RAM if Postgres goes away briefly

export class ArchiveWriter {
  private queue: QueueItem[] = [];
  private dropped = 0;
  private flushTimer: NodeJS.Timeout | null = null;
  private flushing = false;
  private lastFlushAt = 0;
  private totalWritten = 0;
  // Per-flush stats so /api/status can report what the last flush did.
  // Updated atomically at the end of each flush() call.
  private lastFlushRecords = 0;
  private lastFlushTables = 0;
  private lastFlushMs = 0;
  private totalFlushes = 0;

  /**
   * Push a single record onto the queue. Non-blocking. Drops oldest
   * records if the queue exceeds HARD_CAP — this is a coarse defense,
   * the writer should be flushing every ARCHIVE_FLUSH_INTERVAL.
   */
  push(table: ArchiveTable, row: ArchiveRow): void {
    if (this.queue.length >= HARD_CAP) {
      // Trim 10% off the oldest to make room — avoids drop-one,
      // accept-one churn under sustained overload.
      const trim = Math.ceil(HARD_CAP / 10);
      this.queue.splice(0, trim);
      this.dropped += trim;
    }
    this.queue.push({ table, row });
  }

  /** Convenience: push many records of the same table in one call. */
  pushMany(table: ArchiveTable, rows: ArchiveRow[]): void {
    for (const row of rows) {
      this.queue.push({ table, row });
    }
    // Enforce cap once after the batch lands. Doing it pre-push could
    // trim the *existing* queue without trimming oversized incoming
    // rows; doing it post-push handles both at once.
    if (this.queue.length > HARD_CAP) {
      const overflow = this.queue.length - HARD_CAP;
      this.queue.splice(0, overflow);
      this.dropped += overflow;
    }
  }

  startFlushLoop(intervalMs: number = config.ARCHIVE_FLUSH_INTERVAL_MS): void {
    if (this.flushTimer) return;
    this.flushTimer = setInterval(() => {
      void this.flush();
    }, intervalMs);
    this.flushTimer.unref?.();
  }

  async stopAndFlush(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    // One final drain so we don't lose buffered rows on shutdown.
    await this.flush();
  }

  /**
   * Drain the queue once. Groups rows by destination table and emits
   * one multi-row INSERT per table.
   *
   * Returns counts so the periodic logging can show flush throughput.
   */
  async flush(): Promise<{ tables: number; rows: number; ms: number }> {
    if (this.flushing) return { tables: 0, rows: 0, ms: 0 };
    if (this.queue.length === 0) return { tables: 0, rows: 0, ms: 0 };

    const pool = await getWriterPool();
    if (!pool) {
      // No DB configured (yet) — keep queueing in RAM. Cap will kick in
      // if we go for a really long time without a target.
      return { tables: 0, rows: 0, ms: 0 };
    }

    this.flushing = true;
    const start = Date.now();

    // Take ownership of the queue; new pushes go into a fresh array.
    const drain = this.queue;
    this.queue = [];

    // Bucket by table.
    const buckets = new Map<ArchiveTable, ArchiveRow[]>();
    for (const item of drain) {
      const existing = buckets.get(item.table);
      if (existing) {
        existing.push(item.row);
      } else {
        buckets.set(item.table, [item.row]);
      }
    }

    // Run per-table inserts in parallel — previously the for-of loop
    // serialised them, so a slow archive_waze flush blocked the small
    // archive_misc/rfs/power flushes behind it (production: 24s
    // 5-table flushes). With the dedicated writer pool (8 slots) and
    // chunk-sizes already tuned per family, parallel flush wallclock
    // drops to max(per-table) instead of sum.
    let totalRows = 0;
    const results = await Promise.all(
      Array.from(buckets.entries()).map(async ([table, rows]) => {
        try {
          await this.insertBatch(pool, table, rows);
          return { table, ok: true, rows: rows.length };
        } catch (err) {
          log.error(
            { err, table, count: rows.length },
            'ArchiveWriter: insert failed; re-queueing',
          );
          return { table, ok: false, rows: rows.length, requeue: rows };
        }
      }),
    );
    for (const r of results) {
      if (r.ok) {
        totalRows += r.rows;
      } else if (r.requeue) {
        // Re-queue failed batch so next flush retries it. HARD_CAP
        // protects from runaway growth on persistent DB failure.
        for (const row of r.requeue) {
          this.push(r.table, row);
        }
      }
    }

    const ms = Date.now() - start;
    this.flushing = false;
    this.lastFlushAt = Math.floor(Date.now() / 1000);
    this.totalWritten += totalRows;
    // Track per-flush stats for /api/status. Even empty flushes count
    // toward totalFlushes so the panel can show "last 0 records, X
    // total flushes" if the writer is idle.
    this.lastFlushRecords = totalRows;
    this.lastFlushTables = buckets.size;
    this.lastFlushMs = ms;
    this.totalFlushes += 1;
    // Only log non-empty flushes at info — empty cycles fire every
    // 30s and were drowning the log without adding any signal.
    if (totalRows > 0) {
      log.info({ tables: buckets.size, rows: totalRows, ms }, 'archive flush');
    }
    return { tables: buckets.size, rows: totalRows, ms };
  }

  /**
   * Batched multi-VALUES INSERT. Splits very large batches into
   * INSERT_CHUNK_SIZE sub-batches so each statement finishes well
   * under the (database-default) statement_timeout, even when the
   * archive queue has been growing while a previous flush was stuck.
   * With archive_waze under heavy ingest the queue can reach 5k+ rows
   * per flush window, and a single INSERT of that size with 4 indexes
   * + JSONB blobs reliably hit 60s. Chunking keeps each insert in the
   * single-digit-second range.
   */
  private async insertBatch(
    pool: Pool,
    table: ArchiveTable,
    rows: ArchiveRow[],
  ): Promise<void> {
    if (rows.length === 0) return;

    // archive_waze has 4 indexes (source/ts/lat/lng + JSONB GIN-ish).
    // Under heavy ingest with concurrent /api/waze/police-heatmap
    // reads, a 500-row INSERT was hitting the 30s SET LOCAL timeout
    // (production: 22:21:13 — 3200-row batch failed on its 500-row
    // sub-chunk). Smaller chunks for waze keep each statement well
    // under the timeout; other tables stay at 500 since they don't
    // see the same write pressure.
    const INSERT_CHUNK_SIZE = table === 'archive_waze' ? 250 : 500;
    // Reuse one connection across chunks — `SET LOCAL` only takes
    // effect inside an explicit transaction, so each chunk runs in
    // its own BEGIN/COMMIT pair. Holding one client for the whole
    // batch avoids burning pool slots while archive_waze ingest is
    // hot. Failure on any chunk surfaces to the caller, which
    // re-queues the entire input batch (the writer-side dedup catches
    // duplicates the next round-trip — there is no UNIQUE constraint
    // on archive_*).
    const client = await pool.connect();
    try {
      for (let start = 0; start < rows.length; start += INSERT_CHUNK_SIZE) {
        const slice = rows.slice(start, start + INSERT_CHUNK_SIZE);
        await this.insertChunk(client, table, slice);
      }
    } finally {
      client.release();
    }
  }

  private async insertChunk(
    client: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
    table: ArchiveTable,
    rows: ArchiveRow[],
  ): Promise<void> {
    if (rows.length === 0) return;

    // APPEND-ONLY writes — pure INSERT, no UPDATE. is_latest and
    // is_live columns dropped in migration 013 after they caused
    // chronic I/O contention and write timeouts. is_live truthy
    // semantics are now derived from JSONB at read time only.
    const cols = [
      'source',
      'source_id',
      'fetched_at',
      'lat',
      'lng',
      'category',
      'subcategory',
      'data',
    ];
    const placeholders: string[] = [];
    const params: unknown[] = [];
    let i = 0;
    for (const r of rows) {
      const tuple: string[] = [];
      for (let c = 0; c < cols.length; c++) {
        i += 1;
        tuple.push(`$${i}`);
      }
      placeholders.push(`(${tuple.join(',')})`);
      params.push(
        r.source,
        r.source_id ?? null,
        new Date(r.fetched_at * 1000).toISOString(),
        r.lat ?? null,
        r.lng ?? null,
        r.category ?? null,
        r.subcategory ?? null,
        JSON.stringify(r.data),
      );
    }

    const insertSql =
      `INSERT INTO ${table} (${cols.join(',')}) VALUES ` +
      placeholders.join(',');

    await client.query('BEGIN');
    try {
      await client.query("SET LOCAL statement_timeout = '60s'");
      await client.query(insertSql, params);
      await client.query('COMMIT');
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch { /* ignore */ }
      throw err;
    }
  }

  /** Snapshot of writer state for /api/status. */
  metrics(): {
    queue_size: number;
    dropped: number;
    last_flush_age_secs: number | null;
    total_written: number;
    last_flush_records: number;
    last_flush_tables: number;
    last_flush_ms: number;
    total_flushes: number;
  } {
    return {
      queue_size: this.queue.length,
      dropped: this.dropped,
      last_flush_age_secs: this.lastFlushAt
        ? Math.floor(Date.now() / 1000) - this.lastFlushAt
        : null,
      total_written: this.totalWritten,
      last_flush_records: this.lastFlushRecords,
      last_flush_tables: this.lastFlushTables,
      last_flush_ms: this.lastFlushMs,
      total_flushes: this.totalFlushes,
    };
  }
}

export const archiveWriter = new ArchiveWriter();
