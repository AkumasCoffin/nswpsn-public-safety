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
import { getPool } from '../db/pool.js';

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

    const pool = await getPool();
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

    let totalRows = 0;
    for (const [table, rows] of buckets) {
      try {
        await this.insertBatch(pool, table, rows);
        totalRows += rows.length;
      } catch (err) {
        log.error(
          { err, table, count: rows.length },
          'ArchiveWriter: insert failed; re-queueing',
        );
        // Re-queue so the next flush retries. Cap protects from
        // infinite growth if the DB is permanently unhappy.
        for (const row of rows) {
          this.push(table, row);
        }
      }
    }

    const ms = Date.now() - start;
    this.flushing = false;
    this.lastFlushAt = Math.floor(Date.now() / 1000);
    this.totalWritten += totalRows;
    if (totalRows > 0) {
      // Only log non-empty flushes at info — empty cycles fire every
      // 30s and were drowning the log without adding any signal.
      if (totalRows > 0) {
        log.info({ tables: buckets.size, rows: totalRows, ms }, 'archive flush');
      } else {
        log.debug({ ms }, 'archive flush idle');
      }
    }
    return { tables: buckets.size, rows: totalRows, ms };
  }

  /** Batched multi-VALUES INSERT. One round-trip per table per flush. */
  private async insertBatch(
    pool: Pool,
    table: ArchiveTable,
    rows: ArchiveRow[],
  ): Promise<void> {
    if (rows.length === 0) return;

    // Build $1, $2, ... placeholders for 8 columns per row.
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
        // Postgres `fetched_at` column is TIMESTAMPTZ; pass an ISO string.
        new Date(r.fetched_at * 1000).toISOString(),
        r.lat ?? null,
        r.lng ?? null,
        r.category ?? null,
        r.subcategory ?? null,
        JSON.stringify(r.data),
      );
    }

    const sql =
      `INSERT INTO ${table} (${cols.join(',')}) VALUES ` +
      placeholders.join(',');

    // 30s budget per batch. Beyond that something's wrong; better to
    // re-queue and let the next flush try a smaller batch.
    const client = await pool.connect();
    try {
      await client.query("SET LOCAL statement_timeout = '30s'");
      await client.query(sql, params);
    } finally {
      client.release();
    }
  }

  /** Snapshot of writer state for /api/status. */
  metrics(): {
    queue_size: number;
    dropped: number;
    last_flush_age_secs: number | null;
    total_written: number;
  } {
    return {
      queue_size: this.queue.length,
      dropped: this.dropped,
      last_flush_age_secs: this.lastFlushAt
        ? Math.floor(Date.now() / 1000) - this.lastFlushAt
        : null,
      total_written: this.totalWritten,
    };
  }
}

export const archiveWriter = new ArchiveWriter();
