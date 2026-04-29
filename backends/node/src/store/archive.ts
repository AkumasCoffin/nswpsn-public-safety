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
import { computeFlushTimeTombstones } from '../services/archiveLiveness.js';

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

    // Liveness diff at flush time: for each table bucket, compute
    // tombstones for source_ids that were live before this flush but
    // aren't in the incoming rows. ONE SELECT per table covers every
    // non-exempt source in the bucket, instead of one-per-source-per-
    // poll. Tombstones append to the same bucket so they ride the
    // single multi-VALUES INSERT below — no extra round-trips.
    const fetchedAtNow = Math.floor(Date.now() / 1000);
    await Promise.all(
      Array.from(buckets.entries()).map(async ([table, rows]) => {
        try {
          const tombstones = await computeFlushTimeTombstones({
            pool,
            table,
            rows,
            fetchedAt: fetchedAtNow,
          });
          if (tombstones.length > 0) rows.push(...tombstones);
        } catch (err) {
          // Liveness failures must never block the live INSERT.
          log.warn(
            { err: (err as Error).message, table },
            'ArchiveWriter: liveness diff failed (non-fatal)',
          );
        }
      }),
    );

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
   *
   * Single transaction across all chunks: previously each chunk had
   * its own BEGIN/SET LOCAL/COMMIT, costing 3 round-trips per chunk
   * (so a 10-chunk waze flush burnt 40 round trips — measured ~150ms
   * pure overhead per flush in production). Now BEGIN once, SET LOCAL
   * once, INSERT each chunk, COMMIT once → 13 round trips for the
   * same flush. Failure semantics unchanged: any chunk error rolls
   * back the whole batch and the caller re-queues — same all-or-
   * nothing the previous insertChunk-per-tx code already had via
   * flush()'s requeue path.
   */
  private async insertBatch(
    pool: Pool,
    table: ArchiveTable,
    rows: ArchiveRow[],
  ): Promise<void> {
    if (rows.length === 0) return;

    // archive_waze has 3 compound indexes plus the partition's own
    // primary key — under concurrent userscript ingest a 500-row
    // INSERT was hitting the 30s SET LOCAL timeout. 250-row chunks
    // keep each statement well under the timeout; other tables stay
    // at 500 since they don't see the same write pressure.
    const INSERT_CHUNK_SIZE = table === 'archive_waze' ? 250 : 500;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      try {
        await client.query("SET LOCAL statement_timeout = '60s'");
        for (let start = 0; start < rows.length; start += INSERT_CHUNK_SIZE) {
          const slice = rows.slice(start, start + INSERT_CHUNK_SIZE);
          await this.insertChunk(client, table, slice);
        }
        // Incremental heatmap aggregation. Done in the same transaction
        // as the archive_waze INSERTs so a row in archive_waze always
        // has its bin counted (or, on error, neither happens). Only
        // aggregates waze_police rows with non-null lat/lng — same
        // filter as the historical archive_waze GROUP BY scan that
        // this hook replaces.
        if (table === 'archive_waze') {
          await this.upsertPoliceHeatmapBinDaily(client, rows);
        }
        // Incremental filter-facets aggregation. Skipped for archive_waze
        // because waze facets come from the LiveStore in-memory snapshot
        // (the wazeIngestCache), not the archive. Including archive_waze
        // here would double-count waze types in the dropdown.
        if (table !== 'archive_waze') {
          await this.upsertFilterFacetsDaily(client, table, rows);
        }
        await client.query('COMMIT');
      } catch (err) {
        try { await client.query('ROLLBACK'); } catch { /* ignore */ }
        throw err;
      }
    } finally {
      client.release();
    }
  }

  /**
   * Aggregate the waze_police rows in this batch by
   * (day, lat_bin, lng_bin, subcategory) and UPSERT one VALUES tuple
   * per distinct cell into police_heatmap_bin_daily. Replaces the
   * 30-day GROUP BY scan that ran every 10 min from the heatmap
   * refresher and stalled the disk for half an hour at a time.
   *
   * subcategory normalisation mirrors the SQL the previous refresh
   * used: COALESCE(NULLIF(subcategory, ''), data->>'subtype',
   * 'POLICE_VISIBLE'). The indexBuilder backfills already populate the
   * subcategory column on existing rows; this is the in-flight equivalent.
   *
   * UTC for the day-bucket so ISO date strings match what the
   * indexBuilder backfill writes (which uses fetched_at AT TIME
   * ZONE 'UTC'::date).
   */
  private async upsertPoliceHeatmapBinDaily(
    client: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
    rows: ArchiveRow[],
  ): Promise<void> {
    const BIN_DEG = 0.001;
    type Cell = { day: string; lat_bin: number; lng_bin: number; sub: string; count: number };
    const cells = new Map<string, Cell>();
    for (const r of rows) {
      if (r.source !== 'waze_police') continue;
      if (r.lat == null || r.lng == null) continue;
      const data = (r.data ?? {}) as Record<string, unknown>;
      const sub =
        r.subcategory && r.subcategory !== ''
          ? r.subcategory
          : typeof data['subtype'] === 'string' && data['subtype'] !== ''
            ? (data['subtype'] as string)
            : 'POLICE_VISIBLE';
      const lat_bin = Math.floor(r.lat / BIN_DEG) * BIN_DEG;
      const lng_bin = Math.floor(r.lng / BIN_DEG) * BIN_DEG;
      const day = new Date(r.fetched_at * 1000).toISOString().slice(0, 10);
      const key = `${day}|${lat_bin}|${lng_bin}|${sub}`;
      const existing = cells.get(key);
      if (existing) existing.count += 1;
      else cells.set(key, { day, lat_bin, lng_bin, sub, count: 1 });
    }
    if (cells.size === 0) return;

    // Multi-VALUES upsert. ON CONFLICT updates by adding incoming count
    // to existing — additive semantics so multiple flushes per day
    // accumulate correctly.
    const placeholders: string[] = [];
    const params: unknown[] = [];
    let i = 0;
    for (const c of cells.values()) {
      placeholders.push(`($${i + 1}::date, $${i + 2}::float8, $${i + 3}::float8, $${i + 4}::text, $${i + 5}::int)`);
      params.push(c.day, c.lat_bin, c.lng_bin, c.sub, c.count);
      i += 5;
    }
    const sql = `
      INSERT INTO police_heatmap_bin_daily (day, lat_bin, lng_bin, subcategory, count)
      VALUES ${placeholders.join(',')}
      ON CONFLICT (day, lat_bin, lng_bin, subcategory)
      DO UPDATE SET count = police_heatmap_bin_daily.count + EXCLUDED.count
    `;
    await client.query(sql, params);
  }

  /**
   * Aggregate this batch by (day, archive, source, category, subcategory)
   * and UPSERT into filter_facets_daily. Replaces the 5-min GROUP BY
   * scans that filterCache used to run over each archive_* partition,
   * which were timing out at 60s under disk pressure. Stored values
   * preserve raw category/subcategory; the read side does its own
   * filtering (numeric capcodes, empty strings).
   *
   * NULL category/subcategory are coerced to '' because the PRIMARY KEY
   * can't match NULLs under ON CONFLICT — Postgres treats NULL as never
   * equal to NULL.
   */
  private async upsertFilterFacetsDaily(
    client: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
    table: ArchiveTable,
    rows: ArchiveRow[],
  ): Promise<void> {
    if (rows.length === 0) return;
    type Cell = {
      day: string;
      archive: string;
      source: string;
      category: string;
      subcategory: string;
      count: number;
    };
    const cells = new Map<string, Cell>();
    for (const r of rows) {
      const day = new Date(r.fetched_at * 1000).toISOString().slice(0, 10);
      const category = r.category ?? '';
      const subcategory = r.subcategory ?? '';
      const key = `${day}|${table}|${r.source}|${category}|${subcategory}`;
      const existing = cells.get(key);
      if (existing) existing.count += 1;
      else
        cells.set(key, {
          day,
          archive: table,
          source: r.source,
          category,
          subcategory,
          count: 1,
        });
    }
    if (cells.size === 0) return;

    const placeholders: string[] = [];
    const params: unknown[] = [];
    let i = 0;
    for (const c of cells.values()) {
      placeholders.push(
        `($${i + 1}::date, $${i + 2}::text, $${i + 3}::text, $${i + 4}::text, $${i + 5}::text, $${i + 6}::int)`,
      );
      params.push(c.day, c.archive, c.source, c.category, c.subcategory, c.count);
      i += 6;
    }
    const sql = `
      INSERT INTO filter_facets_daily (day, archive, source, category, subcategory, count)
      VALUES ${placeholders.join(',')}
      ON CONFLICT (day, archive, source, category, subcategory)
      DO UPDATE SET count = filter_facets_daily.count + EXCLUDED.count
    `;
    await client.query(sql, params);
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

    // BEGIN/SET LOCAL/COMMIT lifted to insertBatch — this method now
    // assumes the caller already holds an open transaction with the
    // statement_timeout set.
    await client.query(insertSql, params);
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
