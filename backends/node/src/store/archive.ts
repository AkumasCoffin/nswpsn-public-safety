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
import { createHash } from 'node:crypto';
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
  /** Epoch seconds the upstream feed published / last-updated this
   *  incident — extracted from the source's own timestamp field.
   *  Null when the upstream doesn't expose a timestamp; readers
   *  fall back to fetched_at in that case. */
  source_timestamp_unix?: number | null;
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

/**
 * Stable hash of a row's data field for write-time dedup. Excludes
 * fields that change every poll regardless of upstream state — those
 * would defeat the dedup if hashed.
 *
 * Uses sorted JSON.stringify so key ordering doesn't churn the hash;
 * SHA-1 truncated to 20 chars is plenty of entropy for distinguishing
 * states of a single incident (collision space ~10^24 vs handful of
 * snapshots per incident).
 */
function hashRowData(row: ArchiveRow): string {
  const stable = stableSerialize(row.data);
  return createHash('sha1').update(stable).digest('hex').slice(0, 20);
}

/**
 * JSON.stringify with deterministic key order at every nesting level.
 * Cheaper than canonicalising RFC 8785 — we only need stability across
 * Node restarts, not interop with other implementations.
 */
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

    // archive_waze: 3 compound indexes + partition PK; large chunks
    // hit timeouts under userscript ingest pressure.
    // archive_power: essential_future polls produce 1000+ rows in a
    // single bucket; on disk-saturated hosts a 500-row chunk has been
    // hitting the 60s timeout (~22 rows/sec sustained insert rate).
    // archive_traffic: similar story for traffic_roadwork (391 rows).
    // archive_misc + archive_rfs stay at 500 — small per-flush volumes.
    const INSERT_CHUNK_SIZE =
      table === 'archive_waze'
        ? 250
        : table === 'archive_power' || table === 'archive_traffic'
          ? 200
          : 500;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      try {
        // 90s per-statement budget. Postgres applies statement_timeout
        // per-statement (not per-transaction), so each chunk INSERT plus
        // the heatmap and facets upserts each get their own clock — this
        // is just defensive headroom for a single chunk on a slow host.
        await client.query("SET LOCAL statement_timeout = '90s'");

        // Write-time dedup (migration 018). Compute a stable hash of
        // each row's data, look up existing sidecar hashes, and only
        // INSERT to the parent when the data has actually changed
        // since we last stored it. Polls that bring no new info
        // bump sidecar.last_seen_at without touching the parent.
        const hashed = rows.map((r) => ({ row: r, hash: hashRowData(r) }));
        const existingHashes = await this.lookupExistingHashes(client, table, hashed);
        const toInsert: ArchiveRow[] = [];
        for (const e of hashed) {
          // Rows with no source_id can't be deduped (no stable key) — always insert.
          if (e.row.source_id == null || e.row.source_id === '') {
            toInsert.push(e.row);
            continue;
          }
          const key = `${e.row.source}${e.row.source_id}`;
          const existing = existingHashes.get(key);
          if (existing === undefined || existing !== e.hash) {
            toInsert.push(e.row);
          }
        }

        if (toInsert.length > 0) {
          for (let start = 0; start < toInsert.length; start += INSERT_CHUNK_SIZE) {
            const slice = toInsert.slice(start, start + INSERT_CHUNK_SIZE);
            await this.insertChunk(client, table, slice);
          }
        }

        // Always UPSERT the sidecar so last_seen_at advances even when
        // the data was unchanged. Sets latest_fetched_at + data_hash
        // only when EXCLUDED.data_hash differs from the stored one
        // (so unchanged-row UPSERTs don't drift the change pointer).
        await this.upsertLatestSidecar(client, table, hashed);

        // Incremental heatmap aggregation. Aggregates the rows we
        // actually inserted (= changed rows) so the heatmap reflects
        // real movement, not poll cadence.
        if (table === 'archive_waze') {
          await this.upsertPoliceHeatmapBinDaily(client, toInsert);
        }
        if (table !== 'archive_waze') {
          await this.upsertFilterFacetsDaily(client, table, toInsert);
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
   * Bulk-fetch existing data_hash values from the sidecar for the
   * incoming batch. One SELECT regardless of batch size, using a
   * tuple IN-clause with PK lookups via the (source, source_id) PK
   * index. Returns a map of `sourcesource_id` -> data_hash.
   */
  private async lookupExistingHashes(
    client: { query: (sql: string, params?: unknown[]) => Promise<{ rows: Array<{ source: string; source_id: string; data_hash: string | null }> }> },
    table: ArchiveTable,
    hashed: Array<{ row: ArchiveRow; hash: string }>,
  ): Promise<Map<string, string>> {
    // Dedupe lookup keys by (source, source_id) — a flush can have
    // multiple rows for the same incident, but we only need one
    // SELECT per pair.
    const seenKey = new Set<string>();
    const params: unknown[] = [];
    const placeholders: string[] = [];
    for (const e of hashed) {
      if (e.row.source_id == null || e.row.source_id === '') continue;
      const key = `${e.row.source}${e.row.source_id}`;
      if (seenKey.has(key)) continue;
      seenKey.add(key);
      placeholders.push(`($${params.length + 1}, $${params.length + 2})`);
      params.push(e.row.source, e.row.source_id);
    }
    const out = new Map<string, string>();
    if (placeholders.length === 0) return out;
    const sql = `
      SELECT source, source_id, data_hash
        FROM ${table}_latest
       WHERE (source, source_id) IN (${placeholders.join(',')})
    `;
    const r = await client.query(sql, params);
    for (const row of r.rows) {
      if (row.data_hash != null) {
        out.set(`${row.source}${row.source_id}`, row.data_hash);
      }
    }
    return out;
  }

  /**
   * Maintain the per-table _latest sidecar (migrations 017 + 018).
   * One row per (source, source_id) tracking:
   *   latest_fetched_at — when the most recently STORED parent row was
   *                       inserted. Only advances when data_hash
   *                       changes; stable when polls bring no new info.
   *   last_seen_at      — when we most recently saw this incident in
   *                       any poll, regardless of whether the data
   *                       changed. Bumped every flush.
   *   data_hash         — SHA-1-prefix of the latest stored data, used
   *                       on the next poll to detect "data unchanged"
   *                       and skip the parent INSERT.
   *
   * Within a flush, a (source, source_id) pair can appear multiple
   * times — collapse to the max-fetched_at row's hash so the UPSERT
   * EXCLUDED tuples are unique on the conflict key.
   *
   * Rows with NULL/empty source_id can't be unique=1 deduped (no
   * stable key) and don't go in the sidecar at all.
   */
  private async upsertLatestSidecar(
    client: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
    table: ArchiveTable,
    hashed: Array<{ row: ArchiveRow; hash: string }>,
  ): Promise<void> {
    if (hashed.length === 0) return;
    const map = new Map<
      string,
      {
        source: string;
        source_id: string;
        fetched_at: number;
        hash: string;
        source_ts_unix: number | null;
      }
    >();
    for (const e of hashed) {
      const sid = e.row.source_id;
      if (sid == null || sid === '') continue;
      const key = `${e.row.source}${sid}`;
      const existing = map.get(key);
      if (!existing || e.row.fetched_at > existing.fetched_at) {
        map.set(key, {
          source: e.row.source,
          source_id: sid,
          fetched_at: e.row.fetched_at,
          hash: e.hash,
          source_ts_unix: e.row.source_timestamp_unix ?? null,
        });
      }
    }
    if (map.size === 0) return;

    const placeholders: string[] = [];
    const params: unknown[] = [];
    let i = 0;
    for (const v of map.values()) {
      placeholders.push(
        `($${i + 1}::text, $${i + 2}::text, to_timestamp($${i + 3}::bigint), to_timestamp($${i + 4}::bigint), $${i + 5}::text, $${i + 6}::bigint)`,
      );
      params.push(v.source, v.source_id, v.fetched_at, v.fetched_at, v.hash, v.source_ts_unix);
      i += 6;
    }
    // last_seen_at always advances. latest_fetched_at + data_hash +
    // source_timestamp_unix advance only when EXCLUDED.data_hash
    // differs from the stored one — so a stable incident polled 1000
    // times keeps a single archive row, the sidecar's source-side
    // timestamp stays pinned to whenever the upstream actually
    // updated it, and last_seen_at advances every poll.
    const sql = `
      INSERT INTO ${table}_latest
        (source, source_id, latest_fetched_at, last_seen_at, data_hash, source_timestamp_unix)
      VALUES ${placeholders.join(',')}
      ON CONFLICT (source, source_id) DO UPDATE
        SET last_seen_at = GREATEST(${table}_latest.last_seen_at, EXCLUDED.last_seen_at),
            latest_fetched_at = CASE
              WHEN EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                THEN EXCLUDED.latest_fetched_at
              ELSE ${table}_latest.latest_fetched_at
            END,
            data_hash = CASE
              WHEN EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                THEN EXCLUDED.data_hash
              ELSE ${table}_latest.data_hash
            END,
            source_timestamp_unix = CASE
              WHEN EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                THEN EXCLUDED.source_timestamp_unix
              ELSE ${table}_latest.source_timestamp_unix
            END
    `;
    await client.query(sql, params);
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
