/**
 * ArchiveWriter — the archive half of the new architecture.
 *
 * Append-only batched insert into the per-source `archive_*` tables.
 * **No UPDATE statements on the parent tables.** The write side does
 * exactly one thing: every poll snapshot becomes one INSERT, batched
 * with siblings for the same flush window. (The `_latest` sidecar
 * does carry one UPSERT per flush — that's where mutable per-incident
 * state lives.)
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

export type ArchiveTable =
  | 'archive_waze'
  | 'archive_traffic'
  | 'archive_rfs'
  | 'archive_power'
  | 'archive_misc';

/**
 * Namespace key for the per-`_latest`-table advisory lock that
 * serialises every sidecar mutation. The live writer's upsert and the
 * one-shot backfills (archiveLatestDimsBackfill / archiveLatestBackfill)
 * all take `pg_advisory_xact_lock(SIDECAR_LOCK_NAMESPACE, hashtext(
 * '<table>_latest'))` before touching the sidecar, so they take turns
 * rather than interleaving row locks in opposite orders and deadlocking
 * (SQLSTATE 40P01). Sorting/ORDER BY can't guarantee a shared lock order
 * across an INSERT…ON CONFLICT and an UPDATE…FROM — a mutex can.
 * Uncontended (after the one-shot backfills finish) it's a no-op.
 */
export const SIDECAR_LOCK_NAMESPACE = 0x51de; // "sidecar"

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

// Max rows per table written in a single flush. A persistent DB slowdown
// was letting the queue grow to 15k+ rows; each flush then wrapped ALL of
// them in one transaction that blew the statement_timeout, and the failure
// re-queued the whole lot — so the next flush was even bigger. That death
// spiral pinned the DB and starved everything else. Capping per table makes
// every flush bounded and able to complete; overflow waits for the next
// 30s cycle, and HARD_CAP still sheds the oldest if the backlog runs away.
const MAX_PER_TABLE_PER_FLUSH = Math.max(
  500,
  Number(process.env['ARCHIVE_MAX_ROWS_PER_TABLE_PER_FLUSH'] ?? '3000') || 3000,
);

/**
 * Top-level keys excluded from the dedup hash. These are "noise" fields
 * that change every poll regardless of whether the underlying incident
 * actually changed, so including them in the hash forces a fresh INSERT
 * on every poll and defeats the dedup. Verified by per-field diff query
 * against archive_power: lastUpdated changed 5,615× in 6h, streets
 * changed 1,367× (upstream rotates among multiple affected addresses on
 * the same outage), and title + location_text are derived aliases of
 * streets (archiveExtract.applyAliases at line 282/290) so they MUST be
 * stripped here too or they'd re-introduce the noise.
 *
 * Fields stay in the data blob — readers still see them via JSONB
 * projections. Only the dedup-equality check ignores them.
 */
const DEDUP_HASH_IGNORE = new Set<string>([
  'lastUpdated',     // endeavour upstream per-poll timestamp
  'streets',         // endeavour upstream rotates among affected addresses
  'title',           // alias of streets / name / headline (derived)
  'location_text',   // alias of streets / suburb (derived)
]);

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
/**
 * Pull a string value out of a (potentially) JSONB-ish data blob. Used
 * by upsertLatestSidecar to populate the title/location_text/status/
 * severity columns added in migration 022 — those fields used to live
 * inside `data` and got extracted at read time with `(data->>'key')`,
 * which doesn't use indexes. Storing them top-level on the sidecar
 * lets WHERE clauses hit a real index.
 */
function extractStrField(data: unknown, key: string): string | null {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
  const v = (data as Record<string, unknown>)[key];
  if (typeof v === 'string' && v.length > 0) return v;
  return null;
}

/** Same shape as extractStrField but for boolean-ish values. Accepts
 *  native booleans, '0'/'1' strings, and 'true'/'false' (any case) so
 *  the writer doesn't reject upstream payloads that serialise the
 *  field as a string. is_active falls back to null when absent — the
 *  reader's truthy check treats null as "unknown, default visible". */
function extractBoolField(data: unknown, key: string): boolean | null {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
  const v = (data as Record<string, unknown>)[key];
  if (typeof v === 'boolean') return v;
  if (typeof v === 'number') return v !== 0;
  if (typeof v === 'string') {
    const t = v.trim().toLowerCase();
    if (t === '1' || t === 'true') return true;
    if (t === '0' || t === 'false') return false;
  }
  return null;
}

function hashRowData(row: ArchiveRow): string {
  let data: unknown = row.data;
  if (data && typeof data === 'object' && !Array.isArray(data)) {
    const filtered: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(data as Record<string, unknown>)) {
      if (!DEDUP_HASH_IGNORE.has(k)) filtered[k] = v;
    }
    data = filtered;
  }
  const stable = stableSerialize(data);
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
    // Final drain so we don't lose buffered rows on shutdown. A single
    // flush() isn't enough: if a periodic flush is mid-drain, flush()
    // no-ops (this.flushing guard) and rows pushed after that drain
    // started would be dropped on exit. Wait out any in-flight drain
    // and keep flushing until the queue is empty — bounded so a wedged
    // DB can't hang shutdown forever.
    const deadline = Date.now() + 30_000;
    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
    for (;;) {
      if (Date.now() > deadline) {
        if (this.queue.length > 0) {
          log.warn(
            { queued: this.queue.length },
            'ArchiveWriter: shutdown drain deadline hit; dropping queued rows',
          );
        }
        break;
      }
      if (this.flushing) {
        await sleep(100);
        continue;
      }
      if (this.queue.length === 0) break;
      const before = this.queue.length;
      await this.flush();
      if (this.queue.length >= before) {
        // No forward progress (DB down/unconfigured or batch failed and
        // re-queued) — brief pause so this can't spin, retry to deadline.
        await sleep(500);
      }
    }
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

    // Bound per-table work so a backlog can't build one giant transaction
    // that times out (and then re-queues even bigger). Defer the overflow
    // to the next flush cycle.
    let deferred = 0;
    for (const [table, rows] of buckets) {
      if (rows.length > MAX_PER_TABLE_PER_FLUSH) {
        const overflow = rows.splice(MAX_PER_TABLE_PER_FLUSH);
        for (const row of overflow) this.push(table, row);
        deferred += overflow.length;
      }
    }
    if (deferred > 0) {
      log.info({ deferred, queued: this.queue.length }, 'archive flush: per-table cap hit; deferred overflow to next cycle');
    }

    // Tombstone INSERTs removed: incident state lives on the sidecar
    // (the writer's upsert below always advances last_seen_at). No
    // more "we noticed it ended" rows accumulating in the parent
    // archive — those rows added ~25% to the archive volume for no
    // semantic value the sidecar can't already provide.

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
        const allHashed = rows.map((r) => ({ row: r, hash: hashRowData(r) }));
        // Within-batch dedup pass: when overlapping waze polls land in
        // the same flush, we get the same (source, source_id) row 2-3
        // times — all hash-identical, but the cross-batch sidecar check
        // would admit all of them because the sidecar has no entry yet
        // for this incident. Collapse to one survivor (max fetched_at)
        // before consulting the sidecar.
        const collapsed = new Map<string, { row: ArchiveRow; hash: string }>();
        const noKeyRows: Array<{ row: ArchiveRow; hash: string }> = [];
        for (const e of allHashed) {
          const sid = e.row.source_id;
          if (sid == null || sid === '') {
            noKeyRows.push(e);
            continue;
          }
          // \x01 separator matches lookupExistingHashes' keys — without
          // one, ('essential', '_current123') and ('essential_current',
          // '123') would collide.
          const key = `${e.row.source}\x01${sid}`;
          const existingInBatch = collapsed.get(key);
          if (!existingInBatch || e.row.fetched_at > existingInBatch.row.fetched_at) {
            collapsed.set(key, e);
          }
        }
        const hashed = [...collapsed.values(), ...noKeyRows];
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

        // Serialise the sidecar write against the one-shot backfills.
        // Taken late (parent inserts already done) so the lock window is
        // just the sidecar/heatmap/facets tail of the txn; released at
        // COMMIT. See SIDECAR_LOCK_NAMESPACE.
        await client.query(
          'SELECT pg_advisory_xact_lock($1, hashtext($2))',
          [SIDECAR_LOCK_NAMESPACE, `${table}_latest`],
        );

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
    const sources: string[] = [];
    const sourceIds: string[] = [];
    for (const e of hashed) {
      if (e.row.source_id == null || e.row.source_id === '') continue;
      const key = `${e.row.source}${e.row.source_id}`;
      if (seenKey.has(key)) continue;
      seenKey.add(key);
      sources.push(e.row.source);
      sourceIds.push(e.row.source_id);
    }
    const out = new Map<string, string>();
    if (sources.length === 0) return out;
    // unnest($1::text[], $2::text[]) replaces the prior tuple-IN list.
    // Old shape sent 2 params per (source, source_id), so a 5k-incident
    // waze flush meant 10k params; Postgres switches join strategies
    // around 1k tuples and slows down. The array form sends exactly 2
    // params no matter how many keys we're looking up — planner caches
    // the plan once and reuses it forever.
    const sql = `
      SELECT source, source_id, data_hash
        FROM ${table}_latest
       WHERE (source, source_id) IN (
         SELECT s, sid FROM unnest($1::text[], $2::text[]) AS u(s, sid)
       )
    `;
    const r = await client.query(sql, [sources, sourceIds]);
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
        category: string | null;
        subcategory: string | null;
        title: string | null;
        location_text: string | null;
        status: string | null;
        severity: string | null;
        is_active: boolean | null;
      }
    >();
    for (const e of hashed) {
      const sid = e.row.source_id;
      if (sid == null || sid === '') continue;
      const key = `${e.row.source}\x01${sid}`; // separator: see collapse map above
      const existing = map.get(key);
      if (!existing || e.row.fetched_at > existing.fetched_at) {
        map.set(key, {
          source: e.row.source,
          source_id: sid,
          fetched_at: e.row.fetched_at,
          hash: e.hash,
          source_ts_unix: e.row.source_timestamp_unix ?? null,
          category: e.row.category ?? null,
          subcategory: e.row.subcategory ?? null,
          title: extractStrField(e.row.data, 'title'),
          location_text: extractStrField(e.row.data, 'location_text'),
          status: extractStrField(e.row.data, 'status'),
          severity: extractStrField(e.row.data, 'severity'),
          is_active: extractBoolField(e.row.data, 'is_active'),
        });
      }
    }
    if (map.size === 0) return;

    // Deterministic lock order. Every concurrent transaction that writes
    // ${table}_latest — this upsert, archiveLatestDimsBackfill's UPDATE,
    // archiveLatestBackfill's INSERT — must acquire row locks on
    // (source, source_id) in the SAME order, or two batches that overlap
    // on a key deadlock (SQLSTATE 40P01 "deadlock detected" while
    // inserting an index tuple in archive_*_latest). For INSERT ... ON
    // CONFLICT, lock acquisition follows the VALUES order, so sorting the
    // tuples by the conflict key here pins this side of the contract.
    const ordered = [...map.values()].sort((a, b) =>
      a.source === b.source
        ? a.source_id < b.source_id
          ? -1
          : a.source_id > b.source_id
            ? 1
            : 0
        : a.source < b.source
          ? -1
          : 1,
    );

    // 13 params per row: source, source_id, fetched_at (×2 — also
    // becomes last_seen_at on first insert), data_hash, source_ts_unix,
    // category, subcategory, title, location_text, status, severity,
    // is_active.
    const placeholders: string[] = [];
    const params: unknown[] = [];
    let i = 0;
    for (const v of ordered) {
      placeholders.push(
        `($${i + 1}::text, $${i + 2}::text, ` +
          `to_timestamp($${i + 3}::bigint), to_timestamp($${i + 4}::bigint), ` +
          `$${i + 5}::text, $${i + 6}::bigint, ` +
          `$${i + 7}::text, $${i + 8}::text, ` +
          `$${i + 9}::text, $${i + 10}::text, ` +
          `$${i + 11}::text, $${i + 12}::text, $${i + 13}::boolean)`,
      );
      params.push(
        v.source,
        v.source_id,
        v.fetched_at,
        v.fetched_at,
        v.hash,
        v.source_ts_unix,
        v.category,
        v.subcategory,
        v.title,
        v.location_text,
        v.status,
        v.severity,
        v.is_active,
      );
      i += 13;
    }
    // last_seen_at always advances. data_hash + latest_fetched_at refresh
    // only when EXCLUDED.data_hash differs from the stored hash — i.e.
    // when the upstream actually published a new state for this incident.
    // A stable incident polled 1000 times keeps a single archive row.
    //
    // Display fields (category, subcategory, title, location_text, status,
    // severity, is_active, source_timestamp_unix) ALSO refresh when the
    // stored value is NULL — this self-heals sidecar rows that were
    // created before migrations 021/022 added these columns and have been
    // NULL ever since because the upstream's data_hash hasn't drifted.
    // For active incidents the next poll fills them in; HOT updates stay
    // intact in the dominant case (incident already populated + unchanged)
    // because the CASE returns the same value and Postgres treats that
    // as a no-op write.
    const sql = `
      INSERT INTO ${table}_latest
        (source, source_id, latest_fetched_at, last_seen_at, data_hash,
         source_timestamp_unix, category, subcategory,
         title, location_text, status, severity, is_active)
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
              WHEN ${table}_latest.source_timestamp_unix IS NULL
                OR EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                THEN EXCLUDED.source_timestamp_unix
              ELSE ${table}_latest.source_timestamp_unix
            END,
            category = CASE
              WHEN ${table}_latest.category IS NULL
                OR EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                OR EXCLUDED.category IS DISTINCT FROM ${table}_latest.category
                THEN EXCLUDED.category
              ELSE ${table}_latest.category
            END,
            subcategory = CASE
              WHEN ${table}_latest.subcategory IS NULL
                OR EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                OR EXCLUDED.subcategory IS DISTINCT FROM ${table}_latest.subcategory
                THEN EXCLUDED.subcategory
              ELSE ${table}_latest.subcategory
            END,
            title = CASE
              WHEN ${table}_latest.title IS NULL
                OR EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                THEN EXCLUDED.title
              ELSE ${table}_latest.title
            END,
            location_text = CASE
              WHEN ${table}_latest.location_text IS NULL
                OR EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                THEN EXCLUDED.location_text
              ELSE ${table}_latest.location_text
            END,
            status = CASE
              WHEN ${table}_latest.status IS NULL
                OR EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                THEN EXCLUDED.status
              ELSE ${table}_latest.status
            END,
            severity = CASE
              WHEN ${table}_latest.severity IS NULL
                OR EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                THEN EXCLUDED.severity
              ELSE ${table}_latest.severity
            END,
            is_active = CASE
              WHEN ${table}_latest.is_active IS NULL
                OR EXCLUDED.data_hash IS DISTINCT FROM ${table}_latest.data_hash
                THEN EXCLUDED.is_active
              ELSE ${table}_latest.is_active
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

    // APPEND-ONLY writes — pure INSERT, no UPDATE on the parent.
    // The is_latest / is_live columns added in migration 012 were
    // dropped in 013 after they caused chronic I/O contention and
    // write timeouts; mutable per-incident state now lives on the
    // _latest sidecar.
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
