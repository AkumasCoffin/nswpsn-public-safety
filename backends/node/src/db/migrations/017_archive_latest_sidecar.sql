-- 017_archive_latest_sidecar.sql
--
-- Sidecar tables: one row per (source, source_id) holding the id +
-- fetched_at of that incident's most recent row in the parent
-- archive_*. Lets unique=1 history reads do an O(N_unique_incidents)
-- index walk + PK JOIN instead of the bounded DISTINCT ON over
-- UNIQUE_INNER_CAP recent rows, which had been timing out at 30s on
-- archive_waze under ingest pressure.
--
-- This is the third attempt at "latest per incident" for this codebase:
--   - Original Python: is_latest column on data_history. Worked but
--     required the writer to flip is_latest=false on the previous row,
--     creating UPDATE/INSERT contention.
--   - Migration 012: revived is_latest on archive_*. Same contention,
--     reverted in 013.
--   - This migration: keep the parent tables append-only; do the
--     "which row is latest" bookkeeping in a *separate* table, so the
--     hot ingest path stays pure-INSERT.
--
-- On every successful chunked INSERT in ArchiveWriter.insertBatch the
-- writer issues a multi-VALUES UPSERT into the matching sidecar with
-- ON CONFLICT (source, source_id) DO UPDATE WHERE EXCLUDED.fetched_at
-- > existing.fetched_at. Same transaction as the parent INSERT, so the
-- two stay atomic.
--
-- Backfill is *not* done in this migration — running DISTINCT ON over
-- archive_waze inside a migration could exceed statement_timeout, and
-- the migration runner blocks startup. A separate startup task in
-- src/services/archiveLatestBackfill.ts walks each parent in id-order
-- chunks and fills the sidecar incrementally; it's idempotent (the
-- ON CONFLICT does the right thing) and safe to re-run.

DO $$
DECLARE
  archive_table text;
BEGIN
  FOREACH archive_table IN ARRAY ARRAY[
    'archive_waze',
    'archive_traffic',
    'archive_rfs',
    'archive_power',
    'archive_misc'
  ]
  LOOP
    -- Schema: (source, source_id) PK + latest_fetched_at to point at the
    -- most recent parent row for that incident. Reads JOIN to the parent
    -- on (source, source_id, fetched_at) — those columns are already
    -- indexed via idx_archive_*_src_sid_ts. We deliberately don't store
    -- the parent row's id here: it would require RETURNING from every
    -- INSERT chunk (extra round-trip + plumbing) for a degenerate edge
    -- case (two rows with identical source/source_id/fetched_at) that
    -- the dedup-per-poll writer doesn't produce.
    EXECUTE format($f$
      CREATE TABLE IF NOT EXISTS %I (
        source             TEXT        NOT NULL,
        source_id          TEXT        NOT NULL,
        latest_fetched_at  TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (source, source_id)
      )
    $f$, archive_table || '_latest');

    -- Order-by-time index for the no-source-filter unique=1 path.
    EXECUTE format($f$
      CREATE INDEX IF NOT EXISTS %I
        ON %I (latest_fetched_at DESC)
    $f$, archive_table || '_latest_ts', archive_table || '_latest');

    -- Per-source order-by-time index for the common
    -- ?source=...&unique=1 path. Composite supports both
    -- WHERE source = ? ORDER BY latest_fetched_at DESC and
    -- WHERE source IN (...) ORDER BY latest_fetched_at DESC scans.
    EXECUTE format($f$
      CREATE INDEX IF NOT EXISTS %I
        ON %I (source, latest_fetched_at DESC)
    $f$, archive_table || '_latest_src_ts', archive_table || '_latest');
  END LOOP;
END $$;
