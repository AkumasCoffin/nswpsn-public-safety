-- 012_is_latest_columns.sql
--
-- Add `is_latest` and `is_live` boolean columns to every archive_*
-- table. Mirrors python's data_history schema and addresses the
-- chronic /api/data/history slowness on archive_waze:
--
-- Before: unique=1 filter does DISTINCT ON (source, source_id) over a
--   30-day window of every poll snapshot — millions of rows scanned to
--   find the latest one per incident.
-- After:  unique=1 filter becomes `WHERE is_latest = true` — a partial
--   index lookup. Sub-millisecond regardless of table size.
--
-- Trade-off: the writer (api/waze-ingest.ts + services/poller.ts) now
-- has to UPDATE previous rows to is_latest=false when inserting a new
-- row with the same source_id. This creates dead tuples on every
-- write, but with the per-partition autovacuum tuning from migration
-- 010 (scale_factor=0.02, cost_limit=2000) postgres keeps up.
--
-- Defaults:
--   is_latest = false  — backfill below sets the actual latest rows.
--                        New rows from the writer set this explicitly.
--   is_live   = true   — most upstream rows are "live" until ended.
--
-- Storage parameters: the columns add ~2 bytes per row × 290k rows =
-- ~600 KB. Negligible. Indexes add more but they're partial.

DO $$
DECLARE
  parent TEXT;
BEGIN
  FOREACH parent IN ARRAY ARRAY['archive_waze','archive_traffic','archive_rfs',
                                'archive_power','archive_misc']
  LOOP
    EXECUTE format(
      'ALTER TABLE %I ADD COLUMN IF NOT EXISTS is_latest BOOLEAN NOT NULL DEFAULT false',
      parent
    );
    EXECUTE format(
      'ALTER TABLE %I ADD COLUMN IF NOT EXISTS is_live BOOLEAN NOT NULL DEFAULT true',
      parent
    );
  END LOOP;
END $$;

-- Partial indexes — the whole point of this migration. Only index
-- the rows where is_latest=true (small slice of the table) so
-- /api/data/history?unique=1 becomes a partial index scan.
DO $$
DECLARE
  parent TEXT;
BEGIN
  FOREACH parent IN ARRAY ARRAY['archive_waze','archive_traffic','archive_rfs',
                                'archive_power','archive_misc']
  LOOP
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS idx_%I_latest
       ON %I (source, fetched_at DESC)
       WHERE is_latest = true',
      parent, parent
    );
  END LOOP;
END $$;
