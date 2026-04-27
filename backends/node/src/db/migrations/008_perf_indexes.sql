-- noTransaction
-- 008_perf_indexes.sql
--
-- Performance indexes for the /api/data/history hot paths.
--
-- ⚠ Notes:
--   1. Cannot use CREATE INDEX CONCURRENTLY here — Postgres rejects it
--      on partitioned tables (`archive_*` are RANGE-partitioned by month).
--      Plain CREATE INDEX takes a SHARE lock on each partition while
--      building; on archive_waze (multi-million-row partitions) this can
--      block writes for minutes. Acceptable: the writer re-queues failed
--      INSERTs, so the worst case is brief queue growth during deploy.
--   2. statement_timeout = 0 unlocks the migration runner's default 30s
--      cap (set in src/db/migrate.ts).
--   3. `IF NOT EXISTS` makes this idempotent if a previous run got
--      partway through before timing out.

SET statement_timeout = 0;

-- ---------------------------------------------------------------------
-- Covering composite for the unique=1 path.
-- DISTINCT ON (source, source_id) ORDER BY source, source_id, fetched_at DESC
-- can do an index-only scan with this; without it the planner sorts
-- 10k rows from a fetched_at-only index.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_archive_waze_src_sid_ts
  ON archive_waze (source, source_id, fetched_at DESC)
  INCLUDE (lat, lng, category, subcategory);

CREATE INDEX IF NOT EXISTS idx_archive_traffic_src_sid_ts
  ON archive_traffic (source, source_id, fetched_at DESC)
  INCLUDE (lat, lng, category, subcategory);

CREATE INDEX IF NOT EXISTS idx_archive_power_src_sid_ts
  ON archive_power (source, source_id, fetched_at DESC)
  INCLUDE (lat, lng, category, subcategory);

CREATE INDEX IF NOT EXISTS idx_archive_misc_src_sid_ts
  ON archive_misc (source, source_id, fetched_at DESC)
  INCLUDE (lat, lng, category, subcategory);

CREATE INDEX IF NOT EXISTS idx_archive_rfs_src_sid_ts
  ON archive_rfs (source, source_id, fetched_at DESC)
  INCLUDE (lat, lng, category, subcategory);

-- ---------------------------------------------------------------------
-- Live-only partial index. The default logs.html view filters to
-- is_live-truthy rows; the partial index halves the row set for the
-- count query's COUNT(*) FILTER aggregate.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_archive_waze_live
  ON archive_waze (source, fetched_at DESC)
  WHERE (data->>'is_live') IN ('1','true','True');

CREATE INDEX IF NOT EXISTS idx_archive_traffic_live
  ON archive_traffic (source, fetched_at DESC)
  WHERE (data->>'is_live') IN ('1','true','True');

CREATE INDEX IF NOT EXISTS idx_archive_power_live
  ON archive_power (source, fetched_at DESC)
  WHERE (data->>'is_live') IN ('1','true','True');

CREATE INDEX IF NOT EXISTS idx_archive_misc_live
  ON archive_misc (source, fetched_at DESC)
  WHERE (data->>'is_live') IN ('1','true','True');

CREATE INDEX IF NOT EXISTS idx_archive_rfs_live
  ON archive_rfs (source, fetched_at DESC)
  WHERE (data->>'is_live') IN ('1','true','True');

-- ---------------------------------------------------------------------
-- Heatmap helper: only index rows with non-null coords so the query's
-- `WHERE lat IS NOT NULL AND lng IS NOT NULL` becomes index-implicit.
-- archive_waze only — the others don't go through that path.
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_archive_waze_heatmap
  ON archive_waze (source, fetched_at DESC)
  WHERE lat IS NOT NULL AND lng IS NOT NULL;
