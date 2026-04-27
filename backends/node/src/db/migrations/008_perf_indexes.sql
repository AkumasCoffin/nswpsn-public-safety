-- Performance indexes for the /api/data/history hot paths.
--
-- Identified by the W4 perf audit. The data + count queries on
-- archive_waze were spending most of their budget on JSONB extraction
-- and DISTINCT ON sorts; these indexes let Postgres do those scans
-- index-only.
--
-- All CREATE INDEX statements are CONCURRENTLY-safe but the migration
-- runner doesn't run them outside transactions, so they're plain
-- CREATE INDEX IF NOT EXISTS — first deploy will block briefly per
-- index. Acceptable: the production tables are large but no single
-- index here scans more than ~100GB.

-- 1. Covering composite for the unique=1 path.
--    `DISTINCT ON (source, source_id) ORDER BY source, source_id, fetched_at DESC`
--    can do an index-only scan with this. Earlier the planner had to
--    sort 10k rows from a fetched_at-only index.
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

-- 2. Live-only partial index. The default logs.html view filters to
--    is_live-truthy rows; a partial index halves the row set the
--    count query has to scan.
--    Truthy convention matches formatRecord's read predicate.
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

-- 3. Heatmap helper: only index rows with non-null coords so the
--    heatmap query's `WHERE lat_v IS NOT NULL AND lng_v IS NOT NULL`
--    becomes index-implicit.
CREATE INDEX IF NOT EXISTS idx_archive_waze_heatmap
  ON archive_waze (source, fetched_at DESC)
  WHERE lat IS NOT NULL AND lng IS NOT NULL;
