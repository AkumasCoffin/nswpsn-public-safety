-- 015_police_heatmap_bin_daily.sql
--
-- Day-bucketed incremental aggregation table for the police heatmap.
-- Replaces the 30-day GROUP BY scan over archive_waze that was hanging
-- the heatmap refresh for 30+ minutes per cycle on disk-IO-saturated
-- hosts (every refresh sat in IO/AioIoCompletion while autovacuum,
-- archiveLiveness, and the archive flush all queued behind it).
--
-- The archive writer (store/archive.ts) upserts into this table on
-- every waze flush, scoped to (day, lat_bin, lng_bin, subcategory).
-- The heatmap refresher (services/policeHeatmapCache.ts) reads a
-- 30-day SUM over this table — milliseconds instead of half an hour.
--
-- Storage is bounded: at most a few thousand populated cells per day
-- (NSW × 0.001° grid, typical police-alert density). 30 days × ~3k
-- cells × ~50B/row = ~5 MB total.

CREATE TABLE IF NOT EXISTS police_heatmap_bin_daily (
  day         DATE             NOT NULL,
  lat_bin     DOUBLE PRECISION NOT NULL,
  lng_bin     DOUBLE PRECISION NOT NULL,
  subcategory TEXT             NOT NULL,
  count       INTEGER          NOT NULL,
  PRIMARY KEY (day, lat_bin, lng_bin, subcategory)
);

-- The PK serves the upsert path. This index serves the prune path
-- (DELETE WHERE day < now() - 30d in cleanup) and any future
-- range-scan reads.
CREATE INDEX IF NOT EXISTS idx_police_heatmap_bin_daily_day
  ON police_heatmap_bin_daily (day);
