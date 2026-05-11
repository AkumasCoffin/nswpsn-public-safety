-- 011_police_heatmap_cache.sql
--
-- Materialise the police heatmap aggregation into a real table,
-- mirroring python's `police_heatmap_cache`. Reasoning:
--
-- The heatmap query hammers archive_waze every 5 min — scanning
-- ~290k rows × 30 days, doing GROUP BY (lat_bin, lng_bin) plus a
-- subcategory filter. With ongoing waze ingest writes + dead-tuple
-- bloat from periodic UPDATEs, this aggregation reliably hits the
-- 60s statement_timeout. Python avoided this by pre-aggregating
-- once every 10 min in a background thread and serving requests
-- from a tiny indexed cache table — sub-millisecond on every hit.
--
-- This table holds one row per (lat_bin, lng_bin, subcategory). The
-- background refresher (services/policeHeatmapCache.ts) TRUNCATE+
-- INSERTs into it; the route handler reads from it filtered by
-- subtype + bbox.

CREATE TABLE IF NOT EXISTS police_heatmap_cache (
  lat_bin     DOUBLE PRECISION NOT NULL,
  lng_bin     DOUBLE PRECISION NOT NULL,
  subcategory TEXT             NOT NULL,
  count       INTEGER          NOT NULL,
  updated_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
  PRIMARY KEY (lat_bin, lng_bin, subcategory)
);

-- Index for the bbox + subtype filter the route handler uses.
CREATE INDEX IF NOT EXISTS idx_police_heatmap_cache_bbox
  ON police_heatmap_cache (subcategory, lat_bin, lng_bin);
