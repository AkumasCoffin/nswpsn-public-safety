-- 016_filter_facets_daily.sql
--
-- Day-bucketed incremental facet aggregation. Replaces the 5-min
-- GROUP BY scans over archive_misc / archive_traffic / archive_rfs /
-- archive_power that filterCache used to run — every cycle was hitting
-- the 60s statement_timeout under disk pressure, leaving the dropdown
-- with stale or zero counts.
--
-- The archive writer (store/archive.ts) upserts into this table on
-- every flush, scoped to (day, archive, source, category, subcategory).
-- filterCache reads a 7-day SUM per (source, category, subcategory)
-- in milliseconds.
--
-- archive_waze rows are NOT counted here — waze counts come from
-- LiveStore (the in-memory waze ingest cache), so adding archive_waze
-- rows would double-count.
--
-- category/subcategory are NOT NULL with default '' so the PRIMARY KEY
-- can match in ON CONFLICT (NULL never equals NULL in PG, so a NULL
-- column would prevent the upsert from finding the existing row).
-- Read-side treats '' as "no value".
--
-- Storage is bounded: each day adds at most a few hundred distinct
-- (source, category, subcategory) cells across all 4 archive tables.
-- 14-day retention × ~300 cells/day × ~80 B/row = ~340 KB total.

CREATE TABLE IF NOT EXISTS filter_facets_daily (
  day         DATE    NOT NULL,
  archive     TEXT    NOT NULL,
  source      TEXT    NOT NULL,
  category    TEXT    NOT NULL DEFAULT '',
  subcategory TEXT    NOT NULL DEFAULT '',
  count       INTEGER NOT NULL,
  PRIMARY KEY (day, archive, source, category, subcategory)
);

-- Serves the prune path (DELETE WHERE day < cutoff in cleanup) and
-- any future range-scan reads.
CREATE INDEX IF NOT EXISTS idx_filter_facets_daily_day
  ON filter_facets_daily (day);
