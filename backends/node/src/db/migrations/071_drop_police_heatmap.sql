-- Drop the police-heatmap derived tables.
--
-- The Waze ingest was retired in cca6872 (the police pins and density heatmap
-- came out of map.html) and the server-side machinery that fed these went with
-- it: the /api/waze/police-heatmap endpoint, its in-process cache, the 5-minute
-- RAM refresh and the 10-minute materialised refresh. Nothing reads either
-- table now, and the writers that kept them growing are gone — so they were
-- pure retained bytes plus a nightly prune doing work for no reader.
--
-- Safe to drop and cheap to rebuild if ever needed: both are DERIVED entirely
-- from archive_waze, which is deliberately kept so /api/data/history and the
-- logs browser can still serve the retained history. police_heatmap_bin_daily
-- was itself created empty and backfilled from archive_waze by indexBuilder
-- (migration 015), and police_heatmap_cache is a materialised aggregation
-- (migration 011). Regenerating either is a query, not a data-loss event.
--
-- IF EXISTS so this is idempotent across environments that never ran the
-- heatmap (a fresh install, or a node that predates migration 011/015).
DROP TABLE IF EXISTS police_heatmap_cache;
DROP TABLE IF EXISTS police_heatmap_bin_daily;
