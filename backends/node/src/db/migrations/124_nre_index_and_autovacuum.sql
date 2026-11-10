-- 124: drop two redundant indexes on node_radio_events, and let autovacuum
-- actually reach the table.
--
-- The table was carrying FOUR TIMES more index than data: heap 1454 MB,
-- indexes 5795 MB across 14 of them, on a database container with 1 GB of
-- shared_buffers and 5 GB of RAM. That ratio is what decides whether a range
-- scan over one day of detail costs 124 ms (measured warm) or 35 s (measured
-- cold) — and the cold case is what stalled the hourly rollup after the 123
-- deploy, because its catch-up batches could not finish inside the writer
-- pool's statement timeout.
--
-- Two of the fourteen earn nothing.
--
-- idx_nre_node_tg_time_calls_cover (601 MB) is not in any migration and not in
-- any commit on any branch: it was created by hand on the server. It leads on
-- the same (node_id, talkgroup, received_at) as idx_nre_node_tg_time (044) and
-- adds the call predicate plus four INCLUDE columns. Those columns were the
-- point of it and they are not working: pg_stat_user_indexes reports
-- idx_tup_fetch 3,825,368 against idx_tup_read 3,877,890, i.e. 99 % of its
-- rows still go to the heap, so it is not getting index-only scans at all.
-- With continuous inserts and ~17 % dead tuples the visibility map is almost
-- never all-visible, which is why — and migration 110 declined to add more
-- INCLUDE columns to its own index for exactly this reason.
--
-- idx_nre_node_time (043, 472 MB) was superseded and never dropped. Migration
-- 116 diagnosed it as the wrong shape for the node-scoped queries behind
-- /api/feeder/nodes/:id/stats — not partial on the call predicate, so the
-- planner preferred the system/site indexes and discarded tens of thousands of
-- other nodes' rows — and added idx_nre_node_time_calls (node_id, received_at)
-- WHERE <call predicate> in its place. Every node_id+time read now carries that
-- predicate except services/nodeEvents.ts:801 (markRecorded) and :887 (its
-- miss-only diagnostic), and both of those also filter talkgroup, so they want
-- idx_nre_node_tg_time rather than this one.
--
-- To put either back, if a staff page turns slow:
--
--   CREATE INDEX CONCURRENTLY idx_nre_node_time
--     ON node_radio_events (node_id, received_at);
--
--   CREATE INDEX CONCURRENTLY idx_nre_node_tg_time_calls_cover
--     ON node_radio_events (node_id, talkgroup, received_at)
--     INCLUDE (wacn, system, site_rfss, site_id)
--     WHERE event_type LIKE 'CALL_GROUP%' OR event_type LIKE 'CALL_PATCH_GROUP%';
--
-- A plain DROP takes a brief ACCESS EXCLUSIVE lock on the table. It is a
-- catalog change plus a file unlink, so it is milliseconds, and the API is
-- restarting around this migration anyway.
DROP INDEX IF EXISTS idx_nre_node_tg_time_calls_cover;
DROP INDEX IF EXISTS idx_nre_node_time;

-- Autovacuum could not keep up, because it was never being asked to.
--
-- node_radio_events sat at 968,716 dead tuples against 5.63 M live — 17.2 % —
-- with its last autovacuum three days earlier. At the cluster default
-- autovacuum_vacuum_scale_factor of 0.2 the trigger is ~1.13 M dead tuples, so
-- the table idled permanently just below it, and no table in this database had
-- a per-table override at all. 0.05 holds it near 5 % instead, which is the
-- difference between a heap whose live rows are dense enough to stay in page
-- cache and one that is a fifth air.
--
-- The analyze factor comes down with it: the planner's row estimates on a
-- table taking continuous ingest go stale between vacuums, and this is the
-- table every rollup and every staff Data page plans against.
--
-- adsb_tracks is the same story at 11.2 %, from the same cause (per-position
-- updates on a table that is also pruned on a timer).
ALTER TABLE node_radio_events
  SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.05);
ALTER TABLE adsb_tracks
  SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.05);
