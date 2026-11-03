-- 136: statistics for the new partitioned node_radio_events.
--
-- A partitioned parent starts with none, and autovacuum never analyses one
-- (only its partitions), so until this runs the planner guesses at the
-- per-event grouping lookup's self-join. Samples across the partitions; takes
-- only SHARE UPDATE EXCLUSIVE, so nothing waits on it. The retention job
-- re-runs it daily (services/nodeEventsPruner.ts).
ANALYZE node_radio_events;
