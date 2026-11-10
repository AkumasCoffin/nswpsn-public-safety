-- Drop nre_tg_time_calls (talkgroup, received_at) INCLUDE (system, wacn), the
-- call-group partial index, from node_radio_events and every partition.
--
-- 382 MB, nearly all of it on the legacy partition, for 162 scans in the two
-- days since statistics were reset on 2026-11-03. Every one of them is the
-- automatic-patch merge (nodeEvents.mergeAutomaticPatch): talkgroup = ANY,
-- received_at within 5 s, call-group rows. That lookup reads a 10-second slice,
-- which nre_time (received_at) serves just as cheaply, and it runs only when a
-- reception carries a patch of two or more talkgroups. The logs talkgroup
-- filter and the talkgroup enrich it was also built for never chose it.
--
-- nre_node_tg_time stays: it is the only node_id-leading index on the day
-- partitions, for node-scoped staff reads and the nodes FK cascade.
--
-- A partitioned index cannot be dropped CONCURRENTLY. DROP INDEX takes ACCESS
-- EXCLUSIVE on the parent and each partition for a catalogue-only change, so
-- the lock_timeout makes the deploy stop rather than queue every insert
-- behind a long read. Re-running the deploy is the recovery.
SET LOCAL lock_timeout = '10s';

DROP INDEX IF EXISTS nre_tg_time_calls;
