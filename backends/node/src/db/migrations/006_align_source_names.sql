-- 006_align_source_names.sql
-- Rename archive rows that the early Node poller wrote under LiveStore
-- keys instead of python's canonical data_history.source values.
-- Without this, /api/data/history?source=rfs (and bom_warning, and
-- traffic_incident) returns zero matches for any rows the Node poller
-- ingested between the cutover and the source-name fix — only the
-- migration-backfilled python rows showed up.
--
-- The poller now uses src.archiveSource (rfs_incidents -> rfs,
-- bom_warnings -> bom_warning, traffic_incidents -> traffic_incident);
-- this migration retroactively patches anything already written with
-- the wrong source value so historical and live data agree.

UPDATE archive_rfs
   SET source = 'rfs'
 WHERE source = 'rfs_incidents';

UPDATE archive_misc
   SET source = 'bom_warning'
 WHERE source = 'bom_warnings';

UPDATE archive_traffic
   SET source = 'traffic_incident'
 WHERE source = 'traffic_incidents';
