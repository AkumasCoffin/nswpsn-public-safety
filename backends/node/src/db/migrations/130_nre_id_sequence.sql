-- 130: node_radio_events.id becomes a plain sequence default, not an identity.
--
-- Two reasons, one now and one later.
--
-- Now: the ingest path takes the row's id BEFORE inserting it
-- (services/nodeEvents.ts), so the event that opens a call can carry its own
-- id as logical_call_id in the INSERT. Until now that event was inserted and
-- then immediately updated with `SET logical_call_id = id`, and logical_call_id
-- is indexed, so the update could never be HOT: a second heap tuple plus a
-- second entry in every index on the table. Measured on production
-- 2026-11-03, 30 % of rows open a call (not the "one in ~29" that was
-- assumed), which made it ~7,300 non-HOT updates an hour.
--
-- Later: the table is going to become a partitioned table with this one as its
-- first partition, and PostgreSQL 17+ refuses to attach a table that has an
-- identity column of its own. Doing the switch here keeps that cutover purely
-- structural.
--
-- The sequence keeps the name the identity used, and is deliberately NOT
-- `OWNED BY` the column: once this table is a partition that gets dropped at
-- the end of its retention, the parent's default must not go with it.
--
-- All of it is catalogue-only. The ALTERs take ACCESS EXCLUSIVE for
-- milliseconds, and the lock_timeout makes the migration fail (and so the
-- deploy stop before the restart) rather than queue behind a long staff read
-- while every insert queues behind it. Re-running the deploy is the recovery.
SET LOCAL lock_timeout = '10s';

ALTER TABLE node_radio_events ALTER COLUMN id DROP IDENTITY;

CREATE SEQUENCE node_radio_events_id_seq AS bigint;

-- The ALTER above holds ACCESS EXCLUSIVE, so max(id) cannot move under us.
SELECT setval('node_radio_events_id_seq', COALESCE(max(id), 0) + 1, false)
  FROM node_radio_events;

ALTER TABLE node_radio_events
  ALTER COLUMN id SET DEFAULT nextval('node_radio_events_id_seq');

-- An INVALID, empty leftover of an interrupted concurrent rebuild of
-- idx_nre_tg_time_calls, found on production 2026-11-03. Never in a migration.
DROP INDEX IF EXISTS idx_nre_tg_time_calls_ccnew;
